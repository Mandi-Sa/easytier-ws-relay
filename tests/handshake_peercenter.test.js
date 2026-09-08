import { test } from 'node:test';
import assert from 'node:assert/strict';
import { handshakeDigestBytes, digestBytesFromGroupKey } from '../src/worker/core/env.js';
import { PeerManager } from '../src/worker/core/peer_manager.js';
import { sniffPeerCenterReport, requestGlobalPeerMapFromCenter, handleRpcResp } from '../src/worker/core/rpc_handler.js';
import { MAGIC, MY_PEER_ID, PacketType } from '../src/worker/core/constants.js';
import { handleHandshake } from '../src/worker/core/basic_handlers.js';
import { parseHeader } from '../src/worker/core/packet.js';
import zlib from 'zlib';

function confirm(pm, a = 111, b = 222) {
  pm.ingestPeerCenterEdges('g', a, [[a, b]]);
  pm.ingestPeerCenterEdges('g', b, [[b, a]]);
}

test('handshake digest is always 32 bytes and echoes the client digest', () => {
  const src = Buffer.from('0123456789abcdef0123456789abcdef');
  const out = handshakeDigestBytes(src);
  assert.equal(out.length, 32);
  assert.equal(Buffer.compare(out, src), 0);
  assert.equal(handshakeDigestBytes(Buffer.alloc(0)).length, 32);
  assert.ok(handshakeDigestBytes(null).equals(Buffer.alloc(32)));
});

test('group key digest is recovered from name:hex', () => {
  const hex = '11'.repeat(32);
  const buf = digestBytesFromGroupKey(`public_server:${hex}`);
  assert.equal(buf.length, 32);
  assert.equal(buf[0], 0x11);
  assert.equal(buf[31], 0x11);
});

test('handshake replies with the client digest and pushes routes immediately', () => {
  const digest = Buffer.alloc(32, 7);
  const encoded = Buffer.from('hs');
  const types = {
    HandshakeRequest: {
      decode() {
        return {
          magic: MAGIC,
          myPeerId: 111,
          version: 1,
          features: [],
          networkName: 'public_server',
          networkSecretDigrest: digest,
        };
      },
      encode(payload) {
        return {
          finish() {
            assert.equal(Buffer.from(payload.networkSecretDigrest)[0], 7);
            assert.equal(Buffer.from(payload.networkSecretDigrest).length, 32);
            return encoded;
          },
        };
      },
    },
  };
  const sent = [];
  const ws = {
    readyState: 1,
    send(buf) { sent.push(Buffer.from(buf)); },
    close() { this.readyState = 3; },
  };
  const pm = new PeerManager();
  let pushed = 0;
  pm.pushRouteUpdateTo = () => { pushed += 1; };
  pm.broadcastRouteUpdate = () => { pushed += 1; };
  handleHandshake(ws, { fromPeerId: 111, toPeerId: MY_PEER_ID }, Buffer.from('x'), types, pm);
  assert.ok(sent.length >= 1);
  const header = parseHeader(sent[0]);
  assert.equal(header.fromPeerId, MY_PEER_ID);
  assert.equal(header.toPeerId, 111);
  assert.ok(pushed >= 1);
});

test('forwarded PeerCenter ReportPeers is ingested without answering as the center', () => {
  const types = {
    RpcPacket: {
      decode() {
        return {
          descriptor: { protoName: 'peer_rpc', serviceName: 'PeerCenterRpc', methodIndex: 1 },
          body: Buffer.from('body'),
        };
      },
    },
    RpcRequest: {
      decode() { throw new Error('no wrapper'); },
    },
    ReportPeersRequest: {
      decode() {
        return {
          myPeerId: 111,
          peerInfos: { directPeers: { 222: { latencyMs: 10 } } },
        };
      },
    },
  };
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  const ws = { peerId: 111, groupKey: 'g', readyState: 1, send() { throw new Error('should not answer'); }, close() {} };
  const ok = sniffPeerCenterReport(ws, { fromPeerId: 111, toPeerId: 222 }, Buffer.from('rpc'), types, pm);
  assert.equal(ok, true);
  confirm(pm);
  const edges = pm.collectReportedEdges('g');
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
  assert.ok(Object.keys(pm.getStats().lastPeerCenter).includes('111'));
});

test('PeerCenter sniff matches protoName PeerCenterRpc', () => {
  const types = {
    RpcPacket: {
      decode() {
        return {
          descriptor: { protoName: 'PeerCenterRpc', serviceName: 'PeerCenterRpc', methodIndex: 1 },
          body: Buffer.from('body'),
        };
      },
    },
    RpcRequest: {
      decode() { throw new Error('no wrapper'); },
    },
    ReportPeersRequest: {
      decode() {
        return {
          myPeerId: 111,
          peerInfos: { directPeers: { 222: { latencyMs: 4 } } },
        };
      },
    },
  };
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  const ok = sniffPeerCenterReport(
    { peerId: 111, groupKey: 'g', readyState: 1, send() {}, close() {} },
    { fromPeerId: 111, toPeerId: 222 },
    Buffer.from('rpc'),
    types,
    pm,
  );
  assert.equal(ok, true);
  confirm(pm);
  assert.ok(pm.collectReportedEdges('g').some(([a, b]) => a === 111 && b === 222));
});

test('PeerCenter sniff decompresses zstd ReportPeers bodies', () => {
  const plain = Buffer.from('plain-report');
  const types = {
    RpcPacket: {
      decode() {
        return {
          fromPeer: 111,
          transactionId: 9,
          totalPieces: 1,
          pieceIdx: 0,
          descriptor: { protoName: 'peer_rpc', serviceName: 'PeerCenterRpc', methodIndex: 1 },
          body: zlib.zstdCompressSync(plain),
          compressionInfo: { algo: 2, acceptedAlgo: 2 },
        };
      },
    },
    RpcRequest: {
      decode(body) {
        assert.equal(Buffer.from(body).toString(), 'plain-report');
        throw new Error('no wrapper');
      },
    },
    ReportPeersRequest: {
      decode(body) {
        assert.equal(Buffer.from(body).toString(), 'plain-report');
        return {
          myPeerId: 111,
          peerInfos: { directPeers: { 222: { latencyMs: 12 } } },
        };
      },
    },
  };
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  const ok = sniffPeerCenterReport(
    { peerId: 111, groupKey: 'g', readyState: 1, send() {}, close() {} },
    { fromPeerId: 111, toPeerId: 222 },
    Buffer.from('rpc'),
    types,
    pm,
  );
  assert.equal(ok, true);
  confirm(pm);
  assert.ok(pm.collectReportedEdges('g').some(([a, b]) => a === 111 && b === 222));
  assert.ok(Object.keys(pm.getStats().lastPeerCenter).includes('111'));
});

test('PeerCenter sniff ingests GetGlobalPeerMap responses', () => {
  const types = {
    RpcPacket: {
      decode() {
        return {
          descriptor: { protoName: 'peer_rpc', serviceName: 'PeerCenterRpc', methodIndex: 2 },
          body: Buffer.from('map'),
        };
      },
    },
    RpcResponse: {
      decode() { throw new Error('no wrapper'); },
    },
    GetGlobalPeerMapResponse: {
      decode() {
        return {
          globalPeerMap: {
            111: { directPeers: { 222: { latencyMs: 9 } } },
          },
        };
      },
    },
  };
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  const ok = sniffPeerCenterReport(
    { peerId: 111, groupKey: 'g', readyState: 1, send() {}, close() {} },
    { fromPeerId: 111, toPeerId: 333 },
    Buffer.from('rpc'),
    types,
    pm,
  );
  assert.equal(ok, true);
  confirm(pm);
  assert.ok(pm.collectReportedEdges('g').some(([a, b]) => a === 111 && b === 222));
});

test('requestGlobalPeerMapFromCenter queries only the smallest live peer', () => {
  const sent = [];
  const types = {
    GetGlobalPeerMapRequest: { encode() { return { finish() { return Buffer.from('req'); } }; } },
    RpcRequest: { encode() { return { finish() { return Buffer.from('wrap'); } }; } },
    RpcPacket: {
      encode(p) {
        return {
          finish() {
            const desc = p && p.descriptor;
            assert.equal(desc.protoName, 'PeerCenterRpc');
            assert.equal(desc.serviceName, 'PeerCenterRpc');
            assert.equal(desc.domainName, 'public_server');
            assert.equal(desc.methodIndex, 2);
            return Buffer.from('pkt');
          },
        };
      },
    },
  };
  const pm = new PeerManager();
  const wsP2 = { peerId: 111111111, groupKey: 'public_server:ab', domainName: 'public_server', readyState: 1, send(buf) { sent.push(Buffer.from(buf)); }, close() {} };
  const wsNd1 = { peerId: 155555555, groupKey: 'public_server:ab', domainName: 'public_server', readyState: 1, send(buf) { sent.push(Buffer.from(buf)); }, close() {} };
  pm.addPeer(155555555, wsNd1);
  pm.addPeer(111111111, wsP2);
  assert.equal(requestGlobalPeerMapFromCenter(pm, types), true);
  assert.equal(sent.length, 1);
  const header = parseHeader(sent[0]);
  assert.equal(header.packetType, PacketType.RpcReq);
  assert.equal(header.fromPeerId, MY_PEER_ID);
  assert.equal(header.toPeerId, 111111111);
  assert.equal(pm.getStats().peerCenterPulls, 1);
  assert.equal(pm.pickPeerCenterId('public_server:ab'), 111111111);
});

test('GetGlobalPeerMap from a non-center peer is ignored', () => {
  const types = {
    RpcPacket: {
      decode() {
        return {
          descriptor: { protoName: 'PeerCenterRpc', serviceName: 'PeerCenterRpc', methodIndex: 2 },
          body: Buffer.from('map'),
        };
      },
    },
    RpcResponse: {
      decode() { throw new Error('no wrapper'); },
    },
    GetGlobalPeerMapResponse: {
      decode() {
        return {
          globalPeerMap: {
            155555555: { directPeers: { 111111111: { latencyMs: 68 } } },
            111111111: { directPeers: { 155555555: { latencyMs: 68 } } },
          },
        };
      },
    },
  };
  const pm = new PeerManager();
  pm.addPeer(111111111, { peerId: 111111111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(155555555, { peerId: 155555555, groupKey: 'g', readyState: 1, close() {} });
  const ok = sniffPeerCenterReport(
    { peerId: 155555555, groupKey: 'g', readyState: 1, send() {}, close() {} },
    { fromPeerId: 155555555, toPeerId: 10000001 },
    Buffer.from('rpc'),
    types,
    pm,
  );
  assert.equal(ok, false);
  assert.deepEqual(pm.collectReportedEdges('g'), []);
});

test('handleRpcResp ingests GetGlobalPeerMap at 1-based method index 2', () => {
  const inner = Buffer.from('map-body');
  const types = {
    RpcPacket: {
      decode() {
        return {
          fromPeer: 111,
          transactionId: 7,
          totalPieces: 1,
          pieceIdx: 0,
          descriptor: { protoName: 'PeerCenterRpc', serviceName: 'PeerCenterRpc', methodIndex: 2 },
          body: inner,
          compressionInfo: { algo: 1, acceptedAlgo: 2 },
        };
      },
    },
    RpcResponse: {
      decode() {
        return { response: inner, error: null };
      },
    },
    GetGlobalPeerMapResponse: {
      decode() {
        return {
          globalPeerMap: {
            111: { directPeers: { 222: { latencyMs: 5 }, 333: { latencyMs: 8 } } },
            222: { directPeers: { 111: { latencyMs: 5 } } },
          },
        };
      },
    },
  };
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  handleRpcResp(
    { peerId: 111, groupKey: 'g', readyState: 1, send() {}, close() {} },
    { fromPeerId: 111, toPeerId: MY_PEER_ID },
    Buffer.from('rpc'),
    types,
    pm,
  );
  const edges = pm.collectReportedEdges('g');
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
  assert.ok(Object.keys(pm.getStats().lastPeerCenter).includes('111'));
});
