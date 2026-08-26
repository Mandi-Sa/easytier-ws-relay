import { test } from 'node:test';
import assert from 'node:assert/strict';
import { handshakeDigestBytes, digestBytesFromGroupKey } from '../src/worker/core/env.js';
import { PeerManager } from '../src/worker/core/peer_manager.js';
import { sniffPeerCenterReport } from '../src/worker/core/rpc_handler.js';
import { MAGIC, MY_PEER_ID } from '../src/worker/core/constants.js';
import { handleHandshake } from '../src/worker/core/basic_handlers.js';
import { parseHeader } from '../src/worker/core/packet.js';

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
          descriptor: { protoName: 'peer_rpc', serviceName: 'PeerCenterRpc', methodIndex: 0 },
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
  const edges = pm.collectReportedEdges('g');
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
  assert.ok(Object.keys(pm.getStats().lastPeerCenter).includes('111'));
});
