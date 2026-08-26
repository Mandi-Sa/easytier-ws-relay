import { test } from 'node:test';
import assert from 'node:assert/strict';
import zlib from 'zlib';
import { readFileSync } from 'node:fs';
import { PeerManager } from '../src/worker/core/peer_manager.js';
import { parseHeader, createHeader, bufferFromMessage, splitPackets, parseForeignNetworkPayload, buildForeignNetworkPayload } from '../src/worker/core/packet.js';
import { MY_PEER_ID, HEADER_SIZE, PacketType } from '../src/worker/core/constants.js';
import { handleForwarding } from '../src/worker/core/basic_handlers.js';
import { RpcPieceMerger } from '../src/worker/core/rpc_pieces.js';
import { decompressRpcBody } from '../src/worker/core/compress.js';
import { RPC_COMPRESSION_NONE, RPC_COMPRESSION_ZSTD } from '../src/worker/core/rpc_compress.js';
import {
  connectionLimit,
  shouldAcceptConnection,
  debugEnabled,
} from '../src/worker/core/env.js';

test('relay_room imports parseHeader for handshake dispatch', () => {
  const src = readFileSync(new URL('../src/worker/relay_room.js', import.meta.url), 'utf8');
  assert.match(src, /import \{[^}]*parseHeader[^}]*\} from '\.\/core\/packet\.js'/);
  assert.match(src, /parseHeader\(buffer\)/);
});

function ws(peerId, groupKey = 'g') {
  return { peerId, groupKey, readyState: 1, close() { this.readyState = 3; } };
}

function inst(a = 1, b = 2, c = 3, d = 4) {
  return { part1: a, part2: b, part3: c, part4: d };
}

class MemoryStorage {
  constructor(init = {}) {
    this.m = new Map(Object.entries(init));
  }
  async get(key) {
    if (Array.isArray(key)) {
      const out = {};
      for (const k of key) out[k] = this.m.get(k);
      return out;
    }
    return this.m.get(key);
  }
  async put(key, value) {
    if (key && typeof key === 'object' && value === undefined) {
      for (const [k, v] of Object.entries(key)) this.m.set(k, v);
      return;
    }
    this.m.set(key, value);
  }
}

test('same inst_id replaces the previous peer_id', () => {
  const pm = new PeerManager();
  const id = inst();
  const oldWs = ws(144444444);
  const newWs = ws(233333333);
  pm.addPeer(144444444, oldWs);
  pm.updatePeerInfo('g', 144444444, { peerId: 144444444, version: 1, instId: id, hostname: 'node-b' });
  pm.addPeer(233333333, newWs);
  pm.updatePeerInfo('g', 233333333, { peerId: 233333333, version: 2, instId: id, hostname: 'node-b' });
  assert.equal(pm.getPeerWs(144444444, 'g'), undefined);
  assert.equal(pm.getPeerWs(233333333, 'g'), newWs);
  assert.deepEqual(pm.getAdvertisedPeerIds('g'), [MY_PEER_ID, 233333333]);
  assert.equal(oldWs.readyState, 3);
});

test('live socket without peer info is advertised but not stubbed', () => {
  const pm = new PeerManager();
  pm.addPeer(111, ws(111));
  assert.deepEqual(pm.getAdvertisedPeerIds('g'), [MY_PEER_ID, 111]);
  const infos = pm.collectPeerInfosForRoute('g');
  assert.equal(infos.some((i) => i.peerId === 111), false);
  assert.equal(infos.some((i) => i.peerId === MY_PEER_ID), true);
});

test('peer managers do not share live sockets', () => {
  const a = new PeerManager();
  const b = new PeerManager();
  a.addPeer(1, ws(1));
  assert.equal(b.listPeerIdsInGroup('g').length, 0);
  assert.equal(a.listPeerIdsInGroup('g').length, 1);
});

test('relay identity survives storage roundtrip', async () => {
  const storage = new MemoryStorage();
  const pm1 = new PeerManager();
  await pm1.hydrateIdentity(storage);
  const first = pm1.ensureMyInfo();
  const pm2 = new PeerManager();
  await pm2.hydrateIdentity(storage);
  const second = pm2.ensureMyInfo();
  assert.deepEqual(second.instId, first.instId);
  assert.equal(String(second.peerRouteId), String(first.peerRouteId));
});

test('peer center map only includes live sockets', () => {
  const pm = new PeerManager();
  pm.addPeer(111, ws(111));
  pm.updatePeerInfo('g', 111, { peerId: 111, version: 1 });
  pm.updatePeerInfo('g', 222, { peerId: 222, version: 1 });
  const map = pm.buildPeerCenterResponseMap('g');
  assert.equal(Object.prototype.hasOwnProperty.call(map, '111'), true);
  assert.equal(Object.prototype.hasOwnProperty.call(map, '222'), false);
  assert.equal(Object.prototype.hasOwnProperty.call(map, String(MY_PEER_ID)), false);
});

test('parseHeader accepts extra trailing bytes used by data frames', () => {
  const payload = Buffer.from('abcd');
  const header = createHeader(1, 2, 8, payload.length);
  const full = Buffer.concat([header, payload]);
  assert.equal(parseHeader(full).len, payload.length);
  assert.equal(parseHeader(full.subarray(0, HEADER_SIZE - 1)), null);
  const withTail = Buffer.concat([full, Buffer.from([0, 1, 2])]);
  assert.equal(parseHeader(withTail).len, payload.length);
  const packets = splitPackets(withTail);
  assert.equal(packets.length, 1);
  assert.equal(packets[0].payload.toString(), 'abcd');
});

test('splitPackets forwards coalesced data frames separately', () => {
  const a = Buffer.concat([createHeader(1, 2, 1, 2), Buffer.from('aa')]);
  const b = Buffer.concat([createHeader(1, 3, 1, 2), Buffer.from('bb')]);
  const packets = splitPackets(Buffer.concat([a, b]));
  assert.equal(packets.length, 2);
  assert.equal(packets[0].header.toPeerId, 2);
  assert.equal(packets[0].payload.toString(), 'aa');
  assert.equal(packets[1].header.toPeerId, 3);
  assert.equal(packets[1].payload.toString(), 'bb');
});

test('bufferFromMessage honors TypedArray byteOffset', () => {
  const raw = new Uint8Array([0, 1, 2, 3, 4, 5]);
  const view = raw.subarray(2, 5);
  assert.deepEqual(Array.from(bufferFromMessage(view)), [2, 3, 4]);
});

test('rpc piece merger concatenates in order', () => {
  const merger = new RpcPieceMerger();
  assert.equal(merger.add({ fromPeer: 1, transactionId: 9, pieceIdx: 0, totalPieces: 2, body: Buffer.from('aa') }), null);
  const out = merger.add({ fromPeer: 1, transactionId: 9, pieceIdx: 1, totalPieces: 2, body: Buffer.from('bb') });
  assert.equal(out.toString(), 'aabb');
});

test('inbound rpc decompression uses zstd for algo 2', () => {
  const raw = Buffer.from('easytier-rpc-body');
  const zstd = zlib.zstdCompressSync(raw);
  assert.equal(decompressRpcBody(raw, RPC_COMPRESSION_NONE).toString(), 'easytier-rpc-body');
  assert.equal(decompressRpcBody(zstd, RPC_COMPRESSION_ZSTD).toString(), 'easytier-rpc-body');
  assert.throws(() => decompressRpcBody(zstd, RPC_COMPRESSION_ZSTD + 1));
});

test('connection limit rejects only overflow', () => {
  assert.equal(connectionLimit({}), 256);
  assert.equal(shouldAcceptConnection(255, 256), true);
  assert.equal(shouldAcceptConnection(256, 256), false);
});

test('debug logging is off by default', () => {
  const prev = process.env.EASYTIER_DEBUG;
  delete process.env.EASYTIER_DEBUG;
  assert.equal(debugEnabled(), false);
  process.env.EASYTIER_DEBUG = '1';
  assert.equal(debugEnabled(), true);
  if (prev === undefined) delete process.env.EASYTIER_DEBUG;
  else process.env.EASYTIER_DEBUG = prev;
});

test('foreign network payload roundtrip exposes dst peer and inner packet', () => {
  const inner = Buffer.concat([createHeader(111, 222, PacketType.Data, 4), Buffer.from('ping')]);
  const payload = buildForeignNetworkPayload(222, 'public_server', inner);
  const parsed = parseForeignNetworkPayload(payload);
  assert.equal(parsed.dstPeerId, 222);
  assert.equal(parsed.networkName, 'public_server');
  assert.equal(Buffer.compare(parsed.inner, inner), 0);
});

test('foreign network packet to the relay is unwrapped and forwarded', () => {
  const pm = new PeerManager();
  const sent = [];
  const dst = {
    peerId: 222,
    groupKey: 'g',
    readyState: 1,
    send(buf) { sent.push(Buffer.from(buf)); },
    close() { this.readyState = 3; },
  };
  const src = { peerId: 111, groupKey: 'g', readyState: 1, close() { this.readyState = 3; } };
  pm.addPeer(222, dst);
  const inner = Buffer.concat([createHeader(111, 222, PacketType.Data, 4), Buffer.from('ping')]);
  const payload = buildForeignNetworkPayload(222, 'public_server', inner);
  const outer = Buffer.concat([createHeader(111, MY_PEER_ID, PacketType.ForeignNetworkPacket, payload.length), payload]);
  const header = parseHeader(outer);
  const ok = handleForwarding(src, header, outer, null, pm);
  assert.equal(ok, true);
  assert.equal(sent.length, 1);
  assert.equal(Buffer.compare(sent[0], inner), 0);
  assert.equal(pm.getStats().forwardOk, 1);
});
