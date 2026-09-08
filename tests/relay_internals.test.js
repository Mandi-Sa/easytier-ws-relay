import { test } from 'node:test';
import assert from 'node:assert/strict';
import zlib from 'zlib';
import { readFileSync } from 'node:fs';
import { PeerManager } from '../src/worker/core/peer_manager.js';
import { parseHeader, createHeader, bufferFromMessage, splitPackets, parseForeignNetworkPayload, buildForeignNetworkPayload, wrapAsForeignNetwork } from '../src/worker/core/packet.js';
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
import { buildStarAndReportedBitmap } from '../src/worker/core/topology.js';

test('relay_room imports parseHeader for handshake dispatch', () => {
  const src = readFileSync(new URL('../src/worker/relay_room.js', import.meta.url), 'utf8');
  assert.match(src, /import \{[^}]*parseHeader[^}]*\} from '\.\/core\/packet\.js'/);
  assert.match(src, /parseHeader\(buffer\)/);
});

function ws(peerId, groupKey = 'g') {
  return { peerId, groupKey, readyState: 1, close() { this.readyState = 3; } };
}

function confirm(pm, pairs) {
  for (const [a, b] of pairs) {
    pm.ingestPeerCenterEdges('g', a, [[a, b]]);
    pm.ingestPeerCenterEdges('g', b, [[b, a]]);
  }
}

function inst(a = 1, b = 2, c = 3, d = 4) {
  return { part1: a, part2: b, part3: c, part4: d };
}

function varint(value) {
  let n = Number(value) >>> 0;
  const out = [];
  do {
    let byte = n & 0x7f;
    n >>>= 7;
    if (n) byte |= 0x80;
    out.push(byte);
  } while (n);
  return Buffer.from(out);
}

function lengthDelimited(fieldNumber, payload) {
  return Buffer.concat([varint((fieldNumber << 3) | 2), varint(payload.length), payload]);
}

function encodedConnPeerList(rows) {
  const entries = rows.map(({ peerId, version = 1, connected = [] }) => {
    const peerIdVersion = Buffer.concat([
      varint(0x08), varint(peerId),
      varint(0x10), varint(version),
    ]);
    const fields = [lengthDelimited(1, peerIdVersion)];
    if (connected.length > 0) {
      fields.push(lengthDelimited(2, Buffer.concat(connected.map(varint))));
    }
    return lengthDelimited(1, Buffer.concat(fields));
  });
  return lengthDelimited(7, Buffer.concat(entries));
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
  assert.equal(merger.add({ fromPeer: 1, transactionId: 9, pieceIdx: 0, totalPieces: 2, body: Buffer.from('aa'), descriptor: { methodIndex: 2 } }), null);
  const out = merger.add({ fromPeer: 1, transactionId: 9, pieceIdx: 1, totalPieces: 2, body: Buffer.from('bb') });
  assert.equal(out.body.toString(), 'aabb');
  assert.equal(out.descriptor.methodIndex, 2);
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
  const forwarded = parseHeader(sent[0]);
  assert.equal(forwarded.packetType, PacketType.ForeignNetworkPacket);
  assert.equal(forwarded.toPeerId, 222);
  assert.equal(forwarded.fromPeerId, MY_PEER_ID);
  const wrapped = parseForeignNetworkPayload(sent[0].subarray(16));
  assert.equal(wrapped.dstPeerId, 222);
  assert.equal(Buffer.compare(wrapped.inner, inner), 0);
  assert.equal(pm.getStats().forwardOk, 1);
});

test('inst_id replacement notifies topology listeners', () => {
  const pm = new PeerManager();
  const events = [];
  pm.onTopologyChange = (gk) => events.push(gk);
  const id = { part1: 1, part2: 2, part3: 3, part4: 4 };
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() { this.readyState = 3; } });
  pm.updatePeerInfo('g', 111, { peerId: 111, version: 1, instId: id });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() { this.readyState = 3; } });
  pm.updatePeerInfo('g', 222, { peerId: 222, version: 2, instId: id });
  assert.equal(pm.getPeerWs(111, 'g'), undefined);
  assert.ok(events.includes('g'));
});

test('conn bitmap versions jump forward after a restart', () => {
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  const v = pm.bumpPeerConnVersion('g', 111);
  assert.ok(v >= Math.floor(Date.now() / 1000) - 1);
});

test('reported direct edges are merged into the star bitmap', () => {
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  pm.ingestReportedEdges('g', 111, [[111, 222]]);
  confirm(pm, [[111, 222]]);
  const edges = pm.collectReportedEdges('g');
  assert.deepEqual(edges, [[111, 222]]);
});

test('ospf star bitmap does not wipe peer-center p2p edges', () => {
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  pm.ingestPeerCenterEdges('g', 111, [[111, 222]]);
  confirm(pm, [[111, 222]]);
  pm.ingestConnInfo('g', 111, {
    connBitmap: {
      peerIds: [{ peerId: MY_PEER_ID }, { peerId: 111 }, { peerId: 222 }],
      bitmap: Buffer.from([0xff]),
    },
  }, null);
  const edges = pm.collectReportedEdges('g');
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
});

test('client conn bitmap is not treated as a p2p mesh', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222, 333]) pm.addPeer(peerId, ws(peerId));
  pm.ingestConnInfo('g', 111, {
    connBitmap: {
      peerIds: [
        { peerId: MY_PEER_ID, version: 1 },
        { peerId: 111, version: 1 },
        { peerId: 222, version: 1 },
        { peerId: 333, version: 1 },
      ],
      bitmap: Buffer.from([0xff, 0xff]),
    },
  }, null);
  const edges = pm.collectReportedEdges('g');
  assert.equal(edges.some(([a, b]) => a === 222 && b === 333), false);
});

test('star conn bitmap does not invent p2p edges', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222]) pm.addPeer(peerId, ws(peerId));
  const ids = [MY_PEER_ID, 111, 222];
  const bitmap = buildStarAndReportedBitmap(ids, [], MY_PEER_ID);
  pm.ingestConnInfo('g', 111, {
    connBitmap: {
      peerIds: ids.map((peerId) => ({ peerId, version: 1 })),
      bitmap,
    },
  }, null);
  assert.deepEqual(pm.collectReportedEdges('g'), []);
});

test('ospf snapshot replacement removes stale edges without removing shared evidence', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222, 333]) {
    pm.addPeer(peerId, { peerId, groupKey: 'g', readyState: 1, close() {} });
  }
  pm.ingestOspfEdges('g', 111, [[111, 222], [111, 333]]);
  pm.ingestOspfEdges('g', 222, [[222, 111], [222, 333]]);
  pm.ingestOspfEdges('g', 333, [[333, 111], [333, 222]]);
  pm.ingestOspfEdges('g', 111, [[111, 222]]);
  const edges = pm.collectReportedEdges('g');
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
  assert.ok(edges.some(([a, b]) => a === 222 && b === 333));
  assert.equal(edges.some(([a, b]) => a === 111 && b === 333), false);
});

test('older OSPF connection versions cannot restore a newer snapshot', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222, 333]) {
    pm.addPeer(peerId, { peerId, groupKey: 'g', readyState: 1, close() {} });
  }
  pm.ingestOspfEdges('g', 111, [[111, 222]], 10);
  pm.ingestOspfEdges('g', 222, [[222, 111]], 10);
  pm.ingestOspfEdges('g', 111, [[111, 333]], 9);
  assert.deepEqual(pm.collectReportedEdges('g'), [[111, 222]]);
  pm.ingestOspfEdges('g', 111, [[111, 333]], 11);
  pm.ingestOspfEdges('g', 333, [[333, 111]], 11);
  assert.deepEqual(pm.collectReportedEdges('g'), [[111, 333]]);
});

test('conn peer list replaces rows and accepts an empty withdrawal row', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222, 333]) {
    pm.addPeer(peerId, { peerId, groupKey: 'g', readyState: 1, close() {} });
  }
  pm.ingestConnInfo('g', 111, {}, encodedConnPeerList([
    { peerId: 111, version: 10, connected: [222, 333] },
  ]));
  pm.ingestOspfEdges('g', 222, [[222, 111]]);
  pm.ingestOspfEdges('g', 333, [[333, 111]]);
  assert.deepEqual(pm.collectReportedEdges('g'), [[111, 222], [111, 333]]);
  pm.ingestConnInfo('g', 111, {}, encodedConnPeerList([
    { peerId: 111, version: 11, connected: [] },
  ]));
  assert.deepEqual(pm.collectReportedEdges('g'), []);
});

test('empty conn peer list does not fall back to a mesh bitmap', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222, 333]) pm.addPeer(peerId, ws(peerId));
  pm.ingestConnInfo('g', 111, {}, encodedConnPeerList([
    { peerId: 111, version: 10, connected: [222] },
  ]));
  pm.ingestOspfEdges('g', 222, [[222, 111]]);
  pm.ingestConnInfo('g', 111, {
    connBitmap: {
      peerIds: [
        { peerId: 111, version: 1 },
        { peerId: 222, version: 1 },
        { peerId: 333, version: 1 },
      ],
      bitmap: Buffer.from([0xff, 0xff]),
    },
  }, encodedConnPeerList([]));
  assert.deepEqual(pm.collectReportedEdges('g'), [[111, 222]]);
});

test('sync without a connection row does not clear the previous OSPF row', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222]) {
    pm.addPeer(peerId, { peerId, groupKey: 'g', readyState: 1, close() {} });
  }
  pm.ingestConnInfo('g', 111, {}, encodedConnPeerList([
    { peerId: 111, version: 1, connected: [222] },
  ]));
  pm.ingestOspfEdges('g', 222, [[222, 111]]);
  pm.ingestConnInfo('g', 111, { peerInfos: { items: [] } }, encodedConnPeerList([
    { peerId: 222, version: 2, connected: [] },
  ]));
  assert.deepEqual(pm.collectReportedEdges('g'), [[111, 222]]);
});

test('empty peer-center report does not clear existing p2p edges', () => {
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  pm.ingestPeerCenterEdges('g', 111, [[111, 222]]);
  confirm(pm, [[111, 222]]);
  pm.ingestPeerCenterEdges('g', 111, []);
  const edges = pm.collectReportedEdges('g');
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
});

test('non-empty peer-center report replaces the previous snapshot', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222, 333]) pm.addPeer(peerId, ws(peerId));
  pm.ingestPeerCenterEdges('g', 111, [[111, 222], [111, 333]]);
  confirm(pm, [[111, 222], [111, 333]]);
  pm.ingestPeerCenterEdges('g', 111, [[111, 222]]);
  const edges = pm.collectReportedEdges('g');
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
  assert.equal(edges.some(([a, b]) => a === 111 && b === 333), false);
});

test('stale full-mesh OSPF snapshot is replaced by the current reporter list', () => {
  const pm = new PeerManager();
  const phone = 222222222;
  const nc = 122222222;
  const p1 = 188888888;
  const nd1 = 199999999;
  const c15 = 166666666;
  for (const peerId of [phone, nc, p1, nd1, c15]) pm.addPeer(peerId, ws(peerId));
  pm.ingestOspfEdges('g', phone, [[phone, nc], [phone, p1], [phone, nd1], [phone, c15]]);
  pm.ingestOspfEdges('g', nc, [[nc, phone]]);
  pm.ingestOspfEdges('g', p1, [[p1, phone]]);
  pm.ingestOspfEdges('g', nd1, [[nd1, phone]]);
  pm.ingestOspfEdges('g', c15, [[c15, phone]]);
  pm.ingestOspfEdges('g', phone, [[phone, nc], [phone, p1]]);
  const edges = pm.collectReportedEdges('g');
  const has = (a, b) => edges.some(([x, y]) => (x === a && y === b) || (x === b && y === a));
  assert.equal(has(phone, nc), true);
  assert.equal(has(phone, p1), true);
  assert.equal(has(phone, nd1), false);
  assert.equal(has(phone, c15), false);
});

test('disconnect removes the peer from every topology row and persists the cleanup', async () => {
  const store = new Map();
  const pm = new PeerManager();
  pm.storage = {
    async put(key, value) { store.set(key, value); },
    async get(key) { return store.get(key); },
  };
  const dead = ws(222);
  pm.addPeer(111, ws(111));
  pm.addPeer(222, dead);
  pm.addPeer(333, ws(333));
  pm.ingestOspfEdges('g', 111, [[111, 222]], 10);
  pm.ingestOspfEdges('g', 333, [[333, 222]], 10);
  assert.equal(pm.removePeer(dead), true);
  assert.deepEqual(pm.collectReportedEdges('g'), []);
  assert.equal(store.get('topology').reported.g, undefined);
});

test('expired topology snapshots of disconnected reporters are not broadcast', () => {
  const pm = new PeerManager();
  pm.topologyTtlMs = 1000;
  pm.ingestOspfEdges('g', 111, [[111, 222]], 10, 1000);
  assert.deepEqual(pm.collectReportedEdges('g', 2001), []);
});

test('live reporter snapshots survive ttl', () => {
  const pm = new PeerManager();
  pm.topologyTtlMs = 1000;
  for (const peerId of [111, 222]) pm.addPeer(peerId, ws(peerId));
  pm.ingestOspfEdges('g', 111, [[111, 222]], 10, 1000);
  pm.ingestOspfEdges('g', 222, [[222, 111]], 10, 1000);
  assert.deepEqual(pm.collectReportedEdges('g', 2001), [[111, 222]]);
});

test('star-only conn peer list does not wipe p2p edges', () => {
  const pm = new PeerManager();
  for (const peerId of [111, 222]) pm.addPeer(peerId, ws(peerId));
  pm.ingestConnInfo('g', 111, {}, encodedConnPeerList([
    { peerId: 111, version: 10, connected: [222] },
  ]));
  pm.ingestOspfEdges('g', 222, [[222, 111]]);
  pm.ingestConnInfo('g', 111, {}, encodedConnPeerList([
    { peerId: 111, version: 11, connected: [MY_PEER_ID] },
  ]));
  assert.deepEqual(pm.collectReportedEdges('g'), [[111, 222]]);
});

test('topology edges survive storage roundtrip', async () => {
  const store = new Map();
  const storage = {
    async get(k) { return store.get(k); },
    async put(k, v) { store.set(k, v); },
  };
  const pm = new PeerManager();
  pm.storage = storage;
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  pm.ingestOspfEdges('g', 111, [[111, 222]]);
  pm.ingestOspfEdges('g', 222, [[222, 111]]);
  await pm.persistTopology();
  const pm2 = new PeerManager();
  await pm2.hydrateIdentity(storage);
  pm2.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm2.addPeer(222, { peerId: 222, groupKey: 'g', readyState: 1, close() {} });
  const edges = pm2.collectReportedEdges('g');
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
});

test('one-sided PeerCenter edge is not flooded as DIRECT', () => {
  const pm = new PeerManager();
  const nc = 133333333;
  const p2 = 111111111;
  const nd1 = 155555555;
  for (const peerId of [nc, p2, nd1]) pm.addPeer(peerId, ws(peerId));
  pm.ingestPeerCenterEdges('g', p2, [[p2, nc], [p2, nd1]]);
  pm.ingestPeerCenterEdges('g', nd1, [[nd1, p2]]);
  const edges = pm.collectReportedEdges('g');
  const has = (a, b) => edges.some(([x, y]) => (x === a && y === b) || (x === b && y === a));
  assert.equal(has(p2, nd1), true);
  assert.equal(has(p2, nc), false);
});

test('center map replace drops missing reporters and stale OSPF pairs', () => {
  const pm = new PeerManager();
  const nc = 133333333;
  const nd1 = 155555555;
  const p2 = 111111111;
  const p1 = 177777777;
  const c15 = 211111111;
  for (const peerId of [nc, nd1, p2, p1, c15]) pm.addPeer(peerId, ws(peerId));
  pm.ingestPeerCenterEdges('g', nd1, [[nd1, p2], [nd1, nc]]);
  pm.ingestPeerCenterEdges('g', p2, [[p2, nd1], [p2, nc], [p2, p1], [p2, c15]]);
  pm.ingestPeerCenterEdges('g', nc, [[nc, c15], [nc, nd1], [nc, p2], [nc, p1]]);
  pm.ingestPeerCenterEdges('g', c15, [[c15, nc], [c15, p1], [c15, p2], [c15, nd1]]);
  pm.ingestOspfEdges('g', nc, [[nc, c15]]);
  pm.ingestOspfEdges('g', c15, [[c15, nc]]);
  const has = (edges, a, b) => edges.some(([x, y]) => (x === a && y === b) || (x === b && y === a));
  assert.equal(has(pm.collectReportedEdges('g'), nd1, p2), true);
  assert.equal(has(pm.collectReportedEdges('g'), nc, c15), true);

  pm.replacePeerCenterMap('g', {
    [String(p2)]: { directPeers: { [String(nc)]: { latencyMs: 128 }, [String(p1)]: { latencyMs: 99 }, [String(c15)]: { latencyMs: 108 } } },
    [String(nc)]: { directPeers: { [String(p2)]: { latencyMs: 106 }, [String(nd1)]: { latencyMs: 22 }, [String(p1)]: { latencyMs: 135 } } },
    [String(p1)]: { directPeers: { [String(p2)]: { latencyMs: 99 }, [String(nc)]: { latencyMs: 135 }, [String(nd1)]: { latencyMs: 144 }, [String(c15)]: { latencyMs: 1 } } },
    [String(c15)]: { directPeers: { [String(p2)]: { latencyMs: 108 }, [String(nd1)]: { latencyMs: 180 }, [String(p1)]: { latencyMs: 1 } } },
  });
  const edges = pm.collectReportedEdges('g');
  assert.equal(has(edges, nd1, p2), false);
  assert.equal(has(edges, nc, c15), false);
  assert.equal(has(edges, nc, nd1), false);
  assert.equal(has(edges, p2, nc), true);
  assert.equal(has(edges, p2, p1), true);
  assert.equal(has(edges, p2, c15), true);
  assert.equal(has(edges, p1, nc), true);
  assert.equal(has(edges, p1, c15), true);
});

test('empty GetGlobalPeerMap does not wipe existing center snapshots', () => {
  const pm = new PeerManager();
  pm.addPeer(111, ws(111));
  pm.addPeer(222, ws(222));
  pm.ingestPeerCenterEdges('g', 111, [[111, 222]]);
  pm.ingestPeerCenterEdges('g', 222, [[222, 111]]);
  assert.equal(pm.replacePeerCenterMap('g', {}), false);
  assert.deepEqual(pm.collectReportedEdges('g'), [[111, 222]]);
});

test('legacy array topology dumps are not reloaded', async () => {
  const storage = {
    async get(key) {
      if (key === 'topology') {
        return { reported: { g: { 111: [222, 333] } }, peerCenter: { g: { 111: [222] } } };
      }
      return undefined;
    },
    async put() {},
  };
  const pm = new PeerManager();
  await pm.hydrateIdentity(storage);
  for (const peerId of [111, 222, 333]) pm.addPeer(peerId, ws(peerId));
  assert.deepEqual(pm.collectReportedEdges('g'), []);
});

test('duplicate peer_route_id from the same peer is detected', () => {
  const pm = new PeerManager();
  pm.addPeer(111, { peerId: 111, groupKey: 'g', readyState: 1, close() {} });
  pm.updatePeerInfo('g', 111, { peerId: 111, version: 1, peerRouteId: '1' });
  assert.equal(pm.isDuplicatePeerId('g', 111, { peerId: 111, peerRouteId: '2' }), true);
  assert.equal(pm.isDuplicatePeerId('g', 111, { peerId: 111, peerRouteId: '1' }), false);
});
