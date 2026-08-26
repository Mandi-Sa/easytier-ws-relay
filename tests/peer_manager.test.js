import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PeerManager } from '../src/worker/core/peer_manager.js';
import { MY_PEER_ID } from '../src/worker/core/constants.js';
import { getWsPath, getPublicServerNetworkName } from '../src/worker/core/env.js';
import { negotiateRpcCompression, RPC_COMPRESSION_NONE } from '../src/worker/core/rpc_compress.js';

function ws(peerId, groupKey = 'g') {
  return { peerId, groupKey, readyState: 1, close() { this.readyState = 3; } };
}

test('removePeer of an old socket does not drop the replacement connection', () => {
  const pm = new PeerManager();
  const oldWs = ws(111);
  const newWs = ws(111);
  pm.addPeer(111, oldWs);
  pm.addPeer(111, newWs);
  const removed = pm.removePeer(oldWs);
  assert.equal(removed, false);
  assert.equal(pm.getPeerWs(111, 'g'), newWs);
});

test('removePeer of the active socket forgets that peer', () => {
  const pm = new PeerManager();
  const sock = ws(111);
  pm.addPeer(111, sock);
  pm.updatePeerInfo('g', 111, { peerId: 111, version: 1 });
  assert.equal(pm.removePeer(sock), true);
  assert.equal(pm.getPeerWs(111, 'g'), undefined);
  assert.equal(pm.listPeerIdsInGroup('g').includes(111), false);
});

test('advertised peers are live sockets only, not stale infos', () => {
  const pm = new PeerManager();
  const live = ws(111);
  pm.addPeer(111, live);
  pm.updatePeerInfo('g', 111, { peerId: 111, version: 1, hostname: 'live' });
  pm.updatePeerInfo('g', 222, { peerId: 222, version: 1, hostname: 'ghost' });
  const advertised = pm.getAdvertisedPeerIds('g');
  assert.deepEqual(advertised, [MY_PEER_ID, 111]);
  assert.equal(pm.shouldAcceptPeerInfo('g', 222, 111), false);
  assert.equal(pm.shouldAcceptPeerInfo('g', 111, 111), true);
  assert.equal(pm.shouldAcceptPeerInfo('g', MY_PEER_ID, 111), false);
});

test('pruneStalePeerInfos drops infos without a live websocket', () => {
  const pm = new PeerManager();
  pm.addPeer(111, ws(111));
  pm.updatePeerInfo('g', 111, { peerId: 111, version: 1 });
  pm.updatePeerInfo('g', 222, { peerId: 222, version: 1 });
  pm.pruneStalePeerInfos('g');
  const infos = pm._getPeerInfosMap('g', false);
  assert.equal(infos.has(111), true);
  assert.equal(infos.has(222), false);
});

test('restored sockets are re-registered like a Durable Object wake', () => {
  const pm = new PeerManager();
  const restored = ws(233333333, 'public_server:abc');
  pm.addPeer(restored.peerId, restored);
  assert.equal(pm.getPeerWs(233333333, 'public_server:abc'), restored);
  assert.deepEqual(pm.getAdvertisedPeerIds('public_server:abc'), [MY_PEER_ID, 233333333]);
});

test('ws path and public network name share one default', () => {
  assert.equal(getWsPath({ WS_PATH: 'ws' }), '/ws');
  assert.equal(getWsPath({}), '/ws');
  assert.equal(getPublicServerNetworkName(), 'public_server');
});

test('rpc compression never claims zstd for gzip payloads', () => {
  const { body, compressionInfo } = negotiateRpcCompression(Buffer.from('hello'));
  assert.equal(body.toString(), 'hello');
  assert.equal(compressionInfo.algo, RPC_COMPRESSION_NONE);
  assert.equal(compressionInfo.acceptedAlgo, RPC_COMPRESSION_NONE);
});
