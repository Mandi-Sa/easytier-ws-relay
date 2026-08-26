import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  setBitmapBit,
  getBitmapBit,
  parseConnBitmapEdges,
  buildStarAndReportedBitmap,
} from '../src/worker/core/topology.js';
import { MY_PEER_ID } from '../src/worker/core/constants.js';

test('bitmap roundtrip and reporter-only edges', () => {
  const n = 3;
  const ids = [MY_PEER_ID, 111, 222];
  const bitmap = new Uint8Array(Math.ceil((n * n) / 8));
  setBitmapBit(bitmap, n, 1, 2);
  setBitmapBit(bitmap, n, 2, 1);
  assert.equal(getBitmapBit(bitmap, n, 1, 2), true);
  const edges = parseConnBitmapEdges({
    peerIds: ids.map((peerId) => ({ peerId })),
    bitmap: Buffer.from(bitmap),
  }, 111);
  assert.ok(edges.some(([a, b]) => a === 111 && b === 222));
});

test('star bitmap includes relay plus reported p2p edge', () => {
  const ids = [MY_PEER_ID, 111, 222];
  const buf = buildStarAndReportedBitmap(ids, [[111, 222]], MY_PEER_ID);
  const n = 3;
  assert.equal(getBitmapBit(buf, n, 0, 1), true);
  assert.equal(getBitmapBit(buf, n, 1, 0), true);
  assert.equal(getBitmapBit(buf, n, 1, 2), true);
  assert.equal(getBitmapBit(buf, n, 2, 1), true);
});
