export function bitmapIndex(n, row, col) {
  return row * n + col;
}

export function setBitmapBit(bitmap, n, row, col) {
  const idx = bitmapIndex(n, row, col);
  bitmap[Math.floor(idx / 8)] |= (1 << (idx % 8));
}

export function getBitmapBit(bitmap, n, row, col) {
  const idx = bitmapIndex(n, row, col);
  const byte = bitmap[Math.floor(idx / 8)] || 0;
  return ((byte >> (idx % 8)) & 1) === 1;
}

export function parseConnBitmapEdges(connBitmap, reporterPeerId) {
  if (!connBitmap || !connBitmap.peerIds || !connBitmap.bitmap) return [];
  const ids = connBitmap.peerIds.map((p) => Number(p.peerId));
  const n = ids.length;
  if (n === 0) return [];
  const buf = Buffer.isBuffer(connBitmap.bitmap)
    ? connBitmap.bitmap
    : Buffer.from(connBitmap.bitmap);
  const reporter = Number(reporterPeerId);
  const edges = [];
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < n; j++) {
      if (i === j) continue;
      if (!getBitmapBit(buf, n, i, j)) continue;
      if (ids[i] !== reporter && ids[j] !== reporter) continue;
      edges.push([ids[i], ids[j]]);
    }
  }
  return edges;
}

function readVarint(buf, pos) {
  let x = 0;
  let shift = 0;
  while (pos < buf.length) {
    const b = buf[pos++];
    x |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) return { value: x >>> 0, pos };
    shift += 7;
    if (shift > 35) break;
  }
  return null;
}

function skipField(buf, pos, wireType) {
  if (wireType === 0) {
    const v = readVarint(buf, pos);
    return v ? v.pos : buf.length;
  }
  if (wireType === 1) return pos + 8;
  if (wireType === 5) return pos + 4;
  if (wireType === 2) {
    const v = readVarint(buf, pos);
    if (!v) return buf.length;
    return v.pos + v.value;
  }
  return buf.length;
}

function parsePeerConnInfo(buf) {
  let pos = 0;
  let peerId = null;
  const connected = [];
  while (pos < buf.length) {
    const tag = readVarint(buf, pos);
    if (!tag) break;
    pos = tag.pos;
    const field = tag.value >>> 3;
    const wire = tag.value & 7;
    if (field === 1 && wire === 2) {
      const len = readVarint(buf, pos);
      if (!len) break;
      pos = len.pos;
      const inner = buf.subarray(pos, pos + len.value);
      pos += len.value;
      let ip = 0;
      while (ip < inner.length) {
        const t = readVarint(inner, ip);
        if (!t) break;
        ip = t.pos;
        const f = t.value >>> 3;
        const w = t.value & 7;
        if (f === 1 && w === 0) {
          const v = readVarint(inner, ip);
          if (!v) break;
          peerId = v.value;
          ip = v.pos;
        } else {
          ip = skipField(inner, ip, w);
        }
      }
    } else if (field === 2 && wire === 0) {
      const v = readVarint(buf, pos);
      if (!v) break;
      connected.push(v.value);
      pos = v.pos;
    } else if (field === 2 && wire === 2) {
      const len = readVarint(buf, pos);
      if (!len) break;
      pos = len.pos;
      const packed = buf.subarray(pos, pos + len.value);
      pos += len.value;
      let p = 0;
      while (p < packed.length) {
        const v = readVarint(packed, p);
        if (!v) break;
        connected.push(v.value);
        p = v.pos;
      }
    } else {
      pos = skipField(buf, pos, wire);
    }
  }
  return peerId == null ? null : { peerId, connected };
}

export function parseConnPeerList(rawBytes) {
  if (!rawBytes || !rawBytes.length) return [];
  const buf = Buffer.isBuffer(rawBytes) ? rawBytes : Buffer.from(rawBytes);
  let pos = 0;
  const infos = [];
  while (pos < buf.length) {
    const tag = readVarint(buf, pos);
    if (!tag) break;
    pos = tag.pos;
    const field = tag.value >>> 3;
    const wire = tag.value & 7;
    if (field === 7 && wire === 2) {
      const len = readVarint(buf, pos);
      if (!len) break;
      pos = len.pos;
      const list = buf.subarray(pos, pos + len.value);
      pos += len.value;
      let lp = 0;
      while (lp < list.length) {
        const t = readVarint(list, lp);
        if (!t) break;
        lp = t.pos;
        const f = t.value >>> 3;
        const w = t.value & 7;
        if (f === 1 && w === 2) {
          const ilen = readVarint(list, lp);
          if (!ilen) break;
          lp = ilen.pos;
          const info = parsePeerConnInfo(list.subarray(lp, lp + ilen.value));
          lp += ilen.value;
          if (info) infos.push(info);
        } else {
          lp = skipField(list, lp, w);
        }
      }
    } else {
      pos = skipField(buf, pos, wire);
    }
  }
  return infos;
}

export function edgesFromConnPeerList(infos, reporterPeerId) {
  const reporter = Number(reporterPeerId);
  const edges = [];
  for (const info of infos || []) {
    if (Number(info.peerId) !== reporter) continue;
    for (const dst of info.connected || []) {
      if (Number(dst) === reporter) continue;
      edges.push([reporter, Number(dst)]);
    }
  }
  return edges;
}

export function buildStarAndReportedBitmap(peerIds, reportedEdges, relayPeerId) {
  const n = peerIds.length;
  const bitmap = new Uint8Array(Math.ceil((n * n) / 8) || 1);
  const idx = new Map();
  for (let i = 0; i < n; i++) idx.set(Number(peerIds[i]), i);
  for (let i = 0; i < n; i++) setBitmapBit(bitmap, n, i, i);
  const serverIdx = idx.get(Number(relayPeerId));
  if (serverIdx !== undefined) {
    for (let i = 0; i < n; i++) {
      if (i === serverIdx) continue;
      setBitmapBit(bitmap, n, serverIdx, i);
      setBitmapBit(bitmap, n, i, serverIdx);
    }
  }
  for (const [a, b] of reportedEdges || []) {
    const i = idx.get(Number(a));
    const j = idx.get(Number(b));
    if (i === undefined || j === undefined || i === j) continue;
    setBitmapBit(bitmap, n, i, j);
    setBitmapBit(bitmap, n, j, i);
  }
  return Buffer.from(bitmap);
}
