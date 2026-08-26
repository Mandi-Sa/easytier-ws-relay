export class RpcPieceMerger {
  constructor() {
    this.pending = new Map();
  }

  _key(fromPeer, transactionId) {
    return `${fromPeer}:${transactionId}`;
  }

  add({ fromPeer, transactionId, pieceIdx, totalPieces, body }) {
    const total = Number(totalPieces || 1);
    const idx = Number(pieceIdx || 0);
    const chunk = Buffer.isBuffer(body) ? body : Buffer.from(body || []);
    if (!Number.isFinite(total) || total <= 1) {
      return chunk;
    }
    if (!Number.isFinite(idx) || idx < 0 || idx >= total) {
      return null;
    }
    const key = this._key(fromPeer, transactionId);
    let slot = this.pending.get(key);
    if (!slot || slot.total !== total) {
      slot = { total, pieces: new Array(total), got: 0 };
      this.pending.set(key, slot);
    }
    if (!slot.pieces[idx]) {
      slot.pieces[idx] = chunk;
      slot.got += 1;
    }
    if (slot.got < total) return null;
    this.pending.delete(key);
    return Buffer.concat(slot.pieces);
  }
}
