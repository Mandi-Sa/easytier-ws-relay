import { HEADER_SIZE } from './constants.js';

export function bufferFromMessage(message) {
  if (message instanceof ArrayBuffer) {
    return Buffer.from(message);
  }
  if (ArrayBuffer.isView(message)) {
    return Buffer.from(message.buffer, message.byteOffset, message.byteLength);
  }
  if (Buffer.isBuffer(message)) {
    return message;
  }
  return null;
}

export function parseHeader(buffer) {
  if (!buffer || buffer.length < HEADER_SIZE) return null;
  return {
    fromPeerId: buffer.readUInt32LE(0),
    toPeerId: buffer.readUInt32LE(4),
    packetType: buffer.readUInt8(8),
    flags: buffer.readUInt8(9),
    forwardCounter: buffer.readUInt8(10),
    reserved: buffer.readUInt8(11),
    len: buffer.readUInt32LE(12),
  };
}

export function splitPackets(buffer) {
  const packets = [];
  let offset = 0;
  while (offset + HEADER_SIZE <= buffer.length) {
    const slice = buffer.subarray(offset);
    const header = parseHeader(slice);
    if (!header) break;
    const available = slice.length - HEADER_SIZE;
    const take = (Number.isFinite(header.len) && header.len >= 0 && header.len <= available)
      ? header.len
      : available;
    const raw = slice.subarray(0, HEADER_SIZE + take);
    const payload = raw.subarray(HEADER_SIZE);
    packets.push({ header, payload, raw });
    offset += raw.length;
    if (take === available && header.len > available) break;
  }
  return packets;
}

export function createHeader(fromPeerId, toPeerId, packetType, payloadLen) {
  const buffer = Buffer.alloc(HEADER_SIZE);
  buffer.writeUInt32LE(fromPeerId, 0);
  buffer.writeUInt32LE(toPeerId, 4);
  buffer.writeUInt8(packetType, 8);
  buffer.writeUInt8(0, 9);
  buffer.writeUInt8(1, 10);
  buffer.writeUInt8(0, 11);
  buffer.writeUInt32LE(payloadLen, 12);
  return buffer;
}
