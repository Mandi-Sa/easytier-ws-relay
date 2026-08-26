import { Buffer } from 'buffer';
import { HEADER_SIZE, MY_PEER_ID, PacketType } from './constants.js';

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

// ForeignNetworkPacketHeader is packed LE:
// u16 header_len, u32 dst_peer_id, u16 name_offset, u16 name_len, then name bytes.
export function parseForeignNetworkPayload(payload) {
  if (!payload || payload.length < 10) return null;
  const headerLen = payload.readUInt16LE(0);
  const dstPeerId = payload.readUInt32LE(2);
  const nameOff = payload.readUInt16LE(6);
  const nameLen = payload.readUInt16LE(8);
  if (!Number.isFinite(headerLen) || headerLen < 10 || headerLen > payload.length) return null;
  if (nameOff + nameLen > payload.length) return null;
  return {
    headerLen,
    dstPeerId,
    networkName: payload.subarray(nameOff, nameOff + nameLen).toString('utf8'),
    inner: payload.subarray(headerLen),
  };
}

export function buildForeignNetworkPayload(dstPeerId, networkName, innerPacket) {
  const name = Buffer.from(String(networkName || ''), 'utf8');
  const headerLen = 10 + name.length;
  const payload = Buffer.alloc(headerLen + innerPacket.length);
  payload.writeUInt16LE(headerLen, 0);
  payload.writeUInt32LE(dstPeerId, 2);
  payload.writeUInt16LE(10, 6);
  payload.writeUInt16LE(name.length, 8);
  name.copy(payload, 10);
  innerPacket.copy(payload, headerLen);
  return payload;
}

export function wrapAsForeignNetwork(innerPacket, dstPeerId, networkName) {
  const inner = Buffer.isBuffer(innerPacket) ? innerPacket : Buffer.from(innerPacket);
  const payload = buildForeignNetworkPayload(dstPeerId, networkName, inner);
  const header = createHeader(MY_PEER_ID, dstPeerId, PacketType.ForeignNetworkPacket, payload.length);
  return Buffer.concat([header, payload]);
}
