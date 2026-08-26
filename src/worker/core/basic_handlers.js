import { Buffer } from 'buffer';
import { MAGIC, VERSION, MY_PEER_ID, PacketType } from './constants.js';
import { createHeader, parseForeignNetworkPayload, wrapAsForeignNetwork } from './packet.js';
import { wrapPacket, randomU64String, deriveKeys } from './crypto.js';
import { getPublicServerNetworkName, persistSocketMeta, debugLog, sendWs } from './env.js';

const WS_OPEN = (typeof WebSocket !== 'undefined' && WebSocket.OPEN) ? WebSocket.OPEN : 1;

export function handleHandshake(ws, header, payload, types, peerManager) {
  let sent = false;
  try {
    const req = types.HandshakeRequest.decode(payload);
    try {
      const dig = req.networkSecretDigrest ? Buffer.from(req.networkSecretDigrest) : Buffer.alloc(0);
      debugLog(`Handshake networkSecretDigest(hex)=${dig.toString('hex')}`);
    } catch (_) {
      // ignore
    }

    if (req.magic !== MAGIC) {
      console.error('Invalid magic');
      ws.close();
      return;
    }

    const clientNetworkName = req.networkName || '';
    const clientDigest = req.networkSecretDigrest ? Buffer.from(req.networkSecretDigrest) : Buffer.alloc(0);
    const digestHex = clientDigest.toString('hex');
    const groupKey = peerManager.registerNetwork(clientNetworkName, digestHex);
    if (!groupKey) {
      console.error(`Rejecting handshake from ${req.myPeerId}: digest mismatch for network "${clientNetworkName}"`);
      ws.close();
      return;
    }
    const serverNetworkName = getPublicServerNetworkName();
    const digest = new Uint8Array(32);

    ws.domainName = clientNetworkName;

    const respPayload = {
      magic: MAGIC,
      myPeerId: MY_PEER_ID,
      version: VERSION,
      features: ["node-server-v1"],
      networkName: serverNetworkName,
      networkSecretDigrest: digest
    };

    ws.groupKey = groupKey;
    ws.peerId = req.myPeerId;
    peerManager.addPeer(req.myPeerId, ws);
    peerManager.setPublicServerFlag(true);
    const features = req.features || [];
    const wantCrypto = features.some((f) => /aes|encrypt|gcm/i.test(String(f)));
    if (wantCrypto) {
      const keys = deriveKeys('');
      ws.crypto = { enabled: true, algorithm: 'aes-gcm', key128: keys.key128, key256: keys.key256 };
    } else {
      ws.crypto = { enabled: false };
    }

    const respBuffer = types.HandshakeRequest.encode(respPayload).finish();
    const respHeader = createHeader(MY_PEER_ID, req.myPeerId, PacketType.HandShake, respBuffer.length);
    const out = Buffer.concat([respHeader, Buffer.from(respBuffer)]);
    sendWs(ws, out);
    sent = true;
    if (!ws.serverSessionId) {
      ws.serverSessionId = randomU64String();
    }
    persistSocketMeta(ws);
    if (ws.weAreInitiator === undefined) {
      ws.weAreInitiator = false;
    }

    setTimeout(() => {
      try {
        if (ws.readyState === WS_OPEN) {
          peerManager.pushRouteUpdateTo(req.myPeerId, ws, types, { forceFull: true });
          peerManager.broadcastRouteUpdate(types, ws.groupKey, req.myPeerId, { forceFull: true });
        }
      } catch (e) {
        console.error(`Failed to push initial route update to ${req.myPeerId}:`, e.message);
      }
    }, 50);

  } catch (e) {
    console.error('Handshake error:', e);
    if (!sent) {
      try { ws.close(); } catch (_) { }
    }
  }
}

export function handlePing(ws, header, payload) {
  const msg = wrapPacket(createHeader, MY_PEER_ID, header.fromPeerId, PacketType.Pong, payload, ws);
  sendWs(ws, msg);
}

export function handleForwarding(sourceWs, header, fullMessage, types, peerManager) {
  let targetPeerId = header.toPeerId;
  let body = fullMessage;

  if (header.packetType === PacketType.ForeignNetworkPacket && header.toPeerId === MY_PEER_ID) {
    const foreign = parseForeignNetworkPayload(
      Buffer.isBuffer(fullMessage) ? fullMessage.subarray(16) : Buffer.from(fullMessage).subarray(16)
    );
    if (!foreign || !foreign.inner || foreign.inner.length < 16) {
      peerManager.noteForwardDrop();
      return false;
    }
    targetPeerId = foreign.dstPeerId;
    const networkName = foreign.networkName || (sourceWs && sourceWs.domainName) || getPublicServerNetworkName();
    body = wrapAsForeignNetwork(foreign.inner, targetPeerId, networkName);
  } else if (targetPeerId === MY_PEER_ID) {
    return false;
  }

  let targetWs = peerManager.getPeerWs(targetPeerId, sourceWs && sourceWs.groupKey);
  if (!targetWs) {
    targetWs = peerManager.findPeerWs(targetPeerId);
  }

  if (targetWs && targetWs.readyState === WS_OPEN) {
    try {
      sendWs(targetWs, body);
      peerManager.noteForwardOk();
      return true;
    } catch (e) {
      console.error(`Forward to ${targetPeerId} failed: ${e.message}`);
      const groupKey = sourceWs && sourceWs.groupKey;
      peerManager.removePeer(targetWs);
      try {
        peerManager.broadcastRouteUpdate(types, groupKey);
      } catch (err) {
        console.error(`Broadcast after forward failure failed: ${err.message}`);
      }
      peerManager.noteForwardDrop();
      return false;
    }
  }
  peerManager.noteForwardDrop();
  return false;
}
