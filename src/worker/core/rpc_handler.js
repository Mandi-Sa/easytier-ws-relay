import { MY_PEER_ID, PacketType, RpcMethod } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String, sha256 } from './crypto.js';
import { decompressRpcBody } from './compress.js';
import { negotiateRpcCompression } from './rpc_compress.js';
import { debugLog, sendWs, getPublicServerNetworkName } from './env.js';

function rpcLeafName(value) {
  const s = String(value || '');
  const idx = s.lastIndexOf('.');
  return idx >= 0 ? s.slice(idx + 1) : s;
}

function isPeerCenterService(descriptor) {
  return rpcLeafName(descriptor && descriptor.serviceName) === 'PeerCenterRpc'
    || rpcLeafName(descriptor && descriptor.protoName) === 'PeerCenterRpc';
}

function ingestPeerCenterReport(ws, types, peerManager, innerReqBody) {
  const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
  const req = types.ReportPeersRequest.decode(innerReqBody);
  const myPeerId = req.myPeerId;
  const peers = req.peerInfos || req.peer_infos || { directPeers: {} };
  const rawDirect = (peers && (peers.directPeers || peers.direct_peers)) || {};

  const directPeers = {};
  for (const [dstPeerId, info] of Object.entries(rawDirect)) {
    directPeers[String(dstPeerId)] = { latencyMs: (info && typeof info.latencyMs === 'number') ? info.latencyMs : 0 };
  }
  const state = peerManager.getPeerCenterState(groupKey);
  state.globalPeerMap.set(String(myPeerId), { directPeers, lastSeen: Date.now() });
  const edges = Object.keys(directPeers).map((dst) => [Number(myPeerId), Number(dst)]);
  peerManager.ingestPeerCenterEdges(groupKey, myPeerId, edges);
  const snapshot = peerManager.buildPeerCenterResponseMap(groupKey);
  state.digest = calcPeerCenterDigestFromMap(snapshot);
  return { myPeerId, directPeers };
}

function ingestGlobalPeerMap(ws, types, peerManager, innerRespBody, fromPeerId) {
  if (!types.GetGlobalPeerMapResponse) return false;
  const resp = types.GetGlobalPeerMapResponse.decode(innerRespBody);
  const mapObj = resp.globalPeerMap || resp.global_peer_map || {};
  const keys = Object.keys(mapObj);
  // Digest-matched empty response must not wipe a good snapshot.
  if (keys.length === 0) return false;
  const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
  const center = peerManager.pickPeerCenterId(groupKey);
  if (center && fromPeerId && Number(fromPeerId) !== center) return false;
  return peerManager.replacePeerCenterMap(groupKey, mapObj);
}

export function sniffPeerCenterReport(ws, header, payload, types, peerManager) {
  if (!types || !peerManager || !payload) return false;
  try {
    const rpcPacket = types.RpcPacket.decode(payload);
    const descriptor = rpcPacket.descriptor || {};
    if (!isPeerCenterService(descriptor)) return false;
    const method = Number(descriptor.methodIndex);
    if (method !== RpcMethod.PeerCenterReportPeers && method !== RpcMethod.PeerCenterGetGlobalPeerMap) return false;
    if (!decodeRpcBody(rpcPacket, header || {}, peerManager)) return false;
    if (rpcPacket.descriptor) Object.assign(descriptor, rpcPacket.descriptor);
    let innerBody = rpcPacket.body;
    if (method === RpcMethod.PeerCenterReportPeers) {
      try {
        const rpcReqWrapper = types.RpcRequest.decode(rpcPacket.body);
        if (rpcReqWrapper.request && rpcReqWrapper.request.length > 0) {
          innerBody = rpcReqWrapper.request;
        }
      } catch (_) { }
      ingestPeerCenterReport(ws, types, peerManager, innerBody);
      return true;
    }
    try {
      const rpcRespWrapper = types.RpcResponse.decode(rpcPacket.body);
      if (rpcRespWrapper.response && rpcRespWrapper.response.length > 0) {
        innerBody = rpcRespWrapper.response;
      }
    } catch (_) { }
    return ingestGlobalPeerMap(ws, types, peerManager, innerBody, header && header.fromPeerId);
  } catch (_) {
    return false;
  }
}

function calcPeerCenterDigestFromMap(mapObj) {
  const h = sha256();
  const keys = Object.keys(mapObj).sort();
  for (const k of keys) {
    h.update(k);
    const directPeers = mapObj[k].directPeers || {};
    const dKeys = Object.keys(directPeers).sort();
    for (const dk of dKeys) {
      h.update(dk);
      const v = directPeers[dk];
      h.update(Buffer.from(String(v && v.latencyMs !== undefined ? v.latencyMs : 0)));
    }
  }
  const b = h.digest();
  let x = 0n;
  for (let i = 0; i < 8; i++) {
    x = (x << 8n) | BigInt(b[i]);
  }
  const u64 = x & 0xFFFFFFFFFFFFFFFFn;
  return u64.toString();
}

function sendRpcResponse(ws, toPeerId, reqRpcPacket, types, responseBodyBytes) {
  if (!ws || ws.readyState !== 1) {
    console.error(`sendRpcResponse aborted: socket not open (readyState=${ws ? ws.readyState : 'nil'}) toPeer=${toPeerId}`);
    return;
  }
  const compressEnabled = process.env.EASYTIER_COMPRESS_RPC !== '0';
  const accepted = reqRpcPacket && reqRpcPacket.compressionInfo && reqRpcPacket.compressionInfo.acceptedAlgo;
  let responseBody = responseBodyBytes;
  let compressionInfo = { algo: 1, acceptedAlgo: 2 };
  if (compressEnabled && responseBodyBytes && responseBodyBytes.length > 256) {
    const negotiated = negotiateRpcCompression(responseBodyBytes, accepted);
    responseBody = negotiated.body;
    compressionInfo = negotiated.compressionInfo;
  }

  const rpcResponsePayload = {
    response: responseBody,
    error: null,
    runtimeUs: 0,
  };
  const rpcResponseBytes = types.RpcResponse.encode(rpcResponsePayload).finish();

  const rpcRespPacket = {
    fromPeer: MY_PEER_ID,
    toPeer: toPeerId,
    transactionId: reqRpcPacket.transactionId,
    descriptor: reqRpcPacket.descriptor,
    body: rpcResponseBytes,
    isRequest: false,
    totalPieces: 1,
    pieceIdx: 0,
    traceId: reqRpcPacket.traceId,
    compressionInfo,
  };
  const rpcPacketBytes = types.RpcPacket.encode(rpcRespPacket).finish();
  const buf = wrapPacket(createHeader, MY_PEER_ID, toPeerId, PacketType.RpcResp, rpcPacketBytes, ws);
  try {
    sendWs(ws, buf);
    debugLog(`RpcResp -> to=${toPeerId} txLen=${buf.length} txTransaction=${reqRpcPacket.transactionId}`);
  } catch (e) {
    console.error(`sendRpcResponse to ${toPeerId} failed: ${e.message}`);
  }
}

function decodeRpcBody(rpcPacket, header, peerManager) {
  const merged = peerManager.rpcMerger.add({
    fromPeer: rpcPacket.fromPeer || header.fromPeerId,
    transactionId: rpcPacket.transactionId,
    pieceIdx: rpcPacket.pieceIdx || 0,
    totalPieces: rpcPacket.totalPieces || 1,
    body: rpcPacket.body,
    descriptor: rpcPacket.descriptor,
    compressionInfo: rpcPacket.compressionInfo,
  });
  if (merged === null) return false;
  rpcPacket.body = merged.body;
  if (merged.descriptor) rpcPacket.descriptor = merged.descriptor;
  const compressionInfo = merged.compressionInfo || rpcPacket.compressionInfo;
  if (compressionInfo && compressionInfo.algo > 1) {
    try {
      rpcPacket.body = decompressRpcBody(rpcPacket.body, compressionInfo.algo);
      rpcPacket.compressionInfo = { ...compressionInfo, algo: 1 };
    } catch (e) {
      console.error(`RpcPacket decompress failed from ${header.fromPeerId}: ${e.message}`);
      peerManager.noteSyncFailure();
      return false;
    }
  }
  return true;
}

export function handleRpcReq(ws, header, payload, types, peerManager) {
  try {
    const rpcPacket = types.RpcPacket.decode(payload);
    if (!decodeRpcBody(rpcPacket, header, peerManager)) return;
    const descriptor = rpcPacket.descriptor;

    let innerReqBody = rpcPacket.body;
    try {
      const rpcReqWrapper = types.RpcRequest.decode(rpcPacket.body);
      if (rpcReqWrapper.request && rpcReqWrapper.request.length > 0) {
        innerReqBody = rpcReqWrapper.request;
      }
    } catch (e) {
      debugLog('Failed to decode RpcRequest wrapper, assuming raw body:', e.message);
    }

    if (isPeerCenterService(descriptor) && Number(descriptor.methodIndex) === RpcMethod.PeerCenterReportPeers) {
      ingestPeerCenterReport(ws, types, peerManager, innerReqBody);
      const respBytes = types.ReportPeersResponse.encode({}).finish();
      sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, respBytes);
      return;
    }

    if (isPeerCenterService(descriptor) && Number(descriptor.methodIndex) === RpcMethod.PeerCenterGetGlobalPeerMap) {
      const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
      const state = peerManager.getPeerCenterState(groupKey);
      const req = types.GetGlobalPeerMapRequest.decode(innerReqBody);
      const reqDigest = req.digest !== undefined && req.digest !== null ? String(req.digest) : '0';
      if (reqDigest === state.digest && reqDigest !== '0') {
        const respBytes = types.GetGlobalPeerMapResponse.encode({}).finish();
        sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, respBytes);
        return;
      }

      const snapshot = peerManager.buildPeerCenterResponseMap(groupKey);
      state.digest = calcPeerCenterDigestFromMap(snapshot);
      const respBytes = types.GetGlobalPeerMapResponse.encode({
        globalPeerMap: snapshot,
        digest: state.digest,
      }).finish();
      sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, respBytes);
      return;
    }

    if (isPeerCenterService(descriptor)) {
      debugLog(`Unhandled PeerCenterRpc methodIndex=${descriptor.methodIndex}`);
      return;
    }

    if ((descriptor.serviceName === 'peer_rpc.DirectConnectorRpc' || descriptor.serviceName === 'DirectConnectorRpc')
      && (descriptor.protoName === 'peer_rpc' || !descriptor.protoName)) {
      if (Number(descriptor.methodIndex) === RpcMethod.DirectConnectorGetIpList && types.GetIpListResponse) {
        const respBytes = types.GetIpListResponse.encode({
          publicIpv4: null,
          interfaceIpv4s: [],
          publicIpv6: null,
          interfaceIpv6s: [],
          listeners: [],
        }).finish();
        sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, respBytes);
        return;
      }
    }

    if ((descriptor.serviceName === 'peer_rpc.OspfRouteRpc' || descriptor.serviceName === 'OspfRouteRpc')
      && (descriptor.protoName === 'peer_rpc' || descriptor.protoName === 'peer_rpc.OspfRouteRpc' || descriptor.protoName === 'OspfRouteRpc' || !descriptor.protoName)) {
      const req = types.SyncRouteInfoRequest.decode(innerReqBody);
      const fromPeerId = header.fromPeerId;
      debugLog(`SyncRouteInfo from ${fromPeerId} session=${req.mySessionId} initiator=${req.isInitiator}`);
      if (Number(descriptor.methodIndex) === RpcMethod.OspfSyncRouteInfo) {
        handleSyncRouteInfo(ws, fromPeerId, rpcPacket, req, types, peerManager, innerReqBody);
        return;
      }
      debugLog(`Unhandled OspfRouteRpc methodIndex=${descriptor.methodIndex}`);
      return;
    }

    debugLog(`Unhandled RPC Service: ${descriptor.serviceName} (proto: ${descriptor.protoName})`);

  } catch (e) {
    console.error('RPC Decode error:', e);
    if (peerManager) peerManager.noteSyncFailure();
  }
}

export function handleRpcResp(ws, header, payload, types, peerManager) {
  try {
    debugLog(`RpcResp <- from=${header.fromPeerId} to=${header.toPeerId} len=${payload.length}`);
    const rpcPacket = types.RpcPacket.decode(payload);
    if (!decodeRpcBody(rpcPacket, header, peerManager)) return;

    const descriptor = rpcPacket.descriptor || {};
    let rpcRespBody = rpcPacket.body;
    let rpcResponseDecoded = null;
    try {
      rpcResponseDecoded = types.RpcResponse.decode(rpcRespBody);
      rpcRespBody = rpcResponseDecoded.response || rpcRespBody;
    } catch (e) {
      debugLog(`RpcResp wrapper decode failed from ${header.fromPeerId}: ${e.message}`);
    }
    if (isPeerCenterService(descriptor) && Number(descriptor.methodIndex) === RpcMethod.PeerCenterGetGlobalPeerMap) {
      if (rpcResponseDecoded && rpcResponseDecoded.error) {
        debugLog(`GetGlobalPeerMap error from ${header.fromPeerId}:`, rpcResponseDecoded.error);
        if (peerManager && typeof peerManager.noteRpcError === 'function') {
          peerManager.noteRpcError(rpcResponseDecoded.error);
        }
        return;
      }
      ingestGlobalPeerMap(ws, types, peerManager, rpcRespBody, header.fromPeerId);
      return;
    }
    if ((descriptor.serviceName === 'peer_rpc.OspfRouteRpc' || descriptor.serviceName === 'OspfRouteRpc')
      && (descriptor.protoName === 'peer_rpc' || descriptor.protoName === 'peer_rpc.OspfRouteRpc' || descriptor.protoName === 'OspfRouteRpc' || !descriptor.protoName)) {
      try {
        const resp = types.SyncRouteInfoResponse.decode(rpcRespBody);
        const sessionId = resp && resp.sessionId ? resp.sessionId : null;
        if (sessionId && ws && ws.groupKey !== undefined) {
          peerManager.onRouteSessionAck(ws.groupKey, header.fromPeerId, sessionId, ws.weAreInitiator);
          debugLog(`RpcResp SyncRouteInfoResponse from=${header.fromPeerId} sessionId=${sessionId} acked`);
        }
      } catch (e) {
        console.error(`Decode SyncRouteInfoResponse failed from ${header.fromPeerId}: ${e.message}`);
        peerManager.noteSyncFailure();
      }
      return;
    }

    if (rpcResponseDecoded) {
      if (rpcResponseDecoded.error) {
        console.warn(`RpcResp error from ${header.fromPeerId}:`, rpcResponseDecoded.error);
      } else {
        debugLog(`RpcResp from=${header.fromPeerId} ok`);
      }
    }
  } catch (e) {
    console.error('RPC Resp Decode error:', e);
    if (peerManager) peerManager.noteSyncFailure();
  }
}

function handleSyncRouteInfo(ws, fromPeerId, reqRpcPacket, syncReq, types, peerManager, rawSyncBytes) {
  const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';

  if (!ws.serverSessionId) {
    ws.serverSessionId = randomU64String();
  }

  ws.weAreInitiator = true;
  peerManager.onRouteSessionAck(groupKey, fromPeerId, syncReq.mySessionId, true);

  let duplicate = false;
  if (syncReq.peerInfos && syncReq.peerInfos.items) {
    for (const info of syncReq.peerInfos.items) {
      if (peerManager.isDuplicatePeerId(groupKey, fromPeerId, info)) {
        duplicate = true;
        break;
      }
    }
  }

  if (duplicate) {
    const respBytes = types.SyncRouteInfoResponse.encode({
      isInitiator: !syncReq.isInitiator,
      sessionId: ws.serverSessionId,
      error: 0,
    }).finish();
    sendRpcResponse(ws, fromPeerId, reqRpcPacket, types, respBytes);
    return;
  }

  if (syncReq.peerInfos && syncReq.peerInfos.items) {
    syncReq.peerInfos.items.forEach(info => {
      if (!peerManager.shouldAcceptPeerInfo(groupKey, info.peerId, fromPeerId)) {
        return;
      }
      peerManager.updatePeerInfo(groupKey, info.peerId, info);
    });
  }

  peerManager.ingestConnInfo(groupKey, fromPeerId, syncReq, rawSyncBytes);

  const respPayload = {
    isInitiator: !syncReq.isInitiator,
    sessionId: ws.serverSessionId
  };
  const respBytes = types.SyncRouteInfoResponse.encode(respPayload).finish();
  sendRpcResponse(ws, fromPeerId, reqRpcPacket, types, respBytes);

  peerManager.pushRouteUpdateTo(fromPeerId, ws, types, { forceFull: false });
  peerManager.broadcastRouteUpdate(types, groupKey, fromPeerId, { forceFull: false });
}

function peerCenterDomainName(ws) {
  if (ws && ws.domainName) return String(ws.domainName);
  const gk = String(ws && ws.groupKey || '');
  const sep = gk.indexOf(':');
  if (sep > 0) return gk.slice(0, sep);
  return getPublicServerNetworkName();
}

export function requestGlobalPeerMap(ws, types, peerManager) {
  if (!ws || ws.readyState !== 1 || !types || !ws.peerId) return false;
  const toPeerId = Number(ws.peerId);
  const reqBytes = types.GetGlobalPeerMapRequest.encode({ digest: 0 }).finish();
  const rpcRequestBytes = types.RpcRequest.encode({ request: reqBytes, timeoutMs: 5000 }).finish();
  const rpcReqPacket = {
    fromPeer: MY_PEER_ID,
    toPeer: toPeerId,
    transactionId: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
    descriptor: {
      // Match easytier-core ServiceKey: domain = overlay network_name,
      // proto_name/service_name = descriptor.proto_name()/name() = "PeerCenterRpc".
      // OSPF uses the same shape (OspfRouteRpc/OspfRouteRpc) on this WSS.
      domainName: peerCenterDomainName(ws),
      protoName: 'PeerCenterRpc',
      serviceName: 'PeerCenterRpc',
      methodIndex: RpcMethod.PeerCenterGetGlobalPeerMap,
    },
    body: rpcRequestBytes,
    isRequest: true,
    totalPieces: 1,
    pieceIdx: 0,
    traceId: 0,
    compressionInfo: { algo: 1, acceptedAlgo: 2 },
  };
  const rpcPacketBytes = types.RpcPacket.encode(rpcReqPacket).finish();
  try {
    sendWs(ws, wrapPacket(createHeader, MY_PEER_ID, toPeerId, PacketType.RpcReq, rpcPacketBytes, ws));
    if (peerManager && typeof peerManager.notePeerCenterPull === 'function') {
      peerManager.notePeerCenterPull(toPeerId);
    }
    return true;
  } catch (e) {
    console.error(`requestGlobalPeerMap to ${toPeerId} failed: ${e.message}`);
    if (peerManager) peerManager.noteSyncFailure();
    return false;
  }
}

export function requestGlobalPeerMapFromCenter(peerManager, types) {
  if (!peerManager || !types) return false;
  let any = false;
  const groups = peerManager.peersByGroup ? Array.from(peerManager.peersByGroup.keys()) : [''];
  if (groups.length === 0) groups.push('');
  for (const gk of groups) {
    const center = peerManager.pickPeerCenterId(gk);
    if (!center) continue;
    const ws = peerManager.getPeerWs(center, gk) || peerManager.findPeerWs(center);
    if (requestGlobalPeerMap(ws, types, peerManager)) any = true;
  }
  return any;
}

