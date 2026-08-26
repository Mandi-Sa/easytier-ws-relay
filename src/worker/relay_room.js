import { bufferFromMessage, parseHeader } from './core/packet.js';
import { PacketType, MY_PEER_ID } from './core/constants.js';
import { loadProtos } from './core/protos.js';
import { handleHandshake, handlePing, handleForwarding } from './core/basic_handlers.js';
import { handleRpcReq, handleRpcResp } from './core/rpc_handler.js';
import { PeerManager } from './core/peer_manager.js';
import { randomU64String, maybeDecryptIncoming } from './core/crypto.js';
import {
  applyWorkerEnv,
  getWsPath,
  persistSocketMeta,
  debugLog,
  connectionLimit,
  shouldAcceptConnection,
} from './core/env.js';

export class RelayRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    applyWorkerEnv(env);
    this.types = loadProtos();
    this.peerManager = new PeerManager();
    this.peerManager.setTypes(this.types);
    this.peerManager.onTopologyChange = (groupKey) => {
      try {
        this.peerManager.broadcastRouteUpdate(this.types, groupKey, undefined, { forceFull: true });
      } catch (e) {
        console.error('topology broadcast failed:', e);
      }
    };
    if (env && env.EASYTIER_DISABLE_RELAY !== undefined) {
      this.peerManager.setPureP2PMode(env.EASYTIER_DISABLE_RELAY === '1');
    }
    this.ready = this._boot();
  }

  async _boot() {
    try {
      await this.peerManager.hydrateIdentity(this.state.storage);
    } catch (e) {
      console.error('hydrateIdentity failed:', e);
    }
    // EasyTier Ping payloads vary, so WebSocket auto-response cannot match them.
    this.state.getWebSockets().forEach((ws) => this._restoreSocket(ws));
  }

  async fetch(request) {
    await this.ready;
    const url = new URL(request.url);
    if (url.pathname === '/stats') {
      return Response.json(this.peerManager.getStats());
    }
    const wsPath = getWsPath(this.env);
    if (url.pathname !== wsPath && url.pathname !== wsPath + '/') {
      return new Response('Not found', { status: 404 });
    }
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected websocket', { status: 400 });
    }
    const max = connectionLimit(this.env);
    if (!shouldAcceptConnection(this.state.getWebSockets().length, max)) {
      return new Response('too many connections', { status: 503 });
    }

    const pair = new WebSocketPair();
    const server = pair[1];
    const client = pair[0];
    await this.handleSession(server);

    return new Response(null, { status: 101, webSocket: client });
  }

  async handleSession(webSocket) {
    this.state.acceptWebSocket(webSocket);
    this._initSocket(webSocket);
  }

  async webSocketMessage(ws, message) {
    await this.ready;
    try {
      const buffer0 = bufferFromMessage(message);
      if (!buffer0) {
        console.warn('[ws] unsupported message type', typeof message);
        return;
      }
      const buffer = maybeDecryptIncoming(buffer0, ws);
      if (!buffer) {
        debugLog('[ws] decrypt failed');
        return;
      }
      debugLog(`[ws] recv len=${buffer.length}`);
      ws.lastSeen = Date.now();
      const header = parseHeader(buffer);
      if (!header) {
        debugLog('[ws] parseHeader failed');
        return;
      }
      const payload = buffer.subarray(16);
      this._dispatchPacket(ws, { header, payload, raw: buffer });
    } catch (e) {
      console.error('relay_room message handling error:', e);
      try { ws.close(1011, 'internal error'); } catch (_) { }
    }
  }

  async webSocketClose(ws) {
    await this.ready;
    if (ws.peerId) {
      const groupKey = ws.groupKey;
      const removed = this.peerManager.removePeer(ws);
      if (removed) {
        try {
          this.peerManager.broadcastRouteUpdate(this.types, groupKey);
        } catch (_) { }
      }
    }
  }

  async webSocketError(ws) {
    await this.webSocketClose(ws);
  }

  _dispatchPacket(ws, pkt) {
    const { header, payload, raw } = pkt;
    debugLog(`[ws] header from=${header.fromPeerId} to=${header.toPeerId} type=${header.packetType} len=${header.len}`);
    switch (header.packetType) {
      case PacketType.HandShake:
        handleHandshake(ws, header, payload, this.types, this.peerManager);
        break;
      case PacketType.Ping:
        handlePing(ws, header, payload);
        break;
      case PacketType.RpcReq:
        if (header.toPeerId === undefined || header.toPeerId === null || header.toPeerId === MY_PEER_ID) {
          handleRpcReq(ws, header, payload, this.types, this.peerManager);
          break;
        }
        handleForwarding(ws, header, raw, this.types, this.peerManager);
        break;
      case PacketType.RpcResp:
        if (header.toPeerId === undefined || header.toPeerId === null || header.toPeerId === MY_PEER_ID) {
          handleRpcResp(ws, header, payload, this.types, this.peerManager);
          break;
        }
        handleForwarding(ws, header, raw, this.types, this.peerManager);
        break;
      case PacketType.Data:
      default:
        handleForwarding(ws, header, raw, this.types, this.peerManager);
    }
  }

  _initSocket(ws, meta = {}) {
    ws.peerId = meta.peerId || null;
    ws.groupKey = meta.groupKey || null;
    ws.domainName = meta.domainName || null;
    ws.lastSeen = Date.now();
    ws.serverSessionId = meta.serverSessionId || randomU64String();
    ws.weAreInitiator = false;
    ws.crypto = { enabled: false };
    persistSocketMeta(ws);
  }

  _restoreSocket(ws) {
    const meta = ws.deserializeAttachment ? (ws.deserializeAttachment() || {}) : {};
    this._initSocket(ws, meta);
    if (ws.peerId) {
      this.peerManager.addPeer(ws.peerId, ws);
    }
  }
}
