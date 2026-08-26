import { Buffer } from 'buffer';
import { MY_PEER_ID, PacketType } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String } from './crypto.js';
import { getPublicServerNetworkName } from './env.js';
import { sendWs } from './env.js';
import { RpcPieceMerger } from './rpc_pieces.js';
import { parseConnBitmapEdges, parseConnPeerList, edgesFromConnPeerList, buildStarAndReportedBitmap } from './topology.js';

const WS_OPEN = 1; // WebSocket.OPEN in CF runtime

function parseIpv4ToU32Be(ip) {
  const parts = String(ip).trim().split('.').map(x => Number(x));
  if (parts.length !== 4 || parts.some(x => !Number.isInteger(x) || x < 0 || x > 255)) {
    throw new Error(`Invalid IPv4: ${ip}`);
  }
  return ((parts[0] << 24) >>> 0) + (parts[1] << 16) + (parts[2] << 8) + parts[3];
}

function mask32FromLen(len) {
  const l = Number(len);
  if (!Number.isFinite(l) || l <= 0) return 0;
  if (l >= 32) return 0xFFFFFFFF >>> 0;
  return (0xFFFFFFFF << (32 - l)) >>> 0;
}

function deriveSameNetworkIpv4(peerAddr, networkLength, myPeerId) {
  const mask = mask32FromLen(networkLength);
  const net = (peerAddr >>> 0) & mask;
  const hostBits = 32 - Number(networkLength);
  if (!Number.isFinite(hostBits) || hostBits <= 1 || hostBits > 30) {
    return null;
  }
  const hostMax = (1 << hostBits) >>> 0;
  const peerHost = (peerAddr >>> 0) & (~mask >>> 0);
  let host = (Number(myPeerId) % 250) + 2;
  if (host >= hostMax) {
    host = (Number(myPeerId) % Math.max(hostMax - 2, 1)) + 1;
  }
  if (host === peerHost) {
    host = (host + 1) % hostMax;
    if (host === 0) host = 1;
  }
  return (net | host) >>> 0;
}

function makeInstId() {
  return {
    part1: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
    part2: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
    part3: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
    part4: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
  };
}

function instIdKey(instId) {
  if (!instId || typeof instId !== 'object') return null;
  const a = Number(instId.part1 || 0);
  const b = Number(instId.part2 || 0);
  const c = Number(instId.part3 || 0);
  const d = Number(instId.part4 || 0);
  if (!a && !b && !c && !d) return null;
  return `${a}:${b}:${c}:${d}`;
}

export class PeerManager {
  constructor() {
    this.peersByGroup = new Map(); // groupKey -> Map(peerId -> ws)
    this.peerInfosByGroup = new Map(); // groupKey -> Map(peerId -> peerInfo)
    this.routeSessions = new Map(); // groupKey -> peerId -> session state
    this.peerConnVersions = new Map(); // groupKey -> peerId -> version
    this.types = null;

    this.allowVirtualIP = false;
    this.ipConfiguredByEnv = !!process.env.EASYTIER_IPV4_ADDR;
    this.netConfiguredByEnv = process.env.EASYTIER_NETWORK_LENGTH !== undefined;
    this.ipAutoAssigned = false;
    this.myInfo = null; // lazily initialized to avoid random in global scope
    this.sessionTtlMs = Number(process.env.EASYTIER_SESSION_TTL_MS || 3 * 60 * 1000);
    this.lastSessionCleanup = 0;

    this.pureP2PMode = (process.env.EASYTIER_DISABLE_RELAY === '1');
    this.networkDigests = new Map();
    this.instIdByGroup = new Map();
    this.peerCenterByGroup = new Map();
    this.rpcMerger = new RpcPieceMerger();
    this.storedInstId = null;
    this.storedPeerRouteId = null;
    this.syncFailures = 0;
    this.forwardDrops = 0;
    this.forwardOk = 0;
    this.onTopologyChange = null;
    this.reportedConns = new Map();
    this.lastSyncByPeer = new Map();
    this.storage = null;
  }

  async hydrateIdentity(storage) {
    this.storage = storage || null;
    if (!storage || typeof storage.get !== 'function') return;
    const instId = await storage.get('relayInstId');
    const peerRouteId = await storage.get('relayPeerRouteId');
    if (instId) this.storedInstId = instId;
    if (peerRouteId !== undefined && peerRouteId !== null) this.storedPeerRouteId = peerRouteId;
    const savedVersions = await storage.get('connVersions');
    if (savedVersions && typeof savedVersions === 'object') {
      for (const [gk, mapObj] of Object.entries(savedVersions)) {
        const m = this._getPeerConnVersionMap(gk, true);
        for (const [pid, ver] of Object.entries(mapObj || {})) {
          m.set(Number(pid), Number(ver) || 0);
        }
      }
    }
    const my = this.ensureMyInfo();
    if (typeof storage.put !== 'function') return;
    if (!instId) await storage.put('relayInstId', my.instId);
    if (peerRouteId === undefined || peerRouteId === null) {
      await storage.put('relayPeerRouteId', my.peerRouteId);
    }
  }

  async persistConnVersions() {
    if (!this.storage || typeof this.storage.put !== 'function') return;
    const out = {};
    for (const [gk, m] of this.peerConnVersions.entries()) {
      out[gk] = Object.fromEntries(m.entries());
    }
    try {
      await this.storage.put('connVersions', out);
    } catch (_) { }
  }

  setTypes(types) {
    this.types = types;
  }

  ensureMyInfo() {
    if (this.myInfo) return this.myInfo;
    const myInfo = {
      peerId: MY_PEER_ID,
      instId: this.storedInstId || makeInstId(),
      cost: 1,
      version: 1,
      featureFlag: {
        isPublicServer: true,
        avoidRelayData: this.pureP2PMode,
        kcpInput: true,
        noRelayKcp: false,
        quicInput: true,
        supportConnListSync: true,
      },
      networkLength: Number(process.env.EASYTIER_NETWORK_LENGTH || 24),
      easytierVersion: process.env.EASYTIER_VERSION || "cf-ws-relay",
      lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
      hostname: process.env.EASYTIER_HOSTNAME || "PublicServer_WorkerRelay",
      udpStunInfo: 0,
      peerRouteId: this.storedPeerRouteId || randomU64String(),
      groups: [],
    };

    if (this.allowVirtualIP) {
      const ipEnv = process.env.EASYTIER_IPV4_ADDR;
      if (ipEnv) {
        myInfo.ipv4Addr = { addr: parseIpv4ToU32Be(ipEnv) };
        this.ipAutoAssigned = false;
      } else if (process.env.EASYTIER_AUTO_IPV4_ADDR === '1') {
        const lastOctet = (Number(MY_PEER_ID) % 250) + 2;
        myInfo.ipv4Addr = { addr: parseIpv4ToU32Be(`10.0.0.${lastOctet}`) };
        this.ipAutoAssigned = true;
      }
    }

    this.myInfo = myInfo;
    return this.myInfo;
  }

  bumpMyInfoVersion() {
    const myInfo = this.ensureMyInfo();
    myInfo.version = (myInfo.version || 0) + 1;
    myInfo.lastUpdate = { seconds: Math.floor(Date.now() / 1000), nanos: 0 };
  }

  _getPeerConnVersionMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peerConnVersions.get(k);
    if (!m && create) {
      m = new Map();
      this.peerConnVersions.set(k, m);
    }
    return m;
  }

  _getInstMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.instIdByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.instIdByGroup.set(k, m);
    }
    return m;
  }

  registerNetwork(networkName, digestHex) {
    const name = String(networkName || '');
    const existing = this.networkDigests.get(name);
    if (existing && existing !== digestHex) return null;
    if (!existing) this.networkDigests.set(name, digestHex);
    return `${name}:${this.networkDigests.get(name) || ''}`;
  }

  bumpPeerConnVersion(groupKey, peerId) {
    const m = this._getPeerConnVersionMap(groupKey, true);
    const current = m.get(peerId) || 0;
    const next = Math.max(current + 1, Math.floor(Date.now() / 1000));
    m.set(peerId, next);
    this.persistConnVersions();
    return next;
  }

  getPeerConnVersion(groupKey, peerId) {
    const m = this._getPeerConnVersionMap(groupKey, false);
    return m ? (m.get(peerId) || 0) : 0;
  }

  bumpAllPeerConnVersions(groupKey) {
    const allPeers = new Set(this.listPeerIdsInGroup(groupKey));
    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) {
      for (const pid of infos.keys()) {
        allPeers.add(pid);
      }
    }
    allPeers.add(MY_PEER_ID);
    for (const pid of allPeers) {
      this.bumpPeerConnVersion(groupKey, pid);
    }
  }

  _getReportedConns(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.reportedConns.get(k);
    if (!m && create) {
      m = new Map();
      this.reportedConns.set(k, m);
    }
    return m;
  }

  ingestReportedEdges(groupKey, fromPeerId, edges) {
    const m = this._getReportedConns(groupKey, true);
    const connected = new Set();
    for (const [a, b] of edges || []) {
      const other = Number(a) === Number(fromPeerId) ? Number(b) : Number(b) === Number(fromPeerId) ? Number(a) : null;
      if (other == null || other === Number(fromPeerId) || other === MY_PEER_ID) continue;
      connected.add(other);
    }
    const prev = m.get(Number(fromPeerId));
    const prevKey = prev ? Array.from(prev).sort().join(',') : '';
    const nextKey = Array.from(connected).sort().join(',');
    m.set(Number(fromPeerId), connected);
    if (prevKey !== nextKey) {
      this.bumpPeerConnVersion(groupKey, fromPeerId);
      this.bumpPeerConnVersion(groupKey, MY_PEER_ID);
    }
  }

  ingestConnInfo(groupKey, fromPeerId, syncReq, rawSyncBytes) {
    const edges = [];
    if (syncReq && syncReq.connBitmap) {
      edges.push(...parseConnBitmapEdges(syncReq.connBitmap, fromPeerId));
    }
    if (rawSyncBytes) {
      const list = parseConnPeerList(rawSyncBytes);
      edges.push(...edgesFromConnPeerList(list, fromPeerId));
    }
    this.ingestReportedEdges(groupKey, fromPeerId, edges);
    this.notePeerSync(fromPeerId);
  }

  collectReportedEdges(groupKey) {
    const live = new Set(this.listPeerIdsInGroup(groupKey).map(Number));
    live.add(MY_PEER_ID);
    const edges = [];
    const m = this._getReportedConns(groupKey, false);
    if (!m) return edges;
    for (const [src, dsts] of m.entries()) {
      if (!live.has(Number(src))) continue;
      for (const dst of dsts) {
        if (!live.has(Number(dst))) continue;
        edges.push([Number(src), Number(dst)]);
      }
    }
    return edges;
  }

  notePeerSync(peerId) {
    this.lastSyncByPeer.set(Number(peerId), Date.now());
  }

  routeIdKey(routeId) {
    if (routeId == null) return '';
    if (typeof routeId === 'object') {
      return `${routeId.part1 || 0}:${routeId.part2 || 0}:${routeId.part3 || 0}:${routeId.part4 || 0}`;
    }
    return String(routeId);
  }

  isDuplicatePeerId(groupKey, fromPeerId, info) {
    if (!info || Number(info.peerId) !== Number(fromPeerId)) return false;
    const existing = this._getPeerInfosMap(groupKey, false)?.get(fromPeerId);
    if (!existing || existing.peerRouteId == null || info.peerRouteId == null) return false;
    const a = this.routeIdKey(existing.peerRouteId);
    const b = this.routeIdKey(info.peerRouteId);
    return !!(a && b && a !== b);
  }

  setPublicServerFlag(isPublicServer) {
    const myInfo = this.ensureMyInfo();
    const next = !!isPublicServer;
    const prev = !!(myInfo.featureFlag && myInfo.featureFlag.isPublicServer);
    myInfo.featureFlag = {
      ...myInfo.featureFlag,
      isPublicServer: next,
    };
    if (next !== prev) {
      this.bumpMyInfoVersion();
    }
  }

  setPureP2PMode(enabled) {
    const next = !!enabled;
    if (next === this.pureP2PMode) return;
    this.pureP2PMode = next;
    const myInfo = this.ensureMyInfo();
    myInfo.featureFlag = {
      ...myInfo.featureFlag,
      avoidRelayData: this.pureP2PMode,
    };
    this.bumpMyInfoVersion();
  }

  isPureP2PMode() {
    return !!this.pureP2PMode;
  }

  _getPeersMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peersByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.peersByGroup.set(k, m);
    }
    return m;
  }

  _getPeerInfosMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peerInfosByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.peerInfosByGroup.set(k, m);
    }
    return m;
  }

  _getSession(groupKey, peerId, create = false) {
    const now = Date.now();
    if (now - this.lastSessionCleanup > Math.max(30_000, Math.min(this.sessionTtlMs / 2, 120_000))) {
      this.cleanupSessions(now);
    }
    const gk = String(groupKey || '');
    let g = this.routeSessions.get(gk);
    if (!g && create) {
      g = new Map();
      this.routeSessions.set(gk, g);
    }
    if (!g) return null;
    let s = g.get(peerId);
    if (!s && create) {
      s = {
        mySessionId: null,
        dstSessionId: null,
        weAreInitiator: false,
        peerInfoVerMap: new Map(),
        connBitmapVerMap: new Map(),
        foreignNetVer: 0,
        lastTouch: Date.now(),
        lastConnBitmapSig: null,
      };
      g.set(peerId, s);
    }
    if (s) s.lastTouch = Date.now();
    return s;
  }

  cleanupSessions(nowTs = Date.now()) {
    this.lastSessionCleanup = nowTs;
    const ttl = this.sessionTtlMs;
    for (const [gk, m] of this.routeSessions.entries()) {
      for (const [pid, s] of m.entries()) {
        if (nowTs - (s.lastTouch || 0) > ttl) {
          m.delete(pid);
        }
      }
      if (m.size === 0) this.routeSessions.delete(gk);
    }
  }

  onRouteSessionAck(groupKey, peerId, theirSessionId, weAreInitiator) {
    const s = this._getSession(groupKey, peerId, true);
    if (s.dstSessionId !== theirSessionId) {
      s.peerInfoVerMap.clear();
      s.connBitmapVerMap.clear();
      s.foreignNetVer = 0;
      s.lastConnBitmapSig = null;
    }
    s.dstSessionId = theirSessionId;
    if (typeof weAreInitiator === 'boolean') {
      s.weAreInitiator = weAreInitiator;
    }
  }

  addPeer(peerId, ws) {
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    const peers = this._getPeersMap(groupKey, true);
    const existing = peers.get(peerId);
    const isNewPeer = !existing;
    if (existing && existing !== ws) {
      existing.replacedByNewConnection = true;
      existing.peerId = null;
      try { existing.close(1000, 'replaced'); } catch (_) { }
    }
    peers.set(peerId, ws);
    if (isNewPeer) {
      this.bumpAllPeerConnVersions(groupKey);
    }
  }

  removePeer(ws) {
    if (!ws || ws.replacedByNewConnection) return false;
    const peerId = ws.peerId;
    const groupKey = ws.groupKey ? String(ws.groupKey) : '';
    if (!peerId) return false;
    const peers = this._getPeersMap(groupKey, false);
    if (!peers || peers.get(peerId) !== ws) return false;
    peers.delete(peerId);
    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) infos.delete(peerId);
    const sessions = this.routeSessions.get(groupKey);
    if (sessions) {
      sessions.delete(peerId);
      if (sessions.size === 0) this.routeSessions.delete(groupKey);
    }
    const connVers = this._getPeerConnVersionMap(groupKey, false);
    if (connVers) connVers.delete(peerId);

    this.pruneStalePeerInfos(groupKey);

    if (peers.size > 0) {
      this.bumpAllPeerConnVersions(groupKey);
    }

    if (peers.size === 0) {
      this.peersByGroup.delete(groupKey);
      this.peerInfosByGroup.delete(groupKey);
      this.peerConnVersions.delete(groupKey);
      this.instIdByGroup.delete(groupKey);
    }
    this._clearInstId(groupKey, peerId);
    const reported = this._getReportedConns(groupKey, false);
    if (reported) reported.delete(Number(peerId));
    this.lastSyncByPeer.delete(Number(peerId));
    return true;
  }

  _clearInstId(groupKey, peerId) {
    const m = this._getInstMap(groupKey, false);
    if (!m) return;
    for (const [key, pid] of m.entries()) {
      if (pid === peerId) m.delete(key);
    }
  }

  forgetPeerId(groupKey, peerId) {
    const peers = this._getPeersMap(groupKey, false);
    const oldWs = peers ? peers.get(peerId) : undefined;
    if (peers) peers.delete(peerId);
    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) infos.delete(peerId);
    const sessions = this.routeSessions.get(groupKey);
    if (sessions) {
      sessions.delete(peerId);
      if (sessions.size === 0) this.routeSessions.delete(groupKey);
    }
    const connVers = this._getPeerConnVersionMap(groupKey, false);
    if (connVers) connVers.delete(peerId);
    this._clearInstId(groupKey, peerId);
    const center = this.peerCenterByGroup.get(String(groupKey || ''));
    if (center) center.globalPeerMap.delete(String(peerId));
    this.pruneStalePeerInfos(groupKey);
    if (peers && peers.size > 0) this.bumpAllPeerConnVersions(groupKey);
    return oldWs;
  }

  replaceByInstId(groupKey, peerId, instId) {
    const key = instIdKey(instId);
    if (!key) return;
    const m = this._getInstMap(groupKey, true);
    const oldPid = m.get(key);
    if (oldPid && oldPid !== peerId) {
      const oldWs = this.forgetPeerId(groupKey, oldPid);
      if (oldWs) {
        oldWs.replacedByNewConnection = true;
        oldWs.peerId = null;
        try { oldWs.close(1000, 'replaced-inst'); } catch (_) { }
      }
      if (typeof this.onTopologyChange === 'function') {
        try { this.onTopologyChange(groupKey); } catch (_) { }
      }
    }
    m.set(key, peerId);
  }

  getAdvertisedPeerIds(groupKey) {
    const live = this.listPeerIdsInGroup(groupKey);
    const ids = new Set(live);
    ids.add(MY_PEER_ID);
    return [MY_PEER_ID, ...Array.from(ids).filter((p) => p !== MY_PEER_ID).sort((a, b) => Number(a) - Number(b))];
  }

  shouldAcceptPeerInfo(groupKey, peerId, _fromPeerId) {
    if (peerId === MY_PEER_ID) return false;
    return !!this.getPeerWs(peerId, groupKey);
  }

  pruneStalePeerInfos(groupKey) {
    const live = new Set(this.listPeerIdsInGroup(groupKey));
    const infos = this._getPeerInfosMap(groupKey, false);
    if (!infos) return;
    for (const pid of Array.from(infos.keys())) {
      if (pid !== MY_PEER_ID && !live.has(pid)) {
        infos.delete(pid);
      }
    }
  }

  collectPeerInfosForRoute(groupKey) {
    const items = [];
    for (const pid of this.getAdvertisedPeerIds(groupKey)) {
      const info = (pid === MY_PEER_ID)
        ? this.ensureMyInfo()
        : (this._getPeerInfosMap(groupKey, false)?.get(pid));
      if (info) items.push(info);
    }
    return items;
  }

  getStats() {
    let peers = 0;
    for (const m of this.peersByGroup.values()) peers += m.size;
    return {
      ok: true,
      peers,
      groups: this.peersByGroup.size,
      syncFailures: this.syncFailures,
      forwardDrops: this.forwardDrops,
      forwardOk: this.forwardOk,
      peerIds: this.listAllPeerIds(),
      lastSync: Object.fromEntries(this.lastSyncByPeer.entries()),
    };
  }

  listAllPeerIds() {
    const ids = [];
    for (const m of this.peersByGroup.values()) {
      for (const pid of m.keys()) ids.push(pid);
    }
    return ids;
  }

  findPeerWs(peerId) {
    for (const m of this.peersByGroup.values()) {
      const ws = m.get(peerId);
      if (ws) return ws;
    }
    return undefined;
  }

  noteForwardDrop() {
    this.forwardDrops += 1;
  }

  noteForwardOk() {
    this.forwardOk += 1;
  }

  noteSyncFailure() {
    this.syncFailures += 1;
  }

  getPeerCenterState(groupKey) {
    const k = String(groupKey || '');
    let s = this.peerCenterByGroup.get(k);
    if (!s) {
      s = { globalPeerMap: new Map(), digest: '0', lastTouch: Date.now() };
      this.peerCenterByGroup.set(k, s);
    }
    s.lastTouch = Date.now();
    return s;
  }

  buildPeerCenterResponseMap(groupKey) {
    const out = {};
    const state = this.getPeerCenterState(groupKey);
    for (const peerId of this.listPeerIdsInGroup(groupKey)) {
      const key = String(peerId);
      const existing = state.globalPeerMap.get(key);
      out[key] = existing ? { ...existing } : { directPeers: {} };
      if (!out[key].directPeers) out[key].directPeers = {};
      out[key].directPeers[String(MY_PEER_ID)] = { latencyMs: 0 };
    }
    return out;
  }

  getPeerWs(peerId, groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? peers.get(peerId) : undefined;
  }

  listPeerIdsInGroup(groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? Array.from(peers.keys()) : [];
  }

  listPeersInGroup(groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? Array.from(peers.entries()) : [];
  }

  updatePeerInfo(groupKey, peerId, info) {
    const infos = this._getPeerInfosMap(groupKey, true);
    const isNew = !infos.has(peerId);
    infos.set(peerId, info);
    this.replaceByInstId(groupKey, peerId, info && info.instId);
    if (isNew) {
      this.bumpAllPeerConnVersions(groupKey);
    }

    if (this.allowVirtualIP && !this.ipConfiguredByEnv && this.ipAutoAssigned) {
      const myInfo = this.ensureMyInfo();
      const peerIpv4 = info && info.ipv4Addr && typeof info.ipv4Addr.addr === 'number' ? (info.ipv4Addr.addr >>> 0) : null;
      const peerNetLen = info && (info.networkLength || info.network_length);
      const netLen = Number(peerNetLen || myInfo.networkLength || 24);
      if (peerIpv4 !== null && Number.isFinite(netLen) && netLen > 0) {
        const derived = deriveSameNetworkIpv4(peerIpv4, netLen, MY_PEER_ID);
        if (derived !== null) {
          let changed = false;
          if (!this.netConfiguredByEnv) {
            if (myInfo.networkLength !== netLen) {
              myInfo.networkLength = netLen;
              changed = true;
            }
          }
          const prevAddr = myInfo.ipv4Addr && typeof myInfo.ipv4Addr.addr === 'number'
            ? (myInfo.ipv4Addr.addr >>> 0)
            : null;
          if (prevAddr !== derived) {
            myInfo.ipv4Addr = { addr: derived };
            changed = true;
          }

          if (changed) {
            this.bumpMyInfoVersion();
            this.ipAutoAssigned = false;
          }
        }
      }
    }
  }

  broadcastRouteUpdate(types, groupKey, excludePeerId, opts = {}) {
    const forceFull = opts.forceFull !== undefined ? !!opts.forceFull : true;
    if (groupKey !== undefined) {
      const peers = this._getPeersMap(groupKey, false);
      if (!peers) return;
      for (const [peerId, ws] of peers.entries()) {
        if (peerId === excludePeerId) continue;
        if (ws.readyState === WS_OPEN) {
          this.pushRouteUpdateTo(peerId, ws, types, { forceFull });
        }
      }
      return;
    }
    for (const [gk, peers] of this.peersByGroup.entries()) {
      for (const [peerId, ws] of peers.entries()) {
        if (peerId === excludePeerId) continue;
        if (ws.readyState === WS_OPEN) {
          this.pushRouteUpdateTo(peerId, ws, types, { forceFull });
        }
      }
    }
  }

  pushRouteUpdateTo(targetPeerId, ws, types, opts = {}) {
    const forceFull = !!opts.forceFull;
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    this.pruneStalePeerInfos(groupKey);
    const session = this._getSession(groupKey, targetPeerId, true);
    const myInfo = this.ensureMyInfo();
    if (!ws.serverSessionId) {
      ws.serverSessionId = randomU64String();
    }
    session.mySessionId = ws.serverSessionId;
    const forceFullLocal = forceFull || !session.dstSessionId;

    const relevantPeers = this.getAdvertisedPeerIds(groupKey);

    const peerInfosItems = [];
    for (const pid of relevantPeers) {
      const info = (pid === MY_PEER_ID)
        ? myInfo
        : (this._getPeerInfosMap(groupKey, false)?.get(pid));
      if (!info) continue;
      const version = info && info.version ? info.version : 1;
      const prev = forceFullLocal ? 0 : (session.peerInfoVerMap.get(pid) || 0);
      if (forceFullLocal || version > prev) {
        peerInfosItems.push(info);
        session.peerInfoVerMap.set(pid, version);
      }
    }

    let connBitmap = null;
    if (relevantPeers.length > 0) {
      const connVersions = this._getPeerConnVersionMap(groupKey, true);
      const peerIdVersions = relevantPeers.map((pid) => {
        const existing = connVersions.get(pid) || Math.floor(Date.now() / 1000);
        return { peerId: pid, version: existing };
      });
      const N = peerIdVersions.length;
      const reported = this.collectReportedEdges(groupKey);
      const bitmapBuf = buildStarAndReportedBitmap(
        peerIdVersions.map((p) => p.peerId),
        reported,
        MY_PEER_ID,
      );
      const sig = `${peerIdVersions.map(p => `${p.peerId}:${p.version}`).join(',')}|${bitmapBuf.toString('hex')}`;
      const connVersion = session.connBitmapVerMap.get(targetPeerId) || 0;
      const nextConnVersion = connVersion || Math.max(...peerIdVersions.map(p => p.version));
      if (sig !== session.lastConnBitmapSig) {
        session.connBitmapVerMap.set(targetPeerId, nextConnVersion);
        session.lastConnBitmapSig = sig;
        connBitmap = { peerIds: peerIdVersions, bitmap: bitmapBuf, version: nextConnVersion };
      }
    }

    const foreignNetworkInfos = (() => {
      const mode = (process.env.EASYTIER_HANDSHAKE_MODE || 'foreign').toLowerCase();
      if (mode === 'same' || mode === 'same_network') return null;
      const version = session.foreignNetVer + 1;
      session.foreignNetVer = version;
      const livePeers = this.listPeerIdsInGroup(groupKey);
      return {
        infos: [{
          key: {
            peerId: MY_PEER_ID,
            networkName: getPublicServerNetworkName()
          },
          value: {
            foreignPeerIds: livePeers,
            lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
            version,
            networkSecretDigest: Buffer.alloc(32),
            myPeerIdForThisNetwork: MY_PEER_ID
          }
        }]
      };
    })();

    const t = this.types;
    if (!t) {
      throw new Error('PeerManager types not set');
    }
    const rawPeerInfos = peerInfosItems.length > 0
      ? peerInfosItems.map(info => t.RoutePeerInfo.encode(info).finish())
      : null;

    const reqPayload = {
      myPeerId: MY_PEER_ID,
      mySessionId: ws.serverSessionId,
      isInitiator: !!ws.weAreInitiator,
      peerInfos: peerInfosItems.length > 0 ? { items: peerInfosItems } : null,
      rawPeerInfos: rawPeerInfos,
      connBitmap: connBitmap,
      foreignNetworkInfos: foreignNetworkInfos
    };

    const reqBytes = t.SyncRouteInfoRequest.encode(reqPayload).finish();
    const rpcRequestPayload = { request: reqBytes, timeoutMs: 5000 };
    const rpcRequestBytes = t.RpcRequest.encode(rpcRequestPayload).finish();

    const rpcReqPacket = {
      fromPeer: MY_PEER_ID,
      toPeer: targetPeerId,
      transactionId: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
      descriptor: {
        domainName: ws.domainName || "public_server",
        protoName: 'OspfRouteRpc',
        serviceName: 'OspfRouteRpc',
        methodIndex: process.env.EASYTIER_OSPF_ROUTE_METHOD_INDEX ? Number(process.env.EASYTIER_OSPF_ROUTE_METHOD_INDEX) : 1
      },
      body: rpcRequestBytes,
      isRequest: true,
      totalPieces: 1,
      pieceIdx: 0,
      traceId: 0,
      compressionInfo: { algo: 1, acceptedAlgo: 2 }
    };

    const rpcPacketBytes = t.RpcPacket.encode(rpcReqPacket).finish();
    try {
      sendWs(ws, wrapPacket(createHeader, MY_PEER_ID, targetPeerId, PacketType.RpcReq, rpcPacketBytes, ws));
    } catch (e) {
      // ignore
    }
  }
}

