import { Buffer } from 'buffer';
import { MY_PEER_ID, PacketType } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String } from './crypto.js';
import { getPublicServerNetworkName, digestBytesFromGroupKey } from './env.js';
import { sendWs } from './env.js';
import { RpcPieceMerger } from './rpc_pieces.js';
import { parseConnBitmapEdges, parseConnPeerList, hasConnPeerList, connRowsFromPeerList, buildStarAndReportedBitmap } from './topology.js';
import { isZstdDecompressAvailable } from './compress.js';

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
    this.topologyTtlMs = Number(process.env.EASYTIER_TOPOLOGY_TTL_MS || 3 * 60 * 1000);
    if (!Number.isFinite(this.topologyTtlMs) || this.topologyTtlMs <= 0) this.topologyTtlMs = 3 * 60 * 1000;
    this.peerCenterTtlMs = Number(process.env.EASYTIER_PEER_CENTER_TTL_MS || 3 * 60 * 1000);
    if (!Number.isFinite(this.peerCenterTtlMs) || this.peerCenterTtlMs <= 0) this.peerCenterTtlMs = 3 * 60 * 1000;

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
    this.peerCenterConns = new Map();
    this.lastSyncByPeer = new Map();
    this.lastPeerCenterByPeer = new Map();
    this.peerCenterPulls = 0;
    this.lastPeerCenterPullPeer = 0;
    this.lastRpcError = '';
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
    const savedTopo = await storage.get('topology');
    if (savedTopo && typeof savedTopo === 'object') {
      const loadedAt = Date.now();
      this._loadConnStore(this.reportedConns, savedTopo.reported, loadedAt);
      this._loadConnStore(this.peerCenterConns, savedTopo.peerCenter, loadedAt);
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

  _snapshotPeers(snapshot) {
    if (snapshot instanceof Set) return new Set(snapshot);
    if (Array.isArray(snapshot)) return new Set(snapshot.map(Number));
    if (snapshot && typeof snapshot === 'object') {
      if (snapshot.peers instanceof Set) return new Set(snapshot.peers);
      if (Array.isArray(snapshot.peers)) return new Set(snapshot.peers.map(Number));
      if (Array.isArray(snapshot.connected)) return new Set(snapshot.connected.map(Number));
    }
    return new Set();
  }

  _snapshotVersion(snapshot) {
    if (!snapshot || typeof snapshot !== 'object' || snapshot instanceof Set || Array.isArray(snapshot)) return 0;
    const version = Number(snapshot.version);
    return Number.isFinite(version) && version >= 0 ? version : 0;
  }

  _snapshotUpdatedAt(snapshot, fallback = Date.now()) {
    if (snapshot && typeof snapshot === 'object' && !(snapshot instanceof Set)) {
      const value = Number(snapshot.updatedAt);
      if (Number.isFinite(value)) return value;
    }
    return fallback;
  }

  _storeSnapshot(store, groupKey, peerId, connected, version = 0, updatedAt = Date.now(), legacy = false) {
    const m = this._getConnMap(store, groupKey, true);
    const snapshot = {
      peers: Array.from(new Set(connected)).map(Number),
      version: Number(version) || 0,
      updatedAt: Number(updatedAt),
    };
    if (legacy) snapshot.legacy = true;
    m.set(Number(peerId), snapshot);
  }

  _dumpConnStore(store) {
    const out = {};
    for (const [gk, m] of store.entries()) {
      out[gk] = {};
      for (const [pid, snapshot] of m.entries()) {
        out[gk][String(pid)] = {
          peers: Array.from(this._snapshotPeers(snapshot)).map(Number),
          version: this._snapshotVersion(snapshot),
          updatedAt: this._snapshotUpdatedAt(snapshot),
          ...(snapshot && snapshot.legacy ? { legacy: true } : {}),
        };
      }
    }
    return out;
  }

  _loadConnStore(store, dump, loadedAt = Date.now()) {
    if (!dump || typeof dump !== 'object') return;
    for (const [gk, mapObj] of Object.entries(dump)) {
      const m = this._getConnMap(store, gk, true);
      for (const [pid, value] of Object.entries(mapObj || {})) {
        // Pre-snapshot dumps were merge-only arrays and re-poison the mesh if reloaded.
        if (Array.isArray(value)) continue;
        if (!value || typeof value !== 'object') continue;
        this._storeSnapshot(
          store,
          gk,
          pid,
          this._snapshotPeers(value),
          this._snapshotVersion(value),
          this._snapshotUpdatedAt(value, loadedAt),
          !!value.legacy,
        );
      }
      if (m.size === 0) store.delete(gk);
    }
  }

  _clearPeerFromConnStore(store, groupKey, peerId, nowTs = Date.now()) {
    const m = this._getConnMap(store, groupKey, false);
    if (!m) return [];
    const target = Number(peerId);
    const changed = new Set();
    for (const [sourceId, snapshot] of Array.from(m.entries())) {
      const source = Number(sourceId);
      const peers = this._snapshotPeers(snapshot);
      const sourceRemoved = source === target;
      const targetRemoved = peers.delete(target);
      if (!sourceRemoved && !targetRemoved) continue;
      changed.add(source);
      if (sourceRemoved || peers.size === 0) {
        m.delete(sourceId);
      } else {
        this._storeSnapshot(
          store,
          groupKey,
          source,
          peers,
          Math.max(this._snapshotVersion(snapshot) + 1, Math.floor(nowTs / 1000)),
          nowTs,
          !!snapshot.legacy,
        );
      }
    }
    if (m.size === 0) store.delete(String(groupKey || ''));
    return Array.from(changed);
  }

  _clearPeerFromTopology(groupKey, peerId, notify = true) {
    const changed = new Set([Number(peerId)]);
    let touched = false;
    for (const store of [this.reportedConns, this.peerCenterConns]) {
      const sources = this._clearPeerFromConnStore(store, groupKey, peerId);
      if (sources.length > 0) touched = true;
      for (const source of sources) changed.add(Number(source));
    }
    if (!touched) return false;
    this._bumpEdgeVersions(groupKey, changed);
    this.persistTopology();
    if (notify) this._notifyTopology(groupKey);
    return true;
  }

  async persistTopology() {
    if (!this.storage || typeof this.storage.put !== 'function') return;
    try {
      await this.storage.put('topology', {
        reported: this._dumpConnStore(this.reportedConns),
        peerCenter: this._dumpConnStore(this.peerCenterConns),
      });
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

  _getConnMap(store, groupKey, create = false) {
    const k = String(groupKey || '');
    let m = store.get(k);
    if (!m && create) {
      m = new Map();
      store.set(k, m);
    }
    return m;
  }

  _getReportedConns(groupKey, create = false) {
    return this._getConnMap(this.reportedConns, groupKey, create);
  }

  _getPeerCenterConns(groupKey, create = false) {
    return this._getConnMap(this.peerCenterConns, groupKey, create);
  }

  _connectedSetFromEdges(fromPeerId, edges) {
    const connected = new Set();
    for (const [a, b] of edges || []) {
      const other = Number(a) === Number(fromPeerId) ? Number(b) : Number(b) === Number(fromPeerId) ? Number(a) : null;
      if (other == null || other === Number(fromPeerId) || other === MY_PEER_ID) continue;
      connected.add(other);
    }
    return connected;
  }

  _bumpEdgeVersions(groupKey, peerIds) {
    const seen = new Set();
    for (const pid of peerIds) {
      const n = Number(pid);
      if (!n || seen.has(n)) continue;
      seen.add(n);
      this.bumpPeerConnVersion(groupKey, n);
    }
    this.bumpPeerConnVersion(groupKey, MY_PEER_ID);
  }

  _notifyTopology(groupKey) {
    if (typeof this.onTopologyChange === 'function') {
      try { this.onTopologyChange(groupKey); } catch (_) { }
    }
  }

  ingestReportedEdges(groupKey, fromPeerId, edges) {
    return this.ingestPeerCenterEdges(groupKey, fromPeerId, edges);
  }

  _writePeerSnapshot(store, groupKey, peerId, connected, version = 0, updatedAt = Date.now(), allowUnversionedReplace = false) {
    const current = this._getConnMap(store, groupKey, true).get(Number(peerId));
    const incomingVersion = Number(version) || 0;
    const currentVersion = this._snapshotVersion(current);
    if (incomingVersion > 0 && incomingVersion < currentVersion) return false;
    if (incomingVersion > 0 && incomingVersion === currentVersion) {
      if (current && this._snapshotPeers(current).size === new Set(connected).size
        && Array.from(this._snapshotPeers(current)).every((peer) => new Set(connected).has(peer))) {
        this._storeSnapshot(store, groupKey, peerId, this._snapshotPeers(current), currentVersion, updatedAt, !!current.legacy);
      }
      return false;
    }
    if (incomingVersion === 0 && current && currentVersion > 0 && !allowUnversionedReplace) return false;
    const nextVersion = incomingVersion > 0
      ? incomingVersion
      : Math.max(currentVersion + 1, Math.floor(Number(updatedAt) / 1000));
    const previous = this._snapshotPeers(current);
    const next = new Set(connected);
    const changed = previous.size !== next.size || Array.from(previous).some((peer) => !next.has(peer));
    this._storeSnapshot(store, groupKey, peerId, next, nextVersion, updatedAt);
    return changed;
  }

  ingestPeerCenterEdges(groupKey, fromPeerId, edges, version = 0, updatedAt = Date.now()) {
    const connected = this._connectedSetFromEdges(fromPeerId, edges);
    this.lastPeerCenterByPeer.set(Number(fromPeerId), Date.now());
    // Sniffed PeerCenter frames can arrive empty; do not treat that as a withdrawal.
    if (connected.size === 0) return false;
    const changed = this._writePeerSnapshot(
      this.peerCenterConns,
      groupKey,
      fromPeerId,
      connected,
      version,
      updatedAt,
      true,
    );
    if (!changed) return false;
    this._bumpEdgeVersions(groupKey, [fromPeerId, ...connected]);
    this.persistTopology();
    this._notifyTopology(groupKey);
    return true;
  }

  pickPeerCenterId(groupKey) {
    const ids = groupKey != null && groupKey !== ''
      ? this.listPeerIdsInGroup(groupKey)
      : this.listAllPeerIds();
    let min = 0;
    for (const id of ids) {
      const n = Number(id);
      if (!n || n === MY_PEER_ID) continue;
      const info = groupKey != null && groupKey !== ''
        ? this._getPeerInfosMap(groupKey, false)?.get(n)
        : undefined;
      if (info && info.featureFlag && info.featureFlag.isPublicServer) continue;
      if (!min || n < min) min = n;
    }
    return min;
  }

  replacePeerCenterMap(groupKey, mapObj, updatedAt = Date.now()) {
    const entries = Object.entries(mapObj || {});
    if (entries.length === 0) return false;
    const m = this._getConnMap(this.peerCenterConns, groupKey, true);
    const nextReporters = new Set();
    const snapshots = [];
    for (const [src, info] of entries) {
      const srcId = Number(src);
      if (!srcId || srcId === MY_PEER_ID) continue;
      nextReporters.add(srcId);
      const rawDirect = (info && (info.directPeers || info.direct_peers)) || {};
      const connected = new Set();
      for (const dst of Object.keys(rawDirect)) {
        const n = Number(dst);
        if (!n || n === srcId || n === MY_PEER_ID) continue;
        connected.add(n);
      }
      snapshots.push([srcId, connected]);
    }
    let changed = false;
    const changedPeerIds = new Set();
    for (const [srcId, connected] of snapshots) {
      this.lastPeerCenterByPeer.set(srcId, updatedAt);
      if (connected.size === 0) {
        if (m.has(srcId)) {
          m.delete(srcId);
          changed = true;
          changedPeerIds.add(srcId);
        }
        continue;
      }
      if (this._writePeerSnapshot(this.peerCenterConns, groupKey, srcId, connected, 0, updatedAt, true)) {
        changed = true;
        changedPeerIds.add(srcId);
        for (const dst of connected) changedPeerIds.add(Number(dst));
      }
    }
    for (const srcId of Array.from(m.keys())) {
      const n = Number(srcId);
      if (nextReporters.has(n)) continue;
      m.delete(n);
      this.lastPeerCenterByPeer.delete(n);
      changed = true;
      changedPeerIds.add(n);
    }
    if (!changed) return false;
    this._bumpEdgeVersions(groupKey, changedPeerIds);
    this.persistTopology();
    this._notifyTopology(groupKey);
    return true;
  }

  dumpPeerCenterGraph(groupKey) {
    const out = {};
    const groups = groupKey != null
      ? [groupKey]
      : Array.from(this.peerCenterConns.keys());
    for (const gk of groups) {
      const m = this._getConnMap(this.peerCenterConns, gk, false);
      if (!m) continue;
      for (const [src, snapshot] of m.entries()) {
        out[String(src)] = Array.from(this._snapshotPeers(snapshot)).sort((a, b) => a - b);
      }
    }
    return out;
  }

  _ingestOspfRows(groupKey, rows, updatedAt = Date.now(), allowUnversionedReplace = false) {
    let changed = false;
    const changedPeerIds = new Set();
    for (const row of rows || []) {
      const peerId = Number(row.peerId);
      if (!Number.isFinite(peerId) || peerId === 0 || peerId === MY_PEER_ID) continue;
      const connected = new Set();
      for (const dst of row.connected || []) {
        const n = Number(dst);
        if (!n || n === peerId || n === MY_PEER_ID) continue;
        connected.add(n);
      }
      // A public-server SyncRouteInfo is usually the star (only the relay).
      // Writing that empty snapshot wipes real P2P learned earlier.
      if (connected.size === 0 && !row.allowEmptyWithdrawal) continue;
      if (this._writePeerSnapshot(
        this.reportedConns,
        groupKey,
        peerId,
        connected,
        row.version,
        updatedAt,
        allowUnversionedReplace,
      )) {
        changed = true;
        changedPeerIds.add(peerId);
        for (const dst of connected) changedPeerIds.add(Number(dst));
      }
    }
    if (!changed) return false;
    this._bumpEdgeVersions(groupKey, changedPeerIds);
    this.persistTopology();
    this._notifyTopology(groupKey);
    return true;
  }

  ingestOspfEdges(groupKey, fromPeerId, edges, version = 0, updatedAt = Date.now()) {
    const connected = this._connectedSetFromEdges(fromPeerId, edges);
    return this._ingestOspfRows(groupKey, [{
      peerId: Number(fromPeerId),
      version,
      connected,
    }], updatedAt, true);
  }

  ingestConnInfo(groupKey, fromPeerId, syncReq, rawSyncBytes) {
    // conn_bitmap is a network-wide LSA and often echoes the relay star.
    // Only the reporter's own row (conn_peer_list, else reporter-incident bits)
    // is treated as a connection snapshot. Star-only rows must not wipe P2P.
    if (hasConnPeerList(rawSyncBytes)) {
      const reporter = Number(fromPeerId);
      const rows = connRowsFromPeerList(parseConnPeerList(rawSyncBytes))
        .filter((row) => Number(row.peerId) === reporter)
        .map((row) => {
          const raw = Array.from(row.connected || []).map(Number);
          const p2p = raw.filter((n) => n && n !== reporter && n !== MY_PEER_ID);
          const starOnly = p2p.length === 0 && raw.some((n) => n === MY_PEER_ID);
          return {
            peerId: reporter,
            version: row.version,
            connected: new Set(p2p),
            allowEmptyWithdrawal: p2p.length === 0 && !starOnly,
          };
        })
        .filter((row) => row.allowEmptyWithdrawal || row.connected.size > 0);
      this._ingestOspfRows(groupKey, rows, Date.now(), true);
    } else if (syncReq && syncReq.connBitmap) {
      const edges = parseConnBitmapEdges(syncReq.connBitmap, fromPeerId);
      const connected = this._connectedSetFromEdges(fromPeerId, edges);
      if (connected.size > 0) {
        this.ingestOspfEdges(groupKey, fromPeerId, edges);
      }
    }
    this.notePeerSync(fromPeerId);
  }

  collectReportedEdges(groupKey, nowTs = Date.now()) {
    const live = new Set(this.listPeerIdsInGroup(groupKey).map(Number));
    live.add(MY_PEER_ID);
    const directed = new Map();
    const addDir = (src, dst) => {
      const a = Number(src);
      const b = Number(dst);
      if (!live.has(a) || !live.has(b) || a === b || a === MY_PEER_ID || b === MY_PEER_ID) return;
      let set = directed.get(a);
      if (!set) {
        set = new Set();
        directed.set(a, set);
      }
      set.add(b);
    };
    const pcMap = this._getConnMap(this.peerCenterConns, groupKey, false);
    // Once a center map exists, OSPF conn_peer_list snapshots are stale
    // leftovers (star LSAs never withdraw P2P). Flooding them recreates
    // dead DIRECT hops. OSPF is only a bootstrap before the first map.
    const stores = (pcMap && pcMap.size > 0)
      ? [this.peerCenterConns]
      : [this.reportedConns];
    for (const store of stores) {
      const m = this._getConnMap(store, groupKey, false);
      if (!m) continue;
      for (const [src, snapshot] of m.entries()) {
        const ttl = store === this.peerCenterConns ? this.peerCenterTtlMs : this.topologyTtlMs;
        const liveReporter = live.has(Number(src));
        // PeerCenter reports stop traversing WSS once P2P to the center exists.
        // Expire only after the reporter itself has left; do not flap live nodes.
        if (!liveReporter && nowTs - this._snapshotUpdatedAt(snapshot) > ttl) continue;
        for (const dst of this._snapshotPeers(snapshot)) addDir(src, dst);
      }
    }
    // One-sided PeerCenter/OSPF rows create fake DIRECT: the other node has
    // no tunnel, overlay ping dies, and a working relay hop is skipped.
    const edges = [];
    for (const [src, dests] of directed.entries()) {
      for (const dst of dests) {
        if (src < dst && directed.get(dst) && directed.get(dst).has(src)) {
          edges.push([src, dst]);
        }
      }
    }
    return edges;
  }

  notePeerSync(peerId) {
    this.lastSyncByPeer.set(Number(peerId), Date.now());
  }

  notePeerCenterPull(peerId) {
    this.peerCenterPulls += 1;
    this.lastPeerCenterPullPeer = Number(peerId) || 0;
  }

  noteRpcError(error) {
    if (error == null) {
      this.lastRpcError = '';
      return;
    }
    if (typeof error === 'string') this.lastRpcError = error;
    else this.lastRpcError = JSON.stringify(error);
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
    this._clearPeerFromTopology(groupKey, peerId, false);
    this.lastSyncByPeer.delete(Number(peerId));
    this.lastPeerCenterByPeer.delete(Number(peerId));
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
    this._clearPeerFromTopology(groupKey, peerId, false);
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
      lastPeerCenter: Object.fromEntries(this.lastPeerCenterByPeer.entries()),
      peerCenterId: this.pickPeerCenterId(),
      peerCenterGraph: this.dumpPeerCenterGraph(),
      reportedEdges: this.collectAllReportedEdges(),
      zstdDecompress: isZstdDecompressAvailable(),
      peerCenterPulls: this.peerCenterPulls,
      lastPeerCenterPullPeer: this.lastPeerCenterPullPeer,
      lastRpcError: this.lastRpcError || '',
    };
  }

  collectAllReportedEdges() {
    const out = [];
    for (const gk of new Set([...this.peerCenterConns.keys(), ...this.reportedConns.keys()])) {
      for (const [a, b] of this.collectReportedEdges(gk)) {
        out.push([a, b]);
      }
    }
    return out;
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
    const now = Date.now();
    let expired = false;
    for (const [peerId, info] of s.globalPeerMap.entries()) {
      if (now - Number(info && info.lastSeen || 0) > this.peerCenterTtlMs) {
        s.globalPeerMap.delete(peerId);
        expired = true;
      }
    }
    if (expired) s.digest = '0';
    s.lastTouch = now;
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
      const version = Math.max(session.foreignNetVer + 1, Math.floor(Date.now() / 1000));
      session.foreignNetVer = version;
      const livePeers = this.listPeerIdsInGroup(groupKey);
      const digest = digestBytesFromGroupKey(groupKey);
      const now = { seconds: Math.floor(Date.now() / 1000), nanos: 0 };
      const infos = [{
        key: {
          peerId: MY_PEER_ID,
          networkName: getPublicServerNetworkName()
        },
        value: {
          foreignPeerIds: livePeers,
          lastUpdate: now,
          version,
          networkSecretDigest: digest,
          myPeerIdForThisNetwork: MY_PEER_ID
        }
      }];
      const staleName = 'dev-websocket-relay';
      if (getPublicServerNetworkName() !== staleName) {
        infos.push({
          key: { peerId: MY_PEER_ID, networkName: staleName },
          value: {
            foreignPeerIds: [],
            lastUpdate: now,
            version,
            networkSecretDigest: Buffer.alloc(32),
            myPeerIdForThisNetwork: MY_PEER_ID
          }
        });
      }
      return { infos };
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

