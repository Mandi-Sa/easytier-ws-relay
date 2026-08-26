const WORKER_STRING_VARS = [
  'WS_PATH',
  'EASYTIER_DISABLE_RELAY',
  'EASYTIER_COMPRESS_RPC',
  'EASYTIER_PUBLIC_SERVER_NETWORK_NAME',
  'EASYTIER_HOSTNAME',
  'EASYTIER_VERSION',
  'EASYTIER_NETWORK_LENGTH',
  'EASYTIER_IPV4_ADDR',
  'EASYTIER_AUTO_IPV4_ADDR',
  'EASYTIER_HANDSHAKE_MODE',
  'EASYTIER_OSPF_ROUTE_METHOD_INDEX',
  'EASYTIER_SESSION_TTL_MS',
  'EASYTIER_PEER_CENTER_TTL_MS',
  'EASYTIER_DEBUG',
  'EASYTIER_MAX_CONNECTIONS',
];

export function applyWorkerEnv(env) {
  if (!env) return;
  for (const key of WORKER_STRING_VARS) {
    const value = env[key];
    if (typeof value === 'string') {
      process.env[key] = value;
    }
  }
}

export function getWsPath(env) {
  const raw = (env && env.WS_PATH) || process.env.WS_PATH || 'ws';
  return '/' + String(raw).replace(/^\/+/, '');
}

export function getPublicServerNetworkName() {
  return process.env.EASYTIER_PUBLIC_SERVER_NETWORK_NAME || 'public_server';
}

export function persistSocketMeta(ws) {
  if (!ws || typeof ws.serializeAttachment !== 'function') return;
  ws.serializeAttachment({
    peerId: ws.peerId || null,
    groupKey: ws.groupKey || null,
    domainName: ws.domainName || null,
    serverSessionId: ws.serverSessionId || null,
  });
}

export function debugEnabled() {
  return process.env.EASYTIER_DEBUG === '1';
}

export function debugLog(...args) {
  if (debugEnabled()) console.log(...args);
}

export function connectionLimit(env) {
  const raw = (env && env.EASYTIER_MAX_CONNECTIONS) || process.env.EASYTIER_MAX_CONNECTIONS || '256';
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : 256;
}

export function shouldAcceptConnection(current, max) {
  return Number(current) < Number(max);
}
