// Cloudflare Worker entry for EasyTier WebSocket relay backed by Durable Object
import { RelayRoom } from './worker/relay_room';
import { getWsPath } from './worker/core/env.js';

export { RelayRoom };

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const { pathname, searchParams } = url;

    if (pathname === '/healthz') {
      return new Response('ok', { status: 200 });
    }

    if (pathname === '/stats') {
      const roomId = searchParams.get('room') || 'default-v2';
      const roomStub = env.RELAY_ROOM.get(env.RELAY_ROOM.idFromName(roomId));
      return roomStub.fetch(request);
    }

    const wsPath = getWsPath(env);
    if (pathname === wsPath || pathname === wsPath + '/') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected WebSocket upgrade', { status: 400 });
      }

      const roomId = searchParams.get('room') || 'default-v2';
      const roomStub = env.RELAY_ROOM.get(env.RELAY_ROOM.idFromName(roomId));
      return roomStub.fetch(request);
    }

    return new Response('Not found', { status: 404 });
  },
};
