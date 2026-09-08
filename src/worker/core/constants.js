export const MAGIC = 0xd1e1a5e1;
export const VERSION = 1;
export const MY_PEER_ID = 10000001; // Server Peer ID
export const HEADER_SIZE = 16;

export const PacketType = {
  Invalid: 0,
  Data: 1,
  HandShake: 2,
  RoutePacket: 3, // deprecated
  Ping: 4,
  Pong: 5,
  TaRpc: 6, // deprecated
  Route: 7, // deprecated
  RpcReq: 8,
  RpcResp: 9,
  ForeignNetworkPacket: 10,
  KcpSrc: 11,
  KcpDst: 12,
};

// easytier-proto/build/rpc.rs assigns method indices as (enumerate + 1).
export const RpcMethod = {
  OspfSyncRouteInfo: 1,
  DirectConnectorGetIpList: 1,
  PeerCenterReportPeers: 1,
  PeerCenterGetGlobalPeerMap: 2,
};
