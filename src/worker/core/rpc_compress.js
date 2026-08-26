// EasyTier CompressionAlgoPb: Invalid=0, None=1, Zstd=2.
// Cloudflare Workers can gzip, but EasyTier clients only decode none/zstd.
// Never advertise gzip as zstd — that makes sync_route_info time out.

export const RPC_COMPRESSION_NONE = 1;
export const RPC_COMPRESSION_ZSTD = 2;

export function negotiateRpcCompression(bodyBytes) {
  const body = bodyBytes;
  return {
    body,
    compressionInfo: {
      algo: RPC_COMPRESSION_NONE,
      acceptedAlgo: RPC_COMPRESSION_NONE,
    },
  };
}
