import { compressRpcBody, isZstdAvailable } from './compress.js';

export const RPC_COMPRESSION_NONE = 1;
export const RPC_COMPRESSION_ZSTD = 2;

export function negotiateRpcCompression(bodyBytes, acceptedAlgo) {
  const body = bodyBytes;
  const accept = Number(acceptedAlgo || RPC_COMPRESSION_NONE);
  const canZstd = isZstdAvailable();
  const accepted = canZstd ? RPC_COMPRESSION_ZSTD : RPC_COMPRESSION_NONE;
  if (canZstd && accept >= RPC_COMPRESSION_ZSTD && body && body.length > 256) {
    try {
      const compressed = compressRpcBody(body, RPC_COMPRESSION_ZSTD);
      if (compressed && compressed.length < body.length) {
        return {
          body: compressed,
          compressionInfo: { algo: RPC_COMPRESSION_ZSTD, acceptedAlgo: accepted },
        };
      }
    } catch (_) {
      // fall through to none
    }
  }
  return {
    body,
    compressionInfo: { algo: RPC_COMPRESSION_NONE, acceptedAlgo: accepted },
  };
}
