import zlib from 'zlib';
import { RPC_COMPRESSION_NONE, RPC_COMPRESSION_ZSTD } from './rpc_compress.js';

const hasZlib = !!(zlib && typeof zlib.gzipSync === 'function' && typeof zlib.gunzipSync === 'function');
const hasZstdDecompress = !!(zlib && typeof zlib.zstdDecompressSync === 'function');
const hasZstdCompress = !!(zlib && typeof zlib.zstdCompressSync === 'function');

export function gzipMaybe(data) {
  if (hasZlib) {
    return zlib.gzipSync(data);
  }
  return data;
}

export function gunzipMaybe(data) {
  if (hasZlib) {
    return zlib.gunzipSync(data);
  }
  return data;
}

export function isCompressionAvailable() {
  return hasZlib;
}

export function isZstdAvailable() {
  return hasZstdCompress && hasZstdDecompress;
}

export function compressRpcBody(body, algo) {
  const a = Number(algo || RPC_COMPRESSION_NONE);
  if (!body || a <= RPC_COMPRESSION_NONE) return body;
  if (a === RPC_COMPRESSION_ZSTD) {
    if (!hasZstdCompress) throw new Error('zstd unavailable');
    return zlib.zstdCompressSync(body);
  }
  throw new Error(`unsupported rpc compression algo ${a}`);
}

export function decompressRpcBody(body, algo) {
  const a = Number(algo || RPC_COMPRESSION_NONE);
  if (!body || a <= RPC_COMPRESSION_NONE) {
    return body;
  }
  if (a === RPC_COMPRESSION_ZSTD) {
    if (!hasZstdDecompress) {
      throw new Error('zstd unavailable');
    }
    return zlib.zstdDecompressSync(body);
  }
  throw new Error(`unsupported rpc compression algo ${a}`);
}
