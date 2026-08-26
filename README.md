# EasyTier WebSocket Relay for Cloudflare Workers

EasyTier 的第三方 WebSocket 公网中继：Cloudflare Worker + Durable Object。
用于节点发现和 P2P 打不成时的 WSS 转发，不是完整的 `easytier-core` 公网节点（没有 UDP/TCP/WG 监听，也不能代发打洞 UDP）。

## 部署

```bash
npm install
npx wrangler login
npx wrangler deploy
```

`wrangler.toml` 里常用变量：

- `WS_PATH`：WebSocket 路径，默认 `ws`
- `EASYTIER_PUBLIC_SERVER_NETWORK_NAME`：握手与 foreign network 名称，默认 `public_server`
- `EASYTIER_COMPRESS_RPC`：默认 `1`（按对端 `acceptedAlgo` 协商 none/zstd，不会把 gzip 标成 zstd）
- `EASYTIER_DEBUG`：`1` 时打印逐包日志
- `EASYTIER_MAX_CONNECTIONS`：单个 Durable Object 连接上限，默认 `256`

## 客户端

EasyTier 里端口 `0` 表示协议默认端口（ws=80，wss=443）。

```text
wss://your-worker.workers.dev:0/ws
```

自定义域名同样加 `/ws`。查询 `/healthz` 返回 `ok`，`/stats` 返回当前房间在线数。

## 开发

```bash
npm test
npx wrangler dev --ip 0.0.0.0
```
