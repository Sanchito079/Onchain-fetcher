# On-Chain Price Fetcher

Real-time Solana price watcher + token-order resolver.  
Reads pool pairs from PostgreSQL and writes live prices back via WebSocket subscriptions.

## Services

| Service | Directory | What it does |
|---------|-----------|--------------|
| **watcher** | `cmd/watcher` | Continuous WebSocket daemon — Solana program-level + account subscribers |
| **resolver** | `cmd/resolve-solana` | Scheduled job — resolves token order for new pools, writes `token_order_cache.json` |

---

## Deploy to Fly.io

### 1. Install flyctl
```bash
curl -L https://fly.io/install.sh | sh
fly auth login
```

### 2. Create a shared volume (once)
Both services share this volume for `token_order_cache.json`:
```bash
fly volumes create watcher_cache --region ord --size 1 --app onchain-watcher
fly volumes create watcher_cache --region ord --size 1 --app onchain-resolver
```

### 3. Deploy the resolver
```bash
fly launch --name onchain-resolver --no-deploy --config cmd/resolve-solana/fly.toml
fly secrets set DATABASE_URL="postgresql://fly-user:PASSWORD@direct.HOST.flympg.net/fly-db" --app onchain-resolver
fly secrets set RPC_ENDPOINT_SOLANA="https://solana-mainnet.core.chainstack.com/YOUR_KEY" --app onchain-resolver
fly deploy --config cmd/resolve-solana/fly.toml
```

### 4. Deploy the watcher
```bash
fly launch --name onchain-watcher --no-deploy --config cmd/watcher/fly.toml
fly secrets set DATABASE_URL="postgresql://fly-user:PASSWORD@direct.HOST.flympg.net/fly-db" --app onchain-watcher
fly secrets set RPC_ENDPOINT_SOLANA="https://solana-mainnet.core.chainstack.com/YOUR_KEY" --app onchain-watcher
fly deploy --config cmd/watcher/fly.toml
```

### 5. Check logs
```bash
fly logs --app onchain-resolver
fly logs --app onchain-watcher
```

---

## Environment Variables

All sensitive values are set via `fly secrets set`. Non-sensitive defaults are in `fly.toml [env]`.

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | ✅ secret | Postgres connection string |
| `RPC_ENDPOINT_SOLANA` | secret/env | Solana HTTP RPC endpoint |
| `RPC_ENDPOINT_BSC` | env | BSC HTTP RPC (watcher only) |
| `RPC_ENDPOINT_BASE` | env | Base HTTP RPC (watcher only) |
| `TOKEN_ORDER_CACHE` | env | Path to JSON cache file (default `/app/cache/token_order_cache.json`) |
| `RESOLVE_INTERVAL` | env | How often resolver re-runs (default `15m`, or `once`) |

---

## Local Development

```bash
export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:55422/postgres?sslmode=disable"
export RPC_ENDPOINT_SOLANA="https://api.mainnet-beta.solana.com/"

# Run resolver once to populate the cache
RESOLVE_INTERVAL=once go run cmd/resolve-solana/main.go

# Start the watcher
go run cmd/watcher/main.go
```
