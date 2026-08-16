# Root Dockerfile — builds BOTH binaries into a single image.
# The fly.toml [processes] section picks which one to run.
#
#   /app/watcher   — continuous price watcher daemon
#   /app/resolver  — scheduled token-order resolver

# ── Build stage ──────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Cache module downloads separately from source changes
COPY go.mod go.sum ./
RUN go mod download

# Copy full source tree
COPY . .

# Build both binaries in one layer
RUN CGO_ENABLED=0 GOOS=linux go build -o /watcher  ./cmd/watcher       && \
    CGO_ENABLED=0 GOOS=linux go build -o /resolver ./cmd/resolve-solana

# ── Runtime stage ─────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /watcher  /app/watcher
COPY --from=builder /resolver /app/resolver

# Shared volume for token_order_cache.json
VOLUME ["/app/cache"]

# Default: run the watcher.
# fly.toml [processes] overrides this per process group.
CMD ["/app/watcher"]
