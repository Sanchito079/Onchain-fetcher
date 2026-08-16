# Root Dockerfile — works for both the watcher and the resolver.
# Select which binary to build via the BUILD_TARGET build arg:
#
#   watcher:  BUILD_TARGET=cmd/watcher
#   resolver: BUILD_TARGET=cmd/resolve-solana
#
# This is set in each app's fly.toml [build.args] section.

ARG BUILD_TARGET=cmd/watcher

# ── Build stage ──────────────────────────────────────────
FROM golang:1.22-alpine AS builder

ARG BUILD_TARGET

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Cache dependency downloads separately from source changes
COPY go.mod go.sum ./
RUN go mod download

# Copy full source tree (all internal packages needed)
COPY . .

# Build the selected binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /run-app ./${BUILD_TARGET}

# ── Runtime stage ─────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /run-app /app/run-app

# Shared volume for token_order_cache.json
VOLUME ["/app/cache"]

ENTRYPOINT ["/app/run-app"]
