# ── Build stage ──────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy module files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy full source
COPY . .

# Build the watcher binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /watcher ./cmd/watcher

# ── Runtime stage ─────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /watcher /app/watcher

# token_order_cache.json is written here at runtime
VOLUME ["/app/cache"]

ENTRYPOINT ["/app/watcher"]
