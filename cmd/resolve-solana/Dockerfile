# ── Build stage ──────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the resolver binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /resolver ./cmd/resolve-solana

# ── Runtime stage ─────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /resolver /app/resolver

# Shared cache volume — mount the same volume as the watcher
VOLUME ["/app/cache"]

ENTRYPOINT ["/app/resolver"]
