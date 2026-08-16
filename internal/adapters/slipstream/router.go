package slipstream

import (
	"strings"

	"on-chain-price-fetcher/internal/adapters/shared"
)

type Router struct{}

func (r Router) Select(pair shared.Pair) string {
	dex := strings.ToLower(pair.DexName)
	poolType := strings.ToLower(strings.TrimSpace(pair.DexName))

	switch {
	case strings.Contains(dex, "aerodrome-base"):
		return "reserves"
	case strings.Contains(dex, "aerodrome-slipstream") || strings.Contains(dex, "slipstream") || strings.Contains(poolType, "slipstream-3"):
		return "slot0"
	default:
		return "reserves"
	}
}
