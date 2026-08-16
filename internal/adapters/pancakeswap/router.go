package pancakeswap

import (
	"strings"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// Router selects the correct PancakeSwap query strategy based on the pair metadata.
type Router struct{}

func (r Router) Select(pair shared.Pair) string {
	dex := strings.ToLower(pair.DexName)
	network := strings.ToLower(pair.Network)

	switch {
	case strings.Contains(dex, "clmm") || strings.Contains(dex, "infinity"):
		return "clmm"
	case strings.Contains(dex, "v3") && network == "base":
		return "v3-base"
	case strings.Contains(dex, "v3"):
		return "v3"
	default:
		return "v2"
	}
}
