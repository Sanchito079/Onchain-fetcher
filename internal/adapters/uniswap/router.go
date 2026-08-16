package uniswap

import (
    "strings"
    "on-chain-price-fetcher/internal/adapters/shared"
)

type Router struct{}

func (r Router) Select(pair shared.Pair) string {
    dex := strings.ToLower(pair.DexName)
    network := strings.ToLower(pair.Network)

    switch {
    case strings.Contains(dex, "v4"):
        return "v4"
    case strings.Contains(dex, "v3"):
        return "v3"
    case strings.Contains(dex, "uniswap") && strings.EqualFold(network, "bsc"):
        return "v3"
    default:
        return "v2"
    }
}
