package uniswap

import (
    "testing"

    "on-chain-price-fetcher/internal/adapters/shared"
)

func TestRouterSelectsV3ForUniswapBSCGenericDexName(t *testing.T) {
    r := Router{}
    pair := shared.Pair{Network: "bsc", DexName: "uniswap-bsc"}

    if got := r.Select(pair); got != "v3" {
        t.Fatalf("expected v3 for uniswap-bsc rows, got %q", got)
    }
}
