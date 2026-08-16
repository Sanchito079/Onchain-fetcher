package main

import (
    "testing"

    "on-chain-price-fetcher/internal/adapters/pumpswap"
    "on-chain-price-fetcher/internal/adapters/shared"
)

func TestBuildAdapterReturnsPumpSwapAdapter(t *testing.T) {
    pair := shared.Pair{
        Network: "solana",
        DexName: "PumpSwap",
    }

    adapter, err := buildAdapter(pair, pair.Network)
    if err != nil {
        t.Fatalf("expected adapter builder to succeed, got error: %v", err)
    }

    switch adapter.(type) {
    case pumpswap.Adapter:
        // expected
    default:
        t.Fatalf("expected pumpswap.Adapter, got %T", adapter)
    }
}
