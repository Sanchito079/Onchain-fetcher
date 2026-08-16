package pancakeswap

import (
	"testing"

	"on-chain-price-fetcher/internal/adapters/shared"
)

func TestSupportsPancakeSwapPairs(t *testing.T) {
	a := Adapter{}
	pair := shared.Pair{ID: "1", DexName: "pancakeswap v2", PoolAddress: "0xabc"}
	if !a.Supports(pair) {
		t.Fatalf("expected PancakeSwap pair to be supported")
	}
}

func TestRouterSelectsStrategy(t *testing.T) {
	r := Router{}
	cases := []struct {
		name    string
		dex     string
		network string
		want    string
	}{
		{name: "v2", dex: "pancakeswap v2", network: "bsc", want: "v2"},
		{name: "v3", dex: "pancakeswap v3", network: "bsc", want: "v3"},
		{name: "v3-base", dex: "pancakeswap v3", network: "base", want: "v3-base"},
		{name: "clmm", dex: "pancakeswap infinity clmm", network: "bsc", want: "clmm"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pair := shared.Pair{DexName: tc.dex, Network: tc.network}
			if got := r.Select(pair); got != tc.want {
				t.Fatalf("router selected %q, want %q", got, tc.want)
			}
		})
	}
}
