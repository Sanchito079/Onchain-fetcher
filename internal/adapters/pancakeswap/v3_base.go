package pancakeswap

import (
	"fmt"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// V3BaseQuery builds the query plan for PancakeSwap V3 pools on Base.
type V3BaseQuery struct{}

func (q V3BaseQuery) Query(pair shared.Pair) string {
	return fmt.Sprintf("pancakeswap-v3-base:slot0:%s", pair.PoolAddress)
}

func (q V3BaseQuery) Description() string {
	return "PancakeSwap V3 Base slot0 lookup"
}
