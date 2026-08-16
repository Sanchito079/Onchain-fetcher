package pancakeswap

import (
	"fmt"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// V3PoolQuery builds the calldata-style query plan used for PancakeSwap V3 pools.
type V3PoolQuery struct{}

func (q V3PoolQuery) Query(pair shared.Pair) string {
	return fmt.Sprintf("pancakeswap-v3:slot0:%s", pair.PoolAddress)
}

func (q V3PoolQuery) Description() string {
	return "PancakeSwap V3 slot0 lookup"
}
