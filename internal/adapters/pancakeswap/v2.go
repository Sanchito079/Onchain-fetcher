package pancakeswap

import (
	"fmt"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// V2PoolQuery builds the calldata-style query plan used for PancakeSwap V2 pools.
type V2PoolQuery struct{}

func (q V2PoolQuery) Query(pair shared.Pair) string {
	return fmt.Sprintf("pancakeswap-v2:getReserves:%s:%s", pair.PoolAddress, pair.DexName)
}

func (q V2PoolQuery) Description() string {
	return "PancakeSwap V2 reserve lookup"
}
