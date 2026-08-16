package pancakeswap

import (
	"fmt"
	"math/big"
	"strings"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// CLMMQuery builds the query plan for PancakeSwap Infinity CLMM pools.
type CLMMQuery struct{}

func (q CLMMQuery) Query(pair shared.Pair) string {
	return fmt.Sprintf("pancakeswap-infinity-clmm:getSlot0:%s", pair.PoolAddress)
}

func (q CLMMQuery) Description() string {
	return "PancakeSwap Infinity CLMM slot0 lookup"
}

func parseCLMMSlot0Response(raw string) (*big.Int, error) {
	trimmed := strings.TrimPrefix(raw, "0x")
	if len(trimmed) < 64 {
		return nil, fmt.Errorf("invalid clmm response")
	}
	value := new(big.Int)
	_, ok := value.SetString(trimmed[:64], 16)
	if !ok {
		return nil, fmt.Errorf("invalid clmm response")
	}
	return value, nil
}

func calculateCLMMPrice(sqrtPriceX96 *big.Int, decimal0, decimal1 int, token0IsBase bool) float64 {
	if sqrtPriceX96 == nil || sqrtPriceX96.Sign() == 0 {
		return 0
	}

	price := new(big.Rat).SetInt(sqrtPriceX96)
	price.Mul(price, price)
	price.Quo(price, new(big.Rat).SetInt(one192))

	delta := decimal0 - decimal1
	if delta > 0 {
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(delta)), nil)
		price.Mul(price, new(big.Rat).SetInt(multiplier))
	} else if delta < 0 {
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-delta)), nil)
		price.Quo(price, new(big.Rat).SetInt(divisor))
	}

	value, _ := price.Float64()
	if !token0IsBase {
		value = 1 / value
	}
	return value
}
