package slipstream

import (
	"math"
	"math/big"
)

var one96 = new(big.Int).Lsh(big.NewInt(1), 96)
var one192 = new(big.Int).Lsh(big.NewInt(1), 192)

func chooseSanePrice(candidates ...float64) float64 {
	var best float64
	var found bool
	for _, candidate := range candidates {
		if !isFinitePrice(candidate) || candidate <= 0 {
			continue
		}
		if !found {
			best = candidate
			found = true
			continue
		}
		if math.Abs(candidate) >= 1e-6 && math.Abs(candidate) <= 1e12 {
			if math.Abs(candidate) < math.Abs(best) || math.Abs(best) < 1e-6 || math.Abs(best) > 1e12 {
				best = candidate
			}
			continue
		}
		if math.Abs(best) < 1e-6 || math.Abs(best) > 1e12 {
			best = candidate
		}
	}
	if !found {
		return 0
	}
	return best
}

func isFinitePrice(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func calculateV2Price(reserve0, reserve1 *big.Int, decimal0, decimal1 int, token0IsBase bool) float64 {
	if reserve0 == nil || reserve1 == nil {
		return 0
	}
	if reserve0.Sign() == 0 || reserve1.Sign() == 0 {
		return 0
	}

	res0 := new(big.Float).SetInt(reserve0)
	res1 := new(big.Float).SetInt(reserve1)
	dec0 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimal0)), nil))
	dec1 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimal1)), nil))

	priceFloat := new(big.Float).Quo(new(big.Float).Quo(res1, dec1), new(big.Float).Quo(res0, dec0))
	price, _ := priceFloat.Float64()
	if !token0IsBase {
		price = 1 / price
	}
	return price
}

func calculateV3Price(sqrtPriceX96 *big.Int, decimal0, decimal1 int, token0IsBase bool) float64 {
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
