package shared

import (
    "math/big"
)

var one128 = new(big.Int).Lsh(big.NewInt(1), 128)
var one192 = new(big.Int).Lsh(big.NewInt(1), 192)

// ConvertSqrtPriceX96ToPrice converts a Uniswap/Pancake-style sqrtPriceX96
// value (as returned by slot0) into a *big.Rat representing token1/token0
// before decimal adjustments. Returns nil for nil/zero input.
func ConvertSqrtPriceX96ToPrice(sqrtPriceX96 *big.Int) *big.Rat {
    if sqrtPriceX96 == nil || sqrtPriceX96.Sign() == 0 {
        return nil
    }
    r := new(big.Rat).SetInt(sqrtPriceX96)
    r.Mul(r, r) // square the sqrt price
    r.Quo(r, new(big.Rat).SetInt(one192))
    return r
}

// ConvertSqrtPriceX64ToPrice converts a DAMM v2-style sqrtPrice_x64
// value into a *big.Rat representing token1/token0 before decimal adjustments.
// Returns nil for nil/zero input.
func ConvertSqrtPriceX64ToPrice(sqrtPriceX64 *big.Int) *big.Rat {
    if sqrtPriceX64 == nil || sqrtPriceX64.Sign() == 0 {
        return nil
    }
    r := new(big.Rat).SetInt(sqrtPriceX64)
    r.Mul(r, r)
    r.Quo(r, new(big.Rat).SetInt(one128))
    return r
}

// ApplyDecimalAdjustments multiplies or divides the provided price by 10^(decimal0-decimal1)
// so the returned rational expresses the price using the canonical token decimals.
// If price is nil, returns nil.
func ApplyDecimalAdjustments(price *big.Rat, decimal0, decimal1 int) *big.Rat {
    if price == nil {
        return nil
    }
    delta := decimal0 - decimal1
    if delta == 0 {
        return new(big.Rat).Set(price)
    }
    if delta > 0 {
        multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(delta)), nil)
        return new(big.Rat).Mul(price, new(big.Rat).SetInt(multiplier))
    }
    // delta < 0
    divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-delta)), nil)
    return new(big.Rat).Quo(price, new(big.Rat).SetInt(divisor))
}

// ChoosePrice accepts two price candidates (direct and inverted) as *big.Rat and
// returns a float64 chosen by the existing ChooseSanePrice logic. If a candidate
// is nil it is ignored. This keeps selection consistent across adapters.
func ChoosePrice(direct, inverted *big.Rat) float64 {
    var d, i float64
    if direct != nil {
        d, _ = direct.Float64()
    }
    if inverted != nil {
        i, _ = inverted.Float64()
    }
    return ChooseSanePrice(d, i)
}
