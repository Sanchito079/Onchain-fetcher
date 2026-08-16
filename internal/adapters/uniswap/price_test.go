package uniswap

import (
    "math"
    "math/big"
    "testing"
)

func TestCalculateV3PriceUsesQ192Normalization(t *testing.T) {
    sqrtPriceX96, ok := new(big.Int).SetString("112045541949572287496682733568", 10)
    if !ok {
        t.Fatal("failed to parse sqrtPriceX96")
    }

    price := calculateV3Price(sqrtPriceX96, 18, 18, true)
    if math.Abs(price-2.0) > 1e-12 {
        t.Fatalf("expected price near 2.0, got %v", price)
    }
}
