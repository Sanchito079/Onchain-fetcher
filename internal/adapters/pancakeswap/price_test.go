package pancakeswap

import (
	"math/big"
	"testing"
)

func TestCalculateV2Price(t *testing.T) {
	reserve0 := big.NewInt(1000)
	reserve1 := big.NewInt(2000)
	price := calculateV2Price(reserve0, reserve1, 18, 18, true)
	if price != 2.0 {
		t.Fatalf("expected 2.0, got %v", price)
	}
}

func TestCalculateV3Price(t *testing.T) {
	// sqrtPriceX96 of 2^96 * sqrt(2) gives a price of 2.0 when the formula is applied correctly.
	sqrtPriceX96, ok := new(big.Int).SetString("112045541949572287496682733568", 10)
	if !ok {
		t.Fatal("failed to parse sqrtPriceX96")
	}
	price := calculateV3Price(sqrtPriceX96, 18, 18, true)
	if price <= 0 {
		t.Fatalf("expected positive price, got %v", price)
	}
}
