package pancakeswap

import (
	"math/big"
	"testing"
)

func TestDecodeCLMMSlot0Response(t *testing.T) {
	payload := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	value, err := parseCLMMSlot0Response(payload)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value == nil || value.Sign() == 0 {
		t.Fatal("expected a non-zero sqrt price")
	}
	if got := value.Text(16); got != "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef" {
		t.Fatalf("unexpected parsed value: %s", got)
	}
}

func TestCalculateCLMMPrice(t *testing.T) {
	sqrtPriceX96, ok := new(big.Int).SetString("1132462085230717978043491120000", 10)
	if !ok {
		t.Fatal("failed to parse sqrtPriceX96")
	}
	price := calculateCLMMPrice(sqrtPriceX96, 18, 18, true)
	if price <= 0 {
		t.Fatalf("expected positive CLMM price, got %v", price)
	}
}
