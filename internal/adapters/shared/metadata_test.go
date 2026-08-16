package shared

import "testing"

func TestResolveDecimalsPrefersOnChainValue(t *testing.T) {
	if got := ResolveDecimals(6, 18); got != 6 {
		t.Fatalf("expected on-chain decimals to override db decimals, got %d", got)
	}
}

func TestResolveDecimalsFallsBackToDatabaseValue(t *testing.T) {
	if got := ResolveDecimals(6, 0); got != 6 {
		t.Fatalf("expected database decimals to be used when on-chain decimals are missing, got %d", got)
	}
}

func TestResolveTokenPairUsesOnChainAddressesWhenDatabaseValuesAreMissing(t *testing.T) {
	base, quote := ResolveTokenPair("", "", "0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222")
	if base != "0x1111111111111111111111111111111111111111" {
		t.Fatalf("expected base token to fall back to on-chain address, got %s", base)
	}
	if quote != "0x2222222222222222222222222222222222222222" {
		t.Fatalf("expected quote token to fall back to on-chain address, got %s", quote)
	}
}
