package slipstream

import "testing"

func TestChooseSanePricePrefersFinitePositiveValues(t *testing.T) {
	price := chooseSanePrice(1e20, 0.5, 2e-8)
	if price != 0.5 {
		t.Fatalf("expected 0.5, got %v", price)
	}
}
