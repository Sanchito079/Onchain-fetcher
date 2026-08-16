package shared

import (
    "math"
    "testing"
)

func TestChooseSanePricePrefersReasonableMagnitude(t *testing.T) {
    direct := 9.95e24
    inverted := 0.10050886447441296

    got := ChooseSanePrice(direct, inverted)
    if math.Abs(got-inverted) > 1e-12 {
        t.Fatalf("expected to choose the reasonable price candidate, got %v", got)
    }
}

func TestChooseSanePricePrefersSmallerSaneCandidateForAmbiguousV4Pairs(t *testing.T) {
    got := ChooseSanePrice(9.949, 0.10050886447441296)
    if got != 0.10050886447441296 {
        t.Fatalf("expected the smaller sane candidate for ambiguous V4 pricing, got %v", got)
    }
}

func TestChooseSanePriceFallsBackToFirstFinitePositiveWhenAllAreSane(t *testing.T) {
    got := ChooseSanePrice(1.2, 3.4)
    if got != 1.2 {
        t.Fatalf("expected first sane candidate, got %v", got)
    }
}
