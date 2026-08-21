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
    // When both candidates are in the sane range, ChooseSanePrice returns the
    // FIRST candidate. The caller is responsible for passing the candidates in
    // the correct order (correct orientation first) — ChooseSanePrice no longer
    // applies a magnitude heuristic because picking smaller is not reliably correct
    // (e.g. ETH/USDT: 3500 is the right price, 0.000286 is the wrong inversion).
    got := ChooseSanePrice(9.949, 0.10050886447441296)
    if got != 9.949 {
        t.Fatalf("expected first sane candidate (9.949), got %v", got)
    }
    // Verify order matters: passing the smaller one first returns the smaller one.
    got2 := ChooseSanePrice(0.10050886447441296, 9.949)
    if got2 != 0.10050886447441296 {
        t.Fatalf("expected first sane candidate (0.100...) when passed first, got %v", got2)
    }
}

func TestChooseSanePriceFallsBackToFirstFinitePositiveWhenAllAreSane(t *testing.T) {
    got := ChooseSanePrice(1.2, 3.4)
    if got != 1.2 {
        t.Fatalf("expected first sane candidate, got %v", got)
    }
}
