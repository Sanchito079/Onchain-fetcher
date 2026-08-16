package sync

import "testing"

func TestIsExtremePriceValue(t *testing.T) {
	cases := []struct {
		name  string
		price float64
		want  bool
	}{
		{name: "zero", price: 0, want: true},
		{name: "negative", price: -1, want: true},
		{name: "tiny", price: 1e-8, want: false},
		{name: "normal", price: 42.5, want: false},
		{name: "too large", price: 1e12, want: true},
	}

	for _, tc := range cases {
		if got := isExtremePriceValue(tc.price); got != tc.want {
			t.Fatalf("%s: expected %v, got %v", tc.name, tc.want, got)
		}
	}
}
