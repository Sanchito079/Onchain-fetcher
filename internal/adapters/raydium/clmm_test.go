package raydium

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"testing"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// Test that parseCurveReserves values produce the expected normalized price
// when passed through calculateSolanaPrice with given decimals.
func TestCurveReservesPriceCalculation(t *testing.T) {
	// base amount = 1_000_000 (e.g., 6 decimals), quote amount = 35_000_633 (price ~35.000633)
	poolRaw := make([]byte, 16)
	baseAmt := uint64(1000000)
	quoteAmt := uint64(35000633)
	binaryLittleEndianPutUint64(poolRaw[0:8], baseAmt)
	binaryLittleEndianPutUint64(poolRaw[8:16], quoteAmt)

	baseBal, quoteBal, ok := parseCurveReserves(poolRaw)
	if !ok {
		t.Fatalf("expected parseCurveReserves to succeed")
	}

	// both tokens use 6 decimals here
	price := calculateSolanaPrice(baseBal, quoteBal, 6, 6)
	expected := float64(quoteAmt) / float64(baseAmt)
	if math.Abs(price-expected) > 1e-9 {
		t.Fatalf("price mismatch: got %g expected %g", price, expected)
	}
}

// Test sqrtPriceX96 -> price conversion and decimal adjustment using shared helpers.
func TestSqrtPriceX96ConversionAndDecimalAdjustments(t *testing.T) {
	// Known sqrtPriceX96 that yields price 2.0 when decimals equal (from other adapters/tests)
	sqrtPriceX96, ok := new(big.Int).SetString("112045541949572287496682733568", 10)
	if !ok {
		t.Fatal("failed to parse sqrtPriceX96")
	}

	rat := shared.ConvertSqrtPriceX96ToPrice(sqrtPriceX96)
	if rat == nil {
		t.Fatal("ConvertSqrtPriceX96ToPrice returned nil")
	}
	f, _ := rat.Float64()
	if math.Abs(f-2.0) > 1e-12 {
		t.Fatalf("expected raw price near 2.0, got %v", f)
	}

	// apply decimal adjustment: token0 decimals 6, token1 decimals 8 -> price scaled by 10^(6-8)=1/100
	adj := shared.ApplyDecimalAdjustments(rat, 6, 8)
	af, _ := adj.Float64()
	if math.Abs(af-(2.0/100.0)) > 1e-12 {
		t.Fatalf("decimal-adjusted price mismatch: got %g expected %g", af, 2.0/100.0)
	}
}

func TestParseCLMMPriceReadsSqrtPriceField(t *testing.T) {
	sqrtPriceX96, ok := new(big.Int).SetString("112045541949572287496682733568", 10)
	if !ok {
		t.Fatal("failed to parse sqrtPriceX96")
	}

	raw := make([]byte, 512)
	valueBytes := sqrtPriceX96.Bytes()
	for i := 0; i < len(valueBytes); i++ {
		raw[192+i] = valueBytes[len(valueBytes)-1-i]
	}

	price, ok := parseCLMMPrice(raw, "base", "quote", 6, 6, nil, false, true)
	if !ok {
		t.Fatal("expected parseCLMMPrice to find a valid sqrt-price field")
	}
	if math.Abs(price-2.0) > 1e-6 && math.Abs(price-0.5) > 1e-6 {
		t.Fatalf("expected CLMM price near 2.0 or 0.5, got %g", price)
	}
}

func TestParseCLMMPriceRespectsToken0Order(t *testing.T) {
	sqrtPriceX96, ok := new(big.Int).SetString("112045541949572287496682733568", 10)
	if !ok {
		t.Fatal("failed to parse sqrtPriceX96")
	}

	raw := make([]byte, 512)
	valueBytes := sqrtPriceX96.Bytes()
	for i := 0; i < len(valueBytes); i++ {
		raw[192+i] = valueBytes[len(valueBytes)-1-i]
	}

	price, ok := parseCLMMPrice(raw, "base", "quote", 6, 6, nil, true, true)
	if !ok {
		t.Fatal("expected parseCLMMPrice to find a valid sqrt-price field")
	}
	if math.Abs(price-2.0) > 1e-6 {
		t.Fatalf("expected price 2.0 when token0 is base, got %g", price)
	}

	invertedPrice, ok := parseCLMMPrice(raw, "base", "quote", 6, 6, nil, true, false)
	if !ok {
		t.Fatal("expected parseCLMMPrice to find a valid sqrt-price field")
	}
	if math.Abs(invertedPrice-0.5) > 1e-6 {
		t.Fatalf("expected price 0.5 when token0 is quote, got %g", invertedPrice)
	}
}

func TestParseCLMMPoolStateReadsKnownRaydiumLayout(t *testing.T) {
	raw := make([]byte, clmmSqrtPriceX64Offset+16)
	token0 := bytes.Repeat([]byte{1}, 32)
	token1 := bytes.Repeat([]byte{2}, 32)
	copy(raw[clmmTokenMint0Offset:clmmTokenMint0Offset+32], token0)
	copy(raw[clmmTokenMint1Offset:clmmTokenMint1Offset+32], token1)
	raw[clmmDecimals0Offset] = 9
	raw[clmmDecimals1Offset] = 6
	sqrtPrice := new(big.Int).SetUint64(99676455149125488)
	sqrtBytes := sqrtPrice.FillBytes(make([]byte, 16))
	for i := 0; i < 16; i++ {
		raw[clmmSqrtPriceX64Offset+i] = sqrtBytes[15-i]
	}

	state, ok := parseCLMMPoolState(raw)
	if !ok {
		t.Fatal("expected parseCLMMPoolState to succeed")
	}
	if state.TokenMint0 != encodeBase58(token0) {
		t.Fatalf("unexpected token0 mint: %s", state.TokenMint0)
	}
	if state.TokenMint1 != encodeBase58(token1) {
		t.Fatalf("unexpected token1 mint: %s", state.TokenMint1)
	}
	if state.Decimals0 != 9 || state.Decimals1 != 6 {
		t.Fatalf("unexpected decimals: %d/%d", state.Decimals0, state.Decimals1)
	}
	if state.SqrtPriceX64.Cmp(sqrtPrice) != 0 {
		t.Fatalf("unexpected sqrt price: %s", state.SqrtPriceX64.String())
	}
}

func TestParseCLMMPoolStateReadsAlternateRaydiumLayout(t *testing.T) {
	raw := make([]byte, clmmSqrtPriceX64Offset+16)
	token0 := bytes.Repeat([]byte{1}, 32)
	token1 := bytes.Repeat([]byte{2}, 32)
	copy(raw[clmmTokenMint0Offset:clmmTokenMint0Offset+32], token0)
	copy(raw[clmmTokenMint1Offset:clmmTokenMint1Offset+32], token1)
	raw[clmmDecimals0Offset] = 9
	raw[clmmDecimals1Offset] = 6
	sqrtPrice := new(big.Int).SetUint64(99676455149125488)
	sqrtBytes := sqrtPrice.FillBytes(make([]byte, 16))
	for i := 0; i < 16; i++ {
		raw[clmmSqrtPriceX64AltOffset+i] = sqrtBytes[15-i]
	}

	state, ok := parseCLMMPoolState(raw)
	if !ok {
		t.Fatal("expected parseCLMMPoolState to succeed for alternate layout")
	}
	if state.TokenMint0 != encodeBase58(token0) {
		t.Fatalf("unexpected token0 mint: %s", state.TokenMint0)
	}
	if state.TokenMint1 != encodeBase58(token1) {
		t.Fatalf("unexpected token1 mint: %s", state.TokenMint1)
	}
	if state.Decimals0 != 9 || state.Decimals1 != 6 {
		t.Fatalf("unexpected decimals: %d/%d", state.Decimals0, state.Decimals1)
	}
	if state.SqrtPriceX64.Cmp(sqrtPrice) != 0 {
		t.Fatalf("unexpected sqrt price for alternate layout: %s", state.SqrtPriceX64.String())
	}
}

func TestParseCLMMPoolStatePrefersLiveOffsetOverBogusNearByCandidate(t *testing.T) {
	raw := make([]byte, 1024)
	token0 := bytes.Repeat([]byte{1}, 32)
	token1 := bytes.Repeat([]byte{2}, 32)
	copy(raw[clmmTokenMint0Offset:clmmTokenMint0Offset+32], token0)
	copy(raw[clmmTokenMint1Offset:clmmTokenMint1Offset+32], token1)
	raw[clmmDecimals0Offset] = 9
	raw[clmmDecimals1Offset] = 6

	bogus, ok := new(big.Int).SetString("25421006255315480477696", 10)
	if !ok {
		t.Fatal("failed to parse bogus candidate as bigint")
	}
	bogusBytes := bogus.FillBytes(make([]byte, 16))
	for i := 0; i < 16; i++ {
		raw[clmmSqrtPriceX64AltOffset+i] = bogusBytes[15-i]
	}

	live := new(big.Int).SetUint64(99676455149125488)
	liveBytes := live.FillBytes(make([]byte, 16))
	for i := 0; i < 16; i++ {
		raw[clmmSqrtPriceX64LiveOffset+i] = liveBytes[15-i]
	}

	state, ok := parseCLMMPoolState(raw)
	if !ok {
		t.Fatal("expected parseCLMMPoolState to succeed with live offset present")
	}
	if state.SqrtPriceX64.Cmp(live) != 0 {
		t.Fatalf("expected parser to prefer the live offset over the bogus nearby candidate: got %s expected %s", state.SqrtPriceX64.String(), live.String())
	}
}

const liveRaydiumPool1Hex = "" +
	"f7ede3f5d7c3de46febf03242a15ce182d3ec4b69d9663b011886609d4d10a4cbfed8c9d1b272b07" +
	"691c45a709c4e57288d577393ec593a55d9e8e01bc96efde17703fa235351f6201069b8857feab81" +
	"84fb687f634618c035dac439dc1aeb3b5598a0f000000000010c45f7df8d9e72956284933f6d98b7" +
	"57032e83df84604fb5e117fff61d5b12f986df5d87223870de5bcd76162f063a2878beff472c8415" +
	"44f33cb544d65442752e9c8fa65c5cf3f8bff21d96940170de44b9c4438d62c72c52d596ca0547f0" +
	"90650f3289d9400ae42a143d306bffef9538a3a076daf3b547f7c9705b343ab98a09060a00cbde92" +
	"9c08ff000000000000000000005690c46b463299520500000000000000a1820000000000002ba52e" +
	"b235391404000000000000000034c7c62676947eae00000000000000003f524d00000000002e1777" +
	"2200000000428a79289f600600000000000000000077164f4c181226010000000000000000507a56" +
	"eed96c26010000000000000000dd4e617c2860060000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000001c45" +
	"a709c4e57288d577393ec593a55d9e8e01bc96efde17703fa235351f620100000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"000000000000000000000000000000000000000000001c45a709c4e57288d577393ec593a55d9e8e01" +
	"bc96efde17703fa235351f6201000000000000000000000000000000000000000000000800000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000400000000000000f4ffffff1301000000000000000000000000000000000000000000" +
	"000000000000000000000000000000000000000000000000000000000000e35451145f010000e17d" +
	"833756010000c22ba62d503f0000da98ba22c63d0000af072e010000000010d10c16000000000000" +
	"0000000000000000f603000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"00000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	"000000000000000000000000000000000000000000000000"

func TestParseCLMMPoolStateReadsLivePool1Layout(t *testing.T) {
	raw, err := hex.DecodeString(liveRaydiumPool1Hex)
	if err != nil {
		t.Fatalf("failed to decode live pool fixture hex: %v", err)
	}

	state, ok := parseCLMMPoolState(raw)
	if !ok {
		t.Fatal("expected parseCLMMPoolState to succeed on live pool fixture")
	}
	if !looksLikeSolanaAddress(state.TokenMint0) || !looksLikeSolanaAddress(state.TokenMint1) {
		t.Fatalf("expected live fixture token mints to be valid addresses, got %s and %s", state.TokenMint0, state.TokenMint1)
	}
	expectedSqrt, ok := new(big.Int).SetString("25135504391457721046528", 10)
	if !ok {
		t.Fatal("failed to parse expected sqrt price")
	}
	if state.SqrtPriceX64.Cmp(expectedSqrt) != 0 {
		t.Fatalf("unexpected sqrt price for live pool fixture: got %s expected %s", state.SqrtPriceX64.String(), expectedSqrt.String())
	}
}

func TestCalculateCLMMPriceFromStateUsesOrientationAndDecimals(t *testing.T) {
	sqrtPrice := new(big.Int).SetUint64(99676455149125488)
	priceRat := shared.ConvertSqrtPriceX64ToPrice(sqrtPrice)
	if priceRat == nil {
		t.Fatal("failed to convert sqrt price")
	}

	expectedDirect := shared.ApplyDecimalAdjustments(priceRat, 9, 6)
	if expectedDirect == nil {
		t.Fatal("failed to calculate expected direct price")
	}
	expectedDirectFloat, _ := expectedDirect.Float64()

	inverted := new(big.Rat).Inv(priceRat)
	expectedInverse := shared.ApplyDecimalAdjustments(inverted, 6, 9)
	if expectedInverse == nil {
		t.Fatal("failed to calculate expected inverse price")
	}
	expectedInverseFloat, _ := expectedInverse.Float64()

	direct := calculateCLMMPriceFromState(sqrtPrice, 9, 6, true)
	inverse := calculateCLMMPriceFromState(sqrtPrice, 9, 6, false)

	if math.Abs(direct-expectedDirectFloat) > 1e-12 {
		t.Fatalf("direct orientation price mismatch: got %g expected %g", direct, expectedDirectFloat)
	}
	if math.Abs(inverse-expectedInverseFloat) > 1e-6 {
		t.Fatalf("inverse orientation price mismatch: got %g expected %g", inverse, expectedInverseFloat)
	}
}

func TestCalculateCLMMPriceFromStateHandlesLivePoolOrientation(t *testing.T) {
	sqrtPrice, ok := new(big.Int).SetString("98217061666667347968", 10)
	if !ok {
		t.Fatal("failed to parse live pool sqrt price")
	}

	price := calculateCLMMPriceFromState(sqrtPrice, 9, 6, false)
	if price <= 0 || price >= 0.001 {
		t.Fatalf("expected a small inverse price for the live pool case, got %g", price)
	}
	if price < 1e-5 || price > 1e-4 {
		t.Fatalf("expected the live pool price to land in the 1e-5..1e-4 range, got %g", price)
	}
}

func TestParseCLMMPricePrefersStatePriceOverReserveMismatch(t *testing.T) {
	raw := make([]byte, 512)
	token0 := bytes.Repeat([]byte{1}, 32)
	token1 := bytes.Repeat([]byte{2}, 32)
	copy(raw[clmmTokenMint0Offset:clmmTokenMint0Offset+32], token0)
	copy(raw[clmmTokenMint1Offset:clmmTokenMint1Offset+32], token1)
	raw[clmmDecimals0Offset] = 9
	raw[clmmDecimals1Offset] = 6

	sqrtPrice, ok := new(big.Int).SetString("98217061666667347968", 10)
	if !ok {
		t.Fatal("failed to parse sqrt price")
	}
	sqrtBytes := sqrtPrice.FillBytes(make([]byte, 16))
	for i := 0; i < 16; i++ {
		raw[clmmSqrtPriceX64Offset+i] = sqrtBytes[15-i]
	}

	binaryLittleEndianPutUint64(raw[0:8], 1000000)
	binaryLittleEndianPutUint64(raw[8:16], 1)

	getAccountInfo := func(string) ([]byte, error) {
		return nil, fmt.Errorf("no token accounts")
	}

	price, ok := parseCLMMPrice(raw, encodeBase58(token0), encodeBase58(token1), 9, 6, getAccountInfo, true, false)
	if !ok {
		t.Fatal("expected parseCLMMPrice to return a state price")
	}
	if price <= 0 || price >= 0.1 {
		t.Fatalf("expected a sane state-derived price in the 0.02..0.1 range, got %g", price)
	}
	if math.Abs(price-0.028348783657882674) > 1e-6 {
		t.Fatalf("expected the state-derived price to match the selected direct-orientation branch, got %g", price)
	}
}

func TestParseCLMMPriceChoosesInverseOrientationWhenMetadataIsFlipped(t *testing.T) {
	raw := make([]byte, 512)
	token0 := bytes.Repeat([]byte{1}, 32)
	token1 := bytes.Repeat([]byte{2}, 32)
	copy(raw[clmmTokenMint0Offset:clmmTokenMint0Offset+32], token0)
	copy(raw[clmmTokenMint1Offset:clmmTokenMint1Offset+32], token1)
	raw[clmmDecimals0Offset] = 9
	raw[clmmDecimals1Offset] = 6

	sqrtPrice, ok := new(big.Int).SetString("98217061666667347968", 10)
	if !ok {
		t.Fatal("failed to parse sqrt price")
	}
	sqrtBytes := sqrtPrice.FillBytes(make([]byte, 16))
	for i := 0; i < 16; i++ {
		raw[clmmSqrtPriceX64Offset+i] = sqrtBytes[15-i]
	}

	price, ok := parseCLMMPrice(raw, encodeBase58(token0), encodeBase58(token1), 9, 6, nil, true, true)
	if !ok {
		t.Fatal("expected parseCLMMPrice to choose the sane inverse orientation")
	}
	if price <= 0 || price >= 1 {
		t.Fatalf("expected the selected inverse orientation to stay in the sane range, got %g", price)
	}
	if math.Abs(price-0.0000352748820573097) > 1e-8 {
		t.Fatalf("expected the inverse-oriented price to match the normalized small sane candidate, got %g", price)
	}
}

func TestParseCLMMPriceUsesPoolStateLayout(t *testing.T) {
	raw := make([]byte, clmmSqrtPriceX64Offset+16)
	token0 := bytes.Repeat([]byte{3}, 32)
	token1 := bytes.Repeat([]byte{4}, 32)
	copy(raw[clmmTokenMint0Offset:clmmTokenMint0Offset+32], token0)
	copy(raw[clmmTokenMint1Offset:clmmTokenMint1Offset+32], token1)
	raw[clmmDecimals0Offset] = 8
	raw[clmmDecimals1Offset] = 6
	sqrtPrice := new(big.Int).SetUint64(99676455149125488)
	sqrtBytes := sqrtPrice.FillBytes(make([]byte, 16))
	for i := 0; i < 16; i++ {
		raw[clmmSqrtPriceX64Offset+i] = sqrtBytes[15-i]
	}

	baseMint := encodeBase58(token0)
	quoteMint := encodeBase58(token1)
	expectedRat := shared.ConvertSqrtPriceX64ToPrice(sqrtPrice)
	expectedRat = shared.ApplyDecimalAdjustments(expectedRat, 8, 6)
	expected, _ := expectedRat.Float64()

	price, ok := parseCLMMPrice(raw, baseMint, quoteMint, 8, 6, nil, false, true)
	if !ok {
		t.Fatal("expected parseCLMMPrice to parse pool state")
	}
	if math.Abs(price-expected) > 1e-12 {
		t.Fatalf("unexpected CLMM price from pool state: got %g expected %g", price, expected)
	}
}

// Helpers to avoid importing encoding/binary for a tiny helper
func binaryLittleEndianPutUint64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
}
