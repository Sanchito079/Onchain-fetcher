package meteora

import (
    "encoding/base64"
    "encoding/binary"
    "math"
    "net/http"
    "net/http/httptest"
    "testing"

    "on-chain-price-fetcher/internal/adapters/shared"
)

type mockRPCClient struct {
    response []byte
    mintDecimals map[string]int
}

func (m mockRPCClient) getAccountInfo(account string) ([]byte, error) {
    return m.response, nil
}

func (m mockRPCClient) getMintDecimals(mint string) (int, error) {
    if m.mintDecimals != nil {
        if decimals, ok := m.mintDecimals[mint]; ok {
            return decimals, nil
        }
    }
    return 0, nil
}

func TestSupportsMeteoraAndDLMM(t *testing.T) {
    tests := []struct {
        dexName string
        want    bool
    }{
        {"meteora", true},
        {"Meteora-v1", true},
        {"dlmm", true},
        {"DLMM-something", true},
        {"damm v2", true},
        {"meteora damm v2", true},
        {"pancakeswap", false},
        {"solana", false},
    }

    for _, tt := range tests {
        got := Adapter{}.Supports(shared.Pair{Network: "solana", DexName: tt.dexName})
        if got != tt.want {
            t.Fatalf("Supports(%q) = %v, want %v", tt.dexName, got, tt.want)
        }
    }
}

func TestFetchPriceParsesMeteoraRawAccountData(t *testing.T) {
    reserveA := uint64(100_000_000_000)
    reserveB := uint64(50_000_000_000)
    raw := make([]byte, 128)
    for i := 0; i < 8; i++ {
        raw[64+i] = byte(reserveA >> (8 * i))
        raw[72+i] = byte(reserveB >> (8 * i))
    }
    encoded := base64.StdEncoding.EncodeToString(raw)

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        _, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"value":{"data":["` + encoded + `","base64"]}}}`))
    }))
    defer server.Close()

    client := &RPCClient{Endpoint: server.URL}
    adapter := Adapter{RPC: client}

    pair := shared.Pair{ID: "pair1", Network: "solana", DexName: "meteora", PoolAddress: "pool1", BaseTokenDecimals: 9, QuoteTokenDecimals: 6}
    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if !result.Valid {
        t.Fatalf("expected valid result, got invalid: %s", result.Reason)
    }
    if result.Price <= 0 {
        t.Fatalf("expected positive price, got %g", result.Price)
    }
}

func TestParseMeteoraAccountDataFallback(t *testing.T) {
    reserveA := uint64(1_000_000_000)
    reserveB := uint64(2_000_000_000)
    raw := make([]byte, 64)
    for i := 0; i < 8; i++ {
        raw[16+i] = byte(reserveA >> (8 * i))
        raw[24+i] = byte(reserveB >> (8 * i))
    }

    parsedA, parsedB, err := parseMeteoraAccountData(raw)
    if err != nil {
        t.Fatalf("unexpected parse error: %v", err)
    }
    if parsedA.Uint64() != reserveA || parsedB.Uint64() != reserveB {
        t.Fatalf("unexpected reserves: %d %d", parsedA.Uint64(), parsedB.Uint64())
    }
}

func TestParseDammV2CandidateOffsets(t *testing.T) {
    raw := make([]byte, 128)
    reserveA := uint64(100)
    reserveB := uint64(50)
    for i := 0; i < 8; i++ {
        raw[80+i] = byte(reserveA >> (8 * i))
        raw[88+i] = byte(reserveB >> (8 * i))
    }

    parsedA, parsedB, ok := parseCandidateOffsets(raw, []int{80, 88, 64, 72, 96, 104, 112, 120, 32, 0, 16, 24}, binary.LittleEndian)
    if !ok {
        t.Fatal("expected parseCandidateOffsets to succeed for DAMM v2 offsets")
    }
    if parsedA.Uint64() != reserveA || parsedB.Uint64() != reserveB {
        t.Fatalf("expected reserves %d/%d, got %d/%d", reserveA, reserveB, parsedA.Uint64(), parsedB.Uint64())
    }
}

func TestParseDammV2AccountDataFallback(t *testing.T) {
    raw := make([]byte, 128)
    reserveA := uint64(100)
    reserveB := uint64(50)
    for i := 0; i < 8; i++ {
        raw[80+i] = byte(reserveA >> (8 * i))
        raw[88+i] = byte(reserveB >> (8 * i))
    }

    adapter := Adapter{RPC: mockRPCClient{response: raw}}
    parsedA, parsedB, mintA, mintB, err := adapter.parseDammV2AccountData(raw, shared.Pair{DexName: "damm v2"})
    if err != nil {
        t.Fatalf("expected no error from parseDammV2AccountData, got %v", err)
    }
    if parsedA.Uint64() != reserveA || parsedB.Uint64() != reserveB {
        t.Fatalf("expected reserves %d/%d, got %d/%d", reserveA, reserveB, parsedA.Uint64(), parsedB.Uint64())
    }
    if mintA != "" || mintB != "" {
        t.Fatalf("expected empty mint values, got %q, %q", mintA, mintB)
    }
}

func TestFetchPriceRequiresDecimals(t *testing.T) {
    raw := make([]byte, 128)
    reserveA := uint64(100_000_000_000)
    reserveB := uint64(50_000_000_000)
    for i := 0; i < 8; i++ {
        raw[64+i] = byte(reserveA >> (8 * i))
        raw[72+i] = byte(reserveB >> (8 * i))
    }
    adapter := Adapter{RPC: mockRPCClient{response: raw, mintDecimals: map[string]int{"mint": 9}}}
    pair := shared.Pair{ID: "pair2", Network: "solana", DexName: "meteora", PoolAddress: "pool2", BaseTokenDecimals: 0, QuoteTokenDecimals: 0}
    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if result.Valid {
        t.Fatalf("expected invalid result due to missing decimals")
    }
}

func TestParseMeteoraAccountDataRejectsShortPayload(t *testing.T) {
    _, _, err := parseMeteoraAccountData([]byte{1, 2, 3})
    if err == nil {
        t.Fatalf("expected error for short payload")
    }
}

func TestFetchPriceUsesReservePriceForDammV2(t *testing.T) {
    raw := make([]byte, 128)
    reserveA := uint64(100)
    reserveB := uint64(50)
    for i := 0; i < 8; i++ {
        raw[80+i] = byte(reserveA >> (8 * i))
        raw[88+i] = byte(reserveB >> (8 * i))
    }

    adapter := Adapter{RPC: mockRPCClient{response: raw}}
    pair := shared.Pair{ID: "pair3", Network: "solana", DexName: "meteora damm v2", PoolAddress: "pool3", BaseTokenDecimals: 9, QuoteTokenDecimals: 9}

    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if !result.Valid {
        t.Fatalf("expected valid result, got invalid: %s", result.Reason)
    }
    if math.Abs(result.Price-0.5) > 1e-9 {
        t.Fatalf("expected reserve-based price 0.5, got %v", result.Price)
    }
}

func TestFetchPriceUsesSqrtPriceForDammV2(t *testing.T) {
    raw := make([]byte, 128)
    reserveA := uint64(10_000)
    reserveB := uint64(20_000)
    for i := 0; i < 8; i++ {
        raw[80+i] = byte(reserveA >> (8 * i))
        raw[88+i] = byte(reserveB >> (8 * i))
    }

    sqrtPriceX64 := uint64(891863535427527991)
    for i := 0; i < 8; i++ {
        raw[104+i] = byte(sqrtPriceX64 >> (8 * i))
    }

    parsed, ok := parseDammV2SqrtPrice(raw)
    if !ok {
        t.Fatalf("expected parseDammV2SqrtPrice to succeed")
    }
    priceRat := shared.ConvertSqrtPriceX64ToPrice(parsed)
    if priceRat == nil {
        t.Fatalf("expected ConvertSqrtPriceX64ToPrice to return a price")
    }
    adjusted := shared.ApplyDecimalAdjustments(priceRat, 6, 9)
    if adjusted == nil {
        t.Fatalf("expected ApplyDecimalAdjustments to return a price")
    }
    p, _ := adjusted.Float64()
    if math.Abs(p-0.000002337530954138738) > 1e-15 {
        t.Fatalf("expected direct sqrt price 2.337530954e-6, got %v", p)
    }

    adapter := Adapter{RPC: mockRPCClient{response: raw}}
    pair := shared.Pair{ID: "pair4", Network: "solana", DexName: "meteora damm v2", PoolAddress: "pool4", BaseTokenDecimals: 6, QuoteTokenDecimals: 9}

    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if !result.Valid {
        t.Fatalf("expected valid result, got invalid: %s", result.Reason)
    }

    if math.Abs(result.Price-0.000002337530954138738) > 1e-12 {
        t.Fatalf("expected sqrt-price-based price close to 2.337530954e-6, got %v", result.Price)
    }
}

func TestCalculateDLMMPriceUsesActiveBinState(t *testing.T) {
    raw := make([]byte, 96)
    activeID := int32(-9834)
    binStep := uint16(8)

    binary.LittleEndian.PutUint32(raw[76:80], uint32(activeID))
    binary.LittleEndian.PutUint16(raw[80:82], binStep)

    got := calculateDLMMPrice(raw, 5, 9)
    want := math.Pow(1+float64(binStep)/10000, float64(activeID)) * math.Pow10(5-9)

    if math.Abs(got-want) > 1e-9 {
        t.Fatalf("calculateDLMMPrice() = %v, want %v", got, want)
    }
}

func TestChooseMeteoraPriceKeepsTinyDirectValue(t *testing.T) {
    direct := 3.867e-9
    inverted := 258959948.56

    got := chooseMeteoraPrice(direct, inverted)
    if math.Abs(got-direct) > 1e-12 {
        t.Fatalf("chooseMeteoraPrice() = %v, want %v", got, direct)
    }
}
