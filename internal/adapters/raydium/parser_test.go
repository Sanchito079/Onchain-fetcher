package raydium

import (
    "encoding/binary"
    "bytes"
    "fmt"
    "math"
    "math/big"
    "os"
    "testing"
)

func TestParseTokenAccountBalance(t *testing.T) {
    raw := make([]byte, 72)
    expected := uint64(123456789)
    binary.LittleEndian.PutUint64(raw[64:72], expected)

    bal, err := parseTokenAccountBalance(raw)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if bal.Cmp(new(big.Int).SetUint64(expected)) != 0 {
        t.Fatalf("balance mismatch: got %s expected %d", bal.String(), expected)
    }
}

func TestChooseRaydiumPricePrefersSaneReciprocal(t *testing.T) {
    got := chooseRaydiumPrice(42.3333, 0.0001063)
    if math.Abs(got-0.0001063) > 1e-9 {
        t.Fatalf("chooseRaydiumPrice() = %v, want ~0.0001063", got)
    }
}

func TestCalculateSolanaPricePrefersDecimalAlignedCandidate(t *testing.T) {
    got := calculateSolanaPrice(big.NewInt(1000000), big.NewInt(1000), 6, 9)
    if math.Abs(got-0.000001) > 1e-12 {
        t.Fatalf("calculateSolanaPrice() = %v, want ~0.000001", got)
    }
}

func TestParsePoolReservesUsesCurveBalances(t *testing.T) {
    poolRaw := make([]byte, 32)
    binary.LittleEndian.PutUint64(poolRaw[0:8], 1234)
    binary.LittleEndian.PutUint64(poolRaw[8:16], 5678)

    getAccountInfo := func(string) ([]byte, error) {
        return nil, nil
    }

    baseBal, quoteBal, err := parsePoolReserves(poolRaw, "", "", getAccountInfo)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if baseBal.Cmp(new(big.Int).SetUint64(1234)) != 0 {
        t.Fatalf("base balance mismatch: %s", baseBal.String())
    }
    if quoteBal.Cmp(new(big.Int).SetUint64(5678)) != 0 {
        t.Fatalf("quote balance mismatch: %s", quoteBal.String())
    }
}

func TestParsePoolReservesWithCandidates(t *testing.T) {
    // craft pool raw with base/quote pubkeys at offset 64
    baseKey := bytes.Repeat([]byte{1}, 32)
    quoteKey := bytes.Repeat([]byte{2}, 32)
    poolRaw := make([]byte, 200)
    copy(poolRaw[64:64+32], baseKey)
    copy(poolRaw[96:96+32], quoteKey)

    // craft token account raws with balances, using realistic SPL Token account size
    baseAcc := make([]byte, 165)
    quoteAcc := make([]byte, 165)
    for i := 0; i < 64; i++ {
        baseAcc[i] = 1
        quoteAcc[i] = 1
    }
    binary.LittleEndian.PutUint64(baseAcc[64:72], 1000)
    binary.LittleEndian.PutUint64(quoteAcc[64:72], 2000)

    // map encoded pubkey -> account data
    m := map[string][]byte{
        encodeBase58(baseKey): baseAcc,
        encodeBase58(quoteKey): quoteAcc,
    }

    getAccountInfo := func(acc string) ([]byte, error) {
        d, ok := m[acc]
        if !ok {
            return nil, nil
        }
        return d, nil
    }

    baseBal, quoteBal, err := parsePoolReserves(poolRaw, "", "", getAccountInfo)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if baseBal.Cmp(new(big.Int).SetUint64(1000)) != 0 {
        t.Fatalf("base balance mismatch: %s", baseBal.String())
    }
    if quoteBal.Cmp(new(big.Int).SetUint64(2000)) != 0 {
        t.Fatalf("quote balance mismatch: %s", quoteBal.String())
    }
}

func TestParsePoolReservesWithRaydiumStandardOffset(t *testing.T) {
    // craft pool raw with base/quote pubkeys at Raydium standard offset 336
    baseKey := bytes.Repeat([]byte{3}, 32)
    quoteKey := bytes.Repeat([]byte{4}, 32)
    poolRaw := make([]byte, 420)
    copy(poolRaw[336:336+32], baseKey)
    copy(poolRaw[368:368+32], quoteKey)

    baseAcc := make([]byte, 165)
    quoteAcc := make([]byte, 165)
    for i := 0; i < 64; i++ {
        baseAcc[i] = 2
        quoteAcc[i] = 2
    }
    binary.LittleEndian.PutUint64(baseAcc[64:72], 4000)
    binary.LittleEndian.PutUint64(quoteAcc[64:72], 8000)

    m := map[string][]byte{
        encodeBase58(baseKey): baseAcc,
        encodeBase58(quoteKey): quoteAcc,
    }
    getAccountInfo := func(acc string) ([]byte, error) {
        d, ok := m[acc]
        if !ok {
            return nil, nil
        }
        return d, nil
    }

    baseBal, quoteBal, err := parsePoolReserves(poolRaw, "", "", getAccountInfo)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if baseBal.Cmp(new(big.Int).SetUint64(4000)) != 0 {
        t.Fatalf("base balance mismatch: %s", baseBal.String())
    }
    if quoteBal.Cmp(new(big.Int).SetUint64(8000)) != 0 {
        t.Fatalf("quote balance mismatch: %s", quoteBal.String())
    }
}

func TestParsePoolReservesReordersByTokenMintMetadata(t *testing.T) {
    baseKey := bytes.Repeat([]byte{5}, 32)
    quoteKey := bytes.Repeat([]byte{6}, 32)
    poolRaw := make([]byte, 420)
    copy(poolRaw[336:336+32], baseKey)
    copy(poolRaw[368:368+32], quoteKey)

    baseMint := bytes.Repeat([]byte{11}, 32)
    quoteMint := bytes.Repeat([]byte{22}, 32)

    // create token accounts with reversed mints relative to the raw pubkey order
    baseAcc := make([]byte, 165)
    quoteAcc := make([]byte, 165)
    for i := 0; i < 64; i++ {
        baseAcc[i] = 2
        quoteAcc[i] = 2
    }
    copy(baseAcc[0:32], quoteMint)
    copy(quoteAcc[0:32], baseMint)
    binary.LittleEndian.PutUint64(baseAcc[64:72], 9000)
    binary.LittleEndian.PutUint64(quoteAcc[64:72], 18000)

    m := map[string][]byte{
        encodeBase58(baseKey):  baseAcc,
        encodeBase58(quoteKey): quoteAcc,
    }
    getAccountInfo := func(acc string) ([]byte, error) {
        d, ok := m[acc]
        if !ok {
            return nil, nil
        }
        return d, nil
    }

    baseBal, quoteBal, err := parsePoolReserves(poolRaw, encodeBase58(baseMint), encodeBase58(quoteMint), getAccountInfo)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if baseBal.Cmp(new(big.Int).SetUint64(18000)) != 0 {
        t.Fatalf("base balance mismatch: %s", baseBal.String())
    }
    if quoteBal.Cmp(new(big.Int).SetUint64(9000)) != 0 {
        t.Fatalf("quote balance mismatch: %s", quoteBal.String())
    }
}

func TestParsePoolReservesBoundsCandidateLookups(t *testing.T) {
    poolRaw := make([]byte, 1024)
    off := 336
    baseKey := bytes.Repeat([]byte{0x11}, 32)
    quoteKey := bytes.Repeat([]byte{0x22}, 32)
    copy(poolRaw[off:off+32], baseKey)
    copy(poolRaw[off+32:off+64], quoteKey)

    callCount := 0
    getAccountInfo := func(string) ([]byte, error) {
        callCount++
        return nil, fmt.Errorf("lookup failed")
    }

    _, _, err := parsePoolReserves(poolRaw, "", "", getAccountInfo)
    if err == nil {
        t.Fatalf("expected an error after bounded candidate probing")
    }
    if callCount > 16 {
        t.Fatalf("expected bounded account lookups, got %d", callCount)
    }
    if callCount == 0 {
        t.Fatalf("expected at least one lookup attempt")
    }
}

func TestParsePoolReservesSearchesBeyondEightCandidates(t *testing.T) {
    poolRaw := make([]byte, 420)

    // Add fake candidate pubkey pairs in the first 8 offsets.
    for i, off := range []int{336, 64, 72, 80, 88, 96, 104, 112} {
        fakeBase := bytes.Repeat([]byte{byte(i + 1)}, 32)
        fakeQuote := bytes.Repeat([]byte{byte(i + 2)}, 32)
        copy(poolRaw[off:off+32], fakeBase)
        copy(poolRaw[off+32:off+64], fakeQuote)
    }

    // Actual pool reserve token accounts are at offset 120.
    baseKey := bytes.Repeat([]byte{0x65}, 32)
    quoteKey := bytes.Repeat([]byte{0x66}, 32)
    copy(poolRaw[120:120+32], baseKey)
    copy(poolRaw[152:152+32], quoteKey)

    baseMint := bytes.Repeat([]byte{0x71}, 32)
    quoteMint := bytes.Repeat([]byte{0x72}, 32)

    baseAcc := make([]byte, 165)
    quoteAcc := make([]byte, 165)
    for i := 0; i < 64; i++ {
        baseAcc[i] = 1
        quoteAcc[i] = 1
    }
    copy(baseAcc[0:32], baseMint)
    copy(quoteAcc[0:32], quoteMint)
    binary.LittleEndian.PutUint64(baseAcc[64:72], 5000)
    binary.LittleEndian.PutUint64(quoteAcc[64:72], 10000)

    accounts := map[string][]byte{
        encodeBase58(baseKey):  baseAcc,
        encodeBase58(quoteKey): quoteAcc,
    }
    getAccountInfo := func(acc string) ([]byte, error) {
        data, ok := accounts[acc]
        if !ok {
            return nil, fmt.Errorf("unknown account %s", acc)
        }
        return data, nil
    }

    baseBal, quoteBal, err := parsePoolReserves(poolRaw, encodeBase58(baseMint), encodeBase58(quoteMint), getAccountInfo)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if baseBal.Cmp(new(big.Int).SetUint64(5000)) != 0 {
        t.Fatalf("base balance mismatch: %s", baseBal.String())
    }
    if quoteBal.Cmp(new(big.Int).SetUint64(10000)) != 0 {
        t.Fatalf("quote balance mismatch: %s", quoteBal.String())
    }
}

func TestParsePoolReservesFallsBackFromInvalidCurveReserves(t *testing.T) {
    baseKey := bytes.Repeat([]byte{3}, 32)
    quoteKey := bytes.Repeat([]byte{4}, 32)
    poolRaw := make([]byte, 420)
    binary.LittleEndian.PutUint64(poolRaw[0:8], 1)
    binary.LittleEndian.PutUint64(poolRaw[8:16], 2)
    copy(poolRaw[64:96], baseKey)
    copy(poolRaw[96:128], quoteKey)

    baseMint := bytes.Repeat([]byte{11}, 32)
    quoteMint := bytes.Repeat([]byte{22}, 32)
    baseAcc := make([]byte, 165)
    quoteAcc := make([]byte, 165)
    for i := 0; i < 64; i++ {
        baseAcc[i] = 2
        quoteAcc[i] = 2
    }
    copy(baseAcc[0:32], baseMint)
    copy(quoteAcc[0:32], quoteMint)
    binary.LittleEndian.PutUint64(baseAcc[64:72], 4000)
    binary.LittleEndian.PutUint64(quoteAcc[64:72], 8000)

    m := map[string][]byte{
        encodeBase58(baseKey):  baseAcc,
        encodeBase58(quoteKey): quoteAcc,
    }
    getAccountInfo := func(acc string) ([]byte, error) {
        d, ok := m[acc]
        if !ok {
            return nil, nil
        }
        return d, nil
    }

    baseBal, quoteBal, err := parsePoolReserves(poolRaw, encodeBase58(baseMint), encodeBase58(quoteMint), getAccountInfo)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if baseBal.Cmp(new(big.Int).SetUint64(4000)) != 0 {
        t.Fatalf("base balance mismatch: %s", baseBal.String())
    }
    if quoteBal.Cmp(new(big.Int).SetUint64(8000)) != 0 {
        t.Fatalf("quote balance mismatch: %s", quoteBal.String())
    }
}

func TestLiveRaydiumPool22WrmyTj8x2TRVQen3fxxi2r4Rn6JDHWoMTpsSmn8RUd(t *testing.T) {
    rpcEndpoint := os.Getenv("RAYDIUM_LIVE_RPC_ENDPOINT")
    if rpcEndpoint == "" {
        t.Skip("set RAYDIUM_LIVE_RPC_ENDPOINT to run live Raydium integration test")
    }

    client := RPCClient{Endpoint: rpcEndpoint}
    pool := "22WrmyTj8x2TRVQen3fxxi2r4Rn6JDHWoMTpsSmn8RUd"
    poolBytes, err := client.getAccountInfo(pool)
    if err != nil {
        t.Fatalf("getAccountInfo failed: %v", err)
    }

    baseMint := "So11111111111111111111111111111111111111112"
    quoteMint := "ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY"
    baseDecimals, err := getMintDecimals(client, baseMint)
    if err != nil {
        t.Fatalf("getMintDecimals(base) failed: %v", err)
    }
    quoteDecimals, err := getMintDecimals(client, quoteMint)
    if err != nil {
        t.Fatalf("getMintDecimals(quote) failed: %v", err)
    }

    baseBal, quoteBal, err := parsePoolReserves(poolBytes, baseMint, quoteMint, client.getAccountInfo)
    if err != nil {
        t.Fatalf("parsePoolReserves failed: %v", err)
    }
    t.Logf("live base balance=%s quote balance=%s", baseBal.String(), quoteBal.String())

    price := calculateSolanaPrice(baseBal, quoteBal, baseDecimals, quoteDecimals)
    if price <= 0 {
        t.Fatalf("invalid live price: %g", price)
    }
    t.Logf("live price=%g", price)

    expectedDirect := 2021.0
    expectedInverse := 0.000494
    if math.Abs(price-expectedDirect) <= 1.0 {
        t.Logf("live price matches expected direct orientation: %g", price)
        return
    }
    if math.Abs(price-expectedInverse) <= 1e-6 {
        t.Logf("live price matches expected inverse orientation: %g", price)
        return
    }
    t.Fatalf("live price %g did not match expected direct %g or inverse %g", price, expectedDirect, expectedInverse)
}

func getMintDecimals(client RPCClient, mint string) (int, error) {
    raw, err := client.getAccountInfo(mint)
    if err != nil {
        return 0, err
    }
    if len(raw) < 45 {
        return 0, fmt.Errorf("mint account data too short: %d", len(raw))
    }
    return int(raw[44]), nil
}
