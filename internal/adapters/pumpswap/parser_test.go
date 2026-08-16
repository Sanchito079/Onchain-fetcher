package pumpswap

import (
    "encoding/base64"
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "math/big"
    "testing"
)

func TestParsePumpSwapAccountDataAtKnownOffset(t *testing.T) {
    reserveA := uint64(100_000_000_000)
    reserveB := uint64(50_000_000_000)
    raw := make([]byte, 128)
    for i := 0; i < 8; i++ {
        raw[64+i] = byte(reserveA >> (8 * i))
        raw[72+i] = byte(reserveB >> (8 * i))
    }

    parsedA, parsedB, err := parsePumpSwapAccountData(raw)
    if err != nil {
        t.Fatalf("unexpected parse error: %v", err)
    }
    if parsedA.Uint64() != reserveA || parsedB.Uint64() != reserveB {
        t.Fatalf("unexpected reserves: %d %d", parsedA.Uint64(), parsedB.Uint64())
    }
}

func TestParsePumpSwapAccountDataUsesScanFallback(t *testing.T) {
    reserveA := uint64(1_000_000_000)
    reserveB := uint64(2_000_000_000)
    raw := make([]byte, 64)
    for i := 0; i < 8; i++ {
        raw[16+i] = byte(reserveA >> (8 * i))
        raw[24+i] = byte(reserveB >> (8 * i))
    }

    parsedA, parsedB, err := parsePumpSwapAccountData(raw)
    if err != nil {
        t.Fatalf("unexpected parse error: %v", err)
    }
    if parsedA.Uint64() != reserveA || parsedB.Uint64() != reserveB {
        t.Fatalf("unexpected reserves: %d %d", parsedA.Uint64(), parsedB.Uint64())
    }
}

func TestRPCClientHandlesRawBase64(t *testing.T) {
    raw := []byte("hello world")
    encoded := base64.RawStdEncoding.EncodeToString(raw)
    decoded, err := base64.RawStdEncoding.DecodeString(encoded)
    if err != nil {
        t.Fatalf("expected decode to succeed: %v", err)
    }
    if hex.EncodeToString(decoded) != hex.EncodeToString(raw) {
        t.Fatalf("expected raw base64 decode to match")
    }
}

func TestParseReserveTokenAccountsByMintScansArbitraryOffsets(t *testing.T) {
    poolBytes := make([]byte, 160)
    tokenA := make([]byte, 32)
    tokenA[0] = 1
    tokenB := make([]byte, 32)
    tokenB[0] = 2

    copy(poolBytes[41:], tokenA)
    copy(poolBytes[81:], tokenB)

    getAccountInfo := func(account string) ([]byte, error) {
        switch account {
        case encodeBase58(tokenA):
            return makeTokenAccountData(tokenA, 100), nil
        case encodeBase58(tokenB):
            return makeTokenAccountData(tokenB, 200), nil
        default:
            return nil, fmt.Errorf("unexpected account %s", account)
        }
    }

    balances, err := parseReserveTokenAccountsByMint(poolBytes, getAccountInfo)
    if err != nil {
        t.Fatalf("expected reserve scan to resolve token account balances, got error: %v", err)
    }
    if got := balances[encodeBase58(tokenA)].Uint64(); got != 100 {
        t.Fatalf("expected token A balance 100, got %d", got)
    }
    if got := balances[encodeBase58(tokenB)].Uint64(); got != 200 {
        t.Fatalf("expected token B balance 200, got %d", got)
    }
}

func TestChoosePumpSwapPriceAcceptsVerySmallPositivePrices(t *testing.T) {
    got := choosePumpSwapPrice(8e-80, 0)
    if got != 8e-80 {
        t.Fatalf("expected very small price to be accepted, got %v", got)
    }
}

func TestParsePumpFunBondingCurveAccountReadsStructFields(t *testing.T) {
    raw := make([]byte, 96)
    binary.LittleEndian.PutUint64(raw[0:8], 0xDEADBEEF)
    binary.LittleEndian.PutUint64(raw[8:16], 100)
    binary.LittleEndian.PutUint64(raw[16:24], 200)
    binary.LittleEndian.PutUint64(raw[24:32], 300)
    binary.LittleEndian.PutUint64(raw[32:40], 400)
    binary.LittleEndian.PutUint64(raw[40:48], 500)

    curve, err := parsePumpFunBondingCurveAccount(raw)
    if err != nil {
        t.Fatalf("expected bonding-curve parse to succeed: %v", err)
    }
    if got := curve.VirtualTokenReserves.Uint64(); got != 100 {
        t.Fatalf("expected virtual token reserves 100, got %d", got)
    }
    if got := curve.VirtualSOLReserves.Uint64(); got != 200 {
        t.Fatalf("expected virtual sol reserves 200, got %d", got)
    }
    if got := curve.RealSOLReserves.Uint64(); got != 400 {
        t.Fatalf("expected real sol reserves 400, got %d", got)
    }
    if got := curve.TokenTotalSupply.Uint64(); got != 500 {
        t.Fatalf("expected total supply 500, got %d", got)
    }
}

func TestCalculatePumpFunPriceUsesDecimalNormalizedVirtualReserveRatio(t *testing.T) {
    curve := &PumpFunBondingCurve{
        VirtualTokenReserves: big.NewInt(877919654780427),
        VirtualSOLReserves:   big.NewInt(36666227642),
    }

    got := calculatePumpFunPrice(curve, 6, 9)
    const expected = 4.176490119835675e-08
    if diff := absFloat64(got - expected); diff > 1e-12 {
        t.Fatalf("expected %.15f SOL/token, got %.15f (diff %.15e)", expected, got, diff)
    }
}

func absFloat64(v float64) float64 {
    if v < 0 {
        return -v
    }
    return v
}

func TestParseReserveTokenAccountsFromPoolUsesPoolState(t *testing.T) {
    poolBytes := make([]byte, 256)
    basePubkey := make([]byte, 32)
    basePubkey[0] = 3
    quotePubkey := make([]byte, 32)
    quotePubkey[0] = 4

    copy(poolBytes[pumpSwapPoolBaseTokenAccountOffset:pumpSwapPoolBaseTokenAccountOffset+32], basePubkey)
    copy(poolBytes[pumpSwapPoolQuoteTokenAccountOffset:pumpSwapPoolQuoteTokenAccountOffset+32], quotePubkey)

    getAccountInfo := func(account string) ([]byte, error) {
        switch account {
        case encodeBase58(basePubkey):
            return makeTokenAccountData(basePubkey, 300), nil
        case encodeBase58(quotePubkey):
            return makeTokenAccountData(quotePubkey, 400), nil
        default:
            return nil, fmt.Errorf("unexpected account %s", account)
        }
    }

    reserveA, reserveB, err := parseReserveTokenAccountsFromPool(poolBytes, getAccountInfo)
    if err != nil {
        t.Fatalf("expected pool-state decode to resolve reserves, got error: %v", err)
    }
    if reserveA.Uint64() != 300 {
        t.Fatalf("expected base reserve 300, got %d", reserveA.Uint64())
    }
    if reserveB.Uint64() != 400 {
        t.Fatalf("expected quote reserve 400, got %d", reserveB.Uint64())
    }
}

func TestParsePoolTokenAccountDetailsResolvesMints(t *testing.T) {
    poolBytes := make([]byte, 256)
    basePubkey := make([]byte, 32)
    basePubkey[0] = 11
    quotePubkey := make([]byte, 32)
    quotePubkey[0] = 22
    baseMint := make([]byte, 32)
    baseMint[0] = 111
    quoteMint := make([]byte, 32)
    quoteMint[0] = 222

    copy(poolBytes[pumpSwapPoolBaseTokenAccountOffset:pumpSwapPoolBaseTokenAccountOffset+32], basePubkey)
    copy(poolBytes[pumpSwapPoolQuoteTokenAccountOffset:pumpSwapPoolQuoteTokenAccountOffset+32], quotePubkey)

    getAccountInfo := func(account string) ([]byte, error) {
        switch account {
        case encodeBase58(basePubkey):
            return makeTokenAccountData(baseMint, 300), nil
        case encodeBase58(quotePubkey):
            return makeTokenAccountData(quoteMint, 400), nil
        default:
            return nil, fmt.Errorf("unexpected account %s", account)
        }
    }

    details, err := parsePoolTokenAccountDetails(poolBytes, getAccountInfo)
    if err != nil {
        t.Fatalf("expected pool token-account details to resolve, got error: %v", err)
    }
    if details.BaseMint != encodeBase58(baseMint) {
        t.Fatalf("expected base mint %s, got %s", encodeBase58(baseMint), details.BaseMint)
    }
    if details.QuoteMint != encodeBase58(quoteMint) {
        t.Fatalf("expected quote mint %s, got %s", encodeBase58(quoteMint), details.QuoteMint)
    }
}

func makeTokenAccountData(mint []byte, balance uint64) []byte {
    data := make([]byte, 72)
    copy(data[0:32], mint)
    binary.LittleEndian.PutUint64(data[64:72], balance)
    return data
}
