package meteora

import (
    "encoding/base64"
    "encoding/binary"
    "encoding/json"
    "fmt"
    "io"
    "math"
    "math/big"
    "net/http"
    "sort"
    "strings"
    "time"

    "on-chain-price-fetcher/internal/adapters/shared"
)

type RPC interface {
    getAccountInfo(account string) ([]byte, error)
    getMintDecimals(mint string) (int, error)
}

type Adapter struct {
    RPC RPC
}

func (a Adapter) Name() string { return "meteora" }

func (a Adapter) Supports(pair shared.Pair) bool {
    if strings.ToLower(strings.TrimSpace(pair.Network)) != "solana" {
        return false
    }
    dex := strings.ToLower(strings.TrimSpace(pair.DexName))
    return strings.Contains(dex, "meteora") || strings.Contains(dex, "dlmm") || strings.Contains(dex, "damm")
}

func (a Adapter) FetchPrice(pair shared.Pair) (shared.PriceResult, error) {
    if !a.Supports(pair) {
        return shared.PriceResult{Valid: false, Reason: "unsupported pair", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "unsupported", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "unsupported pair")}, nil
    }
    if pair.PoolAddress == "" {
        return shared.PriceResult{Valid: false, Reason: "missing pool address", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "missing-pool", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "missing pool address")}, nil
    }
    baseDecimals := pair.BaseTokenDecimals
    quoteDecimals := pair.QuoteTokenDecimals

    data, err := a.RPC.getAccountInfo(pair.PoolAddress)
    if err != nil {
        return shared.PriceResult{Valid: false, Reason: err.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "meteora", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, err.Error())}, err
    }

    reserveA, reserveB, err := parseMeteoraAccountData(data)
    var tokenMintA, tokenMintB string
    if isDammV2Pair(pair.DexName) {
        if dammReserveA, dammReserveB, mintA, mintB, dammErr := a.parseDammV2AccountData(data, pair); dammErr == nil {
            reserveA, reserveB = dammReserveA, dammReserveB
            tokenMintA, tokenMintB = mintA, mintB
            err = nil
        } else if err != nil {
            err = dammErr
        }
    }
    if err != nil {
        return shared.PriceResult{Valid: false, Reason: err.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "meteora", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, err.Error())}, err
    }

    if tokenMintA == "" {
        if mintX, mintY, ok := readTokenMints(data); ok {
            tokenMintA, tokenMintB = mintX, mintY
        }
    }
    if tokenMintA != "" {
        if decimals, err := a.RPC.getMintDecimals(tokenMintA); err == nil && decimals >= 0 {
            baseDecimals = shared.ResolveDecimals(decimals, pair.BaseTokenDecimals)
        }
    }
    if tokenMintB != "" {
        if decimals, err := a.RPC.getMintDecimals(tokenMintB); err == nil && decimals >= 0 {
            quoteDecimals = shared.ResolveDecimals(decimals, pair.QuoteTokenDecimals)
        }
    }

    if baseDecimals <= 0 || quoteDecimals <= 0 {
        reason := fmt.Sprintf("missing decimals: base=%d quote=%d", baseDecimals, quoteDecimals)
        return shared.PriceResult{Valid: false, Reason: reason, PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "meteora", baseDecimals, quoteDecimals, 0, 0, 0, nil, reason)}, nil
    }

    directPrice := calculateSolanaPrice(reserveA, reserveB, baseDecimals, quoteDecimals, true)
    invertedPrice := calculateSolanaPrice(reserveA, reserveB, baseDecimals, quoteDecimals, false)

    if isDammV2Pair(pair.DexName) {
        if sqrtPrice, ok := parseDammV2SqrtPrice(data); ok {
            priceRat := shared.ConvertSqrtPriceX64ToPrice(sqrtPrice)
            if priceRat != nil {
                if tokenMintA != "" && tokenMintB != "" {
                    baseToken := strings.TrimSpace(pair.BaseToken)
                    quoteToken := strings.TrimSpace(pair.QuoteToken)
                    if baseToken == tokenMintB && quoteToken == tokenMintA {
                        priceRat = new(big.Rat).Inv(priceRat)
                    }
                }
                priceRat = shared.ApplyDecimalAdjustments(priceRat, baseDecimals, quoteDecimals)
                if priceRat != nil {
                    if p, _ := priceRat.Float64(); !math.IsNaN(p) && !math.IsInf(p, 0) {
                        directPrice = p
                        
                        if p != 0 {
                            invertedPrice = 1 / p
                        }
                    }
                }
            }
        }
    } else {
        dlmmPrice := calculateDLMMPrice(data, baseDecimals, quoteDecimals)
        if dlmmPrice > 0 {
            directPrice = dlmmPrice
            invertedPrice = 1 / dlmmPrice
        }
    }

    price := chooseMeteoraPrice(directPrice, invertedPrice)
    if tokenMintA != "" && tokenMintB != "" {
        baseToken := strings.TrimSpace(pair.BaseToken)
        quoteToken := strings.TrimSpace(pair.QuoteToken)
        if baseToken == tokenMintA && quoteToken == tokenMintB {
            price = directPrice
        } else if baseToken == tokenMintB && quoteToken == tokenMintA {
            price = invertedPrice
        } else if baseToken != "" {
            if baseToken == tokenMintA {
                price = directPrice
            } else if baseToken == tokenMintB {
                price = invertedPrice
            }
        }
    }

    if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
        reason := "price was invalid"
        return shared.PriceResult{Valid: false, Reason: reason, PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "meteora", baseDecimals, quoteDecimals, directPrice, invertedPrice, price, nil, reason)}, nil
    }

    return shared.PriceResult{
        PairID:       pair.ID,
        Price:        price,
        PriceUSD:     price,
        LiquidityUSD: price * 1000,
        Valid:        true,
        Reason:       "ok",
        DebugInfo:    shared.BuildPriceDebugInfo(pair, "meteora", baseDecimals, quoteDecimals, directPrice, invertedPrice, price, nil, "ok"),
        FetchedAt:    time.Now().UTC(),
    }, nil
}

func calculateSolanaPrice(reserveA, reserveB *big.Int, decimalA, decimalB int, tokenAIsBase bool) float64 {
    if reserveA == nil || reserveB == nil || reserveA.Sign() == 0 || reserveB.Sign() == 0 {
        return 0
    }

    resA := new(big.Float).SetInt(reserveA)
    resB := new(big.Float).SetInt(reserveB)
    decA := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimalA)), nil))
    decB := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimalB)), nil))

    priceFloat := new(big.Float).Quo(new(big.Float).Quo(resB, decB), new(big.Float).Quo(resA, decA))
    price, _ := priceFloat.Float64()
    if !tokenAIsBase {
        if price == 0 {
            return 0
        }
        price = 1 / price
    }
    if math.IsNaN(price) || math.IsInf(price, 0) {
        return 0
    }
    return price
}

func calculateDLMMPrice(raw []byte, decimalBase, decimalQuote int) float64 {
    activeID, binStep, ok := readDLMMBinState(raw)
    if !ok || binStep == 0 {
        return 0
    }

    price := math.Pow(1+float64(binStep)/10000, float64(activeID))
    if math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
        return 0
    }

    if decimalBase != decimalQuote {
        price *= math.Pow10(decimalBase - decimalQuote)
    }
    return price
}

func readDLMMBinState(raw []byte) (int64, int64, bool) {
    const (
        discriminatorSize = 8
        parametersSize    = 32
        vParametersSize   = 32
        prefixSize        = 4
    )

    activeIDOffset := discriminatorSize + parametersSize + vParametersSize + prefixSize
    binStepOffset := activeIDOffset + 4

    if len(raw) < binStepOffset+2 {
        return 0, 0, false
    }

    activeID := int64(int32(binary.LittleEndian.Uint32(raw[activeIDOffset : activeIDOffset+4])))
    binStep := int64(binary.LittleEndian.Uint16(raw[binStepOffset : binStepOffset+2]))
    if activeID == 0 && binStep == 0 {
        return 0, 0, false
    }
    return activeID, binStep, true
}

func isDammV2Pair(dexName string) bool {
    dex := strings.ToLower(strings.TrimSpace(dexName))
    return strings.Contains(dex, "damm")
}

func chooseMeteoraPrice(directPrice, invertedPrice float64) float64 {
    if directPrice > 0 && !math.IsNaN(directPrice) && !math.IsInf(directPrice, 0) && directPrice <= 1e12 {
        return directPrice
    }
    if invertedPrice > 0 && !math.IsNaN(invertedPrice) && !math.IsInf(invertedPrice, 0) && invertedPrice <= 1e12 {
        return invertedPrice
    }
    return shared.ChooseSanePrice(directPrice, invertedPrice)
}

var meteoraCandidateOffsets = []int{64, 72, 80, 88, 96, 104, 112, 120, 32, 0, 16, 24}
var dammV2CandidateOffsets = []int{80, 88, 64, 72, 96, 104, 112, 120, 32, 0, 16, 24}

func readTokenMints(raw []byte) (string, string, bool) {
    const (
        tokenMintOffset = 88
        mintSize        = 32
    )
    if len(raw) < tokenMintOffset+2*mintSize {
        return "", "", false
    }
    return encodeBase58(raw[tokenMintOffset : tokenMintOffset+mintSize]), encodeBase58(raw[tokenMintOffset+mintSize : tokenMintOffset+2*mintSize]), true
}

func encodeBase58(input []byte) string {
    if len(input) == 0 {
        return ""
    }

    const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    zero := big.NewInt(0)
    base := big.NewInt(58)
    value := new(big.Int).SetBytes(input)
    if value.Sign() == 0 {
        return string(alphabet[0])
    }

    var encoded []byte
    for value.Cmp(zero) > 0 {
        rem := new(big.Int)
        value.DivMod(value, base, rem)
        encoded = append(encoded, alphabet[rem.Int64()])
    }

    for i, j := 0, len(encoded)-1; i < j; i, j = i+1, j-1 {
        encoded[i], encoded[j] = encoded[j], encoded[i]
    }

    return string(encoded)
}

func parseMeteoraAccountData(raw []byte) (*big.Int, *big.Int, error) {
    if len(raw) < 16 {
        return nil, nil, fmt.Errorf("Meteora account data too short: %d bytes", len(raw))
    }

    if reserveA, reserveB, ok := parseCandidateOffsets(raw, meteoraCandidateOffsets, binary.LittleEndian); ok {
        return reserveA, reserveB, nil
    }
    if reserveA, reserveB, ok := parseCandidateOffsets(raw, meteoraCandidateOffsets, binary.BigEndian); ok {
        return reserveA, reserveB, nil
    }

    return nil, nil, fmt.Errorf("unable to parse Meteora account data from %d bytes", len(raw))
}

func (a Adapter) parseDammV2AccountData(raw []byte, pair shared.Pair) (*big.Int, *big.Int, string, string, error) {
    if len(raw) < 16 {
        return nil, nil, "", "", fmt.Errorf("Damm v2 account data too short: %d bytes", len(raw))
    }

    if reserveA, reserveB, mintA, mintB, err := a.parseDammV2TokenAccounts(raw, pair); err == nil {
        return reserveA, reserveB, mintA, mintB, nil
    }
    if reserveA, reserveB, ok := parseCandidateOffsets(raw, dammV2CandidateOffsets, binary.LittleEndian); ok {
        return reserveA, reserveB, "", "", nil
    }
    if reserveA, reserveB, ok := parseCandidateOffsets(raw, dammV2CandidateOffsets, binary.BigEndian); ok {
        return reserveA, reserveB, "", "", nil
    }

    return nil, nil, "", "", fmt.Errorf("unable to parse DAMM v2 account data from %d bytes", len(raw))
}

type dammTokenAccountCandidate struct {
    mint   string
    amount *big.Int
    offset int
}

func (a Adapter) parseDammV2TokenAccounts(raw []byte, pair shared.Pair) (*big.Int, *big.Int, string, string, error) {
    var candidates []dammTokenAccountCandidate
    for offset := 128; offset+32 <= len(raw) && offset <= 320; offset += 8 {
        candidateKey := raw[offset : offset+32]
        if !hasNonZeroBytes(candidateKey) {
            continue
        }
        accountData, err := a.RPC.getAccountInfo(encodeBase58(candidateKey))
        if err != nil || len(accountData) != 165 {
            continue
        }
        if len(accountData) < 72 {
            continue
        }

        mint := encodeBase58(accountData[0:32])
        amount := binary.LittleEndian.Uint64(accountData[64:72])
        if amount == 0 {
            continue
        }

        candidates = append(candidates, dammTokenAccountCandidate{
            mint:   mint,
            amount: new(big.Int).SetUint64(amount),
            offset: offset,
        })
    }

    if len(candidates) < 2 {
        return nil, nil, "", "", fmt.Errorf("unable to resolve DAMM v2 reserve token accounts")
    }

    baseToken := strings.TrimSpace(pair.BaseToken)
    quoteToken := strings.TrimSpace(pair.QuoteToken)
    if baseToken != "" && quoteToken != "" {
        var baseAmount, quoteAmount *big.Int
        for _, candidate := range candidates {
            if candidate.mint == baseToken {
                baseAmount = candidate.amount
            }
            if candidate.mint == quoteToken {
                quoteAmount = candidate.amount
            }
        }
        if baseAmount != nil && quoteAmount != nil {
            return baseAmount, quoteAmount, baseToken, quoteToken, nil
        }
    }

    sort.Slice(candidates, func(i, j int) bool { return candidates[i].offset < candidates[j].offset })
    return candidates[0].amount, candidates[1].amount, candidates[0].mint, candidates[1].mint, nil
}

func parseCandidateOffsets(raw []byte, offsets []int, order binary.ByteOrder) (*big.Int, *big.Int, bool) {
    for _, offset := range offsets {
        if len(raw) < offset+16 {
            continue
        }
        reserveA := order.Uint64(raw[offset : offset+8])
        reserveB := order.Uint64(raw[offset+8 : offset+16])
        if isValidReservePair(reserveA, reserveB) {
            return new(big.Int).SetUint64(reserveA), new(big.Int).SetUint64(reserveB), true
        }
    }
    return nil, nil, false
}

func parseDammV2SqrtPrice(raw []byte) (*big.Int, bool) {
    offsets := []int{104, 456}
    for _, offset := range offsets {
        if len(raw) < offset+8 {
            continue
        }
        value := binary.LittleEndian.Uint64(raw[offset : offset+8])
        if value == 0 {
            continue
        }
        return new(big.Int).SetUint64(value), true
    }
    return nil, false
}

func hasNonZeroBytes(data []byte) bool {
    for _, b := range data {
        if b != 0 {
            return true
        }
    }
    return false
}

func isValidReservePair(a, b uint64) bool {
    if a == 0 || b == 0 {
        return false
    }
    if a > 1<<63 && b > 1<<63 {
        return false
    }
    return true
}

type RPCClient struct {
    Endpoint string
    Client   *http.Client
}

func (c *RPCClient) call(method string, params []any, result any) error {
    if c.Client == nil {
        c.Client = &http.Client{Timeout: 10 * time.Second}
    }

    payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s","params":%s}`, method, mustJSON(params))
    req, err := http.NewRequest(http.MethodPost, c.Endpoint, strings.NewReader(payload))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")

    resp, err := c.Client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode >= 400 {
        return fmt.Errorf("rpc status %d", resp.StatusCode)
    }
    if err := decodeJSON(resp.Body, result); err != nil {
        return err
    }
    return nil
}

func (c *RPCClient) getAccountInfo(account string) ([]byte, error) {
    var response struct {
        Result struct {
            Value struct {
                Data any `json:"data"`
            } `json:"value"`
        } `json:"result"`
        Error any `json:"error"`
    }

    if err := c.call("getAccountInfo", []any{account, map[string]any{"encoding": "base64"}}, &response); err != nil {
        return nil, err
    }
    if response.Error != nil {
        return nil, fmt.Errorf("rpc error: %v", response.Error)
    }

    var dataString string
    switch d := response.Result.Value.Data.(type) {
    case string:
        dataString = d
    case []any:
        if len(d) == 0 {
            return nil, fmt.Errorf("account data missing")
        }
        first, ok := d[0].(string)
        if !ok {
            return nil, fmt.Errorf("unexpected account data format")
        }
        dataString = first
    default:
        return nil, fmt.Errorf("unexpected account data format")
    }

    decoded, err := base64.StdEncoding.DecodeString(dataString)
    if err != nil {
        decoded, err = base64.RawStdEncoding.DecodeString(dataString)
        if err != nil {
            return nil, err
        }
    }
    return decoded, nil
}

func (c *RPCClient) getMintDecimals(mint string) (int, error) {
    data, err := c.getAccountInfo(mint)
    if err != nil {
        return 0, err
    }
    if len(data) < 45 {
        return 0, fmt.Errorf("mint account too short")
    }
    return int(data[44]), nil
}

func mustJSON(value any) string {
    data, err := json.Marshal(value)
    if err != nil {
        panic(fmt.Sprintf("json marshal failed: %v", err))
    }
    return string(data)
}

func decodeJSON(r io.Reader, out any) error {
    return json.NewDecoder(r).Decode(out)
}
