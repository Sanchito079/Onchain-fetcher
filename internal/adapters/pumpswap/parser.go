package pumpswap

import (
    "encoding/binary"
    "fmt"
    "math"
    "math/big"
    "strings"
)

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

const (
    pumpSwapAccountDiscriminatorLength = 8
    pumpSwapPoolBaseTokenAccountOffset = pumpSwapAccountDiscriminatorLength + 1 + 2 + 4*32
    pumpSwapPoolQuoteTokenAccountOffset = pumpSwapPoolBaseTokenAccountOffset + 32
)

var pumpSwapCandidateOffsets = []int{64, 72, 80, 88, 96, 104, 112, 120, 32, 24, 16, 8, 0}

type PumpFunBondingCurve struct {
    VirtualTokenReserves *big.Int
    VirtualSOLReserves   *big.Int
    RealTokenReserves    *big.Int
    RealSOLReserves      *big.Int
    TokenTotalSupply     *big.Int
}

func parsePumpFunBondingCurveAccount(raw []byte) (*PumpFunBondingCurve, error) {
    if len(raw) < 48 {
        return nil, fmt.Errorf("pump.fun BondingCurve account data too short: %d bytes", len(raw))
    }

    // The first 8 bytes are the Anchor discriminator. The actual BondingCurve
    // struct starts immediately after that and follows the Pump AMM IDL order:
    // virtual_token_reserves, virtual_sol_reserves, real_token_reserves,
    // real_sol_reserves, token_total_supply.
    curve := &PumpFunBondingCurve{
        VirtualTokenReserves: new(big.Int).SetUint64(binary.LittleEndian.Uint64(raw[8:16])),
        VirtualSOLReserves:   new(big.Int).SetUint64(binary.LittleEndian.Uint64(raw[16:24])),
        RealTokenReserves:    new(big.Int).SetUint64(binary.LittleEndian.Uint64(raw[24:32])),
        RealSOLReserves:      new(big.Int).SetUint64(binary.LittleEndian.Uint64(raw[32:40])),
        TokenTotalSupply:     new(big.Int).SetUint64(binary.LittleEndian.Uint64(raw[40:48])),
    }
    if curve.VirtualTokenReserves.Sign() == 0 || curve.VirtualSOLReserves.Sign() == 0 {
        return nil, fmt.Errorf("pump.fun BondingCurve reserves are empty")
    }
    return curve, nil
}

func calculatePumpFunPrice(curve *PumpFunBondingCurve, baseDecimals, quoteDecimals int) float64 {
    if curve == nil || curve.VirtualTokenReserves == nil || curve.VirtualSOLReserves == nil {
        return 0
    }
    if curve.VirtualTokenReserves.Sign() == 0 || curve.VirtualSOLReserves.Sign() == 0 {
        return 0
    }
    if baseDecimals <= 0 {
        baseDecimals = 9
    }
    if quoteDecimals <= 0 {
        quoteDecimals = 9
    }

    // Pump.fun BondingCurve price = normalized SOL reserve / normalized token reserve.
    // Correct normalization for the live pool is SOL with 9 decimals and the token with 6 decimals.
    solScale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(quoteDecimals)), nil)
    tokenScale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(baseDecimals)), nil)

    solValue := new(big.Float).Quo(new(big.Float).SetInt(curve.VirtualSOLReserves), new(big.Float).SetInt(solScale))
    tokenValue := new(big.Float).Quo(new(big.Float).SetInt(curve.VirtualTokenReserves), new(big.Float).SetInt(tokenScale))
    if tokenValue.Sign() == 0 {
        return 0
    }

    priceFloat := new(big.Float).Quo(solValue, tokenValue)
    price, _ := priceFloat.Float64()
    if math.IsNaN(price) || math.IsInf(price, 0) {
        return 0
    }
    return price
}

func parsePumpSwapAccountData(raw []byte) (*big.Int, *big.Int, error) {
    if len(raw) < 16 {
        return nil, nil, fmt.Errorf("PumpSwap account data too short: %d bytes", len(raw))
    }

    if reserveA, reserveB, ok := parseCandidateOffsets(raw, pumpSwapCandidateOffsets, binary.LittleEndian); ok {
        return reserveA, reserveB, nil
    }

    if reserveA, reserveB, ok := parseCandidateOffsets(raw, pumpSwapCandidateOffsets, binary.BigEndian); ok {
        return reserveA, reserveB, nil
    }

    return nil, nil, fmt.Errorf("unable to parse PumpSwap account data from %d bytes", len(raw))
}

func parsePumpSwapAccountDataWithRPC(raw []byte, pool, baseToken, quoteToken string, getAccountInfo func(string) ([]byte, error), getTokenAccountsByOwner func(string) ([]TokenAccountInfo, error)) (*big.Int, *big.Int, error) {
    if getAccountInfo != nil {
        if reserveA, reserveB, err := parseReserveTokenAccountsFromPool(raw, getAccountInfo); err == nil {
            return reserveA, reserveB, nil
        }
    }

    if getTokenAccountsByOwner != nil {
        if reserveA, reserveB, err := parseReserveTokenAccountsByOwner(pool, baseToken, quoteToken, getTokenAccountsByOwner); err == nil {
            return reserveA, reserveB, nil
        }
    }

    if reserveA, reserveB, err := parsePumpSwapAccountData(raw); err == nil {
        return reserveA, reserveB, nil
    }

    if getAccountInfo == nil {
        return nil, nil, fmt.Errorf("unable to parse PumpSwap account data from %d bytes", len(raw))
    }

    reserveA, reserveB, err := parseReserveTokenAccounts(raw, baseToken, quoteToken, getAccountInfo)
    if err == nil {
        return reserveA, reserveB, nil
    }

    return nil, nil, err
}

func parseReserveTokenAccountsFromPool(raw []byte, getAccountInfo func(string) ([]byte, error)) (*big.Int, *big.Int, error) {
    if getAccountInfo == nil {
        return nil, nil, fmt.Errorf("no account data resolver available")
    }

    baseAccount, quoteAccount, ok := parsePoolTokenAccountPubkeys(raw)
    if !ok {
        return nil, nil, fmt.Errorf("pool token account pubkeys not found in account data")
    }

    baseAccountData, err := getAccountInfo(baseAccount)
    if err != nil {
        return nil, nil, err
    }
    quoteAccountData, err := getAccountInfo(quoteAccount)
    if err != nil {
        return nil, nil, err
    }

    baseBalance, err := parseTokenAccountBalance(baseAccountData)
    if err != nil {
        return nil, nil, err
    }
    quoteBalance, err := parseTokenAccountBalance(quoteAccountData)
    if err != nil {
        return nil, nil, err
    }

    return baseBalance, quoteBalance, nil
}

func parsePoolTokenAccountPubkeys(raw []byte) (string, string, bool) {
    if len(raw) < pumpSwapPoolQuoteTokenAccountOffset+32 {
        return "", "", false
    }

    baseAccountKey := raw[pumpSwapPoolBaseTokenAccountOffset : pumpSwapPoolBaseTokenAccountOffset+32]
    quoteAccountKey := raw[pumpSwapPoolQuoteTokenAccountOffset : pumpSwapPoolQuoteTokenAccountOffset+32]
    if !hasNonZeroBytes(baseAccountKey) || !hasNonZeroBytes(quoteAccountKey) {
        return "", "", false
    }

    return encodeBase58(baseAccountKey), encodeBase58(quoteAccountKey), true
}

type PoolTokenAccountDetails struct {
    BaseAccount string
    QuoteAccount string
    BaseMint    string
    QuoteMint   string
}

func parsePoolTokenAccountDetails(raw []byte, getAccountInfo func(string) ([]byte, error)) (*PoolTokenAccountDetails, error) {
    if getAccountInfo == nil {
        return nil, fmt.Errorf("no account data resolver available")
    }

    baseAccount, quoteAccount, ok := parsePoolTokenAccountPubkeys(raw)
    if !ok {
        return nil, fmt.Errorf("pool token account pubkeys not found in account data")
    }

    baseAccountData, err := getAccountInfo(baseAccount)
    if err != nil {
        return nil, err
    }
    quoteAccountData, err := getAccountInfo(quoteAccount)
    if err != nil {
        return nil, err
    }

    details := &PoolTokenAccountDetails{
        BaseAccount: baseAccount,
        QuoteAccount: quoteAccount,
        BaseMint: parseTokenAccountMint(baseAccountData),
        QuoteMint: parseTokenAccountMint(quoteAccountData),
    }

    if details.BaseMint == "" || details.QuoteMint == "" {
        return nil, fmt.Errorf("unable to resolve pool token account mints")
    }

    return details, nil
}

func parseTokenAccountMint(raw []byte) string {
    if len(raw) < 32 {
        return ""
    }
    return encodeBase58(raw[0:32])
}

func parseReserveTokenAccounts(raw []byte, baseToken, quoteToken string, getAccountInfo func(string) ([]byte, error)) (*big.Int, *big.Int, error) {
    // Backwards-compatible wrapper that prefers mint-aligned resolution
    mintMap, err := parseReserveTokenAccountsByMint(raw, getAccountInfo)
    if err == nil {
        if reserveA, reserveB, ok := orderReservesByTokenMetadata(mintMap, baseToken, quoteToken); ok {
            return reserveA, reserveB, nil
        }

        // return first two balances in deterministic order
        var found []*big.Int
        for _, v := range mintMap {
            found = append(found, v)
            if len(found) == 2 {
                break
            }
        }
        if len(found) >= 2 {
            return found[0], found[1], nil
        }
    }

    // Fallback: original candidate scanning when mint map fails
    candidates := collectReserveAccountCandidates(raw)
    if len(candidates) == 0 {
        return nil, nil, fmt.Errorf("no reserve token accounts found")
    }

    var balances []*big.Int
    for _, candidate := range candidates {
        accountData, err := getAccountInfo(candidate)
        if err != nil {
            continue
        }
        balance, err := parseTokenAccountBalance(accountData)
        if err != nil {
            continue
        }
        balances = append(balances, balance)
        if len(balances) == 2 {
            break
        }
    }

    if len(balances) < 2 {
        return nil, nil, fmt.Errorf("unable to resolve reserve token accounts")
    }
    return balances[0], balances[1], nil
}

func parseReserveTokenAccountsByOwner(pool, baseToken, quoteToken string, getTokenAccountsByOwner func(string) ([]TokenAccountInfo, error)) (*big.Int, *big.Int, error) {
    if getTokenAccountsByOwner == nil {
        return nil, nil, fmt.Errorf("no owner-based token account resolver available")
    }

    accounts, err := getTokenAccountsByOwner(pool)
    if err != nil {
        return nil, nil, err
    }
    if len(accounts) < 2 {
        return nil, nil, fmt.Errorf("owner-based token account query returned %d accounts", len(accounts))
    }

    balancesByMint := make(map[string]*big.Int)
    mintOrder := make([]string, 0, 2)
    for _, account := range accounts {
        if account.Balance == nil || account.Balance.Sign() == 0 {
            continue
        }
        if _, ok := balancesByMint[account.Mint]; ok {
            continue
        }
        balancesByMint[account.Mint] = account.Balance
        mintOrder = append(mintOrder, account.Mint)
        if len(balancesByMint) == 2 {
            break
        }
    }

    if len(balancesByMint) < 2 {
        return nil, nil, fmt.Errorf("unable to resolve two reserve token accounts by owner")
    }

    if reserveA, reserveB, ok := orderReservesByTokenMetadata(balancesByMint, baseToken, quoteToken); ok {
        return reserveA, reserveB, nil
    }

    return balancesByMint[mintOrder[0]], balancesByMint[mintOrder[1]], nil
}

func orderReservesByTokenMetadata(balances map[string]*big.Int, baseToken, quoteToken string) (*big.Int, *big.Int, bool) {
    baseToken = strings.TrimSpace(baseToken)
    quoteToken = strings.TrimSpace(quoteToken)
    if baseToken == "" || quoteToken == "" {
        return nil, nil, false
    }

    var reserveBase *big.Int
    var reserveQuote *big.Int
    for mint, amount := range balances {
        if strings.EqualFold(mint, baseToken) {
            reserveBase = amount
            continue
        }
        if strings.EqualFold(mint, quoteToken) {
            reserveQuote = amount
            continue
        }
    }
    if reserveBase != nil && reserveQuote != nil {
        return reserveBase, reserveQuote, true
    }
    return nil, nil, false
}

type TokenAccountInfo struct {
    Pubkey  string
    Mint    string
    Owner   string
    Balance *big.Int
}

// parseReserveTokenAccountsByMint queries candidate token accounts and returns a map
// of token mint (base58) -> balance for resolved token accounts. This allows
// adapters to align balances to the correct token mint instead of relying on
// candidate ordering.
func parseReserveTokenAccountsByMint(raw []byte, getAccountInfo func(string) ([]byte, error)) (map[string]*big.Int, error) {
    candidates := collectReserveAccountCandidates(raw)
    if len(candidates) == 0 {
        return nil, fmt.Errorf("no reserve token accounts found")
    }

    results := make(map[string]*big.Int)
    for _, candidate := range candidates {
        accountData, err := getAccountInfo(candidate)
        if err != nil {
            continue
        }
        balance, err := parseTokenAccountBalance(accountData)
        if err != nil {
            continue
        }

        // token account layout: mint (32) | owner (32) | amount (8)
        mintKey := accountData[0:32]
        encodedMint := encodeBase58(mintKey)
        if encodedMint == "" {
            continue
        }
        results[encodedMint] = balance
        if len(results) >= 2 {
            break
        }
    }

    if len(results) < 2 {
        return nil, fmt.Errorf("unable to resolve reserve token accounts by mint")
    }
    return results, nil
}

func collectReserveAccountCandidates(raw []byte) []string {
    if len(raw) < 32 {
        return nil
    }

    offsets := []int{72, 104, 136, 168, 200, 232, 264, 296, 328, 360}
    seen := make(map[string]struct{})
    candidates := make([]string, 0, 64)

    addCandidate := func(key []byte) {
        if len(key) < 32 || !hasNonZeroBytes(key) {
            return
        }
        encoded := encodeBase58(key)
        if encoded == "" {
            return
        }
        if _, exists := seen[encoded]; exists {
            return
        }
        seen[encoded] = struct{}{}
        candidates = append(candidates, encoded)
    }

    for _, offset := range offsets {
        if len(raw) < offset+32 {
            continue
        }
        addCandidate(raw[offset : offset+32])
    }

    for offset := 0; offset+32 <= len(raw); offset++ {
        addCandidate(raw[offset : offset+32])
        if len(candidates) >= 64 {
            break
        }
    }

    return candidates
}

func parseTokenAccountBalance(raw []byte) (*big.Int, error) {
    if len(raw) < 72 {
        return nil, fmt.Errorf("token account data too short: %d bytes", len(raw))
    }
    amount := binary.LittleEndian.Uint64(raw[64:72])
    if amount == 0 {
        return nil, fmt.Errorf("token account balance is zero")
    }
    return new(big.Int).SetUint64(amount), nil
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

func isValidReservePair(a, b uint64) bool {
    if a == 0 || b == 0 {
        return false
    }
    if a > 1<<63 && b > 1<<63 {
        return false
    }
    return true
}

func hasNonZeroBytes(data []byte) bool {
    for _, b := range data {
        if b != 0 {
            return true
        }
    }
    return false
}

func encodeBase58(input []byte) string {
    if len(input) == 0 {
        return ""
    }

    zeroes := 0
    for zeroes < len(input) && input[zeroes] == 0 {
        zeroes++
    }

    numerator := new(big.Int).SetBytes(input)
    if numerator.Sign() == 0 {
        return ""
    }

    var encoded []byte
    for numerator.Sign() > 0 {
        remainder := new(big.Int)
        numerator.DivMod(numerator, big.NewInt(58), remainder)
        encoded = append(encoded, base58Alphabet[remainder.Int64()])
    }

    for i := 0; i < zeroes; i++ {
        encoded = append(encoded, base58Alphabet[0])
    }

    for i, j := 0, len(encoded)-1; i < j; i, j = i+1, j-1 {
        encoded[i], encoded[j] = encoded[j], encoded[i]
    }

    return string(encoded)
}
