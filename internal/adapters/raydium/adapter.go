package raydium

import (
    "math"
    "math/big"
    "strings"
    "time"

    "on-chain-price-fetcher/internal/adapters/shared"
)

type RPC interface {
    getAccountInfo(account string) ([]byte, error)
}

type Adapter struct {
    RPC RPCClient
}

func (a Adapter) resolveDecimals(baseMint, quoteMint string) (int, int, error) {
    // Try to read on-chain decimals. If RPC fails or returns invalid
    // values, return zeros so the caller can fall back to DB-supplied
    // decimals via shared.ResolveDecimals.
    var baseDecimals int
    var quoteDecimals int
    if strings.TrimSpace(baseMint) != "" {
        if d, err := a.RPC.getMintDecimals(baseMint); err == nil && d > 0 {
            baseDecimals = d
        }
    }
    if strings.TrimSpace(quoteMint) != "" {
        if d, err := a.RPC.getMintDecimals(quoteMint); err == nil && d > 0 {
            quoteDecimals = d
        }
    }
    return baseDecimals, quoteDecimals, nil
}

func (a Adapter) Name() string { return "raydium" }

func (a Adapter) Supports(pair shared.Pair) bool {
    if !strings.EqualFold(strings.TrimSpace(pair.Network), "solana") {
        return false
    }
    dex := strings.ToLower(strings.TrimSpace(pair.DexName))
    return strings.Contains(dex, "raydium")
}

func (a Adapter) FetchPrice(pair shared.Pair) (shared.PriceResult, error) {
    if !a.Supports(pair) {
        return shared.PriceResult{Valid: false, Reason: "unsupported pair", PairID: pair.ID}, nil
    }
    if pair.PoolAddress == "" {
        return shared.PriceResult{Valid: false, Reason: "missing pool address", PairID: pair.ID}, nil
    }
    data, err := a.RPC.getAccountInfo(pair.PoolAddress)
    if err != nil {
        return shared.PriceResult{Valid: false, Reason: err.Error(), PairID: pair.ID}, err
    }

    dbBaseToken := pair.BaseToken
    dbQuoteToken := pair.QuoteToken
    token0IsBase := true
    token0OrderKnown := false

    // For CLMM pools, use the pool state's token0/token1 mints for token order detection
    if strings.Contains(strings.ToLower(strings.TrimSpace(pair.DexName)), "clmm") {
        if state, ok := parseCLMMPoolState(data); ok {
            if strings.EqualFold(state.TokenMint0, dbBaseToken) && strings.EqualFold(state.TokenMint1, dbQuoteToken) {
                token0OrderKnown = true
                token0IsBase = true
            } else if strings.EqualFold(state.TokenMint0, dbQuoteToken) && strings.EqualFold(state.TokenMint1, dbBaseToken) {
                token0OrderKnown = true
                token0IsBase = false
            }
            pair.BaseToken, pair.QuoteToken = shared.ResolveTokenPair(pair.BaseToken, pair.QuoteToken, state.TokenMint0, state.TokenMint1)
        }
    } else {
        // For non-CLMM pools, use token account mints
        if actualBaseMint, actualQuoteMint, err := resolvePoolTokenMints(data, a.RPC.getAccountInfo); err == nil {
            if actualBaseMint != "" && actualQuoteMint != "" {
                if strings.EqualFold(actualBaseMint, dbBaseToken) && strings.EqualFold(actualQuoteMint, dbQuoteToken) {
                    token0OrderKnown = true
                    token0IsBase = true
                } else if strings.EqualFold(actualBaseMint, dbQuoteToken) && strings.EqualFold(actualQuoteMint, dbBaseToken) {
                    token0OrderKnown = true
                    token0IsBase = false
                }
            }
            pair.BaseToken, pair.QuoteToken = shared.ResolveTokenPair(pair.BaseToken, pair.QuoteToken, actualBaseMint, actualQuoteMint)
        }
    }

    // Resolve decimals: prefer on-chain values but fall back to DB decimals
    // when RPC lookups fail or are rate-limited. resolveDecimals returns
    // zero for any on-chain decimal it couldn't determine.
    onChainBaseDecimals, onChainQuoteDecimals, _ := a.resolveDecimals(pair.BaseToken, pair.QuoteToken)
    baseDecimals := shared.ResolveDecimals(onChainBaseDecimals, pair.BaseTokenDecimals)
    quoteDecimals := shared.ResolveDecimals(onChainQuoteDecimals, pair.QuoteTokenDecimals)

    // For Raydium CLMM pools, prefer on-chain sqrt price parsing over
    // reserve balance extraction. This is more reliable for CLMM/V4 layouts.
    if strings.Contains(strings.ToLower(strings.TrimSpace(pair.DexName)), "clmm") {
        if price, ok := parseCLMMPrice(data, pair.BaseToken, pair.QuoteToken, baseDecimals, quoteDecimals, a.RPC.getAccountInfo, token0OrderKnown, token0IsBase); ok {
            return shared.PriceResult{
                PairID:    pair.ID,
                Price:     price,
                PriceUSD:  price,
                Valid:     true,
                Reason:    "ok",
                FetchedAt: time.Now().UTC(),
            }, nil
        }
    }

    baseBalance, quoteBalance, err := parsePoolReserves(data, pair.BaseToken, pair.QuoteToken, a.RPC.getAccountInfo)
    if err != nil {
        return shared.PriceResult{Valid: false, Reason: err.Error(), PairID: pair.ID}, nil
    }

    directPrice := calculateSolanaPrice(baseBalance, quoteBalance, baseDecimals, quoteDecimals)
    invertedPrice := calculateSolanaPrice(quoteBalance, baseBalance, quoteDecimals, baseDecimals)
    price := chooseRaydiumPrice(directPrice, invertedPrice)
    if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
        return shared.PriceResult{Valid: false, Reason: "invalid price", PairID: pair.ID}, nil
    }

    return shared.PriceResult{
        PairID:    pair.ID,
        Price:     price,
        PriceUSD:  price,
        Valid:     true,
        Reason:    "ok",
        FetchedAt: time.Now().UTC(),
    }, nil
}

func chooseRaydiumPrice(directPrice, invertedPrice float64) float64 {
    // If both prices are valid, pick the smaller one — for memecoins with
    // tiny prices (e.g. SHIKOKU/SOL ≈ 1e-12), the "direct" balance order
    // from the pool may be inverted relative to the DB's base/quote order.
    // The correct price is always the one that is NOT astronomically large.
    dOk := directPrice > 0 && !math.IsNaN(directPrice) && !math.IsInf(directPrice, 0)
    iOk := invertedPrice > 0 && !math.IsNaN(invertedPrice) && !math.IsInf(invertedPrice, 0)

    if dOk && iOk {
        // Both are valid — pick the one that seems more reasonable.
        // If one is extremely large (>1e9) and the other is not, take the smaller one.
        dExtreme := directPrice > 1e9
        iExtreme := invertedPrice > 1e9
        if dExtreme && !iExtreme {
            return invertedPrice
        }
        if iExtreme && !dExtreme {
            return directPrice
        }
        // Neither or both extreme — use ChooseSanePrice
        return shared.ChooseSanePrice(directPrice, invertedPrice)
    }
    if dOk {
        return directPrice
    }
    if iOk {
        return invertedPrice
    }
    return 0
}

func calculateSolanaPrice(base, quote *big.Int, baseDecimals, quoteDecimals int) float64 {
    if base == nil || quote == nil {
        return 0
    }
    if base.Sign() == 0 || quote.Sign() == 0 {
        return 0
    }

    return normalizedSolanaPrice(base, quote, baseDecimals, quoteDecimals)
}

func normalizedSolanaPrice(base, quote *big.Int, baseDecimals, quoteDecimals int) float64 {
    if base == nil || quote == nil || base.Sign() == 0 {
        return 0
    }
    baseF, _ := new(big.Float).Quo(new(big.Float).SetInt(base), new(big.Float).SetFloat64(math.Pow10(baseDecimals))).Float64()
    quoteF, _ := new(big.Float).Quo(new(big.Float).SetInt(quote), new(big.Float).SetFloat64(math.Pow10(quoteDecimals))).Float64()
    if baseF == 0 {
        return 0
    }
    return quoteF / baseF
}

