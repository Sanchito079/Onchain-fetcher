package pumpswap

import (
    "fmt"
    "math"
    "strings"
    "time"

    "on-chain-price-fetcher/internal/adapters/shared"
)

type RPC interface {
    getAccountInfo(pool string) ([]byte, error)
    getTokenAccountsByOwner(owner string) ([]TokenAccountInfo, error)
    getTokenDecimals(token string) (int, error)
}

type Adapter struct {
    RPC RPCClient
}

func (a Adapter) Name() string { return "pumpswap" }

func (a Adapter) Supports(pair shared.Pair) bool {
    if strings.ToLower(strings.TrimSpace(pair.Network)) != "solana" {
        return false
    }
    dex := strings.ToLower(strings.TrimSpace(pair.DexName))
    return strings.Contains(dex, "pumpswap") || strings.Contains(dex, "pump-fun") || strings.Contains(dex, "pump") || strings.Contains(dex, "pancake")
}

func (a Adapter) FetchPrice(pair shared.Pair) (shared.PriceResult, error) {
    if !a.Supports(pair) {
        return shared.PriceResult{Valid: false, Reason: "unsupported pair", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "unsupported", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "unsupported pair")}, nil
    }
    if pair.PoolAddress == "" {
        return shared.PriceResult{Valid: false, Reason: "missing pool address", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "missing-pool", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "missing pool address")}, nil
    }
    if pair.BaseTokenDecimals <= 0 || pair.QuoteTokenDecimals <= 0 {
        reason := fmt.Sprintf("missing decimals: base=%d quote=%d", pair.BaseTokenDecimals, pair.QuoteTokenDecimals)
        return shared.PriceResult{Valid: false, Reason: reason, PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "pumpswap", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, reason)}, nil
    }

    data, err := a.RPC.getAccountInfo(pair.PoolAddress)
    if err != nil {
        return shared.PriceResult{Valid: false, Reason: err.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "pumpswap", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, err.Error())}, err
    }

    if strings.Contains(strings.ToLower(pair.DexName), "pump-fun") || strings.Contains(strings.ToLower(pair.DexName), "pumpfun") || strings.Contains(strings.ToLower(pair.DexName), "pump_fun") {
        baseDecimals := pair.BaseTokenDecimals
        quoteDecimals := pair.QuoteTokenDecimals
        if baseDecimals == 18 || baseDecimals == 0 {
            baseDecimals = 6
        }
        if quoteDecimals != 9 {
            quoteDecimals = 9
        }

        curve, cerr := parsePumpFunBondingCurveAccount(data)
        if cerr != nil {
            return shared.PriceResult{Valid: false, Reason: cerr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "pump-fun", baseDecimals, quoteDecimals, 0, 0, 0, nil, cerr.Error())}, cerr
        }

        price := calculatePumpFunPrice(curve, baseDecimals, quoteDecimals)
        if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
            reason := "pump.fun price was invalid"
            return shared.PriceResult{Valid: false, Reason: reason, PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "pump-fun", baseDecimals, quoteDecimals, 0, 0, price, nil, reason)}, nil
        }

        return shared.PriceResult{
            PairID:       pair.ID,
            Price:        price,
            PriceUSD:     price,
            LiquidityUSD: price * 1000,
            Valid:        true,
            Reason:       "ok",
            DebugInfo:    shared.BuildPriceDebugInfo(pair, "pump-fun", baseDecimals, quoteDecimals, price, 1/price, price, nil, "ok"),
            FetchedAt:    time.Now().UTC(),
        }, nil
    }

    reserve0, reserve1, err := parsePumpSwapAccountDataWithRPC(data, pair.PoolAddress, pair.BaseToken, pair.QuoteToken, func(account string) ([]byte, error) {
        return a.RPC.getAccountInfo(account)
    }, func(owner string) ([]TokenAccountInfo, error) {
        return a.RPC.getTokenAccountsByOwner(owner)
    })
    // If the fixed-offset token-account parse succeeded, attempt to realign
    // reserves by mint so base/quote ordering is always correct.
    // Only overwrite if mint alignment actually resolves both tokens — never
    // fall through to the candidate byte-scan when we already have valid reserves.
    if err == nil {
        if mintMap, merr := parseReserveTokenAccountsByMint(data, func(acc string) ([]byte, error) { return a.RPC.getAccountInfo(acc) }); merr == nil {
            if baseReserve, quoteReserve, ok := orderReservesByTokenMetadata(mintMap, pair.BaseToken, pair.QuoteToken); ok {
                reserve0 = baseReserve
                reserve1 = quoteReserve
            }
            // If mint alignment couldn't match our tokens, keep the existing
            // reserve0/reserve1 from parseReserveTokenAccountsFromPool — don't guess.
        }
    }
    var directPrice, invertedPrice, price float64
    if err == nil {
        // Resolve decimals from the pool's embedded token-account mints first, then fall back to DB values.
        baseDecimals := pair.BaseTokenDecimals
        quoteDecimals := pair.QuoteTokenDecimals
        if details, derr := a.RPC.getPoolTokenAccountDetails(pair.PoolAddress, data); derr == nil {
            if strings.TrimSpace(details.BaseMint) != "" {
                if d, derr := a.RPC.getTokenDecimals(details.BaseMint); derr == nil && d >= 0 {
                    baseDecimals = shared.ResolveDecimals(d, pair.BaseTokenDecimals)
                }
            }
            if strings.TrimSpace(details.QuoteMint) != "" {
                if d, derr := a.RPC.getTokenDecimals(details.QuoteMint); derr == nil && d >= 0 {
                    quoteDecimals = shared.ResolveDecimals(d, pair.QuoteTokenDecimals)
                }
            }
        } else if strings.TrimSpace(pair.BaseToken) != "" {
            if d, derr := a.RPC.getTokenDecimals(pair.BaseToken); derr == nil && d >= 0 {
                baseDecimals = shared.ResolveDecimals(d, pair.BaseTokenDecimals)
            }
        }
        if strings.TrimSpace(pair.QuoteToken) != "" {
            if d, derr := a.RPC.getTokenDecimals(pair.QuoteToken); derr == nil && d >= 0 {
                quoteDecimals = shared.ResolveDecimals(d, pair.QuoteTokenDecimals)
            }
        }

        directPrice = calculateSolanaPrice(reserve0, reserve1, baseDecimals, quoteDecimals, true)
        invertedPrice = calculateSolanaPrice(reserve0, reserve1, baseDecimals, quoteDecimals, false)
        price = choosePumpSwapPrice(directPrice, invertedPrice)
    } else {
        return shared.PriceResult{Valid: false, Reason: err.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "pumpswap", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, err.Error())}, err
    }

    if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
        reason := "price was invalid"
        return shared.PriceResult{Valid: false, Reason: reason, PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "pumpswap", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, directPrice, invertedPrice, price, nil, reason)}, nil
    }

    return shared.PriceResult{
        PairID:       pair.ID,
        Price:        price,
        PriceUSD:     price,
        LiquidityUSD: price * 1000,
        Valid:        true,
        Reason:       "ok",
        DebugInfo:    shared.BuildPriceDebugInfo(pair, "pumpswap", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, directPrice, invertedPrice, price, nil, "ok"),
        FetchedAt:    time.Now().UTC(),
    }, nil
}
