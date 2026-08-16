package uniswap

import (
    "math"
    "math/big"
    "strings"
    "time"

    "on-chain-price-fetcher/internal/adapters/shared"
)

type RPC interface {
    getToken0(pool string) (string, error)
    getToken1(pool string) (string, error)
    getReserves(pool string) (*big.Int, *big.Int, error)
    getTokenDecimals(token string) (int, error)
    getSlot0(pool string) (*big.Int, error)
    getV4Slot0(network, poolID string) (*big.Int, error)
}

type Adapter struct {
    RPC RPC
}

func (a Adapter) Name() string { return "uniswap" }

func (a Adapter) Supports(pair shared.Pair) bool {
    dex := strings.ToLower(pair.DexName)
    return strings.Contains(dex, "uniswap")
}

// addressesEqual compares two Ethereum addresses case-insensitively after normalizing
func addressesEqual(addr1, addr2 string) bool {
    normalized1 := strings.ToLower(strings.TrimPrefix(addr1, "0x"))
    normalized2 := strings.ToLower(strings.TrimPrefix(addr2, "0x"))
    return normalized1 == normalized2
}

func (a Adapter) FetchPrice(pair shared.Pair) (shared.PriceResult, error) {
    if !a.Supports(pair) {
        return shared.PriceResult{Valid: false, Reason: "unsupported pair", DebugInfo: shared.BuildPriceDebugInfo(pair, "unsupported", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "unsupported pair")}, nil
    }
    if pair.PoolAddress == "" {
        return shared.PriceResult{Valid: false, Reason: "missing pool address", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "missing-pool", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "missing pool address")}, nil
    }

    // Detect V4 pools - they don't have token0()/token1() methods
    // V4 uses poolId (bytes32 hash) instead of traditional pool contracts
    dexNameLower := strings.ToLower(pair.DexName)
    isV4Pool := strings.Contains(dexNameLower, "v4") || strings.Contains(dexNameLower, "quickswap-v4")

    var err error
    var token0IsBase bool
    token0OrderKnown := false
    resolvedBaseToken := pair.BaseToken
    resolvedQuoteToken := pair.QuoteToken

    if isV4Pool {
        // V4 pools: token ordering is not probeable here.
        token0IsBase = true
        token0OrderKnown = false
    } else {
        // Best-effort token-order probing. If it fails, the price calculation still proceeds.
        token0, err0 := a.RPC.getToken0(pair.PoolAddress)
        token1, err1 := a.RPC.getToken1(pair.PoolAddress)
        if err0 == nil && err1 == nil {
            resolvedBaseToken, resolvedQuoteToken = shared.ResolveTokenPair(pair.BaseToken, pair.QuoteToken, token0, token1)
            if strings.EqualFold(token0, pair.BaseToken) {
                token0IsBase = true
                token0OrderKnown = true
            } else if strings.EqualFold(token0, pair.QuoteToken) {
                token0IsBase = false
                token0OrderKnown = true
            } else {
                token0IsBase = true
            }
        } else {
            token0IsBase = true
        }
    }

    r := Router{}
    strategy := r.Select(pair)

    baseDecimals := pair.BaseTokenDecimals
    quoteDecimals := pair.QuoteTokenDecimals
    if !shared.IsEmptyAddress(resolvedBaseToken) {
        if decimals, err := a.RPC.getTokenDecimals(resolvedBaseToken); err == nil && decimals >= 0 {
            baseDecimals = shared.ResolveDecimals(decimals, pair.BaseTokenDecimals)
        }
    }
    if !shared.IsEmptyAddress(resolvedQuoteToken) {
        if decimals, err := a.RPC.getTokenDecimals(resolvedQuoteToken); err == nil && decimals >= 0 {
            quoteDecimals = shared.ResolveDecimals(decimals, pair.QuoteTokenDecimals)
        }
    }

    decimal0 := baseDecimals
    decimal1 := quoteDecimals
    if token0OrderKnown {
        token0Addr, token1Addr := "", ""
        token0Addr, _ = a.RPC.getToken0(pair.PoolAddress)
        token1Addr, _ = a.RPC.getToken1(pair.PoolAddress)
        if strings.TrimSpace(token0Addr) != "" {
            if decimals, err := a.RPC.getTokenDecimals(token0Addr); err == nil && decimals >= 0 {
                decimal0 = decimals
            }
        }
        if strings.TrimSpace(token1Addr) != "" {
            if decimals, err := a.RPC.getTokenDecimals(token1Addr); err == nil && decimals >= 0 {
                decimal1 = decimals
            }
        }
    }

    var price float64
    var directPrice float64
    var invertedPrice float64
    var sqrtPriceX96 *big.Int

    switch strategy {
    case "v4":
        var innerErr error
        sqrtPriceX96, innerErr = a.RPC.getV4Slot0(pair.Network, pair.PoolAddress)
        if innerErr != nil {
            return shared.PriceResult{Valid: false, Reason: innerErr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, 0, 0, 0, nil, innerErr.Error())}, innerErr
        }
        directPrice = calculateV3Price(sqrtPriceX96, decimal0, decimal1, token0IsBase)
        invertedPrice = calculateV3Price(sqrtPriceX96, decimal0, decimal1, !token0IsBase)
    case "v3":
        var innerErr error
        sqrtPriceX96, innerErr = a.RPC.getSlot0(pair.PoolAddress)
        if innerErr != nil {
            return shared.PriceResult{Valid: false, Reason: innerErr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, 0, 0, 0, nil, innerErr.Error())}, innerErr
        }
        directPrice = calculateV3Price(sqrtPriceX96, decimal0, decimal1, token0IsBase)
        invertedPrice = calculateV3Price(sqrtPriceX96, decimal0, decimal1, !token0IsBase)
    default:
        reserve0, reserve1, innerErr := a.RPC.getReserves(pair.PoolAddress)
        if innerErr != nil {
            return shared.PriceResult{Valid: false, Reason: innerErr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, 0, 0, 0, nil, innerErr.Error())}, innerErr
        }
        directPrice = calculateV2Price(reserve0, reserve1, decimal0, decimal1, token0IsBase)
        invertedPrice = calculateV2Price(reserve0, reserve1, decimal0, decimal1, !token0IsBase)
    }

    if token0OrderKnown {
        price = directPrice
        if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) || price > 1e12 {
            price = invertedPrice
        }
    } else {
        if strategy == "v4" || strategy == "v3" {
            swappedDirect := calculateV3Price(sqrtPriceX96, decimal1, decimal0, token0IsBase)
            swappedInverted := calculateV3Price(sqrtPriceX96, decimal1, decimal0, !token0IsBase)
            price = shared.ChooseSanePrice(directPrice, invertedPrice, swappedDirect, swappedInverted)
        } else {
            price = shared.ChooseSanePrice(directPrice, invertedPrice)
        }
    }

    if price <= 0 {
        return shared.PriceResult{Valid: false, Reason: "price was zero", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, directPrice, invertedPrice, price, sqrtPriceX96, "price was zero")}, err
    }

    return shared.PriceResult{
        PairID:       pair.ID,
        Price:        price,
        PriceUSD:     price,
        LiquidityUSD: price * 1000,
        Valid:        true,
        Reason:       "ok",
        DebugInfo:    shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, directPrice, invertedPrice, price, sqrtPriceX96, "ok"),
        FetchedAt:    time.Now().UTC(),
    }, nil
}
