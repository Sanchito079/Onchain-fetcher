package slipstream

import (
	"math"
	"math/big"
	"strings"
	"time"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// Adapter handles Slipstream/Aerodrome pool prices by querying the pool address directly.
type Adapter struct {
	RPC RPCClient
}

func (a Adapter) Name() string { return "slipstream" }

func (a Adapter) Supports(pair shared.Pair) bool {
	dex := strings.ToLower(pair.DexName)
	return strings.Contains(dex, "aerodrome") || strings.Contains(dex, "slipstream")
}

func (a Adapter) FetchPrice(pair shared.Pair) (shared.PriceResult, error) {
	if !a.Supports(pair) {
		return shared.PriceResult{Valid: false, Reason: "unsupported pair", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "unsupported", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "unsupported pair")}, nil
	}
	if pair.PoolAddress == "" {
		return shared.PriceResult{Valid: false, Reason: "missing pool address", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "missing-pool", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "missing pool address")}, nil
	}

	strategy := Router{}.Select(pair)

	token0IsBase := true
	token0OrderKnown := false
	baseDecimals := pair.BaseTokenDecimals
	quoteDecimals := pair.QuoteTokenDecimals
	resolvedBaseToken := pair.BaseToken
	resolvedQuoteToken := pair.QuoteToken

	baseToken := strings.ToLower(strings.TrimSpace(pair.BaseToken))
	quoteToken := strings.ToLower(strings.TrimSpace(pair.QuoteToken))
	if token0, err := a.RPC.getToken0(pair.PoolAddress); err == nil && strings.TrimSpace(token0) != "" {
		resolvedBaseToken, resolvedQuoteToken = shared.ResolveTokenPair(pair.BaseToken, pair.QuoteToken, token0, "")
		if baseToken != "" && strings.EqualFold(token0, baseToken) {
			token0IsBase = true
			token0OrderKnown = true
		} else if quoteToken != "" && strings.EqualFold(token0, quoteToken) {
			token0IsBase = false
			token0OrderKnown = true
		}
	}
	if token1, err := a.RPC.getToken1(pair.PoolAddress); err == nil && strings.TrimSpace(token1) != "" {
		resolvedBaseToken, resolvedQuoteToken = shared.ResolveTokenPair(resolvedBaseToken, resolvedQuoteToken, "", token1)
		if !token0OrderKnown {
			if baseToken != "" && strings.EqualFold(token1, baseToken) {
				token0IsBase = false
				token0OrderKnown = true
			} else if quoteToken != "" && strings.EqualFold(token1, quoteToken) {
				token0IsBase = true
				token0OrderKnown = true
			}
		}
	}
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

	token0Decimals := baseDecimals
	token1Decimals := quoteDecimals
	if token0OrderKnown {
		if token0, err := a.RPC.getToken0(pair.PoolAddress); err == nil && strings.TrimSpace(token0) != "" {
			if decimals, err := a.RPC.getTokenDecimals(token0); err == nil && decimals >= 0 {
				token0Decimals = decimals
			}
		}
		if token1, err := a.RPC.getToken1(pair.PoolAddress); err == nil && strings.TrimSpace(token1) != "" {
			if decimals, err := a.RPC.getTokenDecimals(token1); err == nil && decimals >= 0 {
				token1Decimals = decimals
			}
		}
	}

	var price float64
	var err error
	var sqrtPriceX96 *big.Int
	var directPrice float64
	var invertedPrice float64

	switch strategy {
	case "slot0":
		var innerErr error
		sqrtPriceX96, innerErr = a.RPC.getSlot0(pair.PoolAddress)
		if innerErr != nil {
			reserve0, reserve1, reserveErr := a.RPC.getReserves(pair.PoolAddress)
			if reserveErr != nil {
				return shared.PriceResult{Valid: false, Reason: innerErr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, 0, 0, 0, nil, innerErr.Error())}, innerErr
			}
			directPrice = calculateV2Price(reserve0, reserve1, baseDecimals, quoteDecimals, token0IsBase)
			invertedPrice = calculateV2Price(reserve0, reserve1, baseDecimals, quoteDecimals, !token0IsBase)
		} else {
			directPrice = calculateV3Price(sqrtPriceX96, baseDecimals, quoteDecimals, token0IsBase)
			invertedPrice = calculateV3Price(sqrtPriceX96, baseDecimals, quoteDecimals, !token0IsBase)
		}
	default:
		reserve0, reserve1, innerErr := a.RPC.getReserves(pair.PoolAddress)
		if innerErr != nil {
			return shared.PriceResult{Valid: false, Reason: innerErr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, 0, 0, 0, nil, innerErr.Error())}, innerErr
		}
		directPrice = calculateV2Price(reserve0, reserve1, token0Decimals, token1Decimals, token0IsBase)
		invertedPrice = calculateV2Price(reserve0, reserve1, token0Decimals, token1Decimals, !token0IsBase)
	}

	if token0OrderKnown {
		price = directPrice
		if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) || price > 1e12 {
			price = invertedPrice
		}
	} else {
		price = shared.ChooseSanePrice(directPrice, invertedPrice)
	}
	if price <= 0 || !isFinitePrice(price) || price > 1e12 {
		return shared.PriceResult{Valid: false, Reason: "price outside sensible range", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, directPrice, invertedPrice, price, sqrtPriceX96, "price outside sensible range")}, err
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
