package pancakeswap

import (
	"math"
	"math/big"
	"strings"
	"time"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// Adapter handles PancakeSwap V2, V3, and CLMM-style pool prices.
type Adapter struct {
	RPC RPCClient
}

func (a Adapter) Name() string { return "pancakeswap" }

func (a Adapter) Supports(pair shared.Pair) bool {
	dex := strings.ToLower(pair.DexName)
	return strings.Contains(dex, "pancake") || strings.Contains(dex, "pancakeswap")
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

	// Detect V4/Infinity pools - they don't have token0()/token1() methods
	// Infinity pools use poolId (bytes32 hash) like Uniswap V4
	dexNameLower := strings.ToLower(pair.DexName)
	isInfinityPool := strings.Contains(dexNameLower, "infinity") || strings.Contains(dexNameLower, "v4")

	var err error
	var token0IsBase bool
	token0OrderKnown := false
	resolvedBaseToken := pair.BaseToken
	resolvedQuoteToken := pair.QuoteToken
	token0Decimals := pair.BaseTokenDecimals
	token1Decimals := pair.QuoteTokenDecimals

	if isInfinityPool {
		// Infinity/V4 pools: token ordering is not probeable here.
		token0IsBase = true
		token0OrderKnown = false
	} else {
		// Query token ordering from the pool contract when possible.
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

			if decimals, err := a.RPC.getTokenDecimals(token0); err == nil && decimals >= 0 {
				token0Decimals = decimals
			}
			if decimals, err := a.RPC.getTokenDecimals(token1); err == nil && decimals >= 0 {
				token1Decimals = decimals
			}
		} else {
			token0IsBase = true
		}
	}

	router := Router{}
	strategy := router.Select(pair)

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

	if token0OrderKnown {
		if token0IsBase {
			// token0 is the base token, so token0Decimals corresponds to baseDecimals.
			// If we were unable to read on-chain decimals, fall back to semantic values.
			if token0Decimals == 0 {
				token0Decimals = baseDecimals
			}
			if token1Decimals == 0 {
				token1Decimals = quoteDecimals
			}
		} else {
			if token0Decimals == 0 {
				token0Decimals = quoteDecimals
			}
			if token1Decimals == 0 {
				token1Decimals = baseDecimals
			}
		}
	}

	calcDecimals0 := token0Decimals
	calcDecimals1 := token1Decimals
	if !token0OrderKnown {
		calcDecimals0 = baseDecimals
		calcDecimals1 = quoteDecimals
	}

	var price float64
	var directPrice float64
	var invertedPrice float64
	var sqrtPriceX96 *big.Int

	switch strategy {
	case "v3-base", "v3":
		var innerErr error
		sqrtPriceX96, innerErr = a.RPC.getSlot0(pair.PoolAddress)
		if innerErr != nil {
			return shared.PriceResult{Valid: false, Reason: innerErr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, 0, 0, 0, nil, innerErr.Error())}, innerErr
		}
		directPrice = calculateV3Price(sqrtPriceX96, calcDecimals0, calcDecimals1, token0IsBase)
		invertedPrice = calculateV3Price(sqrtPriceX96, calcDecimals0, calcDecimals1, !token0IsBase)
	case "clmm":
		var innerErr error
		sqrtPriceX96, innerErr = a.RPC.getCLMMSlot0(pair.PoolAddress)
		if innerErr != nil {
			return shared.PriceResult{Valid: false, Reason: innerErr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, 0, 0, 0, nil, innerErr.Error())}, innerErr
		}
		directPrice = calculateCLMMPrice(sqrtPriceX96, calcDecimals0, calcDecimals1, token0IsBase)
		invertedPrice = calculateCLMMPrice(sqrtPriceX96, calcDecimals0, calcDecimals1, !token0IsBase)
	default:
		reserve0, reserve1, innerErr := a.RPC.getReserves(pair.PoolAddress)
		if innerErr != nil {
			return shared.PriceResult{Valid: false, Reason: innerErr.Error(), PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, strategy, baseDecimals, quoteDecimals, 0, 0, 0, nil, innerErr.Error())}, innerErr
		}
		directPrice = calculateV2Price(reserve0, reserve1, calcDecimals0, calcDecimals1, token0IsBase)
		invertedPrice = calculateV2Price(reserve0, reserve1, calcDecimals0, calcDecimals1, !token0IsBase)
	}

	if token0OrderKnown {
		price = directPrice
		if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) || price > 1e12 {
			price = invertedPrice
		}
	} else {
		price = shared.ChooseSanePrice(directPrice, invertedPrice)
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
