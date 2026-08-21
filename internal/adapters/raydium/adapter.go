package raydium

import (
    "log"
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
			// Cross-validate the CLMM sqrt-price against a reserve-based price.
			// The brute-force scanner in parseCLMMPrice can accidentally match
			// non-price data (timestamps, fee rates, etc.) as a sqrt price,
			// producing extreme values like 25626 for a memecoin pair.
			if rp, rok := calculateCLMMReservePrice(data, pair.BaseToken, pair.QuoteToken, baseDecimals, quoteDecimals, a.RPC.getAccountInfo); rok {
				if math.Abs(price-rp)/math.Max(math.Abs(rp), 1e-12) > 100 {
					// CLMM sqrt-price diverges wildly from reserves — reject it
					// and fall through to the reserve-based calculation below.
					log.Printf("[raydium] CLMM sqrt-price %.4g rejected (reserve price %.4g, ratio %.1fx) for %s",
						price, rp, price/rp, pair.PoolAddress)
				} else {
					return shared.PriceResult{
						PairID:    pair.ID,
						Price:     price,
						PriceUSD:  price,
						Valid:     true,
						Reason:    "ok",
						FetchedAt: time.Now().UTC(),
					}, nil
				}
			} else {
				// No reserve price available for cross-validation — accept the CLMM price
				// if it looks sane (not astronomically large).
				if price > 0 && price < 1e12 {
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
		}
	}

    // For CPMM pools: read vault pubkeys directly from pool state at known offsets.
    // CP Swap PoolState layout (bytemuckunsafe, verified from raydium_cp_swap.json IDL):
    //   [8:40]    amm_config     pubkey
    //   [40:72]   pool_creator   pubkey
    //   [72:104]  token_0_vault  pubkey  ← vault for token_0
    //   [104:136] token_1_vault  pubkey  ← vault for token_1
    //   [136:168] lp_mint        pubkey
    //   [168:200] token_0_mint   pubkey  ← used to confirm orientation
    //   [200:232] token_1_mint   pubkey
    var baseBalance, quoteBalance *big.Int
    if strings.Contains(strings.ToLower(strings.TrimSpace(pair.DexName)), "cpmm") && len(data) >= 232 {
        vault0Addr := encodeBase58(data[72:104])
        vault1Addr := encodeBase58(data[104:136])
        mint0Addr  := encodeBase58(data[168:200])
        if vault0Addr != "" && vault1Addr != "" && mint0Addr != "" {
            v0Data, err0 := a.RPC.getAccountInfo(vault0Addr)
            v1Data, err1 := a.RPC.getAccountInfo(vault1Addr)
            if err0 == nil && err1 == nil && isLikelyTokenAccount(v0Data) && isLikelyTokenAccount(v1Data) {
                bal0, e0 := parseTokenAccountBalance(v0Data)
                bal1, e1 := parseTokenAccountBalance(v1Data)
                if e0 == nil && e1 == nil && bal0 != nil && bal1 != nil {
                    // vault0 holds token_0_mint; orient by comparing with DB base token
                    if strings.EqualFold(mint0Addr, pair.BaseToken) {
                        baseBalance, quoteBalance = bal0, bal1
                    } else {
                        // token_0 is the quote token
                        baseBalance, quoteBalance = bal1, bal0
                    }
                }
            }
        }
    }

    if baseBalance == nil || quoteBalance == nil {
        var err error
        baseBalance, quoteBalance, err = parsePoolReserves(data, pair.BaseToken, pair.QuoteToken, a.RPC.getAccountInfo)
        if err != nil {
            return shared.PriceResult{Valid: false, Reason: err.Error(), PairID: pair.ID}, nil
        }
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

