package shared

import (
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
)

// Pair is the minimal shape used by the fetcher
// to read pool metadata from the local database layer.
type Pair struct {
	ID                 string
	Network            string
	DexName            string
	PoolAddress        string
	BaseToken          string
	QuoteToken         string
	BaseTokenDecimals  int
	QuoteTokenDecimals int
	BaseSymbol         string
	QuoteSymbol        string
}

// PriceResult is the normalized output returned by an adapter.
type PriceResult struct {
	PairID       string
	Price        float64
	PriceUSD     float64
	LiquidityUSD float64
	Valid        bool
	Reason       string
	DebugInfo    string
	FetchedAt    time.Time
}

// ResolveDecimals picks the most reliable decimal count for a token.
// On-chain values are preferred when available; the database value is used as fallback.
func ResolveDecimals(onChain int, dbValue int) int {
	if onChain > 0 {
		return onChain
	}
	return dbValue
}

// ResolveTokenPair uses the database metadata when it is usable and falls back to
// on-chain token0/token1 values when the DB addresses are empty or malformed.
func ResolveTokenPair(dbBase, dbQuote, onChainBase, onChainQuote string) (string, string) {
	resolvedBase := strings.TrimSpace(dbBase)
	resolvedQuote := strings.TrimSpace(dbQuote)
	if IsEmptyAddress(resolvedBase) {
		resolvedBase = strings.TrimSpace(onChainBase)
	}
	if IsEmptyAddress(resolvedQuote) {
		resolvedQuote = strings.TrimSpace(onChainQuote)
	}
	return normalizeAddress(resolvedBase), normalizeAddress(resolvedQuote)
}

func normalizeAddress(address string) string {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return ""
	}
	// If it already has a 0x prefix, normalize as EVM address
	if strings.HasPrefix(trimmed, "0x") {
		return strings.ToLower(trimmed)
	}
	// Solana base58 addresses are 32–44 chars and contain only base58 characters.
	// Do NOT prepend "0x" to Solana addresses — it breaks equality comparisons
	// with the on-chain decoded values which have no prefix.
	isSolanaLike := len(trimmed) >= 32 && len(trimmed) <= 44
	if isSolanaLike {
		// Check all chars are valid base58
		const b58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
		solana := true
		for _, c := range trimmed {
			if !strings.ContainsRune(b58, c) {
				solana = false
				break
			}
		}
		if solana {
			return trimmed // return as-is, no prefix
		}
	}
	// Default: treat as EVM, add 0x prefix
	return strings.ToLower("0x" + trimmed)
}

// IsEmptyAddress checks whether a token address is effectively missing.
func IsEmptyAddress(address string) bool {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return true
	}
	// Solana base58 addresses (32–44 chars, no 0x prefix) are valid
	if !strings.HasPrefix(trimmed, "0x") && len(trimmed) >= 32 && len(trimmed) <= 44 {
		return false
	}
	// EVM address check: strip 0x and require 40 hex chars
	hexPart := strings.TrimPrefix(trimmed, "0x")
	if len(hexPart) != 40 {
		return true
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return true
	}
	return false
}

// BuildPriceDebugInfo produces a compact block of calculation details for rejected pools.
func BuildPriceDebugInfo(pair Pair, strategy string, baseDecimals, quoteDecimals int, directPrice, invertedPrice, adjustedPrice float64, sqrtPriceX96 *big.Int, reason string) string {
	var builder strings.Builder
	baseLabel := pair.BaseSymbol
	if baseLabel == "" {
		baseLabel = pair.BaseToken
	}
	quoteLabel := pair.QuoteSymbol
	if quoteLabel == "" {
		quoteLabel = pair.QuoteToken
	}
	fmt.Fprintf(&builder, "Pool: %s\n", pair.PoolAddress)
	fmt.Fprintf(&builder, "Token0: %s\n", baseLabel)
	fmt.Fprintf(&builder, "Decimals0: %d\n", baseDecimals)
	fmt.Fprintf(&builder, "Token1: %s\n", quoteLabel)
	fmt.Fprintf(&builder, "Decimals1: %d\n", quoteDecimals)
	fmt.Fprintf(&builder, "Strategy: %s\n", strategy)
	if sqrtPriceX96 != nil {
		fmt.Fprintf(&builder, "sqrtPriceX96: %s\n", sqrtPriceX96.String())
	} else {
		builder.WriteString("sqrtPriceX96: <nil>\n")
	}
	fmt.Fprintf(&builder, "Raw Price: %.12g\n", directPrice)
	fmt.Fprintf(&builder, "Adjusted Price: %.12g\n", adjustedPrice)
	fmt.Fprintf(&builder, "Inverse Price: %.12g\n", invertedPrice)
	if reason == "" {
		reason = "unknown"
	}
	fmt.Fprintf(&builder, "Rejected Because: %s", reason)
	return builder.String()
}

// ChooseSanePrice selects the most plausible finite, positive candidate.
//
// Selection strategy (in priority order):
//  1. Prefer the first candidate that falls inside the "sane" range [1e-18, 1e12].
//     This is a broad range that covers memecoins (1e-18) through expensive assets
//     (BTC at ~1e5, a hypothetical asset at 1e12).
//  2. If no candidate is in range, return the first finite positive value found.
//  3. Returns 0 if all candidates are zero, negative, NaN, or infinite.
//
// We deliberately do NOT prefer smaller values. Choosing the smaller of two
// sane candidates (e.g. 0.000286 vs 3500) is just as likely to be wrong as
// choosing the larger one — the correct orientation must be established by
// the caller (token order resolution), not by magnitude heuristics.
// When token order is truly unknown the first sane value is the best we can do.
func ChooseSanePrice(candidates ...float64) float64 {
	var firstPositive float64
	for _, candidate := range candidates {
		if candidate <= 0 || math.IsNaN(candidate) || math.IsInf(candidate, 0) {
			continue
		}
		// Track the first positive finite value as ultimate fallback.
		if firstPositive == 0 {
			firstPositive = candidate
		}
		// Return the first candidate that is within a practical sane range.
		if candidate >= 1e-18 && candidate <= 1e12 {
			return candidate
		}
	}
	// No in-range candidate found — return first positive finite value (or 0).
	return firstPositive
}

// Adapter defines the contract for a DEX-specific price fetcher.
type Adapter interface {
	Name() string
	Supports(pair Pair) bool
	FetchPrice(pair Pair) (PriceResult, error)
}
