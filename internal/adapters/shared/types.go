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
// It prefers values within a practical range and, when several positive candidates
// are plausible, favors the smaller magnitude one to avoid inflated orientation
// artifacts from V4-style price calculations.
func ChooseSanePrice(candidates ...float64) float64 {
	var best float64
	var found bool
	for _, candidate := range candidates {
		if candidate <= 0 || math.IsNaN(candidate) || math.IsInf(candidate, 0) {
			continue
		}
		if !found {
			best = candidate
			found = true
			continue
		}
		if math.Abs(candidate) >= 1e-6 && math.Abs(candidate) <= 1e12 {
			if math.Abs(best) < 1e-6 || math.Abs(best) > 1e12 || math.Abs(candidate) < math.Abs(best) {
				best = candidate
			}
			continue
		}
		if math.Abs(best) < 1e-6 || math.Abs(best) > 1e12 {
			best = candidate
		}
	}
	if !found {
		return 0
	}
	return best
}

// Adapter defines the contract for a DEX-specific price fetcher.
type Adapter interface {
	Name() string
	Supports(pair Pair) bool
	FetchPrice(pair Pair) (PriceResult, error)
}
