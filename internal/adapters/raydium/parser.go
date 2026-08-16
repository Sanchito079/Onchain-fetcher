package raydium

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"

	"on-chain-price-fetcher/internal/adapters/shared"
)

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
const tokenAccountMinLength = 72
const tokenAccountExpectedLength = 165

const (
	clmmStatePrefix      = 8
	// Official Raydium CLMM PoolState struct layout (#[repr(C, packed)]):
	// +8  discriminator (8 bytes)
	// +8  bump (1 byte) + amm_config (32) + owner (32)  = offset 9-72
	// +73 token_mint_0 (32 bytes)
	// +105 token_mint_1 (32 bytes)
	// +137 token_vault_0 (32) + token_vault_1 (32) + observation_key (32)
	// +233 mint_decimals_0 (1 byte)
	// +234 mint_decimals_1 (1 byte)
	// +235 tick_spacing (2 bytes)
	// +237 liquidity (16 bytes, u128)
	// +253 sqrt_price_x64 (16 bytes, u128)  ← confirmed from source
	clmmTokenMint0Offset        = 73
	clmmTokenMint1Offset        = 105
	clmmDecimals0Offset         = 233
	clmmDecimals1Offset         = 234
	clmmSqrtPriceX64Offset      = 253 // authoritative offset from official PoolState struct
)

var candidateOffsets = []int{336, 64, 72, 80, 88, 96, 104, 112, 120, 32, 24, 16, 8, 0}

// parsePoolReserves attempts to read reserve balances straight from the pool
// account first, then falls back to token-account resolution when the pool
// layout does not expose curve balances directly.
func parsePoolReserves(raw []byte, baseTokenMint, quoteTokenMint string, getAccountInfo func(string) ([]byte, error)) (*big.Int, *big.Int, error) {
	if getAccountInfo == nil {
		return nil, nil, fmt.Errorf("no account resolver provided")
	}

	if tokenBase, tokenQuote, err := resolveTokenAccountReserves(raw, baseTokenMint, quoteTokenMint, getAccountInfo); err == nil {
		return tokenBase, tokenQuote, nil
	}

	if baseBal, quoteBal, ok := parseCurveReserves(raw); ok {
		if orderedBase, orderedQuote, ok := orderCurveReservesByTokenMetadata(raw, baseBal, quoteBal, baseTokenMint, quoteTokenMint, getAccountInfo); ok {
			return orderedBase, orderedQuote, nil
		}
		if strings.TrimSpace(baseTokenMint) == "" || strings.TrimSpace(quoteTokenMint) == "" {
			return baseBal, quoteBal, nil
		}
		return baseBal, quoteBal, nil
	}

	return nil, nil, fmt.Errorf("could not resolve pool reserves")
}

func resolveTokenAccountReserves(raw []byte, baseTokenMint, quoteTokenMint string, getAccountInfo func(string) ([]byte, error)) (*big.Int, *big.Int, error) {
	if !looksLikeSolanaAddress(baseTokenMint) {
		baseTokenMint = ""
	}
	if !looksLikeSolanaAddress(quoteTokenMint) {
		quoteTokenMint = ""
	}
	candidates := findCandidatePubkeyPairs(raw)
	if len(candidates) == 0 {
		return nil, nil, fmt.Errorf("pool token account pubkeys not found")
	}

	maxLookupAttempts := 16
	attempts := 0
	for i := 0; i < len(candidates) && attempts < maxLookupAttempts; i++ {
		p := candidates[i]
		baseAcc := encodeBase58(p.base)
		quoteAcc := encodeBase58(p.quote)

		baseData, err := getAccountInfo(baseAcc)
		if err != nil {
			attempts++
			continue
		}
		quoteData, err := getAccountInfo(quoteAcc)
		if err != nil {
			attempts++
			continue
		}

		attempts++
		if !isLikelyTokenAccount(baseData) || !isLikelyTokenAccount(quoteData) {
			continue
		}

		baseBal, err := parseTokenAccountBalance(baseData)
		if err != nil {
			continue
		}
		quoteBal, err := parseTokenAccountBalance(quoteData)
		if err != nil {
			continue
		}

		if orderedBase, orderedQuote, ok := orderReservesByTokenMetadata(baseData, quoteData, baseTokenMint, quoteTokenMint); ok {
			return orderedBase, orderedQuote, nil
		}

		if strings.TrimSpace(baseTokenMint) != "" && strings.TrimSpace(quoteTokenMint) != "" {
			continue
		}

		return baseBal, quoteBal, nil
	}

	return nil, nil, fmt.Errorf("no valid token account pairs found for pool")
}

func parseCurveReserves(raw []byte) (*big.Int, *big.Int, bool) {
	if len(raw) < 16 {
		return nil, nil, false
	}

	baseAmount := binary.LittleEndian.Uint64(raw[0:8])
	quoteAmount := binary.LittleEndian.Uint64(raw[8:16])
	if baseAmount == 0 || quoteAmount == 0 {
		return nil, nil, false
	}

	return new(big.Int).SetUint64(baseAmount), new(big.Int).SetUint64(quoteAmount), true
}

func orderCurveReservesByTokenMetadata(raw []byte, reserveBase, reserveQuote *big.Int, baseTokenMint, quoteTokenMint string, getAccountInfo func(string) ([]byte, error)) (*big.Int, *big.Int, bool) {
	if reserveBase == nil || reserveQuote == nil {
		return nil, nil, false
	}
	if strings.TrimSpace(baseTokenMint) == "" || strings.TrimSpace(quoteTokenMint) == "" {
		return nil, nil, false
	}

	candidates := findCandidatePubkeyPairs(raw)
	for _, p := range candidates {
		baseAcc := encodeBase58(p.base)
		quoteAcc := encodeBase58(p.quote)

		baseData, err := getAccountInfo(baseAcc)
		if err != nil {
			continue
		}
		quoteData, err := getAccountInfo(quoteAcc)
		if err != nil {
			continue
		}
		if !isLikelyTokenAccount(baseData) || !isLikelyTokenAccount(quoteData) {
			continue
		}

		baseMint := parseTokenAccountMint(baseData)
		quoteMint := parseTokenAccountMint(quoteData)
		if strings.EqualFold(baseMint, baseTokenMint) && strings.EqualFold(quoteMint, quoteTokenMint) {
			return reserveBase, reserveQuote, true
		}
		if strings.EqualFold(baseMint, quoteTokenMint) && strings.EqualFold(quoteMint, baseTokenMint) {
			return reserveQuote, reserveBase, true
		}
	}

	return nil, nil, false
}

type pubkeyPair struct {
	base  []byte
	quote []byte
}

// findCandidatePubkeyPairs returns potential base/quote 32-byte pairs found in
// the raw pool account. We prioritize known candidate offsets then fall back to
// a sliding scan. The caller should validate the returned pubkeys by fetching
// their account info.
func findCandidatePubkeyPairs(raw []byte) []pubkeyPair {
	var out []pubkeyPair
	for _, off := range candidateOffsets {
		if len(raw) < off+64 {
			continue
		}
		baseKey := make([]byte, 32)
		quoteKey := make([]byte, 32)
		copy(baseKey, raw[off:off+32])
		copy(quoteKey, raw[off+32:off+64])
		if hasNonZeroBytes(baseKey) && hasNonZeroBytes(quoteKey) {
			out = append(out, pubkeyPair{base: baseKey, quote: quoteKey})
		}
	}

	// fallback: scan for any two 32-byte non-zero keys nearby
	for i := 0; i+64 <= len(raw); i += 8 {
		baseKey := make([]byte, 32)
		quoteKey := make([]byte, 32)
		copy(baseKey, raw[i:i+32])
		copy(quoteKey, raw[i+32:i+64])
		if hasNonZeroBytes(baseKey) && hasNonZeroBytes(quoteKey) {
			out = append(out, pubkeyPair{base: baseKey, quote: quoteKey})
		}
	}
	return out
}

func parsePoolTokenAccountPubkeys(raw []byte) (string, string, bool) {
	// look for two 32-byte non-zero slices at candidate offsets
	for _, off := range candidateOffsets {
		if len(raw) < off+64 {
			continue
		}
		baseKey := raw[off : off+32]
		quoteKey := raw[off+32 : off+64]
		if hasNonZeroBytes(baseKey) && hasNonZeroBytes(quoteKey) {
			return encodeBase58(baseKey), encodeBase58(quoteKey), true
		}
	}

	// fallback: scan for any two 32-byte non-zero keys nearby
	for i := 0; i+64 <= len(raw); i++ {
		baseKey := raw[i : i+32]
		quoteKey := raw[i+32 : i+64]
		if hasNonZeroBytes(baseKey) && hasNonZeroBytes(quoteKey) {
			return encodeBase58(baseKey), encodeBase58(quoteKey), true
		}
	}
	return "", "", false
}

func resolvePoolTokenMints(raw []byte, getAccountInfo func(string) ([]byte, error)) (string, string, error) {
	candidates := findCandidatePubkeyPairs(raw)
	if len(candidates) == 0 {
		return "", "", fmt.Errorf("pool token account pubkeys not found")
	}
	for _, p := range candidates {
		baseAcc := encodeBase58(p.base)
		quoteAcc := encodeBase58(p.quote)

		baseData, err := getAccountInfo(baseAcc)
		if err != nil {
			continue
		}
		quoteData, err := getAccountInfo(quoteAcc)
		if err != nil {
			continue
		}
		if !isLikelyTokenAccount(baseData) || !isLikelyTokenAccount(quoteData) {
			continue
		}

		baseMint := parseTokenAccountMint(baseData)
		quoteMint := parseTokenAccountMint(quoteData)
		if baseMint == "" || quoteMint == "" {
			continue
		}
		return baseMint, quoteMint, nil
	}
	return "", "", fmt.Errorf("no valid token account mint pair found")
}

func isLikelyTokenAccount(raw []byte) bool {
	if len(raw) < tokenAccountMinLength {
		return false
	}
	if len(raw) != tokenAccountExpectedLength {
		return false
	}
	return hasNonZeroBytes(raw[:32]) && hasNonZeroBytes(raw[32:64])
}

func parseTokenAccountBalance(raw []byte) (*big.Int, error) {
	if len(raw) < tokenAccountMinLength {
		return nil, fmt.Errorf("token account data too short: %d", len(raw))
	}
	amount := binary.LittleEndian.Uint64(raw[64:72])
	if amount == 0 {
		return nil, fmt.Errorf("token account balance is zero")
	}
	return new(big.Int).SetUint64(amount), nil
}

func parseTokenAccountMint(raw []byte) string {
	if len(raw) < 32 {
		return ""
	}
	return encodeBase58(raw[0:32])
}

func orderReservesByTokenMetadata(baseData, quoteData []byte, baseTokenMint, quoteTokenMint string) (*big.Int, *big.Int, bool) {
	if strings.TrimSpace(baseTokenMint) == "" || strings.TrimSpace(quoteTokenMint) == "" {
		return nil, nil, false
	}

	baseMint := parseTokenAccountMint(baseData)
	quoteMint := parseTokenAccountMint(quoteData)
	if baseMint == "" || quoteMint == "" {
		return nil, nil, false
	}

	if strings.EqualFold(baseMint, baseTokenMint) && strings.EqualFold(quoteMint, quoteTokenMint) {
		baseBal, err := parseTokenAccountBalance(baseData)
		if err != nil {
			return nil, nil, false
		}
		quoteBal, err := parseTokenAccountBalance(quoteData)
		if err != nil {
			return nil, nil, false
		}
		return baseBal, quoteBal, true
	}

	if strings.EqualFold(baseMint, quoteTokenMint) && strings.EqualFold(quoteMint, baseTokenMint) {
		baseBal, err := parseTokenAccountBalance(baseData)
		if err != nil {
			return nil, nil, false
		}
		quoteBal, err := parseTokenAccountBalance(quoteData)
		if err != nil {
			return nil, nil, false
		}
		return quoteBal, baseBal, true
	}

	return nil, nil, false
}

type clmmPoolState struct {
	TokenMint0   string
	TokenMint1   string
	Decimals0    int
	Decimals1    int
	SqrtPriceX64 *big.Int
}

func parseCLMMPoolState(raw []byte) (*clmmPoolState, bool) {
	if len(raw) < clmmSqrtPriceX64Offset+16 {
		return nil, false
	}

	tokenMint0 := encodeBase58(raw[clmmTokenMint0Offset : clmmTokenMint0Offset+32])
	tokenMint1 := encodeBase58(raw[clmmTokenMint1Offset : clmmTokenMint1Offset+32])
	if !looksLikeSolanaAddress(tokenMint0) || !looksLikeSolanaAddress(tokenMint1) {
		return nil, false
	}

	decimals0 := int(raw[clmmDecimals0Offset])
	decimals1 := int(raw[clmmDecimals1Offset])

	// Read sqrt_price_x64 as little-endian u128 from the authoritative offset (253).
	// This comes directly from the official PoolState struct layout:
	// #[repr(C, packed)] with liquidity at offset 237 (16 bytes) → sqrt_price_x64 at 253.
	lo := binary.LittleEndian.Uint64(raw[clmmSqrtPriceX64Offset : clmmSqrtPriceX64Offset+8])
	hi := binary.LittleEndian.Uint64(raw[clmmSqrtPriceX64Offset+8 : clmmSqrtPriceX64Offset+16])
	sqrtPrice := new(big.Int).SetUint64(hi)
	sqrtPrice.Lsh(sqrtPrice, 64)
	sqrtPrice.Or(sqrtPrice, new(big.Int).SetUint64(lo))

	if sqrtPrice == nil || sqrtPrice.Sign() == 0 {
		return nil, false
	}

	return &clmmPoolState{
		TokenMint0:   tokenMint0,
		TokenMint1:   tokenMint1,
		Decimals0:    decimals0,
		Decimals1:    decimals1,
		SqrtPriceX64: sqrtPrice,
	}, true
}

func calculateCLMMPriceFromState(sqrtPriceX64 *big.Int, baseDecimals, quoteDecimals int, token0IsBase bool) float64 {
	if sqrtPriceX64 == nil || sqrtPriceX64.Sign() == 0 {
		return 0
	}
	
	// Raydium CLMM uses sqrtPriceX64 (NOT X96 like Uniswap V3)
	priceRat := shared.ConvertSqrtPriceX64ToPrice(sqrtPriceX64)
	if priceRat == nil {
		return 0
	}
	
	adjusted := applyCLMMPriceOrientation(priceRat, baseDecimals, quoteDecimals, token0IsBase)
	if adjusted == nil {
		return 0
	}
	f, _ := adjusted.Float64()
	return f
}

func calculateCLMMReservePrice(raw []byte, baseMint, quoteMint string, baseDecimals, quoteDecimals int, getAccountInfo func(string) ([]byte, error)) (float64, bool) {
	if getAccountInfo == nil {
		return 0, false
	}
	baseBal, quoteBal, err := parsePoolReserves(raw, baseMint, quoteMint, getAccountInfo)
	if err != nil {
		return 0, false
	}
	price := calculateSolanaPrice(baseBal, quoteBal, baseDecimals, quoteDecimals)
	if !isSanePrice(price) {
		return 0, false
	}
	return price, true
}

func parseCLMMPrice(raw []byte, baseMint, quoteMint string, baseDecimals, quoteDecimals int, getAccountInfo func(string) ([]byte, error), token0OrderKnown bool, token0IsBase bool) (float64, bool) {
	if len(raw) == 0 {
		return 0, false
	}

	// Pre-compute a reserve-based price to validate candidate sqrt-price values.
	var reservePrice float64
	var haveReservePrice bool
	if getAccountInfo != nil && strings.TrimSpace(baseMint) != "" && strings.TrimSpace(quoteMint) != "" {
		if rp, ok := calculateCLMMReservePrice(raw, baseMint, quoteMint, baseDecimals, quoteDecimals, getAccountInfo); ok {
			reservePrice = rp
			haveReservePrice = true
		}
	}

	if state, ok := parseCLMMPoolState(raw); ok {
		if !token0OrderKnown && strings.TrimSpace(baseMint) != "" && strings.TrimSpace(quoteMint) != "" {
			if strings.EqualFold(state.TokenMint0, baseMint) && strings.EqualFold(state.TokenMint1, quoteMint) {
				token0OrderKnown = true
				token0IsBase = true
			} else if strings.EqualFold(state.TokenMint0, quoteMint) && strings.EqualFold(state.TokenMint1, baseMint) {
				token0OrderKnown = true
				token0IsBase = false
			}
		}

		if token0OrderKnown {
			if token0IsBase {
				baseDecimals = shared.ResolveDecimals(state.Decimals0, baseDecimals)
				quoteDecimals = shared.ResolveDecimals(state.Decimals1, quoteDecimals)
			} else {
				baseDecimals = shared.ResolveDecimals(state.Decimals1, baseDecimals)
				quoteDecimals = shared.ResolveDecimals(state.Decimals0, quoteDecimals)
			}
			
			// Calculate price using token0IsBase to apply correct orientation
			price := calculateCLMMPriceFromState(state.SqrtPriceX64, baseDecimals, quoteDecimals, token0IsBase)
			if isSanePrice(price) {
				return price, true
			}
		}

		// If the on-chain CLMM pool state is parseable, compare both orientations
		// before falling back to brute-force scanning. This covers token-order
		// mismatches where the DB metadata disagrees with the actual pool layout.
		if directPrice := calculateCLMMPriceFromState(state.SqrtPriceX64, baseDecimals, quoteDecimals, true); directPrice > 0 {
			inversePrice := calculateCLMMPriceFromState(state.SqrtPriceX64, baseDecimals, quoteDecimals, false)
			if chosen := chooseCLMMStatePrice(directPrice, inversePrice, reservePrice, haveReservePrice); chosen > 0 {
				if isSanePrice(chosen) {
					return chosen, true
				}
			}
		}
	}

	if getAccountInfo != nil && !token0OrderKnown && strings.TrimSpace(baseMint) != "" && strings.TrimSpace(quoteMint) != "" {
		if onChainBaseMint, onChainQuoteMint, err := resolvePoolTokenMints(raw, getAccountInfo); err == nil {
			if strings.EqualFold(onChainBaseMint, baseMint) && strings.EqualFold(onChainQuoteMint, quoteMint) {
				token0OrderKnown = true
				token0IsBase = true
			} else if strings.EqualFold(onChainBaseMint, quoteMint) && strings.EqualFold(onChainQuoteMint, baseMint) {
				token0OrderKnown = true
				token0IsBase = false
			}
		}
	}

	var best float64
	var bestBits int = -1

	// Raydium CLMM pools expose an on-chain sqrt-price field in the pool state.
	// We favor candidates that are large enough to look like a real sqrtPriceX96 or
	// sqrtPriceX64 value, and ignore tiny random numeric noise that would otherwise
	// produce a meaningless low price.
	for off := 0; off+8 <= len(raw) && off < 1024; off += 8 {
		value64 := binary.LittleEndian.Uint64(raw[off : off+8])
		if value64 == 0 {
			continue
		}

		checks := []*big.Int{new(big.Int).SetUint64(value64)}
		if off+16 <= len(raw) {
			u128 := new(big.Int)
			u128.SetBytes(reverseBytes(raw[off : off+16]))
			if u128.Sign() != 0 {
				checks = append(checks, u128)
			}
		}

		for _, candidate := range checks {
			if candidate == nil || candidate.Sign() == 0 {
				continue
			}
			if candidate.BitLen() < 32 || candidate.BitLen() > 256 {
				continue
			}

			if token0OrderKnown {
				for _, priceRat := range []*big.Rat{
					shared.ConvertSqrtPriceX96ToPrice(candidate),
					shared.ConvertSqrtPriceX64ToPrice(candidate),
				} {
					if adjusted := applyCLMMPriceOrientation(priceRat, baseDecimals, quoteDecimals, token0IsBase); adjusted != nil {
						if f, _ := adjusted.Float64(); isSanePrice(f) {
							// If we have a reserve-derived price, prefer candidates close to it.
							if haveReservePrice {
								if math.Abs(f-reservePrice)/math.Max(math.Abs(reservePrice), 1e-12) <= 0.5 {
									return f, true
								}
							}
							if candidate.BitLen() > bestBits {
								best = f
								bestBits = candidate.BitLen()
							}
						}
					}
				}
				continue
			}

			prices := []float64{}
			for _, priceRat := range []*big.Rat{
				shared.ConvertSqrtPriceX96ToPrice(candidate),
				shared.ConvertSqrtPriceX64ToPrice(candidate),
			} {
				if priceRat == nil {
					continue
				}

				// priceRat = token1/token0
				// Assume token0 = base, token1 = quote (direct orientation)
				if direct := shared.ApplyDecimalAdjustments(priceRat, quoteDecimals, baseDecimals); direct != nil {
					if f, _ := direct.Float64(); isSanePrice(f) {
						prices = append(prices, f)
					}
				}

				// Try inverse orientation: token0 = quote, token1 = base
				inverse := new(big.Rat).Inv(priceRat)
				// inverse = token0/token1 = quote/base
				if adjusted := shared.ApplyDecimalAdjustments(inverse, quoteDecimals, baseDecimals); adjusted != nil {
					if f, _ := adjusted.Float64(); isSanePrice(f) {
						prices = append(prices, f)
					}
				}
			}

			if len(prices) == 0 {
				continue
			}

			chosen := shared.ChooseSanePrice(prices...)
			if chosen <= 0 || math.IsNaN(chosen) || math.IsInf(chosen, 0) {
				continue
			}
			if haveReservePrice {
				if math.Abs(chosen-reservePrice)/math.Max(math.Abs(reservePrice), 1e-12) <= 0.5 {
					return chosen, true
				}
			}
			if candidate.BitLen() > bestBits {
				best = chosen
				bestBits = candidate.BitLen()
			}
		}
	}

	if bestBits < 0 {
		return 0, false
	}
	return best, true
}

func chooseCLMMStatePrice(directPrice, inversePrice, reservePrice float64, haveReservePrice bool) float64 {
	if directPrice > 0 && isSanePrice(directPrice) {
		if inversePrice > 0 && isSanePrice(inversePrice) {
			if haveReservePrice {
				if math.Abs(inversePrice-reservePrice) < math.Abs(directPrice-reservePrice) {
					return inversePrice
				}
				return directPrice
			}
			return shared.ChooseSanePrice(directPrice, inversePrice)
		}
		return directPrice
	}
	if inversePrice > 0 && isSanePrice(inversePrice) {
		return inversePrice
	}
	return 0
}

func applyCLMMPriceOrientation(priceRat *big.Rat, baseDecimals, quoteDecimals int, token0IsBase bool) *big.Rat {
	if priceRat == nil {
		return nil
	}

	// priceRat = (sqrtPriceX64 / 2^64)^2 = token1/token0 in RAW (atomic) units.
	//
	// To convert raw ratio to human-readable price (quote/base) we must
	// multiply by the ratio of token0 scale to token1 scale:
	//   price_human = priceRat × 10^(dec0) / 10^(dec1) = priceRat × 10^(dec0 - dec1)
	//
	// Case A: token0 = base (dec0=baseDecimals), token1 = quote (dec1=quoteDecimals)
	//   price_human = priceRat × 10^(baseDecimals - quoteDecimals)
	//   → ApplyDecimalAdjustments(priceRat, baseDecimals, quoteDecimals)
	//
	// Case B: token0 = quote (dec0=quoteDecimals), token1 = base (dec1=baseDecimals)
	//   priceRat = base/quote in raw → invert to get quote/base in raw
	//   inverted = 1/priceRat = token0/token1 in raw
	//   price_human = inverted × 10^(dec1 - dec0) = inverted × 10^(baseDecimals - quoteDecimals)
	//   → ApplyDecimalAdjustments(inverted, baseDecimals, quoteDecimals)

	oriented := new(big.Rat).Set(priceRat)

	if !token0IsBase {
		// invert: raw ratio is base/quote, need quote/base
		oriented.Inv(oriented)
	}

	// Both cases end up applying the same adjustment: × 10^(baseDecimals - quoteDecimals)
	return shared.ApplyDecimalAdjustments(oriented, baseDecimals, quoteDecimals)
}

func isSanePrice(value float64) bool {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return false
	}
	// Allow any positive finite price — memecoins can have 20+ zeros (e.g. 1e-25)
	// and high-value assets can be in the millions.
	// Only reject astronomically large values that are clearly calculation errors.
	return value < 1e15
}

func reverseBytes(data []byte) []byte {
	out := make([]byte, len(data))
	for i := 0; i < len(data); i++ {
		out[i] = data[len(data)-1-i]
	}
	return out
}

func hasNonZeroBytes(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return true
		}
	}
	return false
}

func looksLikeSolanaAddress(address string) bool {
	address = strings.TrimSpace(address)
	if address == "" {
		return false
	}
	if len(address) < 32 || len(address) > 44 {
		return false
	}
	for _, c := range address {
		if !strings.ContainsRune(base58Alphabet, c) {
			return false
		}
	}
	return true
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
