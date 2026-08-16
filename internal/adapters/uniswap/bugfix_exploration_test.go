package uniswap

import (
	"math/big"
	"strings"
	"testing"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5**
//
// TestBugConditionReversedTokenOrdering is a bug condition exploration test.
// This test encodes the EXPECTED behavior (correct price calculation) and will
// FAIL on unfixed code where token0IsBase is hardcoded to true.
//
// CRITICAL: This test MUST FAIL on unfixed code - failure confirms the bug exists.
// The test will pass AFTER the fix is implemented.
//
// Bug Condition: When a pool's token0 address does NOT match pair.BaseToken
// (reversed ordering), the unfixed adapter hardcodes token0IsBase=true and
// returns an inverted price.
//
// Expected Behavior: The fixed adapter should detect reversed token ordering
// and return the correct non-inverted price.
//
// APPROACH: Since we're testing on UNFIXED code that doesn't query token0/token1,
// we simulate the bug by setting up pairs where the pool's actual token ordering
// (encoded in reserves) is reversed from the database's base/quote designation.
func TestBugConditionReversedTokenOrdering(t *testing.T) {
	// Test Case 1: WBNB/USDT on PancakeSwap BSC (Reversed Ordering)
	// This is a concrete example from the design document where:
	// - Pool token0 = USDT (quote token)
	// - Pool token1 = WBNB (base token)
	// - Database expects BaseToken = WBNB, QuoteToken = USDT
	// - Unfixed code will return inverted price (USDT/WBNB ≈ 0.002)
	// - Fixed code should return correct price (WBNB/USDT ≈ 600)
	
	t.Run("WBNB_USDT_Reversed_BSC", func(t *testing.T) {
		// Mock RPC client that returns deterministic reserves
		// Using string parsing for large numbers to avoid overflow
		reserve0 := new(big.Int)
		reserve0.SetString("1000000000000000000000000", 10) // 1M USDT (18 decimals)
		reserve1 := new(big.Int)
		reserve1.SetString("1666000000000000000000", 10) // ~1666 WBNB (18 decimals)

		mockRPC := &mockRPCClientReversed{
			token0Address: "0x55d398326f99059fF775485246999027B3197955", // USDT
			token1Address: "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", // WBNB
			reserve0:      reserve0,
			reserve1:      reserve1,
		}

		adapter := Adapter{RPC: mockRPC}

		pair := shared.Pair{
			ID:                 "test-pair-1",
			Network:            "ethereum",
			DexName:            "uniswap-v2",
			PoolAddress:        "0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE",
			BaseToken:          "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", // WBNB (but it's token1 in pool)
			QuoteToken:         "0x55d398326f99059fF775485246999027B3197955", // USDT (but it's token0 in pool)
			BaseTokenDecimals:  18,
			QuoteTokenDecimals: 18,
			BaseSymbol:         "WBNB",
			QuoteSymbol:        "USDT",
		}

		result, err := adapter.FetchPrice(pair)
		if err != nil {
			t.Fatalf("FetchPrice returned error: %v", err)
		}

		// Expected behavior: Price should be WBNB/USDT ≈ 600
		// reserve1/reserve0 = 1666/1000000 ≈ 0.001666
		// But we want WBNB/USDT, which should be 1/0.001666 ≈ 600
		//
		// UNFIXED CODE BUG: Will calculate reserve1/reserve0 with token0IsBase=true
		// This gives USDT/WBNB = 1666/1000000 ≈ 0.001666 (INVERTED!)
		//
		// FIXED CODE: Will detect token0 != BaseToken, set token0IsBase=false,
		// and correctly invert to get WBNB/USDT ≈ 600

		expectedPrice := 600.0
		tolerance := 10.0 // Allow +/- 10 for this test

		if !result.Valid {
			t.Fatalf("Expected valid result, got invalid with reason: %s", result.Reason)
		}

		// Check if price is in the correct range (not inverted)
		if result.Price < (expectedPrice-tolerance) || result.Price > (expectedPrice+tolerance) {
			t.Errorf("COUNTEREXAMPLE FOUND: Price is inverted or incorrect!\n"+
				"  Expected: ~%.2f (WBNB/USDT)\n"+
				"  Got: %.6f\n"+
				"  This confirms the bug: unfixed code returns inverted price when token0 != BaseToken",
				expectedPrice, result.Price)
		}

		// Additional check: if price is extremely small, it's definitely inverted
		if result.Price < 0.01 {
			t.Errorf("COUNTEREXAMPLE: Price %.6f is extremely small (< 0.01), indicating price inversion.\n"+
				"  This happens because unfixed code hardcodes token0IsBase=true even when token0=QuoteToken.",
				result.Price)
		}
	})

	// Test Case 2: ETH/USDC Reversed Ordering
	// Similar scenario where token ordering in pool is opposite of semantic base/quote
	t.Run("ETH_USDC_Reversed", func(t *testing.T) {
		reserve0 := new(big.Int)
		reserve0.SetString("3000000000000", 10) // 3M USDC (6 decimals)
		reserve1 := new(big.Int)
		reserve1.SetString("1000000000000000000000", 10) // 1000 ETH (18 decimals)

		mockRPC := &mockRPCClientReversed{
			token0Address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC (6 decimals)
			token1Address: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", // WETH (18 decimals)
			reserve0:      reserve0,
			reserve1:      reserve1,
		}

		adapter := Adapter{RPC: mockRPC}

		pair := shared.Pair{
			ID:                 "test-pair-2",
			Network:            "ethereum",
			DexName:            "uniswap-v2",
			PoolAddress:        "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
			BaseToken:          "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", // WETH (but it's token1 in pool)
			QuoteToken:         "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC (but it's token0 in pool)
			BaseTokenDecimals:  18,
			QuoteTokenDecimals: 6,
			BaseSymbol:         "ETH",
			QuoteSymbol:        "USDC",
		}

		result, err := adapter.FetchPrice(pair)
		if err != nil {
			t.Fatalf("FetchPrice returned error: %v", err)
		}

		// Expected: ETH/USDC = ~3000 (1000 ETH / 3M USDC, with decimal adjustment)
		// With decimal adjustment: (1000 * 10^18) / (10^18) / (3M * 10^6 / 10^6) = 1000/3M * 10^12 = 0.000333... * 10^12 = 333,333...
		// Actually: reserve1/reserve0 with decimal normalization
		// = (1000 * 10^18 / 10^18) / (3M * 10^6 / 10^6) = 1000 / 3_000_000 = 0.000333...
		// We want ETH/USDC, so need to invert: 3_000_000 / 1000 = 3000
		
		expectedPrice := 3000.0
		tolerance := 500.0

		if !result.Valid {
			t.Fatalf("Expected valid result, got invalid with reason: %s", result.Reason)
		}

		if result.Price < (expectedPrice-tolerance) || result.Price > (expectedPrice+tolerance) {
			t.Errorf("COUNTEREXAMPLE FOUND: Price is inverted or incorrect!\n"+
				"  Expected: ~%.2f (ETH/USDC)\n"+
				"  Got: %.6f\n"+
				"  This confirms the bug: unfixed code returns inverted price when token0 != BaseToken",
				expectedPrice, result.Price)
		}

		if result.Price < 0.001 {
			t.Errorf("COUNTEREXAMPLE: Price %.6f is extremely small, indicating price inversion.", result.Price)
		}
	})

	// Test Case 3: Verify correct ordering still works (preservation test)
	// This tests that pools with correct ordering (token0 = BaseToken) still work
	t.Run("USDC_ETH_Correct_Ordering", func(t *testing.T) {
		reserve0 := new(big.Int)
		reserve0.SetString("3000000000000", 10) // 3M USDC (6 decimals)
		reserve1 := new(big.Int)
		reserve1.SetString("1000000000000000000000", 10) // 1000 ETH (18 decimals)

		mockRPC := &mockRPCClientReversed{
			token0Address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
			token1Address: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", // WETH
			reserve0:      reserve0,
			reserve1:      reserve1,
		}

		adapter := Adapter{RPC: mockRPC}

		pair := shared.Pair{
			ID:                 "test-pair-3",
			Network:            "ethereum",
			DexName:            "uniswap-v2",
			PoolAddress:        "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
			BaseToken:          "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC (token0 in pool - CORRECT)
			QuoteToken:         "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", // WETH (token1 in pool - CORRECT)
			BaseTokenDecimals:  6,
			QuoteTokenDecimals: 18,
			BaseSymbol:         "USDC",
			QuoteSymbol:        "ETH",
		}

		result, err := adapter.FetchPrice(pair)
		if err != nil {
			t.Fatalf("FetchPrice returned error: %v", err)
		}

		// Expected: USDC/ETH = 1/3000 = 0.000333...
		expectedPrice := 0.000333
		tolerance := 0.0001

		if !result.Valid {
			t.Fatalf("Expected valid result, got invalid with reason: %s", result.Reason)
		}

		if result.Price < (expectedPrice-tolerance) || result.Price > (expectedPrice+tolerance) {
			t.Errorf("Price calculation incorrect for correct token ordering!\n"+
				"  Expected: ~%.6f (USDC/ETH)\n"+
				"  Got: %.6f",
				expectedPrice, result.Price)
		}
	})
}

// mockRPCClientReversed is a mock RPC client for testing reversed token ordering scenarios
type mockRPCClientReversed struct {
	token0Address string
	token1Address string
	reserve0      *big.Int
	reserve1      *big.Int
	sqrtPriceX96  *big.Int
}

func (m *mockRPCClientReversed) getToken0(pool string) (string, error) {
	return strings.ToLower(m.token0Address), nil
}

func (m *mockRPCClientReversed) getToken1(pool string) (string, error) {
	return strings.ToLower(m.token1Address), nil
}

func (m *mockRPCClientReversed) getTokenDecimals(token string) (int, error) {
	token = strings.ToLower(strings.TrimSpace(token))
	switch token {
	case strings.ToLower("0x55d398326f99059ff775485246999027b3197955"):
		return 18, nil
	case strings.ToLower("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"):
		return 18, nil
	case strings.ToLower("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"):
		return 6, nil
	case strings.ToLower("0xc02aa39b223fe8d0a0e5c4f27ead9083c756cc2"):
		return 18, nil
	default:
		return 18, nil
	}
}

func (m *mockRPCClientReversed) getReserves(pool string) (*big.Int, *big.Int, error) {
	return m.reserve0, m.reserve1, nil
}

func (m *mockRPCClientReversed) getSlot0(pool string) (*big.Int, error) {
	if m.sqrtPriceX96 != nil {
		return m.sqrtPriceX96, nil
	}
	// Return default sqrtPriceX96 for 1:1 price ratio
	value := new(big.Int)
	value.SetString("79228162514264337593543950336", 10)
	return value, nil // sqrt(1) * 2^96
}

func (m *mockRPCClientReversed) getV4Slot0(network, poolID string) (*big.Int, error) {
	if m.sqrtPriceX96 != nil {
		return m.sqrtPriceX96, nil
	}
	value := new(big.Int)
	value.SetString("79228162514264337593543950336", 10)
	return value, nil
}
