package orca

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strings"
	"time"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// Orca Whirlpool account layout (from official source):
// https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/whirlpool.rs
//
// With 8-byte Anchor discriminator prefix:
//  +8   whirlpools_config: Pubkey (32)
//  +40  whirlpool_bump: [u8;1]
//  +41  tick_spacing: u16
//  +43  fee_tier_index_seed: [u8;2]
//  +45  fee_rate: u16
//  +47  protocol_fee_rate: u16
//  +49  liquidity: u128 (16)
//  +65  sqrt_price: u128 (16)  ← price field
//  +81  tick_current_index: i32
//  +85  protocol_fee_owed_a: u64
//  +93  protocol_fee_owed_b: u64
//  +101 token_mint_a: Pubkey (32)
//  +133 token_vault_a: Pubkey (32)
//  +165 fee_growth_global_a: u128 (16)
//  +181 token_mint_b: Pubkey (32)
//  +213 token_vault_b: Pubkey (32)
const (
	whirlpoolSqrtPriceOffset = 65
	whirlpoolTokenMintAOffset = 101
	whirlpoolTokenMintBOffset = 181
	whirlpoolMinLen           = 213 + 32 // must cover token_mint_b
)

type RPCClient struct {
	Endpoint string
	Client   *http.Client
}

type Adapter struct {
	RPC RPCClient
}

func (a Adapter) Name() string { return "orca" }

func (a Adapter) Supports(pair shared.Pair) bool {
	if !strings.EqualFold(strings.TrimSpace(pair.Network), "solana") {
		return false
	}
	dex := strings.ToLower(strings.TrimSpace(pair.DexName))
	return strings.Contains(dex, "orca")
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

	state, ok := parseWhirlpoolState(data)
	if !ok {
		return shared.PriceResult{Valid: false, Reason: "failed to parse whirlpool state", PairID: pair.ID}, nil
	}

	// Resolve decimals from on-chain mint accounts, fall back to DB values
	baseDecimals := pair.BaseTokenDecimals
	quoteDecimals := pair.QuoteTokenDecimals
	if d, err := a.RPC.getMintDecimals(state.TokenMintA); err == nil && d >= 0 {
		_ = d // we'll assign below
	}
	if state.TokenMintA != "" {
		if d, err := a.RPC.getMintDecimals(state.TokenMintA); err == nil && d > 0 {
			// figure out which DB token matches mintA
			if strings.EqualFold(state.TokenMintA, pair.BaseToken) {
				baseDecimals = shared.ResolveDecimals(d, pair.BaseTokenDecimals)
			} else if strings.EqualFold(state.TokenMintA, pair.QuoteToken) {
				quoteDecimals = shared.ResolveDecimals(d, pair.QuoteTokenDecimals)
			}
		}
	}
	if state.TokenMintB != "" {
		if d, err := a.RPC.getMintDecimals(state.TokenMintB); err == nil && d > 0 {
			if strings.EqualFold(state.TokenMintB, pair.BaseToken) {
				baseDecimals = shared.ResolveDecimals(d, pair.BaseTokenDecimals)
			} else if strings.EqualFold(state.TokenMintB, pair.QuoteToken) {
				quoteDecimals = shared.ResolveDecimals(d, pair.QuoteTokenDecimals)
			}
		}
	}

	// Determine token order: mintA = token0, mintB = token1
	// sqrt_price = sqrt(token1/token0) × 2^64 → price = (sqrt_price/2^64)² × 10^(decA-decB)
	token0IsBase := true // default: mintA = base
	if state.TokenMintA != "" && state.TokenMintB != "" &&
		strings.TrimSpace(pair.BaseToken) != "" && strings.TrimSpace(pair.QuoteToken) != "" {
		if strings.EqualFold(state.TokenMintA, pair.BaseToken) && strings.EqualFold(state.TokenMintB, pair.QuoteToken) {
			token0IsBase = true
		} else if strings.EqualFold(state.TokenMintA, pair.QuoteToken) && strings.EqualFold(state.TokenMintB, pair.BaseToken) {
			token0IsBase = false
		}
	}

	price := calculateWhirlpoolPrice(state.SqrtPriceX64, baseDecimals, quoteDecimals, token0IsBase)
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return shared.PriceResult{Valid: false, Reason: "invalid price computed", PairID: pair.ID}, nil
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

// whirlpoolState holds the parsed fields we need from a Whirlpool account.
type whirlpoolState struct {
	SqrtPriceX64 *big.Int
	TokenMintA   string
	TokenMintB   string
	Decimals0    int // on-chain decimals for mint A (0 if not fetched)
	Decimals1    int // on-chain decimals for mint B (0 if not fetched)
}

func parseWhirlpoolState(raw []byte) (*whirlpoolState, bool) {
	if len(raw) < whirlpoolMinLen {
		return nil, false
	}

	// Read sqrt_price as little-endian u128 at offset 65
	lo := binary.LittleEndian.Uint64(raw[whirlpoolSqrtPriceOffset : whirlpoolSqrtPriceOffset+8])
	hi := binary.LittleEndian.Uint64(raw[whirlpoolSqrtPriceOffset+8 : whirlpoolSqrtPriceOffset+16])
	sqrtPrice := new(big.Int).SetUint64(hi)
	sqrtPrice.Lsh(sqrtPrice, 64)
	sqrtPrice.Or(sqrtPrice, new(big.Int).SetUint64(lo))

	if sqrtPrice.Sign() == 0 {
		return nil, false
	}

	tokenMintA := encodeBase58(raw[whirlpoolTokenMintAOffset : whirlpoolTokenMintAOffset+32])
	tokenMintB := encodeBase58(raw[whirlpoolTokenMintBOffset : whirlpoolTokenMintBOffset+32])

	return &whirlpoolState{
		SqrtPriceX64: sqrtPrice,
		TokenMintA:   tokenMintA,
		TokenMintB:   tokenMintB,
	}, true
}

// calculateWhirlpoolPrice converts a Whirlpool sqrtPriceX64 (u128, same Q64.64
// format as Raydium CLMM) to a human-readable quote/base price.
func calculateWhirlpoolPrice(sqrtPriceX64 *big.Int, baseDecimals, quoteDecimals int, token0IsBase bool) float64 {
	if sqrtPriceX64 == nil || sqrtPriceX64.Sign() == 0 {
		return 0
	}

	// price_raw = (sqrtPriceX64 / 2^64)^2  →  token1/token0 in atomic units
	one128 := new(big.Int).Lsh(big.NewInt(1), 128)
	r := new(big.Rat).SetInt(sqrtPriceX64)
	r.Mul(r, r)
	r.Quo(r, new(big.Rat).SetInt(one128))

	// Invert if token0 is quote (not base)
	if !token0IsBase {
		r.Inv(r)
	}

	// Apply decimal adjustment: × 10^(baseDecimals - quoteDecimals)
	adjusted := shared.ApplyDecimalAdjustments(r, baseDecimals, quoteDecimals)
	if adjusted == nil {
		return 0
	}
	f, _ := adjusted.Float64()
	return f
}

// ── RPC helpers ──────────────────────────────────────────────────────────────

func (c *RPCClient) call(method string, params []any, result any) error {
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 10 * time.Second}
	}
	payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s","params":%s}`, method, mustJSON(params))
	req, err := http.NewRequest(http.MethodPost, c.Endpoint, strings.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("rpc status %d", resp.StatusCode)
	}
	return decodeJSON(resp.Body, result)
}

func (c *RPCClient) getAccountInfo(account string) ([]byte, error) {
	if strings.TrimSpace(account) == "" {
		return nil, fmt.Errorf("empty account")
	}
	var response struct {
		Result struct {
			Value struct {
				Data any `json:"data"`
			} `json:"value"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := c.call("getAccountInfo", []any{account, map[string]any{"encoding": "base64"}}, &response); err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, fmt.Errorf("rpc error: %v", response.Error)
	}
	var dataString string
	switch d := response.Result.Value.Data.(type) {
	case string:
		dataString = d
	case []any:
		if len(d) == 0 {
			return nil, fmt.Errorf("account data missing")
		}
		first, ok := d[0].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected account data format")
		}
		dataString = first
	default:
		return nil, fmt.Errorf("unexpected account data format")
	}
	decoded, err := base64.StdEncoding.DecodeString(dataString)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(dataString)
		if err != nil {
			return nil, err
		}
	}
	return decoded, nil
}

func (c *RPCClient) getMintDecimals(mint string) (int, error) {
	if strings.TrimSpace(mint) == "" {
		return 0, fmt.Errorf("empty mint")
	}
	data, err := c.getAccountInfo(mint)
	if err != nil {
		return 0, err
	}
	// SPL Mint layout: decimals is at byte 44
	if len(data) < 45 {
		return 0, fmt.Errorf("mint account too short")
	}
	return int(data[44]), nil
}

// ── Utility ──────────────────────────────────────────────────────────────────

func encodeBase58(input []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	n := new(big.Int).SetBytes(input)
	if n.Sign() == 0 {
		return ""
	}
	base := big.NewInt(58)
	rem := new(big.Int)
	var encoded []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, rem)
		encoded = append(encoded, alphabet[rem.Int64()])
	}
	for i, j := 0, len(encoded)-1; i < j; i, j = i+1, j-1 {
		encoded[i], encoded[j] = encoded[j], encoded[i]
	}
	return string(encoded)
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func decodeJSON(r io.Reader, out any) error {
	return json.NewDecoder(r).Decode(out)
}
