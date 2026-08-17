// Package watcher — program-level subscription watcher.
//
// Instead of subscribing to 30k individual pool accounts, we subscribe to
// Solana program log notifications. One subscription per DEX program catches
// ALL swaps across ALL pools of that program simultaneously.
//
// Flow:
//  1. logsSubscribe(mentions: [programId])           — one WS sub
//  2. Notification fires → parse logs for pool address + extract event data
//  3. Look up pool in in-memory map → get pair metadata
//  4. Compute price directly from the swap event blob (zero HTTP for Orca)
//     For Raydium: fall back to accountSubscribe-style price via public RPC
//  5. Write price + 24h stats to DB
package watcher

import (
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"on-chain-price-fetcher/internal/adapters/shared"
	"on-chain-price-fetcher/internal/stats"
)

// Known Solana DEX program IDs
const (
	ProgramRaydiumCLMM    = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
	ProgramRaydiumAMM     = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
	ProgramRaydiumCPMM    = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C" // Standard AMM (CPMM)
	ProgramOrcaWhirlpool  = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
	ProgramMeteoraDLMM    = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
	ProgramMeteoraDammV2  = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"
	ProgramMeteoraDamm    = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EkQXCEg"
	ProgramPumpSwap       = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" // Pump.fun bonding curve
	ProgramPumpFunAMM     = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA" // Pump.fun AMM (DEX pools)
)

// ProgramWatcher subscribes to ONE Solana program via logsSubscribe and
// updates prices for any pool in our DB that is touched by a transaction.
type ProgramWatcher struct {
	wsEndpoint  string
	programID   string
	db          *sql.DB

	// poolMap: pool address → pair metadata (loaded from DB at startup)
	mu      sync.RWMutex
	poolMap map[string]PairMeta

	// fetchFn is the HTTP fallback — called when in-event price fails
	fetchFn func(poolAddress string, pair shared.Pair) (float64, error)

	// priceCallback is called whenever a price is successfully computed,
	// with source="event" or source="http". May be nil.
	priceCallback func(poolAddress string, pair shared.Pair, price float64, source string)

	stopCh chan struct{}

	// Debounce: skip duplicate pool address within this window
	lastUpdateMu sync.Mutex
	lastUpdate   map[string]time.Time
	debounce     time.Duration
}

// NewProgramWatcher creates a watcher for a single program.
// pairs is the full set of pairs from the DB — we index them by pool address.
// fetchFn is the HTTP fallback called when in-event price extraction fails.
func NewProgramWatcher(
	wsEndpoint string,
	programID string,
	db *sql.DB,
	pairs []PairMeta,
	fetchFn func(poolAddress string, pair shared.Pair) (float64, error),
) *ProgramWatcher {
	return NewProgramWatcherWithCallback(wsEndpoint, programID, db, pairs, fetchFn, nil)
}

// NewProgramWatcherWithCallback is like NewProgramWatcher but also accepts a
// priceCallback that fires on every successful price update, indicating whether
// the price came from "event" (zero HTTP) or "http" (fallback RPC call).
func NewProgramWatcherWithCallback(
	wsEndpoint string,
	programID string,
	db *sql.DB,
	pairs []PairMeta,
	fetchFn func(poolAddress string, pair shared.Pair) (float64, error),
	priceCallback func(poolAddress string, pair shared.Pair, price float64, source string),
) *ProgramWatcher {
	poolMap := make(map[string]PairMeta, len(pairs))
	for _, p := range pairs {
		trim := strings.TrimSpace(p.PoolAddress)
		poolMap[trim] = p
		poolMap[strings.ToLower(trim)] = p
	}
	return &ProgramWatcher{
		wsEndpoint:    wsEndpoint,
		programID:     programID,
		db:            db,
		poolMap:       poolMap,
		fetchFn:       fetchFn,
		priceCallback: priceCallback,
		stopCh:        make(chan struct{}),
		lastUpdate:    make(map[string]time.Time),
		debounce:      500 * time.Millisecond,
	}
}

// AddPair adds or updates a pool in the in-memory map (for hot-reloading).
func (w *ProgramWatcher) AddPair(p PairMeta) {
	w.mu.Lock()
	trim := strings.TrimSpace(p.PoolAddress)
	w.poolMap[trim] = p
	w.poolMap[strings.ToLower(trim)] = p
	w.mu.Unlock()
}

// Start connects and listens, reconnecting on disconnect.
func (w *ProgramWatcher) Start() {
	log.Printf("[prog/%s] starting program watcher", shortID(w.programID))
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}
		if err := w.connectAndListen(); err != nil {
			log.Printf("[prog/%s] disconnected: %v — reconnecting in 5s", shortID(w.programID), err)
			time.Sleep(5 * time.Second)
		}
	}
}

// Stop shuts down the watcher.
func (w *ProgramWatcher) Stop() {
	close(w.stopCh)
}

func (w *ProgramWatcher) connectAndListen() error {
	conn, _, err := websocket.DefaultDialer.Dial(w.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Keep the connection alive with ping/pong. Extend read deadline on pong.
	conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	conn.SetPongHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		return nil
	})
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()
	// Ping sender
	go func() {
		for {
			select {
			case <-pingTicker.C:
				// ignore write errors; reader will notice broken conn
				_ = conn.WriteMessage(websocket.PingMessage, nil)
			case <-w.stopCh:
				return
			}
		}
	}()

	// Subscribe to program logs
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "logsSubscribe",
		"params": []any{
			map[string]any{
				"mentions": []string{w.programID},
			},
			map[string]any{
				"commitment": "confirmed",
			},
		},
	}
	if err := conn.WriteJSON(req); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Printf("[prog/%s] subscribed to program logs", shortID(w.programID))

	for {
		select {
		case <-w.stopCh:
			return nil
		default:
		}

		// keep read deadline somewhat generous; pong handler extends it
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if strings.EqualFold(strings.TrimSpace(os.Getenv("PROGRAM_WATCHER_DEBUG_LOGS")), "true") {
			log.Printf("[prog/%s] raw websocket msg type=%d len=%d", shortID(w.programID), msgType, len(msg))
			log.Printf("[prog/%s] raw websocket data: %s", shortID(w.programID), string(msg))
		}
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		w.handleMessage(msg)
	}
}

type logsNotification struct {
	Method string `json:"method"`
	Params *struct {
		Subscription int `json:"subscription"`
		Result       *struct {
			Value *struct {
				Signature string   `json:"signature"`
				Logs      []string `json:"logs"`
				Err       any      `json:"err"`
			} `json:"value"`
		} `json:"result"`
	} `json:"params"`
	// Subscription confirmation or RPC error
	ID    *int            `json:"id"`
	Error json.RawMessage `json:"error"`
	Result json.RawMessage `json:"result"`
}

func (w *ProgramWatcher) handleMessage(msg []byte) {
	var notif logsNotification
	if err := json.Unmarshal(msg, &notif); err != nil {
		return
	}

	// Subscription confirmation
	if notif.ID != nil {
		if len(notif.Error) > 0 {
			log.Printf("[prog/%s] subscription failed: %s", shortID(w.programID), strings.TrimSpace(string(notif.Error)))
			return
		}
		log.Printf("[prog/%s] subscription confirmed", shortID(w.programID))
		return
	}

	if notif.Method != "logsNotification" || notif.Params == nil {
		return
	}
	result := notif.Params.Result
	if result == nil || result.Value == nil {
		return
	}

	// Skip failed transactions
	if result.Value.Err != nil {
		return
	}

	// Debug raw logs if enabled
	if strings.EqualFold(strings.TrimSpace(os.Getenv("PROGRAM_WATCHER_DEBUG_LOGS")), "true") {
		log.Printf("[prog/%s] raw logs:\n%s", shortID(w.programID), strings.Join(result.Value.Logs, "\n"))
	}

	// Parse logs to find which pool address was involved.
	// Most notifications will be for pools we don't track — silently skip them.
	
	poolAddress := w.extractPoolAddress(result.Value.Logs)
	if poolAddress == "" {
		log.Printf("[prog/%s] no tracked pool found in logs (sig=%s)", shortID(w.programID), result.Value.Signature)
		return
	}
	poolAddress = strings.TrimSpace(poolAddress)
	log.Printf("[prog/%s] decoded pool address %s", shortID(w.programID), poolAddress)

	// Look up pair metadata (normalize case to avoid base58 case mismatches)
	poolAddressKey := strings.ToLower(poolAddress)
	w.mu.RLock()
	pair, ok := w.poolMap[poolAddressKey]
	w.mu.RUnlock()
	if !ok {
		// Pool isn't in our DB — silently ignore it.
		return
	}

	// Debounce — skip if we updated this pool very recently
	w.lastUpdateMu.Lock()
	last, seen := w.lastUpdate[poolAddress]
	if seen && time.Since(last) < w.debounce {
		w.lastUpdateMu.Unlock()
		return
	}
	w.lastUpdate[poolAddress] = time.Now()
	w.lastUpdateMu.Unlock()

	log.Printf("[prog/%s] pool address %s tracked in DB → %s/%s (binStep=%d)", shortID(w.programID), poolAddress, pair.BaseSymbol, pair.QuoteSymbol, pair.BinStep)

	// Extract the raw event bytes for this pool from the logs so we can
	// compute the price directly without an HTTP round-trip.
	eventData := w.extractEventDataForPool(result.Value.Logs, poolAddress)
	log.Printf("[prog/%s] extractEventData returned len=%d for pool %s", shortID(w.programID), len(eventData), poolAddress)

	// Fetch price asynchronously to not block the WS reader
	go w.fetchAndUpdate(pair, eventData)
}

func (w *ProgramWatcher) fetchAndUpdate(pair PairMeta, eventData []byte) {
	sharedPair := shared.Pair{
		ID:                 pair.ID,
		Network:            "solana",
		DexName:            pair.DexName,
		PoolAddress:        pair.PoolAddress,
		BaseToken:          pair.BaseToken,
		QuoteToken:         pair.QuoteToken,
		BaseTokenDecimals:  pair.BaseTokenDecimals,
		QuoteTokenDecimals: pair.QuoteTokenDecimals,
		BaseSymbol:         pair.BaseSymbol,
		QuoteSymbol:        pair.QuoteSymbol,
	}

	// Try to extract price directly from the event bytes embedded in the WS
	// notification — zero HTTP calls required.
	source := "http"
	var price float64
	if len(eventData) > 0 {
		price = w.priceFromEventData(eventData, pair)
		if price > 0 {
			source = "event"
			log.Printf("[prog/%s] ✓ %s/%s → %.10g (from event)", shortID(w.programID), pair.BaseSymbol, pair.QuoteSymbol, price)
		} else {
			// Debug: log why event price extraction failed
			log.Printf("[prog/%s] event data len=%d but price=0 for %s (binStep=%d)", 
				shortID(w.programID), len(eventData), pair.PoolAddress, pair.BinStep)
		}
	} else {
		// Debug: log when event data is empty
		log.Printf("[prog/%s] no event data for %s, will use HTTP fallback", 
			shortID(w.programID), pair.PoolAddress)
	}

	// Fall back to the adapter's fetchFn (HTTP) only if we couldn't compute in-event.
	if price <= 0 {
		var err error
		price, err = w.fetchFn(pair.PoolAddress, sharedPair)
		if err != nil {
			log.Printf("[prog/%s] price fetch failed for %s: %v", shortID(w.programID), pair.PoolAddress, err)
			return
		}
		if price > 0 {
			log.Printf("[prog/%s] ✓ %s/%s → %.10g (from http)", shortID(w.programID), pair.BaseSymbol, pair.QuoteSymbol, price)
		}
	}

	if price <= 0 {
		log.Printf("[prog/%s] price invalid for %s: %.10g", shortID(w.programID), pair.PoolAddress, price)
		return
	}

	if err := stats.UpdatePriceWithStats(w.db, pair.ID, price, price); err != nil {
		log.Printf("[prog/%s] DB update failed for %s: %v", shortID(w.programID), pair.ID, err)
		return
	}

	// Fire the price callback so callers can log/record the hit with source info.
	if w.priceCallback != nil {
		w.priceCallback(pair.PoolAddress, sharedPair, price, source)
	}
}

// priceFromEventData computes the price directly from the Anchor event bytes
// that are already embedded in the logsSubscribe notification — no HTTP needed.
//
// Raydium CLMM SwapEvent layout (after 8-byte discriminator):
//   [8:40]   poolState: Pubkey
//   [40:72]  sender: Pubkey
//   [72:104] tokenAccount0: Pubkey
//   [104:136] tokenAccount1: Pubkey
//   [136:144] amount0: u64
//   [144:152] transferFee0: u64
//   [152:160] amount1: u64
//   [160:168] transferFee1: u64
//   [168]    zeroForOne: bool
//   [169:185] sqrtPriceX64: u128  ← post-swap price (what we need)
//   [185:201] liquidity: u128
//   [201:205] tick: i32
//   ...
//
// Orca Whirlpool "Traded" event layout (after 8-byte discriminator):
//   [8:40]  whirlpool: Pubkey
//   [40]    a_to_b: bool
//   [41:57] pre_sqrt_price: u128 (little-endian)
//   [57:73] post_sqrt_price: u128 (little-endian)  ← we use this
func (w *ProgramWatcher) priceFromEventData(data []byte, pair PairMeta) float64 {
	switch w.programID {
	case ProgramOrcaWhirlpool:
		return priceFromOrcaEvent(data, pair.BaseTokenDecimals, pair.QuoteTokenDecimals,
			pair.BaseToken, pair.QuoteToken, pair.Token0IsBase, pair.Token0OrderKnown)
	case ProgramRaydiumCLMM, ProgramRaydiumAMM:
		return priceFromRaydiumEvent(data, pair.BaseTokenDecimals, pair.QuoteTokenDecimals,
			pair.BaseToken, pair.QuoteToken, pair.Token0IsBase, pair.Token0OrderKnown)
	case ProgramRaydiumCPMM:
		return priceFromCPMMEvent(data, pair.BaseTokenDecimals, pair.QuoteTokenDecimals,
			pair.BaseToken, pair.QuoteToken)
	case ProgramPumpFunAMM:
		return priceFromPumpFunAMMEvent(data, pair.BaseTokenDecimals, pair.QuoteTokenDecimals)
	case ProgramPumpSwap:
		return priceFromPumpSwapEvent(data, pair.BaseTokenDecimals, pair.QuoteTokenDecimals, pair.BaseToken, pair.QuoteToken)
	default:
		// Meteora DLMM, DAMM, DAMM v2 don't emit events — handled by accountSubscribe fallback
		return 0
	}
}

// priceFromOrcaEvent reads post_sqrt_price from the Orca Traded event blob
// and converts it to a human-readable price. Returns 0 if the blob is too
// short or the computed price is not sane.
func priceFromOrcaEvent(data []byte, baseDecimals, quoteDecimals int, baseToken, quoteToken string, token0IsBase, token0OrderKnown bool) float64 {
	// Minimum length: 8 (discriminator) + 32 (whirlpool) + 1 (a_to_b) + 16 (pre) + 16 (post) = 73
	if len(data) < 73 {
		return 0
	}

	// post_sqrt_price is a u128 stored little-endian at offset 57.
	lo := binary.LittleEndian.Uint64(data[57:65])
	hi := binary.LittleEndian.Uint64(data[65:73])
	sqrtPrice := new(big.Int).SetUint64(hi)
	sqrtPrice.Lsh(sqrtPrice, 64)
	sqrtPrice.Or(sqrtPrice, new(big.Int).SetUint64(lo))

	if sqrtPrice.Sign() == 0 {
		return 0
	}

	// price = (sqrtPriceX64 / 2^64)^2 = mintB/mintA in raw atomic units
	priceRat := shared.ConvertSqrtPriceX64ToPrice(sqrtPrice)
	if priceRat == nil {
		return 0
	}

	if token0OrderKnown {
		// Use known orientation: token0IsBase means mintA==base
		oriented := new(big.Rat).Set(priceRat) // mintB/mintA
		if !token0IsBase {
			// mintA==quote, mintB==base → invert to get quote/base
			oriented.Inv(oriented)
		}
		adjusted := shared.ApplyDecimalAdjustments(oriented, baseDecimals, quoteDecimals)
		if adjusted == nil {
			return 0
		}
		f, _ := adjusted.Float64()
		if f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0
		}
		return f
	}

	// Fallback: try both orientations and pick sane
	direct := shared.ApplyDecimalAdjustments(priceRat, baseDecimals, quoteDecimals)
	inverse := shared.ApplyDecimalAdjustments(new(big.Rat).Inv(priceRat), baseDecimals, quoteDecimals)
	var df, inv float64
	if direct != nil {
		df, _ = direct.Float64()
	}
	if inverse != nil {
		inv, _ = inverse.Float64()
	}
	price := shared.ChooseSanePrice(df, inv)
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0
	}
	return price
}

// priceFromRaydiumEvent reads sqrtPriceX64 from the Raydium CLMM SwapEvent
// blob and converts it to a human-readable price. Zero HTTP calls.
func priceFromRaydiumEvent(data []byte, baseDecimals, quoteDecimals int, baseToken, quoteToken string, token0IsBase, token0OrderKnown bool) float64 {
	if len(data) < 185 {
		return 0
	}

	lo := binary.LittleEndian.Uint64(data[169:177])
	hi := binary.LittleEndian.Uint64(data[177:185])
	sqrtPrice := new(big.Int).SetUint64(hi)
	sqrtPrice.Lsh(sqrtPrice, 64)
	sqrtPrice.Or(sqrtPrice, new(big.Int).SetUint64(lo))

	if sqrtPrice.Sign() == 0 {
		return 0
	}

	// price = (sqrtPriceX64 / 2^64)^2 = token1/token0 in raw atomic units
	priceRat := shared.ConvertSqrtPriceX64ToPrice(sqrtPrice)
	if priceRat == nil {
		return 0
	}

	if token0OrderKnown {
		oriented := new(big.Rat).Set(priceRat) // token1/token0
		if !token0IsBase {
			// token0 == quote, token1 == base → invert to get quote/base
			oriented.Inv(oriented)
		}
		adjusted := shared.ApplyDecimalAdjustments(oriented, baseDecimals, quoteDecimals)
		if adjusted == nil {
			return 0
		}
		f, _ := adjusted.Float64()
		if f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0
		}
		return f
	}

	// Fallback: try both orientations
	direct := shared.ApplyDecimalAdjustments(priceRat, baseDecimals, quoteDecimals)
	inverse := shared.ApplyDecimalAdjustments(new(big.Rat).Inv(priceRat), baseDecimals, quoteDecimals)
	var df, inv float64
	if direct != nil {
		df, _ = direct.Float64()
	}
	if inverse != nil {
		inv, _ = inverse.Float64()
	}
	price := shared.ChooseSanePrice(df, inv)
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0
	}
	return price
}

// priceFromCPMMEvent reads vault balances from the Raydium CPMM SwapEvent blob
// and computes price as a ratio. Zero HTTP calls.
//
// CPMM SwapEvent layout (from raydium_cp.txt IDL):
//   [0:8]    discriminator [64,198,205,232,38,8,113,226]
//   [8:40]   pool_id: pubkey
//   [40:48]  input_vault_before: u64   ← vault balance before swap (raw atomic units)
//   [48:56]  output_vault_before: u64  ← vault balance before swap (raw atomic units)
//   [56:64]  input_amount: u64
//   [64:72]  output_amount: u64
//   [72:80]  input_transfer_fee: u64
//   [80:88]  output_transfer_fee: u64
//   [88]     base_input: bool (1 byte)
//   [89:121] input_mint: pubkey (32 bytes)
//   [121:153] output_mint: pubkey (32 bytes)
//
// Price = output_vault_before / input_vault_before adjusted for decimals.
// We orient by matching input_mint/output_mint against DB base/quote tokens.
func priceFromCPMMEvent(data []byte, baseDecimals, quoteDecimals int, baseToken, quoteToken string) float64 {
	// Need at least 153 bytes to read output_mint
	if len(data) < 153 {
		return 0
	}

	inputVault := binary.LittleEndian.Uint64(data[40:48])
	outputVault := binary.LittleEndian.Uint64(data[48:56])
	if inputVault == 0 || outputVault == 0 {
		return 0
	}

	inputMint := encodeBase58(data[89:121])
	_ = encodeBase58(data[121:153]) // outputMint — not needed since we determine orientation from inputMint alone

	// Determine orientation: is inputMint == base or quote?
	// price = quoteVault / baseVault (base per quote doesn't make sense, we want quote/base)
	var baseVault, quoteVault uint64
	var baseDecAdj, quoteDecAdj int

	inputIsBase := strings.EqualFold(inputMint, baseToken)
	inputIsQuote := strings.EqualFold(inputMint, quoteToken)

	if inputIsBase {
		baseVault = inputVault
		quoteVault = outputVault
		baseDecAdj = baseDecimals
		quoteDecAdj = quoteDecimals
	} else if inputIsQuote {
		baseVault = outputVault
		quoteVault = inputVault
		baseDecAdj = baseDecimals
		quoteDecAdj = quoteDecimals
	} else {
		// Mint doesn't match either DB token — fall back to ratio and let ChooseSanePrice decide
		direct := vaultRatioToPrice(inputVault, outputVault, baseDecimals, quoteDecimals)
		inverse := vaultRatioToPrice(outputVault, inputVault, baseDecimals, quoteDecimals)
		return shared.ChooseSanePrice(direct, inverse)
	}

	price := vaultRatioToPrice(quoteVault, baseVault, quoteDecAdj, baseDecAdj)
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0
	}
	return price
}

// vaultRatioToPrice converts raw vault balances to a human-readable price.
// price = (numeratorVault / 10^numeratorDecimals) / (denominatorVault / 10^denominatorDecimals)
func vaultRatioToPrice(numeratorVault, denominatorVault uint64, numeratorDecimals, denominatorDecimals int) float64 {
	if denominatorVault == 0 {
		return 0
	}
	num := new(big.Rat).SetUint64(numeratorVault)
	den := new(big.Rat).SetUint64(denominatorVault)
	ratio := new(big.Rat).Quo(num, den)
	adjusted := shared.ApplyDecimalAdjustments(ratio, denominatorDecimals, numeratorDecimals)
	if adjusted == nil {
		return 0
	}
	f, _ := adjusted.Float64()
	return f
}

// priceFromDLMMEvent is DEPRECATED and UNUSED.
// Meteora DLMM does NOT emit Anchor events - it's account-based.
// DLMM pools are handled by accountSubscribe watchers, not logsSubscribe.
//
// This function is kept for reference only and may be removed in the future.
func priceFromDLMMEvent(data []byte, baseDecimals, quoteDecimals int, baseToken, quoteToken string, binStep int64) float64 {
	if len(data) < 97 {
		return 0
	}

	// Verify this is actually a SwapEvent by checking the discriminator.
	// Anchor discriminator = sha256("event:SwapEvent")[0:8]
	// = [64, 198, 205, 232, 38, 8, 113, 226]
	// If the discriminator doesn't match, this is a different DLMM event
	// (AddLiquidity, RemoveLiquidity, etc.) that also has lbPair at offset 8.
	dlmmSwapDisc := [8]byte{64, 198, 205, 232, 38, 8, 113, 226}
	var disc [8]byte
	copy(disc[:], data[0:8])
	if disc != dlmmSwapDisc {
		return 0
	}

	endBinID := int64(int32(binary.LittleEndian.Uint32(data[76:80])))
	// swapForY tells us direction but price is always expressed as tokenY/tokenX regardless
	// endBinID is the active bin after the swap = current price bin

	if binStep > 0 && endBinID != 0 {
		// Canonical DLMM price: (1 + binStep/10000)^endBinId
		price := math.Pow(1+float64(binStep)/10000, float64(endBinID))
		if math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
			goto fallback
		}
		if baseDecimals != quoteDecimals {
			price *= math.Pow10(baseDecimals - quoteDecimals)
		}
		if price > 0 && !math.IsNaN(price) && !math.IsInf(price, 0) {
			return price
		}
	}

fallback:
	// Fallback: amountOut/amountIn ratio adjusted for decimals
	amountIn := binary.LittleEndian.Uint64(data[80:88])
	amountOut := binary.LittleEndian.Uint64(data[88:96])
	if amountIn == 0 || amountOut == 0 {
		return 0
	}
	direct := vaultRatioToPrice(amountOut, amountIn, quoteDecimals, baseDecimals)
	inverse := vaultRatioToPrice(amountIn, amountOut, baseDecimals, quoteDecimals)
	price := shared.ChooseSanePrice(direct, inverse)
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0
	}
	return price
}

// priceFromDammV2Event is DEPRECATED and UNUSED.
// Meteora DAMM v2 does NOT emit Anchor events - it's account-based.
// DAMM v2 pools are handled by accountSubscribe watchers, not logsSubscribe.
//
// This function is kept for reference only and may be removed in the future.
func priceFromDammV2Event(data []byte, baseDecimals, quoteDecimals int, baseToken, quoteToken string) float64 {
	const reserveAOffset = 115
	const reserveBOffset = 123
	if len(data) < reserveBOffset+8 {
		return 0
	}

	reserveA := binary.LittleEndian.Uint64(data[reserveAOffset : reserveAOffset+8])
	reserveB := binary.LittleEndian.Uint64(data[reserveBOffset : reserveBOffset+8])
	if reserveA == 0 || reserveB == 0 {
		return 0
	}

	// price = reserveB/reserveA adjusted for decimals; try both orientations
	direct := vaultRatioToPrice(reserveB, reserveA, quoteDecimals, baseDecimals)
	inverse := vaultRatioToPrice(reserveA, reserveB, baseDecimals, quoteDecimals)
	price := shared.ChooseSanePrice(direct, inverse)
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0
	}
	return price
}

// priceFromPumpSwapEvent reads reserve balances from the PumpSwap swap event blob
// and computes price as a ratio. Zero HTTP calls.
//
// PumpSwap SwapEvent layout (from inspector output — verified against 3 live events):
//   [0:8]    discriminator [189, 219, 127, 211, 78, 230, 97, 238]
//   [8:40]   pool_id: pubkey
//   [40:48]  reserve0: u64  ← pool base token reserve (raw atomic units)
//   [48:56]  reserve1: u64  ← pool quote token reserve (raw atomic units)
//
// The event does not contain mint addresses, so we cannot determine orientation
// from the event bytes alone. We use the pair's token addresses from the DB to
// disambiguate when the HTTP adapter has already resolved them, and fall back
// to ChooseSanePrice when they are not available.
func priceFromPumpSwapEvent(data []byte, baseDecimals, quoteDecimals int, baseToken, quoteToken string) float64 {
	// Need at least 56 bytes to read both reserves
	if len(data) < 56 {
		return 0
	}

	reserve0 := binary.LittleEndian.Uint64(data[40:48])
	reserve1 := binary.LittleEndian.Uint64(data[48:56])
	if reserve0 == 0 || reserve1 == 0 {
		return 0
	}

	// direct: reserve0=base, reserve1=quote → price = (reserve1/10^quoteDecimals) / (reserve0/10^baseDecimals)
	direct := vaultRatioToPrice(reserve1, reserve0, quoteDecimals, baseDecimals)
	// inverse: reserve0=quote, reserve1=base → price = (reserve0/10^quoteDecimals) / (reserve1/10^baseDecimals)
	inverse := vaultRatioToPrice(reserve0, reserve1, quoteDecimals, baseDecimals)

	price := shared.ChooseSanePrice(direct, inverse)
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0
	}
	return price
}

// priceFromPumpFunAMMEvent reads pool reserve balances from the Pump.fun AMM swap event.
// 
// Pump.fun AMM SwapEvent layout (from Solscan):
//   [0:8]     discriminator
//   [8:40]    pool: pubkey
//   [40:48]   timestamp: i64
//   [48:56]   baseAmountOut: u64
//   [56:64]   maxQuoteAmountIn: u64
//   [64:72]   userBaseTokenReserves: u64
//   [72:80]   userQuoteTokenReserves: u64
//   [80:88]   poolBaseTokenReserves: u64  ← POST-SWAP pool base reserves
//   [88:96]   poolQuoteTokenReserves: u64 ← POST-SWAP pool quote reserves
//   ... (remaining fields)
//
// Price = poolQuoteTokenReserves / poolBaseTokenReserves adjusted for decimals.
func priceFromPumpFunAMMEvent(data []byte, baseDecimals, quoteDecimals int) float64 {
	// Need at least 96 bytes to read both pool reserves
	if len(data) < 96 {
		return 0
	}

	poolBaseReserves := binary.LittleEndian.Uint64(data[80:88])
	poolQuoteReserves := binary.LittleEndian.Uint64(data[88:96])
	
	if poolBaseReserves == 0 || poolQuoteReserves == 0 {
		return 0
	}

	// Price = poolQuoteReserves / poolBaseReserves, adjusted for decimals
	// Try both orientations in case we have the orientation wrong
	direct := vaultRatioToPrice(poolQuoteReserves, poolBaseReserves, quoteDecimals, baseDecimals)
	inverse := vaultRatioToPrice(poolBaseReserves, poolQuoteReserves, baseDecimals, quoteDecimals)
	
	price := shared.ChooseSanePrice(direct, inverse)
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0
	}
	return price
}

// extractEventDataForPool returns the raw decoded bytes of the Program data:
// line that belongs to the swap for the given pool. It finds the data line
// that immediately precedes or follows the swap instruction for this pool.
func (w *ProgramWatcher) extractEventDataForPool(logs []string, poolAddress string) []byte {
	// Find the "Program data:" line associated with the pool swap.
	// Strategy: find all data lines and return the one whose decoded bytes
	// contain our pool address at offset 8 (canonical Anchor event layout).
	//
	// ALL Anchor programs (Raydium, Orca, Meteora) emit events as base64 in "Program data:" lines.
	dataLines := 0
	for _, line := range logs {
		if !strings.Contains(line, "Program data: ") {
			continue
		}
		dataLines++
		b64 := strings.TrimSpace(strings.SplitN(line, "Program data:", 2)[1])
		data, ok := decodeBase64EventData(b64)
		if !ok || len(data) < 40 {
			continue
		}
		// Check if this event's pool address (at offset 8) matches.
		candidate := encodeBase58(data[8:40])
		log.Printf("[prog/%s] Checking data line %d: candidate=%s, looking_for=%s", 
			shortID(w.programID), dataLines, candidate[:8]+"...", poolAddress[:8]+"...")
		if strings.EqualFold(candidate, poolAddress) {
			log.Printf("[prog/%s] MATCH! found event data for %s (scanned %d lines)", 
				shortID(w.programID), poolAddress, dataLines)
			return data
		}
	}
	log.Printf("[prog/%s] NO MATCH for %s (scanned %d lines, program=%s)", 
		shortID(w.programID), poolAddress, dataLines, w.programID)
	return nil
}

// extractPoolAddress parses the transaction logs to find which pool address
// was modified.
//
// Raydium CLMM SwapV2 and Swap instructions emit a "Program data: <base64>"
// log line immediately after "Program log: Instruction: SwapV2".
// The first 8 bytes are an Anchor discriminator, bytes 8-40 are the pool_state pubkey.
//
// Orca Whirlpool emits a "Traded" event where the whirlpool pubkey is the FIRST
// field after the 8-byte discriminator (bytes 8-40). We scan ALL "Program data:"
// lines rather than only looking after "Instruction: Swap", because in CPI
// scenarios the Swap instruction can be nested inside an outer program and the
// data line appears at any nesting depth.
//
// For both programs we also scan all "Program <X> invoke" lines for pubkeys
// that match our pool map as a fallback.
func (w *ProgramWatcher) extractPoolAddress(logs []string) string {
	// Pass 1: scan all "Program data:" lines — works for both Raydium and Orca.
	// Raydium: pool_state is at bytes 8-40 of the SwapV2 event.
	// Orca: whirlpool pubkey is at bytes 8-40 of the Traded event (first field).
	dataLineCount := 0
	for _, line := range logs {
		if strings.Contains(line, "Program data: ") {
			dataLineCount++
			dataB64 := strings.TrimSpace(strings.SplitN(line, "Program data:", 2)[1])
			if addr := w.decodePoolFromProgramData(dataB64); addr != "" {
				return addr
			}
		}
		if strings.Contains(line, "Program return:") {
			parts := strings.Fields(line)
			if len(parts) > 0 {
				dataB64 := strings.TrimSpace(parts[len(parts)-1])
				if addr := w.decodePoolFromProgramData(dataB64); addr != "" {
					return addr
				}
			}
		}
	}
	

	// Pass 2: scan "Program <X> invoke" lines for any address in our pool map.
	for _, line := range logs {
		if strings.Contains(line, " invoke [") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				addr := parts[1]
				if looksLikeSolanaAddr(addr) && w.poolExists(addr) {
					return addr
				}
			}
		}
	}

	// Pass 3 (fallback): scan every token in every log line.
	for _, line := range logs {
		parts := splitNonAlphaNum(line)
		for _, tok := range parts {
			if looksLikeSolanaAddr(tok) && w.poolExists(tok) {
				return tok
			}
		}
	}
	return ""
}

func (w *ProgramWatcher) decodePoolFromProgramData(b64str string) string {
	data, ok := decodeBase64EventData(b64str)
	if !ok {
		return ""
	}

	switch w.programID {
	case ProgramRaydiumCLMM, ProgramRaydiumAMM:
		return decodeRaydiumPoolFromEventData(data)
	case ProgramOrcaWhirlpool:
		return w.decodeOrcaPoolFromEventData(data)
	case ProgramRaydiumCPMM:
		// CPMM SwapEvent: pool_id is also at bytes [8:40]
		return decodeRaydiumPoolFromEventData(data)
	case ProgramPumpFunAMM:
		// Pump.fun AMM: pool pubkey at bytes [8:40]
		return decodeRaydiumPoolFromEventData(data)
	default:
		// Meteora programs don't emit events, so this should never be called for them
		return decodeRaydiumPoolFromEventData(data)
	}
}

func decodeBase64EventData(b64str string) ([]byte, bool) {
	b64str = strings.TrimSpace(b64str)
	if b64str == "" {
		return nil, false
	}
	data, err := base64.StdEncoding.DecodeString(b64str)
	if err != nil {
		data, err = base64.RawStdEncoding.DecodeString(b64str)
		if err != nil {
			return nil, false
		}
	}
	return data, true
}

func decodeRaydiumPoolFromEventData(data []byte) string {
	if len(data) < 40 {
		return ""
	}
	return encodeBase58(data[8:40])
}

func (w *ProgramWatcher) decodeOrcaPoolFromEventData(data []byte) string {
	if len(data) < 40 {
		return ""
	}

	// The Orca Whirlpool "Traded" event layout (Anchor discriminator = 8 bytes):
	//   [0:8]   discriminator
	//   [8:40]  whirlpool: Pubkey  ← pool address
	// Try the canonical offset first — it's fast and correct for all normal swaps.
	addr := encodeBase58(data[8:40])
	if w.poolExists(addr) {
		return addr
	}

	// Fallback: slide every 32-byte window. Handles any variant event layouts
	// or future changes to the event struct.
	for start := 0; start+32 <= len(data); start++ {
		candidate := data[start : start+32]
		a := encodeBase58(candidate)
		if w.poolExists(a) {
			return a
		}
	}
	return ""
}

func (w *ProgramWatcher) poolExists(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return false
	}
	w.mu.RLock()
	_, found := w.poolMap[strings.ToLower(addr)]
	w.mu.RUnlock()
	return found
}

// decodePoolFromEventData decodes a base64 Anchor event data blob and extracts
// the pool_state pubkey. For Raydium CLMM SwapV2 events, the pool_state is
// at bytes 8..40 (after the 8-byte discriminator).
func (w *ProgramWatcher) decodePoolFromEventData(b64str string) string {
	data, ok := decodeBase64EventData(b64str)
	if !ok {
		return ""
	}
	return decodeRaydiumPoolFromEventData(data)
}

// extractMeteoraEventFromLogs is DEPRECATED and UNUSED.
// Meteora DLMM/DAMM do NOT emit parseable event logs - they're account-based.
// This function was an attempt to parse log strings but Meteora doesn't emit structured events.
//
// This function is kept for reference only and may be removed in the future.
func (w *ProgramWatcher) extractMeteoraEventFromLogs(logs []string, poolAddress string) []byte {
	log.Printf("[prog/%s] Attempting to parse Meteora event from logs (total %d lines)", 
		shortID(w.programID), len(logs))
	
	// For DLMM, we need to parse the entire event structure across potentially multiple lines
	var eventBuilder strings.Builder
	inEventBlock := false
	eventType := "" // "SwapEvent", "Swap2Evt", or "Swap2"
	
	for _, line := range logs {
		// Check if this is the start of a Meteora swap event
		if strings.Contains(line, "Program log: SwapEvent {") {
			inEventBlock = true
			eventType = "SwapEvent"
			eventBuilder.Reset()
			eventBuilder.WriteString(line)
			continue
		} else if strings.Contains(line, "Program log: Swap2Evt {") {
			inEventBlock = true
			eventType = "Swap2Evt"
			eventBuilder.Reset()
			eventBuilder.WriteString(line)
			continue
		} else if strings.Contains(line, "Program log: Swap2 {") {
			inEventBlock = true
			eventType = "Swap2"
			eventBuilder.Reset()
			eventBuilder.WriteString(line)
			continue
		}
		
		// If we're in an event block, accumulate lines until we find the closing brace
		if inEventBlock {
			eventBuilder.WriteString(" ")
			eventBuilder.WriteString(line)
			
			// Check if this line closes the event block
			if strings.Contains(line, "}") {
				// We've captured the complete event, now parse it
				fullEvent := eventBuilder.String()
				log.Printf("[prog/%s] Found complete %s: %s", shortID(w.programID), eventType, fullEvent)
				
				// Parse the event based on type
				switch eventType {
				case "SwapEvent", "Swap2Evt":
					return w.parseDLMMSwapEvent(fullEvent)
				case "Swap2":
					return w.parseDammSwapEvent(fullEvent)
				}
				
				// Reset for next event
				inEventBlock = false
				eventType = ""
				eventBuilder.Reset()
			}
		}
	}
	
	log.Printf("[prog/%s] Could not find complete Meteora event in logs", shortID(w.programID))
	return nil
}

// parseDLMMSwapEvent parses a DLMM SwapEvent or Swap2Evt log string and constructs event bytes
func (w *ProgramWatcher) parseDLMMSwapEvent(eventLog string) []byte {
	// Extract fields using the parseAllFieldsFromLogLine helper
	fields := parseAllFieldsFromLogLine(eventLog)
	
	endBinID, hasEndBin := fields["endBinId"]
	amountIn, hasAmountIn := fields["amountIn"]
	amountOut, hasAmountOut := fields["amountOut"]
	
	// Parse swapForY boolean
	swapForY := strings.Contains(eventLog, "swapForY: true")
	
	log.Printf("[prog/%s] Parsed DLMM event: endBinId=%d (has=%v), amountIn=%d (has=%v), amountOut=%d (has=%v), swapForY=%v", 
		shortID(w.programID), endBinID, hasEndBin, amountIn, hasAmountIn, amountOut, hasAmountOut, swapForY)
	
	// Validate we have the minimum required fields
	if !hasEndBin || (!hasAmountIn && !hasAmountOut) {
		log.Printf("[prog/%s] Missing required DLMM fields", shortID(w.programID))
		return nil
	}
	
	// Create a minimal event data structure matching priceFromDLMMEvent expectations
	// DLMM SwapEvent layout:
	//   [0:8]    discriminator (use known DLMM swap discriminator)
	//   [8:40]   lbPair: pubkey (skip - not needed for price calc)
	//   [40:72]  from: pubkey (skip)
	//   [72:76]  startBinId: i32 (skip)
	//   [76:80]  endBinId: i32 ← THIS IS WHAT WE NEED
	//   [80:88]  amountIn: u64
	//   [88:96]  amountOut: u64
	//   [96]     swapForY: bool
	eventData := make([]byte, 97)
	
	// Set DLMM swap discriminator [64, 198, 205, 232, 38, 8, 113, 226]
	dlmmSwapDisc := []byte{64, 198, 205, 232, 38, 8, 113, 226}
	copy(eventData[0:8], dlmmSwapDisc)
	
	// Set endBinId (little-endian i32) - this is the active bin after swap
	binary.LittleEndian.PutUint32(eventData[76:80], uint32(int32(endBinID)))
	
	// Set amountIn (little-endian u64)
	binary.LittleEndian.PutUint64(eventData[80:88], uint64(amountIn))
	
	// Set amountOut (little-endian u64)
	binary.LittleEndian.PutUint64(eventData[88:96], uint64(amountOut))
	
	// Set swapForY (boolean)
	if swapForY {
		eventData[96] = 1
	} else {
		eventData[96] = 0
	}
	
	log.Printf("[prog/%s] ✓ Successfully constructed DLMM event data from logs: endBinId=%d, amountIn=%d, amountOut=%d, swapForY=%v", 
		shortID(w.programID), endBinID, amountIn, amountOut, swapForY)
	return eventData
}

// parseDammSwapEvent parses a DAMM/DAMM v2 Swap2 event log string and constructs event bytes
func (w *ProgramWatcher) parseDammSwapEvent(eventLog string) []byte {
	// For DAMM v2, we need reserve amounts after the swap
	// The event structure contains reserve information
	fields := parseAllFieldsFromLogLine(eventLog)
	
	// DAMM v2 Swap2 event layout (as documented in the prompt):
	//   pool: pubkey
	//   tradeDirection: u8
	//   collectFeeMode: u8  
	//   hasReferral: bool
	//   SwapParameters2: { amountIn: u64, minimumAmountOut: u64 }
	//   SwapResult2: { amountIn: u64, amountOut: u64, tradeFee: u64 }
	//   includedTransferFeeAmountIn: u64
	//   includedTransferFeeAmountOut: u64
	//   excludedTransferFeeAmountOut: u64
	//   currentTimestamp: u64
	//   reserveAAmount: u64 ← POST-SWAP RESERVE A
	//   reserveBAmount: u64 ← POST-SWAP RESERVE B
	
	reserveA, hasReserveA := fields["reserveAAmount"]
	reserveB, hasReserveB := fields["reserveBAmount"]
	
	log.Printf("[prog/%s] Parsed DAMM event: reserveA=%d (has=%v), reserveB=%d (has=%v)", 
		shortID(w.programID), reserveA, hasReserveA, reserveB, hasReserveB)
	
	if !hasReserveA || !hasReserveB {
		log.Printf("[prog/%s] Missing required DAMM reserve fields", shortID(w.programID))
		return nil
	}
	
	if reserveA == 0 || reserveB == 0 {
		log.Printf("[prog/%s] Invalid DAMM reserves (zero value)", shortID(w.programID))
		return nil
	}
	
	// Create event data matching priceFromDammV2Event expectations
	// DAMM v2 Swap event layout:
	//   [0:8]    discriminator
	//   [8:40]   pool: pubkey (skip)
	//   ... (skip intermediate fields)
	//   [115:123] reserveAAmount: u64 ← at offset 115
	//   [123:131] reserveBAmount: u64 ← at offset 123
	const reserveAOffset = 115
	const reserveBOffset = 123
	eventData := make([]byte, reserveBOffset+8)
	
	// We don't need to set a specific discriminator for DAMM - priceFromDammV2Event
	// just reads the reserves at fixed offsets
	
	// Set reserves at the expected offsets
	binary.LittleEndian.PutUint64(eventData[reserveAOffset:reserveAOffset+8], uint64(reserveA))
	binary.LittleEndian.PutUint64(eventData[reserveBOffset:reserveBOffset+8], uint64(reserveB))
	
	log.Printf("[prog/%s] ✓ Successfully constructed DAMM v2 event data from logs: reserveA=%d, reserveB=%d", 
		shortID(w.programID), reserveA, reserveB)
	return eventData
}

// parseAllFieldsFromLogLine extracts all fieldName: value pairs from a log line
// Handles both simple numeric fields and negative numbers (e.g., i32 bin IDs)
func parseAllFieldsFromLogLine(line string) map[string]int64 {
	fields := make(map[string]int64)
	
	// Look for patterns like "fieldName: 123" or "fieldName: -123" or "fieldName:123"
	// Use a regex to capture field names and numbers (including negative)
	re := regexp.MustCompile(`(\w+):\s*(-?\d+)`)
	matches := re.FindAllStringSubmatch(line, -1)
	
	for _, match := range matches {
		if len(match) == 3 {
			fieldName := match[1]
			fieldValue := match[2]
			
			// Parse as signed int64 to handle negative bin IDs
			if val, err := strconv.ParseInt(fieldValue, 10, 64); err == nil {
				fields[fieldName] = val
			}
		}
	}
	
	return fields
}

// parseNumber extracts a numeric value from a string
func parseNumber(s string) (int64, bool) {
	// Skip whitespace
	s = strings.TrimSpace(s)
	
	// Find the first number sequence
	var numStr strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			numStr.WriteRune(r)
		} else if numStr.Len() > 0 {
			break // Stop at first non-digit after we've started a number
		}
	}
	
	if numStr.Len() == 0 {
		return 0, false
	}
	
	val, err := strconv.ParseInt(numStr.String(), 10, 64)
	if err != nil {
		return 0, false
	}
	
	return val, true
}

// encodeBase58 encodes raw bytes to a Solana-style base58 string.
func encodeBase58(input []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	if len(input) == 0 {
		return ""
	}
	zeroes := 0
	for zeroes < len(input) && input[zeroes] == 0 {
		zeroes++
	}
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
	// Preserve leading zero bytes as '1' characters in base58
	for i := 0; i < zeroes; i++ {
		encoded = append(encoded, alphabet[0])
	}
	for i, j := 0, len(encoded)-1; i < j; i, j = i+1, j-1 {
		encoded[i], encoded[j] = encoded[j], encoded[i]
	}
	return string(encoded)
}

func looksLikeSolanaAddr(s string) bool {
	s = normalizeAddress(s)
	if len(s) < 32 || len(s) > 44 {
		return false
	}
	const b58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	for _, c := range s {
		if !strings.ContainsRune(b58, c) {
			return false
		}
	}
	return true
}

func normalizeAddress(s string) string {
	return strings.TrimSpace(s)
}

// splitNonAlphaNum splits a string into tokens separated by any rune
// that is not alphanumeric. Keeps tokens of letters/digits only.
func splitNonAlphaNum(s string) []string {
	var out []string
	var cur strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			cur.WriteRune(r)
		} else {
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func shortID(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
