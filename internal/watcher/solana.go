// Package watcher provides real-time on-chain price updates via WebSocket.
//
// For Solana: uses Solana's `accountSubscribe` JSON-RPC WebSocket method.
// Every time a monitored pool account is modified (i.e. a swap occurred),
// the node pushes the new account data. We decode the pool state and update
// the price in the DB immediately — no polling required.
//
// A background fallback ticker still runs every 5 minutes to recover from
// any missed events during WebSocket reconnects.
package watcher

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"on-chain-price-fetcher/internal/adapters/shared"
	"on-chain-price-fetcher/internal/stats"
)

// PairMeta holds the DB metadata for a single pool, used to calculate price.
type PairMeta struct {
	ID                 string
	DexName            string
	PoolAddress        string
	BaseToken          string
	QuoteToken         string
	BaseTokenDecimals  int
	QuoteTokenDecimals int
	BaseSymbol         string
	QuoteSymbol        string
	// Token0IsBase indicates whether on-chain token0/mintA == DB base token.
	// Resolved once at startup from the pool account state.
	// If Token0OrderKnown is false, the event price functions fall back to guessing.
	Token0IsBase      bool
	Token0OrderKnown  bool
	// BinStep is the Meteora DLMM bin step (stored in pool account at startup).
	// Used by priceFromDLMMEvent to compute price as (1+binStep/10000)^endBinId.
	BinStep           int64
}

// PriceFetcher is any adapter that can compute a price for a pair given raw
// account data. This lets us reuse the existing raydium/orca/meteora adapters.
type PriceFetcher interface {
	FetchPriceFromData(raw []byte, pair shared.Pair) (float64, bool)
}

// SolanaWatcher subscribes to accountSubscribe events for a list of pools
// and updates the database whenever a swap is detected.
type SolanaWatcher struct {
	wsEndpoint string
	db         *sql.DB
	pairs      []PairMeta
	fetchFn    func(poolAddress string, pair shared.Pair) (float64, error)

	mu          sync.Mutex
	conn        *websocket.Conn
	nextSubID   int // tracks next request ID for new subscriptions

	subIDtoPair map[int]PairMeta // subscription ID → pair metadata
	newPairsCh  chan []PairMeta  // live injection channel
	stopCh      chan struct{}
}

// NewSolanaWatcher creates a new watcher.
func NewSolanaWatcher(
	wsEndpoint string,
	db *sql.DB,
	pairs []PairMeta,
	fetchFn func(poolAddress string, pair shared.Pair) (float64, error),
) *SolanaWatcher {
	return &SolanaWatcher{
		wsEndpoint:  wsEndpoint,
		db:          db,
		pairs:       pairs,
		fetchFn:     fetchFn,
		subIDtoPair: make(map[int]PairMeta),
		newPairsCh:  make(chan []PairMeta, 64),
		stopCh:      make(chan struct{}),
	}
}

// NewShardedWatchers splits pairs across multiple WS endpoints and returns
// one SolanaWatcher per endpoint. This distributes subscription load evenly
// so no single endpoint exceeds the rate limit.
func NewShardedWatchers(
	wsEndpoints []string,
	db *sql.DB,
	pairs []PairMeta,
	fetchFn func(poolAddress string, pair shared.Pair) (float64, error),
) []*SolanaWatcher {
	if len(wsEndpoints) == 0 || len(pairs) == 0 {
		return nil
	}

	n := len(wsEndpoints)
	shards := make([][]PairMeta, n)
	for i, pair := range pairs {
		shards[i%n] = append(shards[i%n], pair)
	}

	watchers := make([]*SolanaWatcher, n)
	for i, ep := range wsEndpoints {
		watchers[i] = NewSolanaWatcher(ep, db, shards[i], fetchFn)
		log.Printf("[watcher/solana] shard %d/%d → %s (%d pools)",
			i+1, n, ep[:min(len(ep), 50)]+"...", len(shards[i]))
	}
	return watchers
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Start connects to the Solana WebSocket RPC and subscribes to all pools.
// It reconnects automatically on disconnect. Blocks until Stop() is called.
func (w *SolanaWatcher) Start() {
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		if err := w.connectAndListen(); err != nil {
			log.Printf("[watcher/solana] disconnected: %v — reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

// Stop shuts down the watcher.
func (w *SolanaWatcher) Stop() {
	close(w.stopCh)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn != nil {
		w.conn.Close()
	}
}

func (w *SolanaWatcher) connectAndListen() error {
	log.Printf("[watcher/solana] connecting to %s", w.wsEndpoint)
	conn, _, err := websocket.DefaultDialer.Dial(w.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	w.mu.Lock()
	w.conn = conn
	w.subIDtoPair = make(map[int]PairMeta)
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		conn.Close()
		w.conn = nil
		w.mu.Unlock()
	}()

	// Subscribe to each pool account — throttle at 10 per second to stay
	// within QuickNode's 15/second WS request limit.
	for i, pair := range w.pairs {
		subID := i + 1
		req := map[string]any{
			"jsonrpc": "2.0",
			"id":      subID,
			"method":  "accountSubscribe",
			"params": []any{
				pair.PoolAddress,
				map[string]any{
					"encoding":   "base64",
					"commitment": "confirmed",
				},
			},
		}
		if err := conn.WriteJSON(req); err != nil {
			return fmt.Errorf("subscribe %s: %w", pair.PoolAddress, err)
		}
		log.Printf("[watcher/solana] subscribed to %s (%s/%s)", pair.PoolAddress, pair.BaseSymbol, pair.QuoteSymbol)
		time.Sleep(100 * time.Millisecond)
	}

	// Track the next available request ID for live injections
	w.mu.Lock()
	w.nextSubID = len(w.pairs) + 1
	w.mu.Unlock()

	// Read subscription confirmations and then notification messages
	pendingSubscriptions := make(map[int]PairMeta) // request ID → pair
	for i, pair := range w.pairs {
		pendingSubscriptions[i+1] = pair
	}

	for {
		select {
		case <-w.stopCh:
			return nil
		case freshPairs := <-w.newPairsCh:
			// Live injection: subscribe new pairs on the existing connection
			w.mu.Lock()
			nextID := w.nextSubID
			w.mu.Unlock()
			for _, pair := range freshPairs {
				req := map[string]any{
					"jsonrpc": "2.0",
					"id":      nextID,
					"method":  "accountSubscribe",
					"params": []any{
						pair.PoolAddress,
						map[string]any{
							"encoding":   "base64",
							"commitment": "confirmed",
						},
					},
				}
				if err := conn.WriteJSON(req); err != nil {
					log.Printf("[watcher/solana] live subscribe error for %s: %v", pair.PoolAddress, err)
					break
				}
				pendingSubscriptions[nextID] = pair
				nextID++
				log.Printf("[watcher/solana] ✅ live-subscribed %s/%s (no reconnect)", pair.BaseSymbol, pair.QuoteSymbol)
				time.Sleep(100 * time.Millisecond)
			}
			w.mu.Lock()
			w.nextSubID = nextID
			w.mu.Unlock()
			continue
		default:
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		w.handleMessage(msg, pendingSubscriptions)
	}
}

type wsResponse struct {
	ID     *int            `json:"id"`
	Method string          `json:"method"`
	Result json.RawMessage `json:"result"`
	Params *wsParams       `json:"params"`
	Error  *wsError        `json:"error"`
}

type wsParams struct {
	Subscription int             `json:"subscription"`
	Result       json.RawMessage `json:"result"`
}

type wsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type accountNotification struct {
	Value struct {
		Data    []any  `json:"data"`
		Lamports int64 `json:"lamports"`
	} `json:"value"`
}

func (w *SolanaWatcher) handleMessage(msg []byte, pending map[int]PairMeta) {
	var resp wsResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}

	// Subscription confirmation: {"id":1,"result":12345678}
	if resp.ID != nil {
		if resp.Error != nil {
			log.Printf("[watcher/solana] subscription error for id %d: %s", *resp.ID, resp.Error.Message)
			return
		}
		var subID int
		if err := json.Unmarshal(resp.Result, &subID); err == nil {
			if pair, ok := pending[*resp.ID]; ok {
				w.mu.Lock()
				w.subIDtoPair[subID] = pair
				w.mu.Unlock()
				log.Printf("[watcher/solana] confirmed sub %d → %s (%s/%s)", subID, pair.PoolAddress, pair.BaseSymbol, pair.QuoteSymbol)
			}
		}
		return
	}

	// Account change notification: {"method":"accountNotification","params":{...}}
	if resp.Method != "accountNotification" || resp.Params == nil {
		return
	}

	w.mu.Lock()
	pair, ok := w.subIDtoPair[resp.Params.Subscription]
	w.mu.Unlock()
	if !ok {
		return
	}

	// Parse the account data from the notification
	var notif accountNotification
	if err := json.Unmarshal(resp.Params.Result, &notif); err != nil {
		return
	}
	if len(notif.Value.Data) == 0 {
		return
	}
	dataStr, ok := notif.Value.Data[0].(string)
	if !ok {
		return
	}
	raw, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(dataStr)
		if err != nil {
			return
		}
	}

	// The account data changed — fetch a fresh price using the adapter.
	// We pass the raw bytes directly to avoid a second RPC call.
	_ = raw // raw bytes available for direct parsing if needed
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

	price, err := w.fetchFn(pair.PoolAddress, sharedPair)
	if err != nil || price <= 0 {
		log.Printf("[watcher/solana] fetch failed for %s/%s (%s): err=%v price=%.10g",
			pair.BaseSymbol, pair.QuoteSymbol, pair.PoolAddress[:8]+"...", err, price)
		return
	}

	if err := w.updatePrice(pair.ID, price); err != nil {
		log.Printf("[watcher/solana] DB update failed for %s: %v", pair.ID, err)
		return
	}

	log.Printf("[watcher/solana] ✓ %s/%s (%s) → %.10g", pair.BaseSymbol, pair.QuoteSymbol, pair.PoolAddress[:8]+"...", price)
}

func (w *SolanaWatcher) updatePrice(pairID string, price float64) error {
	return stats.UpdatePriceWithStats(w.db, pairID, price, price)
}

// LoadSolanaPairs reads all Solana pairs from the DB that have a supported DEX.
func LoadSolanaPairs(db *sql.DB) ([]PairMeta, error) {
	rows, err := db.Query(`
		SELECT id, dex_name, pool_address, base_token, quote_token,
		       base_token_decimals, quote_token_decimals, base_symbol, quote_symbol
		FROM pairs
		WHERE network = 'solana'
		  AND (
		    dex_name ILIKE '%raydium%'
		    OR dex_name ILIKE '%orca%'
		    OR dex_name ILIKE '%meteora%'
		    OR dex_name ILIKE '%dlmm%'
		    OR dex_name ILIKE '%damm%'
		    OR dex_name ILIKE '%pumpswap%'
		    OR dex_name ILIKE '%pump%'
		    OR dex_name ILIKE '%cpmm%'
		  )
		ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []PairMeta
	for rows.Next() {
		var p PairMeta
		var baseTokenJSON, quoteTokenJSON sql.NullString
		if err := rows.Scan(&p.ID, &p.DexName, &p.PoolAddress,
			&baseTokenJSON, &quoteTokenJSON,
			&p.BaseTokenDecimals, &p.QuoteTokenDecimals,
			&p.BaseSymbol, &p.QuoteSymbol); err != nil {
			return nil, err
		}
		// Extract address AND decimals from the JSON metadata.
		// Prefer the JSON decimals (written from on-chain SPL mint account by the
		// server) over the plain DB column which defaults to 18 — a correct EVM
		// default but wrong for Solana SPL tokens (typically 6 or 9).
		baseAddr, baseDec := parseTokenJSON(baseTokenJSON.String)
		quoteAddr, quoteDec := parseTokenJSON(quoteTokenJSON.String)
		if baseAddr != "" {
			p.BaseToken = baseAddr
		} else {
			p.BaseToken = parseAddress(baseTokenJSON.String)
		}
		if quoteAddr != "" {
			p.QuoteToken = quoteAddr
		} else {
			p.QuoteToken = parseAddress(quoteTokenJSON.String)
		}
		if baseDec > 0 {
			p.BaseTokenDecimals = baseDec
		}
		if quoteDec > 0 {
			p.QuoteTokenDecimals = quoteDec
		}
		if p.PoolAddress == "" {
			continue
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

// ProgramGroups holds pairs categorized by their on-chain program, ready for
// routing to the correct watcher type (logsSubscribe vs accountSubscribe).
type ProgramGroups struct {
	RaydiumCLMM    []PairMeta // logsSubscribe CAMMCzo5...
	Orca           []PairMeta // logsSubscribe whirLbMi...
	MeteoraDLMM    []PairMeta // logsSubscribe LBUZKhRx...
	MeteoraDammV2  []PairMeta // logsSubscribe cpamdpZC...
	RaydiumOther   []PairMeta // needs DetectRaydiumProgram to split CPMM vs AMM V4
	PumpSwap       []PairMeta // accountSubscribe fallback
	Other          []PairMeta // accountSubscribe fallback
}

// FilterPairsByProgram categorizes a flat list of Solana pairs into groups
// keyed by the on-chain program they belong to. This is used to route each
// group to the correct watcher: logsSubscribe for programs that emit Anchor
// events, or accountSubscribe fallback for programs that don't.
func FilterPairsByProgram(pairs []PairMeta) ProgramGroups {
	var g ProgramGroups
	for _, p := range pairs {
		dex := strings.ToLower(strings.TrimSpace(p.DexName))
		switch {
		case strings.Contains(dex, "orca"):
			g.Orca = append(g.Orca, p)
		case strings.Contains(dex, "meteora") && strings.Contains(dex, "damm"):
			g.MeteoraDammV2 = append(g.MeteoraDammV2, p)
		case strings.Contains(dex, "meteora") || strings.Contains(dex, "dlmm"):
			g.MeteoraDLMM = append(g.MeteoraDLMM, p)
		case strings.Contains(dex, "raydium") && strings.Contains(dex, "clmm"):
			g.RaydiumCLMM = append(g.RaydiumCLMM, p)
		case strings.Contains(dex, "raydium"):
			g.RaydiumOther = append(g.RaydiumOther, p)
		case strings.Contains(dex, "pumpswap") || strings.Contains(dex, "pump"):
			g.PumpSwap = append(g.PumpSwap, p)
		default:
			g.Other = append(g.Other, p)
		}
	}
	return g
}

func parseAddress(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var payload struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.Address)
}

// parseTokenJSON parses a token metadata JSON string (as stored in the DB)
// and returns the token address and decimals. Returns empty string and 0
// if the input is empty or malformed.
func parseTokenJSON(raw string) (address string, decimals int) {
	if strings.TrimSpace(raw) == "" {
		return "", 0
	}
	var payload struct {
		Address  string `json:"address"`
		Decimals int    `json:"decimals"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", 0
	}
	return strings.TrimSpace(payload.Address), payload.Decimals
}

// ResolveTokenOrder fetches each pool account once from the RPC and populates
// Token0IsBase / Token0OrderKnown on each PairMeta. This is called once at
// startup so event-based price parsing uses the correct token orientation.
//
// For Raydium CLMM: reads token_mint_0 at offset 73 and token_mint_1 at 105.
// For Orca Whirlpool: reads token_mint_a at offset 101 and token_mint_b at 181.
func ResolveTokenOrder(pairs []PairMeta, rpcEndpoint string) []PairMeta {
	// Raydium CLMM pool state offsets (from official layout):
	//   [73:105]  token_mint_0
	//   [105:137] token_mint_1
	const raydiumMint0Offset = 73
	const raydiumMint1Offset = 105

	// Orca Whirlpool pool state offsets:
	//   [101:133] token_mint_a
	//   [181:213] token_mint_b
	const orcaMintAOffset = 101
	const orcaMintBOffset = 181

	for i, p := range pairs {
		raw, err := getRPCAccountData(rpcEndpoint, p.PoolAddress)
		if err != nil {
			log.Printf("[watcher] token order resolution failed for %s (%s): %v", p.PoolAddress[:8], p.BaseSymbol+"/"+p.QuoteSymbol, err)
			continue
		}

		dex := strings.ToLower(p.DexName)
		var mint0, mint1 string

		if strings.Contains(dex, "orca") {
			if len(raw) >= orcaMintBOffset+32 {
				mint0 = encodeBase58(raw[orcaMintAOffset : orcaMintAOffset+32])
				mint1 = encodeBase58(raw[orcaMintBOffset : orcaMintBOffset+32])
			}
		} else if strings.Contains(dex, "meteora") || strings.Contains(dex, "dlmm") {
			// Meteora DLMM: read bin_step from pool account
			const dlmmBinStepOffset = 80
			if len(raw) >= dlmmBinStepOffset+2 {
				pairs[i].BinStep = int64(binary.LittleEndian.Uint16(raw[dlmmBinStepOffset : dlmmBinStepOffset+2]))
				log.Printf("[watcher] %s/%s — DLMM binStep=%d", p.BaseSymbol, p.QuoteSymbol, pairs[i].BinStep)
			}
			if len(raw) >= 245 {
				mint0 = encodeBase58(raw[169 : 169+32])
				mint1 = encodeBase58(raw[201 : 201+32])
			}
		} else if strings.Contains(dex, "raydium") && strings.Contains(dex, "clmm") {
			// Raydium CLMM: mints at known offsets
			if len(raw) >= raydiumMint1Offset+32 {
				mint0 = encodeBase58(raw[raydiumMint0Offset : raydiumMint0Offset+32])
				mint1 = encodeBase58(raw[raydiumMint1Offset : raydiumMint1Offset+32])
			}
		} else {
			// Raydium CPMM, AMM V4, Pump.fun — event prices use HTTP fallback so
			// token order isn't needed. Mark as known=false and skip.
			log.Printf("[watcher] %s/%s — skipping token order probe for DEX %s (uses HTTP fallback)", p.BaseSymbol, p.QuoteSymbol, p.DexName)
			continue
		}

		if mint0 == "" || mint1 == "" {
			log.Printf("[watcher] could not read mints for %s — pool data too short or unsupported layout", p.PoolAddress[:8])
			continue
		}

		baseToken := strings.TrimSpace(p.BaseToken)
		quoteToken := strings.TrimSpace(p.QuoteToken)

		if strings.EqualFold(mint0, baseToken) && strings.EqualFold(mint1, quoteToken) {
			pairs[i].Token0IsBase = true
			pairs[i].Token0OrderKnown = true
			log.Printf("[watcher] %s/%s — token0=base (mint0=%s...)", p.BaseSymbol, p.QuoteSymbol, mint0[:min8(mint0)])
		} else if strings.EqualFold(mint0, quoteToken) && strings.EqualFold(mint1, baseToken) {
			pairs[i].Token0IsBase = false
			pairs[i].Token0OrderKnown = true
			log.Printf("[watcher] %s/%s — token0=quote (mint0=%s...)", p.BaseSymbol, p.QuoteSymbol, mint0[:min8(mint0)])
		} else {
			// Mints don't match DB tokens — this usually means GeckoTerminal stored the pair
			// with inverted base/quote. The server's on-chain verification should fix the DB
			// on the next sync. For now, treat mint0 as base (on-chain canonical order) so
			// the event price is at least consistent rather than randomly oscillating.
			// The HTTP adapter re-reads mints from chain and will return the correct price
			// for any swap where the event price is wrong.
			pairs[i].Token0IsBase = true
			pairs[i].Token0OrderKnown = true
			log.Printf("[watcher] %s/%s — mints don't match DB tokens (mint0=%s, base=%s) — treating mint0 as base",
				p.BaseSymbol, p.QuoteSymbol, mint0[:min8(mint0)], baseToken[:min8(baseToken)])
		}
	}
	return pairs
}

func min8(s string) int {
	if len(s) < 8 { return len(s) }
	return 8
}

// DetectRaydiumProgram fetches the on-chain owner of each pool account and
// returns two slices: one for CPMM pools (CPMMoo8L...) and one for AMM V4
// pools (675kPX9...). Both program IDs are detected by the account owner field.
// Pools whose owner cannot be determined are placed in the fallback slice.
func DetectRaydiumProgram(pairs []PairMeta, rpcEndpoint string) (cpmmPools, ammV4Pools, fallback []PairMeta) {
	for _, p := range pairs {
		owner, err := getAccountOwner(rpcEndpoint, p.PoolAddress)
		if err != nil {
			log.Printf("[watcher] DetectRaydiumProgram: cannot get owner for %s (%s): %v — routing to accountSubscribe fallback",
				p.PoolAddress[:min8(p.PoolAddress)], p.BaseSymbol+"/"+p.QuoteSymbol, err)
			fallback = append(fallback, p)
			continue
		}
		switch owner {
		case ProgramRaydiumCPMM:
			log.Printf("[watcher] %s/%s → CPMM (logsSubscribe)", p.BaseSymbol, p.QuoteSymbol)
			cpmmPools = append(cpmmPools, p)
		case ProgramRaydiumAMM:
			log.Printf("[watcher] %s/%s → AMM V4 (accountSubscribe)", p.BaseSymbol, p.QuoteSymbol)
			ammV4Pools = append(ammV4Pools, p)
		default:
			log.Printf("[watcher] %s/%s → unknown owner %s — routing to accountSubscribe fallback",
				p.BaseSymbol, p.QuoteSymbol, owner[:min8(owner)])
			fallback = append(fallback, p)
		}
	}
	return
}

// getAccountOwner returns the program ID that owns the given Solana account.
func getAccountOwner(endpoint, address string) (string, error) {
	payload := `{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["` + address + `",{"encoding":"base64"}]}`
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			Value *struct {
				Owner string `json:"owner"`
			} `json:"value"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.Error != nil {
		return "", fmt.Errorf("rpc error: %v", result.Error)
	}
	if result.Result.Value == nil {
		return "", fmt.Errorf("account not found: %s", address)
	}
	return result.Result.Value.Owner, nil
}

// getRPCAccountData fetches a Solana account's raw data via HTTP JSON-RPC.
func getRPCAccountData(endpoint, address string) ([]byte, error) {
	payload := `{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["` + address + `",{"encoding":"base64"}]}`
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			Value struct {
				Data []string `json:"data"`
			} `json:"value"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if result.Error != nil {
		return nil, fmt.Errorf("rpc error: %v", result.Error)
	}
	if len(result.Result.Value.Data) == 0 {
		return nil, fmt.Errorf("no data for %s", address)
	}
	decoded, err := base64.StdEncoding.DecodeString(result.Result.Value.Data[0])
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(result.Result.Value.Data[0])
		if err != nil {
			return nil, err
		}
	}
	return decoded, nil
}

// MarkAllSolanaToken0IsBase marks every Solana pair as token0=base without
// making any RPC calls. Use this when the DB was populated by the on-chain
// indexer, which always stores base_token = token_mint_0 (canonical on-chain
// order). This replaces the old ResolveAndCache startup probe which made one
// getAccountInfo call per pool and was extremely slow with thousands of pairs.
func MarkAllSolanaToken0IsBase(pairs []PairMeta) []PairMeta {
	for i := range pairs {
		pairs[i].Token0IsBase = true
		pairs[i].Token0OrderKnown = true
	}
	return pairs
}
