package watcher

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"on-chain-price-fetcher/internal/adapters/shared"
	"on-chain-price-fetcher/internal/stats"
)

// EVMPairMeta holds metadata for an EVM pool.
type EVMPairMeta struct {
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

// EVMWatcher subscribes to Swap events on EVM chains (Base, BSC) via
// eth_subscribe → logs. It extracts sqrtPriceX96 (V3/CLMM) or reserve
// amounts (V2) directly from the log data without additional RPC calls.
type EVMWatcher struct {
	wsEndpoint string
	db         *sql.DB
	network    string // "bsc" or "base"

	// fetchFn is called with pair metadata when a swap event is received.
	fetchFn func(pair shared.Pair) (float64, error)

	// priceCallback is called whenever a price is successfully written to DB.
	// source is "event" (extracted from swap data) or "http" (HTTP fallback).
	// May be nil.
	priceCallback func(pair EVMPairMeta, price float64, source string)

	mu    sync.Mutex
	pairs []EVMPairMeta
	conn  *websocket.Conn // live connection, nil when disconnected

	// newPairsCh receives new pairs to subscribe without reconnecting
	newPairsCh chan []EVMPairMeta

	stopCh chan struct{}
}

// Swap event topic hashes
const (
	// Uniswap V3 / PancakeSwap V3: Swap(address,address,int256,int256,uint160,uint128,int24)
	swapTopicV3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
	// Uniswap V2 / PancakeSwap V2: Swap(address,uint256,uint256,uint256,uint256,address)
	swapTopicV2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
	// Slipstream Base V2: Swap(address,address,uint256,uint256,uint256,uint256)
	swapTopicSlipstreamV2 = "0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b"
	// PancakeSwap Infinity CLMM / Uniswap V4: Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24,uint16)
	swapTopicCLMM = "0x04206ad2b7c0f463bff3dd4f33c5735b0f2957a351e4f79763a4fa9e775dd237"
	
	// Pool Manager addresses
	pancakeInfinityPoolManager = "0xa0ffb9c1ce1fe56963b0321b32e7a0302114058b" // BSC
	uniswapV4PoolManagerBSC    = "0x28e2ea090877bf75740558f6bfb36a5ffee9e9df" // BSC
	uniswapV4PoolManagerBase   = "0x498581ff718922c3f8e6a244956af099b2652b2b" // Base
)

func NewEVMWatcher(
	wsEndpoint string,
	db *sql.DB,
	pairs []EVMPairMeta,
	fetchFn func(pair shared.Pair) (float64, error),
) *EVMWatcher {
	return NewEVMWatcherWithCallback(wsEndpoint, db, pairs, fetchFn, nil)
}

// NewEVMWatcherWithCallback creates a watcher that calls priceCallback on every
// successful price update with source="event" or source="http".
func NewEVMWatcherWithCallback(
	wsEndpoint string,
	db *sql.DB,
	pairs []EVMPairMeta,
	fetchFn func(pair shared.Pair) (float64, error),
	priceCallback func(pair EVMPairMeta, price float64, source string),
) *EVMWatcher {
	network := "base"
	if len(pairs) > 0 {
		network = pairs[0].Network
	}
	return &EVMWatcher{
		wsEndpoint:    wsEndpoint,
		db:            db,
		pairs:         pairs,
		network:       network,
		fetchFn:       fetchFn,
		priceCallback: priceCallback,
		newPairsCh:    make(chan []EVMPairMeta, 64),
		stopCh:        make(chan struct{}),
	}
}

// NewShardedEVMWatchers distributes pairs evenly across multiple WS endpoints.
func NewShardedEVMWatchers(
	wsEndpoints []string,
	db *sql.DB,
	pairs []EVMPairMeta,
	fetchFn func(pair shared.Pair) (float64, error),
) []*EVMWatcher {
	return NewShardedEVMWatchersWithCallback(wsEndpoints, db, pairs, fetchFn, nil)
}

// NewShardedEVMWatchersWithCallback is like NewShardedEVMWatchers but fires
// priceCallback on every successful DB write.
func NewShardedEVMWatchersWithCallback(
	wsEndpoints []string,
	db *sql.DB,
	pairs []EVMPairMeta,
	fetchFn func(pair shared.Pair) (float64, error),
	priceCallback func(pair EVMPairMeta, price float64, source string),
) []*EVMWatcher {
	if len(wsEndpoints) == 0 || len(pairs) == 0 {
		return nil
	}
	n := len(wsEndpoints)
	shards := make([][]EVMPairMeta, n)
	for i, p := range pairs {
		shards[i%n] = append(shards[i%n], p)
	}
	watchers := make([]*EVMWatcher, n)
	for i, ep := range wsEndpoints {
		watchers[i] = NewEVMWatcherWithCallback(ep, db, shards[i], fetchFn, priceCallback)
		log.Printf("[watcher/evm] shard %d/%d → %s (%d pools)",
			i+1, n, ep, len(shards[i]))
	}
	return watchers
}

func (w *EVMWatcher) Start() {
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}
		if err := w.connectAndListen(); err != nil {
			log.Printf("[watcher/evm] disconnected: %v — reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (w *EVMWatcher) Stop() {
	close(w.stopCh)
}

func (w *EVMWatcher) connectAndListen() error {
	if len(w.pairs) == 0 {
		return nil
	}

	log.Printf("[watcher/evm] connecting to %s (%d pools)", w.wsEndpoint, len(w.pairs))
	conn, _, err := websocket.DefaultDialer.Dial(w.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Build list of pool addresses for the log filter
	// Separate standard pools from CLMM/V4 pool IDs
	var standardAddresses []string
	var clmmPoolIDs []string
	addrToPair := make(map[string]EVMPairMeta)
	poolIDToPair := make(map[string]EVMPairMeta)
	
	for _, p := range w.pairs {
		addr := strings.ToLower(p.PoolAddress)
		dex := strings.ToLower(p.DexName)
		
		// 66-character addresses (0x + 64 hex) are pool IDs for CLMM/V4
		if len(addr) == 66 && (strings.Contains(dex, "infinity") || strings.Contains(dex, "v4")) {
			// Keep the 0x prefix for topic filtering
			clmmPoolIDs = append(clmmPoolIDs, addr)
			poolIDToPair[addr] = p
			continue
		}
		
		// 42-character addresses are standard pool contracts
		if len(addr) == 42 {
			standardAddresses = append(standardAddresses, addr)
			addrToPair[addr] = p
			continue
		}
	}
	
	if len(standardAddresses) == 0 && len(clmmPoolIDs) == 0 {
		log.Printf("[watcher/evm] no valid addresses to subscribe to")
		return nil
	}
	
	log.Printf("[watcher/evm] loaded %d standard pools and %d CLMM/V4 pools",
		len(standardAddresses), len(clmmPoolIDs))

	// CRITICAL: Infura has a limit (~20) on addresses per eth_subscribe filter
	// Create subscriptions for standard pools (batched)
	const maxAddressesPerSub = 20
	var subscriptionBatches [][]string
	
	// Batch standard pool addresses
	for i := 0; i < len(standardAddresses); i += maxAddressesPerSub {
		end := i + maxAddressesPerSub
		if end > len(standardAddresses) {
			end = len(standardAddresses)
		}
		subscriptionBatches = append(subscriptionBatches, standardAddresses[i:end])
	}
	
	// Add CLMM Pool Manager subscriptions if we have CLMM/V4 pools
	if len(clmmPoolIDs) > 0 {
		// Separate pool IDs by Pool Manager
		var infinityPoolIDs []string
		var v4PoolIDs []string
		
		for _, poolID := range clmmPoolIDs {
			pair := poolIDToPair[poolID]
			dex := strings.ToLower(pair.DexName)
			if strings.Contains(dex, "infinity") {
				infinityPoolIDs = append(infinityPoolIDs, poolID)
			} else if strings.Contains(dex, "v4") {
				v4PoolIDs = append(v4PoolIDs, poolID)
			}
		}
		
		// Add Infinity Pool Manager subscription
		if len(infinityPoolIDs) > 0 {
			subscriptionBatches = append(subscriptionBatches, []string{pancakeInfinityPoolManager})
		}
		
		// Add V4 Pool Manager subscription
		if len(v4PoolIDs) > 0 {
			v4Manager := uniswapV4PoolManagerBSC
			if w.network == "base" {
				v4Manager = uniswapV4PoolManagerBase
			}
			subscriptionBatches = append(subscriptionBatches, []string{v4Manager})
		}
	}

	clmmManagerCount := 0
	if len(clmmPoolIDs) > 0 {
		clmmManagerCount = len(subscriptionBatches) - (len(standardAddresses)+maxAddressesPerSub-1)/maxAddressesPerSub
	}
	
	log.Printf("[watcher/evm] creating %d subscriptions (%d standard batches, %d pool managers for %d CLMM/V4 pools)",
		len(subscriptionBatches), len(subscriptionBatches)-clmmManagerCount, clmmManagerCount, len(clmmPoolIDs))

	// Subscribe to each batch
	var subscriptionIDs []string
	standardBatchCount := (len(standardAddresses) + maxAddressesPerSub - 1) / maxAddressesPerSub
	
	for batchIdx, batch := range subscriptionBatches {
		// Determine if this is a Pool Manager batch and which one
		v4Manager := uniswapV4PoolManagerBSC
		if len(w.pairs) > 0 && w.pairs[0].Network == "base" {
			v4Manager = uniswapV4PoolManagerBase
		}
		
		isInfinityManager := len(clmmPoolIDs) > 0 && batchIdx == standardBatchCount && len(batch) == 1 && batch[0] == pancakeInfinityPoolManager
		isV4Manager := len(clmmPoolIDs) > 0 && len(batch) == 1 && batch[0] == v4Manager
		
		var topics [][]string
		var poolIDsForBatch []string
		
		if isInfinityManager {
			// Get Infinity pool IDs
			for _, poolID := range clmmPoolIDs {
				pair := poolIDToPair[poolID]
				if strings.Contains(strings.ToLower(pair.DexName), "infinity") {
					poolIDsForBatch = append(poolIDsForBatch, poolID)
				}
			}
			topics = [][]string{
				{swapTopicCLMM},
				poolIDsForBatch,
			}
		} else if isV4Manager {
			// Get V4 pool IDs
			for _, poolID := range clmmPoolIDs {
				pair := poolIDToPair[poolID]
				if strings.Contains(strings.ToLower(pair.DexName), "v4") {
					poolIDsForBatch = append(poolIDsForBatch, poolID)
				}
			}
			topics = [][]string{
				{swapTopicCLMM},
				poolIDsForBatch,
			}
		} else {
			// Standard pools: V2 and V3 swap topics
			topics = [][]string{{swapTopicV3, swapTopicV2}}
		}
		
		subReq := map[string]any{
			"jsonrpc": "2.0",
			"id":      batchIdx + 1,
			"method":  "eth_subscribe",
			"params": []any{
				"logs",
				map[string]any{
					"address": batch,
					"topics":  topics,
				},
			},
		}
		if err := conn.WriteJSON(subReq); err != nil {
			return fmt.Errorf("subscribe batch %d: %w", batchIdx, err)
		}
		
		// Wait for subscription confirmation
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("confirm batch %d: %w", batchIdx, err)
		}
		
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(msg, &raw); err != nil {
			return fmt.Errorf("parse batch %d response: %w", batchIdx, err)
		}
		
		// Check for error
		if errMsg, ok := raw["error"]; ok {
			return fmt.Errorf("batch %d error: %s", batchIdx, string(errMsg))
		}
		
		// Get subscription ID
		var subID string
		if result, ok := raw["result"]; ok {
			json.Unmarshal(result, &subID)
			subscriptionIDs = append(subscriptionIDs, subID)
			
			batchType := "standard"
			poolCount := len(batch)
			if isInfinityManager {
				batchType = "Infinity CLMM manager"
				poolCount = len(poolIDsForBatch)
			} else if isV4Manager {
				batchType = "V4 manager"
				poolCount = len(poolIDsForBatch)
			}
			
			log.Printf("[watcher/evm] subscription %d/%d confirmed: %s (%d %s pools)",
				batchIdx+1, len(subscriptionBatches), subID, poolCount, batchType)
		} else {
			return fmt.Errorf("batch %d: unexpected response format", batchIdx)
		}
	}

	for {
		select {
		case <-w.stopCh:
			return nil
		case freshPairs := <-w.newPairsCh:
			// New pools arrived — subscribe them on the live connection without reconnecting
			w.subscribeNewPairs(conn, freshPairs, addrToPair, poolIDToPair, len(subscriptionBatches))
			continue
		default:
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(msg, &raw); err != nil {
			continue
		}

		// Log notification
		method, _ := raw["method"]
		if string(method) != `"eth_subscription"` {
			continue
		}

		var params struct {
			Subscription string `json:"subscription"`
			Result       struct {
				Address string   `json:"address"`
				Topics  []string `json:"topics"`
				Data    string   `json:"data"`
			} `json:"result"`
		}
		if err := json.Unmarshal(raw["params"], &params); err != nil {
			continue
		}

		addr := strings.ToLower(params.Result.Address)
		
		// Check if this is a CLMM Pool Manager event (PancakeSwap Infinity or Uniswap V4)
		var pair EVMPairMeta
		var ok bool
		
		// Get network-specific V4 Pool Manager address
		v4PoolManager := uniswapV4PoolManagerBSC
		if w.network == "base" {
			v4PoolManager = uniswapV4PoolManagerBase
		}
		
		isPoolManager := addr == pancakeInfinityPoolManager || addr == v4PoolManager
		if isPoolManager && len(params.Result.Topics) >= 2 {
			// Extract pool ID from topic[1] (first indexed parameter) - keep 0x prefix
			poolID := strings.ToLower(params.Result.Topics[1])
			pair, ok = poolIDToPair[poolID]
			if !ok {
				continue // Pool ID not in our list
			}
		} else {
			// Standard pool contract event
			pair, ok = addrToPair[addr]
			if !ok {
				continue
			}
		}

		// Log swap detected with appropriate context
		if isPoolManager {
			// Show pool manager address for CLMM/V4
			log.Printf("[%s] swap detected on %s/%s", addr[:10]+"...", pair.BaseSymbol, pair.QuoteSymbol)
		} else {
			// Standard pool - use generic watcher tag
			log.Printf("[watcher/evm] swap detected on %s/%s (%s)", pair.BaseSymbol, pair.QuoteSymbol, addr[:10]+"...")
		}

		// For V3/CLMM: extract sqrtPriceX96 from the non-indexed data field
		// For V2: just trigger a fetch — it's fast with no slot0 call needed
		var price float64
		source := "event"
		if len(params.Result.Topics) > 0 {
			topic0 := params.Result.Topics[0]
			if strings.EqualFold(topic0, swapTopicV3) || strings.EqualFold(topic0, swapTopicCLMM) {
				// V3 and CLMM both have sqrtPriceX96 in the data field
				price = w.extractV3Price(params.Result.Data, pair)
			}
		}

		if price <= 0 {
			// Fallback: trigger a full fetch via the existing adapter
			source = "http"
			sharedPair := shared.Pair{
				ID:                 pair.ID,
				Network:            pair.Network,
				DexName:            pair.DexName,
				PoolAddress:        pair.PoolAddress,
				BaseToken:          pair.BaseToken,
				QuoteToken:         pair.QuoteToken,
				BaseTokenDecimals:  pair.BaseTokenDecimals,
				QuoteTokenDecimals: pair.QuoteTokenDecimals,
				BaseSymbol:         pair.BaseSymbol,
				QuoteSymbol:        pair.QuoteSymbol,
			}
			var fetchErr error
			price, fetchErr = w.fetchFn(sharedPair)
			if fetchErr != nil || price <= 0 {
				continue
			}
		}

		// Log detailed debug info before DB update
		log.Printf("[watcher/evm] calculated price for %s/%s: %.18g (len=%d chars)", 
			pair.BaseSymbol, pair.QuoteSymbol, price, len(fmt.Sprintf("%v", price)))
		
		if err := w.updatePrice(pair.ID, price); err != nil {
			log.Printf("[watcher/evm] DB update failed for %s (%s/%s) price=%.18g: %v", 
				pair.ID, pair.BaseSymbol, pair.QuoteSymbol, price, err)
			// Log the pair details to help debug
			log.Printf("[watcher/evm] pair details: base_decimals=%d quote_decimals=%d pool=%s", 
				pair.BaseTokenDecimals, pair.QuoteTokenDecimals, pair.PoolAddress)
			continue
		}
		log.Printf("[watcher/evm] ✓ %s/%s → %.10g", pair.BaseSymbol, pair.QuoteSymbol, price)

		// Fire price callback for JSONL logging
		if w.priceCallback != nil {
			w.priceCallback(pair, price, source)
		}
	}
}

// subscribeNewPairs sends eth_subscribe for newly added pools on the live connection.
// Standard pools get batched into groups of maxAddressesPerSub (Infura limit).
// CLMM/V4 pools are already covered by the Pool Manager subscription — just added to lookup.
// This runs inside the event loop and does NOT disconnect existing subscriptions.
func (w *EVMWatcher) subscribeNewPairs(
	conn *websocket.Conn,
	newPairs []EVMPairMeta,
	addrToPair map[string]EVMPairMeta,
	poolIDToPair map[string]EVMPairMeta,
	nextSubID int,
) {
	const maxAddressesPerSub = 20

	var stdAddrs []string
	for _, p := range newPairs {
		addr := strings.ToLower(p.PoolAddress)
		dex := strings.ToLower(p.DexName)
		if len(addr) == 66 && (strings.Contains(dex, "infinity") || strings.Contains(dex, "v4")) {
			// CLMM/V4 already covered by Pool Manager subscription — just register in lookup
			poolIDToPair[addr] = p
			log.Printf("[watcher/evm] CLMM/V4 %s/%s added to lookup (pool manager sub already active)", p.BaseSymbol, p.QuoteSymbol)
			continue
		}
		if len(addr) == 42 {
			stdAddrs = append(stdAddrs, addr)
			addrToPair[addr] = p
		}
	}

	// Send new eth_subscribe for standard pools in batches of 20
	for i := 0; i < len(stdAddrs); i += maxAddressesPerSub {
		end := i + maxAddressesPerSub
		if end > len(stdAddrs) {
			end = len(stdAddrs)
		}
		batch := stdAddrs[i:end]

		subReq := map[string]any{
			"jsonrpc": "2.0",
			"id":      nextSubID,
			"method":  "eth_subscribe",
			"params": []any{
				"logs",
				map[string]any{
					"address": batch,
					"topics":  [][]string{{swapTopicV3, swapTopicV2, swapTopicSlipstreamV2}},
				},
			},
		}
		if err := conn.WriteJSON(subReq); err != nil {
			log.Printf("[watcher/evm] live subscribe error for new batch: %v", err)
			return
		}
		nextSubID++
		log.Printf("[watcher/evm] ✅ live-subscribed %d new pools (no reconnect needed)", len(batch))
	}
}

// extractV3Price parses the sqrtPriceX96 from a V3 Swap event data field
// and converts it to a human-readable quote/base price.
// It calculates both direct and inverted prices, then selects the sane one.
func (w *EVMWatcher) extractV3Price(data string, pair EVMPairMeta) float64 {
	// data is hex-encoded: 0x + 5 × 32 bytes = 320 hex chars after 0x
	hex := strings.TrimPrefix(data, "0x")
	if len(hex) < 320 {
		return 0
	}
	// sqrtPriceX96 is at offset 64 (bytes 64–95, i.e. hex chars 128–191)
	sqrtHex := hex[128:192]
	sqrtPrice := new(big.Int)
	sqrtPrice.SetString(sqrtHex, 16)
	if sqrtPrice.Sign() == 0 {
		return 0
	}

	// Get decimals for token0 and token1
	// We need to determine token ordering - which token is token0?
	decimal0 := pair.BaseTokenDecimals
	decimal1 := pair.QuoteTokenDecimals
	
	// Calculate both orientations
	directPrice := w.calculateV3PriceOriented(sqrtPrice, decimal0, decimal1, true)
	invertedPrice := w.calculateV3PriceOriented(sqrtPrice, decimal0, decimal1, false)
	
	// Also calculate with swapped decimals to handle all possible token orderings
	swappedDirect := w.calculateV3PriceOriented(sqrtPrice, decimal1, decimal0, true)
	swappedInverted := w.calculateV3PriceOriented(sqrtPrice, decimal1, decimal0, false)
	
	// Use the same logic as HTTP adapters: choose the most reasonable price
	price := shared.ChooseSanePrice(directPrice, invertedPrice, swappedDirect, swappedInverted)
	
	return price
}

// calculateV3PriceOriented calculates V3 price with specific token ordering
func (w *EVMWatcher) calculateV3PriceOriented(sqrtPriceX96 *big.Int, decimal0, decimal1 int, token0IsBase bool) float64 {
	if sqrtPriceX96 == nil || sqrtPriceX96.Sign() == 0 {
		return 0
	}

	// price = (sqrtPriceX96 / 2^96)^2
	priceRat := shared.ConvertSqrtPriceX96ToPrice(sqrtPriceX96)
	if priceRat == nil {
		return 0
	}

	// Apply decimal adjustment: delta = decimal0 - decimal1
	delta := decimal0 - decimal1
	if delta > 0 {
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(delta)), nil)
		priceRat.Mul(priceRat, new(big.Rat).SetInt(multiplier))
	} else if delta < 0 {
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-delta)), nil)
		priceRat.Quo(priceRat, new(big.Rat).SetInt(divisor))
	}

	value, _ := priceRat.Float64()
	
	// If token0 is NOT base, invert the price
	if !token0IsBase {
		if value != 0 {
			value = 1 / value
		} else {
			return 0
		}
	}
	
	return value
}

func (w *EVMWatcher) updatePrice(pairID string, price float64) error {
	// Validate price is within reasonable bounds
	// Allow very small prices (down to 1e-18) and large prices (up to 1e12)
	if price <= 0 || price > 1e12 {
		return fmt.Errorf("price out of valid range (0, 1e12]: %g", price)
	}
	
	// Format price to exactly 18 decimal places to match Ethereum precision
	// This prevents any float64 representation issues and matches typical EVM precision
	formattedPrice := fmt.Sprintf("%.18f", price)
	roundedPrice, err := strconv.ParseFloat(formattedPrice, 64)
	if err != nil {
		return fmt.Errorf("failed to format price: %w", err)
	}
	
	return stats.UpdatePriceWithStats(w.db, pairID, roundedPrice, roundedPrice)
}

// LoadEVMPairs loads Base and BSC pairs from the DB.
func LoadEVMPairs(db *sql.DB, network string) ([]EVMPairMeta, error) {
	rows, err := db.Query(`
		SELECT id, network, dex_name, pool_address,
		       base_token, quote_token,
		       base_token_decimals, quote_token_decimals,
		       base_symbol, quote_symbol
		FROM pairs
		WHERE network = $1
		  AND (
		    dex_name ILIKE '%pancake%'
		    OR dex_name ILIKE '%uniswap%'
		    OR dex_name ILIKE '%slipstream%'
		    OR dex_name ILIKE '%aerodrome%'
		  )
		ORDER BY id
	`, network)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []EVMPairMeta
	for rows.Next() {
		var p EVMPairMeta
		var baseTokenJSON, quoteTokenJSON sql.NullString
		if err := rows.Scan(&p.ID, &p.Network, &p.DexName, &p.PoolAddress,
			&baseTokenJSON, &quoteTokenJSON,
			&p.BaseTokenDecimals, &p.QuoteTokenDecimals,
			&p.BaseSymbol, &p.QuoteSymbol); err != nil {
			return nil, err
		}
		p.BaseToken = parseEVMAddress(baseTokenJSON.String)
		p.QuoteToken = parseEVMAddress(quoteTokenJSON.String)
		if p.PoolAddress == "" {
			continue
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

func parseEVMAddress(raw string) string {
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
