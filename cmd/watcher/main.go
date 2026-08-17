// cmd/watcher: real-time price update daemon — all chains.
//
// 1. Runs an initial sync for Solana pools.
// 2. Connects to Solana WebSocket (sharded across 5 endpoints).
// 3. Connects to BSC WebSocket (3 endpoints, sharded).
// 4. Connects to Base WebSocket (3 endpoints, sharded).
// 5. Fallback full sync every 5 minutes.
//
// Usage:
//   DATABASE_URL=... RPC_ENDPOINT_SOLANA=https://... go run cmd/watcher/main.go
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
	"on-chain-price-fetcher/internal/adapters/meteora"
	"on-chain-price-fetcher/internal/adapters/orca"
	"on-chain-price-fetcher/internal/adapters/pancakeswap"
	"on-chain-price-fetcher/internal/adapters/pumpswap"
	"on-chain-price-fetcher/internal/adapters/raydium"
	"on-chain-price-fetcher/internal/adapters/shared"
	"on-chain-price-fetcher/internal/adapters/slipstream"
	"on-chain-price-fetcher/internal/adapters/uniswap"
	"on-chain-price-fetcher/internal/watcher"
)

// PriceHit represents a logged price event
type PriceHit struct {
	Time        string  `json:"time"`
	Chain       string  `json:"chain"`
	Source      string  `json:"source"` // "websocket" for WebSocket-triggered events
	PoolAddress string  `json:"pool"`
	Pair        string  `json:"pair"`
	Dex         string  `json:"dex"`
	Price       float64 `json:"price,omitempty"`
	Error       string  `json:"error,omitempty"`
}

// JSONLLogger thread-safe JSONL price logger
type JSONLLogger struct {
	mu   sync.Mutex
	file *os.File
}

func NewJSONLLogger(filename string) (*JSONLLogger, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	return &JSONLLogger{file: f}, nil
}

func (l *JSONLLogger) LogHit(hit PriceHit) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, _ := json.Marshal(hit)
	l.file.Write(b)
	l.file.WriteString("\n")
}

func (l *JSONLLogger) Close() {
	l.file.Close()
}

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// toWSS converts https:// → wss:// for WebSocket connections.
func toWSS(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if strings.HasPrefix(endpoint, "https://") {
		return "wss://" + endpoint[8:]
	}
	if strings.HasPrefix(endpoint, "http://") {
		return "ws://" + endpoint[7:]
	}
	return endpoint
}

func main() {
	dsn := getEnv("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:55422/postgres?sslmode=disable")

	// Initialize JSONL price hit logger
	priceLogger, err := NewJSONLLogger("watcher_price_hits.jsonl")
	if err != nil {
		log.Fatal("Failed to create price logger:", err)
	}
	defer priceLogger.Close()
	log.Println("Price logging to: watcher_price_hits.jsonl")

	// ── Solana endpoints ──────────────────────────────────────────────────────
	// HTTP: Round-robin across multiple endpoints to avoid 429 rate limits
	// All 3 Chainstack nodes + 2 Infura as fallback
	solanaHTTPEndpoints := []string{
		"https://solana-mainnet.core.chainstack.com/49074bea8522c7e9a1e16b9d971842cf",
		"https://solana-mainnet.core.chainstack.com/54ce8267c02c230db8cf40ae8c432e1e",
		"https://solana-mainnet.core.chainstack.com/d367c1187485443d0f826f06ff52c072",
		"https://solana-mainnet.infura.io/v3/dc04465ea4df4ada8447799e1d601a09",
		"https://solana-mainnet.infura.io/v3/02ffb571dc0c415c87c4bf14f9af15e1",
	}
	// Primary HTTP endpoint for adapters (used at startup for token order resolution)
	solanaRPC := solanaHTTPEndpoints[0]

	// WS: 5 fully tested endpoints (logsSubscribe + accountSubscribe)
	solanaWSEndpoints := []string{
		"wss://api.mainnet-beta.solana.com",
		"wss://solana-rpc.publicnode.com",
		"wss://solana-mainnet.core.chainstack.com/49074bea8522c7e9a1e16b9d971842cf",
		"wss://solana-mainnet.core.chainstack.com/54ce8267c02c230db8cf40ae8c432e1e",
		"wss://solana-mainnet.core.chainstack.com/d367c1187485443d0f826f06ff52c072",
	}

	// ── BSC endpoints ─────────────────────────────────────────────────────────
	// HTTP: round-robin across multiple endpoints — mirrors the Solana pattern.
	// Primary is QuickNode; fallbacks are public nodes for resilience.
	bscHTTPEndpoints := []string{
		getEnv("RPC_ENDPOINT_BSC", "https://smart-sly-voice.bsc.quiknode.pro/68e23973e7772747604cc40a754b8349c20db22c/"),
		"https://bsc-dataseed.nariox.org/",
		"https://bsc-dataseed1.defibit.io/",
		"https://bsc-dataseed1.ninicoin.io/",
		"https://bsc.drpc.org",
	}
	bscWSEndpoints := []string{
		"wss://bsc-mainnet.infura.io/ws/v3/a3830c325e5f45d78770301a768c6339",
		"wss://bsc-mainnet.infura.io/ws/v3/0a3026fd58c745bf8836a13e1f56d3fb",
		"wss://bsc-mainnet.infura.io/ws/v3/353787b97b914a159acb32a6dd6900ab",
		"wss://bsc-rpc.publicnode.com",
		"wss://bsc.drpc.org",
	}

	// ── Base endpoints ────────────────────────────────────────────────────────
	// HTTP: round-robin across multiple endpoints.
	// Primary is QuickNode; fallbacks are public Base nodes.
	baseHTTPEndpoints := []string{
		getEnv("RPC_ENDPOINT_BASE", "https://palpable-divine-shape.base-mainnet.quiknode.pro/6bbce19b5765801546265b33f2d1fdb2aafa9cc8/"),
		"https://mainnet.base.org",
		"https://base.drpc.org",
		"https://base-rpc.publicnode.com",
	}
	baseWSEndpoints := []string{
		"wss://base-mainnet.infura.io/ws/v3/9df9281ff02b4bd4b446bf1744a69f84",
		"wss://base-mainnet.infura.io/ws/v3/e305c7c893eb48bd8de255d3c32548c1",
		"wss://base-mainnet.infura.io/ws/v3/ef85f4ac62914935a71b3f65ab1d51aa",
	}
	if ep := strings.TrimSpace(os.Getenv("RPC_WS_BASE_1")); ep != "" { baseWSEndpoints[0] = ep }
	if ep := strings.TrimSpace(os.Getenv("RPC_WS_BASE_2")); ep != "" { baseWSEndpoints[1] = ep }
	if ep := strings.TrimSpace(os.Getenv("RPC_WS_BASE_3")); ep != "" { baseWSEndpoints[2] = ep }

	log.Printf("=== Multi-Chain Price Watcher ===")
	log.Printf("Solana WS shards: %d  HTTP endpoints: %d", len(solanaWSEndpoints), len(solanaHTTPEndpoints))
	log.Printf("BSC    WS shards: %d  HTTP endpoints: %d", len(bscWSEndpoints), len(bscHTTPEndpoints))
	log.Printf("Base   WS shards: %d  HTTP endpoints: %d", len(baseWSEndpoints), len(baseHTTPEndpoints))

	// Log whether DATABASE_URL was provided (without leaking the password)
	if rawURL := strings.TrimSpace(os.Getenv("DATABASE_URL")); rawURL == "" {
		log.Println("[db] WARNING: DATABASE_URL not set — using local fallback (will fail on fly.io)")
	} else {
		log.Printf("[db] DATABASE_URL is set (length=%d)", len(rawURL))
	}
	log.Printf("[db] connecting with DSN length=%d", len(dsn))

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("[db] sql.Open failed: %v", err)
	}
	defer db.Close()

	// Retry db.Ping up to 5 times with backoff — fly.io internal DNS sometimes
	// takes a few seconds to resolve on first boot.
	connected := false
	for attempt := 1; attempt <= 5; attempt++ {
		if err := db.Ping(); err != nil {
			log.Printf("[db] ping attempt %d/5 failed: %v — retrying in 3s", attempt, err)
			time.Sleep(3 * time.Second)
			continue
		}
		connected = true
		break
	}
	if !connected {
		log.Fatal("[db] could not connect to database after 5 attempts — check DATABASE_URL secret")
	}
	log.Println("[db] Connected to database")

	// ── Adapters (HTTP) ───────────────────────────────────────────────────────
	// EVM adapters are created on-the-fly per call in evmFetch (round-robin),
	// mirroring the Solana pattern. No single-endpoint adapters are pre-built.

	// ── Step 1: Initial sync — all chains (DISABLED) ──────────────────────────
	// HTTP polling disabled in favor of real-time WebSocket watchers only
	log.Println("HTTP polling DISABLED — using WebSocket watchers only")

	// ── Step 2: Solana fetch function with round-robin HTTP load balancing ────
	// Round-robin across multiple HTTP endpoints to avoid 429 rate limits.
	// Each call picks the next endpoint in rotation.
	var solanaRPCIdx int64
	solanaFetch := func(poolAddress string, pair shared.Pair) (float64, error) {
		// Round-robin: pick next endpoint
		idx := int(atomic.AddInt64(&solanaRPCIdx, 1)-1) % len(solanaHTTPEndpoints)
		endpoint := solanaHTTPEndpoints[idx]

		dex := strings.ToLower(pair.DexName)

		// Build adapters on-the-fly with the selected endpoint
		var result shared.PriceResult
		var fetchErr error
		switch {
		case strings.Contains(dex, "raydium"):
			result, fetchErr = raydium.Adapter{RPC: raydium.RPCClient{Endpoint: endpoint}}.FetchPrice(pair)
		case strings.Contains(dex, "orca"):
			result, fetchErr = orca.Adapter{RPC: orca.RPCClient{Endpoint: endpoint}}.FetchPrice(pair)
		case strings.Contains(dex, "meteora") || strings.Contains(dex, "dlmm") || strings.Contains(dex, "damm"):
			result, fetchErr = meteora.Adapter{RPC: &meteora.RPCClient{Endpoint: endpoint}}.FetchPrice(pair)
		case strings.Contains(dex, "pumpswap") || strings.Contains(dex, "pump"):
			result, fetchErr = pumpswap.Adapter{RPC: pumpswap.RPCClient{Endpoint: endpoint}}.FetchPrice(pair)
		default:
			fetchErr = fmt.Errorf("unsupported dex: %s", pair.DexName)
		}
		if fetchErr != nil { return 0, fetchErr }
		if !result.Valid { return 0, fmt.Errorf("invalid: %s", result.Reason) }
		return result.Price, nil
	}

	// ── Step 3: EVM fetch function with round-robin HTTP load balancing ───────
	// Same pattern as Solana: each chain has its own endpoint slice and counter.
	// On every swap event that needs an HTTP fallback, picks the next endpoint.
	var bscRPCIdx, baseRPCIdx int64
	evmFetch := func(pair shared.Pair) (float64, error) {
		dex := strings.ToLower(pair.DexName)
		network := strings.ToLower(pair.Network)

		// Pick endpoint round-robin per chain
		var endpoint string
		if network == "base" {
			idx := int(atomic.AddInt64(&baseRPCIdx, 1)-1) % len(baseHTTPEndpoints)
			endpoint = baseHTTPEndpoints[idx]
		} else {
			idx := int(atomic.AddInt64(&bscRPCIdx, 1)-1) % len(bscHTTPEndpoints)
			endpoint = bscHTTPEndpoints[idx]
		}

		// Build adapter on-the-fly with the selected endpoint
		var result shared.PriceResult
		var fetchErr error
		switch {
		case strings.Contains(dex, "uniswap"):
			result, fetchErr = uniswap.Adapter{RPC: uniswap.RPCClient{Endpoint: endpoint}}.FetchPrice(pair)
		case strings.Contains(dex, "slipstream") || strings.Contains(dex, "aerodrome"):
			result, fetchErr = slipstream.Adapter{RPC: slipstream.RPCClient{Endpoint: endpoint}}.FetchPrice(pair)
		default:
			result, fetchErr = pancakeswap.Adapter{RPC: pancakeswap.RPCClient{Endpoint: endpoint}}.FetchPrice(pair)
		}
		if fetchErr != nil { return 0, fetchErr }
		if !result.Valid { return 0, fmt.Errorf("invalid: %s", result.Reason) }
		return result.Price, nil
	}

	// ── Step 4: Load pairs ────────────────────────────────────────────────────
	solanaPairs, err := watcher.LoadSolanaPairs(db)
	if err != nil { log.Fatalf("Failed to load Solana pairs: %v", err) }
	log.Printf("Loaded %d Solana pools", len(solanaPairs))

	// Resolve token order with cache — only RPC-probe pools not already cached.
	// Cache file persists across restarts so subsequent startups are instant.
	tokenOrderCache := watcher.NewTokenOrderCache("token_order_cache.json")
	cachedPairs, uncachedPairs := tokenOrderCache.ApplyToSolanaPairs(solanaPairs)
	log.Printf("[token-order-cache] %d pairs from cache, %d need RPC probe",
		len(cachedPairs), len(uncachedPairs))

	if len(uncachedPairs) > 0 {
		log.Printf("Resolving token order for %d new Solana pairs via RPC...", len(uncachedPairs))
		resolvedNew := watcher.ResolveAndCache(uncachedPairs, solanaRPC, tokenOrderCache)
		solanaPairs = append(cachedPairs, resolvedNew...)
	} else {
		solanaPairs = cachedPairs
		log.Printf("All Solana pairs loaded from token order cache — skipping RPC probes")
	}

	basePairs, err := watcher.LoadEVMPairs(db, "base")
	if err != nil { log.Fatalf("Failed to load Base pairs: %v", err) }
	log.Printf("Loaded %d Base pools", len(basePairs))

	bscPairs, err := watcher.LoadEVMPairs(db, "bsc")
	if err != nil { log.Fatalf("Failed to load BSC pairs: %v", err) }
	log.Printf("Loaded %d BSC pools", len(bscPairs))

	// ── Step 6: Start WebSocket watchers ──────────────────────────────────────

	// Program-level watchers — each subscribes to ONE program logsSubscribe
	// This catches ALL swaps across ALL pools in that program with a single subscription
	// Using public Solana WS endpoint (QuickNode WSS blocks logsSubscribe)
	raydiumCLMMWatcher := watcher.NewProgramWatcherWithCallback(
		solanaWSEndpoints[0],
		watcher.ProgramRaydiumCLMM,
		db, solanaPairs,
		func(poolAddress string, pair shared.Pair) (float64, error) {
			return solanaFetch(poolAddress, pair)
		},
		func(poolAddress string, pair shared.Pair, price float64, source string) {
			priceLogger.LogHit(PriceHit{
				Time:        time.Now().UTC().Format(time.RFC3339),
				Chain:       "solana",
				Source:      source, // "event" or "http-fallback"
				PoolAddress: pair.PoolAddress,
				Pair:        pair.BaseSymbol + "/" + pair.QuoteSymbol,
				Dex:         pair.DexName,
				Price:       price,
			})
		},
	)
	orcaWatcher := watcher.NewProgramWatcherWithCallback(
		solanaWSEndpoints[0],
		watcher.ProgramOrcaWhirlpool,
		db, solanaPairs,
		func(poolAddress string, pair shared.Pair) (float64, error) {
			return solanaFetch(poolAddress, pair)
		},
		func(poolAddress string, pair shared.Pair, price float64, source string) {
			priceLogger.LogHit(PriceHit{
				Time:        time.Now().UTC().Format(time.RFC3339),
				Chain:       "solana",
				Source:      source,
				PoolAddress: pair.PoolAddress,
				Pair:        pair.BaseSymbol + "/" + pair.QuoteSymbol,
				Dex:         pair.DexName,
				Price:       price,
			})
		},
	)
	raydiumCPMMWatcher := watcher.NewProgramWatcherWithCallback(
		solanaWSEndpoints[0],
		watcher.ProgramRaydiumCPMM,
		db, solanaPairs,
		func(poolAddress string, pair shared.Pair) (float64, error) {
			return solanaFetch(poolAddress, pair)
		},
		func(poolAddress string, pair shared.Pair, price float64, source string) {
			priceLogger.LogHit(PriceHit{
				Time:        time.Now().UTC().Format(time.RFC3339),
				Chain:       "solana",
				Source:      source,
				PoolAddress: pair.PoolAddress,
				Pair:        pair.BaseSymbol + "/" + pair.QuoteSymbol,
				Dex:         pair.DexName,
				Price:       price,
			})
		},
	)
	pumpSwapWatcher := watcher.NewProgramWatcherWithCallback(
		solanaWSEndpoints[0],
		watcher.ProgramPumpSwap,
		db, solanaPairs,
		func(poolAddress string, pair shared.Pair) (float64, error) {
			return solanaFetch(poolAddress, pair)
		},
		func(poolAddress string, pair shared.Pair, price float64, source string) {
			priceLogger.LogHit(PriceHit{
				Time:        time.Now().UTC().Format(time.RFC3339),
				Chain:       "solana",
				Source:      source,
				PoolAddress: pair.PoolAddress,
				Pair:        pair.BaseSymbol + "/" + pair.QuoteSymbol,
				Dex:         pair.DexName,
				Price:       price,
			})
		},
	)

	go raydiumCLMMWatcher.Start()
	go orcaWatcher.Start()
	go raydiumCPMMWatcher.Start()
	go pumpSwapWatcher.Start()
	log.Println("Started program-level watchers: Raydium CLMM/CPMM + Orca + Pump.fun bonding curve")

	// Fallback accountSubscribe watchers — covers Meteora DLMM/DAMM, Raydium AMM V4, Pump.fun AMM
	// These programs don't emit usable logsSubscribe events so we subscribe per-pool
	fallbackSolanaWatchers := watcher.NewShardedWatchers(
		solanaWSEndpoints,
		db, solanaPairs,
		func(poolAddress string, pair shared.Pair) (float64, error) {
			price, err := solanaFetch(poolAddress, pair)
			src := "http"
			if err == nil && price > 0 {
				priceLogger.LogHit(PriceHit{
					Time:        time.Now().UTC().Format(time.RFC3339),
					Chain:       "solana",
					Source:      src,
					PoolAddress: pair.PoolAddress,
					Pair:        pair.BaseSymbol + "/" + pair.QuoteSymbol,
					Dex:         pair.DexName,
					Price:       price,
				})
			} else if err != nil {
				priceLogger.LogHit(PriceHit{
					Time:        time.Now().UTC().Format(time.RFC3339),
					Chain:       "solana",
					Source:      src,
					PoolAddress: pair.PoolAddress,
					Pair:        pair.BaseSymbol + "/" + pair.QuoteSymbol,
					Dex:         pair.DexName,
					Error:       err.Error(),
				})
			}
			return price, err
		})
	for _, sw := range fallbackSolanaWatchers {
		go sw.Start()
	}
	log.Printf("Started %d Solana fallback per-pool watcher shard(s)", len(fallbackSolanaWatchers))

	// BSC — sharded across 5 endpoints
	bscWatchers := watcher.NewShardedEVMWatchersWithCallback(bscWSEndpoints, db, bscPairs, evmFetch,
		func(pair watcher.EVMPairMeta, price float64, source string) {
			priceLogger.LogHit(PriceHit{
				Time:        time.Now().UTC().Format(time.RFC3339),
				Chain:       "bsc",
				Source:      source,
				PoolAddress: pair.PoolAddress,
				Pair:        pair.BaseSymbol + "/" + pair.QuoteSymbol,
				Dex:         pair.DexName,
				Price:       price,
			})
		})
	for _, ew := range bscWatchers {
		go ew.Start()
	}
	log.Printf("Started %d BSC EVM watcher shards (%d pools)", len(bscWatchers), len(bscPairs))

	// Base — sharded across 3 Infura endpoints
	baseWatchers := watcher.NewShardedEVMWatchersWithCallback(baseWSEndpoints, db, basePairs, evmFetch,
		func(pair watcher.EVMPairMeta, price float64, source string) {
			priceLogger.LogHit(PriceHit{
				Time:        time.Now().UTC().Format(time.RFC3339),
				Chain:       "base",
				Source:      source,
				PoolAddress: pair.PoolAddress,
				Pair:        pair.BaseSymbol + "/" + pair.QuoteSymbol,
				Dex:         pair.DexName,
				Price:       price,
			})
		})
	for _, ew := range baseWatchers {
		go ew.Start()
	}
	log.Printf("Started %d Base EVM watcher shards (%d pools)", len(baseWatchers), len(basePairs))

	// ── Step 7: Pair reloader — picks up newly indexed pools every 15 min ────
	// When the pair indexer adds new pools to the DB, this will detect them
	// and add them to the appropriate watchers for live price tracking.
	reloader := watcher.NewPairReloader(
		db,
		15*time.Minute,
		// onNewEVMPairs: distribute new EVM pairs across existing watchers
		func(network string, newPairs []watcher.EVMPairMeta) {
			var watchers []*watcher.EVMWatcher
			if network == "bsc" {
				watchers = bscWatchers
			} else {
				watchers = baseWatchers
			}
			if len(watchers) == 0 {
				return
			}
			// Round-robin distribution across shards
			for i, p := range newPairs {
				watchers[i%len(watchers)].AddPairs([]watcher.EVMPairMeta{p})
			}
		},
		// onNewSolanaPairs: resolve token order first, cache it, then inject into watchers
		func(newPairs []watcher.PairMeta) {
			if len(fallbackSolanaWatchers) == 0 {
				return
			}
			// Resolve token order for new pools and add to cache
			resolvedPairs := watcher.ResolveAndCache(newPairs, solanaRPC, tokenOrderCache)
			for i, p := range resolvedPairs {
				fallbackSolanaWatchers[i%len(fallbackSolanaWatchers)].AddPairs([]watcher.PairMeta{p})
			}
		},
	)
	// Seed with already-loaded pairs so they don't trigger false callbacks
	reloader.SeedKnown(
		append(append([]watcher.EVMPairMeta{}, bscPairs...), basePairs...),
		solanaPairs,
	)
	go reloader.Start()
	log.Println("Started pair reloader — checking for new pools every 15 minutes")

	// ── Step 8: Fallback polling DISABLED ────────────────────────────────────
	// Disabled periodic sync in favor of WebSocket-only approach
	// ticker := time.NewTicker(5 * time.Minute)
	// defer ticker.Stop()

	log.Println("✅ All watchers running — real-time updates active across Solana, BSC, Base")
	log.Println("   HTTP polling DISABLED — WebSocket-only mode")

	// Keep alive - wait for interrupt instead of polling
	select {}
}
