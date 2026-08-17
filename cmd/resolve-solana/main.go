// cmd/resolve-solana: Solana token-order resolver — scheduled job.
//
// Connects to the production PostgreSQL (DATABASE_URL), loads all Solana pairs,
// and runs ResolveAndCache via RPC to populate token_order_cache.json.
// Then waits RESOLVE_INTERVAL (default 15 min) and repeats — picking up any
// new pools that the pair-indexer has added since the last run.
//
// The watcher (cmd/watcher/main.go) reads the same token_order_cache.json at
// startup, so running this job before or alongside the watcher means the
// watcher never blocks on RPC probes at start time.
//
// Usage:
//
//	DATABASE_URL=postgresql://... go run cmd/resolve-solana/main.go
//
// Environment variables:
//
//	DATABASE_URL         Postgres connection string (required)
//	RPC_ENDPOINT_SOLANA  HTTP RPC endpoint used for getAccountInfo probes
//	                     (defaults to public mainnet-beta)
//	TOKEN_ORDER_CACHE    Path to the JSON cache file
//	                     (defaults to token_order_cache.json)
//	RESOLVE_INTERVAL     How often to re-run, e.g. "15m", "1h"
//	                     (defaults to 15m; pass "once" to exit after one run)
package main

import (
	"database/sql"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"on-chain-price-fetcher/internal/watcher"
)

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func main() {
	dsn := getEnv("DATABASE_URL",
		"postgres://postgres:postgres@127.0.0.1:55422/postgres?sslmode=disable")

	solanaRPC := getEnv("RPC_ENDPOINT_SOLANA",
		"https://api.mainnet-beta.solana.com/")

	cachePath := getEnv("TOKEN_ORDER_CACHE", "token_order_cache.json")

	intervalStr := getEnv("RESOLVE_INTERVAL", "15m")
	once := strings.EqualFold(intervalStr, "once")

	var interval time.Duration
	if !once {
		var err error
		interval, err = time.ParseDuration(intervalStr)
		if err != nil {
			log.Fatalf("[resolve-solana] invalid RESOLVE_INTERVAL %q: %v", intervalStr, err)
		}
	}

	// Connect to DB with retry — fly.io internal DNS can take a few seconds on boot.
	if rawURL := strings.TrimSpace(os.Getenv("DATABASE_URL")); rawURL == "" {
		log.Println("[resolve-solana] WARNING: DATABASE_URL not set — using local fallback (will fail on fly.io)")
	} else {
		log.Printf("[resolve-solana] DATABASE_URL is set (length=%d)", len(rawURL))
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("[resolve-solana] db open: %v", err)
	}
	defer db.Close()

	connected := false
	for attempt := 1; attempt <= 5; attempt++ {
		if err := db.Ping(); err != nil {
			log.Printf("[resolve-solana] db ping attempt %d/5 failed: %v — retrying in 3s", attempt, err)
			time.Sleep(3 * time.Second)
			continue
		}
		connected = true
		break
	}
	if !connected {
		log.Fatal("[resolve-solana] could not connect to database after 5 attempts — check DATABASE_URL secret")
	}
	log.Println("[resolve-solana] connected to database")

	// ── Startup diagnostics ───────────────────────────────────────────────────
	// Log a quick summary of what's in the pairs table so we can verify
	// the DB has the expected data before attempting resolution.
	var totalPairs, solanaPairs int
	_ = db.QueryRow(`SELECT COUNT(*) FROM pairs`).Scan(&totalPairs)
	_ = db.QueryRow(`SELECT COUNT(*) FROM pairs WHERE network = 'solana'`).Scan(&solanaPairs)
	log.Printf("[resolve-solana] DB summary: total_pairs=%d, solana_pairs=%d", totalPairs, solanaPairs)

	// Show distinct dex names for Solana pairs so we know what's there
	dexRows, err := db.Query(`
		SELECT dex_name, COUNT(*) as cnt
		FROM pairs
		WHERE network = 'solana'
		GROUP BY dex_name
		ORDER BY cnt DESC
		LIMIT 20
	`)
	if err == nil {
		defer dexRows.Close()
		log.Println("[resolve-solana] Solana dex breakdown:")
		for dexRows.Next() {
			var dexName string
			var cnt int
			if err := dexRows.Scan(&dexName, &cnt); err == nil {
				log.Printf("[resolve-solana]   %-30s %d pairs", dexName, cnt)
			}
		}
	}
	// ─────────────────────────────────────────────────────────────────────────

	// Load the persistent cache (may already have entries from previous runs).
	cache := watcher.NewTokenOrderCache(cachePath)
	log.Printf("[resolve-solana] cache loaded: %d existing entries from %s",
		cache.Len(), cachePath)

	// runOnce performs a single resolve cycle.
	runOnce := func() {
		log.Println("[resolve-solana] loading Solana pairs from DB...")
		allPairs, err := watcher.LoadSolanaPairs(db)
		if err != nil {
			log.Printf("[resolve-solana] LoadSolanaPairs error: %v", err)
			return
		}
		log.Printf("[resolve-solana] loaded %d Solana pairs total", len(allPairs))

		// Split into already-cached and new (need RPC probe).
		cached, uncached := cache.ApplyToSolanaPairs(allPairs)
		log.Printf("[resolve-solana] %d already cached, %d need RPC probe",
			len(cached), len(uncached))

		if len(uncached) == 0 {
			log.Println("[resolve-solana] nothing new to resolve — cache is up to date")
			return
		}

		log.Printf("[resolve-solana] resolving token order for %d new pairs via RPC (%s)...",
			len(uncached), solanaRPC)

		resolved := watcher.ResolveAndCache(uncached, solanaRPC, cache)

		// Count how many came back with known order.
		knownCount := 0
		for _, p := range resolved {
			if p.Token0OrderKnown {
				knownCount++
			}
		}
		log.Printf("[resolve-solana] ✓ resolved %d/%d pairs (token order known)",
			knownCount, len(uncached))
		log.Printf("[resolve-solana] cache now has %d entries saved to %s",
			cache.Len(), cachePath)
	}

	// Run immediately.
	runOnce()

	if once {
		log.Println("[resolve-solana] RESOLVE_INTERVAL=once — exiting after single run")
		return
	}

	// Schedule subsequent runs.
	log.Printf("[resolve-solana] scheduling re-runs every %s (set RESOLVE_INTERVAL to change)",
		interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		log.Printf("[resolve-solana] scheduled run triggered (interval=%s)", interval)
		runOnce()
	}
}
