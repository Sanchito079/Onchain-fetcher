package watcher

import (
	"encoding/json"
	"log"
	"os"
	"sync"
)

// TokenOrderEntry holds the resolved token order for a single Solana pool.
type TokenOrderEntry struct {
	PoolAddress      string `json:"pool_address"`
	Token0IsBase     bool   `json:"token0_is_base"`
	Token0OrderKnown bool   `json:"token0_order_known"`
	BinStep          int64  `json:"bin_step,omitempty"`
}

// TokenOrderCache persists and serves token order resolutions so RPC probes
// only happen once per pool across restarts.
type TokenOrderCache struct {
	mu       sync.RWMutex
	entries  map[string]TokenOrderEntry // key: pool address
	filePath string
}

// NewTokenOrderCache loads the cache from disk (or starts empty if not found).
func NewTokenOrderCache(filePath string) *TokenOrderCache {
	c := &TokenOrderCache{
		entries:  make(map[string]TokenOrderEntry),
		filePath: filePath,
	}
	c.load()
	return c
}

// load reads the cache file from disk.
func (c *TokenOrderCache) load() {
	data, err := os.ReadFile(c.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[token-order-cache] load error (starting empty): %v", err)
		}
		return
	}
	var entries []TokenOrderEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Printf("[token-order-cache] parse error (starting empty): %v", err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range entries {
		c.entries[e.PoolAddress] = e
	}
	log.Printf("[token-order-cache] loaded %d entries from %s", len(c.entries), c.filePath)
}

// Save writes the current cache to disk atomically.
func (c *TokenOrderCache) Save() {
	c.mu.RLock()
	entries := make([]TokenOrderEntry, 0, len(c.entries))
	for _, e := range c.entries {
		entries = append(entries, e)
	}
	c.mu.RUnlock()

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		log.Printf("[token-order-cache] marshal error: %v", err)
		return
	}
	if err := os.WriteFile(c.filePath, data, 0644); err != nil {
		log.Printf("[token-order-cache] write error: %v", err)
		return
	}
	log.Printf("[token-order-cache] saved %d entries to %s", len(entries), c.filePath)
}

// Get returns the cached entry for a pool address, if it exists.
func (c *TokenOrderCache) Get(poolAddress string) (TokenOrderEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[poolAddress]
	return e, ok
}

// Set stores an entry in the cache (does not auto-save to disk).
func (c *TokenOrderCache) Set(e TokenOrderEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[e.PoolAddress] = e
}

// Len returns the number of cached entries.
func (c *TokenOrderCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// ApplyToSolanaPairs applies cached token order resolutions to a slice of PairMeta,
// and returns two slices: resolved pairs (from cache) and unresolved pairs (need RPC probe).
func (c *TokenOrderCache) ApplyToSolanaPairs(pairs []PairMeta) (resolved []PairMeta, unresolved []PairMeta) {
	for _, p := range pairs {
		if entry, ok := c.Get(p.PoolAddress); ok && entry.Token0OrderKnown {
			p.Token0IsBase = entry.Token0IsBase
			p.Token0OrderKnown = entry.Token0OrderKnown
			if entry.BinStep > 0 {
				p.BinStep = entry.BinStep
			}
			resolved = append(resolved, p)
		} else {
			unresolved = append(unresolved, p)
		}
	}
	return
}

// ResolveAndCache resolves token order for unresolved pairs via RPC, updates the
// cache in memory, saves to disk, and returns the fully populated pairs slice.
// This is safe to call at startup and for newly indexed pools.
func ResolveAndCache(pairs []PairMeta, rpcEndpoint string, cache *TokenOrderCache) []PairMeta {
	if len(pairs) == 0 {
		return pairs
	}
	log.Printf("[token-order-cache] resolving token order for %d pairs via RPC...", len(pairs))

	resolved := ResolveTokenOrder(pairs, rpcEndpoint)

	// Persist newly resolved entries to cache
	added := 0
	for _, p := range resolved {
		if p.Token0OrderKnown {
			cache.Set(TokenOrderEntry{
				PoolAddress:      p.PoolAddress,
				Token0IsBase:     p.Token0IsBase,
				Token0OrderKnown: p.Token0OrderKnown,
				BinStep:          p.BinStep,
			})
			added++
		}
	}
	if added > 0 {
		cache.Save()
		log.Printf("[token-order-cache] cached %d newly resolved token orders", added)
	}
	return resolved
}
