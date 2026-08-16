package watcher

import (
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	"on-chain-price-fetcher/internal/adapters/shared"
)

// PairReloader polls the DB for new pairs at a regular interval.
// When new pairs are found, it calls the provided reload callbacks
// so each watcher can restart with the updated pair list.
//
// Design: Rather than surgically injecting new subscriptions into a live
// WebSocket connection (which is complex and error-prone), we simply signal
// affected watchers to reconnect with the full updated pair list.
// The reconnect is nearly instant (<1s) so no events are missed in practice.
type PairReloader struct {
	db           *sql.DB
	interval     time.Duration
	stopCh       chan struct{}

	mu           sync.Mutex
	knownEVM     map[string]struct{} // known pair IDs for EVM
	knownSolana  map[string]struct{} // known pair IDs for Solana

	// Callbacks called when new pairs are discovered
	onNewEVMPairs    func(network string, newPairs []EVMPairMeta)
	onNewSolanaPairs func(newPairs []PairMeta)
}

// NewPairReloader creates a reloader that checks for new pairs every interval.
// onNewEVMPairs is called with (network, newPairs) when new EVM pools are found.
// onNewSolanaPairs is called with newPairs when new Solana pools are found.
func NewPairReloader(
	db *sql.DB,
	interval time.Duration,
	onNewEVMPairs func(network string, newPairs []EVMPairMeta),
	onNewSolanaPairs func(newPairs []PairMeta),
) *PairReloader {
	return &PairReloader{
		db:               db,
		interval:         interval,
		stopCh:           make(chan struct{}),
		knownEVM:         make(map[string]struct{}),
		knownSolana:      make(map[string]struct{}),
		onNewEVMPairs:    onNewEVMPairs,
		onNewSolanaPairs: onNewSolanaPairs,
	}
}

// SeedKnown pre-populates the known set so existing pairs don't trigger callbacks.
// Call this once with the pairs already loaded before calling Start().
func (r *PairReloader) SeedKnown(evmPairs []EVMPairMeta, solanaPairs []PairMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range evmPairs {
		r.knownEVM[p.ID] = struct{}{}
	}
	for _, p := range solanaPairs {
		r.knownSolana[p.ID] = struct{}{}
	}
}

// Start begins the background polling loop. Blocks until Stop() is called.
func (r *PairReloader) Start() {
	log.Printf("[pair-reloader] started — checking for new pairs every %s", r.interval)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			log.Printf("[pair-reloader] stopped")
			return
		case <-ticker.C:
			r.checkForNewPairs()
		}
	}
}

func (r *PairReloader) Stop() {
	close(r.stopCh)
}

func (r *PairReloader) checkForNewPairs() {
	// Check BSC
	r.checkEVM("bsc")
	// Check Base
	r.checkEVM("base")
	// Check Solana
	r.checkSolana()
}

func (r *PairReloader) checkEVM(network string) {
	allPairs, err := LoadEVMPairs(r.db, network)
	if err != nil {
		log.Printf("[pair-reloader] error loading %s pairs: %v", network, err)
		return
	}

	r.mu.Lock()
	var newPairs []EVMPairMeta
	for _, p := range allPairs {
		if _, known := r.knownEVM[p.ID]; !known {
			newPairs = append(newPairs, p)
			r.knownEVM[p.ID] = struct{}{}
		}
	}
	r.mu.Unlock()

	if len(newPairs) > 0 {
		log.Printf("[pair-reloader] found %d new %s pairs — notifying watchers", len(newPairs), network)
		if r.onNewEVMPairs != nil {
			r.onNewEVMPairs(network, newPairs)
		}
	}
}

func (r *PairReloader) checkSolana() {
	allPairs, err := LoadSolanaPairs(r.db)
	if err != nil {
		log.Printf("[pair-reloader] error loading Solana pairs: %v", err)
		return
	}

	r.mu.Lock()
	var newPairs []PairMeta
	for _, p := range allPairs {
		if _, known := r.knownSolana[p.ID]; !known {
			newPairs = append(newPairs, p)
			r.knownSolana[p.ID] = struct{}{}
		}
	}
	r.mu.Unlock()

	if len(newPairs) > 0 {
		log.Printf("[pair-reloader] found %d new Solana pairs — notifying watchers", len(newPairs))
		if r.onNewSolanaPairs != nil {
			r.onNewSolanaPairs(newPairs)
		}
	}
}

// ── Dynamic subscription support for EVMWatcher ──────────────────────────────

// AddPairs injects new pairs into a running EVM watcher WITHOUT reconnecting.
// It sends new pairs via a channel that the event loop reads and subscribes
// on the live connection immediately.
func (w *EVMWatcher) AddPairs(newPairs []EVMPairMeta) {
	// Deduplicate against already-known pairs
	w.mu.Lock()
	existing := make(map[string]struct{}, len(w.pairs))
	for _, p := range w.pairs {
		existing[strings.ToLower(p.PoolAddress)] = struct{}{}
	}
	var fresh []EVMPairMeta
	for _, p := range newPairs {
		if _, ok := existing[strings.ToLower(p.PoolAddress)]; !ok {
			w.pairs = append(w.pairs, p)
			fresh = append(fresh, p)
		}
	}
	w.mu.Unlock()

	if len(fresh) == 0 {
		return
	}
	log.Printf("[watcher/evm] queuing %d new pools for live subscription", len(fresh))
	// Non-blocking send — if the channel is full, fall back to reconnect on next cycle
	select {
	case w.newPairsCh <- fresh:
	default:
		log.Printf("[watcher/evm] newPairsCh full — new pools will be picked up on next reconnect")
	}
}

// ── Dynamic subscription support for SolanaWatcher ───────────────────────────

// AddPairs injects new pairs into a running Solana watcher WITHOUT reconnecting.
// Sends via channel — the event loop picks it up and sends accountSubscribe immediately.
func (w *SolanaWatcher) AddPairs(newPairs []PairMeta) {
	// Deduplicate
	w.mu.Lock()
	existing := make(map[string]struct{}, len(w.pairs))
	for _, p := range w.pairs {
		existing[p.PoolAddress] = struct{}{}
	}
	var fresh []PairMeta
	for _, p := range newPairs {
		if _, ok := existing[p.PoolAddress]; !ok {
			w.pairs = append(w.pairs, p)
			fresh = append(fresh, p)
		}
	}
	w.mu.Unlock()

	if len(fresh) == 0 {
		return
	}
	log.Printf("[watcher/solana] queuing %d new pools for live subscription", len(fresh))
	select {
	case w.newPairsCh <- fresh:
	default:
		log.Printf("[watcher/solana] newPairsCh full — new pools will be picked up on next reconnect")
	}
}

// ── Helper: build a shared.Pair from EVMPairMeta ─────────────────────────────

func EVMPairMetaToShared(p EVMPairMeta) shared.Pair {
	return shared.Pair{
		ID:                 p.ID,
		Network:            p.Network,
		DexName:            p.DexName,
		PoolAddress:        p.PoolAddress,
		BaseToken:          p.BaseToken,
		QuoteToken:         p.QuoteToken,
		BaseTokenDecimals:  p.BaseTokenDecimals,
		QuoteTokenDecimals: p.QuoteTokenDecimals,
		BaseSymbol:         p.BaseSymbol,
		QuoteSymbol:        p.QuoteSymbol,
	}
}
