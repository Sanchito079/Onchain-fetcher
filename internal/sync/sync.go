package sync

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"on-chain-price-fetcher/internal/adapters/meteora"
	"on-chain-price-fetcher/internal/adapters/orca"
	"on-chain-price-fetcher/internal/adapters/pancakeswap"
	"on-chain-price-fetcher/internal/adapters/pumpswap"
	"on-chain-price-fetcher/internal/adapters/raydium"
	"on-chain-price-fetcher/internal/adapters/shared"
	"on-chain-price-fetcher/internal/adapters/slipstream"
	"on-chain-price-fetcher/internal/adapters/uniswap"
	"on-chain-price-fetcher/internal/stats"
)

// PairRow is the minimal shape used to read from local Supabase.
type PairRow struct {
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

type SyncFailure struct {
	Type        string `json:"type"`
	Chain       string `json:"chain"`
	PairID      string `json:"pair_id"`
	DexName     string `json:"dex_name"`
	PoolAddress string `json:"pool_address"`
	Reason      string `json:"reason"`
	Error       string `json:"error,omitempty"`
}

type Syncer struct {
	DB *sql.DB
}

func NewSyncer(db *sql.DB) *Syncer {
	return &Syncer{DB: db}
}

func getEnvOrDefault(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

func getRPCEndpoint(network string) string {
	net := strings.ToLower(strings.TrimSpace(network))
	switch net {
	case "base":
		return getEnvOrDefault("RPC_ENDPOINT_BASE", "https://palpable-divine-shape.base-mainnet.quiknode.pro/6bbce19b5765801546265b33f2d1fdb2aafa9cc8/")
	case "solana":
		return getEnvOrDefault("RPC_ENDPOINT_SOLANA", "https://api.mainnet-beta.solana.com/")
	default:
		return getEnvOrDefault("RPC_ENDPOINT_BSC", "https://smart-sly-voice.bsc.quiknode.pro/68e23973e7772747604cc40a754b8349c20db22c/")
	}
}

func buildSyncQueryAndArgs() (string, []any) {
	filterEnv := strings.TrimSpace(os.Getenv("SYNC_DEX_FILTER"))
	networkFilterEnv := strings.ToLower(strings.TrimSpace(os.Getenv("SYNC_NETWORK_FILTER")))
	var networkClauses []string
	switch networkFilterEnv {
	case "solana":
		networkClauses = []string{"'solana'"}
	case "base":
		networkClauses = []string{"'base'"}
	case "bsc":
		networkClauses = []string{"'bsc'"}
	case "", "all":
		networkClauses = []string{"'bsc'", "'base'", "'solana'"}
	default:
		networkClauses = []string{"'bsc'", "'base'", "'solana'"}
	}

	if filterEnv == "" {
		return fmt.Sprintf(`
		SELECT id, network, dex_name, pool_address, base_token, quote_token,
		       base_token_decimals, quote_token_decimals, base_symbol, quote_symbol
		FROM pairs
		WHERE network IN (%s)
		  AND (
		    dex_name ILIKE '%%pancake%%'
		    OR dex_name ILIKE '%%uniswap%%'
		    OR dex_name ILIKE '%%slipstream%%'
		    OR dex_name ILIKE '%%aerodrome%%'
		    OR dex_name ILIKE '%%pumpswap%%'
		    OR dex_name ILIKE '%%pump-fun%%'
		    OR dex_name ILIKE '%%meteora%%'
		    OR dex_name ILIKE '%%dlmm%%'
		    OR dex_name ILIKE '%%raydium%%'
		    OR dex_name ILIKE '%%orca%%'
		    OR dex_name ILIKE '%%cpmm%%'
		  )
		ORDER BY id
		`, strings.Join(networkClauses, ", ")), nil
	}

	patterns := strings.Split(filterEnv, ",")
	var clauses []string
	var args []any
	for idx, rawPattern := range patterns {
		pattern := strings.TrimSpace(rawPattern)
		if pattern == "" {
			continue
		}
		clauses = append(clauses, fmt.Sprintf("dex_name ILIKE $%d", idx+1))
		args = append(args, "%"+pattern+"%")
	}
	if len(clauses) == 0 {
		return buildSyncQueryAndArgs()
	}

	return fmt.Sprintf(`
		SELECT id, network, dex_name, pool_address, base_token, quote_token,
		       base_token_decimals, quote_token_decimals, base_symbol, quote_symbol
		FROM pairs
		WHERE network IN (%s)
		  AND (
		    %s
		  )
		ORDER BY id
		`, strings.Join(networkClauses, ", "), strings.Join(clauses, "\n\t\t    OR ")), args
}

func isExtremePriceValue(price float64) bool {
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return true
	}
	// Allow any positive finite price regardless of how many decimal zeros it has.
	// Only block clearly nonsensical values above 1 quadrillion (1e15).
	return price > 1e15
}

func (s *Syncer) SyncOnce() error {
	query, args := buildSyncQueryAndArgs()
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	bscAdapter := pancakeswap.Adapter{RPC: pancakeswap.RPCClient{Endpoint: getRPCEndpoint("bsc")}}
	baseAdapter := pancakeswap.Adapter{RPC: pancakeswap.RPCClient{Endpoint: getRPCEndpoint("base")}}
	uniswapBsc := uniswap.Adapter{RPC: uniswap.RPCClient{Endpoint: getRPCEndpoint("bsc")}}
	uniswapBase := uniswap.Adapter{RPC: uniswap.RPCClient{Endpoint: getRPCEndpoint("base")} }
	slipstreamBase := slipstream.Adapter{RPC: slipstream.RPCClient{Endpoint: getRPCEndpoint("base")}}
	solanaAdapter := pumpswap.Adapter{RPC: pumpswap.RPCClient{Endpoint: getRPCEndpoint("solana")}}
	meteoraAdapter := meteora.Adapter{RPC: &meteora.RPCClient{Endpoint: getRPCEndpoint("solana")}}
	raydiumAdapter := raydium.Adapter{RPC: raydium.RPCClient{Endpoint: getRPCEndpoint("solana")}}
	orcaAdapter := orca.Adapter{RPC: orca.RPCClient{Endpoint: getRPCEndpoint("solana")}}
	var totalCount, updatedCount, fetchFailedCount, invalidCount, extremeCount int
	var bscUpdatedCount, baseUpdatedCount, solanaUpdatedCount int
	var fetchFailed []SyncFailure
	var invalidPools []SyncFailure
	for rows.Next() {
		totalCount++
		var row PairRow
		var baseTokenJSON, quoteTokenJSON sql.NullString
		if err := rows.Scan(&row.ID, &row.Network, &row.DexName, &row.PoolAddress, &baseTokenJSON, &quoteTokenJSON, &row.BaseTokenDecimals, &row.QuoteTokenDecimals, &row.BaseSymbol, &row.QuoteSymbol); err != nil {
			return err
		}
		baseAddress, baseDecimals, _ := parseTokenMetadata(baseTokenJSON.String)
		quoteAddress, quoteDecimals, _ := parseTokenMetadata(quoteTokenJSON.String)
		if baseAddress != "" {
			row.BaseToken = baseAddress
		}
		if quoteAddress != "" {
			row.QuoteToken = quoteAddress
		}
		// Prefer on-chain decimals from the JSON metadata when available.
		// The DB column defaults to 18, which is correct for EVM tokens but
		// wrong for Solana SPL tokens (typically 6 or 9). If the metadata
		// has a non-zero decimal value, always use it — it was read from the
		// on-chain SPL mint account and is the authoritative source.
		if baseDecimals > 0 {
			row.BaseTokenDecimals = baseDecimals
		}
		if quoteDecimals > 0 {
			row.QuoteTokenDecimals = quoteDecimals
		}
		// Final guard: if still 0 after both sources, keep the DB column value
		// (may be 0 for missing data — adapters will handle it gracefully).

		pair := shared.Pair{
			ID:                 row.ID,
			Network:            row.Network,
			DexName:            row.DexName,
			PoolAddress:        row.PoolAddress,
			BaseToken:          row.BaseToken,
			QuoteToken:         row.QuoteToken,
			BaseTokenDecimals:  row.BaseTokenDecimals,
			QuoteTokenDecimals: row.QuoteTokenDecimals,
			BaseSymbol:         row.BaseSymbol,
			QuoteSymbol:        row.QuoteSymbol,
		}
		// Choose adapter based on dex name and network
		dexLower := strings.ToLower(strings.TrimSpace(row.DexName))
		var result shared.PriceResult
		switch {
		case strings.Contains(dexLower, "uniswap"):
			adapter := uniswapBsc
			if strings.EqualFold(strings.TrimSpace(row.Network), "base") {
				adapter = uniswapBase
			}
			result, err = adapter.FetchPrice(pair)
		case strings.Contains(dexLower, "slipstream") || strings.Contains(dexLower, "aerodrome"):
			result, err = slipstreamBase.FetchPrice(pair)
		case strings.Contains(dexLower, "pancake") && strings.EqualFold(strings.TrimSpace(row.Network), "solana"):
			result, err = solanaAdapter.FetchPrice(pair)
		case strings.Contains(dexLower, "meteora") || strings.Contains(dexLower, "dlmm") || strings.Contains(dexLower, "damm"):
			if strings.EqualFold(strings.TrimSpace(row.Network), "solana") {
				result, err = meteoraAdapter.FetchPrice(pair)
			} else {
				result = shared.PriceResult{Valid: false, Reason: "unsupported network for Meteora", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "meteora", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "unsupported network for Meteora")}
			}
		case strings.Contains(dexLower, "pumpswap") || strings.Contains(dexLower, "pump-fun") || strings.Contains(dexLower, "pump"):
			if strings.EqualFold(strings.TrimSpace(row.Network), "solana") {
				result, err = solanaAdapter.FetchPrice(pair)
			} else {
				result = shared.PriceResult{Valid: false, Reason: "unsupported network for PumpSwap", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "pumpswap", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "unsupported network for PumpSwap")}
			}
		case strings.Contains(dexLower, "raydium"):
			if strings.EqualFold(strings.TrimSpace(row.Network), "solana") {
				result, err = raydiumAdapter.FetchPrice(pair)
			} else {
				result = shared.PriceResult{Valid: false, Reason: "unsupported network for Raydium", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "raydium", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "unsupported network for Raydium")}
			}
		case strings.Contains(dexLower, "orca"):
			if strings.EqualFold(strings.TrimSpace(row.Network), "solana") {
				result, err = orcaAdapter.FetchPrice(pair)
			} else {
				result = shared.PriceResult{Valid: false, Reason: "unsupported network for Orca", PairID: pair.ID}
			}
		default:
			if strings.EqualFold(strings.TrimSpace(row.Network), "solana") {
				result = shared.PriceResult{Valid: false, Reason: "unsupported Solana dex", PairID: pair.ID, DebugInfo: shared.BuildPriceDebugInfo(pair, "unsupported", pair.BaseTokenDecimals, pair.QuoteTokenDecimals, 0, 0, 0, nil, "unsupported Solana dex")}
			} else {
				adapter := bscAdapter
				if strings.EqualFold(strings.TrimSpace(row.Network), "base") {
					adapter = baseAdapter
				}
				result, err = adapter.FetchPrice(pair)
			}
		}
		if err != nil {
			fetchFailedCount++
			fetchFailed = append(fetchFailed, SyncFailure{
				Type:        "fetch_failed",
				Chain:       strings.ToLower(strings.TrimSpace(pair.Network)),
				PairID:      pair.ID,
				DexName:     pair.DexName,
				PoolAddress: pair.PoolAddress,
				Error:       err.Error(),
			})
			log.Printf("fetch failed for %s: %v", pair.ID, err)
			continue
		}
		if !result.Valid {
			invalidCount++
			invalidPools = append(invalidPools, SyncFailure{
				Type:        "invalid_price",
				Chain:       strings.ToLower(strings.TrimSpace(pair.Network)),
				PairID:      pair.ID,
				DexName:     pair.DexName,
				PoolAddress: pair.PoolAddress,
				Reason:      result.Reason,
			})
			logMessage := fmt.Sprintf("invalid price for %s: %s", pair.ID, result.Reason)
			if result.DebugInfo != "" {
				logMessage = fmt.Sprintf("%s\n%s", logMessage, result.DebugInfo)
			}
			log.Print(logMessage)
			continue
		}
		if isExtremePriceValue(result.Price) || isExtremePriceValue(result.PriceUSD) {
			extremeCount++
			logMessage := fmt.Sprintf("skipping extreme price for %s: %.6f / %.6f", pair.ID, result.Price, result.PriceUSD)
			if result.DebugInfo != "" {
				logMessage = fmt.Sprintf("%s\n%s", logMessage, result.DebugInfo)
			}
			log.Print(logMessage)
			continue
		}
		if err := stats.UpdatePriceWithStats(s.DB, pair.ID, result.Price, result.PriceUSD); err != nil {
			return err
		}
		updatedCount++
		switch strings.ToLower(strings.TrimSpace(pair.Network)) {
		case "bsc":
			bscUpdatedCount++
		case "base":
			baseUpdatedCount++
		case "solana":
			solanaUpdatedCount++
		}
		log.Printf("updated %s -> %g", pair.ID, result.Price)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	log.Printf("sync summary: total=%d, updated=%d, fetch_failed=%d, invalid=%d, extreme_skipped=%d", totalCount, updatedCount, fetchFailedCount, invalidCount, extremeCount)
	log.Printf("sync breakdown: bsc_updated=%d, base_updated=%d, solana_updated=%d", bscUpdatedCount, baseUpdatedCount, solanaUpdatedCount)
	if err := writeSyncFailureRundown(fetchFailed, invalidPools); err != nil {
		log.Printf("failed to write sync failure rundown: %v", err)
	}
	return nil
}

func writeSyncFailureRundown(fetchFailed, invalidPools []SyncFailure) error {
	timestamp := time.Now().UTC().Format("20060102T150405")
	jsonFile := fmt.Sprintf("sync_failures_%s.json", timestamp)
	csvFile := fmt.Sprintf("sync_failures_%s.csv", timestamp)
	data := struct {
		FetchFailed []SyncFailure `json:"fetch_failed"`
		Invalid     []SyncFailure `json:"invalid_price"`
	}{
		FetchFailed: fetchFailed,
		Invalid:     invalidPools,
	}

	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(jsonFile, jsonBytes, 0o644); err != nil {
		return err
	}

	f, err := os.Create(csvFile)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	if err := writer.Write([]string{"type", "chain", "pair_id", "dex_name", "pool_address", "reason", "error"}); err != nil {
		return err
	}

	for _, row := range append(fetchFailed, invalidPools...) {
		if err := writer.Write([]string{row.Type, row.Chain, row.PairID, row.DexName, row.PoolAddress, row.Reason, row.Error}); err != nil {
			return err
		}
	}
	if err := writer.Error(); err != nil {
		return err
	}

	log.Printf("wrote failure rundown to %s and %s", jsonFile, csvFile)
	return nil
}

func parseTokenMetadata(raw string) (string, int, error) {
	if strings.TrimSpace(raw) == "" {
		return "", 0, nil
	}
	var payload struct {
		Address string `json:"address"`
		Decimals int `json:"decimals"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", 0, err
	}
	if payload.Address == "" && payload.Decimals == 0 {
		return "", 0, nil
	}
	return payload.Address, payload.Decimals, nil
}

func main() {
	fmt.Println("sync package placeholder")
}
