package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
	"on-chain-price-fetcher/internal/adapters/pancakeswap"
	"on-chain-price-fetcher/internal/adapters/pumpswap"
	"on-chain-price-fetcher/internal/adapters/shared"
	"on-chain-price-fetcher/internal/adapters/slipstream"
	"on-chain-price-fetcher/internal/adapters/uniswap"
)

type pairRow struct {
	ID                 string
	Network            string
	DexName            string
	PoolAddress        string
	RawBaseToken       sql.NullString
	RawQuoteToken      sql.NullString
	BaseTokenDecimals  int
	QuoteTokenDecimals int
	BaseSymbol         string
	QuoteSymbol        string
}

type tokenMetadata struct {
	Address  string `json:"address"`
	Decimals int    `json:"decimals"`
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

func parseTokenMetadata(raw string) (string, int, error) {
	if strings.TrimSpace(raw) == "" {
		return "", 0, nil
	}
	var payload tokenMetadata
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", 0, err
	}
	if payload.Address == "" && payload.Decimals == 0 {
		return "", 0, nil
	}
	return payload.Address, payload.Decimals, nil
}

func normalizeAddress(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if !strings.HasPrefix(trimmed, "0x") {
		trimmed = "0x" + trimmed
	}
	return strings.ToLower(trimmed)
}

func isEmptyAddress(address string) bool {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return true
	}
	trimmed = strings.TrimPrefix(trimmed, "0x")
	return len(trimmed) != 40
}

func main() {
	var (
		network          = flag.String("network", "base", "network for the pool: base or bsc")
		dexName          = flag.String("dex", "", "dex name or substring: uniswap, slipstream, pancakeswap")
		poolAddress      = flag.String("pool-address", "", "pool contract address or pool id for V4/V4-like")
		baseToken        = flag.String("base-token", "", "base token address or symbol")
		quoteToken       = flag.String("quote-token", "", "quote token address or symbol")
		baseDecimals     = flag.Int("base-decimals", 0, "base token decimals")
		quoteDecimals    = flag.Int("quote-decimals", 0, "quote token decimals")
		baseSymbol       = flag.String("base-symbol", "", "base token symbol")
		quoteSymbol      = flag.String("quote-symbol", "", "quote token symbol")
		databaseURL      = flag.String("database-url", "", "Postgres URL for pair lookup")
		queryDB          = flag.Bool("query-db", false, "lookup pool metadata from the local pairs table")
	)
	flag.Parse()

	if strings.TrimSpace(*poolAddress) == "" {
		log.Fatal("--pool-address is required")
	}
	if strings.TrimSpace(*dexName) == "" {
		log.Fatal("--dex is required")
	}

	if *queryDB {
		if strings.TrimSpace(*databaseURL) == "" {
			*databaseURL = getEnvOrDefault("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:55422/postgres?sslmode=disable")
		}
		row, err := queryPairRow(*databaseURL, *poolAddress)
		if err != nil {
			log.Fatalf("failed to query pool metadata: %v", err)
		}
		if row == nil {
			log.Fatalf("no pair found for pool address %s", *poolAddress)
		}
		if strings.TrimSpace(*baseToken) == "" {
			if addr, _, err := parseTokenMetadata(row.RawBaseToken.String); err == nil && addr != "" {
				*baseToken = addr
			} else {
				*baseToken = row.BaseSymbol
			}
		}
		if strings.TrimSpace(*quoteToken) == "" {
			if addr, _, err := parseTokenMetadata(row.RawQuoteToken.String); err == nil && addr != "" {
				*quoteToken = addr
			} else {
				*quoteToken = row.QuoteSymbol
			}
		}
		if *baseDecimals == 0 {
			if _, decimals, err := parseTokenMetadata(row.RawBaseToken.String); err == nil && decimals > 0 {
				*baseDecimals = decimals
			}
		}
		if *quoteDecimals == 0 {
			if _, decimals, err := parseTokenMetadata(row.RawQuoteToken.String); err == nil && decimals > 0 {
				*quoteDecimals = decimals
			}
		}
		if strings.TrimSpace(*baseSymbol) == "" {
			*baseSymbol = row.BaseSymbol
		}
		if strings.TrimSpace(*quoteSymbol) == "" {
			*quoteSymbol = row.QuoteSymbol
		}
		if strings.TrimSpace(*network) == "" {
			*network = row.Network
		}
	}

	pair := shared.Pair{
		ID:                 fmt.Sprintf("probe-%s", strings.ToLower(strings.TrimSpace(*poolAddress))),
		Network:            strings.ToLower(strings.TrimSpace(*network)),
		DexName:            strings.ToLower(strings.TrimSpace(*dexName)),
		PoolAddress:        normalizeAddress(*poolAddress),
		BaseToken:          strings.TrimSpace(*baseToken),
		QuoteToken:         strings.TrimSpace(*quoteToken),
		BaseTokenDecimals:  *baseDecimals,
		QuoteTokenDecimals: *quoteDecimals,
		BaseSymbol:         strings.TrimSpace(*baseSymbol),
		QuoteSymbol:        strings.TrimSpace(*quoteSymbol),
	}

	if pair.PoolAddress == "" {
		log.Fatal("pool-address cannot be empty after normalization")
	}

	adapter, err := buildAdapter(pair, pair.Network)
	if err != nil {
		log.Fatalf("adapter selection failed: %v", err)
	}

	result, err := adapter.FetchPrice(pair)
	if err != nil {
		log.Printf("FetchPrice error: %v", err)
	}

	fmt.Printf("Adapter: %T\n", adapter)
	fmt.Printf("Pair: %+v\n", pair)
	fmt.Printf("Valid: %v\n", result.Valid)
	fmt.Printf("Reason: %s\n", result.Reason)
	fmt.Printf("Price: %g\n", result.Price)
	fmt.Printf("PriceUSD: %g\n", result.PriceUSD)
	fmt.Printf("LiquidityUSD: %g\n", result.LiquidityUSD)
	fmt.Printf("DebugInfo:\n%s\n", result.DebugInfo)
}

func buildAdapter(pair shared.Pair, network string) (interface{ FetchPrice(shared.Pair) (shared.PriceResult, error) }, error) {
	name := strings.ToLower(strings.TrimSpace(pair.DexName))
	rpcEndpoint := getRPCEndpoint(network)
	if strings.Contains(name, "uniswap") {
		return uniswap.Adapter{RPC: uniswap.RPCClient{Endpoint: rpcEndpoint}}, nil
	}
	if strings.Contains(name, "slipstream") || strings.Contains(name, "aerodrome") {
		return slipstream.Adapter{RPC: slipstream.RPCClient{Endpoint: rpcEndpoint}}, nil
	}
	if strings.Contains(name, "pancake") {
		return pancakeswap.Adapter{RPC: pancakeswap.RPCClient{Endpoint: rpcEndpoint}}, nil
	}
	if strings.Contains(name, "pumpswap") || strings.Contains(name, "pump-fun") || strings.Contains(name, "pump") {
		return pumpswap.Adapter{RPC: pumpswap.RPCClient{Endpoint: rpcEndpoint}}, nil
	}
	return nil, fmt.Errorf("unknown adapter for dex %q", pair.DexName)
}

func queryPairRow(databaseURL, poolAddress string) (*pairRow, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return nil, err
	}

	row := db.QueryRow(`
		SELECT id, network, dex_name, pool_address, base_token, quote_token,
		       base_token_decimals, quote_token_decimals, base_symbol, quote_symbol
		FROM pairs
		WHERE lower(pool_address) = lower($1)
		LIMIT 1
	`, poolAddress)
	var result pairRow
	if err := row.Scan(&result.ID, &result.Network, &result.DexName, &result.PoolAddress, &result.RawBaseToken, &result.RawQuoteToken, &result.BaseTokenDecimals, &result.QuoteTokenDecimals, &result.BaseSymbol, &result.QuoteSymbol); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &result, nil
}
