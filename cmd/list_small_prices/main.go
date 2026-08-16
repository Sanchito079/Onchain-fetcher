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
)

type tokenMeta struct {
	Address  string `json:"address"`
	Decimals int    `json:"decimals"`
}

type row struct {
	ID                string
	Network           string
	DexName           string
	PoolAddress       string
	RawBaseToken      sql.NullString
	RawQuoteToken     sql.NullString
	BaseSymbol        sql.NullString
	QuoteSymbol       sql.NullString
	BaseTokenDecimals sql.NullInt64
	QuoteTokenDecimals sql.NullInt64
	GeckoPrice        sql.NullFloat64
	GeckoPriceUSD     sql.NullFloat64
}

func getEnvOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func parseTokenMetadata(raw string) (string, int, error) {
	if strings.TrimSpace(raw) == "" {
		return "", 0, nil
	}
	var t tokenMeta
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return "", 0, err
	}
	return strings.ToLower(strings.TrimSpace(t.Address)), t.Decimals, nil
}

func normalizeAddress(addr string) string {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return ""
	}
	if !strings.HasPrefix(trimmed, "0x") {
		trimmed = "0x" + trimmed
	}
	return strings.ToLower(trimmed)
}

func main() {
	var (
		threshold = flag.Float64("threshold", 1e-5, "maximum gecko_price to include")
		limit     = flag.Int("limit", 200, "maximum number of rows to return")
		network   = flag.String("network", "", "filter by network (bsc or base)")
		databaseURL = flag.String("database-url", "", "Postgres database URL")
	)
	flag.Parse()

	if strings.TrimSpace(*databaseURL) == "" {
		*databaseURL = getEnvOrDefault("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:55422/postgres?sslmode=disable")
	}

	db, err := sql.Open("postgres", *databaseURL)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	query := `
SELECT id, network, dex_name, pool_address, base_token, quote_token, base_symbol, quote_symbol,
       base_token_decimals, quote_token_decimals, gecko_price, gecko_price_usd
FROM pairs
WHERE gecko_price IS NOT NULL
  AND gecko_price > 0
  AND gecko_price <= $1
`
	args := []any{*threshold}
	if strings.TrimSpace(*network) != "" {
		query += " AND lower(network) = lower($2)"
		args = append(args, *network)
	}
	query += " ORDER BY gecko_price ASC LIMIT " + fmt.Sprint(*limit)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ID, &r.Network, &r.DexName, &r.PoolAddress, &r.RawBaseToken, &r.RawQuoteToken, &r.BaseSymbol, &r.QuoteSymbol, &r.BaseTokenDecimals, &r.QuoteTokenDecimals, &r.GeckoPrice, &r.GeckoPriceUSD); err != nil {
			log.Fatalf("scan failed: %v", err)
		}

		baseTokenAddr, baseTokenDecimals, _ := parseTokenMetadata(r.RawBaseToken.String)
		quoteTokenAddr, quoteTokenDecimals, _ := parseTokenMetadata(r.RawQuoteToken.String)
		if r.BaseTokenDecimals.Valid && r.BaseTokenDecimals.Int64 > 0 {
			baseTokenDecimals = int(r.BaseTokenDecimals.Int64)
		}
		if r.QuoteTokenDecimals.Valid && r.QuoteTokenDecimals.Int64 > 0 {
			quoteTokenDecimals = int(r.QuoteTokenDecimals.Int64)
		}

		baseSymbol := strings.TrimSpace(r.BaseSymbol.String)
		quoteSymbol := strings.TrimSpace(r.QuoteSymbol.String)
		if baseSymbol == "" {
			baseSymbol = baseTokenAddr
		}
		if quoteSymbol == "" {
			quoteSymbol = quoteTokenAddr
		}

		fmt.Printf("%d. id=%s network=%s dex=%s pool=%s\n", count+1, r.ID, r.Network, r.DexName, r.PoolAddress)
		fmt.Printf("   base=%s(%s dec=%d) quote=%s(%s dec=%d) gecko_price=%g gecko_price_usd=%g\n",
			baseSymbol, baseTokenAddr, baseTokenDecimals, quoteSymbol, quoteTokenAddr, quoteTokenDecimals,
			r.GeckoPrice.Float64, r.GeckoPriceUSD.Float64)
		fmt.Println()
		count++
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows error: %v", err)
	}
	if count == 0 {
		fmt.Println("no small-price pools found")
	}
}
