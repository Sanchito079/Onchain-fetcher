package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
	"on-chain-price-fetcher/internal/adapters/pumpswap"
	"on-chain-price-fetcher/internal/adapters/shared"
)

func main() {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:55422/postgres?sslmode=disable"
	}

	rpcEndpoint := strings.TrimSpace(os.Getenv("RPC_ENDPOINT_SOLANA"))
	if rpcEndpoint == "" {
		rpcEndpoint = "https://api.mainnet-beta.solana.com/"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(`
		SELECT id, network, dex_name, pool_address, base_token_decimals, quote_token_decimals, base_symbol, quote_symbol
		FROM pairs
		WHERE network = 'solana'
		  AND (
		    dex_name ILIKE '%pumpswap%'
		    OR dex_name ILIKE '%pump-fun%'
		    OR dex_name ILIKE '%pump%'
		  )
		ORDER BY id
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	adapter := pumpswap.Adapter{RPC: pumpswap.RPCClient{Endpoint: rpcEndpoint}}
	count := 0

	for rows.Next() {
		var (
			id, network, dexName, poolAddress, baseSymbol, quoteSymbol string
			baseDecimals, quoteDecimals int
		)
		if err := rows.Scan(&id, &network, &dexName, &poolAddress, &baseDecimals, &quoteDecimals, &baseSymbol, &quoteSymbol); err != nil {
			log.Fatal(err)
		}

		pair := shared.Pair{
			ID:                 id,
			Network:            network,
			DexName:            dexName,
			PoolAddress:        poolAddress,
			BaseTokenDecimals:  baseDecimals,
			QuoteTokenDecimals: quoteDecimals,
			BaseSymbol:         baseSymbol,
			QuoteSymbol:        quoteSymbol,
		}

		result, err := adapter.FetchPrice(pair)
		if err != nil {
			fmt.Printf("[%d] %s | %s | %s | ERROR: %v\n", count+1, pair.ID, pair.DexName, pair.PoolAddress, err)
			count++
			continue
		}

		fmt.Printf("[%d] %s | %s | %s | price=%.8f | price_usd=%.8f | valid=%t | reason=%s\n",
			count+1,
			pair.ID,
			pair.DexName,
			pair.PoolAddress,
			result.Price,
			result.PriceUSD,
			result.Valid,
			result.Reason,
		)
		count++
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Printed %d PumpSwap pool entries.\n", count)
}
