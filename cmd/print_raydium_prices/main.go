package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"

    _ "github.com/lib/pq"
    "on-chain-price-fetcher/internal/adapters/raydium"
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

    poolAddress := strings.TrimSpace(os.Getenv("TARGET_POOL_ADDRESS"))
    query := `
        SELECT id, network, dex_name, pool_address, base_token, quote_token, base_token_decimals, quote_token_decimals, base_symbol, quote_symbol
        FROM pairs
        WHERE network = 'solana'
          AND (
            dex_name ILIKE '%raydium%'
            OR dex_name ILIKE '%ray%'
          )`
    args := []any{}
    if poolAddress != "" {
        query += " AND pool_address = $1"
        args = append(args, poolAddress)
    }
    query += "\nORDER BY id\n"

    rows, err := db.Query(query, args...)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    if poolAddress != "" {
        fmt.Printf("Target pool address: %s\n", poolAddress)
    }
    adapter := raydium.Adapter{RPC: raydium.RPCClient{Endpoint: rpcEndpoint}}
    count := 0

    baseDecimalsOverride := getIntEnv("BASE_DECIMALS", 0)
    quoteDecimalsOverride := getIntEnv("QUOTE_DECIMALS", 0)

    for rows.Next() {
        var (
            id, network, dexName, poolAddress, baseSymbol, quoteSymbol string
            baseTokenJSON, quoteTokenJSON sql.NullString
            baseDecimals, quoteDecimals int
        )
        if err := rows.Scan(&id, &network, &dexName, &poolAddress, &baseTokenJSON, &quoteTokenJSON, &baseDecimals, &quoteDecimals, &baseSymbol, &quoteSymbol); err != nil {
            log.Fatal(err)
        }

        baseToken, _, _ := parseTokenMetadata(baseTokenJSON.String)
        quoteToken, _, _ := parseTokenMetadata(quoteTokenJSON.String)

        if baseDecimalsOverride > 0 {
            baseDecimals = baseDecimalsOverride
        }
        if quoteDecimalsOverride > 0 {
            quoteDecimals = quoteDecimalsOverride
        }

        pair := shared.Pair{
            ID:                 id,
            Network:            network,
            DexName:            dexName,
            PoolAddress:        poolAddress,
            BaseToken:          baseToken,
            QuoteToken:         quoteToken,
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

    fmt.Printf("Printed %d Raydium pool entries.\n", count)
}

func getIntEnv(key string, fallback int) int {
    s := strings.TrimSpace(os.Getenv(key))
    if s == "" {
        return fallback
    }
    v, err := strconv.Atoi(s)
    if err != nil {
        return fallback
    }
    return v
}

type tokenMetadata struct {
    Address  string `json:"address"`
    Decimals int    `json:"decimals"`
}

func parseTokenMetadata(raw string) (string, int, error) {
    if strings.TrimSpace(raw) == "" {
        return "", 0, nil
    }
    var payload tokenMetadata
    if err := json.Unmarshal([]byte(raw), &payload); err != nil {
        return "", 0, err
    }
    if strings.TrimSpace(payload.Address) == "" {
        return "", 0, nil
    }
    return strings.TrimSpace(payload.Address), payload.Decimals, nil
}
