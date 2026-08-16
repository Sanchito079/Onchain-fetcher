package pumpswap

import (
    "encoding/base64"
    "encoding/json"
    "fmt"
    "net/http"
    "net/http/httptest"
    "testing"

    "on-chain-price-fetcher/internal/adapters/shared"
)

func TestAdapterSupportsPumpSwap(t *testing.T) {
    adapter := Adapter{}
    pair := shared.Pair{Network: "solana", DexName: "PumpSwap"}
    if !adapter.Supports(pair) {
        t.Fatalf("expected PumpSwap adapter to support PumpSwap dex name")
    }
}

func TestAdapterSupportsSolanaPancakeSwap(t *testing.T) {
    adapter := Adapter{}
    pair := shared.Pair{Network: "solana", DexName: "pancakeswap-v3-solana"}
    if !adapter.Supports(pair) {
        t.Fatalf("expected PumpSwap adapter to support Solana PancakeSwap dex name")
    }
}

func TestFetchPriceReturnsValidPrice(t *testing.T) {
    reserveA := uint64(100_000_000_000) // 100 SOL with 9 decimals
    reserveB := uint64(50_000_000_000)  // 50,000 USDC with 6 decimals
    raw := make([]byte, 32)
    for i := 0; i < 8; i++ {
        raw[i] = byte(reserveA >> (8 * i))
        raw[8+i] = byte(reserveB >> (8 * i))
    }
    encoded := base64.StdEncoding.EncodeToString(raw)

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintf(w, `{"jsonrpc":"2.0","result":{"value":{"data":["%s","base64"]}}}` , encoded)
    }))
    defer server.Close()

    adapter := Adapter{RPC: RPCClient{Endpoint: server.URL}}
    pair := shared.Pair{
        ID:                 "test-pair",
        Network:            "solana",
        DexName:            "PumpSwap",
        PoolAddress:        "ExamplePoolPubkey",
        BaseTokenDecimals:  9,
        QuoteTokenDecimals: 6,
        BaseSymbol:         "SOL",
        QuoteSymbol:        "USDC",
    }

    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if !result.Valid {
        t.Fatalf("expected valid result, got invalid: %s", result.Reason)
    }
    if result.Price <= 0 {
        t.Fatalf("expected positive price, got %g", result.Price)
    }
    expected := 500.0
    if result.Price != expected {
        t.Fatalf("expected price %g, got %g", expected, result.Price)
    }
}

func TestFetchPriceUsesOwnerTokenAccountsFallback(t *testing.T) {
    poolData := make([]byte, 8)
    encodedPool := base64.StdEncoding.EncodeToString(poolData)

    type rpcRequest struct {
        Method string `json:"method"`
    }

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        var req rpcRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            t.Fatalf("failed to decode rpc request: %v", err)
        }

        switch req.Method {
        case "getAccountInfo":
            fmt.Fprintf(w, `{"jsonrpc":"2.0","result":{"value":{"data":["%s","base64"]}}}` , encodedPool)
        case "getTokenAccountsByOwner":
            tokenAccountA := make([]byte, 72)
            for i := 0; i < 32; i++ {
                tokenAccountA[i] = byte(i + 1)
            }
            tokenAccountA[64] = 2
            tokenAccountA[65] = 0
            tokenAccountA[66] = 0
            tokenAccountA[67] = 0
            tokenAccountA[68] = 0
            tokenAccountA[69] = 0
            tokenAccountA[70] = 0
            tokenAccountA[71] = 0

            tokenAccountB := make([]byte, 72)
            for i := 0; i < 32; i++ {
                tokenAccountB[i] = byte(i + 101)
            }
            tokenAccountB[64] = 4
            tokenAccountB[65] = 0
            tokenAccountB[66] = 0
            tokenAccountB[67] = 0
            tokenAccountB[68] = 0
            tokenAccountB[69] = 0
            tokenAccountB[70] = 0
            tokenAccountB[71] = 0

            encodedA := base64.StdEncoding.EncodeToString(tokenAccountA)
            encodedB := base64.StdEncoding.EncodeToString(tokenAccountB)
            fmt.Fprintf(w, `{"jsonrpc":"2.0","result":{"value":[{"pubkey":"accountA","account":{"data":["%s","base64"]}},{"pubkey":"accountB","account":{"data":["%s","base64"]}}]}}`, encodedA, encodedB)
        default:
            t.Fatalf("unexpected rpc method: %s", req.Method)
        }
    }))
    defer server.Close()

    adapter := Adapter{RPC: RPCClient{Endpoint: server.URL}}
    pair := shared.Pair{
        ID:                 "test-pair-owner-fallback",
        Network:            "solana",
        DexName:            "PumpSwap",
        PoolAddress:        "ExamplePoolPubkey",
        BaseTokenDecimals:  6,
        QuoteTokenDecimals: 9,
        BaseSymbol:         "BASE",
        QuoteSymbol:        "QUOTE",
    }

    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if !result.Valid {
        t.Fatalf("expected valid result, got invalid: %s", result.Reason)
    }
    expected := 0.002
    if result.Price != expected {
        t.Fatalf("expected price %g, got %g", expected, result.Price)
    }
}

func TestFetchPriceOrdersOwnerTokenAccountsByBaseQuoteMetadata(t *testing.T) {
    poolData := make([]byte, 8)
    encodedPool := base64.StdEncoding.EncodeToString(poolData)

    tokenAccountA := make([]byte, 72)
    for i := 0; i < 32; i++ {
        tokenAccountA[i] = byte(1)
    }
    tokenAccountA[64] = 10
    tokenAccountA[65] = 0
    tokenAccountA[66] = 0
    tokenAccountA[67] = 0
    tokenAccountA[68] = 0
    tokenAccountA[69] = 0
    tokenAccountA[70] = 0
    tokenAccountA[71] = 0

    tokenAccountB := make([]byte, 72)
    for i := 0; i < 32; i++ {
        tokenAccountB[i] = byte(2)
    }
    tokenAccountB[64] = 20
    tokenAccountB[65] = 0
    tokenAccountB[66] = 0
    tokenAccountB[67] = 0
    tokenAccountB[68] = 0
    tokenAccountB[69] = 0
    tokenAccountB[70] = 0
    tokenAccountB[71] = 0

    mintA := encodeBase58(tokenAccountA[0:32])
    mintB := encodeBase58(tokenAccountB[0:32])

    encodedA := base64.StdEncoding.EncodeToString(tokenAccountA)
    encodedB := base64.StdEncoding.EncodeToString(tokenAccountB)

    type rpcRequest struct {
        Method string `json:"method"`
    }

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        var req rpcRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            t.Fatalf("failed to decode rpc request: %v", err)
        }

        switch req.Method {
        case "getAccountInfo":
            fmt.Fprintf(w, `{"jsonrpc":"2.0","result":{"value":{"data":["%s","base64"]}}}` , encodedPool)
        case "getTokenAccountsByOwner":
            fmt.Fprintf(w, `{"jsonrpc":"2.0","result":{"value":[{"pubkey":"accountA","account":{"data":["%s","base64"]}},{"pubkey":"accountB","account":{"data":["%s","base64"]}}]}}`, encodedA, encodedB)
        default:
            t.Fatalf("unexpected rpc method: %s", req.Method)
        }
    }))
    defer server.Close()

    adapter := Adapter{RPC: RPCClient{Endpoint: server.URL}}
    pair := shared.Pair{
        ID:                 "test-pair-owner-order",
        Network:            "solana",
        DexName:            "PumpSwap",
        PoolAddress:        "ExamplePoolPubkey",
        BaseToken:          mintB,
        QuoteToken:         mintA,
        BaseTokenDecimals:  6,
        QuoteTokenDecimals: 6,
    }

    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if !result.Valid {
        t.Fatalf("expected valid result, got invalid: %s", result.Reason)
    }
    expected := 0.5
    if result.Price != expected {
        t.Fatalf("expected price %g, got %g", expected, result.Price)
    }
}

func TestFetchPricePrefersOwnerTokenAccountsOverRawParse(t *testing.T) {
    // Create raw pool bytes that parse successfully with a bogus reserve pair,
    // while owner-based token accounts contain the actual reserve balances.
    poolData := make([]byte, 128)
    reserveA := uint64(1_000_000)
    reserveB := uint64(5_500)
    for i := 0; i < 8; i++ {
        poolData[64+i] = byte(reserveA >> (8 * i))
        poolData[72+i] = byte(reserveB >> (8 * i))
    }
    encodedPool := base64.StdEncoding.EncodeToString(poolData)

    type rpcRequest struct {
        Method string `json:"method"`
    }

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        var req rpcRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            t.Fatalf("failed to decode rpc request: %v", err)
        }

        switch req.Method {
        case "getAccountInfo":
            fmt.Fprintf(w, `{"jsonrpc":"2.0","result":{"value":{"data":["%s","base64"]}}}` , encodedPool)
        case "getTokenAccountsByOwner":
            tokenAccountA := make([]byte, 72)
            tokenAccountA[64] = 2
            tokenAccountB := make([]byte, 72)
            tokenAccountB[64] = 4
            encodedA := base64.StdEncoding.EncodeToString(tokenAccountA)
            encodedB := base64.StdEncoding.EncodeToString(tokenAccountB)
            fmt.Fprintf(w, `{"jsonrpc":"2.0","result":{"value":[{"pubkey":"accountA","account":{"data":["%s","base64"]}},{"pubkey":"accountB","account":{"data":["%s","base64"]}}]}}`, encodedA, encodedB)
        default:
            t.Fatalf("unexpected rpc method: %s", req.Method)
        }
    }))
    defer server.Close()

    adapter := Adapter{RPC: RPCClient{Endpoint: server.URL}}
    pair := shared.Pair{
        ID:                 "test-pair-owner-prefer",
        Network:            "solana",
        DexName:            "PumpSwap",
        PoolAddress:        "ExamplePoolPubkey",
        BaseTokenDecimals:  6,
        QuoteTokenDecimals: 9,
        BaseSymbol:         "BASE",
        QuoteSymbol:        "QUOTE",
    }

    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if !result.Valid {
        t.Fatalf("expected valid result, got invalid: %s", result.Reason)
    }
    expected := 0.0000055
    if result.Price != expected {
        t.Fatalf("expected price %g, got %g", expected, result.Price)
    }
}
