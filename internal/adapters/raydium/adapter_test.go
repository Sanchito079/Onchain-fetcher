package raydium

import (
    "encoding/base64"
    "encoding/binary"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"

    "on-chain-price-fetcher/internal/adapters/shared"
)

func TestFetchPriceUsesOnChainMintDecimals(t *testing.T) {
    poolBytes := make([]byte, 16)
    binary.LittleEndian.PutUint64(poolBytes[0:8], 100)
    binary.LittleEndian.PutUint64(poolBytes[8:16], 50)

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            Method string `json:"method"`
            Params []any  `json:"params"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            return
        }

        switch req.Method {
        case "getAccountInfo":
            account := ""
            if len(req.Params) > 0 {
                if s, ok := req.Params[0].(string); ok {
                    account = s
                }
            }
            switch account {
            case "pool-address":
                payload := base64.StdEncoding.EncodeToString(poolBytes)
                _ = json.NewEncoder(w).Encode(map[string]any{
                    "result": map[string]any{
                        "value": map[string]any{"data": []any{payload}},
                    },
                })
            case "base-mint":
                _ = json.NewEncoder(w).Encode(map[string]any{
                    "result": map[string]any{
                        "value": map[string]any{
                            "data": map[string]any{
                                "parsed": map[string]any{
                                    "info": map[string]any{"decimals": 9},
                                },
                            },
                        },
                    },
                })
            case "quote-mint":
                _ = json.NewEncoder(w).Encode(map[string]any{
                    "result": map[string]any{
                        "value": map[string]any{
                            "data": map[string]any{
                                "parsed": map[string]any{
                                    "info": map[string]any{"decimals": 6},
                                },
                            },
                        },
                    },
                })
            default:
                http.Error(w, "unknown account", http.StatusBadRequest)
            }
        default:
            http.Error(w, "unexpected method", http.StatusBadRequest)
        }
    }))
    defer server.Close()

    adapter := Adapter{RPC: RPCClient{Endpoint: server.URL}}
    pair := shared.Pair{
        ID:                 "pair-1",
        Network:            "solana",
        DexName:            "raydium",
        PoolAddress:        "pool-address",
        BaseToken:          "base-mint",
        QuoteToken:         "quote-mint",
        BaseTokenDecimals:  0,
        QuoteTokenDecimals: 0,
    }

    result, err := adapter.FetchPrice(pair)
    if err != nil {
        t.Fatalf("FetchPrice returned error: %v", err)
    }
    if !result.Valid {
        t.Fatalf("expected a valid price, got invalid reason=%q", result.Reason)
    }
    if result.Price < 499.999999 && result.Price > 500.000001 {
        t.Fatalf("expected price around 500, got %v", result.Price)
    }
}

func TestSupportsRaydiumClmm(t *testing.T) {
    adapter := Adapter{}
    pair := shared.Pair{Network: "solana", DexName: "raydium-clmm"}
    if !adapter.Supports(pair) {
        t.Fatalf("expected raydium-clmm to be supported by Raydium adapter")
    }
}
