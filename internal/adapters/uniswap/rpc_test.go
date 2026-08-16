package uniswap

import (
    "encoding/hex"
    "fmt"
    "io"
    "math/big"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
)

func TestRPCGetSlot0UsesUniswapV3Selector(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        body, err := io.ReadAll(r.Body)
        if err != nil {
            t.Fatalf("failed to read request body: %v", err)
        }
        if !strings.Contains(string(body), `"data":"0x3850c7bd"`) {
            t.Fatalf("unexpected selector payload: %s", string(body))
        }
        w.Write([]byte(`{"result":"0x000000000000000000000000000000000000000000000000000000000000000000"}`))
    }))
    defer server.Close()

    client := RPCClient{Endpoint: server.URL, Client: server.Client()}
    _, err := client.getSlot0("0xabc")
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
}

func TestRPCGetReservesUsesV2SelectorAndParsesValues(t *testing.T) {
    reserve0Bytes := make([]byte, 32)
    reserve0Bytes[31] = 1
    reserve1Bytes := make([]byte, 32)
    reserve1Bytes[31] = 1
    payload := "0x" + hex.EncodeToString(append(reserve0Bytes, reserve1Bytes...))

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        body, err := io.ReadAll(r.Body)
        if err != nil {
            t.Fatalf("failed to read request body: %v", err)
        }
        if !strings.Contains(string(body), `"data":"0x0902f1ac"`) {
            t.Fatalf("unexpected selector payload: %s", string(body))
        }
        w.Write([]byte(fmt.Sprintf(`{"result":"%s"}`, payload)))
    }))
    defer server.Close()

    client := RPCClient{Endpoint: server.URL, Client: server.Client()}
    reserve0, reserve1, err := client.getReserves("0xabc")
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
    if reserve0 == nil || reserve1 == nil {
        t.Fatal("expected reserves to be parsed")
    }
    if reserve0.Cmp(big.NewInt(1)) != 0 || reserve1.Cmp(big.NewInt(1)) != 0 {
        t.Fatalf("unexpected reserves: %s, %s", reserve0.String(), reserve1.String())
    }
}

func TestRPCGetV4Slot0UsesBaseStateView(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        body, err := io.ReadAll(r.Body)
        if err != nil {
            t.Fatalf("failed to read request body: %v", err)
        }
        if !strings.Contains(string(body), `"to":"0xa3c0c9b65bad0b08107aa264b0f3db444b867a71"`) {
            t.Fatalf("unexpected target address: %s", string(body))
        }
        w.Write([]byte(`{"result":"0x000000000000000000000000000000000000000000000000000000000000000000"}`))
    }))
    defer server.Close()

    client := RPCClient{Endpoint: server.URL, Client: server.Client()}
    _, err := client.getV4Slot0("base", "0xabc")
    if err != nil {
        t.Fatalf("expected no error, got %v", err)
    }
}
