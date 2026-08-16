package main

import (
    "bytes"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "net/http"
    "strings"
    "time"
)

func main() {
    endpoint := "https://bsc-dataseed.nariox.org/"
    pools := []string{
        "0x529433bb50c9c6907055be3f76788421ab8a3be4",
        "0x7231d17fa843e5516bf1d7d6f6c87147d279c2ba",
        "0x83fcd80d7973cca1aa821590bbec66d27a2d4ad4",
    }

    for _, pool := range pools {
        fmt.Println("===", pool, "===")
        payload := map[string]any{
            "jsonrpc": "2.0",
            "method":  "eth_call",
            "params": []any{
                map[string]string{"to": pool, "data": "0x3850c7bd"},
                "latest",
            },
            "id": 1,
        }
        data, _ := json.Marshal(payload)
        req, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(data))
        req.Header.Set("Content-Type", "application/json")
        client := &http.Client{Timeout: 15 * time.Second}
        resp, err := client.Do(req)
        if err != nil {
            fmt.Println("request err:", err)
            continue
        }
        defer resp.Body.Close()
        var out struct {
            Result string `json:"result"`
            Error  any    `json:"error"`
        }
        _ = json.NewDecoder(resp.Body).Decode(&out)
        fmt.Println("status:", resp.StatusCode)
        fmt.Println("result:", out.Result)
        if out.Error != nil {
            fmt.Printf("error: %#v\n", out.Error)
        }
        if out.Result != "" {
            trimmed := strings.TrimPrefix(out.Result, "0x")
            if len(trimmed) >= 64 {
                decoded, _ := hex.DecodeString(trimmed[0:64])
                fmt.Printf("decoded bytes: %x\n", decoded)
            }
        }
        fmt.Println()
    }
}
