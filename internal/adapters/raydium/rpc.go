package raydium

import (
    "encoding/base64"
    "encoding/json"
    "fmt"
    "net/http"
    "strings"
    "sync"
    "time"
)

type RPCClient struct {
    Endpoint string
    Client   *http.Client

    mu            sync.RWMutex
    accountCache  map[string][]byte
}

func (c *RPCClient) call(method string, params []any, result any) error {
    // Create or reuse an HTTP client with a modest timeout. Retry on
    // transient failures (HTTP 429 / 5xx / network timeouts) to reduce
    // spurious invalid-price results caused by RPC rate-limits.
    if c.Client == nil {
        c.Client = &http.Client{Timeout: 6 * time.Second}
    }
    payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s","params":%s}` , method, mustJSON(params))

    var lastErr error
    maxAttempts := 3
    for attempt := 1; attempt <= maxAttempts; attempt++ {
        req, err := http.NewRequest(http.MethodPost, c.Endpoint, strings.NewReader(payload))
        if err != nil {
            return err
        }
        req.Header.Set("Content-Type", "application/json")
        resp, err := c.Client.Do(req)
        if err != nil {
            lastErr = err
            // transient network error: back off and retry
            if attempt < maxAttempts {
                time.Sleep(time.Duration(attempt*150) * time.Millisecond)
                continue
            }
            return err
        }
        defer resp.Body.Close()
        if resp.StatusCode == 429 || resp.StatusCode >= 500 {
            lastErr = fmt.Errorf("rpc status %d", resp.StatusCode)
            if attempt < maxAttempts {
                time.Sleep(time.Duration(attempt*150) * time.Millisecond)
                continue
            }
            return lastErr
        }
        if resp.StatusCode >= 400 {
            return fmt.Errorf("rpc status %d", resp.StatusCode)
        }
        if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
            lastErr = err
            if attempt < maxAttempts {
                time.Sleep(time.Duration(attempt*150) * time.Millisecond)
                continue
            }
            return err
        }
        return nil
    }
    if lastErr != nil {
        return lastErr
    }
    return fmt.Errorf("rpc call failed")
}

func (c *RPCClient) getAccountInfo(account string) ([]byte, error) {
    if strings.TrimSpace(account) == "" {
        return nil, fmt.Errorf("empty account")
    }
    if data, ok := c.getCachedAccountInfo(account); ok {
        return data, nil
    }
    var response struct {
        Result struct {
            Value struct {
                Data any `json:"data"`
            } `json:"value"`
        } `json:"result"`
        Error any `json:"error"`
    }
    if err := c.call("getAccountInfo", []any{account, map[string]any{"encoding": "base64"}}, &response); err != nil {
        return nil, err
    }
    if response.Error != nil {
        return nil, fmt.Errorf("rpc error: %v", response.Error)
    }
    var dataString string
    switch d := response.Result.Value.Data.(type) {
    case string:
        dataString = d
    case []any:
        if len(d) == 0 {
            return nil, fmt.Errorf("account data missing")
        }
        first, ok := d[0].(string)
        if !ok {
            return nil, fmt.Errorf("unexpected account data format")
        }
        dataString = first
    default:
        return nil, fmt.Errorf("unexpected account data format")
    }
    decoded, err := base64.StdEncoding.DecodeString(dataString)
    if err != nil {
        decoded, err = base64.RawStdEncoding.DecodeString(dataString)
        if err != nil {
            return nil, err
        }
    }
    c.setCachedAccountInfo(account, decoded)
    return decoded, nil
}

func (c *RPCClient) getMintDecimals(token string) (int, error) {
    if strings.TrimSpace(token) == "" {
        return 0, fmt.Errorf("empty token")
    }

    var response struct {
        Result struct {
            Value *struct {
                Data struct {
                    Parsed struct {
                        Info struct {
                            Decimals int `json:"decimals"`
                        } `json:"info"`
                    } `json:"parsed"`
                } `json:"data"`
            } `json:"value"`
        } `json:"result"`
        Error any `json:"error"`
    }

    if err := c.call("getAccountInfo", []any{token, map[string]any{"encoding": "jsonParsed"}}, &response); err != nil {
        return 0, err
    }
    if response.Error != nil {
        return 0, fmt.Errorf("rpc error: %v", response.Error)
    }
    if response.Result.Value == nil {
        return 0, fmt.Errorf("mint account not found: %s", token)
    }

    return response.Result.Value.Data.Parsed.Info.Decimals, nil
}

func (c *RPCClient) getCachedAccountInfo(account string) ([]byte, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    if c.accountCache == nil {
        return nil, false
    }
    data, ok := c.accountCache[account]
    if !ok {
        return nil, false
    }
    return append([]byte(nil), data...), true
}

func (c *RPCClient) setCachedAccountInfo(account string, data []byte) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.accountCache == nil {
        c.accountCache = make(map[string][]byte)
    }
    c.accountCache[account] = append([]byte(nil), data...)
}

func mustJSON(v any) string {
    b, _ := json.Marshal(v)
    return string(b)
}
