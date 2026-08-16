package pumpswap

import (
    "encoding/base64"
    "encoding/binary"
    "encoding/json"
    "fmt"
    "io"
    "math/big"
    "net/http"
    "strings"
    "sync"
    "time"
)

type RPCClient struct {
    Endpoint string
    Client   *http.Client

    mu                sync.RWMutex
    accountCache     map[string][]byte
    decimalsCache    map[string]int
    poolDetailsCache map[string]*PoolTokenAccountDetails
}

func (c *RPCClient) call(method string, params []any, result any) error {
    if c.Client == nil {
        c.Client = &http.Client{Timeout: 10 * time.Second}
    }

    payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s","params":%s}` , method, mustJSON(params))
    req, err := http.NewRequest(http.MethodPost, c.Endpoint, strings.NewReader(payload))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")

    resp, err := c.Client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode >= 400 {
        return fmt.Errorf("rpc status %d", resp.StatusCode)
    }

    if err := decodeJSON(resp.Body, result); err != nil {
        return err
    }
    return nil
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

func (c *RPCClient) getPoolTokenAccountDetails(pool string, poolData []byte) (*PoolTokenAccountDetails, error) {
    if strings.TrimSpace(pool) == "" {
        return nil, fmt.Errorf("empty pool")
    }

    c.mu.RLock()
    if c.poolDetailsCache != nil {
        if details, ok := c.poolDetailsCache[pool]; ok && details != nil {
            c.mu.RUnlock()
            return details, nil
        }
    }
    c.mu.RUnlock()

    details, err := parsePoolTokenAccountDetails(poolData, func(account string) ([]byte, error) {
        return c.getAccountInfo(account)
    })
    if err != nil {
        return nil, err
    }

    c.mu.Lock()
    if c.poolDetailsCache == nil {
        c.poolDetailsCache = make(map[string]*PoolTokenAccountDetails)
    }
    c.poolDetailsCache[pool] = details
    c.mu.Unlock()
    return details, nil
}

func (c *RPCClient) getTokenAccountsByOwner(owner string) ([]TokenAccountInfo, error) {
    var response struct {
        Result struct {
            Value []struct {
                Pubkey  string `json:"pubkey"`
                Account struct {
                    Data any `json:"data"`
                } `json:"account"`
            } `json:"value"`
        } `json:"result"`
        Error any `json:"error"`
    }

    params := []any{owner, map[string]any{"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}, map[string]any{"encoding": "base64"}}
    if err := c.call("getTokenAccountsByOwner", params, &response); err != nil {
        return nil, err
    }
    if response.Error != nil {
        return nil, fmt.Errorf("rpc error: %v", response.Error)
    }

    accounts := make([]TokenAccountInfo, 0, len(response.Result.Value))
    for _, item := range response.Result.Value {
        var dataString string
        switch d := item.Account.Data.(type) {
        case string:
            dataString = d
        case []any:
            if len(d) == 0 {
                continue
            }
            first, ok := d[0].(string)
            if !ok {
                continue
            }
            dataString = first
        default:
            continue
        }

        decoded, err := base64.StdEncoding.DecodeString(dataString)
        if err != nil {
            decoded, err = base64.RawStdEncoding.DecodeString(dataString)
            if err != nil {
                continue
            }
        }
        if len(decoded) < 72 {
            continue
        }
        mint := encodeBase58(decoded[0:32])
        ownerKey := encodeBase58(decoded[32:64])
        amount := binary.LittleEndian.Uint64(decoded[64:72])
        accounts = append(accounts, TokenAccountInfo{
            Pubkey:  item.Pubkey,
            Mint:    mint,
            Owner:   ownerKey,
            Balance: new(big.Int).SetUint64(amount),
        })
    }

    return accounts, nil
}

// getTokenDecimals returns the decimals for an SPL token mint by querying
// the mint account with jsonParsed encoding and reading the parsed info.decimals.
func (c *RPCClient) getTokenDecimals(token string) (int, error) {
    if strings.TrimSpace(token) == "" {
        return 0, fmt.Errorf("empty token")
    }

    c.mu.RLock()
    if c.decimalsCache != nil {
        if decimals, ok := c.decimalsCache[token]; ok {
            c.mu.RUnlock()
            return decimals, nil
        }
    }
    c.mu.RUnlock()

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

    decimals := response.Result.Value.Data.Parsed.Info.Decimals
    c.mu.Lock()
    if c.decimalsCache == nil {
        c.decimalsCache = make(map[string]int)
    }
    c.decimalsCache[token] = decimals
    c.mu.Unlock()
    return decimals, nil
}

type MultipleAccountInfo struct {
    Data  any    `json:"data"`
    Owner string `json:"owner"`
}

func (c *RPCClient) getMultipleAccounts(accounts []string) ([]*MultipleAccountInfo, error) {
    var response struct {
        Result struct {
            Value []*MultipleAccountInfo `json:"value"`
        } `json:"result"`
        Error any `json:"error"`
    }

    if err := c.call("getMultipleAccounts", []any{accounts, map[string]any{"encoding": "base64"}}, &response); err != nil {
        return nil, err
    }
    if response.Error != nil {
        return nil, fmt.Errorf("rpc error: %v", response.Error)
    }
    return response.Result.Value, nil
}

func mustJSON(value any) string {
    data, err := json.Marshal(value)
    if err != nil {
        panic(fmt.Sprintf("json marshal failed: %v", err))
    }
    return string(data)
}

func decodeJSON(r io.Reader, out any) error {
    return json.NewDecoder(r).Decode(out)
}
