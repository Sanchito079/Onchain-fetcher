package slipstream

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"
)

type RPCClient struct {
	Endpoint string
	Client   *http.Client
}

func (c RPCClient) call(method string, params []any) (string, error) {
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 30 * time.Second}
	}
	payload := fmt.Sprintf(`{"jsonrpc":"2.0","method":"%s","params":%s,"id":1}`, method, mustJSON(params))
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*500) * time.Millisecond)
		}
		req, err := http.NewRequest(http.MethodPost, c.Endpoint, strings.NewReader(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.Client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("rpc status %d", resp.StatusCode)
			continue
		}
		if resp.StatusCode >= 400 {
			return "", fmt.Errorf("rpc status %d", resp.StatusCode)
		}
		var out struct{ Result string `json:"result"` }
		if err := decodeJSON(resp.Body, &out); err != nil {
			return "", err
		}
		return out.Result, nil
	}
	return "", lastErr
}

func (c RPCClient) getReserves(pool string) (*big.Int, *big.Int, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": pool, "data": "0x0902f1ac"}, "latest"})
	if err != nil {
		return nil, nil, err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 128 {
		return nil, nil, fmt.Errorf("invalid rpc response")
	}
	reserve0, ok := new(big.Int).SetString(trimmed[0:64], 16)
	if !ok {
		return nil, nil, fmt.Errorf("invalid reserve0")
	}
	reserve1, ok := new(big.Int).SetString(trimmed[64:128], 16)
	if !ok {
		return nil, nil, fmt.Errorf("invalid reserve1")
	}
	return reserve0, reserve1, nil
}

func (c RPCClient) getSlot0(pool string) (*big.Int, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": pool, "data": "0x3850c7bd"}, "latest"})
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 64 {
		return nil, fmt.Errorf("invalid rpc response")
	}
	decoded, err := hex.DecodeString(trimmed[0:64])
	if err != nil {
		return nil, err
	}
	if len(decoded) < 32 {
		return nil, fmt.Errorf("invalid rpc response")
	}
	value := new(big.Int).SetBytes(decoded[0:32])
	return value, nil
}

func (c RPCClient) getToken0(pool string) (string, error) {
	return c.getTokenAddress(pool, "0x0dfe1681")
}

func (c RPCClient) getToken1(pool string) (string, error) {
	return c.getTokenAddress(pool, "0xd21220a7")
}

func (c RPCClient) getTokenAddress(pool, selector string) (string, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": pool, "data": selector}, "latest"})
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 64 {
		return "", fmt.Errorf("invalid token response")
	}
	return "0x" + trimmed[len(trimmed)-40:], nil
}

func (c RPCClient) getTokenDecimals(token string) (int, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": token, "data": "0x313ce567"}, "latest"})
	if err != nil {
		return 0, err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 1 {
		return 0, fmt.Errorf("invalid decimals response")
	}
	value, ok := new(big.Int).SetString(trimmed, 16)
	if !ok {
		return 0, fmt.Errorf("invalid decimals")
	}
	return int(value.Int64()), nil
}
