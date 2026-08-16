package pancakeswap

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"
)

// RPCClient is a minimal HTTP RPC client for PancakeSwap pool reads.
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
		var out struct {
			Result string `json:"result"`
		}
		if err := decodeJSON(resp.Body, &out); err != nil {
			return "", err
		}
		return out.Result, nil
	}
	return "", lastErr
}

func (c RPCClient) getToken0(pool string) (string, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": pool, "data": "0x0dfe1681"}, "latest"})
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 64 {
		return "", errInvalidResponse
	}
	// Address is in the last 20 bytes (40 hex chars) of the 32-byte response
	address := "0x" + trimmed[24:64]
	return strings.ToLower(address), nil
}

func (c RPCClient) getToken1(pool string) (string, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": pool, "data": "0xd21220a7"}, "latest"})
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 64 {
		return "", errInvalidResponse
	}
	// Address is in the last 20 bytes (40 hex chars) of the 32-byte response
	address := "0x" + trimmed[24:64]
	return strings.ToLower(address), nil
}

func (c RPCClient) getReserves(pool string) (*big.Int, *big.Int, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": pool, "data": "0x0902f1ac"}, "latest"})
	if err != nil {
		return nil, nil, err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 128 {
		return nil, nil, errInvalidResponse
	}
	reserve0 := new(big.Int)
	reserve1 := new(big.Int)
	reserve0.SetString(trimmed[0:64], 16)
	reserve1.SetString(trimmed[64:128], 16)
	return reserve0, reserve1, nil
}

func (c RPCClient) getTokenDecimals(token string) (int, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": token, "data": "0x313ce567"}, "latest"})
	if err != nil {
		return 0, err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 1 {
		return 0, errInvalidResponse
	}
	value, ok := new(big.Int).SetString(trimmed, 16)
	if !ok {
		return 0, errInvalidResponse
	}
	return int(value.Int64()), nil
}

func encodeBytes32Arg(raw string) (string, error) {
	trimmed := strings.TrimPrefix(raw, "0x")
	if len(trimmed)%2 == 1 {
		trimmed = "0" + trimmed
	}
	value, err := hex.DecodeString(trimmed)
	if err != nil {
		return "", err
	}
	if len(value) > 32 {
		return "", fmt.Errorf("argument too long")
	}
	padded := make([]byte, 32)
	copy(padded[32-len(value):], value)
	return hex.EncodeToString(padded), nil
}

const infinityCLMMManagerAddress = "0xa0FfB9c1CE1Fe56963B0321B32E7A0302114058b"

func (c RPCClient) getSlot0(pool string) (*big.Int, error) {
	res, err := c.call("eth_call", []any{map[string]string{"to": pool, "data": "0x3850c7bd"}, "latest"})
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 64 {
		return nil, errInvalidResponse
	}
	decoded, err := hex.DecodeString(trimmed[0:64])
	if err != nil {
		return nil, err
	}
	if len(decoded) < 32 {
		return nil, errInvalidResponse
	}
	value := new(big.Int).SetBytes(decoded[0:32])
	return value, nil
}

func (c RPCClient) getCLMMSlot0(poolID string) (*big.Int, error) {
	arg, err := encodeBytes32Arg(poolID)
	if err != nil {
		return nil, err
	}
	data := "0xc815641c" + arg
	res, err := c.call("eth_call", []any{map[string]string{"to": infinityCLMMManagerAddress, "data": data}, "latest"})
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimPrefix(res, "0x")
	if len(trimmed) < 64 {
		return nil, errInvalidResponse
	}
	decoded, err := hex.DecodeString(trimmed[0:64])
	if err != nil {
		return nil, err
	}
	if len(decoded) < 32 {
		return nil, errInvalidResponse
	}
	value := new(big.Int).SetBytes(decoded[0:32])
	return value, nil
}

var errInvalidResponse = fmt.Errorf("invalid rpc response")
