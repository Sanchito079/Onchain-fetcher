package pumpswap

import (
    "encoding/base64"
    "encoding/binary"
    "fmt"
    "math/big"
    "strings"
)

// DebugPool fetches pool account, owner token accounts, decimals and returns a formatted report.
func DebugPool(pool, rpcEndpoint string) (string, error) {
    client := RPCClient{Endpoint: rpcEndpoint}
    var out strings.Builder

    raw, err := client.getAccountInfo(pool)
    if err != nil {
        out.WriteString(fmt.Sprintf("getAccountInfo error: %v\n", err))
    } else {
        out.WriteString(fmt.Sprintf("pool account bytes=%d\n", len(raw)))
        if a, b, err := parsePumpSwapAccountData(raw); err == nil {
            out.WriteString(fmt.Sprintf("parsed reserves (raw parse): %s , %s\n", a.String(), b.String()))
        } else {
            out.WriteString(fmt.Sprintf("raw parse failed: %v\n", err))
        }
    }

    accounts, err := client.getTokenAccountsByOwner(pool)
    if err != nil {
        out.WriteString(fmt.Sprintf("getTokenAccountsByOwner error: %v\n", err))
    } else {
        out.WriteString(fmt.Sprintf("owner token accounts found: %d\n", len(accounts)))
        balances := make(map[string]*big.Int)
        mints := make([]string, 0, 2)
        for _, acc := range accounts {
            if acc.Balance == nil || acc.Balance.Sign() == 0 {
                continue
            }
            if _, ok := balances[acc.Mint]; ok {
                continue
            }
            balances[acc.Mint] = acc.Balance
            mints = append(mints, acc.Mint)
            out.WriteString(fmt.Sprintf("pubkey=%s mint=%s owner=%s balance=%s\n", acc.Pubkey, acc.Mint, acc.Owner, acc.Balance.String()))
            if len(mints) >= 2 {
                break
            }
        }

        if len(mints) >= 2 {
            dec0, _ := client.getTokenDecimals(mints[0])
            dec1, _ := client.getTokenDecimals(mints[1])
            out.WriteString(fmt.Sprintf("mint0=%s decimals=%d\n", mints[0], dec0))
            out.WriteString(fmt.Sprintf("mint1=%s decimals=%d\n", mints[1], dec1))

            price0 := computePriceFromBigInts(balances[mints[0]], balances[mints[1]], dec0, dec1)
            price1 := computePriceFromBigInts(balances[mints[1]], balances[mints[0]], dec1, dec0)
            out.WriteString(fmt.Sprintf("price (mint0 as base, mint1 as quote) = %g\n", price0))
            out.WriteString(fmt.Sprintf("price (mint1 as base, mint0 as quote) = %g\n", price1))
        }
    }

    // Combined parser path
    raw2, err := client.getAccountInfo(pool)
    if err == nil {
        a, b, err := parsePumpSwapAccountDataWithRPC(raw2, pool, "", "", func(acc string) ([]byte, error) { return client.getAccountInfo(acc) }, func(owner string) ([]TokenAccountInfo, error) { return client.getTokenAccountsByOwner(owner) })
        if err != nil {
            out.WriteString(fmt.Sprintf("parsePumpSwapAccountDataWithRPC failed: %v\n", err))
        } else {
            out.WriteString(fmt.Sprintf("parsePumpSwapAccountDataWithRPC reserves: %s , %s\n", a.String(), b.String()))
        }
    }

    return out.String(), nil
}

func DebugPoolDeeper(pool, rpcEndpoint string) (string, error) {
    client := RPCClient{Endpoint: rpcEndpoint}
    var out strings.Builder

    raw, err := client.getAccountInfo(pool)
    if err != nil {
        return "", err
    }
    out.WriteString(fmt.Sprintf("deeper scan raw pool bytes=%d\n", len(raw)))

    candidates := collectReserveAccountCandidates(raw)
    out.WriteString(fmt.Sprintf("candidate reserve accounts found: %d\n", len(candidates)))
    for _, candidate := range candidates {
        accountData, err := client.getAccountInfo(candidate)
        if err != nil {
            out.WriteString(fmt.Sprintf("candidate=%s getAccountInfo failed: %v\n", candidate, err))
            continue
        }
        if len(accountData) < 72 {
            out.WriteString(fmt.Sprintf("candidate=%s data too short (%d bytes)\n", candidate, len(accountData)))
            continue
        }
        mintKey := accountData[0:32]
        amount := binary.LittleEndian.Uint64(accountData[64:72])
        encodedMint := encodeBase58(mintKey)
        out.WriteString(fmt.Sprintf("candidate=%s mint=%s balance=%d\n", candidate, encodedMint, amount))
    }

    return out.String(), nil
}

func DebugPoolVaultScan(pool, rpcEndpoint string) (string, error) {
    client := RPCClient{Endpoint: rpcEndpoint}
    var out strings.Builder

    raw, err := client.getAccountInfo(pool)
    if err != nil {
        return "", err
    }
    out.WriteString(fmt.Sprintf("vault-scan raw pool bytes=%d\n", len(raw)))

    var candidates []string
    seen := make(map[string]struct{})
    for offset := 0; offset+32 <= len(raw); offset += 1 {
        candidate := encodeBase58(raw[offset : offset+32])
        if candidate == "" {
            continue
        }
        if _, ok := seen[candidate]; ok {
            continue
        }
        seen[candidate] = struct{}{}
        candidates = append(candidates, candidate)
    }
    out.WriteString(fmt.Sprintf("vault-scan total unique candidates: %d\n", len(candidates)))

    const batchSize = 10
    found := 0
    for start := 0; start < len(candidates); start += batchSize {
        if found >= 2 {
            break
        }
        end := start + batchSize
        if end > len(candidates) {
            end = len(candidates)
        }
        batch := candidates[start:end]
        accounts, err := client.getMultipleAccounts(batch)
        if err != nil {
            out.WriteString(fmt.Sprintf("getMultipleAccounts failed: %v\n", err))
            continue
        }
        for i, account := range accounts {
            if account == nil || account.Data == nil {
                continue
            }
            if account.Owner != "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" {
                continue
            }
            var dataString string
            switch d := account.Data.(type) {
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
            owner := encodeBase58(decoded[32:64])
            balance := binary.LittleEndian.Uint64(decoded[64:72])
            out.WriteString(fmt.Sprintf("candidate-account=%s owner=%s mint=%s balance=%d\n", batch[i], owner, mint, balance))
            found++
            if found >= 2 {
                break
            }
        }
    }

    return out.String(), nil
}

func computePriceFromBigInts(resA, resB *big.Int, decA, decB int) float64 {
    if resA == nil || resB == nil {
        return 0
    }
    if resA.Sign() == 0 || resB.Sign() == 0 {
        return 0
    }
    resAf := new(big.Float).SetInt(resA)
    resBf := new(big.Float).SetInt(resB)
    decAf := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decA)), nil))
    decBf := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decB)), nil))

    priceFloat := new(big.Float).Quo(new(big.Float).Quo(resBf, decBf), new(big.Float).Quo(resAf, decAf))
    v, _ := priceFloat.Float64()
    return v
}
