package pumpswap

import (
    "math"
    "math/big"

    "on-chain-price-fetcher/internal/adapters/shared"
)

func calculateSolanaPrice(reserveA, reserveB *big.Int, decimalA, decimalB int, tokenAIsBase bool) float64 {
    if reserveA == nil || reserveB == nil {
        return 0
    }
    if reserveA.Sign() == 0 || reserveB.Sign() == 0 {
        return 0
    }

    resA := new(big.Float).SetInt(reserveA)
    resB := new(big.Float).SetInt(reserveB)
    decA := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimalA)), nil))
    decB := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimalB)), nil))

    priceFloat := new(big.Float).Quo(new(big.Float).Quo(resB, decB), new(big.Float).Quo(resA, decA))
    price, _ := priceFloat.Float64()
    if !tokenAIsBase {
        if price == 0 {
            return 0
        }
        price = 1 / price
    }
    if math.IsNaN(price) || math.IsInf(price, 0) {
        return 0
    }
    return price
}

func choosePumpSwapPrice(directPrice, invertedPrice float64) float64 {
    if isAcceptablePumpSwapPrice(directPrice) {
        return directPrice
    }
    if isAcceptablePumpSwapPrice(invertedPrice) {
        return invertedPrice
    }
    return shared.ChooseSanePrice(directPrice, invertedPrice)
}

func isAcceptablePumpSwapPrice(price float64) bool {
    return price > 0 && !math.IsNaN(price) && !math.IsInf(price, 0) && price <= 1e12
}
