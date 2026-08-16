// Package stats handles 24h price statistics: high, low, and % change.
// Strategy:
//   1. Insert every price update into the price_history table.
//   2. Query MIN/MAX over the last 24 hours for the pair.
//   3. Compute % change as (current - price_24h_ago) / price_24h_ago * 100.
//   4. Update gecko_price, gecko_price_usd, gecko_high_24h, gecko_low_24h,
//      gecko_price_change_24h, gecko_updated_at on the pairs row.
package stats

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// UpdatePriceWithStats writes a price to price_history, then computes the
// 24h high, low and % change and writes all of them back to the pairs table.
// It is safe to call concurrently — each call is a single DB transaction.
func UpdatePriceWithStats(db *sql.DB, pairID string, price, priceUSD float64) error {
	now := time.Now().UTC()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Record the price in history
	// Format price to 18 decimals to prevent PostgreSQL NUMERIC overflow
	formattedPrice := fmt.Sprintf("%.18f", price)
	roundedPrice, err := strconv.ParseFloat(formattedPrice, 64)
	if err != nil {
		return fmt.Errorf("failed to format price for history: %w", err)
	}
	formattedPriceUSD := fmt.Sprintf("%.18f", priceUSD)
	roundedPriceUSD, err := strconv.ParseFloat(formattedPriceUSD, 64)
	if err != nil {
		return fmt.Errorf("failed to format priceUSD for history: %w", err)
	}
	
	if _, err := tx.Exec(`
		INSERT INTO price_history (pair_id, price, price_usd, timestamp)
		VALUES ($1, $2, $3, $4)
	`, pairID, roundedPrice, roundedPriceUSD, now); err != nil {
		return err
	}

	// 2. Calculate 24h high/low from history
	var high24h, low24h float64
	row := tx.QueryRow(`
		SELECT
			COALESCE(MAX(price), $2) AS high_24h,
			COALESCE(MIN(price), $2) AS low_24h
		FROM price_history
		WHERE pair_id = $1
		  AND timestamp >= NOW() - INTERVAL '24 hours'
	`, pairID, price)
	if err := row.Scan(&high24h, &low24h); err != nil {
		high24h = price
		low24h = price
	}

	// 3. Get the reference price for % change calculation:
	//    - Ideal: the price from exactly 24 hours ago (or closest record before that)
	//    - Fallback: the oldest price we have (so change starts updating immediately)
	var price24hAgo float64
	row24h := tx.QueryRow(`
		SELECT price
		FROM price_history
		WHERE pair_id = $1
		  AND timestamp <= NOW() - INTERVAL '24 hours'
		ORDER BY timestamp DESC
		LIMIT 1
	`, pairID)
	if err := row24h.Scan(&price24hAgo); err != nil {
		// No 24h-old record yet — use the OLDEST record we have as reference.
		// This makes price_change reflect movement since we first started tracking,
		// and it will naturally become a proper 24h change once we have enough history.
		oldestRow := tx.QueryRow(`
			SELECT price
			FROM price_history
			WHERE pair_id = $1
			ORDER BY timestamp ASC
			LIMIT 1
		`, pairID)
		if err2 := oldestRow.Scan(&price24hAgo); err2 != nil {
			price24hAgo = price // truly no history yet
		}
	}

	// 4. Compute % change
	var priceChange24h float64
	if price24hAgo > 0 {
		priceChange24h = (price - price24hAgo) / price24hAgo * 100
		// Cap price change to prevent numeric overflow in NUMERIC(20,6) column
		// Max safe value: 99999999999999 (14 digits before decimal)
		if priceChange24h > 99999999999999 {
			priceChange24h = 99999999999999
		} else if priceChange24h < -99999999999999 {
			priceChange24h = -99999999999999
		}
	}

	// 5. Update the pairs row with all stats
	// Format all values to prevent numeric overflow
	formattedCurrent := fmt.Sprintf("%.18f", roundedPrice)
	currentForUpdate, _ := strconv.ParseFloat(formattedCurrent, 64)
	formattedCurrentUSD := fmt.Sprintf("%.18f", roundedPriceUSD)
	currentUSDForUpdate, _ := strconv.ParseFloat(formattedCurrentUSD, 64)
	formattedHigh := fmt.Sprintf("%.18f", high24h)
	highForUpdate, _ := strconv.ParseFloat(formattedHigh, 64)
	formattedLow := fmt.Sprintf("%.18f", low24h)
	lowForUpdate, _ := strconv.ParseFloat(formattedLow, 64)
	formattedChange := fmt.Sprintf("%.6f", priceChange24h)
	changeForUpdate, _ := strconv.ParseFloat(formattedChange, 64)
	
	if _, err := tx.Exec(`
		UPDATE pairs
		SET gecko_price           = $1,
		    gecko_price_usd       = $2,
		    gecko_high_24h        = $3,
		    gecko_low_24h         = $4,
		    gecko_price_change_24h = $5,
		    gecko_updated_at      = $6
		WHERE id = $7
	`, currentForUpdate, currentUSDForUpdate, highForUpdate, lowForUpdate, changeForUpdate, now, pairID); err != nil {
		return err
	}

	// 6. Prune old price history beyond 7 days to keep the table lean
	// Run this as best-effort (non-blocking) — don't fail if it errors
	tx.Exec(`
		DELETE FROM price_history
		WHERE pair_id = $1
		  AND timestamp < NOW() - INTERVAL '7 days'
	`, pairID)

	return tx.Commit()
}
