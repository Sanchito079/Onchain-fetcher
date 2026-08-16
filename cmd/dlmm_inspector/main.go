// cmd/dlmm_inspector: Captures the first Meteora DLMM swap transaction
// and prints a complete analysis:
//   - All raw log lines
//   - Every "Program data:" line decoded (base64 → hex + byte dump)
//   - Pool address candidate at offset 8 in each data blob
//   - Summary of what fields are present
//
// Run:
//   go run cmd/dlmm_inspector/main.go
//
// Or with a custom RPC:
//   RPC_WS=wss://... go run cmd/dlmm_inspector/main.go
package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	ProgramMeteoraDLMM   = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
	ProgramMeteoraDammV2 = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"
	ProgramMeteoraDamm   = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EkQXCEg"
	ProgramPumpSwap      = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
)

func main() {
	wsURL := os.Getenv("RPC_WS")
	if wsURL == "" {
		// Default to QuickNode endpoint
		wsURL = "wss://quaint-stylish-meme.solana-mainnet.quiknode.pro/3d26a9037c0fcba060d9e6984c30d28eccfb76cf/"
	}

	// Which program to inspect — default DLMM
	program := os.Getenv("INSPECT_PROGRAM")
	if program == "" {
		program = ProgramMeteoraDLMM
	}

	programName := map[string]string{
		ProgramMeteoraDLMM:   "Meteora DLMM",
		ProgramMeteoraDammV2: "Meteora DAMM v2",
		ProgramMeteoraDamm:   "Meteora DAMM (old)",
		ProgramPumpSwap:      "PumpSwap",
	}[program]
	if programName == "" {
		programName = program
	}

	maxSwaps := 3 // capture this many swap txns then exit

	fmt.Printf("╔══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  Meteora Swap Inspector                                  ║\n")
	fmt.Printf("╚══════════════════════════════════════════════════════════╝\n")
	fmt.Printf("Program : %s\n", programName)
	fmt.Printf("ID      : %s\n", program)
	fmt.Printf("Endpoint: %s\n", wsURL[:min(60, len(wsURL))]+"...")
	fmt.Printf("Waiting for %d swap(s)...\n\n", maxSwaps)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "logsSubscribe",
		"params": []any{
			map[string]any{"mentions": []string{program}},
			map[string]any{"commitment": "confirmed"},
		},
	}
	if err := conn.WriteJSON(req); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	swapCount := 0

	for swapCount < maxSwaps {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatalf("read: %v", err)
		}

		var notif struct {
			Method string `json:"method"`
			Params *struct {
				Result *struct {
					Value *struct {
						Signature string   `json:"signature"`
						Logs      []string `json:"logs"`
						Err       any      `json:"err"`
					} `json:"value"`
				} `json:"result"`
			} `json:"params"`
			ID     *int            `json:"id"`
			Result json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(msg, &notif); err != nil {
			continue
		}

		// Subscription confirmation
		if notif.ID != nil {
			fmt.Printf("✅ Subscribed to %s\n\n", programName)
			continue
		}

		if notif.Method != "logsNotification" || notif.Params == nil {
			continue
		}
		result := notif.Params.Result
		if result == nil || result.Value == nil {
			continue
		}

		// Skip failed transactions
		if result.Value.Err != nil {
			continue
		}

		logs := result.Value.Logs
		sig := result.Value.Signature

		// Only care about transactions that contain a Swap instruction
		isSwap := false
		for _, l := range logs {
			lower := strings.ToLower(l)
			if strings.Contains(lower, "instruction: swap") {
				isSwap = true
				break
			}
		}
		if !isSwap {
			continue
		}

		swapCount++
		printSwapAnalysis(swapCount, sig, logs, program)
	}

	fmt.Printf("\n✅ Captured %d swap(s). Done.\n", swapCount)
}

func printSwapAnalysis(n int, sig string, logs []string, program string) {
	fmt.Printf("\n")
	fmt.Printf("══════════════════════════════════════════════════════════════\n")
	fmt.Printf("SWAP #%d\n", n)
	fmt.Printf("Sig: %s\n", sig)
	fmt.Printf("══════════════════════════════════════════════════════════════\n")

	// 1. Print all raw logs
	fmt.Printf("\n📋 ALL LOG LINES (%d total):\n", len(logs))
	for i, l := range logs {
		fmt.Printf("  [%02d] %s\n", i, l)
	}

	// 2. Find and decode all "Program data:" lines
	fmt.Printf("\n📦 PROGRAM DATA LINES:\n")
	dataCount := 0
	for i, l := range logs {
		if !strings.Contains(l, "Program data: ") {
			continue
		}
		dataCount++
		b64 := strings.TrimSpace(strings.SplitN(l, "Program data:", 2)[1])

		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			data, err = base64.RawStdEncoding.DecodeString(b64)
		}

		fmt.Printf("\n  ── Data line at log[%02d] ──\n", i)
		fmt.Printf("  Base64 : %s\n", b64)

		if err != nil || len(data) == 0 {
			fmt.Printf("  ❌ Failed to decode base64\n")
			continue
		}

		fmt.Printf("  Length : %d bytes\n", len(data))
		fmt.Printf("  Hex    : %s\n", formatHex(data))

		// Discriminator (first 8 bytes)
		if len(data) >= 8 {
			fmt.Printf("  Disc   : [%s]\n", formatDiscriminator(data[:8]))
		}

		// Try to decode as pool address at offset 8
		if len(data) >= 40 {
			poolCandidate := encodeBase58(data[8:40])
			fmt.Printf("  Addr@8 : %s  ← pool address if Anchor event\n", poolCandidate)
		}

		// Print structured breakdown based on length
		analyzeDataBlob(data, program)
	}

	if dataCount == 0 {
		fmt.Printf("  ⚠️  NO 'Program data:' lines found in this transaction!\n")
		fmt.Printf("  This program does NOT emit Anchor events — it's account-based.\n")
	}

	// 3. Check for "Program return:" lines
	fmt.Printf("\n🔄 PROGRAM RETURN LINES:\n")
	returnCount := 0
	for i, l := range logs {
		if strings.Contains(l, "Program return:") {
			returnCount++
			fmt.Printf("  [%02d] %s\n", i, l)
		}
	}
	if returnCount == 0 {
		fmt.Printf("  (none)\n")
	}

	// 4. Summary
	fmt.Printf("\n📊 SUMMARY:\n")
	fmt.Printf("  Total log lines  : %d\n", len(logs))
	fmt.Printf("  Program data     : %d line(s)\n", dataCount)
	fmt.Printf("  Program return   : %d line(s)\n", returnCount)

	if dataCount == 0 {
		fmt.Printf("\n  🔴 CONCLUSION: No event data emitted.\n")
		fmt.Printf("     This program is account-based. Use accountSubscribe instead.\n")
	} else {
		fmt.Printf("\n  🟢 CONCLUSION: Event data found! Can decode price from 'Program data:'\n")
	}
}

func analyzeDataBlob(data []byte, program string) {
	n := len(data)

	if n < 8 {
		return
	}

	// Print byte-by-byte for small blobs
	if n <= 200 {
		fmt.Printf("  Bytes  :\n")
		for i := 0; i < n; i += 16 {
			end := i + 16
			if end > n {
				end = n
			}
			chunk := data[i:end]
			hexPart := hex.EncodeToString(chunk)
			// pad to 32 chars
			for len(hexPart) < 32 {
				hexPart += "  "
			}
			asciiPart := ""
			for _, b := range chunk {
				if b >= 32 && b < 127 {
					asciiPart += string(rune(b))
				} else {
					asciiPart += "."
				}
			}
			fmt.Printf("    [%03d] %s  |%s|\n", i, hexPart, asciiPart)
		}
	}

	// Try to read u64 values at common offsets — potential amounts/reserves
	fmt.Printf("  u64 values at key offsets:\n")
	offsets := []int{40, 48, 56, 64, 72, 76, 80, 88, 96, 104, 107, 115, 123}
	for _, off := range offsets {
		if off+8 <= n {
			v := binary.LittleEndian.Uint64(data[off : off+8])
			if v > 0 && v < (1<<62) {
				fmt.Printf("    [%03d] u64 = %d  (0x%016x)\n", off, v, v)
			}
		}
	}

	// Try i32 at DLMM bin ID offsets
	if program == ProgramMeteoraDLMM && n >= 80 {
		startBin := int32(binary.LittleEndian.Uint32(data[72:76]))
		endBin := int32(binary.LittleEndian.Uint32(data[76:80]))
		fmt.Printf("  DLMM startBinId@72: %d\n", startBin)
		fmt.Printf("  DLMM endBinId@76  : %d  ← active bin = price\n", endBin)
		if n > 96 {
			swapForY := data[96]
			fmt.Printf("  DLMM swapForY@96  : %d  (0=X→Y, 1=Y→X)\n", swapForY)
		}
	}

	// Try PumpSwap-specific fields if this is PumpSwap
	if program == ProgramPumpSwap && n >= 100 {
		fmt.Printf("  PumpSwap analysis:\n")
		// PumpSwap uses a constant product AMM, so it should have reserve amounts
		// Try to find reserve-like fields
		for off := 40; off+8 <= n && off < 120; off += 8 {
			v := binary.LittleEndian.Uint64(data[off : off+8])
			if v > 1000000 && v < (1<<60) {
				fmt.Printf("    [%03d] potential reserve: %d\n", off, v)
			}
		}
	}
}

func formatHex(data []byte) string {
	if len(data) <= 32 {
		return hex.EncodeToString(data)
	}
	return hex.EncodeToString(data[:32]) + fmt.Sprintf("...(%d bytes total)", len(data))
}

func formatDiscriminator(d []byte) string {
	parts := make([]string, len(d))
	for i, b := range d {
		parts[i] = fmt.Sprintf("%d", b)
	}
	return strings.Join(parts, ", ")
}

func encodeBase58(input []byte) string {
	if len(input) == 0 {
		return ""
	}
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	zero := big.NewInt(0)
	base := big.NewInt(58)
	value := new(big.Int).SetBytes(input)
	if value.Sign() == 0 {
		return string(alphabet[0])
	}
	var encoded []byte
	for value.Cmp(zero) > 0 {
		rem := new(big.Int)
		value.DivMod(value, base, rem)
		encoded = append(encoded, alphabet[rem.Int64()])
	}
	for i, j := 0, len(encoded)-1; i < j; i, j = i+1, j-1 {
		encoded[i], encoded[j] = encoded[j], encoded[i]
	}
	return string(encoded)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
