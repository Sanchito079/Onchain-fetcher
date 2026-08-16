package sync

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestParseTokenMetadata(t *testing.T) {
	address, decimals, err := parseTokenMetadata(`{"address":"0xabc","decimals":9}`)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if address != "0xabc" {
		t.Fatalf("expected address to be parsed, got %q", address)
	}
	if decimals != 9 {
		t.Fatalf("expected decimals to be parsed, got %d", decimals)
	}
}

func TestParseTokenMetadataEmpty(t *testing.T) {
	address, decimals, err := parseTokenMetadata("")
	if err != nil {
		t.Fatalf("expected no error for empty payload, got %v", err)
	}
	if address != "" {
		t.Fatalf("expected empty address, got %q", address)
	}
	if decimals != 0 {
		t.Fatalf("expected zero decimals, got %d", decimals)
	}
}

func TestBuildSyncQueryAndArgsIncludesRaydiumByDefault(t *testing.T) {
	os.Unsetenv("SYNC_DEX_FILTER")
	os.Unsetenv("SYNC_NETWORK_FILTER")
	query, args := buildSyncQueryAndArgs()
	if len(args) != 0 {
		t.Fatalf("expected no filter args, got %v", args)
	}
	if !regexp.MustCompile(`(?i)raydium`).MatchString(query) {
		t.Fatalf("expected sync query to include raydium, got %q", query)
	}
}

func TestBuildSyncQueryAndArgsRespectsNetworkFilter(t *testing.T) {
	t.Setenv("SYNC_NETWORK_FILTER", "solana")
	query, args := buildSyncQueryAndArgs()
	if len(args) != 0 {
		t.Fatalf("expected no filter args, got %v", args)
	}
	if !strings.Contains(strings.ToLower(query), "network in ('solana')") {
		t.Fatalf("expected network filter to be applied, got %q", query)
	}
}

func TestGetRPCEndpointDefaults(t *testing.T) {
	// Ensure env vars are unset to test defaults
	os.Unsetenv("RPC_ENDPOINT_BASE")
	os.Unsetenv("RPC_ENDPOINT_BSC")

	base := getRPCEndpoint("base")
	if base != "https://palpable-divine-shape.base-mainnet.quiknode.pro/6bbce19b5765801546265b33f2d1fdb2aafa9cc8/" {
		t.Fatalf("unexpected default base endpoint: %s", base)
	}

	bsc := getRPCEndpoint("bsc")
	if bsc != "https://smart-sly-voice.bsc.quiknode.pro/68e23973e7772747604cc40a754b8349c20db22c/" {
		t.Fatalf("unexpected default bsc endpoint: %s", bsc)
	}
}

func TestGetRPCEndpointEnvOverride(t *testing.T) {
	os.Setenv("RPC_ENDPOINT_BASE", "https://example.base/")
	os.Setenv("RPC_ENDPOINT_BSC", "https://example.bsc/")
	os.Setenv("RPC_ENDPOINT_SOLANA", "https://example.solana/")
	defer func() {
		os.Unsetenv("RPC_ENDPOINT_BASE")
		os.Unsetenv("RPC_ENDPOINT_BSC")
		os.Unsetenv("RPC_ENDPOINT_SOLANA")
	}()

	base := getRPCEndpoint("base")
	if base != "https://example.base/" {
		t.Fatalf("env override failed for base: %s", base)
	}

	bsc := getRPCEndpoint("bsc")
	if bsc != "https://example.bsc/" {
		t.Fatalf("env override failed for bsc: %s", bsc)
	}

	solana := getRPCEndpoint("solana")
	if solana != "https://example.solana/" {
		t.Fatalf("env override failed for solana: %s", solana)
	}
}

func TestSyncOnceProcessesSolanaPumpSwapPair(t *testing.T) {
	reserveA := uint64(100_000_000_000)
	reserveB := uint64(50_000_000_000)
	raw := make([]byte, 128)
	for i := 0; i < 8; i++ {
		raw[64+i] = byte(reserveA >> (8 * i))
		raw[72+i] = byte(reserveB >> (8 * i))
	}
	encoded := base64.StdEncoding.EncodeToString(raw)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"value":{"data":["` + encoded + `","base64"]}}}`))
	}))
	defer server.Close()

	os.Setenv("RPC_ENDPOINT_SOLANA", server.URL)
	defer os.Unsetenv("RPC_ENDPOINT_SOLANA")
	os.Setenv("RPC_ENDPOINT_BSC", "https://example.bsc/")
	os.Setenv("RPC_ENDPOINT_BASE", "https://example.base/")
	defer func() {
		os.Unsetenv("RPC_ENDPOINT_BSC")
		os.Unsetenv("RPC_ENDPOINT_BASE")
	}()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "network", "dex_name", "pool_address", "base_token", "quote_token", "base_token_decimals", "quote_token_decimals", "base_symbol", "quote_symbol"}).
		AddRow("pair1", "solana", "PumpSwap", "pool1", "", "", 9, 6, "SOL", "USDC")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).WillReturnRows(rows)

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE pairs
			SET gecko_price = $1,
			    gecko_price_usd = $2,
			    gecko_price_change_24h = 0,
			    gecko_updated_at = $3
			WHERE id = $4
	`)).WithArgs(500.0, 500.0, sqlmock.AnyArg(), "pair1").WillReturnResult(sqlmock.NewResult(1, 1))

	syncer := NewSyncer(db)
	if err := syncer.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations were not met: %v", err)
	}
}

func TestSyncOnceProcessesSolanaPancakeSwapPair(t *testing.T) {
	reserveA := uint64(100_000_000_000)
	reserveB := uint64(50_000_000_000)
	raw := make([]byte, 128)
	for i := 0; i < 8; i++ {
		raw[64+i] = byte(reserveA >> (8 * i))
		raw[72+i] = byte(reserveB >> (8 * i))
	}
	encoded := base64.StdEncoding.EncodeToString(raw)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"value":{"data":["` + encoded + `","base64"]}}}`))
	}))
	defer server.Close()

	os.Setenv("RPC_ENDPOINT_SOLANA", server.URL)
	defer os.Unsetenv("RPC_ENDPOINT_SOLANA")
	os.Setenv("RPC_ENDPOINT_BSC", "https://example.bsc/")
	os.Setenv("RPC_ENDPOINT_BASE", "https://example.base/")
	defer func() {
		os.Unsetenv("RPC_ENDPOINT_BSC")
		os.Unsetenv("RPC_ENDPOINT_BASE")
	}()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "network", "dex_name", "pool_address", "base_token", "quote_token", "base_token_decimals", "quote_token_decimals", "base_symbol", "quote_symbol"}).
		AddRow("pair1", "solana", "pancakeswap-v3-solana", "pool1", "", "", 9, 6, "SOL", "USDC")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).WillReturnRows(rows)

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE pairs
			SET gecko_price = $1,
			    gecko_price_usd = $2,
			    gecko_price_change_24h = 0,
			    gecko_updated_at = $3
			WHERE id = $4
	`)).WithArgs(500.0, 500.0, sqlmock.AnyArg(), "pair1").WillReturnResult(sqlmock.NewResult(1, 1))

	syncer := NewSyncer(db)
	if err := syncer.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations were not met: %v", err)
	}
}
