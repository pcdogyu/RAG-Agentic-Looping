package marketdata

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestTradabilityImportIsPointInTimeAuditedAndConflictSafeAgainstIsolatedPostgres(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "tradability_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	config, _ := pgxpool.ParseConfig(dsn)
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	assetID := "equity:XNAS:STATUS"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
		VALUES($1,'equity','US','STATUS','Status Inc.','XNAS','USD','[]','[]','[]',1,false)`, assetID); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	input := TradabilityImport{
		SourceName: "Licensed Exchange Status", SourceDocumentID: "daily-status-2026-09-10",
		SourceURL: "https://exchange.example.test/status?token=must-not-persist#rows", LicenseReference: "status-license-1",
		ApprovedBy: "market-data-owner", IdempotencyKey: "status-import-1",
		Observations: []TradabilityImportPoint{
			{SessionDate: "2026-09-09", SourceObservedAt: "2026-09-09T20:01:00Z", Status: Tradable},
			{SessionDate: "2026-09-08", SourceObservedAt: "2026-09-08", Status: TradabilitySuspended},
			{SessionDate: "2026-09-07", SourceObservedAt: "2026-09-07", Status: TradabilityLimitUp},
		},
	}
	receipt, created, err := NewStore(pool).ImportTradability(ctx, assetID, input, now)
	if err != nil || !created || receipt.ObservationCount != 3 || receipt.InsertedCount != 3 || receipt.SessionStart != "2026-09-07" || receipt.SessionEnd != "2026-09-09" || len(receipt.ObservationIDs) != 3 || !receipt.AvailableAt.Equal(now) {
		t.Fatalf("tradability receipt=%#v created=%v err=%v", receipt, created, err)
	}
	if receipt.SourceURL != "https://exchange.example.test/status" || strings.Contains(receipt.SourceURL, "must-not-persist") {
		t.Fatalf("source URL was not sanitized: %s", receipt.SourceURL)
	}
	store := NewStore(pool)
	before, err := store.ListTradabilityAvailable(ctx, assetID, now, now.Add(-time.Second), 100)
	if err != nil || len(before) != 0 {
		t.Fatalf("future availability leaked: items=%#v err=%v", before, err)
	}
	items, err := store.ListTradabilityAvailable(ctx, assetID, now, now, 100)
	if err != nil || len(items) != 3 {
		t.Fatalf("tradability observations=%#v err=%v", items, err)
	}
	for _, item := range items {
		if !item.AvailableAt.Equal(now) || item.ExecutionPoint != "market_close" || item.Metadata["license_reference"] != nil || item.Metadata["approved_by"] != nil {
			t.Fatalf("invalid public observation=%#v", item)
		}
		if item.Status == Tradable && (!item.BuyExecutable || !item.SellExecutable) {
			t.Fatalf("tradable evidence lost execution flags: %#v", item)
		}
		if item.Status != Tradable && (item.BuyExecutable || item.SellExecutable) {
			t.Fatalf("restricted status became executable: %#v", item)
		}
	}
	input.Observations[0], input.Observations[2] = input.Observations[2], input.Observations[0]
	replay, created, err := store.ImportTradability(ctx, assetID, input, now.Add(time.Hour))
	if err != nil || created || replay.ID != receipt.ID || !replay.AvailableAt.Equal(now) {
		t.Fatalf("idempotent replay=%#v created=%v err=%v", replay, created, err)
	}
	input.Observations[0].Status = TradabilityLimitDown
	if _, _, err = store.ImportTradability(ctx, assetID, input, now.Add(2*time.Hour)); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("changed replay was accepted: %v", err)
	}
	second := TradabilityImport{
		SourceName: "Independent Status Vendor", SourceDocumentID: "vendor-status-2026-09-09", SourceURL: "https://vendor.example.test/status",
		LicenseReference: "vendor-license-1", ApprovedBy: "market-data-owner", IdempotencyKey: "status-import-2",
		Observations: []TradabilityImportPoint{{SessionDate: "2026-09-09", SourceObservedAt: "2026-09-09T20:02:00Z", Status: TradabilitySuspended}},
	}
	if _, created, err = store.ImportTradability(ctx, assetID, second, now.Add(3*time.Hour)); err != nil || !created {
		t.Fatalf("second source created=%v err=%v", created, err)
	}
	items, err = store.ListTradabilityAvailable(ctx, assetID, now, now.Add(3*time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	session, _ := time.Parse("2006-01-02", "2026-09-09")
	resolved := ResolveTradability(items, session)
	if resolved.Status != "conflict" || resolved.Reason != "source_disagreement" || resolved.SourceCount != 2 {
		t.Fatalf("source conflict was hidden: %#v", resolved)
	}
	receipts, err := store.ListTradabilityImports(ctx, assetID, 20)
	if err != nil || len(receipts) != 2 || receipts[0].ObservationIDs != nil || receipts[1].ObservationIDs != nil {
		t.Fatalf("receipt audit=%#v err=%v", receipts, err)
	}
	input.IdempotencyKey = "index-not-supported"
	if _, _, err = store.ImportTradability(ctx, "index:CSI:H00300", input, now); err == nil || !strings.Contains(err.Error(), "equity or ETF") {
		t.Fatalf("index tradability import was accepted: %v", err)
	}
	var count int
	for _, table := range []string{"prediction_runs", "outcome_records", "forecast_versions", "valuation_runs", "fundamental_rating_states"} {
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("tradability import mutated %s count=%d err=%v", table, count, err)
		}
	}
}
