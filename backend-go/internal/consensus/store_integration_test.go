package consensus

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

func TestGuidanceHistoryPreservesPointInTimeOldAndNewRangesAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "guidance_history_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	assetID := "equity:NYSE:GUIDANCE"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
		VALUES($1,'equity','US','GUIDANCE','Guidance Test','NYSE','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	period := time.Date(2027, 12, 31, 0, 0, 0, 0, time.UTC)
	firstTime := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	secondTime := firstTime.Add(30 * 24 * time.Hour)
	low, high, newLow, newHigh := 90.0, 110.0, 105.0, 115.0
	store := NewStore(pool)
	old := Guidance{AssetID: assetID, Metric: "revenue", FiscalPeriod: "FY", FiscalPeriodEnd: period, AccountingBasis: "us_gaap", LowValue: &low, HighValue: &high, Currency: "USD", Unit: "millions", PublishedAt: firstTime, AvailableAt: firstTime, SourceName: "Issuer filing", SourceURL: "https://issuer.test/old", SourceDocumentID: "old", SourcePayload: map[string]any{"range": "old"}, RetrievedAt: firstTime}
	current := old
	current.ID, current.LowValue, current.HighValue, current.PublishedAt, current.AvailableAt, current.SourceURL, current.SourceDocumentID, current.SourcePayload, current.RetrievedAt = "", &newLow, &newHigh, secondTime, secondTime, "https://issuer.test/new", "new", map[string]any{"range": "new"}, secondTime
	if created, err := store.SaveGuidance(ctx, old); err != nil || !created {
		t.Fatalf("save old guidance created=%v err=%v", created, err)
	}
	if created, err := store.SaveGuidance(ctx, current); err != nil || !created {
		t.Fatalf("save current guidance created=%v err=%v", created, err)
	}
	beforeRevision, err := store.GuidanceHistory(ctx, assetID, secondTime.Add(-time.Nanosecond), 10)
	if err != nil || len(beforeRevision) != 1 || beforeRevision[0].SourceDocumentID != "old" {
		t.Fatalf("future guidance leaked: items=%#v err=%v", beforeRevision, err)
	}
	history, err := store.GuidanceHistory(ctx, assetID, secondTime, 10)
	if err != nil || len(history) != 2 || history[0].SourceDocumentID != "new" || history[0].SourcePayload["range"] != "new" {
		t.Fatalf("guidance history=%#v err=%v", history, err)
	}
	revisions := BuildGuidanceRevisions(history)
	if len(revisions) != 1 || revisions[0].Direction != "raised" || revisions[0].RangeChange != "narrowed" {
		t.Fatalf("guidance revisions=%#v", revisions)
	}
}
