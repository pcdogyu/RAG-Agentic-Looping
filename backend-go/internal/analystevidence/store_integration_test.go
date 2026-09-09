package analystevidence

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

func TestStorePersistsImmutableTypedEvidenceAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "analyst_evidence_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	const assetID = "equity:XNAS:ACME"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES($1,'equity','US','ACME','Acme','XNAS','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	approved := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	input := validSubmission(BenchmarkExpectation, map[string]any{"benchmark_id": "equity:US:SPY", "expected_return": .05})
	input.AssetID = assetID
	store := NewStore(pool)
	created, wasCreated, err := store.Create(ctx, input, approved)
	if err != nil || !wasCreated || created.ID == "" || created.SourceURL != "https://example.test/report" {
		t.Fatalf("created=%#v wasCreated=%v err=%v", created, wasCreated, err)
	}
	repeated, wasCreated, err := store.Create(ctx, input, approved)
	if err != nil || wasCreated || repeated.ID != created.ID {
		t.Fatalf("repeated=%#v wasCreated=%v err=%v", repeated, wasCreated, err)
	}
	changed := input
	changed.Values = map[string]any{"benchmark_id": "equity:US:SPY", "expected_return": .06}
	if _, _, err = store.Create(ctx, changed, approved); err == nil {
		t.Fatal("idempotency key accepted different evidence")
	}
	items, err := store.Require(ctx, assetID, []string{created.ID}, BenchmarkExpectation, approved)
	if err != nil || len(items) != 1 {
		t.Fatalf("required=%#v err=%v", items, err)
	}
	if _, err = store.Require(ctx, assetID, []string{created.ID}, ValuationMultiple, approved); err == nil {
		t.Fatal("wrong evidence type was accepted")
	}
	if _, err = store.Get(ctx, assetID, created.ID, created.AvailableAt.Add(-time.Nanosecond)); err == nil {
		t.Fatal("evidence leaked before available_at")
	}
}
