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

func TestCorporateActionStorePreservesPointInTimeRevisionsAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "corporate_actions_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

	const assetID = "equity:XNAS:ACTION"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES($1,'equity','US','ACTION','Action Inc.','XNAS','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	effective := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	firstAvailable := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	amount := .25
	value := CorporateActionObservation{AssetID: assetID, Market: "US", Currency: "USD", ActionType: CashDividend, EffectiveAt: effective, ObservedAt: effective, AvailableAt: firstAvailable, CashAmount: &amount, TimePrecision: "date_only", SourceName: "FMP", SourceDocumentID: "fmp-dividends:ACTION"}
	created, err := NewStore(pool).SaveCorporateAction(ctx, value)
	if err != nil || !created {
		t.Fatalf("first save created=%v err=%v", created, err)
	}
	value.AvailableAt = firstAvailable.Add(time.Hour)
	created, err = NewStore(pool).SaveCorporateAction(ctx, value)
	if err != nil || created {
		t.Fatalf("duplicate save created=%v err=%v", created, err)
	}
	revised := .30
	value.CashAmount = &revised
	value.AvailableAt = firstAvailable.Add(time.Hour)
	created, err = NewStore(pool).SaveCorporateAction(ctx, value)
	if err != nil || !created {
		t.Fatalf("revision save created=%v err=%v", created, err)
	}
	before, err := NewStore(pool).ListCorporateActionsAvailable(ctx, assetID, effective, firstAvailable, CashDividend, 10)
	if err != nil || len(before) != 1 || before[0].CashAmount == nil || *before[0].CashAmount != .25 {
		t.Fatalf("historical cutoff leaked revision: items=%#v err=%v", before, err)
	}
	after, err := NewStore(pool).ListCorporateActionsAvailable(ctx, assetID, effective, firstAvailable.Add(time.Hour), CashDividend, 10)
	if err != nil || len(after) != 1 || after[0].CashAmount == nil || *after[0].CashAmount != .30 {
		t.Fatalf("latest revision unavailable: items=%#v err=%v", after, err)
	}
	tooEarly, err := NewStore(pool).ListCorporateActionsAvailable(ctx, assetID, effective, firstAvailable.Add(-time.Second), "", 10)
	if err != nil || len(tooEarly) != 0 {
		t.Fatalf("future availability leaked into cutoff: items=%#v err=%v", tooEarly, err)
	}
}
