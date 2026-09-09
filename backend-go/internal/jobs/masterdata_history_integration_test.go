package jobs

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestPersistMarketRecordsIncludedExcludedAndDelistedMembershipsAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "masterdata_history_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	configDB, _ := pgxpool.ParseConfig(dsn)
	configDB.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, configDB)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
        VALUES('equity:NYSE:OLD','equity','US','OLD','Old Inc.','NYSE','USD','[]','[]','[]',1,true)`); err != nil {
		t.Fatal(err)
	}
	runtime := &masterdataRuntime{db: pool, cfg: config.Config{FMPBaseURL: "https://user:secret@example.test/stable?apikey=secret"}}
	assets := []masterAsset{
		{ID: "equity:NASDAQ:NEW", Class: "equity", Market: "US", Symbol: "NEW", Name: "New Inc.", Exchange: "NASDAQ", Currency: "USD", LotSize: 1, Active: true, AssociationTier: "standard", AssociationReason: "provider_verified"},
		{ID: "equity:NYSE:DELISTED", Class: "equity", Market: "US", Symbol: "DELISTED", Name: "Delisted Inc.", Exchange: "NYSE", Currency: "USD", LotSize: 1, Active: false, AssociationTier: "standard", AssociationReason: "provider_verified"},
	}
	result, err := runtime.persistMarket(ctx, "US", assets)
	if err != nil {
		t.Fatal(err)
	}
	if result["included"] != 1 || result["excluded"] != 1 || result["delisted"] != 1 {
		t.Fatalf("unexpected membership counts: %#v", result)
	}
	snapshots, err := marketdata.NewStore(pool).ListUniverseSnapshots(ctx, "market:US", time.Now().UTC(), 10)
	if err != nil || len(snapshots) != 1 || snapshots[0].SourceURL != "https://example.test/stable/company-screener" {
		t.Fatalf("unexpected snapshot: snapshots=%#v err=%v", snapshots, err)
	}
	for assetID, want := range map[string]string{"equity:NASDAQ:NEW": "included", "equity:NYSE:DELISTED": "delisted", "equity:NYSE:OLD": "excluded"} {
		items, queryErr := marketdata.NewStore(pool).ListAssetUniverseMemberships(ctx, assetID, time.Now().UTC(), 10)
		if queryErr != nil || len(items) != 1 || items[0].MembershipStatus != want {
			t.Fatalf("asset %s membership=%#v err=%v", assetID, items, queryErr)
		}
	}
	var oldActive bool
	if err := pool.QueryRow(ctx, `SELECT active FROM assets WHERE id='equity:NYSE:OLD'`).Scan(&oldActive); err != nil || oldActive {
		t.Fatalf("current projection was not updated after history capture: active=%v err=%v", oldActive, err)
	}
}
