package jobs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestMasterdataPersistencePreservesOverridesAndDeactivatesMissing(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	const market = "MASTERDATA_TEST"
	const first = "equity:MASTERDATA_TEST:ONE"
	const second = "equity:MASTERDATA_TEST:TWO"
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM assets WHERE market=$1`, market)
		_, _ = pool.Exec(context.Background(), `DELETE FROM asset_universe_sync WHERE market=$1`, market)
		_, _ = pool.Exec(context.Background(), `DELETE FROM industries WHERE id IN ('industry:masterdata-test','sector:masterdata-test')`)
	}()
	_, err = pool.Exec(ctx, `INSERT INTO industries(id,parent_id,level,name_zh,name_en,aliases,active) VALUES
		('sector:masterdata-test',NULL,1,'测试板块','Test Sector','[]',true),
		('industry:masterdata-test','sector:masterdata-test',2,'测试行业','Test Industry','[]',true)
		ON CONFLICT(id) DO NOTHING`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,
		sector_id,industry_id,raw_sector,raw_industry,instrument_type,association_tier,association_reason,provider_association_tier,
		provider_association_reason,manual_industry_id,manual_active,manual_association_tier,lot_size,active)
		VALUES($1,'equity',$2,'ONE','Old Name','TEST','USD','["old"]','[]','[]','sector:masterdata-test','industry:masterdata-test','','','common_stock',
		'exact_only','manual_override','standard','provider_verified','industry:masterdata-test',false,'exact_only',1,false)
		ON CONFLICT(id) DO UPDATE SET manual_industry_id='industry:masterdata-test',manual_active=false,manual_association_tier='exact_only'`, first, market)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &masterdataRuntime{db: pool}
	if err := runtime.startMarketSync(ctx, market); err != nil {
		t.Fatal(err)
	}
	runtime.failMarketSync(ctx, market, errors.New("provider unavailable"))
	var syncStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM asset_universe_sync WHERE market=$1`, market).Scan(&syncStatus); err != nil || syncStatus != "failed" {
		t.Fatalf("provider failure was not persisted: status=%s err=%v", syncStatus, err)
	}
	if err := runtime.startMarketSync(ctx, market); err != nil {
		t.Fatal(err)
	}
	assets := []masterAsset{
		{ID: first, Class: "equity", Market: market, Symbol: "ONE", Name: "New Name", Exchange: "TEST", Currency: "USD", Aliases: []string{"new"}, Sector: "provider-sector", Industry: "provider-industry", Instrument: "common_stock", AssociationTier: "standard", AssociationReason: "provider_verified", LotSize: 1, Active: true},
		{ID: second, Class: "equity", Market: market, Symbol: "TWO", Name: "Second", Exchange: "TEST", Currency: "USD", AssociationTier: "standard", AssociationReason: "provider_verified", LotSize: 1, Active: true},
	}
	if _, err := runtime.persistMarket(ctx, market, assets); err != nil {
		t.Fatal(err)
	}
	var industry, tier, providerTier string
	var active bool
	var aliases []byte
	if err := pool.QueryRow(ctx, `SELECT industry_id,association_tier,provider_association_tier,active,aliases::jsonb FROM assets WHERE id=$1`, first).
		Scan(&industry, &tier, &providerTier, &active, &aliases); err != nil {
		t.Fatal(err)
	}
	if industry != "industry:masterdata-test" || tier != "exact_only" || providerTier != "standard" || active || !strings.Contains(string(aliases), "old") || !strings.Contains(string(aliases), "new") {
		t.Fatalf("manual/provider state changed: industry=%s tier=%s provider=%s active=%v aliases=%s", industry, tier, providerTier, active, aliases)
	}
	if err := runtime.startMarketSync(ctx, market); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.persistMarket(ctx, market, assets[:1]); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT active FROM assets WHERE id=$1`, second).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatal("missing provider asset was not deactivated")
	}
}

func TestFundamentalRefreshCandidatesBootstrapOnlyRecentCurrentContractResearch(t *testing.T) {
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
	schema := "fundamental_bootstrap_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	dbConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	dbConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, values := range [][]string{
		{"equity:XNAS:TRACKED", "US", "TRACKED"},
		{"equity:XNAS:RECENT", "US", "RECENT"},
		{"equity:XNAS:LEGACY", "US", "LEGACY"},
		{"equity:XNAS:EXPIRED", "US", "EXPIRED"},
		{"equity:XSHG:CN", "CN", "600000"},
	} {
		if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
			VALUES($1,'equity',$2,$3,$3,'TEST','USD','[]','[]','[]',1,true)`, values[0], values[1], values[2]); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	if _, err = pool.Exec(ctx, `INSERT INTO forecast_versions(id,asset_id,model_version,status,as_of,input_snapshot,assumptions,projection)
		VALUES('forecast-tracked','equity:XNAS:TRACKED','test','unavailable',$1,'{}','[]','{}')`, now.AddDate(0, 0, -60)); err != nil {
		t.Fatal(err)
	}
	for index, values := range []struct {
		assetID string
		asOf    time.Time
		version string
	}{
		{"equity:XNAS:RECENT", now.AddDate(0, 0, -2), "llm-direction-v3"},
		{"equity:XNAS:LEGACY", now.AddDate(0, 0, -2), "deterministic-event-factor-v2"},
		{"equity:XNAS:EXPIRED", now.AddDate(0, 0, -31), "llm-direction-v3"},
		{"equity:XSHG:CN", now.AddDate(0, 0, -2), "llm-direction-v3"},
	} {
		if _, err = pool.Exec(ctx, `INSERT INTO recommendations(id,run_id,asset_id,score,rating,confidence,as_of,payload)
			VALUES($1,$2,$3,0,'watch',0,$4,json_build_object('scoring_version',$5))`, fmt.Sprintf("recommendation-%02d", index), fmt.Sprintf("run-%02d", index), values.assetID, values.asOf, values.version); err != nil {
			t.Fatal(err)
		}
	}
	items, err := (&masterdataRuntime{db: pool}).fundamentalRefreshCandidates(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].AssetID != "equity:XNAS:TRACKED" || items[0].SelectionReason != "explicit_forecast_or_rating" ||
		items[1].AssetID != "equity:XNAS:RECENT" || items[1].SelectionReason != "recent_completed_research" {
		t.Fatalf("unexpected fundamental refresh candidates: %#v", items)
	}
}
