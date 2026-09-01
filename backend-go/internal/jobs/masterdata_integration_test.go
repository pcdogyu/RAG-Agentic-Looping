package jobs

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

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
