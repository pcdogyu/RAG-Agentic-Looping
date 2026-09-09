package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/consensus"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestConsensusSyncCapturesOnlyFirstObservedFMPHistoryAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "consensus_sync_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	assetID := "equity:XNAS:CONSENSUS"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
		VALUES($1,'equity','US','CONSENSUS','Consensus Test','NASDAQ','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apikey") != "isolated" || r.URL.Path != "/analyst-estimates" {
			t.Fatalf("unexpected FMP request: path=%s apikey=%q", r.URL.Path, r.Header.Get("apikey"))
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"symbol": "CONSENSUS", "date": "2027-12-31",
			"revenueLow": 90.0, "revenueAvg": 100.0, "revenueHigh": 110.0, "numAnalystsRevenue": 7.0,
			"epsLow": 1.0, "epsAvg": 2.0, "epsHigh": 3.0, "numAnalystsEps": 5.0,
		}})
	}))
	defer server.Close()
	before := time.Now().UTC()
	runtime := &masterdataRuntime{cfg: config.Config{FMPBaseURL: server.URL, FMPAccessToken: "isolated", FMPRateLimit: 100000}, db: pool, client: server.Client()}
	result, err := runtime.syncAssetConsensus(ctx, assetID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.(map[string]any)["historical_backfill"] != false || result.(map[string]any)["automatic_rating"] != false {
		t.Fatalf("unsafe sync controls: %#v", result)
	}
	after := time.Now().UTC()
	items, err := consensus.NewStore(pool).ListAvailable(ctx, assetID, after.Add(time.Second), 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 6 {
		t.Fatalf("got %d persisted estimates, want six", len(items))
	}
	repeated, err := runtime.syncAssetConsensus(ctx, assetID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.(map[string]any)["inserted"] != 0 {
		t.Fatalf("unchanged provider record created another revision: %#v", repeated)
	}
	var persistedCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM consensus_snapshots WHERE asset_id=$1`, assetID).Scan(&persistedCount); err != nil || persistedCount != 6 {
		t.Fatalf("idempotent observation count=%d err=%v", persistedCount, err)
	}
	for _, item := range items {
		if item.AvailableAt.Before(before) || item.AvailableAt.After(after) || item.SourceName != "FMP analyst estimates" || strings.Contains(item.SourceURL, "isolated") {
			t.Fatalf("invalid first-observed snapshot: %#v", item)
		}
		contract, ok := item.SourcePayload["observation_contract"].(map[string]any)
		if !ok || contract["historical_backfill"] != false || contract["provider_publication_time_available"] != false {
			t.Fatalf("missing observation contract: %#v", item.SourcePayload)
		}
	}
	prior, err := consensus.NewStore(pool).ListAvailable(ctx, assetID, before.Add(-time.Nanosecond), 20)
	if err != nil || len(prior) != 0 {
		t.Fatalf("first observation leaked into history: count=%d err=%v", len(prior), err)
	}
	var ratingCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM fundamental_rating_states WHERE asset_id=$1`, assetID).Scan(&ratingCount); err != nil || ratingCount != 0 {
		t.Fatalf("consensus sync created a rating: count=%d err=%v", ratingCount, err)
	}
}

func TestManualMarketPriceSyncPersistsOnlyProviderObservationsAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "market_price_sync_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	const assetID = "equity:NASDAQ:PRICE"
	const cnAssetID = "equity:XSHG:600000"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
		VALUES($1,'equity','US','PRICE','Price Test','NASDAQ','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
		VALUES($1,'equity','CN','600000','CN Price Test','XSHG','CNY','[]','[]','[]',100,true)`, cnAssetID); err != nil {
		t.Fatal(err)
	}
	prior, earlier := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02"), time.Now().UTC().AddDate(0, 0, -2).Format("2006-01-02")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/prices" {
			request := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request["symbol"] != "600000" || request["market"] != "CN" {
				t.Errorf("unexpected CN price request: request=%#v err=%v", request, err)
				http.Error(w, "unexpected request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
				"date": prior, "adjusted_close": 9.35, "price_field": "adjusted_close",
				"source_name": "Tencent Finance", "source_url": "https://example.test/newfqkline/get?token=secret", "source_document_id": "tencent-kline:sh600000",
			}}})
			return
		}
		if r.Header.Get("apikey") != "isolated-price" || r.URL.Path != "/historical-price-eod/dividend-adjusted" || r.URL.Query().Get("symbol") != "PRICE" {
			t.Errorf("unexpected FMP price request: path=%s symbol=%q apikey=%q", r.URL.Path, r.URL.Query().Get("symbol"), r.Header.Get("apikey"))
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"date": earlier, "close": 48.0, "adjClose": 49.0},
			{"date": prior, "close": 49.0, "adjClose": 50.0},
		})
	}))
	defer server.Close()
	payload, _ := json.Marshal(taskEnvelope{Args: []any{assetID}, Kwargs: map[string]any{"asset_id": assetID, "lookback_days": 14}})
	runtime := &masterdataRuntime{cfg: config.Config{FMPBaseURL: server.URL, FMPAccessToken: "isolated-price", FMPRateLimit: 100000, MarketAdapterURL: server.URL}, db: pool, client: server.Client()}
	before := time.Now().UTC()
	result, err := runtime.syncMarketPriceObservations(ctx, Job{ID: uuid.New(), Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	values := result.(map[string]any)
	if values["status"] != "completed" || values["adjusted_close_received"] != 2 || values["inserted"] != 2 || values["automatic_assumptions"] != false || values["automatic_rating"] != false {
		t.Fatalf("unsafe or incomplete price sync result: %#v", values)
	}
	var count int
	var field, sourceName, sourceURL string
	var firstAvailable time.Time
	if err = pool.QueryRow(ctx, `SELECT count(*),min(price_field),min(source_name),min(source_url),min(available_at) FROM market_price_observations WHERE asset_id=$1`, assetID).Scan(&count, &field, &sourceName, &sourceURL, &firstAvailable); err != nil {
		t.Fatal(err)
	}
	if count != 2 || field != "adjusted_close" || sourceName != "FMP" || strings.Contains(sourceURL, "isolated-price") || firstAvailable.Before(before) {
		t.Fatalf("invalid persisted prices: count=%d field=%s source=%s url=%s available=%s", count, field, sourceName, sourceURL, firstAvailable)
	}
	repeated, err := runtime.syncMarketPriceObservations(ctx, Job{ID: uuid.New(), Payload: payload})
	if err != nil || repeated.(map[string]any)["inserted"] != 0 {
		t.Fatalf("price sync was not idempotent: result=%#v err=%v", repeated, err)
	}
	cnPayload, _ := json.Marshal(taskEnvelope{Args: []any{cnAssetID}, Kwargs: map[string]any{"asset_id": cnAssetID, "lookback_days": 14}})
	cnResult, err := runtime.syncMarketPriceObservations(ctx, Job{ID: uuid.New(), Payload: cnPayload})
	if err != nil || cnResult.(map[string]any)["adjusted_close_received"] != 1 || cnResult.(map[string]any)["inserted"] != 1 {
		t.Fatalf("CN adjusted price sync failed: result=%#v err=%v", cnResult, err)
	}
	var cnField, cnSourceName, cnSourceURL, cnSourceID string
	if err = pool.QueryRow(ctx, `SELECT price_field,source_name,source_url,source_document_id FROM market_price_observations WHERE asset_id=$1`, cnAssetID).Scan(&cnField, &cnSourceName, &cnSourceURL, &cnSourceID); err != nil {
		t.Fatal(err)
	}
	if cnField != "adjusted_close" || cnSourceName != "Tencent Finance" || cnSourceURL != "https://example.test/newfqkline/get" || cnSourceID != "tencent-kline:sh600000" {
		t.Fatalf("CN price lineage is invalid: field=%s source=%s url=%s document=%s", cnField, cnSourceName, cnSourceURL, cnSourceID)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM forecast_versions WHERE asset_id=$1`, assetID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("price sync created forecast data: count=%d err=%v", count, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM fundamental_rating_states WHERE asset_id=$1`, assetID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("price sync created a rating: count=%d err=%v", count, err)
	}
}

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
			VALUES($1,$2,$3,0,'watch',0,$4,json_build_object('scoring_version',$5::text))`, fmt.Sprintf("recommendation-%02d", index), fmt.Sprintf("run-%02d", index), values.assetID, values.asOf, values.version); err != nil {
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
