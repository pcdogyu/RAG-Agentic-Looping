package jobs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
	"github.com/redis/go-redis/v9"
)

func TestMappingGoldenStateAgainstIsolatedServices(t *testing.T) {
	if os.Getenv("MAPPING_TEST_ISOLATED") != "1" {
		t.Skip("MAPPING_TEST_ISOLATED=1 is required")
	}
	databaseURL := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	redisURL := os.Getenv("TEST_REDIS_URL")
	if databaseURL == "" || redisURL == "" {
		t.Fatal("TEST_DATABASE_URL and TEST_REDIS_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	redisClient := redis.NewClient(options)
	defer redisClient.Close()
	if err := redisClient.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	assetID := "equity:NASDAQ:MAP-" + uuid.NewString()
	eventID, newsID, taskID := uuid.New(), uuid.New(), uuid.New()
	ollama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		_ = json.NewDecoder(r.Body).Decode(&request)
		if request["keep_alive"] != float64(-1) {
			t.Fatalf("keep_alive must be numeric: %#v", request["keep_alive"])
		}
		content, _ := json.Marshal(mappingOutput{Candidates: []mappingHint{{
			AssetID: assetID, SourceMention: "Isolated Mapping Corp", Name: "Isolated Mapping Corp",
			Symbol: "MAPX", Market: "US", AssetClass: "equity", Relationship: "entity",
			Confidence: .91, Rationale: "The issuer is explicitly named.",
		}}, IndustryIDs: []string{}, NoAssetReason: ""})
		_ = json.NewEncoder(w).Encode(map[string]any{"message": map[string]any{"content": string(content)}, "prompt_eval_count": 20, "eval_count": 10})
	}))
	defer ollama.Close()

	_, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,sector_id,industry_id,raw_sector,raw_industry,instrument_type,lot_size,active) VALUES($1,'equity','US','MAPX','Isolated Mapping Corp','NASDAQ','USD','[]','[]','[]','','','','','stock',1,true)`, assetID)
	if err != nil {
		t.Fatal(err)
	}
	contentHash := strings.ReplaceAll(newsID.String(), "-", "") + strings.Repeat("0", 32)
	_, err = pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'isolated','professional','Isolated Mapping Corp announces a product','The company is explicitly named.','https://example.test/mapping','en',now(),now(),now(),$2,'[]','{}')`, newsID, contentHash)
	if err != nil {
		t.Fatal(err)
	}
	event := map[string]any{"id": eventID.String(), "news_item_ids": []string{newsID.String()}, "headline": "Isolated Mapping Corp announces a product", "event_type": "product", "entities": []string{"Isolated Mapping Corp"}, "direct_impact": "A product was announced", "horizon_days": 90, "source_quality": "professional", "published_at": iso(time.Now()), "observed_at": iso(time.Now()), "as_of": iso(time.Now()), "candidates": []any{}, "industry_ids": []string{}, "novelty": .5, "priority": .5, "analysis_steps": []any{analysisStep("asset_mapping_queue", "queued", "go-worker", "queued", map[string]any{})}}
	body, _ := json.Marshal(event)
	_, err = pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,$2,'product',$3,.5,now(),now(),now())`, eventID, event["headline"], body)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM model_call_audits WHERE entity_type='news_event' AND entity_id=$1`, eventID.String())
		_, _ = pool.Exec(context.Background(), `DELETE FROM event_research_runs WHERE event_id=$1`, eventID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_events WHERE id=$1`, eventID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_items WHERE id=$1`, newsID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM assets WHERE id=$1`, assetID)
		_ = redisClient.FlushDB(context.Background()).Err()
	})

	runtime := &ExtractRuntime{cfg: config.Config{
		AssistModel: "qwen2.5:7b", AssistURLs: []string{ollama.URL}, ResearchURLs: []string{"http://research.invalid"},
		OllamaTimeout: 5 * time.Second, MappingContextLength: 8192, MappingMaxOutput: 1024,
		OllamaAssistThreads: 1, OllamaKeepAlive: "-1", AutoResearch: true,
	}, db: pool, redis: redisClient, client: ollama.Client()}
	payload, _ := json.Marshal(taskEnvelope{Args: []any{eventID.String()}, Kwargs: map[string]any{"model_instance_id": "assist-0"}})
	job := Job{ID: taskID, Queue: "assist", TaskType: mappingTask, Payload: payload, Attempt: 1, MaxAttempts: 3}
	result, err := runtime.resolveEventAssets(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if stringValue(objectValue(result)["status"]) != "event_research_queued" {
		t.Fatalf("unexpected result: %#v", result)
	}
	var stored []byte
	if err := pool.QueryRow(ctx, `SELECT payload::jsonb FROM news_events WHERE id=$1`, eventID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	var saved map[string]any
	_ = json.Unmarshal(stored, &saved)
	if len(anySlice(saved["candidates"])) != 1 || stringValue(latestAnalysisStep(saved, "asset_mapping_queue")["status"]) != "completed" {
		t.Fatalf("mapping state was not persisted: %#v", saved)
	}
	var auditCount, runCount int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM model_call_audits WHERE entity_type='news_event' AND entity_id=$1 AND operation='asset_mapping' AND status='completed'`, eventID.String()).Scan(&auditCount); err != nil || auditCount != 1 {
		t.Fatalf("audit count=%d err=%v", auditCount, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM event_research_runs WHERE event_id=$1`, eventID).Scan(&runCount); err != nil || runCount != 1 {
		t.Fatalf("run count=%d err=%v", runCount, err)
	}
	if _, err := runtime.resolveEventAssets(ctx, Job{ID: uuid.New(), Queue: "assist", TaskType: mappingTask, Payload: payload, Attempt: 1, MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM event_research_runs WHERE event_id=$1`, eventID).Scan(&runCount); err != nil || runCount != 1 {
		t.Fatalf("idempotent run count=%d err=%v", runCount, err)
	}
}

func TestFMPStockSymbolMapsOnlyCanonicalUSEquity(t *testing.T) {
	if os.Getenv("MAPPING_TEST_ISOLATED") != "1" {
		t.Skip("MAPPING_TEST_ISOLATED=1 is required")
	}
	databaseURL := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	ids := []string{"equity:NYSE:VRT-" + uuid.NewString(), "crypto:coingecko:vrt-" + uuid.NewString(), "equity:NASDAQ:REAL-" + uuid.NewString()}
	fixtures := []struct{ id, class, market, symbol, name, tier string }{
		{ids[0], "equity", "US", "VRT", "Vertiv Holdings Co", "standard"},
		{ids[1], "crypto", "CRYPTO", "VRT", "Venus Reward", "manual_only"},
		{ids[2], "equity", "US", "REAL", "Real", "standard"},
	}
	for _, item := range fixtures {
		if _, err := pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,sector_id,industry_id,raw_sector,raw_industry,instrument_type,association_tier,lot_size,active) VALUES($1,$2,$3,$4,$5,'test','USD','[]','[]','[]','','','','','stock',$6,1,true)`, item.id, item.class, item.market, item.symbol, item.name, item.tier); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM assets WHERE id=ANY($1)`, ids) })
	runtime := &ExtractRuntime{db: pool}
	values, err := runtime.matchAssets(ctx, newsRecord{Source: "FMP Stock News", Symbols: []string{"VRT"}, Title: "Vertiv vs Schneider Electric", Summary: "the real money gets made in the data center race"}, extractedEvent{})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || stringValue(objectValue(objectValue(values[0])["asset"])["asset_id"]) != ids[0] {
		t.Fatalf("FMP VRT mapping was not isolated to the US equity: %#v", values)
	}
}

func TestRecentResearchFilterAgainstPostgres(t *testing.T) {
	if os.Getenv("MAPPING_TEST_ISOLATED") != "1" {
		t.Skip("MAPPING_TEST_ISOLATED=1 is required")
	}
	databaseURL := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}

	suffix := uuid.NewString()
	asset47 := "equity:TEST:recent-completed-" + suffix
	asset48 := "equity:TEST:boundary-" + suffix
	assetInsufficient := "equity:TEST:recent-insufficient-" + suffix
	assetActive := "equity:TEST:active-" + suffix
	industryID := "industry:test:" + suffix
	eventRunID, eventID := uuid.NewString(), uuid.NewString()
	filterEventID, bypassEventID := uuid.NewString(), uuid.NewString()
	now := time.Now().UTC()
	insertRun := func(assetID, status string, completedAt time.Time) {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{"completed_at": iso(completedAt)})
		if _, err := pool.Exec(ctx, `INSERT INTO research_runs(id,asset_id,status,payload,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$5)`, uuid.NewString(), assetID, status, payload, completedAt); err != nil {
			t.Fatal(err)
		}
	}
	insertRun(asset47, "completed", now.Add(-47*time.Hour))
	insertRun(asset48, "completed", now.Add(-48*time.Hour))
	insertRun(assetInsufficient, "insufficient_evidence", now.Add(-47*time.Hour))
	insertRun(assetActive, "running", now)
	if _, err := pool.Exec(ctx, `INSERT INTO industries(id,parent_id,level,name_zh,name_en,aliases,active) VALUES($1,NULL,2,'测试半导体','Test Semiconductors','["芯片测试"]',true)`, industryID); err != nil {
		t.Fatal(err)
	}
	report, _ := json.Marshal(map[string]any{"report": map[string]any{"impacts": []any{map[string]any{"target_type": "sector", "target_name": "芯片测试"}}}})
	if _, err := pool.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at) VALUES($1,$2,'completed',$3,$4,$4)`, eventRunID, eventID, report, now.Add(-47*time.Hour)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM research_runs WHERE asset_id=ANY($1)`, []string{asset47, asset48, assetInsufficient, assetActive})
		_, _ = pool.Exec(context.Background(), `DELETE FROM event_research_runs WHERE id=$1`, eventRunID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_events WHERE id=ANY($1)`, []string{filterEventID, bypassEventID})
		_, _ = pool.Exec(context.Background(), `DELETE FROM industries WHERE id=$1`, industryID)
	})

	candidate := func(assetID, name, relationship string) map[string]any {
		return map[string]any{"relationship": relationship, "asset": map[string]any{"asset_id": assetID, "name": name, "symbol": name, "industry_id": industryID}}
	}
	event := map[string]any{
		"id": filterEventID, "headline": "mixed event", "event_type": "other", "priority": .5,
		"published_at": iso(now), "observed_at": iso(now), "as_of": iso(now), "analysis_steps": []any{},
		"candidates": []any{
			candidate(asset47, "RECENT47", "entity"),
			candidate(asset48, "BOUNDARY48", "entity"),
			candidate(assetInsufficient, "INSUFFICIENT47", "entity"),
			candidate(assetActive, "ACTIVE", "entity"),
		},
		"industry_ids": []any{industryID},
	}
	runtime := &ExtractRuntime{cfg: config.Config{RecentResearchFilter: 48 * time.Hour}, db: pool}
	if err := runtime.applyRecentResearchFilter(ctx, event, true); err != nil {
		t.Fatal(err)
	}
	remaining := anySlice(event["candidates"])
	if len(remaining) != 2 || stringValue(objectValue(objectValue(remaining[0])["asset"])["asset_id"]) != asset48 || stringValue(objectValue(objectValue(remaining[1])["asset"])["asset_id"]) != assetActive {
		t.Fatalf("47h terminal runs must be filtered while 48h and active runs remain: %#v", remaining)
	}
	if len(stringSlice(event["industry_ids"])) != 0 {
		t.Fatalf("recent canonical industry must be filtered: %#v", event["industry_ids"])
	}
	metadata := objectValue(event["recent_research_filter"])
	if len(anySlice(metadata["excluded_asset_ids"])) != 2 || len(anySlice(metadata["excluded_industry_ids"])) != 1 {
		t.Fatalf("filter audit metadata is incomplete: %#v", metadata)
	}

	bypassEvent := map[string]any{
		"id": bypassEventID, "headline": "manual retry", "event_type": "other", "priority": .5,
		"published_at": iso(now), "observed_at": iso(now), "as_of": iso(now), "analysis_steps": []any{},
		"candidates": []any{candidate(asset47, "RECENT47", "entity")}, "industry_ids": []any{industryID},
	}
	if err := runtime.applyRecentResearchFilter(ctx, bypassEvent, false); err != nil {
		t.Fatal(err)
	}
	if len(anySlice(bypassEvent["candidates"])) != 1 || stringValue(latestAnalysisStep(bypassEvent, "recent_research_filter")["status"]) != "bypassed" {
		t.Fatalf("manual bypass must preserve candidates and remain auditable: %#v", bypassEvent)
	}
	queued, err := (&researchRuntime{cfg: config.Config{ResearchCooldown: 48 * time.Hour}, db: pool}).enqueueAssetResearch(
		ctx,
		map[string]any{"id": uuid.NewString(), "headline": "active task guard"},
		objectValue(candidate(assetActive, "ACTIVE", "entity")["asset"]),
		true,
	)
	if err != nil || queued {
		t.Fatalf("manual bypass must not bypass an active research run: queued=%v err=%v", queued, err)
	}
}
