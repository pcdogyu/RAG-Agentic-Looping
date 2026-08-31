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
