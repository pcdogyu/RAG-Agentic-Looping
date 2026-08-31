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

func TestExtractRetryGoldenStateAgainstIsolatedServices(t *testing.T) {
	if os.Getenv("EXTRACT_TEST_ISOLATED") != "1" {
		t.Skip("EXTRACT_TEST_ISOLATED=1 is required")
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

	ollama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			http.NotFound(w, r)
			return
		}
		content, _ := json.Marshal(extractedEvent{
			EventType: "earnings", Entities: []string{"ISOX"}, DirectImpact: "Revenue increased",
			HorizonDays: 730, Novelty: .7, Priority: .8,
		})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message":           map[string]any{"content": string(content)},
			"prompt_eval_count": 12, "eval_count": 8,
		})
	}))
	defer ollama.Close()

	newsID, taskID := uuid.New(), uuid.New()
	assetID := "equity:NASDAQ:ISOX-" + newsID.String()
	contentHash := strings.ReplaceAll(newsID.String(), "-", "") + strings.Repeat("0", 32)
	_, err = pool.Exec(ctx, `INSERT INTO assets(
		id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,
		sector_id,industry_id,raw_sector,raw_industry,instrument_type,lot_size,active
	) VALUES($1,'equity','US','ISOX','Isolated Extract Corp','NASDAQ','USD','[]','[]','[]','','','','','stock',1,true)`, assetID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO news_items(
		id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata
	) VALUES($1,'isolated','professional','Isolated Extract Corp (ISOX) reports earnings','Revenue increased','https://example.test/extract','en',now(),now(),now(),$2,'["ISOX"]','{}')`, newsID, contentHash)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO news_processing(news_id,status,attempt_count,created_at,updated_at) VALUES($1,'queued',0,now(),now())`, newsID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM model_call_audits WHERE entity_type='news_item' AND entity_id=$1`, newsID.String())
		_, _ = pool.Exec(context.Background(), `DELETE FROM event_research_runs WHERE event_id IN (SELECT id FROM news_events WHERE payload::jsonb->'news_item_ids' ? $1)`, newsID.String())
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_events WHERE payload::jsonb->'news_item_ids' ? $1`, newsID.String())
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_processing WHERE news_id=$1`, newsID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_items WHERE id=$1`, newsID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM assets WHERE id=$1`, assetID)
		_ = redisClient.FlushDB(context.Background()).Err()
	})

	runtime := &ExtractRuntime{
		cfg: config.Config{
			ExtractModel: "qwen2.5:3b", ExtractURLs: []string{ollama.URL}, OllamaTimeout: 5 * time.Second,
			OllamaContextLength: 8192, OllamaMaxOutput: 4096, OllamaExtractThreads: 1,
			EventClusterWindow: 72 * time.Hour, AutoResearch: false,
		},
		db: pool, redis: redisClient, client: ollama.Client(),
	}
	payload, _ := json.Marshal(taskEnvelope{Args: []any{newsID.String()}, Kwargs: map[string]any{"model_instance_id": "extract-0"}})
	job := Job{ID: taskID, Queue: "extract", TaskType: retryNewsTask, Payload: payload, Attempt: 1, MaxAttempts: 3}
	result, err := runtime.retryNewsItem(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if stringValue(objectValue(result)["status"]) != "completed" {
		t.Fatalf("unexpected result: %#v", result)
	}
	var processingStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM news_processing WHERE news_id=$1`, newsID).Scan(&processingStatus); err != nil || processingStatus != "completed" {
		t.Fatalf("processing status=%q err=%v", processingStatus, err)
	}
	var eventCount, auditCount int
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM news_events WHERE payload::jsonb->'news_item_ids' ? $1 AND payload::jsonb->>'horizon_days'='30'`, newsID.String()).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("event count=%d err=%v", eventCount, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM model_call_audits WHERE entity_type='news_item' AND entity_id=$1 AND status='completed'`, newsID.String()).Scan(&auditCount); err != nil || auditCount != 1 {
		t.Fatalf("audit count=%d err=%v", auditCount, err)
	}

	second := job
	second.ID = uuid.New()
	if _, err := runtime.retryNewsItem(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM news_events WHERE payload::jsonb->'news_item_ids' ? $1`, newsID.String()).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("idempotent event count=%d err=%v", eventCount, err)
	}
}
