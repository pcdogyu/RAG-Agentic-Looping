package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

func TestBatchThreeCommandsAgainstIsolatedServices(t *testing.T) {
	if os.Getenv("COMMAND_TEST_ISOLATED") != "1" {
		t.Skip("COMMAND_TEST_ISOLATED=1 is required")
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
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	redisClient := redis.NewClient(options)
	defer redisClient.Close()
	if err = redisClient.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ollama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tags" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"models": []map[string]string{
			{"name": "qwen2.5:3b"}, {"name": "qwen2.5:7b"}, {"name": "qwen2.5-coder:7b"},
		}})
	}))
	defer ollama.Close()
	for _, lane := range []string{"EXTRACT", "ASSIST", "RESEARCH", "CODE"} {
		t.Setenv("OLLAMA_"+lane+"_BASE_URLS", "")
		t.Setenv("OLLAMA_"+lane+"_BASE_URL", ollama.URL)
	}

	const adminToken = "isolated-command-test-token"
	server, err := New(config.Config{
		Environment:        "development",
		AdminAPIToken:      adminToken,
		ExtractModel:       "qwen2.5:3b",
		AssistModel:        "qwen2.5:7b",
		ResearchModel:      "qwen2.5:7b",
		CodeModel:          "qwen2.5-coder:7b",
		EvolutionEnabled:   true,
		ResearchCooldown:   0,
		ScanInterval:       10 * time.Minute,
		EvolutionAutoMerge: false,
	}, pool, redisClient)
	if err != nil {
		t.Fatal(err)
	}

	truncate := `TRUNCATE TABLE news_processing_outbox,news_processing,research_runs,event_research_runs,
		news_events,news_items,evolution_candidates,assets RESTART IDENTITY CASCADE`
	if _, err = pool.Exec(ctx, truncate); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), truncate)
		_ = redisClient.FlushDB(context.Background()).Err()
	})

	call := func(method, path, body string, admin bool) (int, map[string]any) {
		t.Helper()
		request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		if admin {
			request.Header.Set("X-Admin-Token", adminToken)
		}
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		payload := map[string]any{}
		if response.Body.Len() > 0 {
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatalf("%s %s returned invalid JSON: %s", method, path, response.Body.String())
			}
		}
		return response.Code, payload
	}
	assertStatus := func(want int, method, path, body string, admin bool) map[string]any {
		t.Helper()
		got, payload := call(method, path, body, admin)
		if got != want {
			t.Fatalf("%s %s status=%d want=%d payload=%v", method, path, got, want, payload)
		}
		return payload
	}

	assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/admin/asset-universe/refresh", `{}`, true)
	assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/admin/asset-universe/backfill?days=3", `{}`, true)
	goBackfill := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/admin/asset-universe/backfill?days=4", `{}`, true)
	var backfillQueue, backfillType string
	if err = pool.QueryRow(ctx, `SELECT queue,task_type FROM go_jobs WHERE id=$1`, stringValue(goBackfill["task_id"])).Scan(&backfillQueue, &backfillType); err != nil {
		t.Fatal(err)
	}
	if backfillQueue != "backfill" || backfillType != "market_loop.backfill_asset_mappings" {
		t.Fatalf("backfill was not routed to Go: queue=%s type=%s", backfillQueue, backfillType)
	}
	assertStatus(http.StatusOK, http.MethodPost, "/api/v1/scan", `{"background":true}`, false)
	assertStatus(http.StatusOK, http.MethodPost, "/api/v1/scan/pause", `{}`, false)
	assertStatus(http.StatusOK, http.MethodPost, "/api/v1/scan/resume", `{}`, false)

	const assetOne = "equity:NASDAQ:ISO1"
	const assetTwo = "equity:NASDAQ:ISO2"
	for _, asset := range []struct{ id, symbol string }{{assetOne, "ISO1"}, {assetTwo, "ISO2"}} {
		_, err = pool.Exec(ctx, `INSERT INTO assets(
			id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,
			sector_id,industry_id,raw_sector,raw_industry,instrument_type,lot_size,active
		) VALUES($1,'equity','US',$2,$2,'NASDAQ','USD','[]','[]','[]','','','','','stock',1,true)`, asset.id, asset.symbol)
		if err != nil {
			t.Fatal(err)
		}
	}

	research := assertStatus(http.StatusOK, http.MethodPost, "/api/v1/research", `{"asset_id":"`+assetOne+`","background":true}`, false)
	if stringValue(research["run_id"]) == "" || stringValue(research["task_id"]) == "" {
		t.Fatalf("research was not durably queued: %v", research)
	}

	const failedRunID = "10000000-0000-0000-0000-000000000001"
	failedPayload := map[string]any{
		"id": failedRunID, "event_id": nil, "asset": map[string]any{"asset_id": assetTwo},
		"status": "failed", "retry_of_run_id": nil, "retry_attempt": 0,
		"retryable_reason": nil, "analysis_steps": []any{}, "created_at": jsonTime(time.Now()), "updated_at": jsonTime(time.Now()),
	}
	failedBody, _ := json.Marshal(failedPayload)
	if _, err = pool.Exec(ctx, `INSERT INTO research_runs(id,event_id,asset_id,status,payload,created_at,updated_at) VALUES($1,NULL,$2,'failed',$3,now(),now())`, failedRunID, assetTwo, failedBody); err != nil {
		t.Fatal(err)
	}
	retry := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/research-runs/"+failedRunID+"/retry", `{}`, false)
	if stringValue(retry["retry_of_run_id"]) != failedRunID || int64Value(retry["retry_attempt"]) != 1 {
		t.Fatalf("research retry lineage is invalid: %v", retry)
	}

	const newsID = "20000000-0000-0000-0000-000000000001"
	if _, err = pool.Exec(ctx, `INSERT INTO news_items(
		id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata
	) VALUES($1,'isolated','professional','isolated retry','','https://example.test/news','en',now(),now(),now(),$2,'[]','{}')`, newsID, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	newsRetry := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/news/"+newsID+"/retry", `{}`, false)
	if stringValue(newsRetry["task_id"]) == "" {
		t.Fatalf("news retry was not queued: %v", newsRetry)
	}
	var processingStatus string
	if err = pool.QueryRow(ctx, `SELECT status FROM news_processing WHERE news_id=$1`, newsID).Scan(&processingStatus); err != nil || processingStatus != "queued" {
		t.Fatalf("news processing status=%q err=%v", processingStatus, err)
	}

	oldCodeTask := "30000000-0000-0000-0000-000000000001"
	snapshot, _ := json.Marshal(map[string]any{"queues": []any{map[string]any{
		"id": "code", "tasks": []any{map[string]any{
			"task_id": oldCodeTask, "kind": "code_evolution", "entity_id": nil,
			"instance_id": "code-0", "title": "isolated retry", "subtitle": "", "error": "boom",
		}}, "instances": []any{},
	}}})
	if err = redisClient.Set(ctx, modelQueueOverviewCacheKey, snapshot, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	codeRetry := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/model-queues/code/tasks/retry", `{"task_id":"`+oldCodeTask+`","kind":"code_evolution","instance_id":"code-0"}`, false)
	if int64Value(codeRetry["retried"]) != 1 {
		t.Fatalf("code retry was not queued: %v", codeRetry)
	}

	const mappingEventID = "35000000-0000-0000-0000-000000000001"
	if _, err = pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,'NVIDIA mapping retry','company','{}',0.8,now(),now(),now())`, mappingEventID); err != nil {
		t.Fatal(err)
	}
	failedMapping := map[string]any{
		"task_id": "35000000-0000-0000-0000-000000000002", "kind": "asset_mapping", "entity_id": mappingEventID,
		"instance_id": "assist-0", "title": "NVIDIA mapping retry", "subtitle": "company", "error": "boom",
	}
	assistSnapshot, _ := json.Marshal(map[string]any{"queues": []any{map[string]any{
		"id": "assist", "tasks": []any{failedMapping}, "instances": []any{map[string]any{"id": "assist-0", "tasks": []any{failedMapping}}},
	}}})
	if err = redisClient.Set(ctx, modelQueueOverviewCacheKey, assistSnapshot, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	assistRetry := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/model-queues/assist/tasks/retry", `{"task_id":"35000000-0000-0000-0000-000000000002","kind":"asset_mapping","entity_id":"`+mappingEventID+`","instance_id":"assist-0","filter_recent_research":false}`, false)
	if assistRetry["filter_recent_research"] != false {
		t.Fatalf("assist retry response did not preserve the bypass: %v", assistRetry)
	}
	var retryPayload []byte
	if err = pool.QueryRow(ctx, `SELECT payload::jsonb FROM go_jobs WHERE id=$1`, stringValue(anySlice(assistRetry["task_ids"])[0])).Scan(&retryPayload); err != nil {
		t.Fatal(err)
	}
	var retryEnvelope map[string]any
	if err = json.Unmarshal(retryPayload, &retryEnvelope); err != nil {
		t.Fatal(err)
	}
	kwargs := objectValue(retryEnvelope["kwargs"])
	if kwargs["force_mapping"] != true || kwargs["filter_recent_research"] != false {
		t.Fatalf("assist retry kwargs are invalid: %#v", kwargs)
	}

	const cancelEventID = "36000000-0000-0000-0000-000000000001"
	const cancelRunID = "36000000-0000-0000-0000-000000000002"
	const cancelTaskID = "36000000-0000-0000-0000-000000000003"
	cancelEventPayload := map[string]any{"id": cancelEventID, "headline": "decimal headline", "news_item_ids": []any{}}
	cancelEventBody, _ := json.Marshal(cancelEventPayload)
	if _, err = pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,'decimal headline','m_and_a',$2,0.8,now(),now(),now())`, cancelEventID, cancelEventBody); err != nil {
		t.Fatal(err)
	}
	cancelRunPayload := map[string]any{
		"id": cancelRunID, "event_id": cancelEventID, "status": "running", "celery_task_id": cancelTaskID,
		"model_instance_id": "research-0", "analysis_steps": []any{}, "created_at": jsonTime(time.Now()), "updated_at": jsonTime(time.Now()),
	}
	cancelRunBody, _ := json.Marshal(cancelRunPayload)
	if _, err = pool.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at) VALUES($1,$2,'running',$3,now(),now())`, cancelRunID, cancelEventID, cancelRunBody); err != nil {
		t.Fatal(err)
	}
	cancelJobBody, _ := json.Marshal(map[string]any{"args": []any{cancelEventID, cancelRunID}, "kwargs": map[string]any{"model_instance_id": "research-0"}})
	if _, err = pool.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,lease_owner,lease_until,heartbeat_at) VALUES($1,'research','market_loop.research_event',$2,'running','isolated-worker',now()+interval '1 minute',now())`, cancelTaskID, cancelJobBody); err != nil {
		t.Fatal(err)
	}
	trackedResearch, _ := json.Marshal(map[string]any{
		"task_id": cancelTaskID, "instance_id": "research-0", "kind": "event_research", "entity_id": cancelEventID,
		"status": "running", "updated_at": jsonTime(time.Now()),
	})
	if err = redisClient.HSet(ctx, "market-loop:model-queue:research:tasks", cancelTaskID, trackedResearch).Err(); err != nil {
		t.Fatal(err)
	}
	cancelResult := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/model-queues/research/tasks/cancel", `{"task_id":"`+cancelTaskID+`","kind":"event_research","entity_id":"`+cancelEventID+`"}`, false)
	if int64Value(cancelResult["cancelled"]) != 1 || int64Value(cancelResult["purged"]) != 1 {
		t.Fatalf("event research task was not cancelled by its durable task id: %v", cancelResult)
	}
	var cancelRunStatus, cancelPayloadStatus, cancelJobStatus string
	var cancelRequestedAt *time.Time
	if err = pool.QueryRow(ctx, `SELECT status,payload->>'status' FROM event_research_runs WHERE id=$1`, cancelRunID).Scan(&cancelRunStatus, &cancelPayloadStatus); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT status,cancel_requested_at FROM go_jobs WHERE id=$1`, cancelTaskID).Scan(&cancelJobStatus, &cancelRequestedAt); err != nil {
		t.Fatal(err)
	}
	if cancelRunStatus != "cancelled" || cancelPayloadStatus != "cancelled" || cancelJobStatus != "running" || cancelRequestedAt == nil {
		t.Fatalf("cancel state was not propagated: run=%q payload=%q job=%q requested_at=%v", cancelRunStatus, cancelPayloadStatus, cancelJobStatus, cancelRequestedAt)
	}
	trackedBody, err := redisClient.HGet(ctx, "market-loop:model-queue:research:tasks", cancelTaskID).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var trackedState map[string]any
	if err = json.Unmarshal(trackedBody, &trackedState); err != nil || stringValue(trackedState["status"]) != "cancelled" {
		t.Fatalf("tracked research task was not cancelled: state=%v err=%v", trackedState, err)
	}

	if err = redisClient.Set(ctx, modelQueueOverviewCacheKey, assistSnapshot, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	bulkRetry := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/model-queues/assist/instances/assist-0/retry", `{}`, false)
	if bulkRetry["filter_recent_research"] != true || int64Value(bulkRetry["retried"]) != 1 {
		t.Fatalf("assist bulk retry must default the filter on: %v", bulkRetry)
	}

	assistTask := "40000000-0000-0000-0000-000000000001"
	if err = redisClient.Del(ctx, "market-loop:model-queue:assist:tasks").Err(); err != nil {
		t.Fatal(err)
	}
	trackedAssist, _ := json.Marshal(map[string]any{
		"task_id": assistTask, "instance_id": "assist-0", "kind": "asset_mapping", "status": "queued",
		"updated_at": jsonTime(time.Now()),
	})
	if err = redisClient.HSet(ctx, "market-loop:model-queue:assist:tasks", assistTask, trackedAssist).Err(); err != nil {
		t.Fatal(err)
	}
	if err = redisClient.LPush(ctx, "mapping", "isolated-message").Err(); err != nil {
		t.Fatal(err)
	}
	assistClear := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/model-queues/assist/clear", `{}`, false)
	if int64Value(assistClear["cancelled"]) != 1 || int64Value(assistClear["purged"]) != 1 {
		t.Fatalf("assist queue was not isolated and cleared: %v", assistClear)
	}

	for _, queue := range []string{"extract", "research", "code"} {
		payload := assertStatus(http.StatusAccepted, http.MethodPost, "/api/v1/model-queues/"+queue+"/clear", `{}`, false)
		if payload["cancelled"] == nil {
			t.Fatalf("%s clear response is incomplete: %v", queue, payload)
		}
	}
	var activeResearch int
	if err = pool.QueryRow(ctx, `SELECT count(*)::int FROM research_runs WHERE status IN ('queued','running','verifying')`).Scan(&activeResearch); err != nil || activeResearch != 0 {
		t.Fatalf("active research remained after clear: count=%d err=%v", activeResearch, err)
	}
}
