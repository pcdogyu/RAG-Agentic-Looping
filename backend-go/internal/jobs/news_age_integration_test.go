package jobs

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestDiscardExpiredNewsAcrossQueuesAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	oldNews, freshNews := uuid.NewString(), uuid.NewString()
	oldEvent, freshEvent := uuid.NewString(), uuid.NewString()
	eventRun, assetRun := uuid.NewString(), uuid.NewString()
	ids := make([]uuid.UUID, 0)
	defer func() {
		clean := context.Background()
		_, _ = pool.Exec(clean, `DELETE FROM go_jobs WHERE id=ANY($1)`, ids)
		_, _ = pool.Exec(clean, `DELETE FROM research_runs WHERE id=$1`, assetRun)
		_, _ = pool.Exec(clean, `DELETE FROM event_research_runs WHERE id=$1`, eventRun)
		_, _ = pool.Exec(clean, `DELETE FROM news_processing_outbox WHERE news_id=$1`, oldNews)
		_, _ = pool.Exec(clean, `DELETE FROM news_processing WHERE news_id=$1`, oldNews)
		_, _ = pool.Exec(clean, `DELETE FROM news_events WHERE id=ANY($1)`, []string{oldEvent, freshEvent})
		_, _ = pool.Exec(clean, `DELETE FROM news_items WHERE id=ANY($1)`, []string{oldNews, freshNews})
	}()
	for index, id := range []string{oldNews, freshNews} {
		published := now.Add(-49 * time.Hour)
		if index == 1 {
			published = now.Add(-47 * time.Hour)
		}
		_, err = pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'test','professional','age test','summary',$2,'en',$3,$3,$3,$4,'[]','{}')`, id, "https://example.test/"+id, published, strings.ReplaceAll(id, "-", ""))
		if err != nil {
			t.Fatal(err)
		}
		eventID := oldEvent
		if index == 1 {
			eventID = freshEvent
		}
		payload, _ := json.Marshal(map[string]any{"id": eventID, "published_at": published.Format(time.RFC3339Nano)})
		_, err = pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,'age test','other',$2,0.5,$3,$3,$3)`, eventID, payload, published)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, run := range []struct{ table, id string }{{"event_research_runs", eventRun}, {"research_runs", assetRun}} {
		payload, _ := json.Marshal(map[string]any{"id": run.id, "event_id": oldEvent, "status": "queued", "news_age_filter_bypass": true})
		if run.table == "research_runs" {
			_, err = pool.Exec(ctx, `INSERT INTO research_runs(id,event_id,asset_id,status,payload,created_at,updated_at) VALUES($1,$2,'equity:NYSE:TEST','queued',$3,now(),now())`, run.id, oldEvent, payload)
		} else {
			_, err = pool.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at) VALUES($1,$2,'queued',$3,now(),now())`, run.id, oldEvent, payload)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = pool.Exec(ctx, `INSERT INTO news_processing(news_id,status,attempt_count,created_at,updated_at) VALUES($1,'queued',0,now(),now())`, oldNews)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO news_processing_outbox(id,news_id,status,force_asset_mapping,dispatch_attempts,available_at,created_at,updated_at) VALUES($1,$2,'pending',false,0,now(),now(),now())`, uuid.NewString(), oldNews)
	if err != nil {
		t.Fatal(err)
	}
	jobs := []struct {
		queue, kind string
		args        []any
		expired     bool
	}{
		{"extract", extractTask, []any{"scan", oldNews}, true},
		{"extract", retryNewsTask, []any{oldNews}, true},
		{"extract", reextractTask, []any{oldEvent, eventRun}, true},
		{"assist", mappingTask, []any{oldEvent}, true},
		{"research", researchEventTask, []any{oldEvent, eventRun}, true},
		{"research", researchAssetTask, []any{"equity:NYSE:TEST", oldEvent, assetRun}, true},
		{"extract", retryNewsTask, []any{freshNews}, false},
		{"assist", mappingTask, []any{freshEvent}, false},
		{"research", researchEventTask, []any{freshEvent, uuid.NewString()}, false},
		{"code", evolveFailuresTask, []any{oldEvent}, false},
	}
	for _, item := range jobs {
		id := uuid.New()
		ids = append(ids, id)
		payload, _ := json.Marshal(taskEnvelope{Args: item.args, Kwargs: map[string]any{"source": "manual", "news_age_filter_bypass": true}})
		_, err = pool.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,created_at,updated_at) VALUES($1,$2,$3,$4,'queued',now(),now())`, id, item.queue, item.kind, payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	// A running task is not swept out from under a worker, but the worker
	// rejects it before invoking its handler even if it was manually retried.
	claimedID := uuid.New()
	ids = append(ids, claimedID)
	claimedPayload, _ := json.Marshal(taskEnvelope{Args: []any{oldEvent}, Kwargs: map[string]any{"source": "manual", "news_age_filter_bypass": true}})
	_, err = pool.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,lease_owner,created_at,updated_at) VALUES($1,'assist',$2,$3,'running','age-test-worker',now(),now())`, claimedID, mappingTask, claimedPayload)
	if err != nil {
		t.Fatal(err)
	}
	count, err := DiscardExpiredNewsJobs(ctx, pool, 500)
	if err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Fatalf("filtered=%d want 6", count)
	}
	for index, item := range jobs {
		var status, reason string
		err = pool.QueryRow(ctx, `SELECT status,coalesce(result->>'reason','') FROM go_jobs WHERE id=$1`, ids[index]).Scan(&status, &reason)
		if err != nil {
			t.Fatal(err)
		}
		if item.expired && (status != "cancelled" || reason != "news_age_filtered") {
			t.Fatalf("%s: status=%s reason=%s", item.kind, status, reason)
		}
		if !item.expired && status != "queued" {
			t.Fatalf("fresh/unrelated %s status=%s", item.kind, status)
		}
	}
	for _, run := range []struct{ table, id string }{{"event_research_runs", eventRun}, {"research_runs", assetRun}} {
		var status string
		err = pool.QueryRow(ctx, `SELECT status FROM `+run.table+` WHERE id=$1`, run.id).Scan(&status)
		if err != nil || status != "filtered" {
			t.Fatalf("%s status=%s err=%v", run.table, status, err)
		}
	}
	filtered, err := DiscardExpiredClaimedNews(ctx, pool, Job{ID: claimedID, Queue: "assist", TaskType: mappingTask, Payload: claimedPayload})
	if err != nil || !filtered {
		t.Fatalf("claimed old news filtered=%t err=%v", filtered, err)
	}
	if err := NewStore(pool).CompleteCancellation(ctx, claimedID, "age-test-worker"); err != nil {
		t.Fatal(err)
	}
	var claimedStatus, claimedReason string
	if err := pool.QueryRow(ctx, `SELECT status,result->>'reason' FROM go_jobs WHERE id=$1`, claimedID).Scan(&claimedStatus, &claimedReason); err != nil || claimedStatus != "cancelled" || claimedReason != "news_age_filtered" {
		t.Fatalf("claimed status=%s reason=%s err=%v", claimedStatus, claimedReason, err)
	}
	count, err = DiscardExpiredNewsOutbox(ctx, pool)
	if err != nil || count != 1 {
		t.Fatalf("outbox filtered=%d err=%v", count, err)
	}
	var status string
	if err = pool.QueryRow(ctx, `SELECT status FROM news_processing_outbox WHERE news_id=$1`, oldNews).Scan(&status); err != nil || status != "cancelled" {
		t.Fatalf("outbox status=%s err=%v", status, err)
	}
}
