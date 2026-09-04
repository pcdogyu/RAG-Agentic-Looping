package jobs

import (
	"context"
	"encoding/json"
	"errors"
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

func TestMaintenanceHandlersAgainstPostgres(t *testing.T) {
	dsn, redisURL := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_URL")
	if dsn == "" || redisURL == "" {
		t.Skip("TEST_DATABASE_URL and TEST_REDIS_URL are required")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
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

	assetID := "equity:XNAS:MAINT" + strings.ToUpper(uuid.NewString()[:6])
	eventIDs := []uuid.UUID{uuid.New(), uuid.New()}
	runIDs := []uuid.UUID{uuid.New(), uuid.New()}
	replayEventID, replayRunID := uuid.New(), uuid.New()
	queuedTaskID := ""
	t.Cleanup(func() {
		if queuedTaskID != "" {
			_, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=$1`, queuedTaskID)
			_ = redisClient.HDel(context.Background(), "market-loop:model-queue:research:tasks", queuedTaskID).Err()
		}
		_, _ = pool.Exec(context.Background(), `DELETE FROM event_research_runs WHERE id=$1`, replayRunID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_events WHERE id=$1`, replayEventID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM research_runs WHERE id=ANY($1)`, runIDs)
		_, _ = pool.Exec(context.Background(), `DELETE FROM assets WHERE id=$1`, assetID)
	})
	if _, err := pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,sector_id,industry_id,raw_sector,raw_industry,instrument_type,lot_size,active)
		VALUES($1,'equity','US','MAINT','Maintenance Test','XNAS','USD','[]','[]','[]','','','','','common_stock',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for index := range runIDs {
		payload := map[string]any{"id": runIDs[index], "asset": map[string]any{"asset_id": assetID}, "status": "queued", "trigger_event_ids": []string{eventIDs[index].String()}, "historical_replay": false, "retry_of_run_id": nil, "analysis_steps": []any{}, "created_at": iso(now.Add(time.Duration(index) * time.Hour)), "updated_at": iso(now)}
		body, _ := json.Marshal(payload)
		if _, err := pool.Exec(ctx, `INSERT INTO research_runs(id,event_id,asset_id,status,payload,created_at,updated_at) VALUES($1,NULL,$2,'queued',$3,$4,$4)`, runIDs[index], assetID, body, now.Add(time.Duration(index)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	runtime := &maintenanceRuntime{cfg: config.Config{ResearchCoalesce: 24 * time.Hour, ResearchURLs: []string{"http://research.test"}}, db: pool, redis: redisClient}
	compactPayload, _ := json.Marshal(taskEnvelope{Kwargs: map[string]any{"dry_run": false}})
	result, err := runtime.compactResearchBacklog(ctx, Job{Payload: compactPayload})
	if err != nil {
		t.Fatal(err)
	}
	if int(numberValue(objectValue(result)["coalesced"])) != 1 {
		t.Fatalf("unexpected compaction result: %v", result)
	}
	var duplicateStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM research_runs WHERE id=$1`, runIDs[1]).Scan(&duplicateStatus); err != nil || duplicateStatus != "coalesced" {
		t.Fatalf("duplicate status=%s err=%v", duplicateStatus, err)
	}

	replayTime := time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	event := map[string]any{"id": replayEventID, "headline": "Maintenance replay", "event_type": "other", "news_item_ids": []any{}, "actions": []any{map[string]any{"id": uuid.NewString(), "actor": "test", "action_type": "statement", "action_stage": "statement", "action": "test", "strength": .1}}, "as_of": iso(replayTime), "published_at": iso(replayTime), "observed_at": iso(replayTime), "priority": .5, "analysis_steps": []any{}}
	eventBody, _ := json.Marshal(event)
	if _, err := pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,$2,'other',$3,.5,$4,$4,$4)`, replayEventID, event["headline"], eventBody, replayTime); err != nil {
		t.Fatal(err)
	}
	run := map[string]any{"id": replayRunID, "event_id": replayEventID, "status": "completed", "as_of": iso(now), "historical_replay": false, "report": map[string]any{"scoring_version": "target-transmission-v2"}, "report_history": []any{}, "analysis_steps": []any{}, "created_at": iso(now), "updated_at": iso(now)}
	runBody, _ := json.Marshal(run)
	if _, err := pool.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at) VALUES($1,$2,'completed',$3,$4,$4)`, replayRunID, replayEventID, runBody, now); err != nil {
		t.Fatal(err)
	}
	replayPayload, _ := json.Marshal(taskEnvelope{Kwargs: map[string]any{"batch_size": 1, "max_active": 10}})
	_, err = runtime.reprocessTargetImpacts(ctx, Job{ID: uuid.New(), Payload: replayPayload})
	var continuation *continuationError
	if !errors.As(err, &continuation) {
		t.Fatalf("replay did not continue after queueing a batch: %v", err)
	}
	var replayStatus, taskID string
	if err := pool.QueryRow(ctx, `SELECT status,payload->>'celery_task_id' FROM event_research_runs WHERE id=$1`, replayRunID).Scan(&replayStatus, &taskID); err != nil || replayStatus != "queued" {
		t.Fatalf("replay status=%s task=%s err=%v progress=%#v", replayStatus, taskID, err, continuation.Progress)
	}
	queuedTaskID = taskID
	var taskType string
	if err := pool.QueryRow(ctx, `SELECT task_type FROM go_jobs WHERE id=$1`, taskID).Scan(&taskType); err != nil || taskType != researchEventTask {
		t.Fatalf("replay task type=%s err=%v", taskType, err)
	}

}
