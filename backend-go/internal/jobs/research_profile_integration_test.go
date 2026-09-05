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

func TestResearchProfileReclassificationAgainstPostgres(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
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

	now := time.Now().UTC()
	newsID, eventID, runID, jobID, runningJobID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.New(), uuid.New()
	setting, _ := json.Marshal(map[string]any{"enabled": true, "whitelist_keywords": []string{"NVIDIA"}, "blacklist_keywords": []string{"天气"}})
	if _, err := pool.Exec(ctx, `INSERT INTO integration_settings(key,payload,updated_at) VALUES('source-filter',$1,now()) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,updated_at=now()`, setting); err != nil {
		t.Fatal(err)
	}
	newsPayload, _ := json.Marshal(map[string]any{})
	if _, err := pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'test','professional','NVIDIA launches a new accelerator','summary',$2,'en',$3,$3,$3,$4,'[]',$5)`, newsID, "https://example.com/"+newsID, now, strings.Repeat("a", 32)+newsID[:32], newsPayload); err != nil {
		t.Fatal(err)
	}
	eventPayload, _ := json.Marshal(map[string]any{"id": eventID, "headline": "NVIDIA launches a new accelerator", "news_item_ids": []string{newsID}, "research_profile": "fast"})
	if _, err := pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,'NVIDIA launches a new accelerator','product_launch',$2,1,$3,$3,$3)`, eventID, eventPayload, now); err != nil {
		t.Fatal(err)
	}
	runPayload, _ := json.Marshal(map[string]any{"id": runID, "event_id": eventID, "status": "queued", "celery_task_id": jobID.String(), "research_profile": "fast"})
	if _, err := pool.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at) VALUES($1,$2,'queued',$3,$4,$4)`, runID, eventID, runPayload, now); err != nil {
		t.Fatal(err)
	}
	jobPayload, _ := json.Marshal(taskEnvelope{Args: []any{eventID, runID}, Kwargs: map[string]any{"research_profile": "fast"}})
	if _, err := pool.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,priority,max_attempts,available_at,created_at,updated_at) VALUES($1,'research',$2,$3,'queued',1,3,now(),now(),now())`, jobID, researchEventTask, jobPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,priority,max_attempts,available_at,created_at,updated_at) VALUES($1,'research',$2,$3,'running',1,3,now(),now(),now())`, runningJobID, researchEventTask, jobPayload); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=ANY($1); DELETE FROM event_research_runs WHERE id=$2; DELETE FROM news_events WHERE id=$3; DELETE FROM news_items WHERE id=$4`, []uuid.UUID{jobID, runningJobID}, runID, eventID, newsID)
	})

	updated, err := ReclassifyQueuedResearchProfiles(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if updated != 1 {
		t.Fatalf("updated=%d want 1", updated)
	}
	var jobProfile, runProfile, eventProfile string
	if err := pool.QueryRow(ctx, `SELECT payload->'kwargs'->>'research_profile' FROM go_jobs WHERE id=$1`, jobID).Scan(&jobProfile); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT payload->>'research_profile' FROM event_research_runs WHERE id=$1`, runID).Scan(&runProfile); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT payload->>'research_profile' FROM news_events WHERE id=$1`, eventID).Scan(&eventProfile); err != nil {
		t.Fatal(err)
	}
	if jobProfile != "deep" || runProfile != "deep" || eventProfile != "deep" {
		t.Fatalf("profiles job=%q run=%q event=%q", jobProfile, runProfile, eventProfile)
	}
	if err := pool.QueryRow(ctx, `SELECT payload->'kwargs'->>'research_profile' FROM go_jobs WHERE id=$1`, runningJobID).Scan(&jobProfile); err != nil {
		t.Fatal(err)
	}
	if jobProfile != "fast" {
		t.Fatalf("running task was modified: %q", jobProfile)
	}
}

func TestResearchRoutingStatePersistenceAgainstPostgres(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
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

	now := time.Now().UTC()
	eventID, runID, jobID := uuid.NewString(), uuid.New(), uuid.New()
	eventPayload, _ := json.Marshal(map[string]any{"id": eventID, "headline": "routing state test"})
	if _, err := pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,'routing state test','other',$2,1,$3,$3,$3)`, eventID, eventPayload, now); err != nil {
		t.Fatal(err)
	}
	runPayload, _ := json.Marshal(map[string]any{"id": runID.String(), "event_id": eventID, "status": "running", "research_profile": "fast", "escalated_to_deep": false, "waiting_for_deep_slot": false})
	if _, err := pool.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at) VALUES($1,$2,'running',$3,$4,$4)`, runID.String(), eventID, runPayload, now); err != nil {
		t.Fatal(err)
	}
	jobPayload, _ := json.Marshal(taskEnvelope{Args: []any{eventID, runID.String()}, Kwargs: map[string]any{"research_profile": "fast"}})
	if _, err := pool.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,priority,max_attempts,available_at,created_at,updated_at) VALUES($1,'research',$2,$3,'running',1,3,now(),now(),now())`, jobID, researchEventTask, jobPayload); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=$1; DELETE FROM event_research_runs WHERE id=$2; DELETE FROM news_events WHERE id=$3`, jobID, runID.String(), eventID)
	})

	runtime := &researchRuntime{db: pool}
	runtime.updateResearchRoutingState(ctx, runID, "event_research_run", true, true)
	var runWaiting, runEscalated, jobWaiting, jobEscalated bool
	if err := pool.QueryRow(ctx, `SELECT (payload->>'waiting_for_deep_slot')::boolean,(payload->>'escalated_to_deep')::boolean FROM event_research_runs WHERE id=$1`, runID.String()).Scan(&runWaiting, &runEscalated); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT (payload->'kwargs'->>'waiting_for_deep_slot')::boolean,(payload->'kwargs'->>'escalated_to_deep')::boolean FROM go_jobs WHERE id=$1`, jobID).Scan(&jobWaiting, &jobEscalated); err != nil {
		t.Fatal(err)
	}
	if !runWaiting || !runEscalated || !jobWaiting || !jobEscalated {
		t.Fatalf("routing state was not persisted: run=%v/%v job=%v/%v", runWaiting, runEscalated, jobWaiting, jobEscalated)
	}

	runtime.updateResearchRoutingState(ctx, runID, "event_research_run", false, false)
	if err := pool.QueryRow(ctx, `SELECT (payload->>'waiting_for_deep_slot')::boolean,(payload->>'escalated_to_deep')::boolean FROM event_research_runs WHERE id=$1`, runID.String()).Scan(&runWaiting, &runEscalated); err != nil {
		t.Fatal(err)
	}
	if runWaiting || !runEscalated {
		t.Fatalf("escalation must remain sticky after slot acquisition: waiting=%v escalated=%v", runWaiting, runEscalated)
	}
}
