package jobs

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestStoreLifecycleAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	id, err := store.Enqueue(ctx, EnqueueParams{Queue: "test", TaskType: "contract.test", Payload: map[string]any{"ok": true}, Priority: 9, DedupeKey: "integration-lifecycle", MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=$1`, id) }()
	job, err := store.Claim(ctx, "test-worker", []string{"test"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != id || job.Attempt != 1 {
		t.Fatalf("unexpected claimed job: %#v", job)
	}
	time.Sleep(5 * time.Millisecond)
	alive, err := store.Heartbeat(ctx, id, "test-worker", time.Minute)
	if err != nil || !alive {
		t.Fatalf("heartbeat alive=%v err=%v", alive, err)
	}
	if err := store.Complete(ctx, id, "test-worker", map[string]any{"done": true}); err != nil {
		t.Fatal(err)
	}
	var status string
	var startedAt *time.Time
	var executionDuration int64
	if err := pool.QueryRow(ctx, `SELECT status,started_at,execution_duration_ms FROM go_jobs WHERE id=$1`, id).Scan(&status, &startedAt, &executionDuration); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || startedAt == nil || executionDuration <= 0 {
		t.Fatalf("status=%s started_at=%v execution_duration_ms=%d", status, startedAt, executionDuration)
	}
}

func TestClaimResearchUsesFastAndDeepPreferredLanesAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	queue := "rt-" + uuid.NewString()[:8]
	fastID, err := store.Enqueue(ctx, EnqueueParams{Queue: queue, TaskType: researchEventTask, Payload: taskEnvelope{Args: []any{"event", "run"}, Kwargs: map[string]any{"research_profile": "fast"}}, Priority: 1, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	deepID, err := store.Enqueue(ctx, EnqueueParams{Queue: queue, TaskType: researchEventTask, Payload: taskEnvelope{Args: []any{"event", "run"}, Kwargs: map[string]any{"research_profile": "deep"}}, Priority: 1, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=ANY($1)`, []uuid.UUID{fastID, deepID})
	}()
	fast, err := store.ClaimResearch(ctx, "fast-worker", []string{queue}, time.Minute, "fast")
	if err != nil || fast.ID != fastID {
		t.Fatalf("fast lane claimed %#v: %v", fast, err)
	}
	deep, err := store.ClaimResearch(ctx, "deep-worker", []string{queue}, time.Minute, "preferred")
	if err != nil || deep.ID != deepID {
		t.Fatalf("preferred lane claimed %#v: %v", deep, err)
	}
}

func TestStoreCompletionClearsRetryError(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	id, err := store.Enqueue(ctx, EnqueueParams{Queue: "test", TaskType: "contract.retry", Payload: map[string]any{}, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=$1`, id) }()
	job, err := store.Claim(ctx, "retry-worker", []string{"test"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(ctx, job, "retry-worker", errors.New("temporary failure")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE go_jobs SET available_at=now() WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	job, err = store.Claim(ctx, "retry-worker", []string{"test"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, id, "retry-worker", map[string]any{"done": true}); err != nil {
		t.Fatal(err)
	}
	var status string
	var errorValue *string
	if err := pool.QueryRow(ctx, `SELECT status,error FROM go_jobs WHERE id=$1`, id).Scan(&status, &errorValue); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || errorValue != nil {
		t.Fatalf("status=%s error=%v", status, errorValue)
	}
}

func TestStoreContinuationPreservesRetryBudgetAndProgress(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	id, err := store.Enqueue(ctx, EnqueueParams{Queue: "test", TaskType: backfillAssetMappingsTask, Payload: map[string]any{}, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=$1`, id) }()
	job, err := store.Claim(ctx, "continuation-worker", []string{"test"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Continue(ctx, id, "continuation-worker", map[string]any{"cursor": "next"}, map[string]any{"phase": "dispatching"}, 0); err != nil {
		t.Fatal(err)
	}
	job, err = store.Claim(ctx, "continuation-worker", []string{"test"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.Attempt != 1 || !strings.Contains(string(job.Payload), "next") {
		t.Fatalf("continuation consumed retry budget or lost payload: %#v", job)
	}
}

func TestStoreEnqueueReturnsExistingActiveDedupeJob(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	key := "integration-dedupe-" + time.Now().UTC().Format("20060102150405.000000000")
	first, err := store.Enqueue(ctx, EnqueueParams{Queue: "extract", TaskType: retryNewsTask, Payload: map[string]any{"args": []any{"one"}}, DedupeKey: key})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=$1`, first) }()
	second, err := store.Enqueue(ctx, EnqueueParams{Queue: "extract", TaskType: retryNewsTask, Payload: map[string]any{"args": []any{"two"}}, DedupeKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("dedupe returned %s, want %s", second, first)
	}
}

func TestStoreClaimsSmallerPriorityFirst(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	dsn = strings.Replace(dsn, "postgresql+psycopg://", "postgresql://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	queue := "priority-" + time.Now().UTC().Format("150405.000000000")
	low, err := store.Enqueue(ctx, EnqueueParams{Queue: queue, TaskType: "contract.low", Payload: map[string]any{}, Priority: 5})
	if err != nil {
		t.Fatal(err)
	}
	high, err := store.Enqueue(ctx, EnqueueParams{Queue: queue, TaskType: "contract.high", Payload: map[string]any{}, Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM go_jobs WHERE id=ANY($1)`, []uuid.UUID{low, high})
	}()
	job, err := store.Claim(ctx, "priority-worker", []string{queue}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != high {
		t.Fatalf("claimed priority %d job %s, want priority 1 job %s", job.Priority, job.ID, high)
	}
}
