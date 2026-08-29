package jobs

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

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
	alive, err := store.Heartbeat(ctx, id, "test-worker", time.Minute)
	if err != nil || !alive {
		t.Fatalf("heartbeat alive=%v err=%v", alive, err)
	}
	if err := store.Complete(ctx, id, "test-worker", map[string]any{"done": true}); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM go_jobs WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("status=%s", status)
	}
}
