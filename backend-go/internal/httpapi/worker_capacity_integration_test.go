package httpapi

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestLiveWorkerCapacitiesOnlyCountsFreshHeartbeats(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "worker_capacity_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	server := &Server{db: pool}
	if empty := server.liveWorkerCapacities(ctx); len(empty) != 0 {
		t.Fatalf("empty worker registry must advertise nothing, got %#v", empty)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO go_worker_instances(id,queues,concurrency,active_jobs,heartbeat_at)
		VALUES('test-extract-live','["extract"]'::jsonb,4,1,now()),
		      ('test-extract-stale','["extract"]'::jsonb,8,0,now()-interval '10 minutes'),
		      ('test-shared','["extract","assist"]'::jsonb,2,0,now())`); err != nil {
		t.Fatal(err)
	}
	capacities := server.liveWorkerCapacities(ctx)
	if capacities["extract"] != 6 || capacities["assist"] != 2 {
		t.Fatalf("unexpected advertised capacities: %#v", capacities)
	}
	if got := resolveLaneCapacity(capacities, "extract", 1); got != 6 {
		t.Fatalf("queue overview must report the advertised worker capacity 6, got %d", got)
	}
}
