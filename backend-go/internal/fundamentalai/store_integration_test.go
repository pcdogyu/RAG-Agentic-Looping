package fundamentalai

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

func TestStoreUpdateRunUsesOneExplicitStatusType(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "fundamental_ai_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	const assetID = "equity:NASDAQ:AIUP"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES($1,'equity','US','AIUP','AI Update','NASDAQ','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	run := Run{ID: uuid.New(), AssetID: assetID, TaskID: uuid.New(), ModelVersion: "test-model"}
	created, wasCreated, err := NewStore(pool).CreateRun(ctx, run, "update-run-"+run.ID.String())
	if err != nil || !wasCreated {
		t.Fatalf("created=%#v wasCreated=%v err=%v", created, wasCreated, err)
	}
	if err = NewStore(pool).UpdateRun(ctx, run.ID, "running", "data_sync", map[string]any{"step": 1}, []string{}); err != nil {
		t.Fatalf("update running state: %v", err)
	}
	if err = NewStore(pool).UpdateRun(ctx, run.ID, "insufficient_data", "completed", map[string]any{"step": 2}, []string{"source_missing"}); err != nil {
		t.Fatalf("update terminal state: %v", err)
	}
	stored, _, _, err := NewStore(pool).Latest(ctx, assetID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != "insufficient_data" || stored.Stage != "completed" || stored.StartedAt == nil || stored.CompletedAt == nil {
		t.Fatalf("unexpected stored run: %#v", stored)
	}
}
