package modelprompt

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

func TestStoreUpdateConflictAndReset(t *testing.T) {
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
	schema := "model_prompt_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	definition := Definition{Key: "test_prompt", DefaultPrompt: "default", RequiredSuffix: "guardrail"}
	store := NewStore(pool)
	updated, err := store.Update(ctx, definition, "custom", "test", 0)
	if err != nil || updated.Version != 1 || !updated.HasOverride || !strings.Contains(updated.EffectivePrompt, "guardrail") {
		t.Fatalf("unexpected update: %#v err=%v", updated, err)
	}
	if _, err = store.Update(ctx, definition, "stale", "test", 0); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	reset, err := store.Reset(ctx, definition, "test", 1)
	if err != nil || reset.Version != 2 || reset.HasOverride || reset.EffectivePrompt != "default" {
		t.Fatalf("unexpected reset: %#v err=%v", reset, err)
	}
	var revisions int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM model_prompt_revisions WHERE prompt_key=$1`, definition.Key).Scan(&revisions); err != nil || revisions != 2 {
		t.Fatalf("revisions=%d err=%v", revisions, err)
	}
}
