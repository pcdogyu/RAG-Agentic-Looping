package migrate

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUpCreatesFreshGoRuntimeSchema(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "phase3_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") }()

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
	if err := Up(ctx, pool); err != nil {
		t.Fatal(err)
	}

	expected := []string{"assets", "mcp_sources", "news_items", "news_events", "research_runs", "recommendations", "go_jobs"}
	for _, table := range expected {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, schema+"."+table).Scan(&exists); err != nil || !exists {
			t.Fatalf("table %s was not created: exists=%v err=%v", table, exists, err)
		}
	}
	var searx, duck int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE name='SearXNG'),count(*) FILTER (WHERE name='DuckDuckGo') FROM mcp_sources`).Scan(&searx, &duck); err != nil {
		t.Fatal(err)
	}
	if searx != 1 || duck != 0 {
		t.Fatalf("unexpected managed MCP seed state: SearXNG=%d DuckDuckGo=%d", searx, duck)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'test','official','title','summary','https://example.com','en',now(),now(),now(),$2,'[]','{}')`, uuid.NewString(), fmt.Sprintf("%064d", 1)); err != nil {
		t.Fatalf("fresh schema rejected a core write: %v", err)
	}
}
