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
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/governance"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/prediction"
)

func TestRuleBaselineCollectionIsForwardOnlyIdempotentAndNotPromotableAgainstPostgres(t *testing.T) {
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
	schema := "rule_collection_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
		VALUES('equity:XNAS:FORWARD','equity','US','FORWARD','Forward Inc.','XNAS','USD','[]','[]','[]',1,true)`); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 1, 10, 14, 0, 0, 0, time.UTC)
	insertRecommendation := func(id string, signalAt time.Time) {
		payload, _ := json.Marshal(map[string]any{"scoring_version": "llm-direction-v3", "signal_available_at": signalAt.Format(time.RFC3339Nano)})
		if _, insertErr := pool.Exec(ctx, `INSERT INTO recommendations(id,run_id,asset_id,score,rating,confidence,as_of,payload)
			VALUES($1,$1,'equity:XNAS:FORWARD',40,'bullish',0,$2,$3)`, id, signalAt, payload); insertErr != nil {
			t.Fatal(insertErr)
		}
	}
	insertRecommendation("00000000-0000-4000-8000-000000000101", base)
	jobPayload, _ := json.Marshal(taskEnvelope{Args: []any{"00000000-0000-4000-8000-000000000101"}, Kwargs: map[string]any{}})
	runtime := &outcomeRuntime{db: pool}
	firstRaw, err := runtime.collectRuleBaselinePrediction(ctx, Job{Payload: jobPayload})
	if err != nil {
		t.Fatal(err)
	}
	first := firstRaw.(map[string]any)
	if first["created"] != 3 || first["trained_sample_count"] != 0 || first["calibrated_probability"] != false || first["promotion_eligible"] != false {
		t.Fatalf("unexpected first collection: %#v", first)
	}
	secondRaw, err := runtime.collectRuleBaselinePrediction(ctx, Job{Payload: jobPayload})
	if err != nil {
		t.Fatal(err)
	}
	second := secondRaw.(map[string]any)
	if second["created"] != 0 || second["existing"] != 3 {
		t.Fatalf("collection was not idempotent: %#v", second)
	}
	insertRecommendation("00000000-0000-4000-8000-000000000102", base.Add(-time.Minute))
	historicalPayload, _ := json.Marshal(taskEnvelope{Args: []any{"00000000-0000-4000-8000-000000000102"}, Kwargs: map[string]any{}})
	historicalRaw, err := runtime.collectRuleBaselinePrediction(ctx, Job{Payload: historicalPayload})
	if err != nil {
		t.Fatal(err)
	}
	historical := historicalRaw.(map[string]any)
	if historical["created"] != 0 || historical["skipped"] != 3 {
		t.Fatalf("historical signal was backfilled: %#v", historical)
	}
	var models, runs, probabilities int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM prediction_models WHERE model_payload::jsonb->>'kind'='fixed_rule_baseline' AND scope->>'deployment_role'='forward_data_collection_only' AND scope->>'promotion_eligible'='false'`).Scan(&models); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*),count(probabilities) FROM prediction_runs`).Scan(&runs, &probabilities); err != nil {
		t.Fatal(err)
	}
	if models != 3 || runs != 3 || probabilities != 0 {
		t.Fatalf("unexpected persisted collection models=%d runs=%d probabilities=%d", models, runs, probabilities)
	}
	decision, err := prediction.New(pool).Promote(ctx, prediction.RuleCollectionBaselineVersion+"-h5", governance.PromotionInput{}, base.AddDate(0, 0, 40))
	if err != nil || decision.Status != "blocked" || len(decision.Reasons) != 1 || decision.Reasons[0] != "fixed_rule_data_collection_only" {
		t.Fatalf("fixed rule promotion was not blocked: decision=%#v err=%v", decision, err)
	}
}
