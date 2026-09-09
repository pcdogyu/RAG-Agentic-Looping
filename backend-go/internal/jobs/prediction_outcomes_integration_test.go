package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/evaluation"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestPredictionOutcomesMatureFiveSessionsWithoutAdvancingTwentyAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "prediction_outcomes_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	dbConfig, _ := pgxpool.ParseConfig(dsn)
	dbConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, values := range [][]string{
		{"equity:XNAS:TARGET", "TARGET", "Target Inc."},
		{"etf:ARCX:BENCH", "BENCH", "Benchmark ETF"},
	} {
		if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
            VALUES($1,'equity','US',$2,$3,'XNAS','USD','[]','[]','[]',1,true)`, values[0], values[1], values[2]); err != nil {
			t.Fatal(err)
		}
	}
	signalAt := time.Date(2026, 1, 1, 22, 0, 0, 0, time.UTC)
	_, _, err = marketdata.NewStore(pool).CreateBenchmarkMapping(ctx, marketdata.BenchmarkMappingSubmission{
		ScopeType: "market", ScopeID: "US", SubjectMarket: "US", SubjectCurrency: "USD", BenchmarkAssetID: "etf:ARCX:BENCH",
		PolicyVersion: "label-test-policy-v1", ValidFrom: signalAt.AddDate(0, 0, -10), SourceName: "test policy", SourceDocumentID: "test-policy-1",
		MappingReason: "isolated regression benchmark", ApprovedBy: "test reviewer", IdempotencyKey: "label-test-us",
	}, signalAt.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, horizon := range []int{5, 20} {
		version := fmt.Sprintf("label-model-%d", horizon)
		if _, err = pool.Exec(ctx, `INSERT INTO prediction_models(version,objective,market,horizon_sessions,feature_schema,model_payload,training_cutoff,artifact_digest,status,scope)
            VALUES($1,'excess_up','US',$2,'{}','{}',$3,$1,'shadow','{"asset_class":"equity","outcome_label_definition_version":"prediction-outcome-label-v1"}')`, version, horizon, signalAt.AddDate(0, 0, -30)); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO prediction_runs(id,asset_id,asset_class,signal_available_at,horizon_sessions,objective,model_version,status,model_status,raw_score,feature_snapshot,exclusion_reason,idempotency_key)
            VALUES($1,'equity:XNAS:TARGET','equity',$2,$3,'excess_up',$4,'uncalibrated','shadow',0.8,'{}','',$1)`, "prediction-horizon-"+version, signalAt, horizon, version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = pool.Exec(ctx, `INSERT INTO prediction_models(version,objective,market,horizon_sessions,feature_schema,model_payload,training_cutoff,artifact_digest,status,scope)
		VALUES('label-model-legacy','excess_up','US',1,'{}','{}',$1,'label-model-legacy','shadow','{"asset_class":"equity"}')`, signalAt.AddDate(0, 0, -30)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO prediction_runs(id,asset_id,asset_class,signal_available_at,horizon_sessions,objective,model_version,status,model_status,raw_score,feature_snapshot,exclusion_reason,idempotency_key)
		VALUES('prediction-legacy','equity:XNAS:TARGET','equity',$1,1,'excess_up','label-model-legacy','uncalibrated','shadow',0.8,'{}','','prediction-legacy')`, signalAt); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		start, finish := 100.0, 102.0
		if symbol == "BENCH" {
			finish = 108
		}
		sessions := []time.Time{
			time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 1, 6, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 7, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC),
		}
		items := make([]map[string]any, len(sessions))
		for index := range sessions {
			value := start + (finish-start)*float64(index)/5
			items[index] = map[string]any{"date": sessions[index].Format("2006-01-02"), "adjClose": value, "close": value}
		}
		_ = json.NewEncoder(w).Encode(items)
	}))
	defer server.Close()
	runtime := &outcomeRuntime{cfg: config.Config{FMPBaseURL: server.URL, FMPAccessToken: "isolated", FMPRateLimit: 100000}, db: pool, client: server.Client()}
	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	summary, err := runtime.evaluatePredictionOutcomes(ctx, now, map[string][]outcomePricePoint{})
	if err != nil || summary["selected"] != 3 || summary["matured"] != 1 || summary["pending"] != 1 || summary["excluded"] != 1 || summary["failed"] != 0 {
		t.Fatalf("unexpected first maturity summary=%#v err=%v", summary, err)
	}
	items, err := evaluation.NewOutcomeStore(pool).ListByAsset(ctx, "equity:XNAS:TARGET", 10)
	if err != nil || len(items) != 2 {
		t.Fatalf("stored outcomes=%#v err=%v", items, err)
	}
	var label evaluation.OutcomeLabel
	legacyExcluded := false
	for _, item := range items {
		if item.Label.Status == "mature" {
			label = item.Label
		}
		if item.PredictionRunID == "prediction-legacy" && item.Label.Status == "excluded" && item.ExclusionReason == "outcome_label_definition_not_pre_registered" {
			legacyExcluded = true
		}
	}
	if !legacyExcluded {
		t.Fatalf("legacy prediction was not audibly excluded: %#v", items)
	}
	if label.Status != "mature" || label.HorizonSessions != 5 || label.AbsoluteLabel != "up" || label.RelativeLabel != "underperform" || label.NetReturn != nil || !label.ResearchResultOnly {
		t.Fatalf("stored label collapsed research and execution semantics: %#v", label)
	}
	if label.PriceField != "adjusted_close" || label.TimePrecision != "daily_close" || label.AlphaDefinition != "arithmetic_asset_total_return_minus_benchmark_total_return" || label.RiskAdjustmentStatus != "not_configured" {
		t.Fatalf("stored label lost its frozen data and risk contract: %#v", label)
	}
	repeated, err := runtime.evaluatePredictionOutcomes(ctx, now, map[string][]outcomePricePoint{})
	if err != nil || repeated["selected"] != 1 || repeated["matured"] != 0 || repeated["pending"] != 1 {
		t.Fatalf("mature record was not idempotent: summary=%#v err=%v", repeated, err)
	}
}
