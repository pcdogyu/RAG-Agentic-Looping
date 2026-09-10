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
		{"equity:XNAS:DELIST", "DELIST", "Delisted Inc."},
		{"equity:XNAS:EXECUTABLE", "EXECUTABLE", "Executable Inc."},
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
		VALUES('label-model-execution','absolute_up','US',1,'{}','{}',$1,'label-model-execution','shadow','{"asset_class":"equity","outcome_label_definition_version":"prediction-outcome-label-v1","execution_assumptions":{"enabled":true,"side":"long","round_trip_cost_bps":10}}')`, signalAt.AddDate(0, 0, -30)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO prediction_runs(id,asset_id,asset_class,signal_available_at,horizon_sessions,objective,model_version,status,model_status,raw_score,feature_snapshot,exclusion_reason,idempotency_key)
		VALUES('prediction-execution','equity:XNAS:TARGET','equity',$1,1,'absolute_up','label-model-execution','uncalibrated','shadow',0.8,'{}','','prediction-execution')`, signalAt); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO prediction_runs(id,asset_id,asset_class,signal_available_at,horizon_sessions,objective,model_version,status,model_status,raw_score,feature_snapshot,exclusion_reason,idempotency_key)
		VALUES('prediction-executable','equity:XNAS:EXECUTABLE','equity',$1,1,'absolute_up','label-model-execution','uncalibrated','shadow',0.8,'{}','','prediction-executable')`, signalAt); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO prediction_runs(id,asset_id,asset_class,signal_available_at,horizon_sessions,objective,model_version,status,model_status,raw_score,feature_snapshot,exclusion_reason,idempotency_key)
		VALUES('prediction-delisted','equity:XNAS:DELIST','equity',$1,20,'excess_up','label-model-20','uncalibrated','shadow',0.8,'{}','','prediction-delisted')`, signalAt); err != nil {
		t.Fatal(err)
	}
	if _, err = marketdata.NewStore(pool).SaveCorporateAction(ctx, marketdata.CorporateActionObservation{
		AssetID: "equity:XNAS:DELIST", Market: "US", Currency: "USD", ActionType: marketdata.Delisting,
		EffectiveAt: signalAt.AddDate(0, 0, 3), ObservedAt: signalAt.AddDate(0, 0, 2), AvailableAt: signalAt.AddDate(0, 0, 2),
		TimePrecision: "date_only", SourceName: "isolated exchange notice", SourceDocumentID: "delist-notice-1",
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = marketdata.NewStore(pool).ImportTradability(ctx, "equity:XNAS:EXECUTABLE", marketdata.TradabilityImport{
		SourceName: "isolated exchange status", SourceDocumentID: "execution-status-1", SourceURL: "https://exchange.example.test/status",
		LicenseReference: "test-license", ApprovedBy: "test-reviewer", IdempotencyKey: "execution-status-1",
		Observations: []marketdata.TradabilityImportPoint{
			{SessionDate: "2026-01-02", SourceObservedAt: "2026-01-02T21:01:00Z", Status: marketdata.Tradable},
			{SessionDate: "2026-01-05", SourceObservedAt: "2026-01-05T21:01:00Z", Status: marketdata.Tradable},
		},
	}, time.Date(2026, 1, 10, 11, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
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
	if err != nil || summary["selected"] != 6 || summary["matured"] != 3 || summary["pending"] != 1 || summary["unavailable"] != 1 || summary["excluded"] != 1 || summary["failed"] != 0 {
		t.Fatalf("unexpected first maturity summary=%#v err=%v", summary, err)
	}
	items, err := evaluation.NewOutcomeStore(pool).ListByAsset(ctx, "equity:XNAS:TARGET", 10)
	if err != nil || len(items) != 3 {
		t.Fatalf("stored outcomes=%#v err=%v", items, err)
	}
	var label evaluation.OutcomeLabel
	var executionLabel evaluation.OutcomeLabel
	legacyExcluded := false
	for _, item := range items {
		if item.PredictionRunID == "prediction-horizon-label-model-5" {
			label = item.Label
		}
		if item.PredictionRunID == "prediction-execution" {
			executionLabel = item.Label
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
	if executionLabel.Status != "mature" || executionLabel.RawReturn == nil || executionLabel.NetReturn != nil || executionLabel.GrossStrategyReturn != nil || executionLabel.SimulationStatus != "unavailable_tradability_evidence" || !executionLabel.ResearchResultOnly {
		t.Fatalf("provider close was incorrectly presented as executable: %#v", executionLabel)
	}
	executable, err := evaluation.NewOutcomeStore(pool).ListByAsset(ctx, "equity:XNAS:EXECUTABLE", 10)
	if err != nil || len(executable) != 1 || executable[0].Label.Status != "mature" || executable[0].Label.NetReturn == nil || executable[0].Label.GrossStrategyReturn == nil || executable[0].Label.SimulationStatus != "simulated_with_pre_registered_assumptions" || executable[0].Label.ResearchResultOnly {
		t.Fatalf("approved tradability evidence did not unlock execution simulation: items=%#v err=%v", executable, err)
	}
	if executable[0].DataQuality["entry_tradability_status"] != "tradable" || executable[0].DataQuality["exit_tradability_status"] != "tradable" {
		t.Fatalf("execution evidence audit missing: %#v", executable[0].DataQuality)
	}
	delisted, err := evaluation.NewOutcomeStore(pool).ListByAsset(ctx, "equity:XNAS:DELIST", 10)
	if err != nil || len(delisted) != 1 || delisted[0].Label.Status != "unavailable" || delisted[0].ExclusionReason != "delisting_before_horizon_exit" {
		t.Fatalf("terminal delisting sample was not retained: items=%#v err=%v", delisted, err)
	}
	if delisted[0].DataQuality["terminal_reason"] != "delisting_before_horizon_exit" || delisted[0].DataQuality["corporate_action_status"] != "observations_present" {
		t.Fatalf("delisting audit evidence missing: %#v", delisted[0].DataQuality)
	}
	repeated, err := runtime.evaluatePredictionOutcomes(ctx, now, map[string][]outcomePricePoint{})
	if err != nil || repeated["selected"] != 1 || repeated["matured"] != 0 || repeated["pending"] != 1 {
		t.Fatalf("mature record was not idempotent: summary=%#v err=%v", repeated, err)
	}
	legacyRecommendationID := uuid.New()
	legacyOutcome := map[string]any{
		"id": legacyRecommendationID.String(), "horizon_days": 5.0,
		"observed_at": now.Format(time.RFC3339), "status": "completed",
	}
	inserted, err := runtime.saveOutcome(ctx, legacyRecommendationID, legacyOutcome)
	if err != nil || !inserted {
		t.Fatalf("legacy outcome with UUID identifiers was not stored: inserted=%v err=%v", inserted, err)
	}
	inserted, err = runtime.saveOutcome(ctx, legacyRecommendationID, legacyOutcome)
	if err != nil || inserted {
		t.Fatalf("legacy outcome idempotency failed: inserted=%v err=%v", inserted, err)
	}
}
