package httpapi

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestPhaseTwoReadinessKeepsMissingTruthExplicit(t *testing.T) {
	report := buildPhaseTwoReadinessReport(phaseTwoReadinessFacts{ActiveEquityAssets: 12, PendingPredictionLabels: 66}, time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC))
	if report.OverallStatus != "blocked" || report.AutomaticCompletion || report.CompletedGates != 0 {
		t.Fatalf("missing production facts were presented as ready: %#v", report)
	}
	if report.Facts.LatestOutcomeEvaluation.Status != "" {
		t.Fatalf("builder unexpectedly invented an outcome evaluation: %#v", report.Facts.LatestOutcomeEvaluation)
	}
	want := map[string]string{
		"analyst_evidence": "waiting_human_input", "approved_fundamental_plan": "blocked_by_dependency",
		"pit_benchmark_coverage": "waiting_human_input", "mature_forward_outcomes": "waiting_natural_maturity",
		"walk_forward_dataset": "blocked_by_dependency", "final_holdout_evaluation": "blocked_by_dependency",
		"approved_prediction_model": "blocked_by_dependency",
	}
	for _, gate := range report.Gates {
		if expected, ok := want[gate.ID]; ok && gate.Status != expected {
			t.Errorf("gate %s status=%s want=%s", gate.ID, gate.Status, expected)
		}
	}
	for _, gate := range report.Gates {
		if gate.ID == "sealed_holdout" && len(gate.Dependencies) != 0 {
			t.Fatalf("future holdout registration must not wait for mature outcomes: %#v", gate)
		}
	}
}

func TestPhaseTwoReadinessUsesOneCompleteMarketInsteadOfEveryActiveEquity(t *testing.T) {
	facts := phaseTwoReadinessFacts{
		ActiveEquityAssets: 14398, BenchmarkCoveredActiveEquities: 2400, BenchmarkReadyEquityMarkets: 1,
	}
	report := buildPhaseTwoReadinessReport(facts, time.Now())
	for _, gate := range report.Gates {
		if gate.ID != "pit_benchmark_coverage" {
			continue
		}
		if gate.Status != "completed" || gate.Current != 1 || gate.Required != 1 || gate.Unit != "个市场" {
			t.Fatalf("one fully covered stock market must satisfy the benchmark gate: %#v", gate)
		}
		if report.Facts.BenchmarkCoveredActiveEquities != 2400 {
			t.Fatalf("asset-level disclosure was lost: %#v", report.Facts)
		}
		return
	}
	t.Fatal("pit benchmark coverage gate is missing")
}

func TestPhaseTwoReadinessRequiresEveryBlockingGate(t *testing.T) {
	facts := phaseTwoReadinessFacts{
		AnalystEvidence: 6, ApprovedFundamentalPlans: 1, ActiveEquityAssets: 10, BenchmarkCoveredActiveEquities: 10, BenchmarkReadyEquityMarkets: 1,
		MatureEquityOutcomes: 120, LargestMatureEquityMarket: 120, HoldoutReservations: 1, WalkForwardDatasets: 1,
		DevelopmentExperiments: 1, LayeredPerformanceReports: 1, ResearchQualityReviews: 1, PassedFailureDrillScenarios: 5,
		ApprovedPredictionModels: 1, SECIdentityConfigured: true, FinalHoldoutEvaluationImplemented: true, FinalHoldoutEvaluations: 1,
		DatasetReadyEvaluationScopes: 1,
	}
	report := buildPhaseTwoReadinessReport(facts, time.Now())
	if report.OverallStatus != "eligible_for_human_acceptance" || report.CompletedGates != report.TotalBlockingGates {
		t.Fatalf("complete fixture was not eligible for human acceptance: %#v", report)
	}
	for _, gate := range report.Gates {
		if gate.Status != "completed" {
			t.Errorf("gate %s remained %s", gate.ID, gate.Status)
		}
	}
}

func TestPhaseTwoReadinessReadsEmptyMigratedPostgres(t *testing.T) {
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
	schema := "phase_two_readiness_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	server := &Server{db: pool, cfg: config.Config{SECIdentity: "Example Research compliance@example.com"}}
	facts, err := loadPhaseTwoReadinessFacts(ctx, server, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !facts.SECIdentityConfigured || !facts.FinalHoldoutEvaluationImplemented || facts.FinalHoldoutEvaluations != 0 || facts.AnalystEvidence != 0 || facts.ActiveEquityAssets != 0 || facts.LatestOutcomeEvaluation.Status != "not_run" || facts.LatestOutcomeEvaluation.PendingReasons == nil {
		t.Fatalf("unexpected empty readiness facts: %#v", facts)
	}
	report := buildPhaseTwoReadinessReport(facts, time.Now().UTC())
	if report.OverallStatus != "blocked" || len(report.Gates) != 13 {
		t.Fatalf("unexpected empty readiness report: %#v", report)
	}

	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES
		('equity-us-1','equity','US','AAA','AAA','test','USD','[]','[]','[]',1,true),
		('equity-us-2','equity','US','BBB','BBB','test','USD','[]','[]','[]',1,true),
		('equity-cn-1','equity','CN','600001','CN','test','CNY','[]','[]','[]',100,true),
		('benchmark-us','index','US','SPXTR','SPX Total Return','test','USD','[]','[]','[]',1,true)`); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now().UTC().Add(-time.Hour)
	if _, err = pool.Exec(ctx, `INSERT INTO benchmark_mapping_observations(
		id,idempotency_key,request_hash,scope_type,scope_id,subject_market,subject_currency,benchmark_asset_id,benchmark_market,benchmark_currency,
		policy_version,valid_from,observed_at,available_at,source_name,source_document_id,mapping_reason,approved_by)
		VALUES('mapping-us','mapping-us','digest','market','US','US','USD','benchmark-us','US','USD','test-v1',$1,$1,$1,'test','test-document','test mapping','reviewer')`, observedAt); err != nil {
		t.Fatal(err)
	}
	facts, err = loadPhaseTwoReadinessFacts(ctx, server, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if facts.ActiveEquityAssets != 3 || facts.BenchmarkCoveredActiveEquities != 2 || facts.BenchmarkReadyEquityMarkets != 1 || facts.DatasetReadyEvaluationScopes != 0 {
		t.Fatalf("readiness did not preserve asset disclosure and one-market completion semantics: %#v", facts)
	}

	jobID := "11111111-1111-4111-8111-111111111111"
	if _, err = pool.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,result,created_at,completed_at)
		VALUES($1,'outcomes','market_loop.evaluate_outcomes','{}','completed',
		'{"prediction_outcomes":{"selected":96,"matured":0,"pending":96,"unavailable":0,"excluded":0,"failed":0,"pending_reasons":{"awaiting_price_sessions":96}}}',
		$2,$2)`, jobID, time.Now().UTC().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	facts, err = loadPhaseTwoReadinessFacts(ctx, server, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	latest := facts.LatestOutcomeEvaluation
	if latest.JobID != jobID || latest.Status != "completed" || latest.Selected != 96 || latest.Pending != 96 || latest.PendingReasons["awaiting_price_sessions"] != 96 || latest.CreatedAt == nil || latest.CompletedAt == nil {
		t.Fatalf("latest outcome evaluation was not summarized safely: %#v", latest)
	}
}
