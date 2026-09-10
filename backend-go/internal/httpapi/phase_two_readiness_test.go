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
	want := map[string]string{
		"analyst_evidence": "waiting_human_input", "approved_fundamental_plan": "blocked_by_dependency",
		"pit_benchmark_coverage": "waiting_human_input", "mature_forward_outcomes": "waiting_natural_maturity",
		"walk_forward_dataset": "blocked_by_dependency", "final_holdout_evaluation": "engineering_gap",
		"approved_prediction_model": "blocked_by_dependency",
	}
	for _, gate := range report.Gates {
		if expected, ok := want[gate.ID]; ok && gate.Status != expected {
			t.Errorf("gate %s status=%s want=%s", gate.ID, gate.Status, expected)
		}
	}
}

func TestPhaseTwoReadinessRequiresEveryBlockingGate(t *testing.T) {
	facts := phaseTwoReadinessFacts{
		AnalystEvidence: 6, ApprovedFundamentalPlans: 1, ActiveEquityAssets: 10, BenchmarkCoveredActiveEquities: 10,
		MatureEquityOutcomes: 120, LargestMatureEquityMarket: 120, HoldoutReservations: 1, WalkForwardDatasets: 1,
		DevelopmentExperiments: 1, LayeredPerformanceReports: 1, ResearchQualityReviews: 1, PassedFailureDrillScenarios: 5,
		ApprovedPredictionModels: 1, SECIdentityConfigured: true, FinalHoldoutEvaluationImplemented: true,
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
	if !facts.SECIdentityConfigured || facts.AnalystEvidence != 0 || facts.ActiveEquityAssets != 0 {
		t.Fatalf("unexpected empty readiness facts: %#v", facts)
	}
	report := buildPhaseTwoReadinessReport(facts, time.Now().UTC())
	if report.OverallStatus != "blocked" || len(report.Gates) != 13 {
		t.Fatalf("unexpected empty readiness report: %#v", report)
	}
}
