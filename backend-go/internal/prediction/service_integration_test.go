package prediction

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/calibration"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/governance"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

func TestPredictionLifecycleAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "prediction_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	const assetID = "equity:XNAS:AAPL"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES($1,'equity','US','AAPL','Apple Inc.','XNAS','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}

	service := New(pool)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	model := signals.BinaryModel{Version: "integration-model-v1", Objective: "excess_up", HorizonSessions: 5, TrainingCutoff: base, FeatureNames: []string{"surprise"}, Means: map[string]float64{"surprise": 0}, Scales: map[string]float64{"surprise": 1}, Coefficients: map[string]float64{"surprise": 1}, SampleCount: 100}
	if err = service.RegisterModel(ctx, ModelRegistration{Model: model, Market: "US", Status: "shadow", ArtifactDigest: ModelArtifactDigest(model), Scope: map[string]any{"asset_class": "equity"}}); err != nil {
		t.Fatal(err)
	}
	now := base.AddDate(1, 0, 0)
	ece := .02
	gate := governance.PromotionInput{HardCorrectnessPassed: true, IndependentSamples: 100, MinimumSamples: 30, ShadowStartedAt: now.AddDate(0, 0, -30), MinimumShadowDays: 14, ECE: &ece, MaximumECE: .1, ApprovedBy: "integration-reviewer"}
	if decision, promoteErr := service.Promote(ctx, model.Version, gate, now); promoteErr != nil || decision.Status != "approved" {
		t.Fatalf("model promotion decision=%#v err=%v", decision, promoteErr)
	}
	observations := make([]calibration.Observation, 40)
	for index := range observations {
		observations[index] = calibration.Observation{SampleID: fmt.Sprintf("sample-%d", index), EventCluster: fmt.Sprintf("cluster-%d", index), Score: float64(index-20) / 4, Label: index >= 20, ObservedAt: base.AddDate(0, 0, index)}
	}
	calibrator, err := service.RegisterCalibration(ctx, CalibrationRegistration{SourceModelVersion: model.Version, Observations: observations, Scope: calibration.Scope{Market: "US", HorizonSessions: 5, EventTypes: []string{"earnings"}}, Status: "shadow"})
	if err != nil {
		t.Fatal(err)
	}
	if decision, promoteErr := service.PromoteCalibration(ctx, calibrator.Version, gate, now); promoteErr != nil || decision.Status != "approved" {
		t.Fatalf("calibration promotion decision=%#v err=%v", decision, promoteErr)
	}

	signalAt := now.Add(time.Hour)
	featureValue := .4
	input := Input{AssetID: assetID, SignalAvailableAt: signalAt, ModelVersion: model.Version, Market: "US", EventType: "earnings", Features: []signals.Feature{{Name: "surprise", Value: &featureValue, AvailableAt: signalAt.Add(-time.Minute), SourceIDs: []string{"consensus-1"}}}}
	first, err := service.Predict(ctx, input)
	if err != nil || !first.Created || first.Status != "calibrated" || first.Probability == nil || first.CalibrationVersion != calibrator.Version {
		t.Fatalf("first prediction=%#v err=%v", first, err)
	}
	repeated, err := service.Predict(ctx, input)
	if err != nil || repeated.Created || repeated.ID != first.ID {
		t.Fatalf("repeated prediction=%#v err=%v", repeated, err)
	}
	changedValue := .8
	input.Features[0].Value = &changedValue
	changed, err := service.Predict(ctx, input)
	if err != nil || !changed.Created || changed.ID == first.ID {
		t.Fatalf("changed prediction=%#v err=%v", changed, err)
	}
	items, err := service.List(ctx, assetID, 20)
	if err != nil || len(items) != 2 {
		t.Fatalf("prediction list=%#v err=%v", items, err)
	}
	if items[0].AssetClass != "equity" {
		t.Fatalf("prediction asset class was not persisted: %#v", items[0])
	}
	crossScope := input
	crossScope.AssetClass = "crypto"
	if _, err = service.Predict(ctx, crossScope); err == nil {
		t.Fatal("prediction request could override the asset master class")
	}
	if _, err = service.RegisterCalibration(ctx, CalibrationRegistration{SourceModelVersion: model.Version, Observations: observations, Scope: calibration.Scope{AssetClass: "crypto", Market: "US", HorizonSessions: 5}, Status: "shadow"}); err == nil {
		t.Fatal("cross-asset calibration scope was accepted for an equity model")
	}
	if _, err = service.RegisterCalibration(ctx, CalibrationRegistration{SourceModelVersion: model.Version, Observations: observations, Scope: calibration.Scope{AssetClass: "equity", Market: "US", HorizonSessions: 20}, Status: "shadow"}); err == nil {
		t.Fatal("cross-horizon calibration scope was accepted for a five-session model")
	}

	candidate := model
	candidate.Version = "integration-candidate-v2"
	candidate.Coefficients = map[string]float64{"surprise": 1.1}
	if err = service.RegisterModel(ctx, ModelRegistration{Model: candidate, Market: "US", Status: "shadow", ArtifactDigest: ModelArtifactDigest(candidate), Scope: map[string]any{"asset_class": "equity"}}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 20; index++ {
		value := float64(index-10) / 10
		at := signalAt.Add(time.Duration(index+1) * time.Hour)
		comparison, compareErr := service.CompareShadow(ctx, ShadowInput{AssetID: assetID, SignalAvailableAt: at, IncumbentModelVersion: model.Version, CandidateModelVersion: candidate.Version, Market: "US", EventType: "earnings", Features: []signals.Feature{{Name: "surprise", Value: &value, AvailableAt: at.Add(-time.Minute), SourceIDs: []string{"consensus-forward"}}}, ExecutionAssumptions: map[string]any{"entry": "next_session_open", "cost_bps": 10}})
		if compareErr != nil || !comparison.Created || !comparison.Incumbent.SignalAvailableAt.Equal(comparison.Candidate.SignalAvailableAt) {
			t.Fatalf("shadow comparison %d=%#v err=%v", index, comparison, compareErr)
		}
	}
	checks, err := service.MonitorShadowModels(ctx, signalAt.AddDate(0, 0, 1))
	if err != nil || len(checks) != 1 || checks[0].Status != "review_required" || checks[0].Action != "alert_and_review" {
		t.Fatalf("shadow checks=%#v err=%v", checks, err)
	}
	var candidateStatus string
	if err = pool.QueryRow(ctx, `SELECT status FROM prediction_models WHERE version=$1`, candidate.Version).Scan(&candidateStatus); err != nil || candidateStatus != "shadow" {
		t.Fatalf("monitor must not automatically switch candidate: status=%q err=%v", candidateStatus, err)
	}
	blockedGate := gate
	blockedGate.HardCorrectnessPassed = false
	if decision, promoteErr := service.Promote(ctx, candidate.Version, blockedGate, signalAt.AddDate(0, 0, 2)); promoteErr != nil || decision.Status != "blocked" {
		t.Fatalf("blocked promotion decision=%#v err=%v", decision, promoteErr)
	}
	history, err := service.ListGovernanceChecks(ctx, 20)
	if err != nil || len(history) < 4 {
		t.Fatalf("governance history=%#v err=%v", history, err)
	}
	foundBlocked := false
	for _, check := range history {
		if check.SubjectVersion == candidate.Version && check.CheckType == "promotion" && check.Status == "blocked" {
			foundBlocked = true
		}
	}
	if !foundBlocked {
		t.Fatal("blocked promotion was not durably audited")
	}
}
