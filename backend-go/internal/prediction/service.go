// Package prediction wires interpretable feature models and independent
// calibrators to durable, point-in-time prediction records.
package prediction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/calibration"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/evaluation"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/governance"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

type ModelRegistration struct {
	Model          signals.BinaryModel `json:"model"`
	Market         string              `json:"market"`
	Status         string              `json:"status"`
	ArtifactDigest string              `json:"artifact_digest"`
	Scope          map[string]any      `json:"scope"`
	ApprovedBy     string              `json:"approved_by,omitempty"`
}

type CalibrationRegistration struct {
	SourceModelVersion string                    `json:"source_model_version"`
	Observations       []calibration.Observation `json:"observations"`
	Scope              calibration.Scope         `json:"scope"`
	ValidUntil         *time.Time                `json:"valid_until,omitempty"`
	Status             string                    `json:"status"`
}

type Input struct {
	AssetID           string            `json:"asset_id"`
	AssetClass        string            `json:"asset_class"`
	EventID           string            `json:"event_id,omitempty"`
	SignalAvailableAt time.Time         `json:"signal_available_at"`
	ModelVersion      string            `json:"model_version"`
	Market            string            `json:"market"`
	EventType         string            `json:"event_type"`
	Features          []signals.Feature `json:"features"`
}

type Run struct {
	ID                 string           `json:"id"`
	AssetID            string           `json:"asset_id"`
	AssetClass         string           `json:"asset_class"`
	EventID            string           `json:"event_id,omitempty"`
	SignalAvailableAt  time.Time        `json:"signal_available_at"`
	HorizonSessions    int              `json:"horizon_sessions"`
	Objective          string           `json:"objective"`
	ModelVersion       string           `json:"model_version"`
	CalibrationVersion string           `json:"calibration_version,omitempty"`
	Status             string           `json:"status"`
	ModelStatus        string           `json:"model_status"`
	RawScore           *float64         `json:"raw_score,omitempty"`
	Probability        *float64         `json:"probability,omitempty"`
	FeatureSnapshot    signals.Snapshot `json:"feature_snapshot"`
	ExclusionReason    string           `json:"exclusion_reason,omitempty"`
	Created            bool             `json:"created"`
}

type Service struct{ db *pgxpool.Pool }

func New(db *pgxpool.Pool) *Service { return &Service{db: db} }

func (s *Service) RegisterModel(ctx context.Context, input ModelRegistration) error {
	if s.db == nil {
		return fmt.Errorf("prediction store is unavailable")
	}
	if input.Model.Version == "" || input.Model.TrainingCutoff.IsZero() || input.Model.HorizonSessions < 1 || strings.TrimSpace(input.Market) == "" || strings.TrimSpace(input.ArtifactDigest) == "" {
		return fmt.Errorf("complete model identity, cutoff, scope and artifact digest are required")
	}
	if err := validateModel(input.Model); err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(input.ArtifactDigest), ModelArtifactDigest(input.Model)) {
		return fmt.Errorf("artifact_digest does not match the registered model payload")
	}
	status := strings.ToLower(strings.TrimSpace(input.Status))
	if status == "" {
		status = "shadow"
	}
	if status != "shadow" {
		return fmt.Errorf("new models must enter shadow status and use the promotion gate before approval")
	}
	featureSchema, _ := json.Marshal(map[string]any{"names": input.Model.FeatureNames, "missing_policy": "reject"})
	payload, _ := json.Marshal(input.Model)
	if input.Scope == nil {
		input.Scope = map[string]any{}
	}
	rawAssetClass, hasAssetClass := input.Scope["asset_class"]
	if !hasAssetClass || rawAssetClass == nil || strings.TrimSpace(fmt.Sprint(rawAssetClass)) == "" {
		input.Scope["asset_class"] = "equity"
	} else {
		input.Scope["asset_class"] = strings.ToLower(strings.TrimSpace(fmt.Sprint(input.Scope["asset_class"])))
	}
	if policy := marketpolicy.Resolve(fmt.Sprint(input.Scope["asset_class"]), input.Market); !policy.PredictionSupported {
		return fmt.Errorf("prediction model scope is unsupported")
	}
	labelPolicy, _ := evaluation.ResolveHorizonPolicy(input.Model.Objective, input.Model.HorizonSessions)
	if raw, exists := input.Scope["execution_assumptions"]; exists {
		body, marshalErr := json.Marshal(raw)
		assumptions := evaluation.ExecutionAssumptions{}
		if marshalErr != nil || json.Unmarshal(body, &assumptions) != nil {
			return fmt.Errorf("execution_assumptions are invalid")
		}
		if _, err := evaluation.WithExecutionAssumptions(labelPolicy, assumptions); err != nil {
			return err
		}
	}
	input.Scope["outcome_label_definition_version"] = evaluation.OutcomeLabelDefinitionVersion
	scope, _ := json.Marshal(input.Scope)
	_, err := s.db.Exec(ctx, `INSERT INTO prediction_models(version,objective,market,horizon_sessions,feature_schema,model_payload,training_cutoff,artifact_digest,status,scope) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(version) DO NOTHING`, input.Model.Version, input.Model.Objective, strings.ToUpper(input.Market), input.Model.HorizonSessions, featureSchema, payload, input.Model.TrainingCutoff, input.ArtifactDigest, status, scope)
	return err
}

func (s *Service) Promote(ctx context.Context, modelVersion string, input governance.PromotionInput, now time.Time) (governance.Decision, error) {
	if s.db == nil || strings.TrimSpace(modelVersion) == "" {
		return governance.Decision{}, fmt.Errorf("prediction store and model_version are required")
	}
	decision := governance.PromotionDecision(input, now)
	if decision.Status != "approved" {
		return decision, s.RecordPromotionDecision(ctx, "model", modelVersion, input, decision, now)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return decision, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, `UPDATE prediction_models SET status='approved',approved_by=$2,approved_at=$3 WHERE version=$1 AND status='shadow'`, strings.TrimSpace(modelVersion), strings.TrimSpace(input.ApprovedBy), now.UTC())
	if err != nil {
		return decision, err
	}
	if tag.RowsAffected() != 1 {
		return decision, fmt.Errorf("shadow prediction model was not found or was already promoted")
	}
	if err = recordPromotionDecisionWith(ctx, tx, "model", modelVersion, input, decision, now); err != nil {
		return decision, err
	}
	return decision, tx.Commit(ctx)
}

func (s *Service) RegisterCalibration(ctx context.Context, input CalibrationRegistration) (calibration.Model, error) {
	if s.db == nil {
		return calibration.Model{}, fmt.Errorf("prediction store is unavailable")
	}
	if input.Scope.AssetClass = strings.ToLower(strings.TrimSpace(input.Scope.AssetClass)); input.Scope.AssetClass == "" {
		input.Scope.AssetClass = "equity"
	}
	if policy := marketpolicy.Resolve(input.Scope.AssetClass, input.Scope.Market); !policy.PredictionSupported {
		return calibration.Model{}, fmt.Errorf("calibration scope is unsupported")
	}
	var sourceMarket string
	var sourceScopeBody []byte
	if err := s.db.QueryRow(ctx, `SELECT market,scope::jsonb FROM prediction_models WHERE version=$1`, input.SourceModelVersion).Scan(&sourceMarket, &sourceScopeBody); err != nil {
		return calibration.Model{}, fmt.Errorf("load source prediction model: %w", err)
	}
	sourceScope := map[string]any{}
	_ = json.Unmarshal(sourceScopeBody, &sourceScope)
	sourceAssetClass := strings.ToLower(strings.TrimSpace(fmt.Sprint(sourceScope["asset_class"])))
	if sourceAssetClass == "" || sourceAssetClass == "<nil>" {
		sourceAssetClass = "equity"
	}
	if !strings.EqualFold(sourceMarket, input.Scope.Market) || sourceAssetClass != input.Scope.AssetClass {
		return calibration.Model{}, fmt.Errorf("calibration scope must match the source model asset class and market")
	}
	model, err := calibration.FitPlatt(input.SourceModelVersion, input.Observations, input.Scope)
	if err != nil {
		return calibration.Model{}, err
	}
	model.ValidUntil = input.ValidUntil
	if model.ValidUntil != nil && !model.ValidUntil.After(model.TrainedUntil) {
		return calibration.Model{}, fmt.Errorf("valid_until must be after the calibration cutoff")
	}
	status := strings.ToLower(strings.TrimSpace(input.Status))
	if status == "" {
		status = "shadow"
	}
	if status != "shadow" {
		return calibration.Model{}, fmt.Errorf("new calibrations must enter shadow status and use the promotion gate before activation")
	}
	parameters, _ := json.Marshal(model)
	scope, _ := json.Marshal(model.Scope)
	conditions, _ := json.Marshal([]string{"scope_change", "valid_until", "drift_alert"})
	_, err = s.db.Exec(ctx, `INSERT INTO probability_calibrations(version,model_version,method,parameters,sample_from,sample_to,sample_count,scope,invalidation_conditions,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(version) DO NOTHING`, model.Version, model.SourceModelVersion, model.Method, parameters, model.TrainedFrom, model.TrainedUntil, model.SampleCount, scope, conditions, status)
	return model, err
}

func ModelArtifactDigest(model signals.BinaryModel) string {
	body, _ := json.Marshal(model)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func validateModel(model signals.BinaryModel) error {
	if strings.TrimSpace(model.Objective) == "" || len(model.FeatureNames) == 0 || model.SampleCount < 1 {
		return fmt.Errorf("model objective, features and positive sample_count are required")
	}
	if _, err := evaluation.ResolveHorizonPolicy(model.Objective, model.HorizonSessions); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, name := range model.FeatureNames {
		name = strings.TrimSpace(name)
		scale, scaleOK := model.Scales[name]
		mean, meanOK := model.Means[name]
		coefficient, coefficientOK := model.Coefficients[name]
		if name == "" || seen[name] || !scaleOK || scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) || !meanOK || math.IsNaN(mean) || math.IsInf(mean, 0) || !coefficientOK || math.IsNaN(coefficient) || math.IsInf(coefficient, 0) {
			return fmt.Errorf("model feature schema and finite parameters are inconsistent")
		}
		seen[name] = true
	}
	if math.IsNaN(model.Intercept) || math.IsInf(model.Intercept, 0) {
		return fmt.Errorf("model intercept must be finite")
	}
	return nil
}

func (s *Service) PromoteCalibration(ctx context.Context, version string, input governance.PromotionInput, now time.Time) (governance.Decision, error) {
	if s.db == nil || strings.TrimSpace(version) == "" {
		return governance.Decision{}, fmt.Errorf("prediction store and calibration version are required")
	}
	decision := governance.PromotionDecision(input, now)
	if decision.Status != "approved" {
		return decision, s.RecordPromotionDecision(ctx, "calibration", version, input, decision, now)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return decision, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var sourceVersion, calibrationStatus, modelStatus string
	if err = tx.QueryRow(ctx, `SELECT c.model_version,c.status,m.status FROM probability_calibrations c JOIN prediction_models m ON m.version=c.model_version WHERE c.version=$1 FOR UPDATE`, strings.TrimSpace(version)).Scan(&sourceVersion, &calibrationStatus, &modelStatus); err != nil {
		return decision, fmt.Errorf("load shadow calibration: %w", err)
	}
	if calibrationStatus != "shadow" || modelStatus != "approved" {
		return decision, fmt.Errorf("calibration activation requires a shadow calibration and approved source model")
	}
	if _, err = tx.Exec(ctx, `UPDATE probability_calibrations SET status='superseded' WHERE model_version=$1 AND status='active'`, sourceVersion); err != nil {
		return decision, err
	}
	if _, err = tx.Exec(ctx, `UPDATE probability_calibrations SET status='active' WHERE version=$1`, strings.TrimSpace(version)); err != nil {
		return decision, err
	}
	if err = recordPromotionDecisionWith(ctx, tx, "calibration", version, input, decision, now); err != nil {
		return decision, err
	}
	if err = tx.Commit(ctx); err != nil {
		return decision, err
	}
	return decision, nil
}

func (s *Service) Predict(ctx context.Context, input Input) (Run, error) {
	if s.db == nil {
		return Run{}, fmt.Errorf("prediction store is unavailable")
	}
	if input.AssetID == "" || input.ModelVersion == "" || input.SignalAvailableAt.IsZero() {
		return Run{}, fmt.Errorf("asset_id, model_version and signal_available_at are required")
	}
	var actualAssetClass, actualMarket string
	if err := s.db.QueryRow(ctx, `SELECT asset_class,market FROM assets WHERE id=$1 AND active=true`, input.AssetID).Scan(&actualAssetClass, &actualMarket); err != nil {
		return Run{}, fmt.Errorf("load active prediction asset: %w", err)
	}
	if strings.TrimSpace(input.AssetClass) == "" {
		input.AssetClass = actualAssetClass
	}
	if strings.TrimSpace(input.Market) == "" {
		input.Market = actualMarket
	}
	if !strings.EqualFold(input.AssetClass, actualAssetClass) || !strings.EqualFold(input.Market, actualMarket) {
		return Run{}, fmt.Errorf("prediction scope does not match the asset master")
	}
	if policy := marketpolicy.Resolve(actualAssetClass, actualMarket); !policy.PredictionSupported {
		return Run{}, fmt.Errorf("prediction is unsupported for the asset market policy")
	}
	var modelBody, scopeBody []byte
	var status, market string
	var horizon int
	err := s.db.QueryRow(ctx, `SELECT model_payload::jsonb,status,market,horizon_sessions,scope::jsonb FROM prediction_models WHERE version=$1`, input.ModelVersion).Scan(&modelBody, &status, &market, &horizon, &scopeBody)
	if err != nil {
		return Run{}, fmt.Errorf("load prediction model: %w", err)
	}
	if status != "approved" && status != "shadow" {
		return Run{}, fmt.Errorf("prediction model is not eligible")
	}
	if !strings.EqualFold(market, input.Market) {
		return Run{}, fmt.Errorf("prediction market is outside model scope")
	}
	assetClass := strings.ToLower(strings.TrimSpace(input.AssetClass))
	if assetClass == "" {
		assetClass = "equity"
	}
	modelScope := map[string]any{}
	_ = json.Unmarshal(scopeBody, &modelScope)
	modelAssetClass := strings.ToLower(strings.TrimSpace(fmt.Sprint(modelScope["asset_class"])))
	if modelAssetClass == "" || modelAssetClass == "<nil>" {
		modelAssetClass = "equity"
	}
	if assetClass != modelAssetClass {
		return Run{}, fmt.Errorf("prediction asset class is outside model scope")
	}
	model := signals.BinaryModel{}
	if err = json.Unmarshal(modelBody, &model); err != nil {
		return Run{}, err
	}
	snapshot := signals.BuildSnapshot(input.SignalAvailableAt, model.FeatureNames, input.Features)
	raw := model.Predict(snapshot)
	run := Run{AssetID: input.AssetID, AssetClass: assetClass, EventID: input.EventID, SignalAvailableAt: input.SignalAvailableAt.UTC(), HorizonSessions: horizon, Objective: model.Objective, ModelVersion: model.Version, Status: raw.Status, ModelStatus: status, RawScore: raw.RawScore, FeatureSnapshot: snapshot, ExclusionReason: raw.Reason}
	if raw.RawScore != nil {
		if calibrated, ok := s.activeCalibration(ctx, model.Version); ok {
			applied := calibrated.Apply(model.Version, *raw.RawScore, assetClass, input.Market, horizon, input.EventType, input.SignalAvailableAt)
			if applied.Status == "calibrated" {
				run.Status = "calibrated"
				run.Probability = applied.Probability
				run.CalibrationVersion = applied.CalibrationVersion
			} else {
				run.ExclusionReason = applied.Reason
			}
		}
	}
	features, _ := json.Marshal(snapshot)
	run.ID = predictionRunID(run)
	var probabilities any
	if run.Probability != nil {
		probabilities, _ = json.Marshal(map[string]any{"up": run.Probability, "status": "calibrated"})
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO prediction_runs(id,asset_id,asset_class,event_id,signal_available_at,horizon_sessions,objective,model_version,calibration_version,status,model_status,raw_score,probabilities,feature_snapshot,exclusion_reason,idempotency_key) VALUES($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8,NULLIF($9,''),$10,$11,$12,$13,$14,$15,$1) ON CONFLICT(idempotency_key) DO NOTHING`, run.ID, run.AssetID, run.AssetClass, run.EventID, run.SignalAvailableAt, run.HorizonSessions, run.Objective, run.ModelVersion, run.CalibrationVersion, run.Status, run.ModelStatus, run.RawScore, probabilities, features, run.ExclusionReason)
	if err != nil {
		return Run{}, err
	}
	run.Created = tag.RowsAffected() == 1
	return run, nil
}

func predictionRunID(run Run) string {
	identityAssetClass := run.AssetClass
	if strings.EqualFold(identityAssetClass, "equity") {
		// Preserve the pre-P3 idempotency identity for existing equity runs.
		identityAssetClass = ""
	}
	identity, _ := json.Marshal(struct {
		AssetID            string           `json:"asset_id"`
		AssetClass         string           `json:"asset_class,omitempty"`
		EventID            string           `json:"event_id"`
		SignalAvailableAt  time.Time        `json:"signal_available_at"`
		HorizonSessions    int              `json:"horizon_sessions"`
		Objective          string           `json:"objective"`
		ModelVersion       string           `json:"model_version"`
		CalibrationVersion string           `json:"calibration_version"`
		FeatureSnapshot    signals.Snapshot `json:"feature_snapshot"`
	}{run.AssetID, identityAssetClass, run.EventID, run.SignalAvailableAt.UTC(), run.HorizonSessions, run.Objective, run.ModelVersion, run.CalibrationVersion, run.FeatureSnapshot})
	sum := sha256.Sum256(identity)
	return "prediction-" + hex.EncodeToString(sum[:])[:40]
}

func (s *Service) activeCalibration(ctx context.Context, modelVersion string) (calibration.Model, bool) {
	var body []byte
	err := s.db.QueryRow(ctx, `SELECT parameters::jsonb FROM probability_calibrations WHERE model_version=$1 AND status='active' ORDER BY sample_to DESC LIMIT 1`, modelVersion).Scan(&body)
	if err != nil {
		return calibration.Model{}, false
	}
	model := calibration.Model{}
	if json.Unmarshal(body, &model) != nil {
		return calibration.Model{}, false
	}
	return model, true
}

func (s *Service) List(ctx context.Context, assetID string, limit int) ([]Run, error) {
	if limit < 1 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.Query(ctx, `SELECT id,asset_id,asset_class,coalesce(event_id::text,''),signal_available_at,horizon_sessions,objective,model_version,coalesce(calibration_version,''),status,model_status,raw_score,probabilities::jsonb,feature_snapshot::jsonb,exclusion_reason FROM prediction_runs WHERE asset_id=$1 ORDER BY signal_available_at DESC LIMIT $2`, assetID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Run{}
	for rows.Next() {
		var item Run
		var probabilities, features []byte
		if err := rows.Scan(&item.ID, &item.AssetID, &item.AssetClass, &item.EventID, &item.SignalAvailableAt, &item.HorizonSessions, &item.Objective, &item.ModelVersion, &item.CalibrationVersion, &item.Status, &item.ModelStatus, &item.RawScore, &probabilities, &features, &item.ExclusionReason); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(features, &item.FeatureSnapshot)
		if len(probabilities) > 0 {
			var value map[string]*float64
			_ = json.Unmarshal(probabilities, &value)
			item.Probability = value["up"]
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
