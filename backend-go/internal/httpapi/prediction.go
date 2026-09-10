package httpapi

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/governance"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/prediction"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

// evaluatePredictionOutcomes enqueues the same production task used by the
// daily outcome scheduler. It is an operator cold-start trigger, not an
// alternate evaluator: pending horizons remain pending and every label still
// uses the frozen point-in-time price and benchmark contract.
func (s *Server) evaluatePredictionOutcomes(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	taskID := uuid.NewString()
	queuedID, err := s.enqueueGoModelJob(r.Context(), "outcomes", taskID, "market_loop.evaluate_outcomes", nil, map[string]any{}, 5, "scheduled:market_loop.evaluate_outcomes")
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "prediction outcome evaluation could not be queued")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"task_id": queuedID, "status": "queued", "task_type": "market_loop.evaluate_outcomes",
		"label_definition_version": "prediction-outcome-label-v1", "early_maturity_allowed": false,
	})
}

// predictionOutcomeEvaluationStatus exposes only branch counts and a derived
// summary state. The generic task endpoint remains backward compatible, while
// the administrator workbench never needs to download raw provider failures.
func (s *Server) predictionOutcomeEvaluationStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	taskID, err := uuid.Parse(chi.URLParam(r, "taskID"))
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "task_id path is invalid")
		return
	}
	facts := phaseTwoOutcomeEvaluationFacts{PendingReasons: map[string]int{}}
	var resultJSON []byte
	err = s.db.QueryRow(r.Context(), `SELECT id::text,status,created_at,completed_at,coalesce(result,'{}'::jsonb)::jsonb
		FROM go_jobs WHERE id=$1 AND task_type='market_loop.evaluate_outcomes'`, taskID).Scan(
		&facts.JobID, &facts.Status, &facts.CreatedAt, &facts.CompletedAt, &resultJSON)
	if err == pgx.ErrNoRows {
		writeError(w, http.StatusNotFound, "outcome evaluation task not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "outcome evaluation status query failed")
		return
	}
	if err = applyPhaseTwoOutcomeEvaluationResult(&facts, resultJSON); err != nil {
		writeError(w, http.StatusInternalServerError, "outcome evaluation status decode failed")
		return
	}
	writeJSON(w, http.StatusOK, facts)
}

func (s *Server) registerPredictionModel(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := prediction.ModelRegistration{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	assetClass := "equity"
	if value := input.Scope["asset_class"]; value != nil {
		assetClass = strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
	}
	if assetClass == "" {
		assetClass = "equity"
	}
	if policy := marketpolicy.Resolve(assetClass, input.Market); !policy.PredictionSupported {
		writeError(w, http.StatusUnprocessableEntity, "prediction model scope is unsupported")
		return
	}
	if err := prediction.New(s.db).RegisterModel(r.Context(), input); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"version": input.Model.Version, "status": input.Status})
}
func (s *Server) registerCalibration(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := prediction.CalibrationRegistration{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	calibrationAssetClass := strings.TrimSpace(input.Scope.AssetClass)
	if calibrationAssetClass == "" {
		calibrationAssetClass = "equity"
	}
	if policy := marketpolicy.Resolve(calibrationAssetClass, input.Scope.Market); !policy.PredictionSupported {
		writeError(w, http.StatusUnprocessableEntity, "calibration scope is unsupported")
		return
	}
	model, err := prediction.New(s.db).RegisterCalibration(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, model)
}

type predictionInput struct {
	EventID           string            `json:"event_id,omitempty"`
	SignalAvailableAt time.Time         `json:"signal_available_at"`
	ModelVersion      string            `json:"model_version"`
	Market            string            `json:"market"`
	EventType         string            `json:"event_type"`
	Features          []signals.Feature `json:"features"`
}

func (s *Server) createPrediction(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := predictionInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	asset, err := s.loadAssetPolicy(r.Context(), assetID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if err = validatePredictionPolicy(asset, input.Market); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	run, err := prediction.New(s.db).Predict(r.Context(), prediction.Input{AssetID: assetID, AssetClass: asset.AssetClass, EventID: input.EventID, SignalAvailableAt: input.SignalAvailableAt, ModelVersion: input.ModelVersion, Market: asset.Market, EventType: input.EventType, Features: input.Features})
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if run.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, run)
}
func (s *Server) listPredictions(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 100)
	if !ok {
		return
	}
	items, err := prediction.New(s.db).List(r.Context(), assetID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "prediction query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"asset_id": assetID, "items": items})
}

type shadowPredictionInput struct {
	EventID               string            `json:"event_id,omitempty"`
	SignalAvailableAt     time.Time         `json:"signal_available_at"`
	IncumbentModelVersion string            `json:"incumbent_model_version"`
	CandidateModelVersion string            `json:"candidate_model_version"`
	Market                string            `json:"market"`
	EventType             string            `json:"event_type"`
	Features              []signals.Feature `json:"features"`
	ExecutionAssumptions  map[string]any    `json:"execution_assumptions"`
}

func (s *Server) createShadowComparison(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := shadowPredictionInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	asset, err := s.loadAssetPolicy(r.Context(), assetID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if err = validatePredictionPolicy(asset, input.Market); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	comparison, err := prediction.New(s.db).CompareShadow(r.Context(), prediction.ShadowInput{AssetID: assetID, AssetClass: asset.AssetClass, EventID: input.EventID, SignalAvailableAt: input.SignalAvailableAt, IncumbentModelVersion: input.IncumbentModelVersion, CandidateModelVersion: input.CandidateModelVersion, Market: asset.Market, EventType: input.EventType, Features: input.Features, ExecutionAssumptions: input.ExecutionAssumptions})
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if comparison.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, comparison)
}

func (s *Server) listGovernanceChecks(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 200)
	if !ok {
		return
	}
	items, err := prediction.New(s.db).ListGovernanceChecks(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "governance check query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) promotionCheck(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := struct {
		ModelVersion string `json:"model_version"`
		governance.PromotionInput
	}{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	decision, err := prediction.New(s.db).Promote(r.Context(), input.ModelVersion, input.PromotionInput, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, decision)
}
func (s *Server) calibrationPromotionCheck(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := governance.PromotionInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	decision, err := prediction.New(s.db).PromoteCalibration(r.Context(), chi.URLParam(r, "version"), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, decision)
}
func (s *Server) driftCheck(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := governance.DriftInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	result, err := governance.PopulationStability(input)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) rollbackPredictionModel(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := prediction.RollbackInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	check, err := prediction.New(s.db).Rollback(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, check)
}

func (s *Server) executeFailureDrill(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := prediction.FailureDrillInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	result, err := prediction.New(s.db).ExecuteFailureDrill(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}

func (s *Server) listFailureDrills(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 200)
	if !ok {
		return
	}
	items, err := prediction.New(s.db).ListFailureDrills(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failure drill query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"contract_version": prediction.FailureDrillContractVersion,
		"production_state_changed": false, "items": items})
}
