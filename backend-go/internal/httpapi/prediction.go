package httpapi

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/governance"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/prediction"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

func (s *Server) registerPredictionModel(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := prediction.ModelRegistration{}
	if !decodeJSONBody(w, r, &input) {
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
	run, err := prediction.New(s.db).Predict(r.Context(), prediction.Input{AssetID: assetID, EventID: input.EventID, SignalAvailableAt: input.SignalAvailableAt, ModelVersion: input.ModelVersion, Market: input.Market, EventType: input.EventType, Features: input.Features})
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
