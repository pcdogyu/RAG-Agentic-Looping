package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/evaluation"
)

func (s *Server) createEvaluationHoldout(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := evaluation.HoldoutReservationInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	item, created, err := evaluation.NewDatasetStore(s.db).CreateHoldoutReservation(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "reservation": item})
}

func (s *Server) listEvaluationHoldouts(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 200)
	if !ok {
		return
	}
	items, err := evaluation.NewDatasetStore(s.db).ListHoldouts(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "evaluation holdout query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"contract_version": evaluation.WalkForwardDatasetContractVersion, "usage_policy": evaluation.FinalHoldoutUsagePolicy, "items": items})
}

func (s *Server) createEvaluationDataset(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := evaluation.DatasetBuildInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	item, created, err := evaluation.NewDatasetStore(s.db).Materialize(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "dataset": item})
}

func (s *Server) listEvaluationDatasets(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 200)
	if !ok {
		return
	}
	items, err := evaluation.NewDatasetStore(s.db).ListDatasets(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "evaluation dataset query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"contract_version": evaluation.WalkForwardDatasetContractVersion, "final_holdout_redacted": true, "items": items})
}

func (s *Server) getEvaluationDataset(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "datasetID"))
	if !strings.HasPrefix(id, "dataset-") || len(id) > 64 {
		writeError(w, http.StatusUnprocessableEntity, "dataset_id path is invalid")
		return
	}
	item, err := evaluation.NewDatasetStore(s.db).GetDataset(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "evaluation dataset not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "evaluation dataset query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"final_holdout_redacted": true, "dataset": item})
}
