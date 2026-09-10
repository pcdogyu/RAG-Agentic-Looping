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

func (s *Server) createFinalHoldoutEvaluation(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := evaluation.FinalHoldoutEvaluationInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	item, created, err := evaluation.NewFinalHoldoutEvaluationStore(s.db).Materialize(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "final_holdout_evaluation": item})
}

func (s *Server) listFinalHoldoutEvaluations(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 200)
	if !ok {
		return
	}
	items, err := evaluation.NewFinalHoldoutEvaluationStore(s.db).List(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "final holdout evaluation query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"contract_version":                    evaluation.FinalHoldoutEvaluationContractVersion,
		"one_evaluation_per_reserved_holdout": true,
		"sample_ids_shown":                    false,
		"automatic_model_selection":           false,
		"items":                               items,
	})
}

func (s *Server) getFinalHoldoutEvaluation(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "evaluationID"))
	if !strings.HasPrefix(id, "final-holdout-") || len(id) > 64 {
		writeError(w, http.StatusUnprocessableEntity, "final holdout evaluation id is invalid")
		return
	}
	item, err := evaluation.NewFinalHoldoutEvaluationStore(s.db).Get(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "final holdout evaluation not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "final holdout evaluation query failed")
		return
	}
	writeJSON(w, http.StatusOK, item)
}
