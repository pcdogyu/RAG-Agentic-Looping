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

func (s *Server) createEvaluationExperiment(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := evaluation.ExperimentBuildInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	item, created, err := evaluation.NewExperimentStore(s.db).Materialize(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "experiment": item})
}

func (s *Server) listEvaluationExperiments(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 200)
	if !ok {
		return
	}
	items, err := evaluation.NewExperimentStore(s.db).List(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "evaluation experiment query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"contract_version": evaluation.DevelopmentExperimentContractVersion,
		"development_only": true, "final_holdout_accessed": false, "items": items})
}

func (s *Server) getEvaluationExperiment(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "experimentID"))
	if !strings.HasPrefix(id, "experiment-") || len(id) > 64 {
		writeError(w, http.StatusUnprocessableEntity, "experiment_id path is invalid")
		return
	}
	item, err := evaluation.NewExperimentStore(s.db).Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "evaluation experiment not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "evaluation experiment query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"development_only": true, "final_holdout_accessed": false, "experiment": item})
}
