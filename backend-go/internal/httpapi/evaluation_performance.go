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

func (s *Server) createEvaluationPerformanceReport(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := evaluation.PerformanceReportInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	item, created, err := evaluation.NewPerformanceReportStore(s.db).Materialize(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "performance_report": item})
}

func (s *Server) listEvaluationPerformanceReports(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 200)
	if !ok {
		return
	}
	items, err := evaluation.NewPerformanceReportStore(s.db).List(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "performance report query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"contract_version": evaluation.LayeredPerformanceReportContractVersion,
		"development_only": true, "final_holdout_accessed": false, "items": items})
}

func (s *Server) getEvaluationPerformanceReport(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	item, err := evaluation.NewPerformanceReportStore(s.db).Get(r.Context(), chi.URLParam(r, "reportID"))
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "performance report not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "performance report query failed")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) createResearchQualityReview(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := evaluation.ResearchQualityReviewInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	item, created, err := evaluation.NewResearchQualityReviewStore(s.db).Create(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "review": item})
}

func (s *Server) listResearchQualityReviews(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 200)
	if !ok {
		return
	}
	items, err := evaluation.NewResearchQualityReviewStore(s.db).List(r.Context(), r.URL.Query().Get("research_run_id"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "research quality review query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
