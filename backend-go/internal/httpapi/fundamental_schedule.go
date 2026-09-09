package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentalresearch"
)

func (s *Server) fundamentalResearchSchedule(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 100)
	if !ok {
		return
	}
	plans, err := fundamentalresearch.NewPlanStore(s.db).History(r.Context(), assetID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "fundamental research schedule query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"asset_id": assetID, "version": fundamentalresearch.ScheduledResearchVersion, "items": plans})
}

func (s *Server) approveFundamentalResearchSchedule(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := fundamentalresearch.PlanSubmission{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if input.AssetID != "" && strings.TrimSpace(input.AssetID) != assetID {
		writeError(w, http.StatusUnprocessableEntity, "asset_id body must match the path")
		return
	}
	input.AssetID = assetID
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	plan, created, err := fundamentalresearch.NewPlanStore(s.db).Approve(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, plan)
}

func (s *Server) pauseFundamentalResearchSchedule(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	paused, err := fundamentalresearch.NewPlanStore(s.db).Pause(r.Context(), assetID, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "fundamental research schedule pause failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"asset_id": assetID, "paused": paused})
}
