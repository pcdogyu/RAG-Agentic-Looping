package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/analystevidence"
)

func (s *Server) createAnalystEvidence(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := analystevidence.Submission{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	record, created, err := analystevidence.NewStore(s.db).Create(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, record)
}

func (s *Server) analystEvidenceAt(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	cutoffValue, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	cutoff := time.Now().UTC()
	if cutoffValue != nil {
		cutoff = cutoffValue.(time.Time)
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 100)
	if !ok {
		return
	}
	evidenceType := strings.TrimSpace(r.URL.Query().Get("evidence_type"))
	items, err := analystevidence.NewStore(s.db).List(r.Context(), assetID, evidenceType, cutoff, limit)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "as_of": cutoff.Format(time.RFC3339Nano),
		"contract_version": analystevidence.ContractVersion, "items": items,
		"automatic_assumptions": false, "automatic_valuation": false, "automatic_rating": false,
	})
}
