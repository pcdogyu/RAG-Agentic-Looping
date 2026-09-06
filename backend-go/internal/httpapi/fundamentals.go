package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentals"
)

// fundamentalsAt exposes stored, normalized financial facts for inspection and
// future P1 consumers. It intentionally returns no valuation, forecast or
// fundamental-rating fields.
func (s *Server) fundamentalsAt(w http.ResponseWriter, r *http.Request) {
	assetID := strings.TrimSpace(chi.URLParam(r, "assetID"))
	if assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id is required")
		return
	}
	cutoffValue, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	cutoff := time.Now().UTC()
	if cutoffValue != nil {
		var typed bool
		cutoff, typed = cutoffValue.(time.Time)
		if !typed {
			writeError(w, http.StatusInternalServerError, "invalid fundamental as_of cutoff")
			return
		}
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 30, 1, 100)
	if !ok {
		return
	}
	items, err := fundamentals.NewStore(s.db).ListAvailable(r.Context(), assetID, cutoff, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "fundamental snapshot query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id":              assetID,
		"as_of":                 cutoff.UTC().Format(time.RFC3339Nano),
		"time_contract_version": fundamentals.TimeContractVersion,
		"fundamental_rating":    map[string]any{"status": "unavailable", "reason": "P1 financial facts are not a fundamental rating"},
		"items":                 items,
	})
}
