package httpapi

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentals"
)

// fundamentalsAt exposes stored, normalized financial facts for inspection and
// future P1 consumers. It intentionally returns no valuation, forecast or
// fundamental-rating fields.
func (s *Server) fundamentalsAt(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	assetID = strings.TrimSpace(assetID)
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

// syncFundamentals queues a bounded FMP refresh for one US equity. It is an
// admin-only command and never turns financial statements into a rating.
func (s *Server) syncFundamentals(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 12, 1, 40)
	if !ok {
		return
	}
	taskID := uuid.NewString()
	queuedID, err := s.enqueueGoModelJob(r.Context(), "masterdata", taskID, "market_loop.sync_fundamental_snapshots", []any{assetID}, map[string]any{"asset_id": assetID, "limit": limit}, 4, "fundamental-snapshot:"+assetID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "fundamental snapshot sync could not be queued")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"task_id": queuedID, "status": "queued", "asset_id": assetID, "limit": limit,
		"source": "FMP", "time_contract_version": fundamentals.TimeContractVersion,
	})
}

func fundamentalAssetID(value string) (string, error) {
	assetID, err := url.PathUnescape(value)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(assetID), nil
}
