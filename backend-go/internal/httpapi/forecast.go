package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/forecast"
)

type forecastSubmissionInput struct {
	AsOf                   time.Time             `json:"as_of"`
	ParentVersionID        string                `json:"parent_version_id,omitempty"`
	Inputs                 forecast.Inputs       `json:"inputs"`
	FundamentalSnapshotIDs []string              `json:"fundamental_snapshot_ids"`
	Assumptions            []forecast.Assumption `json:"assumptions"`
}

// forecastVersions exposes point-in-time deterministic scenarios. The output
// is deliberately not a price target, probability, or fundamental rating.
func (s *Server) forecastVersions(w http.ResponseWriter, r *http.Request) {
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
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 100)
	if !ok {
		return
	}
	items, err := forecast.NewStore(s.db).List(r.Context(), assetID, cutoff, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "forecast version query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "as_of": cutoff.Format(time.RFC3339Nano), "items": items,
		"fundamental_rating": map[string]any{"status": "unavailable", "reason": "financial projection is not a fundamental rating"},
		"prediction":         map[string]any{"status": "uncalibrated", "probability": nil},
	})
}

// createForecastVersion is intentionally admin-only. It accepts explicitly
// approved assumptions and source snapshot IDs, then executes only Go's
// deterministic math. Research models cannot write projections directly.
func (s *Server) createForecastVersion(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := forecastSubmissionInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	version, created, err := forecast.NewStore(s.db).Create(r.Context(), forecast.Submission{
		AssetID: assetID, AsOf: input.AsOf, ParentVersionID: strings.TrimSpace(input.ParentVersionID), Inputs: input.Inputs,
		FundamentalSnapshotIDs: input.FundamentalSnapshotIDs, Assumptions: input.Assumptions,
	})
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{
		"created": created, "version": version,
		"fundamental_rating": map[string]any{"status": "unavailable", "reason": "financial projection is not a fundamental rating"},
		"prediction":         map[string]any{"status": "uncalibrated", "probability": nil},
	})
}
