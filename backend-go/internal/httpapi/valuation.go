package httpapi

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/valuation"
)

type valuationSubmissionInput struct {
	AsOf              time.Time                    `json:"as_of"`
	ForecastVersionID string                       `json:"forecast_version_id"`
	NetDebtSnapshotID string                       `json:"net_debt_snapshot_id,omitempty"`
	DCFScenarios      []valuation.DCFScenario      `json:"dcf_scenarios"`
	MultipleScenarios []valuation.MultipleScenario `json:"multiple_scenarios"`
	SensitivityWACC   []float64                    `json:"sensitivity_wacc,omitempty"`
	SensitivityGrowth []float64                    `json:"sensitivity_terminal_growth,omitempty"`
}

func (s *Server) valuationRuns(w http.ResponseWriter, r *http.Request) {
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
	items, err := valuation.NewStore(s.db).List(r.Context(), assetID, cutoff, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "valuation run query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "as_of": cutoff.Format(time.RFC3339Nano), "items": items,
		"fundamental_rating": map[string]any{"status": "unavailable", "reason": "valuation is not yet a rating policy"},
		"prediction":         map[string]any{"status": "uncalibrated", "probability": nil},
	})
}

// createValuationRun is admin-only because choosing a comparable multiple,
// capital cost, scenario or net-debt bridge is an accountable analyst action.
func (s *Server) createValuationRun(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := valuationSubmissionInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	run, created, err := valuation.NewStore(s.db).Create(r.Context(), valuation.Submission{
		AssetID: assetID, AsOf: input.AsOf, ForecastVersionID: input.ForecastVersionID, NetDebtSnapshotID: input.NetDebtSnapshotID,
		DCFScenarios: input.DCFScenarios, MultipleScenarios: input.MultipleScenarios, SensitivityWACC: input.SensitivityWACC, SensitivityGrowth: input.SensitivityGrowth,
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
		"created": created, "run": run,
		"fundamental_rating": map[string]any{"status": "unavailable", "reason": "valuation is not yet a rating policy"},
		"prediction":         map[string]any{"status": "uncalibrated", "probability": nil},
	})
}
