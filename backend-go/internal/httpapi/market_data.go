package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
)

func (s *Server) marketPrices(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	availableValue, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	availableAsOf := time.Now().UTC()
	if availableValue != nil {
		availableAsOf = availableValue.(time.Time).UTC()
	}
	observedValue, ok := optionalTimeQuery(w, r, "observed_through")
	if !ok {
		return
	}
	observedThrough := availableAsOf
	if observedValue != nil {
		observedThrough = observedValue.(time.Time).UTC()
	}
	field := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("price_field")))
	if field != "" && field != "close" && field != "adjusted_close" {
		writeError(w, http.StatusUnprocessableEntity, "price_field must be close or adjusted_close")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	items, err := marketdata.NewStore(s.db).ListAvailable(r.Context(), assetID, observedThrough, availableAsOf, field, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "market price observation query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "observed_through": observedThrough.Format(time.RFC3339Nano),
		"as_of": availableAsOf.Format(time.RFC3339Nano), "price_field": field,
		"time_contract_version": marketdata.PriceContractVersion, "items": items,
	})
}

func (s *Server) syncMarketPrices(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	lookbackDays, ok := intQuery(w, r.URL.Query(), "lookback_days", 14, 1, 90)
	if !ok {
		return
	}
	taskID := uuid.NewString()
	queuedID, err := s.enqueueGoModelJob(r.Context(), "masterdata", taskID, "market_loop.sync_market_price_observations", []any{assetID}, map[string]any{"asset_id": assetID, "lookback_days": lookbackDays}, 4, "market-price-observations:"+assetID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "market price observation sync could not be queued")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"task_id": queuedID, "status": "queued", "asset_id": assetID, "lookback_days": lookbackDays,
		"time_contract_version": marketdata.PriceContractVersion, "price_field_required": "adjusted_close",
		"automatic_assumptions": false, "automatic_valuation": false, "automatic_rating": false,
	})
}
