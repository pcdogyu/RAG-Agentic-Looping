package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
)

func (s *Server) corporateActions(w http.ResponseWriter, r *http.Request) {
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
	effectiveValue, ok := optionalTimeQuery(w, r, "effective_through")
	if !ok {
		return
	}
	effectiveThrough := availableAsOf
	if effectiveValue != nil {
		effectiveThrough = effectiveValue.(time.Time).UTC()
	}
	actionType := marketdata.CorporateActionType(strings.ToLower(strings.TrimSpace(r.URL.Query().Get("action_type"))))
	if actionType != "" && !validCorporateActionType(actionType) {
		writeError(w, http.StatusUnprocessableEntity, "action_type is invalid")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	items, err := marketdata.NewStore(s.db).ListCorporateActionsAvailable(r.Context(), assetID, effectiveThrough, availableAsOf, actionType, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "corporate action observation query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "effective_through": effectiveThrough.Format(time.RFC3339Nano),
		"as_of": availableAsOf.Format(time.RFC3339Nano), "action_type": actionType,
		"time_contract_version": marketdata.CorporateActionContractVersion, "items": items,
	})
}

func (s *Server) syncCorporateActions(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 500, 1, 1000)
	if !ok {
		return
	}
	taskID := uuid.NewString()
	queuedID, err := s.enqueueGoModelJob(r.Context(), "masterdata", taskID, "market_loop.sync_corporate_actions", []any{assetID}, map[string]any{"asset_id": assetID, "limit": limit}, 4, "corporate-actions:"+assetID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "corporate action sync could not be queued")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"task_id": queuedID, "status": "queued", "asset_id": assetID, "limit": limit,
		"source": "FMP", "time_contract_version": marketdata.CorporateActionContractVersion,
	})
}

func validCorporateActionType(value marketdata.CorporateActionType) bool {
	switch value {
	case marketdata.CashDividend, marketdata.StockSplit, marketdata.ReverseSplit, marketdata.SymbolChange, marketdata.Suspension, marketdata.Delisting:
		return true
	default:
		return false
	}
}
