package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
)

func (s *Server) marketTradability(w http.ResponseWriter, r *http.Request) {
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
	sessionValue, ok := optionalTimeQuery(w, r, "session_through")
	if !ok {
		return
	}
	sessionThrough := availableAsOf
	if sessionValue != nil {
		sessionThrough = sessionValue.(time.Time).UTC()
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 1000)
	if !ok {
		return
	}
	items, err := marketdata.NewStore(s.db).ListTradabilityAvailable(r.Context(), assetID, sessionThrough, availableAsOf, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "market tradability observation query failed")
		return
	}
	bySession := map[string][]marketdata.TradabilityObservation{}
	for _, item := range items {
		key := item.SessionDate.UTC().Format("2006-01-02")
		bySession[key] = append(bySession[key], item)
	}
	resolved := map[string]marketdata.ResolvedTradability{}
	for session, values := range bySession {
		date, _ := time.Parse("2006-01-02", session)
		resolved[session] = marketdata.ResolveTradability(values, date)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "session_through": sessionThrough.Format(time.RFC3339Nano), "as_of": availableAsOf.Format(time.RFC3339Nano),
		"contract_version": marketdata.TradabilityContractVersion, "execution_point": "market_close",
		"resolution_rule": "all_latest_sources_must_agree; only tradable permits execution", "items": items, "resolved_by_session": resolved,
	})
}

func (s *Server) importMarketTradability(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := marketdata.TradabilityImport{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	receipt, created, err := marketdata.NewStore(s.db).ImportTradability(r.Context(), assetID, input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{
		"created": created, "receipt": receipt, "contract_version": marketdata.TradabilityImportContractVersion,
		"available_at_authority": "server_ingestion_time", "automatic_outcome_evaluation": false,
		"automatic_rating": false, "execution_permission": "requires_all_latest_sources_to_agree_on_tradable",
	})
}

func (s *Server) marketTradabilityImports(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 200)
	if !ok {
		return
	}
	items, err := marketdata.NewStore(s.db).ListTradabilityImports(r.Context(), assetID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "market tradability import receipt query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "contract_version": marketdata.TradabilityImportContractVersion, "items": items,
		"administrator_only": true, "license_and_approval_details_public": false,
	})
}
