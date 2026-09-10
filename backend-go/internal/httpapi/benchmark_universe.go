package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/evaluation"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
)

func (s *Server) resolveBenchmarkMapping(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	effectiveValue, ok := optionalTimeQuery(w, r, "effective_at")
	if !ok {
		return
	}
	availableValue, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	now := time.Now().UTC()
	effectiveAt, availableAsOf := now, now
	if effectiveValue != nil {
		effectiveAt = effectiveValue.(time.Time).UTC()
	}
	if availableValue != nil {
		availableAsOf = availableValue.(time.Time).UTC()
	}
	var market, currency string
	if err := s.db.QueryRow(r.Context(), `SELECT market,currency FROM assets WHERE id=$1`, assetID).Scan(&market, &currency); err != nil {
		if err == pgx.ErrNoRows {
			writeError(w, http.StatusNotFound, "asset not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "asset identity query failed")
		return
	}
	// Industry is deliberately caller-supplied. Reading today's asset industry
	// for a historical effective_at would leak a later reclassification.
	industryID := strings.TrimSpace(r.URL.Query().Get("industry_id"))
	resolution, err := marketdata.NewStore(s.db).ResolveBenchmark(r.Context(), marketdata.BenchmarkResolutionRequest{
		AssetID: assetID, Market: market, Currency: currency, IndustryID: industryID,
		PolicyID: strings.TrimSpace(r.URL.Query().Get("policy_id")), EffectiveAt: effectiveAt, AvailableAsOf: availableAsOf,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "benchmark mapping query failed")
		return
	}
	identitySource := "exact_asset_market_currency"
	if industryID != "" {
		identitySource = "explicit_historical_industry_context"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "effective_at": effectiveAt.Format(time.RFC3339Nano), "as_of": availableAsOf.Format(time.RFC3339Nano),
		"industry_id": industryID, "identity_source": identitySource, "time_contract_version": marketdata.BenchmarkMappingContractVersion,
		"resolution": resolution,
	})
}

func (s *Server) createBenchmarkMapping(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := marketdata.BenchmarkMappingSubmission{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	item, created, err := marketdata.NewStore(s.db).CreateBenchmarkMapping(r.Context(), input, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "time_contract_version": marketdata.BenchmarkMappingContractVersion, "mapping": item})
}

func (s *Server) securityUniverseSnapshots(w http.ResponseWriter, r *http.Request) {
	universeID := strings.TrimSpace(chi.URLParam(r, "universeID"))
	if universeID == "" || len(universeID) > 160 {
		writeError(w, http.StatusUnprocessableEntity, "universe_id path is invalid")
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
	limit, ok := intQuery(w, r.URL.Query(), "limit", 20, 1, 200)
	if !ok {
		return
	}
	items, err := marketdata.NewStore(s.db).ListUniverseSnapshots(r.Context(), universeID, availableAsOf, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "security universe snapshot query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"universe_id": universeID, "as_of": availableAsOf.Format(time.RFC3339Nano), "time_contract_version": marketdata.SecurityUniverseContractVersion, "items": items})
}

func (s *Server) securityUniverseMemberships(w http.ResponseWriter, r *http.Request) {
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
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	items, err := marketdata.NewStore(s.db).ListAssetUniverseMemberships(r.Context(), assetID, availableAsOf, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "security universe membership query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"asset_id": assetID, "as_of": availableAsOf.Format(time.RFC3339Nano), "time_contract_version": marketdata.SecurityUniverseContractVersion, "items": items})
}

func (s *Server) marketDataQuality(w http.ResponseWriter, r *http.Request) {
	availableValue, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	asOf := time.Now().UTC()
	if availableValue != nil {
		asOf = availableValue.(time.Time).UTC()
	}
	coverageRows, err := s.db.Query(r.Context(), `SELECT a.market,a.currency,count(*)::int,
        count(*) FILTER (WHERE EXISTS(
            SELECT 1 FROM benchmark_mapping_observations m JOIN assets b ON b.id=m.benchmark_asset_id
            WHERE m.subject_market=a.market AND m.subject_currency=a.currency
              AND m.valid_from<=$1 AND (m.valid_to IS NULL OR m.valid_to>$1) AND m.observed_at<=$1 AND m.available_at<=$1
              AND ((m.scope_type='asset' AND m.scope_id=a.id) OR (m.scope_type='market' AND m.scope_id=a.market))
              AND b.market=m.benchmark_market AND b.currency=m.benchmark_currency
        ))::int FROM assets a WHERE a.active=true GROUP BY a.market,a.currency ORDER BY a.market,a.currency`, asOf)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "benchmark coverage query failed")
		return
	}
	benchmarkCoverage := []map[string]any{}
	for coverageRows.Next() {
		var market, currency string
		var active, covered int
		if err := coverageRows.Scan(&market, &currency, &active, &covered); err != nil {
			coverageRows.Close()
			writeError(w, http.StatusInternalServerError, "benchmark coverage scan failed")
			return
		}
		benchmarkCoverage = append(benchmarkCoverage, map[string]any{"market": market, "currency": currency, "active_assets": active, "covered_assets": covered, "missing_assets": active - covered})
	}
	coverageRows.Close()

	snapshotRows, err := s.db.Query(r.Context(), `WITH latest AS (
        SELECT DISTINCT ON (universe_id) universe_id,market,status,available_at,asset_count,included_count,excluded_count,delisted_count,failure_detail
        FROM security_universe_snapshots WHERE available_at<=$1 ORDER BY universe_id,available_at DESC,observed_at DESC,id DESC
      ) SELECT universe_id,market,status,available_at,asset_count,included_count,excluded_count,delisted_count,failure_detail
        FROM latest ORDER BY universe_id`, asOf)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "universe quality query failed")
		return
	}
	universes := []map[string]any{}
	for snapshotRows.Next() {
		var universeID, market, status, failure string
		var availableAt time.Time
		var assetCount, included, excluded, delisted int
		if err := snapshotRows.Scan(&universeID, &market, &status, &availableAt, &assetCount, &included, &excluded, &delisted, &failure); err != nil {
			snapshotRows.Close()
			writeError(w, http.StatusInternalServerError, "universe quality scan failed")
			return
		}
		universes = append(universes, map[string]any{"universe_id": universeID, "market": market, "status": status, "available_at": availableAt, "asset_count": assetCount, "included_count": included, "excluded_count": excluded, "delisted_count": delisted, "failure_detail": failure})
	}
	snapshotRows.Close()

	counts := map[string]int{}
	for key, query := range map[string]string{
		"price_observations":            `SELECT count(*)::int FROM market_price_observations WHERE available_at<=$1`,
		"corporate_action_observations": `SELECT count(*)::int FROM corporate_action_observations WHERE available_at<=$1`,
		"tradability_observations":      `SELECT count(*)::int FROM market_tradability_observations WHERE available_at<=$1`,
		"restricted_tradability":        `SELECT count(*)::int FROM market_tradability_observations WHERE status<>'tradable' AND available_at<=$1`,
		"tradability_source_conflicts": `WITH latest AS (
			SELECT DISTINCT ON (asset_id,source_name,session_date,execution_point) asset_id,source_name,session_date,status,buy_executable,sell_executable
			FROM market_tradability_observations WHERE available_at<=$1
			ORDER BY asset_id,source_name,session_date,execution_point,available_at DESC,observed_at DESC,created_at DESC,id DESC
		) SELECT count(*)::int FROM (
			SELECT asset_id,session_date FROM latest GROUP BY asset_id,session_date
			HAVING count(DISTINCT (status,buy_executable,sell_executable))>1
		) conflicts`,
		"failed_universe_snapshots":     `SELECT count(*)::int FROM security_universe_snapshots WHERE status='failed' AND available_at<=$1`,
		"outcomes_missing_benchmark":    `SELECT count(*)::int FROM outcomes WHERE observed_at<=$1 AND coalesce(payload->>'benchmark_status','unavailable')<>'available'`,
		"mature_prediction_labels":      `SELECT count(*)::int FROM outcome_records WHERE label_definition_version='prediction-outcome-label-v1' AND status='mature' AND label_available_at<=$1`,
		"unavailable_prediction_labels": `SELECT count(*)::int FROM outcome_records WHERE label_definition_version='prediction-outcome-label-v1' AND status='unavailable' AND label_available_at<=$1`,
		"excluded_prediction_labels":    `SELECT count(*)::int FROM outcome_records WHERE label_definition_version='prediction-outcome-label-v1' AND status='excluded' AND label_available_at<=$1`,
		"delisted_prediction_labels":    `SELECT count(*)::int FROM outcome_records WHERE label_definition_version='prediction-outcome-label-v1' AND status='unavailable' AND exclusion_reason IN ('asset_delisted_at_signal','delisting_before_horizon_exit') AND label_available_at<=$1`,
		"symbol_change_unavailable":     `SELECT count(*)::int FROM outcome_records WHERE label_definition_version='prediction-outcome-label-v1' AND status='unavailable' AND exclusion_reason='symbol_change_price_continuity_unavailable' AND label_available_at<=$1`,
		"execution_tradability_missing": `SELECT count(*)::int FROM outcome_records WHERE label_definition_version='prediction-outcome-label-v1' AND status='mature' AND simulation_status='unavailable_tradability_evidence' AND label_available_at<=$1`,
		"pending_prediction_labels":     `SELECT count(*)::int FROM prediction_runs p JOIN prediction_models m ON m.version=p.model_version WHERE m.scope->>'outcome_label_definition_version'='prediction-outcome-label-v1' AND p.signal_available_at<=$1 AND NOT EXISTS(SELECT 1 FROM outcome_records o WHERE o.prediction_run_id=p.id)`,
	} {
		var value int
		if err := s.db.QueryRow(r.Context(), query, asOf).Scan(&value); err != nil {
			writeError(w, http.StatusInternalServerError, "market data quality count failed")
			return
		}
		counts[key] = value
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"as_of": asOf.Format(time.RFC3339Nano), "benchmark_contract_version": marketdata.BenchmarkMappingContractVersion,
		"universe_contract_version": marketdata.SecurityUniverseContractVersion, "tradability_contract_version": marketdata.TradabilityContractVersion,
		"outcome_label_definition_version": evaluation.OutcomeLabelDefinitionVersion, "benchmark_coverage": benchmarkCoverage,
		"latest_universes": universes, "counts": counts,
		"handling": map[string]any{
			"missing_benchmark":       "relative_return_unavailable_and_excluded_from_relative_aggregates",
			"provider_member_missing": "excluded_not_assumed_delisted", "provider_failure": "retained_without_deactivating_assets",
			"delisting":             "explicit_terminal_samples_retained_as_unavailable_not_deleted",
			"symbol_change":         "missing_price_continuity_becomes_unavailable_after_horizon_specific_grace",
			"suspension":            "trading_session_horizon_remains_pending_until_sessions_resume",
			"price_limit_execution": "net_return_unavailable_without_explicit_entry_and_exit_tradability_evidence",
			"tradability_conflict":  "all_latest_sources_must_agree_otherwise_execution_remains_unavailable",
			"historical_industry":   "must_be_supplied_from_point_in_time_context",
			"historical_membership": "resolved_at_signal_cutoff_not_from_current_classification",
		},
	})
}

func (s *Server) predictionOutcomeLabels(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 200)
	if !ok {
		return
	}
	items, err := evaluation.NewOutcomeStore(s.db).ListByAsset(r.Context(), assetID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "prediction outcome label query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"asset_id": assetID, "definition_version": evaluation.OutcomeLabelDefinitionVersion, "items": items})
}
