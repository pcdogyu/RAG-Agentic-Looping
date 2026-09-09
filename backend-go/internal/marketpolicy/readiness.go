package marketpolicy

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const MarketModelReadinessVersion = "market-model-readiness-v1"

type ReadinessSegment struct {
	AssetClass            string   `json:"asset_class"`
	Market                string   `json:"market"`
	Currency              string   `json:"currency"`
	TimeZone              string   `json:"time_zone"`
	Calendar              string   `json:"calendar"`
	BenchmarkID           string   `json:"benchmark_id,omitempty"`
	BenchmarkPolicy       string   `json:"benchmark_policy"`
	FundamentalMethod     string   `json:"fundamental_method"`
	RequiredInputs        []string `json:"required_inputs"`
	ExecutionConstraints  []string `json:"execution_constraints"`
	ActiveAssets          int      `json:"active_assets"`
	ShadowModels          int      `json:"shadow_models"`
	ApprovedModels        int      `json:"approved_models"`
	PredictionRuns        int      `json:"prediction_runs"`
	CalibratedRuns        int      `json:"calibrated_runs"`
	MatureOutcomes        int      `json:"mature_outcomes"`
	FundamentalStatus     string   `json:"fundamental_status"`
	EffectReportStatus    string   `json:"effect_report_status"`
	AcceptanceStatus      string   `json:"acceptance_status"`
	Reasons               []string `json:"reasons"`
	AutomaticModelRelease bool     `json:"automatic_model_release"`
}

type MarketModelReadinessReport struct {
	Version                  string             `json:"version"`
	Segmentation             string             `json:"segmentation"`
	PooledReport             bool               `json:"pooled_report"`
	CoreEquityMarketAccepted bool               `json:"core_equity_market_accepted"`
	MinimumForwardSamples    int                `json:"minimum_forward_samples"`
	Segments                 []ReadinessSegment `json:"segments"`
}

func BuildMarketModelReadiness(ctx context.Context, db *pgxpool.Pool) (MarketModelReadinessReport, error) {
	if db == nil {
		return MarketModelReadinessReport{}, fmt.Errorf("market readiness store is unavailable")
	}
	scopes := [][2]string{{"equity", "US"}, {"equity", "CN"}, {"equity", "HK"}, {"etf", "US"}, {"etf", "CN"}, {"etf", "HK"}, {"crypto", "CRYPTO"}, {"commodity", "COMMODITY"}}
	result := MarketModelReadinessReport{Version: MarketModelReadinessVersion, Segmentation: "asset_class:market", PooledReport: false, MinimumForwardSamples: 100, Segments: []ReadinessSegment{}}
	for _, scope := range scopes {
		policy := Resolve(scope[0], scope[1])
		segment := ReadinessSegment{AssetClass: scope[0], Market: scope[1], Currency: policy.Currency, TimeZone: policy.TimeZone,
			Calendar: policy.Calendar, BenchmarkID: policy.BenchmarkID, BenchmarkPolicy: policy.BenchmarkPolicy, FundamentalMethod: policy.FundamentalMethod,
			RequiredInputs: policy.RequiredInputs, ExecutionConstraints: policy.ExecutionConstraints, FundamentalStatus: "not_applicable", EffectReportStatus: "unavailable",
			AcceptanceStatus: "blocked", Reasons: []string{}, AutomaticModelRelease: false}
		if policy.FundamentalSupported {
			segment.FundamentalStatus = "supported"
		}
		err := db.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM assets WHERE active=true AND asset_class=$1 AND market=$2)::int,
			(SELECT count(*) FROM prediction_models WHERE coalesce(scope->>'asset_class','equity')=$1 AND market=$2 AND status='shadow')::int,
			(SELECT count(*) FROM prediction_models WHERE coalesce(scope->>'asset_class','equity')=$1 AND market=$2 AND status='approved')::int,
			(SELECT count(*) FROM prediction_runs WHERE asset_class=$1 AND EXISTS(SELECT 1 FROM assets WHERE assets.id=prediction_runs.asset_id AND assets.market=$2))::int,
			(SELECT count(*) FROM prediction_runs WHERE asset_class=$1 AND calibration_version IS NOT NULL AND EXISTS(SELECT 1 FROM assets WHERE assets.id=prediction_runs.asset_id AND assets.market=$2))::int,
			(SELECT count(*) FROM outcome_records outcome JOIN prediction_runs prediction ON prediction.id=outcome.prediction_run_id
				WHERE prediction.asset_class=$1 AND outcome.status='mature' AND EXISTS(SELECT 1 FROM assets WHERE assets.id=prediction.asset_id AND assets.market=$2))::int`,
			scope[0], scope[1]).Scan(&segment.ActiveAssets, &segment.ShadowModels, &segment.ApprovedModels, &segment.PredictionRuns,
			&segment.CalibratedRuns, &segment.MatureOutcomes)
		if err != nil {
			return result, fmt.Errorf("load market readiness for %s:%s: %w", scope[0], scope[1], err)
		}
		result.Segments = append(result.Segments, segment)
	}
	for _, segment := range result.Segments {
		if segment.AssetClass == "equity" && segment.ApprovedModels > 0 && segment.MatureOutcomes >= result.MinimumForwardSamples && segment.CalibratedRuns >= result.MinimumForwardSamples {
			result.CoreEquityMarketAccepted = true
			break
		}
	}
	for index := range result.Segments {
		classifyReadiness(&result.Segments[index], result.CoreEquityMarketAccepted, result.MinimumForwardSamples)
	}
	return result, nil
}

func classifyReadiness(segment *ReadinessSegment, coreAccepted bool, minimum int) {
	if segment.AcceptanceStatus == "" {
		segment.AcceptanceStatus = "blocked"
	}
	if segment.EffectReportStatus == "" {
		segment.EffectReportStatus = "unavailable"
	}
	if segment.ActiveAssets == 0 {
		segment.Reasons = append(segment.Reasons, "no_active_assets")
	}
	if segment.AssetClass != "equity" && !coreAccepted {
		segment.Reasons = append(segment.Reasons, "core_equity_market_not_yet_accepted")
	}
	if segment.ApprovedModels == 0 {
		if segment.ShadowModels > 0 {
			segment.Reasons = append(segment.Reasons, "shadow_model_not_approved")
		} else {
			segment.Reasons = append(segment.Reasons, "approved_model_unavailable")
		}
	}
	if segment.MatureOutcomes < minimum {
		segment.Reasons = append(segment.Reasons, "insufficient_mature_forward_outcomes")
	}
	if segment.CalibratedRuns < minimum {
		segment.Reasons = append(segment.Reasons, "insufficient_scope_specific_calibration_evidence")
	}
	if len(segment.Reasons) == 0 {
		segment.AcceptanceStatus = "eligible_for_human_acceptance"
		segment.EffectReportStatus = "scope_specific_evidence_available"
	} else if segment.PredictionRuns > 0 {
		segment.EffectReportStatus = "partial_scope_specific_evidence"
	}
}
