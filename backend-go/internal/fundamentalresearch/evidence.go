package fundamentalresearch

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/analystevidence"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/forecast"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/rating"
)

func validateManualResearchEvidence(ctx context.Context, db *pgxpool.Pool, input Input) error {
	if err := validateManualPriceEvidence(ctx, db, input.AssetID, input.Rating.AsOfPriceEvidenceID, input.Rating.AsOfPrice, input.AsOf); err != nil {
		return err
	}
	if err := validateValuationEvidence(ctx, db, input.AssetID, input.Valuation, input.AsOf); err != nil {
		return err
	}
	if input.Rating.Policy.RelativeRequired {
		if err := validateBenchmarkEvidence(ctx, db, input.AssetID, input.Rating.Policy.BenchmarkID, input.Rating.BenchmarkEvidenceID, input.Rating.BenchmarkReturn, input.AsOf); err != nil {
			return err
		}
	}
	if err := validateReasonEvidence(ctx, db, input.AssetID, input.Rating.ReasonCodes, input.Rating.EvidenceIDs, input.AsOf); err != nil {
		return err
	}
	if err := validateInvalidationEvidence(ctx, db, input.AssetID, input.Rating.InvalidationRules, input.AsOf); err != nil {
		return err
	}
	return validateAssumptionEvidence(ctx, db, input.AssetID, input.Forecast.Assumptions, input.AsOf)
}

func validateScheduledResearchEvidence(ctx context.Context, db *pgxpool.Pool, assetID string, version forecast.Version, valuationPlan ValuationPlan, ratingPlan ScheduledRatingPlan, cutoff time.Time) error {
	if err := validateValuationEvidence(ctx, db, assetID, valuationPlan, cutoff); err != nil {
		return err
	}
	if ratingPlan.Policy.RelativeRequired {
		if err := validateBenchmarkEvidence(ctx, db, assetID, ratingPlan.Policy.BenchmarkID, ratingPlan.BenchmarkEvidenceID, ratingPlan.BenchmarkReturn, cutoff); err != nil {
			return err
		}
	}
	if err := validateReasonEvidence(ctx, db, assetID, ratingPlan.ReasonCodes, ratingPlan.EvidenceIDs, cutoff); err != nil {
		return err
	}
	if err := validateInvalidationEvidence(ctx, db, assetID, ratingPlan.InvalidationRules, cutoff); err != nil {
		return err
	}
	return validateAssumptionEvidence(ctx, db, assetID, version.Assumptions, cutoff)
}

func validateManualPriceEvidence(ctx context.Context, db *pgxpool.Pool, assetID, evidenceID string, expected *float64, cutoff time.Time) error {
	if expected == nil || strings.TrimSpace(evidenceID) == "" {
		return fmt.Errorf("manual research requires adjusted-close price evidence")
	}
	var actual float64
	err := db.QueryRow(ctx, `SELECT price FROM market_price_observations WHERE id=$1 AND asset_id=$2 AND price_field='adjusted_close' AND available_at<=$3`, strings.TrimSpace(evidenceID), assetID, cutoff.UTC()).Scan(&actual)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("as_of_price evidence is absent, not adjusted_close, belongs to another asset, or was unavailable at as_of")
	}
	if err != nil {
		return fmt.Errorf("validate as_of_price evidence: %w", err)
	}
	if !sameNumber(actual, *expected) {
		return fmt.Errorf("as_of_price does not match its immutable evidence")
	}
	return nil
}

func validateValuationEvidence(ctx context.Context, db *pgxpool.Pool, assetID string, plan ValuationPlan, cutoff time.Time) error {
	store := analystevidence.NewStore(db)
	for _, scenario := range plan.MultipleScenarios {
		items, err := store.Require(ctx, assetID, scenario.ComparableEvidenceIDs, analystevidence.ValuationMultiple, cutoff)
		if err != nil {
			return fmt.Errorf("multiple scenario %q: %w", scenario.Name, err)
		}
		for _, item := range items {
			value, ok := analystevidence.NumericValue(item, "selected_multiple")
			if !ok || !sameNumber(value, scenario.PriceEarningsMultiple) {
				return fmt.Errorf("multiple scenario %q does not match analyst evidence %q", scenario.Name, item.ID)
			}
		}
	}
	for _, scenario := range plan.DCFScenarios {
		items, err := store.Require(ctx, assetID, scenario.CostOfCapitalEvidenceIDs, analystevidence.CostOfCapital, cutoff)
		if err != nil {
			return fmt.Errorf("DCF scenario %q: %w", scenario.Name, err)
		}
		for _, item := range items {
			value, ok := analystevidence.NumericValue(item, "wacc")
			if !ok || !sameNumber(value, scenario.WACC) {
				return fmt.Errorf("DCF scenario %q does not match analyst evidence %q", scenario.Name, item.ID)
			}
		}
	}
	return nil
}

func validateBenchmarkEvidence(ctx context.Context, db *pgxpool.Pool, assetID, benchmarkID, evidenceID string, expectedReturn *float64, cutoff time.Time) error {
	if expectedReturn == nil || strings.TrimSpace(evidenceID) == "" {
		return fmt.Errorf("relative rating requires registered benchmark expectation evidence")
	}
	items, err := analystevidence.NewStore(db).Require(ctx, assetID, []string{evidenceID}, analystevidence.BenchmarkExpectation, cutoff)
	if err != nil {
		return err
	}
	value, ok := analystevidence.NumericValue(items[0], "expected_return")
	if !ok || !sameNumber(value, *expectedReturn) || analystevidence.StringValue(items[0], "benchmark_id") != strings.TrimSpace(benchmarkID) {
		return fmt.Errorf("benchmark expectation does not match its approved evidence")
	}
	return nil
}

func validateReasonEvidence(ctx context.Context, db *pgxpool.Pool, assetID string, reasonCodes, evidenceIDs []string, cutoff time.Time) error {
	if len(cleanEvidenceIDs(reasonCodes)) == 0 {
		return fmt.Errorf("rating reason_codes are required")
	}
	if err := validateGenericEvidence(ctx, db, assetID, evidenceIDs, cutoff); err != nil {
		return err
	}
	covered := map[string]bool{}
	store := analystevidence.NewStore(db)
	for _, id := range cleanEvidenceIDs(evidenceIDs) {
		item, err := store.Get(ctx, assetID, id, cutoff)
		if err != nil || item.EvidenceType != analystevidence.RatingRationale {
			continue
		}
		for _, code := range anyStrings(item.Values["reason_codes"]) {
			covered[code] = true
		}
	}
	for _, code := range cleanEvidenceIDs(reasonCodes) {
		if !covered[code] {
			return fmt.Errorf("rating reason code %q lacks matching approved rationale evidence", code)
		}
	}
	return nil
}

func validateInvalidationEvidence(ctx context.Context, db *pgxpool.Pool, assetID string, rules []rating.InvalidationRule, cutoff time.Time) error {
	store := analystevidence.NewStore(db)
	for index, rule := range rules {
		if err := validateGenericEvidence(ctx, db, assetID, rule.EvidenceIDs, cutoff); err != nil {
			return fmt.Errorf("invalidation rule %d: %w", index, err)
		}
		matched := false
		for _, id := range cleanEvidenceIDs(rule.EvidenceIDs) {
			item, err := store.Get(ctx, assetID, id, cutoff)
			if err == nil && item.EvidenceType == analystevidence.InvalidationRule && analystevidence.StringValue(item, "rule_type") == strings.TrimSpace(rule.RuleType) {
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("invalidation rule %d lacks matching approved evidence", index)
		}
	}
	return nil
}

func validateAssumptionEvidence(ctx context.Context, db *pgxpool.Pool, assetID string, assumptions []forecast.Assumption, cutoff time.Time) error {
	store := analystevidence.NewStore(db)
	for index, assumption := range assumptions {
		if err := validateGenericEvidence(ctx, db, assetID, assumption.EvidenceIDs, cutoff); err != nil {
			return fmt.Errorf("forecast assumption %d: %w", index, err)
		}
		matched := false
		for _, id := range cleanEvidenceIDs(assumption.EvidenceIDs) {
			item, err := store.Get(ctx, assetID, id, cutoff)
			if err != nil || item.EvidenceType != analystevidence.ForecastAssumption || analystevidence.StringValue(item, "field") != strings.TrimSpace(assumption.Field) {
				continue
			}
			if value, ok := analystevidence.NumericValue(item, "value"); ok && sameNumber(value, assumption.Value) {
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("forecast assumption %d lacks matching approved evidence", index)
		}
	}
	return nil
}

func validateGenericEvidence(ctx context.Context, db *pgxpool.Pool, assetID string, ids []string, cutoff time.Time) error {
	ids = cleanEvidenceIDs(ids)
	if len(ids) == 0 {
		return fmt.Errorf("evidence_ids are required")
	}
	store := analystevidence.NewStore(db)
	for _, id := range ids {
		if _, err := store.Get(ctx, assetID, id, cutoff); err == nil {
			continue
		} else if err != pgx.ErrNoRows {
			return err
		}
		var exists bool
		err := db.QueryRow(ctx, `SELECT (
			EXISTS(SELECT 1 FROM fundamental_snapshots WHERE id=$1 AND asset_id=$2 AND available_at<=$3) OR
			EXISTS(SELECT 1 FROM consensus_snapshots WHERE id=$1 AND asset_id=$2 AND available_at<=$3) OR
			EXISTS(SELECT 1 FROM management_guidance_snapshots WHERE id=$1 AND asset_id=$2 AND available_at<=$3) OR
			EXISTS(SELECT 1 FROM guidance_source_documents WHERE id=$1 AND asset_id=$2 AND source_available_at<=$3) OR
			EXISTS(SELECT 1 FROM document_chunks WHERE evidence_id=$1 AND asset_id=$2 AND as_of<=$3) OR
			EXISTS(SELECT 1 FROM market_price_observations WHERE id=$1 AND asset_id=$2 AND available_at<=$3)
		)`, id, assetID, cutoff.UTC()).Scan(&exists)
		if err != nil {
			return fmt.Errorf("validate evidence %q: %w", id, err)
		}
		if !exists {
			return fmt.Errorf("evidence %q is absent, belongs to another asset, or was unavailable at cutoff", id)
		}
	}
	return nil
}

func anyStrings(value any) []string {
	items := []string{}
	switch typed := value.(type) {
	case []string:
		items = typed
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok {
				items = append(items, text)
			}
		}
	}
	return cleanEvidenceIDs(items)
}

func cleanEvidenceIDs(values []string) []string {
	seen := map[string]bool{}
	items := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			items = append(items, value)
		}
	}
	return items
}

func sameNumber(left, right float64) bool {
	return math.Abs(left-right) <= 1e-10*math.Max(1, math.Max(math.Abs(left), math.Abs(right)))
}
