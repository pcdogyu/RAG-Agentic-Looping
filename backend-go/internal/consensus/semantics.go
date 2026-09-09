package consensus

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const AnnouncementSemanticsVersion = "announcement-expectation-semantics-v1"

type YearOverYearComparison struct {
	Status           string   `json:"status"`
	Reason           string   `json:"reason,omitempty"`
	Direction        string   `json:"direction"`
	PreviousValue    *float64 `json:"previous_value"`
	CurrentValue     *float64 `json:"current_value"`
	AbsoluteChange   *float64 `json:"absolute_change"`
	PercentageChange *float64 `json:"percentage_change"`
	PercentageStatus string   `json:"percentage_status"`
}

type AnnouncementAssessment struct {
	Version       string                 `json:"version"`
	Status        string                 `json:"status"`
	SemanticState string                 `json:"semantic_state"`
	YearOverYear  YearOverYearComparison `json:"year_over_year"`
	Consensus     Comparison             `json:"consensus"`
}

// CompareYearOverYear keeps business growth separate from market surprise.
// A percentage is withheld when the prior value is zero or negative because
// the simple percentage would be misleading; the absolute change remains.
func CompareYearOverYear(current, previous Actual) YearOverYearComparison {
	result := YearOverYearComparison{Status: "unavailable", Reason: "incompatible_or_missing_year_over_year_actual", Direction: "unavailable", PercentageStatus: "unavailable"}
	if current.AssetID == "" || current.Metric == "" || current.FiscalPeriodEnd.IsZero() || previous.FiscalPeriodEnd.IsZero() {
		return result
	}
	if strings.TrimSpace(current.FiscalPeriod) == "" || strings.TrimSpace(previous.FiscalPeriod) == "" ||
		strings.TrimSpace(current.AccountingBasis) == "" || strings.TrimSpace(previous.AccountingBasis) == "" ||
		strings.TrimSpace(current.Currency) == "" || strings.TrimSpace(previous.Currency) == "" ||
		strings.TrimSpace(current.Unit) == "" || strings.TrimSpace(previous.Unit) == "" {
		result.Reason = "missing_period_currency_unit_or_accounting_basis"
		return result
	}
	if !strings.EqualFold(current.AssetID, previous.AssetID) || !strings.EqualFold(current.Metric, previous.Metric) ||
		!strings.EqualFold(strings.TrimSpace(current.AccountingBasis), strings.TrimSpace(previous.AccountingBasis)) ||
		!strings.EqualFold(strings.TrimSpace(current.Currency), strings.TrimSpace(previous.Currency)) ||
		!strings.EqualFold(strings.TrimSpace(current.Unit), strings.TrimSpace(previous.Unit)) {
		return result
	}
	if current.FiscalPeriod != "" && previous.FiscalPeriod != "" && !strings.EqualFold(current.FiscalPeriod, previous.FiscalPeriod) {
		result.Reason = "fiscal_period_mismatch"
		return result
	}
	days := dateUTC(current.FiscalPeriodEnd).Sub(dateUTC(previous.FiscalPeriodEnd)).Hours() / 24
	if days < 330 || days > 400 {
		result.Reason = "not_comparable_year_over_year_periods"
		return result
	}
	change := current.Value - previous.Value
	currentValue, previousValue := current.Value, previous.Value
	result.Status, result.Reason = "available", ""
	result.CurrentValue, result.PreviousValue, result.AbsoluteChange = &currentValue, &previousValue, &change
	switch {
	case change > 0:
		result.Direction = "growth"
	case change < 0:
		result.Direction = "decline"
	default:
		result.Direction = "unchanged"
	}
	if previous.Value > 0 {
		percentage := change / previous.Value
		result.PercentageChange, result.PercentageStatus = &percentage, "available"
	} else {
		result.PercentageStatus = "unavailable_non_positive_prior"
	}
	return result
}

func AssessAnnouncement(current, previous Actual, estimates []Estimate, announcementAt time.Time) AnnouncementAssessment {
	yoy := CompareYearOverYear(current, previous)
	surprise := CompareActualToPreAnnouncementConsensus(current, estimates, announcementAt)
	status := "unavailable"
	if yoy.Status == "available" && surprise.Status == "available" {
		status = "available"
	} else if yoy.Status == "available" || surprise.Status == "available" {
		status = "partial"
	}
	semantic := "unavailable"
	if yoy.Status == "available" {
		if surprise.Status == "available" {
			semantic = yoy.Direction + "_" + surprise.SurpriseDirection
		} else {
			semantic = yoy.Direction + "_consensus_unavailable"
		}
	}
	return AnnouncementAssessment{Version: AnnouncementSemanticsVersion, Status: status, SemanticState: semantic, YearOverYear: yoy, Consensus: surprise}
}

type EstimateRevision struct {
	PreviousID               string    `json:"previous_id"`
	CurrentID                string    `json:"current_id"`
	AssetID                  string    `json:"asset_id"`
	Metric                   string    `json:"metric"`
	FiscalPeriodEnd          time.Time `json:"fiscal_period_end"`
	AccountingBasis          string    `json:"accounting_basis"`
	Statistic                string    `json:"statistic"`
	PreviousValue            float64   `json:"previous_value"`
	CurrentValue             float64   `json:"current_value"`
	AbsoluteChange           float64   `json:"absolute_change"`
	Direction                string    `json:"direction"`
	PreviousAnalystCount     *int      `json:"previous_analyst_count,omitempty"`
	CurrentAnalystCount      *int      `json:"current_analyst_count,omitempty"`
	ObservedAt               time.Time `json:"observed_at"`
	Coverage                 string    `json:"coverage"`
	IndividualBehaviorStatus string    `json:"individual_analyst_behavior_status"`
}

func BuildEstimateRevisions(items []Estimate) []EstimateRevision {
	ordered := append([]Estimate{}, items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].AvailableAt.Equal(ordered[j].AvailableAt) {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].AvailableAt.Before(ordered[j].AvailableAt)
	})
	latest := map[string]Estimate{}
	revisions := []EstimateRevision{}
	for _, item := range ordered {
		key := estimateRevisionKey(item)
		previous, exists := latest[key]
		latest[key] = item
		if !exists || previous.Value == item.Value && sameOptionalInt(previous.AnalystCount, item.AnalystCount) {
			continue
		}
		change := item.Value - previous.Value
		revisions = append(revisions, EstimateRevision{
			PreviousID: previous.ID, CurrentID: item.ID, AssetID: item.AssetID, Metric: item.Metric,
			FiscalPeriodEnd: dateUTC(item.FiscalPeriodEnd), AccountingBasis: item.AccountingBasis, Statistic: item.Statistic,
			PreviousValue: previous.Value, CurrentValue: item.Value, AbsoluteChange: change, Direction: changeDirection(change),
			PreviousAnalystCount: previous.AnalystCount, CurrentAnalystCount: item.AnalystCount, ObservedAt: item.AvailableAt.UTC(),
			Coverage: "provider_aggregate_snapshot", IndividualBehaviorStatus: "unavailable_aggregate_snapshots_only",
		})
	}
	return revisions
}

type GuidanceRevision struct {
	Status           string    `json:"status"`
	Reason           string    `json:"reason,omitempty"`
	PreviousID       string    `json:"previous_id"`
	CurrentID        string    `json:"current_id"`
	Metric           string    `json:"metric"`
	FiscalPeriodEnd  time.Time `json:"fiscal_period_end"`
	PreviousLow      *float64  `json:"previous_low"`
	PreviousHigh     *float64  `json:"previous_high"`
	CurrentLow       *float64  `json:"current_low"`
	CurrentHigh      *float64  `json:"current_high"`
	MidpointChange   *float64  `json:"midpoint_change"`
	Direction        string    `json:"direction"`
	RangeWidthChange *float64  `json:"range_width_change"`
	RangeChange      string    `json:"range_change"`
	ChangedBounds    []string  `json:"changed_bounds"`
	AvailableAt      time.Time `json:"available_at"`
}

func CompareGuidanceRevision(previous, current Guidance) GuidanceRevision {
	result := GuidanceRevision{Status: "unavailable", Reason: "guidance_metric_period_currency_unit_or_basis_mismatch", PreviousID: previous.ID, CurrentID: current.ID, Metric: current.Metric, FiscalPeriodEnd: dateUTC(current.FiscalPeriodEnd), PreviousLow: previous.LowValue, PreviousHigh: previous.HighValue, CurrentLow: current.LowValue, CurrentHigh: current.HighValue, Direction: "unavailable", RangeChange: "unavailable", ChangedBounds: []string{}, AvailableAt: current.AvailableAt.UTC()}
	if previous.AssetID == "" || current.AssetID == "" || previous.Metric == "" || current.Metric == "" ||
		!strings.EqualFold(previous.AssetID, current.AssetID) || !strings.EqualFold(previous.Metric, current.Metric) ||
		!sameDate(previous.FiscalPeriodEnd, current.FiscalPeriodEnd) ||
		!strings.EqualFold(strings.TrimSpace(previous.AccountingBasis), strings.TrimSpace(current.AccountingBasis)) ||
		!strings.EqualFold(strings.TrimSpace(previous.Currency), strings.TrimSpace(current.Currency)) ||
		!strings.EqualFold(strings.TrimSpace(previous.Unit), strings.TrimSpace(current.Unit)) {
		return result
	}
	if !current.AvailableAt.After(previous.AvailableAt) {
		result.Reason = "guidance_revision_not_forward_in_time"
		return result
	}
	previousMidpoint, previousOK := guidanceMidpoint(previous)
	currentMidpoint, currentOK := guidanceMidpoint(current)
	if !previousOK || !currentOK {
		result.Reason = "guidance_revision_has_no_comparable_bound"
		return result
	}
	change := currentMidpoint - previousMidpoint
	result.Status, result.Reason, result.MidpointChange, result.Direction = "available", "", &change, changeDirection(change)
	if !sameOptionalFloat(previous.LowValue, current.LowValue) {
		result.ChangedBounds = append(result.ChangedBounds, "low")
	}
	if !sameOptionalFloat(previous.HighValue, current.HighValue) {
		result.ChangedBounds = append(result.ChangedBounds, "high")
	}
	if previous.LowValue != nil && previous.HighValue != nil && current.LowValue != nil && current.HighValue != nil {
		widthChange := (*current.HighValue - *current.LowValue) - (*previous.HighValue - *previous.LowValue)
		result.RangeWidthChange = &widthChange
		switch {
		case widthChange > 0:
			result.RangeChange = "widened"
		case widthChange < 0:
			result.RangeChange = "narrowed"
		default:
			result.RangeChange = "unchanged"
		}
	}
	return result
}

func BuildGuidanceRevisions(items []Guidance) []GuidanceRevision {
	ordered := append([]Guidance{}, items...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].AvailableAt.Before(ordered[j].AvailableAt) })
	latest := map[string]Guidance{}
	result := []GuidanceRevision{}
	for _, item := range ordered {
		key := guidanceRevisionKey(item)
		previous, exists := latest[key]
		latest[key] = item
		if !exists || sameOptionalFloat(previous.LowValue, item.LowValue) && sameOptionalFloat(previous.HighValue, item.HighValue) {
			continue
		}
		result = append(result, CompareGuidanceRevision(previous, item))
	}
	return result
}

func estimateRevisionKey(item Estimate) string {
	return strings.Join([]string{item.AssetID, strings.ToLower(item.Metric), dateUTC(item.FiscalPeriodEnd).Format("2006-01-02"), strings.ToLower(item.AccountingBasis), strings.ToLower(item.Statistic), item.SourceName}, "\x1f")
}

func guidanceRevisionKey(item Guidance) string {
	return strings.Join([]string{item.AssetID, strings.ToLower(item.Metric), dateUTC(item.FiscalPeriodEnd).Format("2006-01-02"), strings.ToLower(item.AccountingBasis), strings.ToUpper(item.Currency), strings.ToLower(item.Unit), item.SourceName}, "\x1f")
}

func guidanceMidpoint(item Guidance) (float64, bool) {
	if item.LowValue != nil && item.HighValue != nil {
		return (*item.LowValue + *item.HighValue) / 2, true
	}
	if item.LowValue != nil {
		return *item.LowValue, true
	}
	if item.HighValue != nil {
		return *item.HighValue, true
	}
	return 0, false
}

func changeDirection(value float64) string {
	switch {
	case value > 0:
		return "raised"
	case value < 0:
		return "lowered"
	default:
		return "unchanged"
	}
}

func sameOptionalInt(left, right *int) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func sameOptionalFloat(left, right *float64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func ValidateAssessmentActuals(current, previous Actual, announcementAt time.Time) error {
	if current.AssetID == "" || current.Metric == "" || current.FiscalPeriodEnd.IsZero() || current.AvailableAt.IsZero() || previous.AssetID == "" || previous.Metric == "" || previous.FiscalPeriodEnd.IsZero() || announcementAt.IsZero() {
		return fmt.Errorf("current actual, prior actual and announcement_at are required")
	}
	return nil
}
