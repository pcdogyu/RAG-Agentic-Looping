// Package consensus owns point-in-time analyst-consensus and management-
// guidance contracts. It intentionally does not turn an estimate surprise into
// a price target, forecast, or rating.
package consensus

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const TimeContractVersion = "consensus-point-in-time-v1"

type Estimate struct {
	ID               string     `json:"id"`
	AssetID          string     `json:"asset_id"`
	Metric           string     `json:"metric"`
	FiscalPeriod     string     `json:"fiscal_period,omitempty"`
	FiscalPeriodEnd  time.Time  `json:"fiscal_period_end"`
	AccountingBasis  string     `json:"accounting_basis"`
	Statistic        string     `json:"statistic"`
	Value            float64    `json:"estimate_value"`
	AnalystCount     *int       `json:"analyst_count,omitempty"`
	Currency         string     `json:"currency,omitempty"`
	Unit             string     `json:"unit"`
	PublishedAt      time.Time  `json:"published_at"`
	AvailableAt      time.Time  `json:"available_at"`
	RevisionAt       *time.Time `json:"revision_at,omitempty"`
	SourceName       string     `json:"source_name"`
	SourceURL        string     `json:"source_url"`
	SourceDocumentID string     `json:"source_document_id,omitempty"`
}

type Actual struct {
	AssetID         string    `json:"asset_id"`
	Metric          string    `json:"metric"`
	FiscalPeriodEnd time.Time `json:"fiscal_period_end"`
	AccountingBasis string    `json:"accounting_basis"`
	Value           float64   `json:"value"`
	Currency        string    `json:"currency,omitempty"`
	Unit            string    `json:"unit"`
	AvailableAt     time.Time `json:"available_at"`
}

type Comparison struct {
	Status              string     `json:"status"`
	Reason              string     `json:"reason,omitempty"`
	Metric              string     `json:"metric"`
	FiscalPeriodEnd     time.Time  `json:"fiscal_period_end"`
	ActualValue         *float64   `json:"actual_value"`
	ExpectedValue       *float64   `json:"expected_value"`
	Surprise            *float64   `json:"surprise"`
	SurpriseDirection   string     `json:"surprise_direction"`
	EstimateID          string     `json:"estimate_id,omitempty"`
	EstimateAvailableAt *time.Time `json:"estimate_available_at,omitempty"`
	TimeContractVersion string     `json:"time_contract_version"`
}

// LatestBeforeAnnouncement chooses the latest estimate already available
// strictly before the announcement. Estimates edited at or after the release
// are intentionally unavailable to this comparison.
func LatestBeforeAnnouncement(items []Estimate, assetID, metric string, periodEnd, announcementAt time.Time) *Estimate {
	if announcementAt.IsZero() {
		return nil
	}
	var best *Estimate
	for _, item := range items {
		if item.AssetID != assetID || !strings.EqualFold(item.Metric, metric) || !sameDate(item.FiscalPeriodEnd, periodEnd) || !item.AvailableAt.Before(announcementAt) {
			continue
		}
		if best == nil || estimateLater(item, *best) {
			copy := item
			best = &copy
		}
	}
	return best
}

func CompareActualToPreAnnouncementConsensus(actual Actual, estimates []Estimate, announcementAt time.Time) Comparison {
	result := Comparison{Status: "unavailable", Reason: "no_pre_announcement_consensus", Metric: actual.Metric, FiscalPeriodEnd: dateUTC(actual.FiscalPeriodEnd), SurpriseDirection: "unavailable", TimeContractVersion: TimeContractVersion}
	if actual.AssetID == "" || actual.Metric == "" || actual.FiscalPeriodEnd.IsZero() || announcementAt.IsZero() {
		result.Reason = "missing_actual_or_announcement_time"
		return result
	}
	if actual.AvailableAt.IsZero() || actual.AvailableAt.Before(announcementAt) {
		result.Reason = "actual_not_available_at_announcement"
		return result
	}
	estimate := LatestBeforeAnnouncement(estimates, actual.AssetID, actual.Metric, actual.FiscalPeriodEnd, announcementAt)
	if estimate == nil {
		return result
	}
	if !compatible(actual, *estimate) {
		result.Reason = "metric_period_currency_unit_or_basis_mismatch"
		return result
	}
	surprise := actual.Value - estimate.Value
	result.Status, result.Reason, result.ActualValue, result.ExpectedValue, result.Surprise = "available", "", &actual.Value, &estimate.Value, &surprise
	result.EstimateID = estimate.ID
	availableAt := estimate.AvailableAt.UTC()
	result.EstimateAvailableAt = &availableAt
	switch {
	case surprise > 0:
		result.SurpriseDirection = "above_consensus"
	case surprise < 0:
		result.SurpriseDirection = "below_consensus"
	default:
		result.SurpriseDirection = "in_line"
	}
	return result
}

func Revision(previous, current Estimate) (float64, error) {
	if previous.AssetID != current.AssetID || !strings.EqualFold(previous.Metric, current.Metric) || !sameDate(previous.FiscalPeriodEnd, current.FiscalPeriodEnd) || !compatible(Actual{AssetID: previous.AssetID, Metric: previous.Metric, FiscalPeriodEnd: previous.FiscalPeriodEnd, AccountingBasis: previous.AccountingBasis, Currency: previous.Currency, Unit: previous.Unit}, current) {
		return 0, fmt.Errorf("consensus revisions require matching asset, metric, period, currency, unit and accounting basis")
	}
	return current.Value - previous.Value, nil
}

func SortByAvailability(items []Estimate) {
	sort.SliceStable(items, func(i, j int) bool { return items[i].AvailableAt.Before(items[j].AvailableAt) })
}

func compatible(actual Actual, estimate Estimate) bool {
	return strings.EqualFold(actual.AssetID, estimate.AssetID) && strings.EqualFold(actual.Metric, estimate.Metric) && sameDate(actual.FiscalPeriodEnd, estimate.FiscalPeriodEnd) && strings.EqualFold(strings.TrimSpace(actual.AccountingBasis), strings.TrimSpace(estimate.AccountingBasis)) && strings.EqualFold(strings.TrimSpace(actual.Currency), strings.TrimSpace(estimate.Currency)) && strings.EqualFold(strings.TrimSpace(actual.Unit), strings.TrimSpace(estimate.Unit))
}

func estimateLater(left, right Estimate) bool {
	leftRevision, rightRevision := left.AvailableAt, right.AvailableAt
	if left.RevisionAt != nil {
		leftRevision = *left.RevisionAt
	}
	if right.RevisionAt != nil {
		rightRevision = *right.RevisionAt
	}
	if !leftRevision.Equal(rightRevision) {
		return leftRevision.After(rightRevision)
	}
	return left.ID > right.ID
}

func sameDate(left, right time.Time) bool { return dateUTC(left).Equal(dateUTC(right)) }
func dateUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}
