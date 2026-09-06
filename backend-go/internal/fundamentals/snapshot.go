// Package fundamentals owns the immutable, point-in-time contract for reported
// company financials. It deliberately does not calculate valuation, forecasts,
// or ratings: those require later, separately governed P1/P2 work.
package fundamentals

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const TimeContractVersion = "fundamental-point-in-time-v1"

type StatementType string

const (
	IncomeStatement StatementType = "income_statement"
	BalanceSheet    StatementType = "balance_sheet"
	CashFlow        StatementType = "cash_flow"
)

func (kind StatementType) Valid() bool {
	return kind == IncomeStatement || kind == BalanceSheet || kind == CashFlow
}

// Snapshot is one provider-observed revision of a reported financial statement.
// Metrics and SourcePayload are kept separately so consumers can read normalized
// metadata without losing the original provider record needed for audit.
type Snapshot struct {
	ID               string         `json:"id"`
	AssetID          string         `json:"asset_id"`
	StatementType    StatementType  `json:"statement_type"`
	FiscalPeriod     string         `json:"fiscal_period,omitempty"`
	ReportPeriodEnd  time.Time      `json:"report_period_end"`
	PublishedAt      time.Time      `json:"published_at"`
	AvailableAt      time.Time      `json:"available_at"`
	RevisionAt       *time.Time     `json:"revision_at,omitempty"`
	Currency         string         `json:"currency,omitempty"`
	Unit             string         `json:"unit"`
	AccountingStd    string         `json:"accounting_standard"`
	SourceName       string         `json:"source_name"`
	SourceURL        string         `json:"source_url"`
	SourceDocumentID string         `json:"source_document_id,omitempty"`
	Metrics          map[string]any `json:"metrics"`
	SourcePayload    map[string]any `json:"source_payload"`
	RetrievedAt      time.Time      `json:"retrieved_at"`
	CreatedAt        time.Time      `json:"created_at,omitempty"`
}

func (s *Snapshot) NormalizeAndValidate() error {
	s.AssetID = strings.TrimSpace(s.AssetID)
	s.StatementType = StatementType(strings.TrimSpace(string(s.StatementType)))
	s.FiscalPeriod = strings.ToUpper(strings.TrimSpace(s.FiscalPeriod))
	s.Currency = strings.ToUpper(strings.TrimSpace(s.Currency))
	s.Unit = strings.ToLower(strings.TrimSpace(s.Unit))
	s.AccountingStd = strings.ToLower(strings.TrimSpace(s.AccountingStd))
	s.SourceName = strings.TrimSpace(s.SourceName)
	s.SourceURL = strings.TrimSpace(s.SourceURL)
	s.SourceDocumentID = strings.TrimSpace(s.SourceDocumentID)
	if s.AssetID == "" {
		return errors.New("fundamental snapshot asset_id is required")
	}
	if !s.StatementType.Valid() {
		return fmt.Errorf("invalid fundamental statement_type %q", s.StatementType)
	}
	if s.ReportPeriodEnd.IsZero() {
		return errors.New("fundamental snapshot report_period_end is required")
	}
	if s.PublishedAt.IsZero() || s.AvailableAt.IsZero() {
		return errors.New("fundamental snapshot requires published_at and available_at")
	}
	s.ReportPeriodEnd = dateUTC(s.ReportPeriodEnd)
	s.PublishedAt = s.PublishedAt.UTC()
	s.AvailableAt = s.AvailableAt.UTC()
	if s.AvailableAt.Before(s.PublishedAt) {
		return errors.New("fundamental snapshot available_at precedes published_at")
	}
	if s.RevisionAt != nil {
		value := s.RevisionAt.UTC()
		s.RevisionAt = &value
	}
	if s.RetrievedAt.IsZero() {
		return errors.New("fundamental snapshot retrieved_at is required")
	}
	s.RetrievedAt = s.RetrievedAt.UTC()
	if s.Unit == "" {
		s.Unit = "reported"
	}
	if s.Unit != "reported" && s.Unit != "thousands" && s.Unit != "millions" && s.Unit != "billions" {
		return fmt.Errorf("invalid fundamental unit %q", s.Unit)
	}
	if s.AccountingStd == "" {
		s.AccountingStd = "unknown"
	}
	if s.SourceName == "" || s.SourceURL == "" {
		return errors.New("fundamental snapshot source_name and source_url are required")
	}
	if s.Metrics == nil {
		s.Metrics = map[string]any{}
	}
	if s.SourcePayload == nil {
		s.SourcePayload = map[string]any{}
	}
	if s.ID == "" {
		s.ID = DeterministicID(*s)
	}
	return nil
}

func DeterministicID(snapshot Snapshot) string {
	parts := []string{
		strings.TrimSpace(snapshot.AssetID), string(snapshot.StatementType), dateUTC(snapshot.ReportPeriodEnd).Format("2006-01-02"),
		strings.TrimSpace(snapshot.SourceName), strings.TrimSpace(snapshot.SourceDocumentID), snapshot.PublishedAt.UTC().Format(time.RFC3339Nano),
	}
	if snapshot.RevisionAt != nil {
		parts = append(parts, snapshot.RevisionAt.UTC().Format(time.RFC3339Nano))
	}
	hash := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(hash[:])
}

// LatestAvailable retains the latest provider revision for every statement
// period that was already available at cutoff. It never uses retrieval time as
// a substitute for disclosure time, preventing hindsight in historical runs.
func LatestAvailable(items []Snapshot, cutoff time.Time) []Snapshot {
	if cutoff.IsZero() {
		return nil
	}
	cutoff = cutoff.UTC()
	latest := map[string]Snapshot{}
	for _, item := range items {
		if item.AvailableAt.After(cutoff) {
			continue
		}
		key := strings.Join([]string{item.AssetID, string(item.StatementType), dateUTC(item.ReportPeriodEnd).Format("2006-01-02")}, "\x1f")
		previous, exists := latest[key]
		if !exists || snapshotLater(item, previous) {
			latest[key] = item
		}
	}
	output := make([]Snapshot, 0, len(latest))
	for _, item := range latest {
		output = append(output, item)
	}
	sort.Slice(output, func(i, j int) bool {
		if !output[i].ReportPeriodEnd.Equal(output[j].ReportPeriodEnd) {
			return output[i].ReportPeriodEnd.After(output[j].ReportPeriodEnd)
		}
		if output[i].StatementType != output[j].StatementType {
			return output[i].StatementType < output[j].StatementType
		}
		return output[i].PublishedAt.After(output[j].PublishedAt)
	})
	return output
}

func snapshotLater(left, right Snapshot) bool {
	leftRevision, rightRevision := left.PublishedAt, right.PublishedAt
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

func dateUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func cloneJSONMap(value map[string]any) map[string]any {
	body, _ := json.Marshal(value)
	output := map[string]any{}
	_ = json.Unmarshal(body, &output)
	return output
}
