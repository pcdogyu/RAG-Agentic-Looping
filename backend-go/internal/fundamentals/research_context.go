package fundamentals

import (
	"sort"
	"strings"
	"time"
)

// ResearchContext is a compact, source-linked view of the snapshots available
// at a research cutoff. It is background financial data, not evidence that a
// news event happened or that it affects a particular issuer.
type ResearchContext struct {
	Status              string           `json:"status"`
	Reason              string           `json:"reason,omitempty"`
	AssetID             string           `json:"asset_id"`
	AsOf                time.Time        `json:"as_of"`
	TimeContractVersion string           `json:"time_contract_version"`
	Snapshots           []ResearchRecord `json:"snapshots"`
	MissingFields       []string         `json:"missing_fields"`
}

type ResearchRecord struct {
	SnapshotID       string         `json:"snapshot_id"`
	StatementType    StatementType  `json:"statement_type"`
	FiscalPeriod     string         `json:"fiscal_period,omitempty"`
	ReportPeriodEnd  time.Time      `json:"report_period_end"`
	PublishedAt      time.Time      `json:"published_at"`
	AvailableAt      time.Time      `json:"available_at"`
	Currency         string         `json:"currency,omitempty"`
	Unit             string         `json:"unit"`
	AccountingStd    string         `json:"accounting_standard"`
	SourceName       string         `json:"source_name"`
	SourceURL        string         `json:"source_url"`
	SourceDocumentID string         `json:"source_document_id,omitempty"`
	Metrics          map[string]any `json:"metrics"`
}

var researchMetricKeys = map[StatementType][]string{
	IncomeStatement: {"revenue", "grossProfit", "operatingIncome", "netIncome", "ebitda", "weightedAverageShsOutDil"},
	BalanceSheet:    {"cashAndCashEquivalents", "totalDebt", "totalAssets"},
	CashFlow:        {"operatingCashFlow", "capitalExpenditure", "freeCashFlow"},
}

// BuildResearchContext deliberately exposes only a small, documented subset
// of the raw provider fields. Full provider payloads remain on the snapshot
// for audit, while prompts stay bounded and do not encourage invented metrics.
func BuildResearchContext(assetID string, cutoff time.Time, snapshots []Snapshot) ResearchContext {
	context := ResearchContext{Status: "unavailable", AssetID: assetID, AsOf: cutoff.UTC(), TimeContractVersion: TimeContractVersion, Snapshots: []ResearchRecord{}, MissingFields: []string{}}
	if cutoff.IsZero() {
		context.Reason = "missing_research_cutoff"
		return context
	}
	available := LatestAvailable(snapshots, cutoff)
	if len(available) == 0 {
		context.Reason = "no_snapshot_available_at_cutoff"
		context.MissingFields = minimumFundamentalFields()
		return context
	}
	seen := map[StatementType]map[string]bool{}
	for _, snapshot := range available {
		metrics := map[string]any{}
		for _, key := range researchMetricKeys[snapshot.StatementType] {
			if value, exists := snapshot.Metrics[key]; exists && value != nil {
				metrics[key] = value
				if seen[snapshot.StatementType] == nil {
					seen[snapshot.StatementType] = map[string]bool{}
				}
				seen[snapshot.StatementType][key] = true
			}
		}
		context.Snapshots = append(context.Snapshots, ResearchRecord{
			SnapshotID: snapshot.ID, StatementType: snapshot.StatementType, FiscalPeriod: snapshot.FiscalPeriod,
			ReportPeriodEnd: snapshot.ReportPeriodEnd, PublishedAt: snapshot.PublishedAt, AvailableAt: snapshot.AvailableAt,
			Currency: snapshot.Currency, Unit: snapshot.Unit, AccountingStd: snapshot.AccountingStd,
			SourceName: snapshot.SourceName, SourceURL: snapshot.SourceURL, SourceDocumentID: snapshot.SourceDocumentID, Metrics: metrics,
		})
	}
	for kind, keys := range researchMetricKeys {
		for _, key := range keys {
			if !seen[kind][key] {
				context.MissingFields = append(context.MissingFields, string(kind)+"."+key)
			}
		}
	}
	sort.Strings(context.MissingFields)
	if len(context.MissingFields) == 0 {
		context.Status = "available"
	} else {
		context.Status, context.Reason = "partial", "missing_minimum_fundamental_fields"
	}
	return context
}

func minimumFundamentalFields() []string {
	values := make([]string, 0, 12)
	for kind, keys := range researchMetricKeys {
		for _, key := range keys {
			values = append(values, string(kind)+"."+key)
		}
	}
	sort.Strings(values)
	return values
}

func IsFinancialInstitution(values ...string) bool {
	for _, value := range values {
		lowered := strings.ToLower(value)
		if strings.Contains(lowered, "financial") || strings.Contains(lowered, "bank") || strings.Contains(lowered, "insurance") || strings.Contains(lowered, "金融") || strings.Contains(lowered, "银行") || strings.Contains(lowered, "保险") {
			return true
		}
	}
	return false
}
