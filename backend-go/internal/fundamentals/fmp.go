package fundamentals

import (
	"fmt"
	"strings"
	"time"
)

// NormalizeFMPStatement converts one FMP statement record into the immutable
// contract. FMP's acceptedDate is preferred because it gives the actual SEC
// acceptance timestamp; filingDate is a conservative date-only fallback. A
// missing disclosure time is rejected rather than silently becoming available
// at fetch time.
func NormalizeFMPStatement(assetID string, statementType StatementType, raw map[string]any, sourceURL string, retrievedAt time.Time) (Snapshot, error) {
	periodEnd, ok := fmpTime(raw["date"])
	if !ok {
		return Snapshot{}, fmt.Errorf("FMP %s statement is missing report period end", statementType)
	}
	published, ok := firstFMPTime(raw, "acceptedDate", "filingDate")
	if !ok {
		return Snapshot{}, fmt.Errorf("FMP %s statement is missing acceptedDate or filingDate", statementType)
	}
	documentID := firstString(raw, "finalLink", "link", "accessionNumber", "fillingDate")
	if documentID == "" {
		documentID = strings.Join([]string{string(statementType), periodEnd.Format("2006-01-02"), strings.ToUpper(firstString(raw, "symbol"))}, ":")
	}
	metrics := cloneJSONMap(raw)
	for _, key := range []string{"date", "acceptedDate", "filingDate", "period", "reportedCurrency", "currency", "symbol", "finalLink", "link", "accessionNumber"} {
		delete(metrics, key)
	}
	snapshot := Snapshot{
		AssetID:          assetID,
		StatementType:    statementType,
		FiscalPeriod:     strings.ToUpper(firstString(raw, "period")),
		ReportPeriodEnd:  periodEnd,
		PublishedAt:      published,
		AvailableAt:      published,
		RevisionAt:       revisionTime(raw),
		Currency:         strings.ToUpper(firstString(raw, "reportedCurrency", "currency")),
		Unit:             "reported",
		AccountingStd:    accountingStandard(raw),
		SourceName:       "FMP",
		SourceURL:        strings.TrimSpace(sourceURL),
		SourceDocumentID: documentID,
		Metrics:          metrics,
		SourcePayload:    cloneJSONMap(raw),
		RetrievedAt:      retrievedAt,
	}
	if err := snapshot.NormalizeAndValidate(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func firstFMPTime(raw map[string]any, keys ...string) (time.Time, bool) {
	for _, key := range keys {
		if value, ok := fmpTime(raw[key]); ok {
			return value, true
		}
	}
	return time.Time{}, false
}

func revisionTime(raw map[string]any) *time.Time {
	value, ok := fmpTime(raw["acceptedDate"])
	if !ok {
		return nil
	}
	return &value
}

func fmpTime(value any) (time.Time, bool) {
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02"} {
		parsed, err := time.Parse(layout, text)
		if err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

func firstString(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(fmt.Sprint(raw[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func accountingStandard(raw map[string]any) string {
	value := strings.ToLower(firstString(raw, "accountingStandard", "accounting_standard", "reportedStandard"))
	switch value {
	case "gaap", "us-gaap", "us_gaap":
		return "us_gaap"
	case "ifrs":
		return "ifrs"
	default:
		return "unknown"
	}
}
