package marketdata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"
)

const CorporateActionContractVersion = "corporate-action-observation-v1"

type CorporateActionType string

const (
	CashDividend CorporateActionType = "cash_dividend"
	StockSplit   CorporateActionType = "split"
	ReverseSplit CorporateActionType = "reverse_split"
	SymbolChange CorporateActionType = "symbol_change"
	Suspension   CorporateActionType = "suspension"
	Delisting    CorporateActionType = "delisting"
)

var corporateActionTypes = map[CorporateActionType]bool{
	CashDividend: true, StockSplit: true, ReverseSplit: true,
	SymbolChange: true, Suspension: true, Delisting: true,
}

// CorporateActionObservation is an immutable provider observation. EffectiveAt
// is when the action affects the security, while AvailableAt is the first time
// this exact provider revision was observed by this system. They intentionally
// may be in either order because announced actions can be known in advance.
type CorporateActionObservation struct {
	ID                 string              `json:"id"`
	EventKey           string              `json:"event_key"`
	AssetID            string              `json:"asset_id"`
	Market             string              `json:"market"`
	Currency           string              `json:"currency"`
	ActionType         CorporateActionType `json:"action_type"`
	EffectiveAt        time.Time           `json:"effective_at"`
	ObservedAt         time.Time           `json:"observed_at"`
	AvailableAt        time.Time           `json:"available_at"`
	AnnouncementAt     *time.Time          `json:"announcement_at,omitempty"`
	RecordAt           *time.Time          `json:"record_at,omitempty"`
	PaymentAt          *time.Time          `json:"payment_at,omitempty"`
	EndAt              *time.Time          `json:"end_at,omitempty"`
	RatioNumerator     *float64            `json:"ratio_numerator,omitempty"`
	RatioDenominator   *float64            `json:"ratio_denominator,omitempty"`
	CashAmount         *float64            `json:"cash_amount,omitempty"`
	AdjustedCashAmount *float64            `json:"adjusted_cash_amount,omitempty"`
	OldSymbol          string              `json:"old_symbol,omitempty"`
	NewSymbol          string              `json:"new_symbol,omitempty"`
	TimePrecision      string              `json:"time_precision"`
	SourceName         string              `json:"source_name"`
	SourceDocumentID   string              `json:"source_document_id"`
	SourceURL          string              `json:"source_url,omitempty"`
	SourcePayload      map[string]any      `json:"source_payload"`
	Metadata           map[string]any      `json:"metadata"`
	CreatedAt          time.Time           `json:"created_at,omitempty"`
}

func (value *CorporateActionObservation) NormalizeAndValidate() error {
	value.AssetID = strings.TrimSpace(value.AssetID)
	value.Market = strings.ToUpper(strings.TrimSpace(value.Market))
	value.Currency = strings.ToUpper(strings.TrimSpace(value.Currency))
	value.ActionType = CorporateActionType(strings.ToLower(strings.TrimSpace(string(value.ActionType))))
	value.TimePrecision = strings.ToLower(strings.TrimSpace(value.TimePrecision))
	value.SourceName = strings.TrimSpace(value.SourceName)
	value.SourceDocumentID = strings.TrimSpace(value.SourceDocumentID)
	value.SourceURL = sanitizeCorporateActionSourceURL(value.SourceURL)
	value.OldSymbol = strings.ToUpper(strings.TrimSpace(value.OldSymbol))
	value.NewSymbol = strings.ToUpper(strings.TrimSpace(value.NewSymbol))
	if value.AssetID == "" || value.Market == "" || value.Currency == "" {
		return fmt.Errorf("corporate action requires asset_id, market and currency")
	}
	if !corporateActionTypes[value.ActionType] {
		return fmt.Errorf("unsupported corporate action type %q", value.ActionType)
	}
	if value.EffectiveAt.IsZero() || value.ObservedAt.IsZero() || value.AvailableAt.IsZero() {
		return fmt.Errorf("corporate action requires effective_at, observed_at and available_at")
	}
	value.EffectiveAt = value.EffectiveAt.UTC()
	value.ObservedAt = value.ObservedAt.UTC()
	value.AvailableAt = value.AvailableAt.UTC()
	normalizeOptionalActionTimes(value)
	if value.EndAt != nil && value.EndAt.Before(value.EffectiveAt) {
		return fmt.Errorf("corporate action end_at precedes effective_at")
	}
	if value.TimePrecision != "date_only" && value.TimePrecision != "timestamped" {
		return fmt.Errorf("corporate action time_precision must be date_only or timestamped")
	}
	if value.SourceName == "" || value.SourceDocumentID == "" {
		return fmt.Errorf("corporate action requires source_name and source_document_id")
	}
	if err := validateCorporateActionTerms(*value); err != nil {
		return err
	}
	if value.SourcePayload == nil {
		value.SourcePayload = map[string]any{}
	}
	if value.Metadata == nil {
		value.Metadata = map[string]any{}
	}
	value.Metadata["contract_version"] = CorporateActionContractVersion
	if value.EventKey == "" {
		value.EventKey = DeterministicCorporateActionEventKey(*value)
	}
	if value.ID == "" {
		value.ID = DeterministicCorporateActionID(*value)
	}
	return nil
}

func normalizeOptionalActionTimes(value *CorporateActionObservation) {
	for _, target := range []**time.Time{&value.AnnouncementAt, &value.RecordAt, &value.PaymentAt, &value.EndAt} {
		if *target != nil {
			normalized := (*target).UTC()
			*target = &normalized
		}
	}
}

func validateCorporateActionTerms(value CorporateActionObservation) error {
	for _, item := range []*float64{value.RatioNumerator, value.RatioDenominator, value.CashAmount, value.AdjustedCashAmount} {
		if item != nil && (math.IsNaN(*item) || math.IsInf(*item, 0)) {
			return fmt.Errorf("corporate action terms must be finite")
		}
	}
	switch value.ActionType {
	case StockSplit, ReverseSplit:
		if value.RatioNumerator == nil || value.RatioDenominator == nil || *value.RatioNumerator <= 0 || *value.RatioDenominator <= 0 || *value.RatioNumerator == *value.RatioDenominator {
			return fmt.Errorf("split action requires a positive non-unit numerator and denominator")
		}
		if value.ActionType == StockSplit && *value.RatioNumerator < *value.RatioDenominator {
			return fmt.Errorf("split ratio describes a reverse split")
		}
		if value.ActionType == ReverseSplit && *value.RatioNumerator > *value.RatioDenominator {
			return fmt.Errorf("reverse split ratio describes a forward split")
		}
		if value.CashAmount != nil || value.AdjustedCashAmount != nil || value.OldSymbol != "" || value.NewSymbol != "" {
			return fmt.Errorf("split action contains terms from another action type")
		}
	case CashDividend:
		if value.CashAmount == nil || *value.CashAmount <= 0 {
			return fmt.Errorf("cash dividend requires a positive cash_amount")
		}
		if value.AdjustedCashAmount != nil && *value.AdjustedCashAmount <= 0 {
			return fmt.Errorf("adjusted cash dividend must be positive")
		}
		if value.RatioNumerator != nil || value.RatioDenominator != nil || value.OldSymbol != "" || value.NewSymbol != "" {
			return fmt.Errorf("cash dividend contains terms from another action type")
		}
	case SymbolChange:
		if value.OldSymbol == "" || value.NewSymbol == "" || value.OldSymbol == value.NewSymbol {
			return fmt.Errorf("symbol change requires distinct old_symbol and new_symbol")
		}
		if value.RatioNumerator != nil || value.RatioDenominator != nil || value.CashAmount != nil || value.AdjustedCashAmount != nil {
			return fmt.Errorf("symbol change contains terms from another action type")
		}
	}
	return nil
}

func DeterministicCorporateActionEventKey(value CorporateActionObservation) string {
	identity := struct {
		AssetID, SourceName, SourceDocumentID string
		ActionType                            CorporateActionType
		EffectiveAt                           time.Time
	}{value.AssetID, value.SourceName, value.SourceDocumentID, value.ActionType, value.EffectiveAt.UTC()}
	body, _ := json.Marshal(identity)
	hash := sha256.Sum256(body)
	return "corporate-action-event-" + hex.EncodeToString(hash[:])[:48]
}

func DeterministicCorporateActionID(value CorporateActionObservation) string {
	identity := struct {
		EventKey                                   string
		ObservedAt                                 time.Time
		RatioNumerator, RatioDenominator           *float64
		CashAmount, AdjustedCashAmount             *float64
		OldSymbol, NewSymbol                       string
		AnnouncementAt, RecordAt, PaymentAt, EndAt *time.Time
	}{value.EventKey, value.ObservedAt.UTC(), value.RatioNumerator, value.RatioDenominator, value.CashAmount, value.AdjustedCashAmount, value.OldSymbol, value.NewSymbol, value.AnnouncementAt, value.RecordAt, value.PaymentAt, value.EndAt}
	body, _ := json.Marshal(identity)
	hash := sha256.Sum256(body)
	return "corporate-action-" + hex.EncodeToString(hash[:])[:48]
}

func sanitizeCorporateActionSourceURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	parsed.User = nil
	parsed.Fragment = ""
	query := parsed.Query()
	for _, key := range []string{"apikey", "api_key", "access_token", "token", "key"} {
		query.Del(key)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
