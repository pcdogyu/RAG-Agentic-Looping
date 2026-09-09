package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type FMPCorporateActionClient struct {
	BaseURL     string
	AccessToken string
	HTTPClient  *http.Client
	Now         func() time.Time
}

func (client FMPCorporateActionClient) Fetch(ctx context.Context, assetID, market, currency, symbol string, limit int) ([]CorporateActionObservation, error) {
	if strings.TrimSpace(client.AccessToken) == "" {
		return nil, fmt.Errorf("FMP access token is not configured")
	}
	if strings.TrimSpace(assetID) == "" || strings.TrimSpace(symbol) == "" || strings.TrimSpace(market) == "" || strings.TrimSpace(currency) == "" {
		return nil, fmt.Errorf("corporate action asset identity is incomplete")
	}
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("FMP corporate action limit must be between 1 and 1000")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(client.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("FMP base URL is required")
	}
	httpClient := client.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	now := time.Now
	if client.Now != nil {
		now = client.Now
	}
	retrievedAt := now().UTC()
	items := []CorporateActionObservation{}
	for _, endpointName := range []string{"dividends", "splits"} {
		endpoint, err := url.Parse(baseURL + "/" + endpointName)
		if err != nil {
			return nil, fmt.Errorf("build FMP corporate action endpoint: %w", err)
		}
		query := endpoint.Query()
		query.Set("symbol", strings.ToUpper(strings.TrimSpace(symbol)))
		query.Set("limit", strconv.Itoa(limit))
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("apikey", client.AccessToken)
		response, err := httpClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("fetch FMP %s: %w", endpointName, err)
		}
		var raw []map[string]any
		decodeErr := json.NewDecoder(response.Body).Decode(&raw)
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("FMP %s HTTP %d", endpointName, response.StatusCode)
		}
		if decodeErr != nil {
			return nil, fmt.Errorf("decode FMP %s: %w", endpointName, decodeErr)
		}
		for _, record := range raw {
			var observation CorporateActionObservation
			if endpointName == "dividends" {
				observation, err = NormalizeFMPDividend(assetID, market, currency, record, endpoint.String(), retrievedAt)
			} else {
				observation, err = NormalizeFMPSplit(assetID, market, currency, record, endpoint.String(), retrievedAt)
			}
			if err != nil {
				return nil, err
			}
			items = append(items, observation)
		}
	}
	return items, nil
}

func NormalizeFMPDividend(assetID, market, currency string, raw map[string]any, sourceURL string, retrievedAt time.Time) (CorporateActionObservation, error) {
	effectiveAt, ok := corporateActionTime(raw["date"])
	if !ok {
		return CorporateActionObservation{}, fmt.Errorf("FMP dividend is missing effective date")
	}
	amount, ok := corporateActionNumber(raw["dividend"])
	if !ok || amount <= 0 {
		return CorporateActionObservation{}, fmt.Errorf("FMP dividend is missing a positive amount")
	}
	symbol := strings.ToUpper(corporateActionString(raw["symbol"]))
	adjusted, adjustedOK := corporateActionNumber(raw["adjDividend"])
	observation := CorporateActionObservation{
		AssetID: assetID, Market: market, Currency: currency, ActionType: CashDividend,
		EffectiveAt: effectiveAt, ObservedAt: effectiveAt, AvailableAt: retrievedAt,
		CashAmount: &amount, TimePrecision: "date_only", SourceName: "FMP",
		SourceDocumentID: "fmp-dividends:" + symbol, SourceURL: sourceURL,
		SourcePayload: cloneCorporateActionMap(raw), Metadata: map[string]any{"symbol": symbol, "frequency": corporateActionString(raw["frequency"]), "yield": raw["yield"]},
	}
	if adjustedOK && adjusted > 0 {
		observation.AdjustedCashAmount = &adjusted
	}
	observation.AnnouncementAt = optionalCorporateActionTime(raw["declarationDate"])
	observation.RecordAt = optionalCorporateActionTime(raw["recordDate"])
	observation.PaymentAt = optionalCorporateActionTime(raw["paymentDate"])
	if err := observation.NormalizeAndValidate(); err != nil {
		return CorporateActionObservation{}, err
	}
	return observation, nil
}

func NormalizeFMPSplit(assetID, market, currency string, raw map[string]any, sourceURL string, retrievedAt time.Time) (CorporateActionObservation, error) {
	effectiveAt, ok := corporateActionTime(raw["date"])
	if !ok {
		return CorporateActionObservation{}, fmt.Errorf("FMP split is missing effective date")
	}
	numerator, numeratorOK := corporateActionNumber(raw["numerator"])
	denominator, denominatorOK := corporateActionNumber(raw["denominator"])
	if !numeratorOK || !denominatorOK || numerator <= 0 || denominator <= 0 || numerator == denominator {
		return CorporateActionObservation{}, fmt.Errorf("FMP split has an invalid ratio")
	}
	actionType := StockSplit
	if numerator < denominator {
		actionType = ReverseSplit
	}
	symbol := strings.ToUpper(corporateActionString(raw["symbol"]))
	observation := CorporateActionObservation{
		AssetID: assetID, Market: market, Currency: currency, ActionType: actionType,
		EffectiveAt: effectiveAt, ObservedAt: effectiveAt, AvailableAt: retrievedAt,
		RatioNumerator: &numerator, RatioDenominator: &denominator,
		TimePrecision: "date_only", SourceName: "FMP", SourceDocumentID: "fmp-splits:" + symbol, SourceURL: sourceURL,
		SourcePayload: cloneCorporateActionMap(raw), Metadata: map[string]any{"symbol": symbol, "provider_split_type": corporateActionString(raw["splitType"])},
	}
	if err := observation.NormalizeAndValidate(); err != nil {
		return CorporateActionObservation{}, err
	}
	return observation, nil
}

func corporateActionTime(value any) (time.Time, bool) {
	text := corporateActionString(value)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02"} {
		parsed, err := time.Parse(layout, text)
		if err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

func optionalCorporateActionTime(value any) *time.Time {
	parsed, ok := corporateActionTime(value)
	if !ok {
		return nil
	}
	return &parsed
}

func corporateActionNumber(value any) (float64, bool) {
	if value == nil {
		return 0, false
	}
	number, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(value)), 64)
	return number, err == nil
}

func corporateActionString(value any) string {
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "<nil>" {
		return ""
	}
	return text
}

func cloneCorporateActionMap(source map[string]any) map[string]any {
	body, _ := json.Marshal(source)
	cloned := map[string]any{}
	_ = json.Unmarshal(body, &cloned)
	return cloned
}
