package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const FMPObservationContractVersion = "fmp-analyst-estimates-first-observed-v1"

// FetchedEstimate keeps the normalized estimate and the exact provider record
// together so persistence never loses provenance.
type FetchedEstimate struct {
	Estimate      Estimate
	SourcePayload map[string]any
}

// FMPClient collects current annual analyst-estimate aggregates. FMP does not
// expose a publication timestamp on this endpoint, so snapshots become
// available only at the local first-observed time. This adapter must never be
// used to synthesize historical estimates before collection began.
type FMPClient struct {
	BaseURL     string
	AccessToken string
	HTTPClient  *http.Client
	Now         func() time.Time
}

func (client FMPClient) FetchAnnual(ctx context.Context, assetID, symbol, currency string, limit int) ([]FetchedEstimate, error) {
	if strings.TrimSpace(client.AccessToken) == "" {
		return nil, fmt.Errorf("FMP access token is not configured")
	}
	assetID, symbol, currency = strings.TrimSpace(assetID), strings.ToUpper(strings.TrimSpace(symbol)), strings.ToUpper(strings.TrimSpace(currency))
	if assetID == "" || symbol == "" || currency == "" {
		return nil, fmt.Errorf("consensus asset_id, symbol and currency are required")
	}
	if limit < 1 || limit > 40 {
		return nil, fmt.Errorf("FMP analyst-estimate limit must be between 1 and 40")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(client.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("FMP base URL is required")
	}
	endpoint, err := url.Parse(baseURL + "/analyst-estimates")
	if err != nil {
		return nil, fmt.Errorf("build FMP analyst-estimates endpoint: %w", err)
	}
	query := endpoint.Query()
	query.Set("symbol", symbol)
	query.Set("period", "annual")
	query.Set("page", "0")
	query.Set("limit", fmt.Sprint(limit))
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("apikey", client.AccessToken)
	httpClient := client.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch FMP analyst estimates: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("FMP analyst estimates HTTP %d", response.StatusCode)
	}
	raw := []map[string]any{}
	if err := json.NewDecoder(response.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode FMP analyst estimates: %w", err)
	}
	now := time.Now
	if client.Now != nil {
		now = client.Now
	}
	observedAt := now().UTC()
	items := []FetchedEstimate{}
	for _, record := range raw {
		normalized, err := normalizeFMPAnnualEstimates(assetID, symbol, currency, record, endpoint.String(), observedAt)
		if err != nil {
			return nil, err
		}
		items = append(items, normalized...)
	}
	return items, nil
}

type fmpMetricSpec struct {
	Metric      string
	LowKey      string
	MeanKey     string
	HighKey     string
	AnalystsKey string
}

var fmpMetricSpecs = []fmpMetricSpec{
	{Metric: "revenue", LowKey: "revenueLow", MeanKey: "revenueAvg", HighKey: "revenueHigh", AnalystsKey: "numAnalystsRevenue"},
	{Metric: "eps", LowKey: "epsLow", MeanKey: "epsAvg", HighKey: "epsHigh", AnalystsKey: "numAnalystsEps"},
	{Metric: "ebitda", LowKey: "ebitdaLow", MeanKey: "ebitdaAvg", HighKey: "ebitdaHigh"},
	{Metric: "ebit", LowKey: "ebitLow", MeanKey: "ebitAvg", HighKey: "ebitHigh"},
	{Metric: "net_income", LowKey: "netIncomeLow", MeanKey: "netIncomeAvg", HighKey: "netIncomeHigh"},
	{Metric: "sga_expense", LowKey: "sgaExpenseLow", MeanKey: "sgaExpenseAvg", HighKey: "sgaExpenseHigh"},
}

func normalizeFMPAnnualEstimates(assetID, symbol, currency string, raw map[string]any, sourceURL string, observedAt time.Time) ([]FetchedEstimate, error) {
	periodEnd, err := time.Parse("2006-01-02", strings.TrimSpace(fmt.Sprint(raw["date"])))
	if err != nil {
		return nil, fmt.Errorf("FMP analyst estimate is missing fiscal period end")
	}
	providerSymbol := strings.ToUpper(strings.TrimSpace(fmt.Sprint(raw["symbol"])))
	if providerSymbol == "" || providerSymbol != symbol {
		return nil, fmt.Errorf("FMP analyst estimate symbol mismatch")
	}
	if observedAt.IsZero() {
		return nil, fmt.Errorf("FMP analyst estimate observation time is required")
	}
	payload := map[string]any{
		"provider_record": raw,
		"observation_contract": map[string]any{
			"version":                             FMPObservationContractVersion,
			"provider_publication_time_available": false,
			"published_at_basis":                  "first_observed_at",
			"historical_backfill":                 false,
		},
	}
	documentID := fmt.Sprintf("analyst-estimates:%s:annual:%s", symbol, periodEnd.Format("2006-01-02"))
	items := []FetchedEstimate{}
	for _, spec := range fmpMetricSpecs {
		analystCount := optionalPositiveInt(raw[spec.AnalystsKey])
		for _, statistic := range []struct{ Name, Key string }{{"low", spec.LowKey}, {"mean", spec.MeanKey}, {"high", spec.HighKey}} {
			value, ok := finiteNumber(raw[statistic.Key])
			if !ok {
				continue
			}
			estimate := Estimate{
				AssetID: assetID, Metric: spec.Metric, FiscalPeriod: "FY", FiscalPeriodEnd: periodEnd.UTC(),
				AccountingBasis: "unknown", Statistic: statistic.Name, Value: value, AnalystCount: analystCount,
				Currency: currency, Unit: "reported", PublishedAt: observedAt.UTC(), AvailableAt: observedAt.UTC(),
				SourceName: "FMP analyst estimates", SourceURL: sourceURL, SourceDocumentID: documentID,
			}
			items = append(items, FetchedEstimate{Estimate: estimate, SourcePayload: payload})
		}
	}
	return items, nil
}

func finiteNumber(value any) (float64, bool) {
	number, ok := value.(float64)
	if !ok || math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, false
	}
	return number, true
}

func optionalPositiveInt(value any) *int {
	number, ok := finiteNumber(value)
	if !ok || number <= 0 || number > math.MaxInt32 || number != math.Trunc(number) {
		return nil
	}
	result := int(number)
	return &result
}
