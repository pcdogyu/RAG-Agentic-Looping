package fundamentals

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// FMPClient is the P1 US-financials adapter. It fetches factual statements
// only; callers choose when to persist them and no request produces a rating.
type FMPClient struct {
	BaseURL     string
	AccessToken string
	HTTPClient  *http.Client
	Now         func() time.Time
}

func (client FMPClient) FetchStatements(ctx context.Context, assetID, symbol string, limit int) ([]Snapshot, error) {
	if strings.TrimSpace(client.AccessToken) == "" {
		return nil, fmt.Errorf("FMP access token is not configured")
	}
	if strings.TrimSpace(assetID) == "" || strings.TrimSpace(symbol) == "" {
		return nil, fmt.Errorf("fundamental asset_id and symbol are required")
	}
	if limit < 1 || limit > 40 {
		return nil, fmt.Errorf("FMP statement limit must be between 1 and 40")
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
	types := []struct {
		endpoint string
		kind     StatementType
	}{
		{"income-statement", IncomeStatement},
		{"balance-sheet-statement", BalanceSheet},
		{"cash-flow-statement", CashFlow},
	}
	items := make([]Snapshot, 0, limit*len(types))
	for _, statement := range types {
		endpoint, err := url.Parse(baseURL + "/" + statement.endpoint)
		if err != nil {
			return nil, fmt.Errorf("build FMP endpoint: %w", err)
		}
		query := endpoint.Query()
		query.Set("symbol", strings.ToUpper(strings.TrimSpace(symbol)))
		query.Set("limit", fmt.Sprint(limit))
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("apikey", client.AccessToken)
		response, err := httpClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("fetch FMP %s: %w", statement.kind, err)
		}
		var raw []map[string]any
		decodeErr := json.NewDecoder(response.Body).Decode(&raw)
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("FMP %s HTTP %d", statement.kind, response.StatusCode)
		}
		if decodeErr != nil {
			return nil, fmt.Errorf("decode FMP %s: %w", statement.kind, decodeErr)
		}
		for _, record := range raw {
			snapshot, err := NormalizeFMPStatement(assetID, statement.kind, record, endpoint.String(), now().UTC())
			if err != nil {
				return nil, err
			}
			items = append(items, snapshot)
		}
	}
	return items, nil
}
