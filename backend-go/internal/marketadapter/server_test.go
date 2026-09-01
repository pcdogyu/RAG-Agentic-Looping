package marketadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type stubProvider struct {
	assets []Asset
	err    error
}

func (p stubProvider) Universe(context.Context, string) ([]Asset, error) {
	return p.assets, p.err
}
func (stubProvider) Prices(context.Context, PriceRequest) ([]map[string]any, error) {
	return []map[string]any{{"date": "2026-09-01", "close": 10.0}}, nil
}
func (stubProvider) Fundamentals(context.Context, string, string) ([]map[string]any, bool, error) {
	return []map[string]any{}, false, nil
}
func (stubProvider) News(context.Context, NewsRequest) ([]map[string]any, error) {
	return []map[string]any{}, nil
}

func TestHealthIdentifiesGoImplementation(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()
	NewHandler(stubProvider{}).ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"implementation":"go"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestResolveAssetsPreservesContract(t *testing.T) {
	provider := stubProvider{assets: []Asset{
		{AssetID: "equity:XHKG:09988", Symbol: "09988", Name: "阿里巴巴", Market: "HK"},
		{AssetID: "equity:XSHG:600000", Symbol: "600000", Name: "浦发银行", Market: "CN"},
	}}
	request := httptest.NewRequest(http.MethodPost, "/v1/assets/resolve", strings.NewReader(`{"query":"阿里","limit":1}`))
	response := httptest.NewRecorder()
	NewHandler(provider).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Items []Asset `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 1 || payload.Items[0].AssetID != "equity:XHKG:09988" {
		t.Fatalf("items=%#v", payload.Items)
	}
}

func TestValidationAndProviderFailures(t *testing.T) {
	tests := []struct {
		name     string
		provider Provider
		path     string
		body     string
		status   int
	}{
		{"missing query", stubProvider{}, "/v1/assets/resolve", `{}`, http.StatusUnprocessableEntity},
		{"unsupported market", stubProvider{}, "/v1/assets/universe", `{"market":"US"}`, http.StatusUnprocessableEntity},
		{"provider failure", stubProvider{err: errors.New("offline")}, "/v1/assets/universe", `{}`, http.StatusBadGateway},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			response := httptest.NewRecorder()
			NewHandler(test.provider).ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}
