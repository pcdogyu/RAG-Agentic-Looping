package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestMarketPricesRejectsAmbiguousPriceFieldBeforeQuery(t *testing.T) {
	server, err := New(config.Config{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/go/market-prices/equity%3ANASDAQ%3ACRWD?price_field=price", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestLicensedBenchmarkImportRequiresAdminAndIdempotency(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"vendor_code":".HSIDV","source_name":"vendor","source_document_id":"export-1","source_url":"https://example.test/export","license_reference":"agreement-1","approved_by":"owner","observations":[{"session_date":"2026-09-09","adjusted_close":123.45}]}`)
	request := httptest.NewRequest(http.MethodPost, "/go/market-prices/index%3AHSI%3AHSIDV/licensed-import", bytes.NewReader(body))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/go/market-prices/index%3AHSI%3AHSIDV/licensed-import", bytes.NewReader(body))
	request.Header.Set("X-Admin-Token", "test-token")
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity || !strings.Contains(response.Body.String(), "Idempotency-Key") {
		t.Fatalf("missing idempotency status=%d body=%s", response.Code, response.Body.String())
	}
}
