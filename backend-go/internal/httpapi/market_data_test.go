package httpapi

import (
	"net/http"
	"net/http/httptest"
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
