package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestFundamentalAssetIDUnescapesCanonicalID(t *testing.T) {
	value, err := fundamentalAssetID("equity%3ANYSE%3AVRT")
	if err != nil || value != "equity:NYSE:VRT" {
		t.Fatalf("asset id=%q err=%v", value, err)
	}
}

func TestFundamentalSyncRequiresAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/go/fundamentals/equity%3ANYSE%3AVRT/sync", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestConsensusImportRequiresAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/go/consensus/equity%3ANYSE%3AVRT/estimates", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestForecastCreateRequiresAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/go/forecasts/equity%3ANYSE%3AVRT", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestValuationCreateRequiresAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/go/valuations/equity%3ANYSE%3AVRT", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestRatingWritesRequireAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/go/ratings/equity%3ANYSE%3AVRT", "/go/ratings/equity%3ANYSE%3AVRT/invalidation-check"} {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("path=%s status=%d want %d", path, response.Code, http.StatusUnauthorized)
		}
	}
}

func TestPredictionAndGovernanceWritesRequireAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"/go/prediction-models", "/go/calibrations", "/go/calibrations/platt-v1/promotion-check", "/go/predictions/equity%3ANYSE%3AVRT", "/go/model-governance/shadow-runs/equity%3ANYSE%3AVRT", "/go/model-governance/promotion-check", "/go/model-governance/drift-check"}
	for _, path := range paths {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("path=%s status=%d", path, response.Code)
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/go/model-governance/checks", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("governance history status=%d", response.Code)
	}
}
