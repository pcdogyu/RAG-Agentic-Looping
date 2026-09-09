package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
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

func TestFundamentalResearchRequiresAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/go/fundamental-research/equity%3ANYSE%3AVRT", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", response.Code)
	}
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		request = httptest.NewRequest(method, "/go/fundamental-research/equity%3ANYSE%3AVRT/schedule", nil)
		response = httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("schedule method=%s status=%d", method, response.Code)
		}
	}
	request = httptest.NewRequest(http.MethodPost, "/go/corporate-actions/equity%3ANYSE%3AVRT/sync", nil)
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("corporate action sync status=%d", response.Code)
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

func TestCounterResearchStatusAndAblationContract(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token", CounterResearchEnabled: true}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/go/counter-research/status", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"confidence_effect":"none"`) || !strings.Contains(response.Body.String(), `"enabled":true`) {
		t.Fatalf("counter-research status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/go/counter-research/ablation", strings.NewReader(`{"observations":[]}`))
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("counter-research ablation status=%d", response.Code)
	}
}

func TestSegmentedEvaluationRequiresAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/go/model-evaluation/segmented-report", strings.NewReader(`{"results":[]}`))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("segmented evaluation status=%d", response.Code)
	}
}

func TestPredictionPolicyRejectsDeclaredMarketMismatch(t *testing.T) {
	item := assetPolicyRecord{AssetClass: "crypto", Market: "CRYPTO", Policy: marketpolicy.Resolve("crypto", "CRYPTO")}
	if err := validatePredictionPolicy(item, "US"); err == nil {
		t.Fatal("declared cross-market prediction was accepted")
	}
	if err := validatePredictionPolicy(item, "CRYPTO"); err != nil {
		t.Fatal(err)
	}
}
