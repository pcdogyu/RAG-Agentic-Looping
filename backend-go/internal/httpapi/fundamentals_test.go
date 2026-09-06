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
