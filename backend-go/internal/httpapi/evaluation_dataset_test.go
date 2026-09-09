package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestEvaluationDatasetEndpointsRequireAdminToken(t *testing.T) {
	server, err := New(config.Config{AdminAPIToken: "test-token"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/go/evaluation-holdouts"},
		{http.MethodPost, "/go/evaluation-holdouts"},
		{http.MethodGet, "/go/evaluation-datasets"},
		{http.MethodPost, "/go/evaluation-datasets"},
		{http.MethodGet, "/go/evaluation-datasets/dataset-00000000000000000000000000000000"},
		{http.MethodGet, "/go/evaluation-experiments"},
		{http.MethodPost, "/go/evaluation-experiments"},
		{http.MethodGet, "/go/evaluation-experiments/experiment-00000000000000000000000000000000"},
	} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(request.method, request.path, nil))
		if response.Code != http.StatusUnauthorized {
			t.Errorf("%s %s status=%d want=%d", request.method, request.path, response.Code, http.StatusUnauthorized)
		}
	}
}
