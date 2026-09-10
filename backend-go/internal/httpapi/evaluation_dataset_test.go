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
		{http.MethodGet, "/go/evaluation-performance-reports"},
		{http.MethodPost, "/go/evaluation-performance-reports"},
		{http.MethodGet, "/go/evaluation-performance-reports/performance-00000000000000000000000000000000"},
		{http.MethodGet, "/go/evaluation-final-holdouts"},
		{http.MethodPost, "/go/evaluation-final-holdouts"},
		{http.MethodGet, "/go/evaluation-final-holdouts/final-holdout-00000000000000000000000000000000"},
		{http.MethodGet, "/go/research-quality-reviews"},
		{http.MethodPost, "/go/research-quality-reviews"},
		{http.MethodPost, "/go/model-governance/rollback"},
		{http.MethodGet, "/go/model-governance/failure-drills"},
		{http.MethodPost, "/go/model-governance/failure-drills"},
		{http.MethodGet, "/go/phase-two/readiness"},
	} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(request.method, request.path, nil))
		if response.Code != http.StatusUnauthorized {
			t.Errorf("%s %s status=%d want=%d", request.method, request.path, response.Code, http.StatusUnauthorized)
		}
	}
}
