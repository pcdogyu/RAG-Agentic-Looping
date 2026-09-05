package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeOllamaReportsConfiguredLaneAvailabilityAndLoadedState(t *testing.T) {
	server := func(installed, loaded []string) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			models := installed
			if r.URL.Path == "/api/ps" {
				models = loaded
			} else if r.URL.Path != "/api/tags" {
				http.NotFound(w, r)
				return
			}
			items := make([]map[string]string, 0, len(models))
			for _, model := range models {
				items = append(items, map[string]string{"name": model})
			}
			writeJSON(w, http.StatusOK, map[string]any{"models": items})
		}))
	}

	extract := server([]string{"qwen2.5:3b"}, []string{"qwen2.5:3b"})
	defer extract.Close()
	assist := server([]string{"qwen2.5:7b"}, []string{"qwen2.5:7b"})
	defer assist.Close()
	research := server([]string{"qwen2.5:7b"}, []string{"qwen2.5:7b"})
	defer research.Close()
	code := server([]string{"qwen2.5-coder:7b"}, nil)
	code.Close()

	for lane, endpoint := range map[string]string{
		"EXTRACT":  extract.URL,
		"ASSIST":   assist.URL,
		"RESEARCH": research.URL,
		"CODE":     code.URL,
	} {
		t.Setenv("OLLAMA_"+lane+"_BASE_URLS", "")
		t.Setenv("OLLAMA_"+lane+"_BASE_URL", endpoint)
	}

	instances, models, statuses := probeOllama(context.Background())
	if len(instances) != 4 {
		t.Fatalf("expected four lane instances, got %d", len(instances))
	}
	if len(models) != 2 {
		t.Fatalf("expected two distinct models from three reachable instances, got %v", models)
	}
	if status := statuses["qwen2.5:7b"]; !status["healthy"] || !status["model_available"] || !status["model_loaded"] {
		t.Fatalf("unexpected research status: %v", status)
	}
	if status := statuses["qwen2.5-coder:7b"]; status["healthy"] || status["model_available"] || status["model_loaded"] {
		t.Fatalf("unexpected code status: %v", status)
	}
}
