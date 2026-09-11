package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
)

func TestModelPromptsReturnsCompleteDefaultCatalog(t *testing.T) {
	server := &Server{}
	request := httptest.NewRequest("GET", "/go/model-prompts", nil)
	response := httptest.NewRecorder()
	server.modelPrompts(response, request)
	if response.Code != 200 {
		t.Fatalf("got status %d: %s", response.Code, response.Body.String())
	}
	var payload struct {
		Items []struct {
			Key             string  `json:"key"`
			DefaultPrompt   string  `json:"default_prompt"`
			EffectivePrompt string  `json:"effective_prompt"`
			CustomPrompt    *string `json:"custom_prompt"`
		} `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != len(jobs.ModelPromptDefinitions()) {
		t.Fatalf("got %d prompts", len(payload.Items))
	}
	for _, item := range payload.Items {
		if item.Key == "" || item.DefaultPrompt == "" || item.EffectivePrompt != item.DefaultPrompt || item.CustomPrompt != nil {
			t.Fatalf("unexpected default prompt: %#v", item)
		}
	}
}
