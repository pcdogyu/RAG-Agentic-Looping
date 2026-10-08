package modelhttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGatewayChat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "qwen3.8-flash-next" || body["max_tokens"] != float64(50) || body["response_format"] == nil {
			t.Errorf("request = %#v", body)
		}
		if _, exists := body["keep_alive"]; exists {
			t.Error("Ollama option leaked")
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"ok\":true}"},"finish_reason":"length"}],"usage":{"prompt_tokens":12,"completion_tokens":50}}`))
	}))
	defer server.Close()
	req, _ := http.NewRequestWithContext(context.Background(), "POST", server.URL+"/v1/api/chat", strings.NewReader(`{"model":"qwen3.8-flash-next","messages":[],"format":{"type":"object"},"options":{"temperature":0,"num_predict":50},"keep_alive":0,"think":false}`))
	response, err := Do(server.Client(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(response.Body).Decode(&body)
	if body["done_reason"] != "length" || body["prompt_eval_count"] != float64(12) || body["eval_count"] != float64(50) {
		t.Fatalf("response = %#v", body)
	}
}

func TestModelsAndNativePassthrough(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			_, _ = w.Write([]byte(`{"data":[{"id":"qwen3.8-flash-next"}]}`))
			return
		}
		if r.URL.Path != "/api/tags" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"models":[{"name":"native"}]}`))
	}))
	defer server.Close()
	for _, path := range []string{"/v1/api/tags", "/v1/api/ps", "/api/tags"} {
		req, _ := http.NewRequest("GET", server.URL+path, nil)
		response, err := Do(server.Client(), req)
		if err != nil {
			t.Fatal(err)
		}
		var body struct{ Models []struct{ Name string } }
		_ = json.NewDecoder(response.Body).Decode(&body)
		_ = response.Body.Close()
		want := "qwen3.8-flash-next"
		if path == "/api/tags" {
			want = "native"
		}
		if len(body.Models) != 1 || body.Models[0].Name != want {
			t.Errorf("%s: %#v", path, body)
		}
	}
}

func TestGatewayErrors(t *testing.T) {
	for _, status := range []int{401, 200} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"unavailable"}`))
		}))
		req, _ := http.NewRequest("POST", server.URL+"/v1/api/chat", strings.NewReader(`{"model":"test"}`))
		response, err := Do(server.Client(), req)
		if status == 200 && err == nil {
			t.Error("missing choices accepted")
		}
		if status == 401 && (err != nil || response.StatusCode != 401) {
			t.Error("HTTP failure lost")
		}
		if response != nil {
			_ = response.Body.Close()
		}
		server.Close()
	}
}
