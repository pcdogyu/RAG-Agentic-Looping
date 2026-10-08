package modelhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestConfiguredGatewayContextAndThinking(t *testing.T) {
	base := os.Getenv("MODEL_GATEWAY_TEST_URL")
	if base == "" {
		t.Skip("MODEL_GATEWAY_TEST_URL is required")
	}
	for _, lane := range []struct {
		context int
		think   bool
	}{{16384, false}, {262144, true}} {
		body := fmt.Sprintf(`{"model":"qwen3.8-flash-next","messages":[{"role":"user","content":"Calculate 2+2 and return a JSON object with integer field answer."}],"format":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false},"think":%t,"options":{"temperature":0,"num_ctx":%d,"num_predict":4096}}`, lane.think, lane.context)
		req, _ := http.NewRequest("POST", strings.TrimRight(base, "/")+"/api/chat", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		response, err := Do(&http.Client{Timeout: 90 * time.Second}, req)
		if err != nil {
			t.Fatal(err)
		}
		var payload struct {
			Message struct{ Content string }
			Prompt  int `json:"prompt_eval_count"`
			Output  int `json:"eval_count"`
		}
		err = json.NewDecoder(response.Body).Decode(&payload)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != 200 {
			t.Fatalf("gateway status=%d decode=%v", response.StatusCode, err)
		}
		var answer struct{ Answer int }
		if err := json.Unmarshal([]byte(payload.Message.Content), &answer); err != nil || answer.Answer != 4 {
			t.Fatalf("structured generation failed: %v", err)
		}
		if payload.Prompt+payload.Output > lane.context {
			t.Fatal("context budget exceeded")
		}
		t.Logf("context=%d thinking=%t input=%d output=%d", lane.context, lane.think, payload.Prompt, payload.Output)
	}
}

func TestContextBudget(t *testing.T) {
	for _, tc := range []struct {
		name                                    string
		count, limit, serverLimit, output, want int
		invalid                                 bool
	}{
		{"16k", 10000, 16384, 262144, 4096, 4096, false},
		{"cap generation", 16000, 16384, 262144, 4096, 384, false},
		{"256k", 200000, 262144, 262144, 16384, 16384, false},
		{"server cap", 16000, 262144, 16384, 4096, 384, false},
		{"oversized", 16384, 16384, 262144, 4096, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if r.URL.Path == "/tokenize" {
					if body["add_generation_prompt"] != true || body["chat_template_kwargs"].(map[string]any)["enable_thinking"] != true {
						t.Errorf("tokenizer template mismatch: %#v", body)
					}
					_, _ = fmt.Fprintf(w, `{"count":%d,"max_model_len":%d}`, tc.count, tc.serverLimit)
					return
				}
				calls++
				if body["max_tokens"] != float64(tc.want) {
					t.Errorf("max_tokens=%v want=%d", body["max_tokens"], tc.want)
				}
				if body["chat_template_kwargs"].(map[string]any)["enable_thinking"] != true {
					t.Error("thinking disabled")
				}
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{}"},"finish_reason":"stop"}]}`))
			}))
			defer server.Close()
			body := fmt.Sprintf(`{"model":"test","messages":[],"think":true,"options":{"num_ctx":%d,"num_predict":%d}}`, tc.limit, tc.output)
			req, _ := http.NewRequest("POST", server.URL+"/v1/api/chat", strings.NewReader(body))
			response, err := Do(server.Client(), req)
			if tc.invalid {
				if err == nil || calls != 0 {
					t.Fatal("oversized input reached generation")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if calls != 1 {
				t.Errorf("generation calls=%d", calls)
			}
		})
	}
}

func TestContextBudgetRequiresValidTokenizer(t *testing.T) {
	for _, payload := range []string{`{}`, `{"count":-1}`, `invalid`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/tokenize" {
				t.Error("generation called without valid token count")
			}
			_, _ = w.Write([]byte(payload))
		}))
		req, _ := http.NewRequest("POST", server.URL+"/v1/api/chat", strings.NewReader(`{"model":"test","options":{"num_ctx":16384}}`))
		if _, err := Do(server.Client(), req); err == nil {
			t.Errorf("invalid token count accepted: %s", payload)
		}
		server.Close()
	}
}

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
