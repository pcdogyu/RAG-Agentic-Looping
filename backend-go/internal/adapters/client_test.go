package adapters

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTokenCount(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/token-count" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"counts":[3]}`))
	}))
	defer server.Close()
	client := New(server.URL, time.Second)
	response, err := client.TokenCount(context.Background(), []string{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Counts) != 1 || response.Counts[0] != 3 {
		t.Fatalf("response=%#v", response)
	}
}

func TestCircuitOpensAfterThreeFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "failed", 500) }))
	defer server.Close()
	client := New(server.URL, time.Second)
	for index := 0; index < 3; index++ {
		_ = client.Post(context.Background(), "/test", map[string]any{}, nil)
	}
	if err := client.Post(context.Background(), "/test", map[string]any{}, nil); err == nil || err.Error() != "adapter circuit is open" {
		t.Fatalf("error=%v", err)
	}
}
