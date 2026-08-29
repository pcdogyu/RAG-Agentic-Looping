package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

var safePaths = []string{
	"/api/v1/assets",
	"/api/v1/news?limit=5",
	"/api/v1/events?limit=5",
	"/api/v1/research-runs?limit=5",
	"/api/v1/event-research-runs?limit=5",
	"/api/v1/recommendations?limit=5",
	"/api/v1/conclusions?limit=5",
	"/api/v1/research-conclusions?limit=5",
	"/api/v1/failed-research-runs?limit=50",
	"/api/v1/outcomes",
	"/api/v1/evolution",
	"/api/v1/model-logs?limit=5",
	"/api/v1/model-usage",
}

func main() {
	legacy := strings.TrimRight(env("LEGACY_API_URL", "http://localhost:8000"), "/")
	goAPI := strings.TrimRight(env("GO_API_URL", "http://localhost:8081"), "/")
	client := &http.Client{Timeout: 30 * time.Second}
	failures := 0
	for _, path := range safePaths {
		left, statusLeft, err := get(client, legacy+path)
		if err != nil {
			fmt.Printf("ERROR legacy %s: %v\n", path, err)
			failures++
			continue
		}
		right, statusRight, err := get(client, goAPI+path)
		if err != nil {
			fmt.Printf("ERROR go %s: %v\n", path, err)
			failures++
			continue
		}
		if statusLeft != statusRight || !bytes.Equal(left, right) {
			fmt.Printf("DIFF %s legacy=%d go=%d legacy_bytes=%d go_bytes=%d\n", path, statusLeft, statusRight, len(left), len(right))
			failures++
		} else {
			fmt.Printf("OK   %s\n", path)
		}
	}
	if failures > 0 {
		fmt.Printf("%d contract differences remain\n", failures)
		os.Exit(1)
	}
}

func get(client *http.Client, url string) ([]byte, int, error) {
	response, err := client.Get(url)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, response.StatusCode, err
	}
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, response.StatusCode, err
	}
	canonical, err := json.Marshal(value)
	return canonical, response.StatusCode, err
}
func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
