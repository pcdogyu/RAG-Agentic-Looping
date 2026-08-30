package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"slices"
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
	"/api/v1/changed-targets?limit=100",
	"/api/v1/target-changes?kind=asset&limit=100",
	"/api/v1/target-changes?kind=macro&limit=100",
	"/api/v1/outcomes",
	"/api/v1/evolution",
	"/api/v1/model-logs?limit=5",
	"/api/v1/model-usage",
	"/api/v1/asset-universe?limit=5",
	"/api/v1/industries",
	"/api/v1/asset-universe/status",
	"/api/v1/source-filter",
	"/api/v1/source-filter/logs?limit=5",
	"/api/v1/admin/mcp-sources",
	"/api/v1/admin/fact-source-groups",
	"/api/v1/integrations/weknora",
	"/api/v1/research-queue?limit=5",
	"/api/v1/news-extraction-queue?limit=5",
	"/api/v1/model-inference-queues",
	"/api/v1/model-queue-overview?limit=5",
	"/api/v1/scan/status",
	"/api/v1/news-board?per_source=1",
	"/api/v1/analysis-logs?limit=5",
	"/api/v1/portfolio",
}

func main() {
	legacy := strings.TrimRight(env("LEGACY_API_URL", "http://localhost:8000"), "/")
	goAPI := strings.TrimRight(env("GO_API_URL", "http://localhost:8081"), "/")
	client := &http.Client{Timeout: 30 * time.Second}
	failures := 0
	for _, path := range safePaths {
		left, statusLeft, err := get(client, legacy+path, path)
		if err != nil {
			fmt.Printf("ERROR legacy %s: %v\n", path, err)
			failures++
			continue
		}
		right, statusRight, err := get(client, goAPI+path, path)
		if err != nil {
			fmt.Printf("ERROR go %s: %v\n", path, err)
			failures++
			continue
		}
		if statusLeft != statusRight || !bytes.Equal(left, right) {
			fmt.Printf("DIFF %s legacy=%d go=%d legacy_bytes=%d go_bytes=%d\n", path, statusLeft, statusRight, len(left), len(right))
			var legacyValue, goValue any
			if json.Unmarshal(left, &legacyValue) == nil && json.Unmarshal(right, &goValue) == nil {
				details := differences(legacyValue, goValue, "$", 8)
				for _, difference := range details {
					fmt.Printf("     %s\n", difference)
				}
				if len(details) == 0 {
					fmt.Printf("     canonical bytes differ at %s\n", firstByteDifference(left, right))
				}
			}
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

func firstByteDifference(left, right []byte) string {
	index := 0
	for index < min(len(left), len(right)) && left[index] == right[index] {
		index++
	}
	start := max(0, index-60)
	leftEnd := min(len(left), index+100)
	rightEnd := min(len(right), index+100)
	return fmt.Sprintf("byte %d legacy=%q go=%q", index, left[start:leftEnd], right[start:rightEnd])
}

func differences(left, right any, path string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	result := make([]string, 0)
	appendDifference := func(value string) bool {
		result = append(result, value)
		return len(result) >= limit
	}
	switch leftValue := left.(type) {
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok {
			return []string{fmt.Sprintf("%s type legacy=object go=%T", path, right)}
		}
		keys := make([]string, 0, len(leftValue)+len(rightValue))
		seen := map[string]bool{}
		for key := range leftValue {
			keys = append(keys, key)
			seen[key] = true
		}
		for key := range rightValue {
			if !seen[key] {
				keys = append(keys, key)
			}
		}
		slices.Sort(keys)
		for _, key := range keys {
			leftItem, leftOK := leftValue[key]
			rightItem, rightOK := rightValue[key]
			if !leftOK || !rightOK {
				if appendDifference(fmt.Sprintf("%s.%s presence legacy=%t go=%t", path, key, leftOK, rightOK)) {
					break
				}
				continue
			}
			remaining := limit - len(result)
			result = append(result, differences(leftItem, rightItem, path+"."+key, remaining)...)
			if len(result) >= limit {
				break
			}
		}
	case []any:
		rightValue, ok := right.([]any)
		if !ok {
			return []string{fmt.Sprintf("%s type legacy=array go=%T", path, right)}
		}
		if len(leftValue) != len(rightValue) && appendDifference(fmt.Sprintf("%s length legacy=%d go=%d", path, len(leftValue), len(rightValue))) {
			return result
		}
		for index := 0; index < min(len(leftValue), len(rightValue)); index++ {
			remaining := limit - len(result)
			result = append(result, differences(leftValue[index], rightValue[index], fmt.Sprintf("%s[%d]", path, index), remaining)...)
			if len(result) >= limit {
				break
			}
		}
	default:
		if !reflect.DeepEqual(left, right) {
			appendDifference(fmt.Sprintf("%s legacy=%s go=%s", path, compact(left), compact(right)))
		}
	}
	return result
}

func compact(value any) string {
	body, _ := json.Marshal(value)
	if len(body) > 160 {
		body = append(body[:157], '.', '.', '.')
	}
	return string(body)
}

func get(client *http.Client, url, path string) ([]byte, int, error) {
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
	canonicalize(path, value)
	canonical, err := json.Marshal(value)
	return canonical, response.StatusCode, err
}

func canonicalize(path string, value any) {
	volatile := map[string]bool{
		"generated_at": true, "server_time": true, "queue_duration_ms": true,
		"execution_duration_ms": true, "average_queue_duration_ms": true,
		"average_execution_duration_ms": true, "longest_wait_ms": true,
		"estimated_clear_ms": true, "throughput_per_hour": true,
	}
	if path == "/api/v1/portfolio" {
		volatile["as_of"] = true
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if volatile[key] {
				delete(typed, key)
				continue
			}
			canonicalize(path, item)
		}
	case []any:
		for _, item := range typed {
			canonicalize(path, item)
		}
	}
}
func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
