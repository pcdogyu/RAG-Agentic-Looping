package contract

import (
	"encoding/json"
	"os"
	"testing"
)

func TestFrozenContractContainsExpectedSurface(t *testing.T) {
	body, err := os.ReadFile("../../contracts/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths      map[string]map[string]any `json:"paths"`
		Components struct {
			Schemas map[string]json.RawMessage `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(body, &spec); err != nil {
		t.Fatal(err)
	}
	operations := 0
	for _, methods := range spec.Paths {
		for method := range methods {
			switch method {
			case "get", "post", "put", "patch", "delete":
				operations++
			}
		}
	}
	if operations != 81 {
		t.Fatalf("public contract changed: got %d operations, want 81", operations)
	}
	for _, path := range []string{"/health", "/api/v1/stream", "/api/v1/research-conclusions", "/api/v1/failed-research-runs", "/api/v1/model-runtime-summary"} {
		if _, ok := spec.Paths[path]; !ok {
			t.Errorf("missing required path %s", path)
		}
	}
	for _, schema := range []string{"ModelRuntimeLane", "ModelRuntimeMetrics", "ModelRuntimeModel", "ModelRuntimeSummaryResponse"} {
		if _, ok := spec.Components.Schemas[schema]; !ok {
			t.Errorf("missing required schema %s", schema)
		}
	}
}
