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
	for schemaName, fields := range map[string][]string{
		"EventReport":    {"news_credibility_score", "report_confidence", "report_confidence_score", "prompt_version", "target_evaluation_version", "report_confidence_version"},
		"TargetImpact":   {"claims", "transmission_steps", "conclusion_status", "impact_channel", "model_target_evaluation", "target_evaluation", "target_evaluation_score"},
		"Recommendation": {"news_credibility_score", "report_confidence", "report_confidence_score", "prompt_version", "target_evaluation", "target_evaluation_score"},
	} {
		var schema struct {
			Properties map[string]json.RawMessage `json:"properties"`
		}
		if err := json.Unmarshal(spec.Components.Schemas[schemaName], &schema); err != nil {
			t.Fatalf("decode schema %s: %v", schemaName, err)
		}
		for _, field := range fields {
			if _, ok := schema.Properties[field]; !ok {
				t.Errorf("schema %s missing v4.1 field %s", schemaName, field)
			}
		}
	}
	for _, schemaName := range []string{"EvidenceAssessment", "ResearchClaim", "TransmissionStep", "TargetEvaluation", "TargetImpact"} {
		var schema struct {
			AdditionalProperties any `json:"additionalProperties"`
		}
		if err := json.Unmarshal(spec.Components.Schemas[schemaName], &schema); err != nil {
			t.Fatalf("decode schema %s: %v", schemaName, err)
		}
		if schema.AdditionalProperties != false {
			t.Errorf("schema %s must reject unknown properties", schemaName)
		}
	}
}
