package httpapi

import (
	"context"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestResearchPolicySummaryDefaultsToShadowWithoutDatabase(t *testing.T) {
	server := &Server{cfg: config.Config{ResearchPolicyMode: "enforce", ResearchPolicyVersion: "p0-evidence-v1", ResearchPredictionMode: "unavailable", PolicyShadowMinDays: 14, PolicyShadowMinReviewed: 100}}
	value := server.researchPolicySummary(context.Background())
	if value["active_mode"] != "shadow" || value["configured_mode"] != "enforce" || value["ready_for_approval"] != false {
		t.Fatalf("unapproved policy must remain shadow: %#v", value)
	}
}
