package httpapi

import (
	"net/http"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/counterresearch"
)

const counterResearchAPIVersion = "counter-research-v1"

func (s *Server) counterResearchStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"version": counterResearchAPIVersion, "enabled": s.cfg.CounterResearchEnabled,
		"mode": "optional_ablation", "confidence_effect": "none",
	})
}

func (s *Server) counterResearchAblation(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := struct {
		Observations []struct {
			BaselineWrong  bool    `json:"baseline_wrong"`
			ErrorFound     bool    `json:"error_found"`
			AddedLatencyMS int64   `json:"added_latency_ms"`
			AddedCost      float64 `json:"added_cost"`
		} `json:"observations"`
	}{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if len(input.Observations) == 0 || len(input.Observations) > 10000 {
		writeError(w, http.StatusUnprocessableEntity, "between 1 and 10000 labelled ablation observations are required")
		return
	}
	observations := make([]counterresearch.AblationObservation, 0, len(input.Observations))
	for _, item := range input.Observations {
		observations = append(observations, counterresearch.AblationObservation{BaselineWrong: item.BaselineWrong, ErrorFound: item.ErrorFound, Latency: time.Duration(item.AddedLatencyMS) * time.Millisecond, Cost: item.AddedCost})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": counterResearchAPIVersion, "label_requirement": "independent_ground_truth",
		"result": counterresearch.SummarizeAblation(observations),
	})
}
