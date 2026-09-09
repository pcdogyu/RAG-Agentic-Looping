package httpapi

import (
	"net/http"
	"strings"
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
			SampleID          string    `json:"sample_id"`
			EventCluster      string    `json:"event_cluster"`
			Fold              string    `json:"fold"`
			SignalCutoff      time.Time `json:"signal_cutoff"`
			BaselineCutoff    time.Time `json:"baseline_input_cutoff"`
			CounterCutoff     time.Time `json:"counter_input_cutoff"`
			BaselineVersion   string    `json:"baseline_version"`
			CounterVersion    string    `json:"counter_version"`
			SameInputCutoff   bool      `json:"same_input_cutoff"`
			GroundTruthSource string    `json:"ground_truth_source"`
			BaselineWrong     bool      `json:"baseline_wrong"`
			ErrorFound        bool      `json:"error_found"`
			AddedLatencyMS    int64     `json:"added_latency_ms"`
			AddedCost         float64   `json:"added_cost"`
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
	clusters := map[string]bool{}
	for _, item := range input.Observations {
		cluster := strings.TrimSpace(item.EventCluster)
		if strings.TrimSpace(item.SampleID) == "" || cluster == "" || clusters[cluster] || strings.TrimSpace(item.Fold) == "" || item.SignalCutoff.IsZero() ||
			item.BaselineCutoff.IsZero() || item.CounterCutoff.IsZero() || !item.BaselineCutoff.Equal(item.SignalCutoff) || !item.CounterCutoff.Equal(item.SignalCutoff) ||
			strings.TrimSpace(item.BaselineVersion) == "" || strings.TrimSpace(item.CounterVersion) == "" || !item.SameInputCutoff || strings.TrimSpace(item.GroundTruthSource) == "" {
			writeError(w, http.StatusUnprocessableEntity, "each observation requires a unique event cluster, fixed versions/cutoff/fold and independent ground truth")
			return
		}
		clusters[cluster] = true
		observations = append(observations, counterresearch.AblationObservation{BaselineWrong: item.BaselineWrong, ErrorFound: item.ErrorFound,
			Latency: time.Duration(item.AddedLatencyMS) * time.Millisecond, Cost: item.AddedCost, Fold: item.Fold})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": counterResearchAPIVersion, "label_requirement": "independent_ground_truth",
		"result": counterresearch.SummarizeAblation(observations),
	})
}
