package httpapi

import (
	"net/http"

	modelevaluation "github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/evaluation"
)

func (s *Server) segmentedEvaluationReport(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	input := struct {
		Results []modelevaluation.PredictionResult `json:"results"`
	}{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if len(input.Results) == 0 || len(input.Results) > 10000 {
		writeError(w, http.StatusUnprocessableEntity, "between 1 and 10000 labelled prediction results are required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"segmentation": "asset_class:market", "pooled_report": false,
		"reports": modelevaluation.ReportBySegment(input.Results),
	})
}
