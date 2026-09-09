package httpapi

import (
	"net/http"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
)

func (s *Server) marketModelReadiness(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	report, err := marketpolicy.BuildMarketModelReadiness(r.Context(), s.db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "market model readiness query failed")
		return
	}
	writeJSON(w, http.StatusOK, report)
}
