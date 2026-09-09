package httpapi

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentalresearch"
)

func (s *Server) prepareFundamentalResearch(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	cutoffValue, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	cutoff := time.Now().UTC()
	if cutoffValue != nil {
		cutoff = cutoffValue.(time.Time)
	}
	result, err := fundamentalresearch.NewPreparationService(s.db).Prepare(r.Context(), assetID, cutoff)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "fundamental research preparation failed")
		return
	}
	writeJSON(w, http.StatusOK, result)
}
