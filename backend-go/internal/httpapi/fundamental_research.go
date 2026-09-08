package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentalresearch"
)

func (s *Server) runFundamentalResearch(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := fundamentalresearch.Input{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if input.AssetID != "" && input.AssetID != assetID {
		writeError(w, http.StatusUnprocessableEntity, "asset_id body must match the path")
		return
	}
	input.AssetID = assetID
	result, err := fundamentalresearch.New(s.db).Run(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}
