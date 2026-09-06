package httpapi

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/consensus"
)

type estimateSnapshotInput struct {
	Estimate      consensus.Estimate `json:"estimate"`
	SourcePayload map[string]any     `json:"source_payload"`
	RetrievedAt   *time.Time         `json:"retrieved_at"`
}

type guidanceSnapshotInput struct {
	Guidance consensus.Guidance `json:"guidance"`
}

// importConsensusEstimate accepts source-linked historical consensus data from
// an authorized integration. It does not scrape a current estimate and label
// it historical: published_at and available_at are required by the store.
func (s *Server) importConsensusEstimate(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	if !s.consensusAssetExists(w, r, assetID) {
		return
	}
	input := estimateSnapshotInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if input.SourcePayload == nil {
		writeError(w, http.StatusUnprocessableEntity, "source_payload is required for consensus provenance")
		return
	}
	input.Estimate.AssetID = assetID
	retrievedAt := time.Now().UTC()
	if input.RetrievedAt != nil {
		retrievedAt = input.RetrievedAt.UTC()
	}
	created, err := consensus.NewStore(s.db).SaveEstimate(r.Context(), input.Estimate, input.SourcePayload, retrievedAt)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"asset_id": assetID, "created": created, "time_contract_version": consensus.TimeContractVersion})
}

func (s *Server) importManagementGuidance(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	if !s.consensusAssetExists(w, r, assetID) {
		return
	}
	input := guidanceSnapshotInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if input.Guidance.SourcePayload == nil {
		writeError(w, http.StatusUnprocessableEntity, "source_payload is required for guidance provenance")
		return
	}
	input.Guidance.AssetID = assetID
	if input.Guidance.RetrievedAt.IsZero() {
		input.Guidance.RetrievedAt = time.Now().UTC()
	}
	created, err := consensus.NewStore(s.db).SaveGuidance(r.Context(), input.Guidance)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"asset_id": assetID, "created": created, "time_contract_version": consensus.TimeContractVersion})
}

func (s *Server) consensusAssetExists(w http.ResponseWriter, r *http.Request, assetID string) bool {
	var exists bool
	if err := s.db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM assets WHERE id=$1)`, assetID).Scan(&exists); err != nil || !exists {
		writeError(w, http.StatusNotFound, "asset not found")
		return false
	}
	return true
}
