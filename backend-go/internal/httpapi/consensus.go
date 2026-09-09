package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
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

type announcementAssessmentInput struct {
	CurrentActual  consensus.Actual `json:"current_actual"`
	PreviousActual consensus.Actual `json:"previous_actual"`
	AnnouncementAt time.Time        `json:"announcement_at"`
}

func (s *Server) consensusAt(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || strings.TrimSpace(assetID) == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	cutoffValue, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	cutoff := time.Now().UTC()
	if cutoffValue != nil {
		var typed bool
		cutoff, typed = cutoffValue.(time.Time)
		if !typed {
			writeError(w, http.StatusInternalServerError, "invalid consensus as_of cutoff")
			return
		}
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 60, 1, 500)
	if !ok {
		return
	}
	history, err := consensus.NewStore(s.db).ListAvailable(r.Context(), assetID, cutoff, 1000)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "consensus snapshot query failed")
		return
	}
	items := history
	if len(items) > limit {
		items = items[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "as_of": cutoff.UTC(), "items": items, "revisions": consensus.BuildEstimateRevisions(history),
		"time_contract_version":               consensus.TimeContractVersion,
		"observation_contract_version":        consensus.FMPObservationContractVersion,
		"provider_publication_time_available": false, "historical_backfill": false,
		"automatic_rating": false, "individual_analyst_behavior_status": "unavailable_aggregate_snapshots_only",
	})
}

func (s *Server) guidanceAt(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || strings.TrimSpace(assetID) == "" {
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
	limit, ok := intQuery(w, r.URL.Query(), "limit", 60, 1, 500)
	if !ok {
		return
	}
	history, err := consensus.NewStore(s.db).GuidanceHistory(r.Context(), assetID, cutoff, 500)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "management guidance snapshot query failed")
		return
	}
	items := history
	if len(items) > limit {
		items = items[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": assetID, "as_of": cutoff.UTC(), "items": items, "revisions": consensus.BuildGuidanceRevisions(history),
		"time_contract_version": consensus.TimeContractVersion, "guidance_is_consensus": false, "automatic_rating": false,
	})
}

func (s *Server) announcementAssessment(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || strings.TrimSpace(assetID) == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := announcementAssessmentInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.CurrentActual.AssetID, input.PreviousActual.AssetID = assetID, assetID
	if err := consensus.ValidateAssessmentActuals(input.CurrentActual, input.PreviousActual, input.AnnouncementAt); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	estimates, err := consensus.NewStore(s.db).EstimatesBefore(r.Context(), assetID, input.CurrentActual.Metric, input.CurrentActual.FiscalPeriodEnd, input.AnnouncementAt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "pre-announcement consensus query failed")
		return
	}
	writeJSON(w, http.StatusOK, consensus.AssessAnnouncement(input.CurrentActual, input.PreviousActual, estimates, input.AnnouncementAt))
}

func (s *Server) syncConsensus(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || strings.TrimSpace(assetID) == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 10, 1, 40)
	if !ok {
		return
	}
	taskID := uuid.NewString()
	queuedID, err := s.enqueueGoModelJob(r.Context(), "masterdata", taskID, "market_loop.sync_consensus_snapshots", []any{assetID}, map[string]any{"asset_id": assetID, "limit": limit}, 4, "consensus-snapshot:"+assetID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "consensus snapshot sync could not be queued")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"task_id": queuedID, "status": "queued", "asset_id": assetID, "limit": limit, "source": "FMP analyst estimates",
		"time_contract_version": consensus.TimeContractVersion, "observation_contract_version": consensus.FMPObservationContractVersion,
		"historical_backfill": false, "automatic_rating": false,
	})
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
