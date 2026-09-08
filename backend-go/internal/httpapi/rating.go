package httpapi

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/rating"
)

type ratingSubmissionInput struct {
	Policy              rating.Policy             `json:"policy"`
	ValuationRunID      string                    `json:"valuation_run_id"`
	AsOfPrice           *float64                  `json:"as_of_price"`
	AsOfPriceEvidenceID string                    `json:"as_of_price_evidence_id"`
	ExpectedDividend    *float64                  `json:"expected_dividend,omitempty"`
	BenchmarkReturn     *float64                  `json:"benchmark_return,omitempty"`
	BenchmarkEvidenceID string                    `json:"benchmark_evidence_id,omitempty"`
	EffectiveAt         time.Time                 `json:"effective_at"`
	ReasonCodes         []string                  `json:"reason_codes"`
	ChangedAssumptions  map[string]any            `json:"changed_assumptions"`
	EvidenceIDs         []string                  `json:"evidence_ids"`
	InvalidationRules   []rating.InvalidationRule `json:"invalidation_rules"`
}

func (s *Server) currentFundamentalRatings(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	items, err := rating.NewStore(s.db).Current(r.Context(), assetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "fundamental rating query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"asset_id": assetID, "items": items, "event_signal_separate": true})
}

func (s *Server) createFundamentalRating(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := ratingSubmissionInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	result, err := rating.NewStore(s.db).EvaluateAndPersist(r.Context(), rating.Submission{
		AssetID: assetID, Policy: input.Policy, ValuationRunID: input.ValuationRunID, AsOfPrice: input.AsOfPrice,
		AsOfPriceEvidenceID: input.AsOfPriceEvidenceID, ExpectedDividend: input.ExpectedDividend, BenchmarkReturn: input.BenchmarkReturn,
		BenchmarkEvidenceID: input.BenchmarkEvidenceID, EffectiveAt: input.EffectiveAt, ReasonCodes: input.ReasonCodes,
		ChangedAssumptions: input.ChangedAssumptions, EvidenceIDs: input.EvidenceIDs, InvalidationRules: input.InvalidationRules,
	})
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}

type invalidationCheckInput struct {
	ObservedAt   time.Time      `json:"observed_at"`
	Observations map[string]any `json:"observations"`
}

func (s *Server) checkRatingInvalidations(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	input := invalidationCheckInput{}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	items, err := rating.NewStore(s.db).EvaluateRules(r.Context(), assetID, input.Observations, input.ObservedAt)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"asset_id": assetID, "observed_at": input.ObservedAt, "items": items})
}
