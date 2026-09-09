// Package fundamentalresearch connects the deterministic forecast, valuation,
// and rating stages without depending on a news event. Inputs remain explicit,
// point-in-time, and evidence linked; the workflow never invents assumptions.
package fundamentalresearch

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/forecast"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/rating"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/valuation"
)

type ForecastPlan struct {
	ParentVersionID        string                `json:"parent_version_id,omitempty"`
	Inputs                 forecast.Inputs       `json:"inputs"`
	FundamentalSnapshotIDs []string              `json:"fundamental_snapshot_ids"`
	Assumptions            []forecast.Assumption `json:"assumptions"`
}

type ValuationPlan struct {
	NetDebtSnapshotID string                       `json:"net_debt_snapshot_id,omitempty"`
	DCFScenarios      []valuation.DCFScenario      `json:"dcf_scenarios"`
	MultipleScenarios []valuation.MultipleScenario `json:"multiple_scenarios"`
	SensitivityWACC   []float64                    `json:"sensitivity_wacc,omitempty"`
	SensitivityGrowth []float64                    `json:"sensitivity_terminal_growth,omitempty"`
}

type RatingPlan struct {
	Policy              rating.Policy             `json:"policy"`
	AsOfPrice           *float64                  `json:"as_of_price"`
	AsOfPriceEvidenceID string                    `json:"as_of_price_evidence_id"`
	ExpectedDividend    *float64                  `json:"expected_dividend,omitempty"`
	BenchmarkReturn     *float64                  `json:"benchmark_return,omitempty"`
	BenchmarkEvidenceID string                    `json:"benchmark_evidence_id,omitempty"`
	ReasonCodes         []string                  `json:"reason_codes"`
	ChangedAssumptions  map[string]any            `json:"changed_assumptions"`
	EvidenceIDs         []string                  `json:"evidence_ids"`
	InvalidationRules   []rating.InvalidationRule `json:"invalidation_rules"`
}

type Input struct {
	AssetID   string        `json:"asset_id"`
	AsOf      time.Time     `json:"as_of"`
	Forecast  ForecastPlan  `json:"forecast"`
	Valuation ValuationPlan `json:"valuation"`
	Rating    RatingPlan    `json:"rating"`
}

type Result struct {
	AssetID               string                 `json:"asset_id"`
	AsOf                  time.Time              `json:"as_of"`
	Status                string                 `json:"status"`
	Reason                string                 `json:"reason,omitempty"`
	MarketPolicy          marketpolicy.Policy    `json:"market_policy"`
	Forecast              forecast.Version       `json:"forecast"`
	Valuation             *valuation.Run         `json:"valuation,omitempty"`
	Rating                *rating.Snapshot       `json:"rating,omitempty"`
	ScheduleDraft         *ScheduleDraft         `json:"schedule_draft,omitempty"`
	ScheduleDraftControls *ScheduleDraftControls `json:"schedule_draft_controls,omitempty"`
}

// ScheduleDraft carries the exact governed inputs from a successful manual
// workflow into the separate schedule-approval step. It deliberately excludes
// the manual run's point-in-time price: scheduled runs must resolve a fresh,
// immutable adjusted-close observation at execution time. ApprovedBy remains
// empty so producing a draft can never approve or activate a schedule.
type ScheduleDraft struct {
	AssetID           string              `json:"asset_id"`
	ForecastVersionID string              `json:"forecast_version_id"`
	Valuation         ValuationPlan       `json:"valuation"`
	Rating            ScheduledRatingPlan `json:"rating"`
	CadenceHours      int                 `json:"cadence_hours"`
	MaxPriceAgeHours  int                 `json:"max_price_age_hours"`
	MaxPlanAgeDays    int                 `json:"max_plan_age_days"`
	ApprovedBy        string              `json:"approved_by"`
}

type ScheduleDraftControls struct {
	ApprovalRequired  bool   `json:"approval_required"`
	AutomaticApproval bool   `json:"automatic_approval"`
	RuntimePriceField string `json:"runtime_price_field"`
}

type Service struct{ db *pgxpool.Pool }

func New(db *pgxpool.Pool) *Service { return &Service{db: db} }

func (s *Service) Run(ctx context.Context, input Input) (Result, error) {
	input.AssetID = strings.TrimSpace(input.AssetID)
	if s.db == nil || input.AssetID == "" || input.AsOf.IsZero() {
		return Result{}, fmt.Errorf("fundamental research store, asset_id and as_of are required")
	}
	input.AsOf = input.AsOf.UTC()
	result := Result{AssetID: input.AssetID, AsOf: input.AsOf, Status: "insufficient_data"}
	var assetClass, market, currency string
	if err := s.db.QueryRow(ctx, `SELECT asset_class,market,currency FROM assets WHERE id=$1 AND active=true`, input.AssetID).Scan(&assetClass, &market, &currency); err != nil {
		return Result{}, fmt.Errorf("load active asset policy: %w", err)
	}
	result.MarketPolicy = marketpolicy.Resolve(assetClass, market)
	if err := marketpolicy.ValidateFundamental(result.MarketPolicy, input.Forecast.Inputs.Currency, input.Rating.Policy.Market, input.Rating.Policy.AssetClass, input.Rating.Policy.BenchmarkID); err != nil {
		result.Status, result.Reason = "not_applicable", err.Error()
		return result, nil
	}
	if !strings.EqualFold(strings.TrimSpace(currency), result.MarketPolicy.Currency) {
		result.Status, result.Reason = "not_applicable", "asset_currency_outside_market_policy"
		return result, nil
	}
	if err := validateManualResearchEvidence(ctx, s.db, input); err != nil {
		return Result{}, fmt.Errorf("evidence gate: %w", err)
	}
	version, _, err := forecast.NewStore(s.db).Create(ctx, forecast.Submission{AssetID: input.AssetID, AsOf: input.AsOf, ParentVersionID: input.Forecast.ParentVersionID, Inputs: input.Forecast.Inputs, FundamentalSnapshotIDs: input.Forecast.FundamentalSnapshotIDs, Assumptions: input.Forecast.Assumptions})
	if err != nil {
		return Result{}, fmt.Errorf("forecast stage: %w", err)
	}
	result.Forecast = version
	if version.Status != "available" {
		result.Reason = version.Projection.Reason
		return result, nil
	}
	run, _, err := valuation.NewStore(s.db).Create(ctx, valuation.Submission{AssetID: input.AssetID, AsOf: input.AsOf, ForecastVersionID: version.ID, NetDebtSnapshotID: input.Valuation.NetDebtSnapshotID, DCFScenarios: input.Valuation.DCFScenarios, MultipleScenarios: input.Valuation.MultipleScenarios, SensitivityWACC: input.Valuation.SensitivityWACC, SensitivityGrowth: input.Valuation.SensitivityGrowth})
	if err != nil {
		return Result{}, fmt.Errorf("valuation stage: %w", err)
	}
	result.Valuation = &run
	if run.Status != "available" {
		result.Reason = run.Result.Reason
		return result, nil
	}
	snapshot, err := rating.NewStore(s.db).EvaluateAndPersist(ctx, rating.Submission{AssetID: input.AssetID, Policy: input.Rating.Policy, ValuationRunID: run.ID, AsOfPrice: input.Rating.AsOfPrice, AsOfPriceEvidenceID: input.Rating.AsOfPriceEvidenceID, ExpectedDividend: input.Rating.ExpectedDividend, BenchmarkReturn: input.Rating.BenchmarkReturn, BenchmarkEvidenceID: input.Rating.BenchmarkEvidenceID, EffectiveAt: input.AsOf, ReasonCodes: input.Rating.ReasonCodes, ChangedAssumptions: input.Rating.ChangedAssumptions, EvidenceIDs: input.Rating.EvidenceIDs, InvalidationRules: input.Rating.InvalidationRules})
	if err != nil {
		return Result{}, fmt.Errorf("rating stage: %w", err)
	}
	result.Rating = &snapshot
	result.Status = snapshot.Result.Status
	result.Reason = snapshot.Result.Reason
	if result.Status == "available" {
		draft := scheduleDraftFromWorkflow(input, version.ID)
		result.ScheduleDraft = &draft
		result.ScheduleDraftControls = &ScheduleDraftControls{ApprovalRequired: true, AutomaticApproval: false, RuntimePriceField: "adjusted_close"}
	}
	return result, nil
}

func scheduleDraftFromWorkflow(input Input, forecastVersionID string) ScheduleDraft {
	return ScheduleDraft{
		AssetID:           input.AssetID,
		ForecastVersionID: forecastVersionID,
		Valuation:         input.Valuation,
		Rating: ScheduledRatingPlan{
			Policy:              input.Rating.Policy,
			ExpectedDividend:    input.Rating.ExpectedDividend,
			BenchmarkReturn:     input.Rating.BenchmarkReturn,
			BenchmarkEvidenceID: input.Rating.BenchmarkEvidenceID,
			ReasonCodes:         input.Rating.ReasonCodes,
			ChangedAssumptions:  input.Rating.ChangedAssumptions,
			EvidenceIDs:         input.Rating.EvidenceIDs,
			InvalidationRules:   input.Rating.InvalidationRules,
		},
		CadenceHours:     24,
		MaxPriceAgeHours: 120,
		MaxPlanAgeDays:   90,
		ApprovedBy:       "",
	}
}
