package fundamentalresearch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/analystevidence"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/forecast"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentals"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/rating"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/valuation"
)

const ScheduledResearchVersion = "scheduled-fundamental-research-v1"

type ScheduledRatingPlan struct {
	Policy              rating.Policy             `json:"policy"`
	ExpectedDividend    *float64                  `json:"expected_dividend,omitempty"`
	BenchmarkReturn     *float64                  `json:"benchmark_return,omitempty"`
	BenchmarkEvidenceID string                    `json:"benchmark_evidence_id,omitempty"`
	ReasonCodes         []string                  `json:"reason_codes"`
	ChangedAssumptions  map[string]any            `json:"changed_assumptions"`
	EvidenceIDs         []string                  `json:"evidence_ids"`
	InvalidationRules   []rating.InvalidationRule `json:"invalidation_rules"`
}

type PlanSubmission struct {
	AssetID           string              `json:"asset_id"`
	ForecastVersionID string              `json:"forecast_version_id"`
	Valuation         ValuationPlan       `json:"valuation"`
	Rating            ScheduledRatingPlan `json:"rating"`
	CadenceHours      int                 `json:"cadence_hours"`
	MaxPriceAgeHours  int                 `json:"max_price_age_hours"`
	MaxPlanAgeDays    int                 `json:"max_plan_age_days"`
	ApprovedBy        string              `json:"approved_by"`
	ApprovalKind      string              `json:"approval_kind,omitempty"`
	PolicyVersion     string              `json:"policy_version,omitempty"`
	IdempotencyKey    string              `json:"-"`
}

type Plan struct {
	ID                      string              `json:"id"`
	AssetID                 string              `json:"asset_id"`
	ForecastVersionID       string              `json:"forecast_version_id"`
	Valuation               ValuationPlan       `json:"valuation"`
	Rating                  ScheduledRatingPlan `json:"rating"`
	CadenceHours            int                 `json:"cadence_hours"`
	MaxPriceAgeHours        int                 `json:"max_price_age_hours"`
	MaxPlanAgeDays          int                 `json:"max_plan_age_days"`
	Status                  string              `json:"status"`
	ApprovedBy              string              `json:"approved_by"`
	ApprovalKind            string              `json:"approval_kind"`
	PolicyVersion           string              `json:"policy_version,omitempty"`
	ApprovedAt              time.Time           `json:"approved_at"`
	EvidenceContractVersion string              `json:"evidence_contract_version"`
	NextRunAt               time.Time           `json:"next_run_at"`
	LastRunAt               *time.Time          `json:"last_run_at,omitempty"`
	LastRunStatus           string              `json:"last_run_status,omitempty"`
	LastRunReason           string              `json:"last_run_reason,omitempty"`
	LastResult              map[string]any      `json:"last_result"`
	CreatedAt               time.Time           `json:"created_at"`
	UpdatedAt               time.Time           `json:"updated_at"`
}

type ScheduledResult struct {
	Version           string                       `json:"version"`
	PlanID            string                       `json:"plan_id"`
	AssetID           string                       `json:"asset_id"`
	Status            string                       `json:"status"`
	Reason            string                       `json:"reason,omitempty"`
	AsOf              time.Time                    `json:"as_of"`
	ForecastVersionID string                       `json:"forecast_version_id"`
	Price             *marketdata.PriceObservation `json:"price,omitempty"`
	Valuation         *valuation.Run               `json:"valuation,omitempty"`
	Rating            *rating.Snapshot             `json:"rating,omitempty"`
}

type PlanStore struct{ db *pgxpool.Pool }

func NewPlanStore(db *pgxpool.Pool) *PlanStore { return &PlanStore{db: db} }

func (s *PlanStore) Approve(ctx context.Context, submission PlanSubmission, approvedAt time.Time) (Plan, bool, error) {
	if s.db == nil || approvedAt.IsZero() {
		return Plan{}, false, fmt.Errorf("scheduled fundamental research store and approved_at are required")
	}
	submission.AssetID = strings.TrimSpace(submission.AssetID)
	submission.ForecastVersionID = strings.TrimSpace(submission.ForecastVersionID)
	submission.ApprovedBy = strings.TrimSpace(submission.ApprovedBy)
	submission.ApprovalKind = strings.ToLower(strings.TrimSpace(submission.ApprovalKind))
	if submission.ApprovalKind == "" {
		submission.ApprovalKind = "human"
	}
	submission.PolicyVersion = strings.TrimSpace(submission.PolicyVersion)
	submission.IdempotencyKey = strings.TrimSpace(submission.IdempotencyKey)
	if submission.AssetID == "" || submission.ForecastVersionID == "" || submission.ApprovedBy == "" || submission.IdempotencyKey == "" {
		return Plan{}, false, fmt.Errorf("asset_id, forecast_version_id, approved_by and idempotency key are required")
	}
	if submission.ApprovalKind != "human" && submission.ApprovalKind != "policy" {
		return Plan{}, false, fmt.Errorf("approval_kind must be human or policy")
	}
	if submission.ApprovalKind == "policy" && (submission.PolicyVersion == "" || !strings.HasPrefix(submission.ApprovedBy, "policy:")) {
		return Plan{}, false, fmt.Errorf("policy-approved research plans require policy_version and an explicit policy actor")
	}
	if submission.ApprovalKind == "human" && submission.PolicyVersion != "" {
		return Plan{}, false, fmt.Errorf("human-approved research plans must not claim a policy version")
	}
	if submission.CadenceHours == 0 {
		submission.CadenceHours = 24
	}
	if submission.MaxPriceAgeHours == 0 {
		submission.MaxPriceAgeHours = 120
	}
	if submission.MaxPlanAgeDays == 0 {
		submission.MaxPlanAgeDays = 90
	}
	if submission.CadenceHours < 1 || submission.CadenceHours > 720 || submission.MaxPriceAgeHours < 1 || submission.MaxPriceAgeHours > 336 || submission.MaxPlanAgeDays < 1 || submission.MaxPlanAgeDays > 365 {
		return Plan{}, false, fmt.Errorf("scheduled fundamental research cadence or age limit is invalid")
	}
	if err := validateScheduledPlans(submission.Valuation, submission.Rating); err != nil {
		return Plan{}, false, err
	}
	if existing, readErr := scanPlan(s.db.QueryRow(ctx, planSelect+` WHERE idempotency_key=$1`, submission.IdempotencyKey)); readErr == nil {
		if !samePlanSubmission(existing, submission) {
			return Plan{}, false, fmt.Errorf("idempotency key was already used for a different research plan")
		}
		return existing, false, nil
	} else if readErr != pgx.ErrNoRows {
		return Plan{}, false, readErr
	}
	version, err := forecast.NewStore(s.db).Get(ctx, submission.AssetID, submission.ForecastVersionID)
	if err != nil || version.Status != "available" {
		return Plan{}, false, fmt.Errorf("approved schedule requires an available forecast version")
	}
	var assetClass, market, currency string
	if err := s.db.QueryRow(ctx, `SELECT asset_class,market,currency FROM assets WHERE id=$1 AND active=true`, submission.AssetID).Scan(&assetClass, &market, &currency); err != nil {
		return Plan{}, false, fmt.Errorf("load scheduled research asset: %w", err)
	}
	policy := marketpolicy.Resolve(assetClass, market)
	if err := marketpolicy.ValidateFundamental(policy, version.Projection.Currency, submission.Rating.Policy.Market, submission.Rating.Policy.AssetClass, submission.Rating.Policy.BenchmarkID); err != nil {
		return Plan{}, false, err
	}
	if !strings.EqualFold(currency, policy.Currency) {
		return Plan{}, false, fmt.Errorf("asset_currency_outside_market_policy")
	}
	if err := validateForecastCoverage(ctx, s.db, version); err != nil {
		return Plan{}, false, err
	}
	var newer bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM fundamental_snapshots WHERE asset_id=$1 AND available_at>$2 AND available_at<=$3)`, submission.AssetID, version.AsOf, approvedAt.UTC()).Scan(&newer); err != nil {
		return Plan{}, false, err
	}
	if newer {
		return Plan{}, false, fmt.Errorf("forecast review is required because newer financial statements are available")
	}
	if err := validateScheduledResearchEvidence(ctx, s.db, submission.AssetID, version, submission.Valuation, submission.Rating, approvedAt); err != nil {
		return Plan{}, false, fmt.Errorf("evidence gate: %w", err)
	}
	approvedAt = approvedAt.UTC()
	valuationBody, _ := json.Marshal(submission.Valuation)
	ratingBody, _ := json.Marshal(submission.Rating)
	id := scheduleID(submission.IdempotencyKey)
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Plan{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if existing, readErr := scanPlan(tx.QueryRow(ctx, planSelect+` WHERE idempotency_key=$1`, submission.IdempotencyKey)); readErr == nil {
		if !samePlanSubmission(existing, submission) {
			return Plan{}, false, fmt.Errorf("idempotency key was already used for a different research plan")
		}
		return existing, false, tx.Commit(ctx)
	} else if readErr != pgx.ErrNoRows {
		return Plan{}, false, readErr
	}
	if _, err := tx.Exec(ctx, `UPDATE fundamental_research_plans SET status='paused',updated_at=$2 WHERE asset_id=$1 AND status='approved'`, submission.AssetID, approvedAt); err != nil {
		return Plan{}, false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO fundamental_research_plans(id,asset_id,forecast_version_id,valuation_plan,rating_plan,cadence_hours,max_price_age_hours,max_plan_age_days,status,idempotency_key,approved_by,approval_kind,policy_version,approved_at,next_run_at,evidence_contract_version)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,'approved',$9,$10,$11,$12,$13,$13,$14)`, id, submission.AssetID, submission.ForecastVersionID, valuationBody, ratingBody, submission.CadenceHours, submission.MaxPriceAgeHours, submission.MaxPlanAgeDays, submission.IdempotencyKey, submission.ApprovedBy, submission.ApprovalKind, submission.PolicyVersion, approvedAt, analystevidence.ContractVersion)
	if err != nil {
		return Plan{}, false, err
	}
	plan, err := scanPlan(tx.QueryRow(ctx, planSelect+` WHERE id=$1`, id))
	if err != nil {
		return Plan{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Plan{}, false, err
	}
	return plan, true, nil
}

func samePlanSubmission(plan Plan, submission PlanSubmission) bool {
	left, _ := json.Marshal(struct {
		AssetID, ForecastVersionID, ApprovedBy, ApprovalKind, PolicyVersion string
		Valuation                                                           ValuationPlan
		Rating                                                              ScheduledRatingPlan
		CadenceHours, MaxPriceAgeHours, MaxPlanAgeDays                      int
	}{plan.AssetID, plan.ForecastVersionID, plan.ApprovedBy, plan.ApprovalKind, plan.PolicyVersion, plan.Valuation, plan.Rating, plan.CadenceHours, plan.MaxPriceAgeHours, plan.MaxPlanAgeDays})
	right, _ := json.Marshal(struct {
		AssetID, ForecastVersionID, ApprovedBy, ApprovalKind, PolicyVersion string
		Valuation                                                           ValuationPlan
		Rating                                                              ScheduledRatingPlan
		CadenceHours, MaxPriceAgeHours, MaxPlanAgeDays                      int
	}{submission.AssetID, submission.ForecastVersionID, submission.ApprovedBy, submission.ApprovalKind, submission.PolicyVersion, submission.Valuation, submission.Rating, submission.CadenceHours, submission.MaxPriceAgeHours, submission.MaxPlanAgeDays})
	return string(left) == string(right)
}

func validateForecastCoverage(ctx context.Context, db *pgxpool.Pool, version forecast.Version) error {
	snapshots, err := fundamentals.NewStore(db).ListAvailable(ctx, version.AssetID, version.AsOf, 100)
	if err != nil {
		return err
	}
	researchContext := fundamentals.BuildResearchContext(version.AssetID, version.AsOf, snapshots)
	if researchContext.Status != "available" {
		return fmt.Errorf("approved schedule requires complete point-in-time financial statements: %s", researchContext.Reason)
	}
	referenced := map[string]bool{}
	switch values := version.InputSnapshot["fundamental_snapshot_ids"].(type) {
	case []any:
		for _, value := range values {
			referenced[strings.TrimSpace(fmt.Sprint(value))] = true
		}
	case []string:
		for _, value := range values {
			referenced[strings.TrimSpace(value)] = true
		}
	}
	latestByType := map[fundamentals.StatementType]string{}
	for _, snapshot := range researchContext.Snapshots {
		if latestByType[snapshot.StatementType] == "" {
			latestByType[snapshot.StatementType] = snapshot.SnapshotID
		}
	}
	for _, kind := range []fundamentals.StatementType{fundamentals.IncomeStatement, fundamentals.BalanceSheet, fundamentals.CashFlow} {
		if latestByType[kind] == "" || !referenced[latestByType[kind]] {
			return fmt.Errorf("approved forecast must reference the latest available %s snapshot", kind)
		}
	}
	return nil
}

func validateScheduledPlans(valuationPlan ValuationPlan, ratingPlan ScheduledRatingPlan) error {
	if len(valuationPlan.DCFScenarios) == 0 && len(valuationPlan.MultipleScenarios) == 0 {
		return fmt.Errorf("scheduled research requires at least one valuation scenario")
	}
	for _, scenario := range valuationPlan.DCFScenarios {
		if strings.TrimSpace(scenario.Name) == "" || len(scenario.CostOfCapitalEvidenceIDs) == 0 || strings.TrimSpace(valuationPlan.NetDebtSnapshotID) == "" {
			return fmt.Errorf("scheduled DCF requires named scenarios, capital-cost evidence and a net-debt snapshot")
		}
	}
	for _, scenario := range valuationPlan.MultipleScenarios {
		if strings.TrimSpace(scenario.Name) == "" || len(scenario.ComparableEvidenceIDs) == 0 {
			return fmt.Errorf("scheduled multiple valuation requires named scenarios and comparable evidence")
		}
	}
	if ratingPlan.Policy.Version == "" || len(ratingPlan.ReasonCodes) == 0 || len(ratingPlan.EvidenceIDs) == 0 {
		return fmt.Errorf("scheduled rating requires policy, reason codes and evidence ids")
	}
	if ratingPlan.Policy.RelativeRequired && (ratingPlan.BenchmarkReturn == nil || strings.TrimSpace(ratingPlan.BenchmarkEvidenceID) == "") {
		return fmt.Errorf("scheduled relative rating requires an approved benchmark expectation and evidence")
	}
	return nil
}

const planSelect = `SELECT id,asset_id,forecast_version_id,valuation_plan::jsonb,rating_plan::jsonb,cadence_hours,max_price_age_hours,max_plan_age_days,status,approved_by,approval_kind,policy_version,approved_at,coalesce(evidence_contract_version,''),next_run_at,last_run_at,last_run_status,last_run_reason,last_result::jsonb,created_at,updated_at FROM fundamental_research_plans`

func scanPlan(row pgx.Row) (Plan, error) {
	var plan Plan
	var valuationRaw, ratingRaw, resultRaw any
	err := row.Scan(&plan.ID, &plan.AssetID, &plan.ForecastVersionID, &valuationRaw, &ratingRaw, &plan.CadenceHours, &plan.MaxPriceAgeHours, &plan.MaxPlanAgeDays, &plan.Status, &plan.ApprovedBy, &plan.ApprovalKind, &plan.PolicyVersion, &plan.ApprovedAt, &plan.EvidenceContractVersion, &plan.NextRunAt, &plan.LastRunAt, &plan.LastRunStatus, &plan.LastRunReason, &resultRaw, &plan.CreatedAt, &plan.UpdatedAt)
	if err != nil {
		return Plan{}, err
	}
	if err := decodePlanJSON(valuationRaw, &plan.Valuation); err != nil {
		return Plan{}, err
	}
	if err := decodePlanJSON(ratingRaw, &plan.Rating); err != nil {
		return Plan{}, err
	}
	if err := decodePlanJSON(resultRaw, &plan.LastResult); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func decodePlanJSON(raw, target any) error {
	var body []byte
	switch value := raw.(type) {
	case []byte:
		body = value
	case string:
		body = []byte(value)
	default:
		var err error
		body, err = json.Marshal(value)
		if err != nil {
			return err
		}
	}
	return json.Unmarshal(body, target)
}

func (s *PlanStore) Current(ctx context.Context, assetID string) (*Plan, error) {
	plan, err := scanPlan(s.db.QueryRow(ctx, planSelect+` WHERE asset_id=$1 AND status='approved' ORDER BY approved_at DESC LIMIT 1`, strings.TrimSpace(assetID)))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &plan, nil
}

func (s *PlanStore) History(ctx context.Context, assetID string, limit int) ([]Plan, error) {
	assetID = strings.TrimSpace(assetID)
	if s.db == nil || assetID == "" || limit < 1 || limit > 100 {
		return nil, fmt.Errorf("invalid scheduled fundamental research history query")
	}
	rows, err := s.db.Query(ctx, planSelect+` WHERE asset_id=$1 ORDER BY approved_at DESC,id DESC LIMIT $2`, assetID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	plans := []Plan{}
	for rows.Next() {
		plan, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, rows.Err()
}

func (s *PlanStore) Pause(ctx context.Context, assetID string, pausedAt time.Time) (bool, error) {
	if s.db == nil || strings.TrimSpace(assetID) == "" || pausedAt.IsZero() {
		return false, fmt.Errorf("asset_id and paused_at are required")
	}
	tag, err := s.db.Exec(ctx, `UPDATE fundamental_research_plans SET status='paused',updated_at=$2 WHERE asset_id=$1 AND status='approved'`, strings.TrimSpace(assetID), pausedAt.UTC())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *PlanStore) Due(ctx context.Context, now time.Time, limit int) ([]Plan, error) {
	if s.db == nil || now.IsZero() || limit < 1 || limit > 100 {
		return nil, fmt.Errorf("invalid scheduled fundamental research due query")
	}
	rows, err := s.db.Query(ctx, planSelect+` WHERE status='approved' AND next_run_at<=$1 ORDER BY next_run_at,asset_id LIMIT $2`, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	plans := []Plan{}
	for rows.Next() {
		plan, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, rows.Err()
}

func (s *PlanStore) Record(ctx context.Context, plan Plan, result ScheduledResult, completedAt time.Time) error {
	status := result.Status
	if status == "" {
		status = "technical_failure"
	}
	planStatus := "approved"
	if status == "review_required" || status == "not_applicable" {
		planStatus = "review_required"
	}
	body, _ := json.Marshal(result)
	delay := time.Duration(plan.CadenceHours) * time.Hour
	if (status == "technical_failure" || status == "data_refresh_failed") && delay > time.Hour {
		delay = time.Hour
	}
	_, err := s.db.Exec(ctx, `UPDATE fundamental_research_plans SET status=CASE WHEN status='approved' THEN $2 ELSE status END,last_run_at=$3,last_run_status=$4,last_run_reason=$5,last_result=$6,next_run_at=$7,updated_at=$3 WHERE id=$1`, plan.ID, planStatus, completedAt.UTC(), status, result.Reason, body, completedAt.UTC().Add(delay))
	return err
}

func (s *PlanStore) Run(ctx context.Context, plan Plan, now time.Time) (ScheduledResult, error) {
	result := ScheduledResult{Version: ScheduledResearchVersion, PlanID: plan.ID, AssetID: plan.AssetID, Status: "insufficient_data", AsOf: now.UTC(), ForecastVersionID: plan.ForecastVersionID}
	if now.IsZero() {
		return result, fmt.Errorf("scheduled research time is required")
	}
	var currentStatus, currentEvidenceContract string
	if err := s.db.QueryRow(ctx, `SELECT status,coalesce(evidence_contract_version,'') FROM fundamental_research_plans WHERE id=$1`, plan.ID).Scan(&currentStatus, &currentEvidenceContract); err != nil {
		return result, err
	}
	if currentStatus != "approved" {
		result.Status, result.Reason = "not_applicable", "scheduled_research_plan_is_not_approved"
		return result, nil
	}
	if currentEvidenceContract != analystevidence.ContractVersion {
		result.Status, result.Reason = "review_required", "analyst_evidence_registration_required"
		return result, nil
	}
	if now.UTC().After(plan.ApprovedAt.AddDate(0, 0, plan.MaxPlanAgeDays)) {
		result.Status, result.Reason = "review_required", "approved_research_plan_expired"
		return result, nil
	}
	version, err := forecast.NewStore(s.db).Get(ctx, plan.AssetID, plan.ForecastVersionID)
	if err != nil || version.Status != "available" {
		result.Status, result.Reason = "review_required", "approved_forecast_unavailable"
		return result, nil
	}
	var newer bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM fundamental_snapshots WHERE asset_id=$1 AND available_at>$2 AND available_at<=$3)`, plan.AssetID, version.AsOf, now.UTC()).Scan(&newer); err != nil {
		return result, err
	}
	if newer {
		result.Status, result.Reason = "review_required", "new_financial_statement_requires_forecast_review"
		return result, nil
	}
	if err := validateForecastCoverage(ctx, s.db, version); err != nil {
		result.Status, result.Reason = "review_required", "forecast_financial_coverage_changed"
		return result, nil
	}
	prices, err := marketdata.NewStore(s.db).ListAvailable(ctx, plan.AssetID, now.UTC(), now.UTC(), "adjusted_close", 1)
	if err != nil {
		return result, err
	}
	if len(prices) == 0 {
		result.Reason = "adjusted_close_unavailable"
		return result, nil
	}
	price := prices[0]
	result.Price = &price
	if now.UTC().Sub(price.ObservedAt) > time.Duration(plan.MaxPriceAgeHours)*time.Hour {
		result.Reason = "adjusted_close_is_stale"
		return result, nil
	}
	effectiveAt := latestTime(version.AsOf, plan.ApprovedAt, price.AvailableAt)
	result.AsOf = effectiveAt
	valuationRun, _, err := valuation.NewStore(s.db).Create(ctx, valuation.Submission{AssetID: plan.AssetID, AsOf: effectiveAt, ForecastVersionID: plan.ForecastVersionID, NetDebtSnapshotID: plan.Valuation.NetDebtSnapshotID, DCFScenarios: plan.Valuation.DCFScenarios, MultipleScenarios: plan.Valuation.MultipleScenarios, SensitivityWACC: plan.Valuation.SensitivityWACC, SensitivityGrowth: plan.Valuation.SensitivityGrowth})
	if err != nil {
		return result, err
	}
	result.Valuation = &valuationRun
	if valuationRun.Status != "available" {
		result.Reason = valuationRun.Result.Reason
		return result, nil
	}
	ratingSnapshot, err := rating.NewStore(s.db).EvaluateAndPersist(ctx, rating.Submission{AssetID: plan.AssetID, Policy: plan.Rating.Policy, ValuationRunID: valuationRun.ID, AsOfPrice: &price.Price, AsOfPriceEvidenceID: price.ID, ExpectedDividend: plan.Rating.ExpectedDividend, BenchmarkReturn: plan.Rating.BenchmarkReturn, BenchmarkEvidenceID: plan.Rating.BenchmarkEvidenceID, EffectiveAt: effectiveAt, ReasonCodes: append([]string{"scheduled_fundamental_review"}, plan.Rating.ReasonCodes...), ChangedAssumptions: plan.Rating.ChangedAssumptions, EvidenceIDs: plan.Rating.EvidenceIDs, InvalidationRules: plan.Rating.InvalidationRules})
	if err != nil {
		return result, err
	}
	result.Rating = &ratingSnapshot
	result.Reason = ratingSnapshot.Result.Reason
	if ratingSnapshot.Result.Status == "available" {
		result.Status = "completed"
	} else {
		result.Status = "insufficient_data"
	}
	return result, nil
}

func latestTime(values ...time.Time) time.Time {
	latest := time.Time{}
	for _, value := range values {
		if value.After(latest) {
			latest = value.UTC()
		}
	}
	return latest
}

func scheduleID(key string) string {
	hash := sha256.Sum256([]byte(strings.TrimSpace(key)))
	return "fundamental-plan-" + hex.EncodeToString(hash[:])[:40]
}
