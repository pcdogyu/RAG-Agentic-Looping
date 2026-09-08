package prediction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/governance"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

type ShadowInput struct {
	AssetID               string            `json:"asset_id"`
	EventID               string            `json:"event_id,omitempty"`
	SignalAvailableAt     time.Time         `json:"signal_available_at"`
	IncumbentModelVersion string            `json:"incumbent_model_version"`
	CandidateModelVersion string            `json:"candidate_model_version"`
	Market                string            `json:"market"`
	EventType             string            `json:"event_type"`
	Features              []signals.Feature `json:"features"`
	ExecutionAssumptions  map[string]any    `json:"execution_assumptions"`
}

type ShadowComparison struct {
	ID                   string         `json:"id"`
	Status               string         `json:"status"`
	Incumbent            Run            `json:"incumbent"`
	Candidate            Run            `json:"candidate"`
	ExecutionAssumptions map[string]any `json:"execution_assumptions"`
	Metrics              map[string]any `json:"metrics"`
	Created              bool           `json:"created"`
}

type GovernanceCheck struct {
	ID               string         `json:"id"`
	SubjectType      string         `json:"subject_type"`
	SubjectVersion   string         `json:"subject_version"`
	ReferenceVersion string         `json:"reference_version,omitempty"`
	CheckType        string         `json:"check_type"`
	Status           string         `json:"status"`
	Action           string         `json:"action"`
	WindowStart      *time.Time     `json:"window_start,omitempty"`
	WindowEnd        *time.Time     `json:"window_end,omitempty"`
	Metrics          map[string]any `json:"metrics"`
	Reasons          []string       `json:"reasons"`
	ApprovedBy       string         `json:"approved_by,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

type governanceExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (s *Service) CompareShadow(ctx context.Context, input ShadowInput) (ShadowComparison, error) {
	if s.db == nil || input.AssetID == "" || input.SignalAvailableAt.IsZero() || input.IncumbentModelVersion == "" || input.CandidateModelVersion == "" {
		return ShadowComparison{}, fmt.Errorf("asset, signal cutoff, incumbent and candidate are required")
	}
	if len(input.ExecutionAssumptions) == 0 {
		return ShadowComparison{}, fmt.Errorf("execution_assumptions are required for an auditable shadow comparison")
	}
	var incumbentStatus, candidateStatus, incumbentObjective, candidateObjective, incumbentMarket, candidateMarket string
	var incumbentHorizon, candidateHorizon int
	query := `SELECT status,objective,market,horizon_sessions FROM prediction_models WHERE version=$1`
	if err := s.db.QueryRow(ctx, query, input.IncumbentModelVersion).Scan(&incumbentStatus, &incumbentObjective, &incumbentMarket, &incumbentHorizon); err != nil {
		return ShadowComparison{}, fmt.Errorf("load incumbent model: %w", err)
	}
	if err := s.db.QueryRow(ctx, query, input.CandidateModelVersion).Scan(&candidateStatus, &candidateObjective, &candidateMarket, &candidateHorizon); err != nil {
		return ShadowComparison{}, fmt.Errorf("load candidate model: %w", err)
	}
	if incumbentStatus != "approved" || candidateStatus != "shadow" {
		return ShadowComparison{}, fmt.Errorf("shadow comparison requires an approved incumbent and shadow candidate")
	}
	if incumbentObjective != candidateObjective || incumbentHorizon != candidateHorizon || !strings.EqualFold(incumbentMarket, candidateMarket) || !strings.EqualFold(input.Market, incumbentMarket) {
		return ShadowComparison{}, fmt.Errorf("models must share objective, horizon, market, and prediction cutoff")
	}
	common := Input{AssetID: input.AssetID, EventID: input.EventID, SignalAvailableAt: input.SignalAvailableAt.UTC(), Market: input.Market, EventType: input.EventType, Features: input.Features}
	common.ModelVersion = input.IncumbentModelVersion
	incumbent, err := s.Predict(ctx, common)
	if err != nil {
		return ShadowComparison{}, err
	}
	common.ModelVersion = input.CandidateModelVersion
	candidate, err := s.Predict(ctx, common)
	if err != nil {
		return ShadowComparison{}, err
	}
	metrics := map[string]any{"same_signal_cutoff": incumbent.SignalAvailableAt.Equal(candidate.SignalAvailableAt), "horizon_sessions": incumbent.HorizonSessions}
	if incumbent.RawScore != nil && candidate.RawScore != nil {
		metrics["raw_score_delta"] = *candidate.RawScore - *incumbent.RawScore
	}
	status := "matched"
	if incumbent.RawScore == nil || candidate.RawScore == nil {
		status = "incomplete"
	}
	id := stableID("shadow", input.AssetID, input.SignalAvailableAt.UTC().Format(time.RFC3339Nano), input.IncumbentModelVersion, input.CandidateModelVersion)
	assumptions, _ := json.Marshal(input.ExecutionAssumptions)
	metricBody, _ := json.Marshal(metrics)
	tag, err := s.db.Exec(ctx, `INSERT INTO shadow_prediction_comparisons(id,asset_id,signal_available_at,incumbent_model_version,candidate_model_version,incumbent_run_id,candidate_run_id,execution_assumptions,status,metrics) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(asset_id,signal_available_at,incumbent_model_version,candidate_model_version) DO NOTHING`, id, input.AssetID, input.SignalAvailableAt.UTC(), input.IncumbentModelVersion, input.CandidateModelVersion, incumbent.ID, candidate.ID, assumptions, status, metricBody)
	if err != nil {
		return ShadowComparison{}, err
	}
	return ShadowComparison{ID: id, Status: status, Incumbent: incumbent, Candidate: candidate, ExecutionAssumptions: input.ExecutionAssumptions, Metrics: metrics, Created: tag.RowsAffected() == 1}, nil
}

// MonitorShadowModels persists review evidence. It never promotes, retrains, or
// rolls back a model; drift can only create a human-review action.
func (s *Service) MonitorShadowModels(ctx context.Context, now time.Time) ([]GovernanceCheck, error) {
	if s.db == nil {
		return nil, fmt.Errorf("prediction store is unavailable")
	}
	rows, err := s.db.Query(ctx, `SELECT candidate.version,coalesce((SELECT incumbent.version FROM prediction_models incumbent WHERE incumbent.status='approved' AND incumbent.objective=candidate.objective AND incumbent.market=candidate.market AND incumbent.horizon_sessions=candidate.horizon_sessions ORDER BY incumbent.approved_at DESC NULLS LAST,incumbent.created_at DESC LIMIT 1),'') FROM prediction_models candidate WHERE candidate.status='shadow' ORDER BY candidate.version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	pairs := [][2]string{}
	for rows.Next() {
		var pair [2]string
		if err := rows.Scan(&pair[0], &pair[1]); err != nil {
			return nil, err
		}
		pairs = append(pairs, pair)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	checks := make([]GovernanceCheck, 0, len(pairs))
	for _, pair := range pairs {
		check, monitorErr := s.monitorShadowModel(ctx, pair[0], pair[1], now.UTC())
		if monitorErr != nil {
			return nil, monitorErr
		}
		checks = append(checks, check)
	}
	return checks, nil
}

func (s *Service) monitorShadowModel(ctx context.Context, candidate, incumbent string, now time.Time) (GovernanceCheck, error) {
	check := GovernanceCheck{SubjectType: "model", SubjectVersion: candidate, ReferenceVersion: incumbent, CheckType: "shadow_monitor", Status: "insufficient_data", Action: "collect_more_forward_samples", Metrics: map[string]any{}, Reasons: []string{}, CreatedAt: now}
	if incumbent == "" {
		check.Reasons = []string{"approved_incumbent_unavailable"}
		return check, s.recordGovernanceCheck(ctx, &check, "scheduled:"+candidate+":"+now.Format("2006-01-02T15"))
	}
	rows, err := s.db.Query(ctx, `SELECT candidate.raw_score,incumbent.raw_score,c.signal_available_at,c.created_at,candidate.feature_snapshot::jsonb,oc.excess_return,oi.excess_return FROM shadow_prediction_comparisons c JOIN prediction_runs candidate ON candidate.id=c.candidate_run_id JOIN prediction_runs incumbent ON incumbent.id=c.incumbent_run_id LEFT JOIN outcome_records oc ON oc.prediction_run_id=c.candidate_run_id AND oc.status='mature' LEFT JOIN outcome_records oi ON oi.prediction_run_id=c.incumbent_run_id AND oi.status='mature' WHERE c.candidate_model_version=$1 AND c.incumbent_model_version=$2 ORDER BY c.signal_available_at DESC LIMIT 200`, candidate, incumbent)
	if err != nil {
		return check, err
	}
	defer rows.Close()
	type sample struct {
		candidate, incumbent             *float64
		signal, created                  time.Time
		featureSnapshot                  []byte
		candidateReturn, incumbentReturn *float64
	}
	samples := []sample{}
	for rows.Next() {
		var item sample
		if err := rows.Scan(&item.candidate, &item.incumbent, &item.signal, &item.created, &item.featureSnapshot, &item.candidateReturn, &item.incumbentReturn); err != nil {
			return check, err
		}
		samples = append(samples, item)
	}
	if err := rows.Err(); err != nil {
		return check, err
	}
	valid, positive, mature := 0, 0, 0
	deltas, latencies := []float64{}, []float64{}
	returns := 0.0
	for _, item := range samples {
		latencies = append(latencies, math.Max(0, item.created.Sub(item.signal).Seconds()))
		if item.candidate != nil && item.incumbent != nil {
			valid++
			deltas = append(deltas, *item.candidate-*item.incumbent)
			if *item.candidate >= 0 {
				positive++
			}
		}
		if item.candidateReturn != nil && item.incumbentReturn != nil {
			mature++
			returns += *item.candidateReturn - *item.incumbentReturn
		}
	}
	coverage, directionShare := 0.0, 0.0
	if len(samples) > 0 {
		coverage = float64(valid) / float64(len(samples))
	}
	if valid > 0 {
		directionShare = float64(positive) / float64(valid)
	}
	check.Metrics = map[string]any{"paired_samples": len(samples), "valid_pairs": valid, "coverage": coverage, "candidate_positive_share": directionShare, "mean_raw_score_delta": mean(deltas), "mean_latency_seconds": mean(latencies), "mature_pairs": mature}
	if mature > 0 {
		check.Metrics["mean_excess_return_delta"] = returns / float64(mature)
	}
	if len(samples) > 0 {
		start, end := samples[len(samples)-1].signal.UTC(), samples[0].signal.UTC()
		check.WindowStart, check.WindowEnd = &start, &end
	}
	if len(samples) < 20 {
		check.Reasons = append(check.Reasons, "fewer_than_20_shadow_attempts")
	} else {
		if coverage < .9 {
			check.Reasons = append(check.Reasons, "coverage_below_90_percent")
		}
		if valid >= 20 && (directionShare < .1 || directionShare > .9) {
			check.Reasons = append(check.Reasons, "direction_bias_above_policy_limit")
		}
		if valid < 40 {
			check.Reasons = append(check.Reasons, "fewer_than_40_valid_paired_forward_samples")
		}
	}
	if valid >= 40 {
		recent, reference := make([]float64, 0, valid/2), make([]float64, 0, valid/2)
		for index, delta := range deltas {
			if index < len(deltas)/2 {
				recent = append(recent, delta)
			} else {
				reference = append(reference, delta)
			}
		}
		drift, driftErr := governance.PopulationStability(governance.DriftInput{Reference: reference, Current: recent, Bins: 10, WarningThreshold: .2})
		if driftErr == nil && drift.PSI != nil {
			check.Metrics["paired_delta_psi"] = *drift.PSI
		}
		if driftErr == nil && drift.Status == "drifted" {
			check.Reasons = append(check.Reasons, "paired_score_distribution_drift")
		}
	}
	if len(samples) >= 20 {
		recentSources, referenceSources := map[string]bool{}, map[string]bool{}
		for index, item := range samples {
			target := recentSources
			if index >= len(samples)/2 {
				target = referenceSources
			}
			var snapshot signals.Snapshot
			if json.Unmarshal(item.featureSnapshot, &snapshot) == nil {
				for _, ids := range snapshot.SourceIDs {
					for _, id := range ids {
						if id = strings.TrimSpace(id); id != "" {
							target[id] = true
						}
					}
				}
			}
		}
		sourceOverlap := setOverlap(referenceSources, recentSources)
		check.Metrics["source_id_overlap"] = sourceOverlap
		if len(referenceSources) > 0 && len(recentSources) > 0 && sourceOverlap < .5 {
			check.Reasons = append(check.Reasons, "source_lineage_changed")
		}
		if len(check.Reasons) == 0 {
			check.Status, check.Action = "stable", "observe"
		} else {
			check.Status, check.Action = "review_required", "alert_and_review"
		}
	}
	err = s.recordGovernanceCheck(ctx, &check, "scheduled:"+candidate+":"+now.Format("2006-01-02T15"))
	return check, err
}

func (s *Service) RecordPromotionDecision(ctx context.Context, subjectType, version string, input governance.PromotionInput, decision governance.Decision, now time.Time) error {
	return recordPromotionDecisionWith(ctx, s.db, subjectType, version, input, decision, now)
}

func recordPromotionDecisionWith(ctx context.Context, execer governanceExecer, subjectType, version string, input governance.PromotionInput, decision governance.Decision, now time.Time) error {
	check := GovernanceCheck{SubjectType: subjectType, SubjectVersion: strings.TrimSpace(version), CheckType: "promotion", Status: decision.Status, Action: "remain_shadow", Metrics: map[string]any{"hard_correctness_passed": input.HardCorrectnessPassed, "independent_samples": input.IndependentSamples, "minimum_samples": input.MinimumSamples, "minimum_shadow_days": input.MinimumShadowDays, "ece": input.ECE, "maximum_ece": input.MaximumECE, "final_holdout_used_for_selection": input.FinalHoldoutUsedForSelection}, Reasons: decision.Reasons, ApprovedBy: strings.TrimSpace(input.ApprovedBy), CreatedAt: now.UTC()}
	if decision.Status == "approved" {
		check.Action = "promote"
	}
	return recordGovernanceCheckWith(ctx, execer, &check, stableID("promotion-attempt", subjectType, version, now.UTC().Format(time.RFC3339Nano)))
}

func (s *Service) recordGovernanceCheck(ctx context.Context, check *GovernanceCheck, idempotencyKey string) error {
	return recordGovernanceCheckWith(ctx, s.db, check, idempotencyKey)
}

func recordGovernanceCheckWith(ctx context.Context, execer governanceExecer, check *GovernanceCheck, idempotencyKey string) error {
	check.ID = stableID("governance", idempotencyKey)
	metrics, _ := json.Marshal(check.Metrics)
	reasons, _ := json.Marshal(check.Reasons)
	_, err := execer.Exec(ctx, `INSERT INTO model_governance_checks(id,subject_type,subject_version,reference_version,check_type,status,action,window_start,window_end,metrics,reasons,approved_by,idempotency_key,created_at) VALUES($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''),$13,$14) ON CONFLICT(idempotency_key) DO NOTHING`, check.ID, check.SubjectType, check.SubjectVersion, check.ReferenceVersion, check.CheckType, check.Status, check.Action, check.WindowStart, check.WindowEnd, metrics, reasons, check.ApprovedBy, idempotencyKey, check.CreatedAt)
	return err
}

func (s *Service) ListGovernanceChecks(ctx context.Context, limit int) ([]GovernanceCheck, error) {
	if limit < 1 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.Query(ctx, `SELECT id,subject_type,subject_version,coalesce(reference_version,''),check_type,status,action,window_start,window_end,metrics::jsonb,reasons::jsonb,coalesce(approved_by,''),created_at FROM model_governance_checks ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []GovernanceCheck{}
	for rows.Next() {
		var item GovernanceCheck
		var metrics, reasons []byte
		if err := rows.Scan(&item.ID, &item.SubjectType, &item.SubjectVersion, &item.ReferenceVersion, &item.CheckType, &item.Status, &item.Action, &item.WindowStart, &item.WindowEnd, &metrics, &reasons, &item.ApprovedBy, &item.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(metrics, &item.Metrics)
		_ = json.Unmarshal(reasons, &item.Reasons)
		items = append(items, item)
	}
	return items, rows.Err()
}

func stableID(prefix string, values ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return prefix + "-" + hex.EncodeToString(sum[:])[:40]
}

func mean(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	result := total / float64(len(values))
	return &result
}

func setOverlap(left, right map[string]bool) float64 {
	union, intersection := map[string]bool{}, 0
	for value := range left {
		union[value] = true
		if right[value] {
			intersection++
		}
	}
	for value := range right {
		union[value] = true
	}
	if len(union) == 0 {
		return 1
	}
	return float64(intersection) / float64(len(union))
}
