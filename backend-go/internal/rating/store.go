package rating

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
)

type InvalidationRule struct {
	ID          string         `json:"id,omitempty"`
	RuleType    string         `json:"rule_type"`
	Operator    string         `json:"operator"`
	Threshold   any            `json:"threshold"`
	EvidenceIDs []string       `json:"evidence_ids"`
	Status      string         `json:"status,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type Submission struct {
	AssetID             string             `json:"asset_id"`
	Policy              Policy             `json:"policy"`
	ValuationRunID      string             `json:"valuation_run_id"`
	AsOfPrice           *float64           `json:"as_of_price"`
	AsOfPriceEvidenceID string             `json:"as_of_price_evidence_id"`
	ExpectedDividend    *float64           `json:"expected_dividend,omitempty"`
	BenchmarkReturn     *float64           `json:"benchmark_return,omitempty"`
	BenchmarkEvidenceID string             `json:"benchmark_evidence_id,omitempty"`
	EffectiveAt         time.Time          `json:"effective_at"`
	ReasonCodes         []string           `json:"reason_codes"`
	ChangedAssumptions  map[string]any     `json:"changed_assumptions"`
	EvidenceIDs         []string           `json:"evidence_ids"`
	InvalidationRules   []InvalidationRule `json:"invalidation_rules"`
}

type Snapshot struct {
	State              *State             `json:"state,omitempty"`
	Revision           Revision           `json:"revision"`
	Result             Result             `json:"result"`
	ReasonCodes        []string           `json:"reason_codes"`
	ChangedAssumptions map[string]any     `json:"changed_assumptions"`
	EvidenceIDs        []string           `json:"evidence_ids"`
	InvalidationRules  []InvalidationRule `json:"invalidation_rules"`
	Created            bool               `json:"created"`
}

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

func (s *Store) EvaluateAndPersist(ctx context.Context, input Submission) (Snapshot, error) {
	if s.db == nil {
		return Snapshot{}, fmt.Errorf("rating store is unavailable")
	}
	input.AssetID, input.ValuationRunID = strings.TrimSpace(input.AssetID), strings.TrimSpace(input.ValuationRunID)
	if input.AssetID == "" || input.ValuationRunID == "" || input.EffectiveAt.IsZero() || input.AsOfPrice == nil || strings.TrimSpace(input.AsOfPriceEvidenceID) == "" {
		return Snapshot{}, fmt.Errorf("asset_id, valuation_run_id, effective_at, as_of_price and as_of_price_evidence_id are required")
	}
	if input.Policy.Version == "" {
		input.Policy = DefaultUSPolicy()
	}
	if input.Policy.RelativeRequired && (input.BenchmarkReturn == nil || strings.TrimSpace(input.BenchmarkEvidenceID) == "") {
		return Snapshot{}, fmt.Errorf("relative rating requires benchmark_return and benchmark_evidence_id")
	}
	input.EffectiveAt = input.EffectiveAt.UTC()
	targetPrice, err := s.valuationTarget(ctx, input.AssetID, input.ValuationRunID, input.EffectiveAt)
	if err != nil {
		return Snapshot{}, err
	}
	result := Evaluate(Input{Policy: input.Policy, AsOfPrice: input.AsOfPrice, TargetPrice: targetPrice, ExpectedDividend: input.ExpectedDividend, BenchmarkReturn: input.BenchmarkReturn, ValuationRunID: input.ValuationRunID})
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	previous, err := readStateForUpdate(ctx, tx, input.AssetID, input.Policy.Version, input.Policy.HorizonDays, input.Policy.BenchmarkID)
	if err != nil {
		return Snapshot{}, err
	}
	revision := Revise(previous, result)
	identity, _ := json.Marshal(map[string]any{"asset_id": input.AssetID, "policy_version": input.Policy.Version, "horizon_days": input.Policy.HorizonDays, "benchmark_id": input.Policy.BenchmarkID, "valuation_run_id": input.ValuationRunID, "effective_at": input.EffectiveAt, "as_of_price": input.AsOfPrice, "benchmark_return": input.BenchmarkReturn})
	revisionID := stableID("rating-revision", string(identity))
	inputSnapshot, _ := json.Marshal(map[string]any{"as_of_price": input.AsOfPrice, "as_of_price_evidence_id": input.AsOfPriceEvidenceID, "expected_dividend": input.ExpectedDividend, "benchmark_return": input.BenchmarkReturn, "benchmark_evidence_id": input.BenchmarkEvidenceID, "changed_assumptions": input.ChangedAssumptions})
	resultJSON, _ := json.Marshal(result)
	reasonsJSON, _ := json.Marshal(nonNilStrings(input.ReasonCodes))
	evidenceJSON, _ := json.Marshal(nonNilStrings(append(input.EvidenceIDs, input.AsOfPriceEvidenceID, input.BenchmarkEvidenceID)))
	tag, err := tx.Exec(ctx, `INSERT INTO fundamental_rating_revisions(id,asset_id,policy_version,horizon_days,benchmark_id,previous_rating,current_rating,action,reason,valuation_run_id,effective_at,idempotency_key,input_snapshot,result,reason_codes,evidence_ids)
		VALUES($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT(idempotency_key) DO NOTHING`, revisionID, input.AssetID, input.Policy.Version, input.Policy.HorizonDays, input.Policy.BenchmarkID, revision.PreviousRating, revision.CurrentRating, revision.Action, revision.Reason, input.ValuationRunID, input.EffectiveAt, revisionID, inputSnapshot, resultJSON, reasonsJSON, evidenceJSON)
	if err != nil {
		return Snapshot{}, fmt.Errorf("insert rating revision: %w", err)
	}
	created := tag.RowsAffected() == 1
	var state *State
	if result.Status == "available" {
		state = &State{AssetID: input.AssetID, PolicyVersion: input.Policy.Version, HorizonDays: input.Policy.HorizonDays, BenchmarkID: input.Policy.BenchmarkID, Rating: result.Rating, ValuationRunID: input.ValuationRunID, EffectiveAt: input.EffectiveAt}
		if created {
			_, err = tx.Exec(ctx, `INSERT INTO fundamental_rating_states(asset_id,policy_version,horizon_days,benchmark_id,rating,valuation_run_id,effective_at,revision_id,result)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(asset_id,policy_version,horizon_days,benchmark_id) DO UPDATE SET rating=excluded.rating,valuation_run_id=excluded.valuation_run_id,effective_at=excluded.effective_at,revision_id=excluded.revision_id,result=excluded.result
				WHERE fundamental_rating_states.effective_at<=excluded.effective_at`, input.AssetID, input.Policy.Version, input.Policy.HorizonDays, input.Policy.BenchmarkID, result.Rating, input.ValuationRunID, input.EffectiveAt, revisionID, resultJSON)
			if err != nil {
				return Snapshot{}, fmt.Errorf("upsert rating state: %w", err)
			}
		}
	} else {
		state = previous
	}
	rules, err := persistInvalidationRules(ctx, tx, input, revisionID, created)
	if err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{State: state, Revision: revision, Result: result, ReasonCodes: nonNilStrings(input.ReasonCodes), ChangedAssumptions: nonNilMap(input.ChangedAssumptions), EvidenceIDs: nonNilStrings(input.EvidenceIDs), InvalidationRules: rules, Created: created}, nil
}

func (s *Store) Current(ctx context.Context, assetID string) ([]Snapshot, error) {
	rows, err := s.db.Query(ctx, `SELECT s.asset_id,s.policy_version,s.horizon_days,s.benchmark_id,s.rating,s.valuation_run_id,s.effective_at,s.result::jsonb,
		r.id,r.previous_rating,r.current_rating,r.action,r.reason,r.reason_codes::jsonb,r.evidence_ids::jsonb,r.input_snapshot::jsonb
		FROM fundamental_rating_states s JOIN fundamental_rating_revisions r ON r.id=s.revision_id WHERE s.asset_id=$1 ORDER BY s.horizon_days,s.policy_version`, strings.TrimSpace(assetID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Snapshot{}
	for rows.Next() {
		state, result, revision := State{}, Result{}, Revision{}
		var resultJSON, reasonsJSON, evidenceJSON, inputJSON []byte
		var revisionID string
		var previousRating, currentRating *string
		if err := rows.Scan(&state.AssetID, &state.PolicyVersion, &state.HorizonDays, &state.BenchmarkID, &state.Rating, &state.ValuationRunID, &state.EffectiveAt, &resultJSON, &revisionID, &previousRating, &currentRating, &revision.Action, &revision.Reason, &reasonsJSON, &evidenceJSON, &inputJSON); err != nil {
			return nil, err
		}
		if previousRating != nil {
			revision.PreviousRating = *previousRating
		}
		if currentRating != nil {
			revision.CurrentRating = *currentRating
		}
		revision.ValuationRunID = state.ValuationRunID
		_ = json.Unmarshal(resultJSON, &result)
		reasons, evidence := []string{}, []string{}
		_ = json.Unmarshal(reasonsJSON, &reasons)
		_ = json.Unmarshal(evidenceJSON, &evidence)
		input := struct {
			ChangedAssumptions map[string]any `json:"changed_assumptions"`
		}{}
		_ = json.Unmarshal(inputJSON, &input)
		rules, _ := s.rules(ctx, revisionID)
		items = append(items, Snapshot{State: &state, Revision: revision, Result: result, ReasonCodes: reasons, ChangedAssumptions: nonNilMap(input.ChangedAssumptions), EvidenceIDs: evidence, InvalidationRules: rules, Created: true})
	}
	return items, rows.Err()
}

func (s *Store) EvaluateRules(ctx context.Context, assetID string, observations map[string]any, observedAt time.Time) ([]RuleEvaluation, error) {
	if s.db == nil || strings.TrimSpace(assetID) == "" || observedAt.IsZero() {
		return nil, fmt.Errorf("rating rule store, asset_id and observed_at are required")
	}
	rows, err := s.db.Query(ctx, `SELECT id,rule_type,operator,threshold::jsonb,evidence_ids::jsonb,status,metadata::jsonb FROM rating_invalidation_rules WHERE asset_id=$1 AND status='active' AND active_from<=$2 ORDER BY id`, strings.TrimSpace(assetID), observedAt.UTC())
	if err != nil {
		return nil, err
	}
	rules := []InvalidationRule{}
	for rows.Next() {
		var rule InvalidationRule
		var threshold, evidence, metadata []byte
		if err := rows.Scan(&rule.ID, &rule.RuleType, &rule.Operator, &threshold, &evidence, &rule.Status, &metadata); err != nil {
			rows.Close()
			return nil, err
		}
		_ = json.Unmarshal(threshold, &rule.Threshold)
		_ = json.Unmarshal(evidence, &rule.EvidenceIDs)
		_ = json.Unmarshal(metadata, &rule.Metadata)
		rules = append(rules, rule)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	results := make([]RuleEvaluation, 0, len(rules))
	for _, rule := range rules {
		observed, exists := observations[rule.RuleType]
		result := EvaluateInvalidationRule(rule, observed, exists)
		results = append(results, result)
		status := "active"
		var triggered any
		if result.Status == "triggered" {
			status, triggered = "triggered", observedAt.UTC()
		}
		if _, err := s.db.Exec(ctx, `UPDATE rating_invalidation_rules SET status=$2,evaluated_at=$3,triggered_at=coalesce(triggered_at,$4) WHERE id=$1`, rule.ID, status, observedAt.UTC(), triggered); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (s *Store) valuationTarget(ctx context.Context, assetID, runID string, cutoff time.Time) (*float64, error) {
	var body []byte
	if err := s.db.QueryRow(ctx, `SELECT result::jsonb FROM valuation_runs WHERE id=$1 AND asset_id=$2 AND status='available' AND as_of<=$3`, runID, assetID, cutoff).Scan(&body); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("valuation run is absent, unavailable, belongs to another asset, or is after effective_at")
		}
		return nil, err
	}
	var value struct {
		Range map[string]*float64 `json:"range"`
	}
	if err := json.Unmarshal(body, &value); err != nil || value.Range["low"] == nil || value.Range["high"] == nil {
		return nil, fmt.Errorf("valuation run has no reproducible value range")
	}
	target := (*value.Range["low"] + *value.Range["high"]) / 2
	return &target, nil
}

func readStateForUpdate(ctx context.Context, tx pgx.Tx, assetID, policyVersion string, horizon int, benchmarkID string) (*State, error) {
	state := State{}
	err := tx.QueryRow(ctx, `SELECT asset_id,policy_version,horizon_days,benchmark_id,rating,valuation_run_id,effective_at FROM fundamental_rating_states WHERE asset_id=$1 AND policy_version=$2 AND horizon_days=$3 AND benchmark_id=$4 FOR UPDATE`, assetID, policyVersion, horizon, benchmarkID).Scan(&state.AssetID, &state.PolicyVersion, &state.HorizonDays, &state.BenchmarkID, &state.Rating, &state.ValuationRunID, &state.EffectiveAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &state, err
}

func persistInvalidationRules(ctx context.Context, tx pgx.Tx, input Submission, revisionID string, created bool) ([]InvalidationRule, error) {
	rules := make([]InvalidationRule, 0, len(input.InvalidationRules))
	for index, rule := range input.InvalidationRules {
		rule.RuleType, rule.Operator = strings.TrimSpace(rule.RuleType), strings.TrimSpace(rule.Operator)
		if rule.RuleType == "" || !contains([]string{"lt", "lte", "eq", "gte", "gt", "changed", "missing"}, rule.Operator) || len(rule.EvidenceIDs) == 0 {
			return nil, fmt.Errorf("invalidation rule %d requires rule_type, supported operator and evidence_ids", index)
		}
		rule.ID = stableID("rating-rule", revisionID+fmt.Sprintf("|%d|%s|%s", index, rule.RuleType, rule.Operator))
		rule.Status = "active"
		if created {
			threshold, _ := json.Marshal(rule.Threshold)
			evidence, _ := json.Marshal(nonNilStrings(rule.EvidenceIDs))
			metadata, _ := json.Marshal(nonNilMap(rule.Metadata))
			if _, err := tx.Exec(ctx, `INSERT INTO rating_invalidation_rules(id,asset_id,rating_revision_id,rule_type,operator,threshold,evidence_ids,status,active_from,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,'active',$8,$9) ON CONFLICT(id) DO NOTHING`, rule.ID, input.AssetID, revisionID, rule.RuleType, rule.Operator, threshold, evidence, input.EffectiveAt, metadata); err != nil {
				return nil, err
			}
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func (s *Store) rules(ctx context.Context, revisionID string) ([]InvalidationRule, error) {
	rows, err := s.db.Query(ctx, `SELECT id,rule_type,operator,threshold::jsonb,evidence_ids::jsonb,status,metadata::jsonb FROM rating_invalidation_rules WHERE rating_revision_id=$1 ORDER BY id`, revisionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rules := []InvalidationRule{}
	for rows.Next() {
		var rule InvalidationRule
		var threshold, evidence, metadata []byte
		if err := rows.Scan(&rule.ID, &rule.RuleType, &rule.Operator, &threshold, &evidence, &rule.Status, &metadata); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(threshold, &rule.Threshold)
		_ = json.Unmarshal(evidence, &rule.EvidenceIDs)
		_ = json.Unmarshal(metadata, &rule.Metadata)
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + "-" + hex.EncodeToString(sum[:])[:40]
}
func nonNilStrings(values []string) []string {
	result := []string{}
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}
func nonNilMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
