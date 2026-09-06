package jobs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const p0PolicyAlgorithmVersion = "p0-evidence-v1"

func signalAvailableAt(event map[string]any, evidence []researchEvidence, generated time.Time) time.Time {
	available := generated.UTC()
	for _, item := range evidence {
		for _, stamp := range []time.Time{item.PublishedAt, item.ObservedAt, item.AsOf} {
			if stamp.After(available) {
				available = stamp
			}
		}
	}
	if eventAsOf := parseTime(event["as_of"]); eventAsOf.After(available) {
		available = eventAsOf
	}
	return available
}

func eventSignalContract(score int, rating, conclusion string, horizon int, asOf, available time.Time) map[string]any {
	status := conclusion
	if status == "neutral_supported" {
		status = "neutral"
	}
	if status == "" {
		status = "insufficient_evidence"
	}
	return map[string]any{
		"status": status, "direction_score": score, "rating": rating,
		"horizon_days": horizon, "horizon_unit": "calendar_days", "as_of": iso(asOf),
		"signal_available_at": iso(available), "algorithm_version": p0PolicyAlgorithmVersion,
	}
}

func p0ResultContract(score int, rating, conclusion string, horizon int, asOf, available time.Time, newsScore float64, verification draftVerification) map[string]any {
	return map[string]any{
		"event_signal": eventSignalContract(score, rating, conclusion, horizon, asOf, available),
		"evidence_quality": map[string]any{
			"score": newsScore, "status": ternaryString(verification.EvidenceComplete, "complete", "incomplete"),
			"rule_version": p0PolicyAlgorithmVersion, "structurally_valid": verification.StructurallyValid,
			"missing_information": nonNilStrings(verification.Missing), "conditional_information": nonNilStrings(verification.Conditional), "contradictions": nonNilStrings(verification.Contradictions),
		},
		"fundamental_rating":    map[string]any{"status": "unavailable", "rating": nil, "reason": "not_implemented_p0"},
		"short_term_prediction": map[string]any{"status": "uncalibrated", "probabilities": nil, "calibration": nil, "reason": "not_available_until_calibration"},
	}
}

func impactEligibility(asset map[string]any, item eventImpactDraft, complete bool) map[string]any {
	valid := complete && item.ConclusionStatus == "directional" && item.DirectionScore != 0
	execution := asset != nil && (stringValue(asset["asset_class"]) == "equity" || stringValue(asset["asset_class"]) == "crypto")
	return map[string]any{
		"research_eligible": asset != nil && item.ConclusionStatus != "insufficient_evidence",
		"signal_valid":      valid, "execution_supported": execution,
		"long_eligible":  valid && item.DirectionScore > 0 && execution,
		"short_eligible": valid && item.DirectionScore < 0 && execution,
	}
}

func (runtime *researchRuntime) recordPolicyEvaluation(ctx context.Context, eventID, assetID string, run, legacy, policy map[string]any) {
	mode := runtime.effectivePolicyMode(ctx)
	input, _ := json.Marshal(policyInputSnapshot(eventID, assetID, run, runtime.cfg.ResearchPolicyVersion, runtime.cfg.ResearchModel))
	legacyJSON, _ := json.Marshal(legacy)
	policyJSON, _ := json.Marshal(policy)
	signal := objectValue(policy["event_signal"])
	comparison, _ := json.Marshal(map[string]any{
		"mode": mode, "policy_version": runtime.cfg.ResearchPolicyVersion,
		"legacy_direction_score": legacy["direction_score"], "legacy_rating": legacy["rating"], "legacy_signal_status": legacy["signal_status"],
		"policy_direction_score": signal["direction_score"], "policy_rating": signal["rating"], "policy_status": signal["status"],
		"direction_changed": numberValue(legacy["direction_score"]) != numberValue(signal["direction_score"]),
	})
	_, _ = runtime.db.Exec(context.WithoutCancel(ctx), `INSERT INTO policy_evaluations(id,event_id,asset_id,policy_version,policy_mode,input_snapshot,legacy_result,policy_result,comparison,code_version,prompt_version,model) VALUES($1,NULLIF($2,''),NULLIF($3,''),$4,$5,$6,$7,$8,$9,$10,$11,$12)`, uuid.NewString(), eventID, assetID, runtime.cfg.ResearchPolicyVersion, mode, input, legacyJSON, policyJSON, comparison, p0PolicyAlgorithmVersion, eventResearchPromptVersion, runtime.cfg.ResearchModel)
}

func policyInputSnapshot(eventID, assetID string, run map[string]any, policyVersion, model string) map[string]any {
	// Persist only structured evidence, routing and time data. Prompts and model
	// reasoning remain in their dedicated audit table and are never duplicated.
	return map[string]any{
		"event_id": eventID, "asset_id": assetID, "policy_version": policyVersion, "model": model,
		"as_of": run["as_of"], "research_profile": run["research_profile"], "route_reason": run["route_reason"],
		"evidence": anySlice(run["evidence"]), "source_event_ids": anySlice(run["trigger_event_ids"]),
	}
}

// Enforce is intentionally opt-in twice: configuration asks for it, then an
// immutable human approval proves the minimum 14-day/100-review shadow gate.
// Otherwise the recorded policy remains shadow even after a mistaken env edit.
func (runtime *researchRuntime) effectivePolicyMode(ctx context.Context) string {
	if runtime.cfg.ResearchPolicyMode != "enforce" || runtime.db == nil {
		return "shadow"
	}
	var approved bool
	var reviewed int
	var started time.Time
	err := runtime.db.QueryRow(ctx, `SELECT true,shadow_started_at FROM policy_release_approvals WHERE policy_version=$1`, runtime.cfg.ResearchPolicyVersion).Scan(&approved, &started)
	if err != nil || !approved {
		return "shadow"
	}
	err = runtime.db.QueryRow(ctx, `SELECT count(*) FROM policy_impact_reviews r JOIN policy_evaluations e ON e.id=r.policy_evaluation_id WHERE e.policy_version=$1 AND r.decision='accepted' AND e.policy_result->'event_signal'->>'status'='directional'`, runtime.cfg.ResearchPolicyVersion).Scan(&reviewed)
	if err != nil || reviewed < runtime.cfg.PolicyShadowMinReviewed || started.After(time.Now().UTC().AddDate(0, 0, -runtime.cfg.PolicyShadowMinDays)) {
		return "shadow"
	}
	return "enforce"
}
