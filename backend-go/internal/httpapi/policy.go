package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// researchPolicySummary makes the shadow gate observable without allowing a
// health probe to change policy.  An enforce configuration is only effective
// after a separately recorded human approval has met both hard thresholds.
func (s *Server) researchPolicySummary(ctx context.Context) map[string]any {
	result := map[string]any{
		"configured_mode":    s.cfg.ResearchPolicyMode,
		"active_mode":        "shadow",
		"version":            s.cfg.ResearchPolicyVersion,
		"prediction_mode":    s.cfg.ResearchPredictionMode,
		"minimum_days":       s.cfg.PolicyShadowMinDays,
		"minimum_reviews":    s.cfg.PolicyShadowMinReviewed,
		"valid_impacts":      0,
		"reviewed_impacts":   0,
		"ready_for_approval": false,
	}
	if s.db == nil {
		return result
	}
	var started *time.Time
	var valid, reviewed int
	_ = s.db.QueryRow(ctx, `
		SELECT min(created_at),
		       count(*) FILTER (WHERE policy_result->'event_signal'->>'status'='directional')
		FROM policy_evaluations WHERE policy_version=$1`, s.cfg.ResearchPolicyVersion).Scan(&started, &valid)
	_ = s.db.QueryRow(ctx, `
		SELECT count(*) FROM policy_impact_reviews r
		JOIN policy_evaluations e ON e.id=r.policy_evaluation_id
		WHERE e.policy_version=$1 AND r.decision='accepted'
		  AND e.policy_result->'event_signal'->>'status'='directional'`, s.cfg.ResearchPolicyVersion).Scan(&reviewed)
	result["valid_impacts"], result["reviewed_impacts"] = valid, reviewed
	if started != nil {
		stamp := started.UTC()
		result["shadow_started_at"] = stamp
		result["shadow_age_days"] = int(time.Since(stamp).Hours() / 24)
		ready := time.Since(stamp) >= time.Duration(s.cfg.PolicyShadowMinDays)*24*time.Hour && reviewed >= s.cfg.PolicyShadowMinReviewed
		result["ready_for_approval"] = ready
	}
	var approved bool
	_ = s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM policy_release_approvals WHERE policy_version=$1)`, s.cfg.ResearchPolicyVersion).Scan(&approved)
	result["approved"] = approved
	if s.cfg.ResearchPolicyMode == "enforce" && approved && reviewed >= s.cfg.PolicyShadowMinReviewed && started != nil && !started.After(time.Now().UTC().AddDate(0, 0, -s.cfg.PolicyShadowMinDays)) {
		result["active_mode"] = "enforce"
	}
	return result
}

func (s *Server) researchPolicyStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.researchPolicySummary(r.Context()))
}

func (s *Server) researchPolicyEvaluations(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 100)
	if !ok {
		return
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT e.id,e.event_id,e.asset_id,coalesce(n.headline,''),coalesce(a.name,''),coalesce(a.symbol,''),e.input_snapshot,e.policy_result,e.comparison,e.created_at,r.decision,r.reviewer,r.created_at
		FROM policy_evaluations e
		LEFT JOIN news_events n ON n.id::text=e.event_id
		LEFT JOIN assets a ON a.id=e.asset_id
		LEFT JOIN policy_impact_reviews r ON r.policy_evaluation_id=e.id
		WHERE e.policy_version=$1 AND e.policy_result->'event_signal'->>'status'='directional'
		ORDER BY e.created_at DESC,e.id DESC LIMIT $2`, s.cfg.ResearchPolicyVersion, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "policy evaluations query failed")
		return
	}
	defer rows.Close()
	items := make([]any, 0, limit)
	for rows.Next() {
		var id string
		var eventID, assetID *string
		var headline, assetName, symbol string
		var inputJSON, policyJSON, comparisonJSON []byte
		var created time.Time
		var decision, reviewer *string
		var reviewedAt *time.Time
		if err := rows.Scan(&id, &eventID, &assetID, &headline, &assetName, &symbol, &inputJSON, &policyJSON, &comparisonJSON, &created, &decision, &reviewer, &reviewedAt); err != nil {
			writeError(w, http.StatusInternalServerError, "policy evaluations decode failed")
			return
		}
		input, policy, comparison := map[string]any{}, map[string]any{}, map[string]any{}
		_ = json.Unmarshal(inputJSON, &input)
		_ = json.Unmarshal(policyJSON, &policy)
		_ = json.Unmarshal(comparisonJSON, &comparison)
		evidence := anySlice(input["evidence"])
		if len(evidence) > 5 {
			evidence = evidence[:5]
		}
		item := map[string]any{"id": id, "event_id": nullablePolicyString(eventID), "asset_id": nullablePolicyString(assetID), "headline": nullableString(headline), "asset_name": nullableString(assetName), "symbol": nullableString(symbol), "event_signal": policy["event_signal"], "evidence_quality": policy["evidence_quality"], "comparison": comparison, "evidence": evidence, "created_at": created.UTC(), "decision": nullablePolicyString(decision), "reviewer": nullablePolicyString(reviewer), "reviewed_at": timeOrNil(reviewedAt)}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "policy evaluations query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "policy": s.researchPolicySummary(r.Context())})
}

func nullablePolicyString(value *string) any {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	return *value
}

type policyReviewRequest struct {
	PolicyEvaluationID string `json:"policy_evaluation_id"`
	Reviewer           string `json:"reviewer"`
	Decision           string `json:"decision"`
	Note               string `json:"note"`
}

func (s *Server) reviewResearchPolicyImpact(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	var body policyReviewRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}
	body.Reviewer, body.Decision = strings.TrimSpace(body.Reviewer), strings.ToLower(strings.TrimSpace(body.Decision))
	if _, err := uuid.Parse(body.PolicyEvaluationID); err != nil || body.Reviewer == "" || (body.Decision != "accepted" && body.Decision != "rejected") {
		writeError(w, http.StatusUnprocessableEntity, "policy_evaluation_id, reviewer and accepted/rejected decision are required")
		return
	}
	var valid bool
	err := s.db.QueryRow(r.Context(), `SELECT policy_result->'event_signal'->>'status'='directional' FROM policy_evaluations WHERE id=$1 AND policy_version=$2`, body.PolicyEvaluationID, s.cfg.ResearchPolicyVersion).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "policy evaluation not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "policy evaluation lookup failed")
		return
	}
	if !valid {
		writeError(w, http.StatusUnprocessableEntity, "only valid directional impacts can be manually reviewed")
		return
	}
	_, err = s.db.Exec(r.Context(), `INSERT INTO policy_impact_reviews(policy_evaluation_id,reviewer,decision,note) VALUES($1,$2,$3,$4)`, body.PolicyEvaluationID, body.Reviewer, body.Decision, strings.TrimSpace(body.Note))
	if err != nil {
		writeError(w, http.StatusConflict, "policy evaluation already reviewed")
		return
	}
	writeJSON(w, http.StatusCreated, s.researchPolicySummary(r.Context()))
}

type policyApprovalRequest struct {
	ApprovedBy string `json:"approved_by"`
	Note       string `json:"note"`
}

func (s *Server) approveResearchPolicy(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	var body policyApprovalRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}
	body.ApprovedBy = strings.TrimSpace(body.ApprovedBy)
	if body.ApprovedBy == "" {
		writeError(w, http.StatusUnprocessableEntity, "approved_by is required")
		return
	}
	summary := s.researchPolicySummary(r.Context())
	if !boolValue(summary["ready_for_approval"]) {
		writeError(w, http.StatusConflict, "policy shadow gate has not met the required duration and reviewed-impact count")
		return
	}
	started, ok := summary["shadow_started_at"].(time.Time)
	if !ok || started.IsZero() {
		writeError(w, http.StatusConflict, "policy shadow start is unavailable")
		return
	}
	_, err := s.db.Exec(r.Context(), `INSERT INTO policy_release_approvals(policy_version,approved_by,approved_at,reviewed_valid_impacts,shadow_started_at,note) VALUES($1,$2,now(),$3,$4,$5)`, s.cfg.ResearchPolicyVersion, body.ApprovedBy, int(numberValue(summary["reviewed_impacts"])), started, strings.TrimSpace(body.Note))
	if err != nil {
		writeError(w, http.StatusConflict, "policy version already has an immutable approval record")
		return
	}
	writeJSON(w, http.StatusCreated, s.researchPolicySummary(r.Context()))
}
