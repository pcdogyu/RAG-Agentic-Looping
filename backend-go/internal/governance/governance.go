package governance

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type PromotionInput struct {
	HardCorrectnessPassed        bool      `json:"hard_correctness_passed"`
	IndependentSamples           int       `json:"independent_samples"`
	MinimumSamples               int       `json:"minimum_samples"`
	ShadowStartedAt              time.Time `json:"shadow_started_at"`
	MinimumShadowDays            int       `json:"minimum_shadow_days"`
	ECE                          *float64  `json:"ece"`
	MaximumECE                   float64   `json:"maximum_ece"`
	ApprovedBy                   string    `json:"approved_by"`
	FinalHoldoutUsedForSelection bool      `json:"final_holdout_used_for_selection"`
}

type Decision struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons"`
}

func PromotionDecision(input PromotionInput, now time.Time) Decision {
	reasons := []string{}
	if now.IsZero() || input.MinimumShadowDays < 1 || input.MaximumECE <= 0 || math.IsNaN(input.MaximumECE) || math.IsInf(input.MaximumECE, 0) {
		reasons = append(reasons, "invalid_governance_thresholds")
	}
	if !input.HardCorrectnessPassed {
		reasons = append(reasons, "hard_correctness_gate_failed")
	}
	if input.MinimumSamples < 1 || input.IndependentSamples < input.MinimumSamples {
		reasons = append(reasons, "insufficient_independent_samples")
	}
	readyAt := input.ShadowStartedAt.Add(time.Duration(input.MinimumShadowDays) * 24 * time.Hour)
	if input.ShadowStartedAt.IsZero() || now.Before(readyAt) {
		reasons = append(reasons, "shadow_period_incomplete")
	}
	if input.ECE == nil {
		reasons = append(reasons, "calibration_metric_unavailable")
	} else if math.IsNaN(*input.ECE) || math.IsInf(*input.ECE, 0) || *input.ECE < 0 || *input.ECE > input.MaximumECE {
		reasons = append(reasons, "calibration_gate_failed")
	}
	if input.FinalHoldoutUsedForSelection {
		reasons = append(reasons, "final_holdout_contaminated")
	}
	if strings.TrimSpace(input.ApprovedBy) == "" {
		reasons = append(reasons, "human_approval_required")
	}
	status := "blocked"
	if len(reasons) == 0 {
		status = "approved"
	}
	return Decision{Status: status, Reasons: reasons}
}

type DriftInput struct {
	Reference        []float64 `json:"reference"`
	Current          []float64 `json:"current"`
	Bins             int       `json:"bins"`
	WarningThreshold float64   `json:"warning_threshold"`
}
type DriftResult struct {
	Status string   `json:"status"`
	PSI    *float64 `json:"psi"`
	Action string   `json:"action"`
}

func PopulationStability(input DriftInput) (DriftResult, error) {
	if len(input.Reference) < 20 || len(input.Current) < 20 {
		return DriftResult{}, fmt.Errorf("at least 20 reference and current observations are required")
	}
	bins := input.Bins
	if bins < 2 {
		bins = 10
	}
	minimum, maximum := input.Reference[0], input.Reference[0]
	for _, value := range append(append([]float64{}, input.Reference...), input.Current...) {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return DriftResult{}, fmt.Errorf("drift values must be finite")
		}
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	if maximum == minimum {
		zero := 0.0
		return DriftResult{Status: "stable", PSI: &zero, Action: "observe"}, nil
	}
	reference, current := make([]float64, bins), make([]float64, bins)
	bucket := func(value float64) int {
		index := int((value - minimum) / (maximum - minimum) * float64(bins))
		if index >= bins {
			return bins - 1
		}
		return index
	}
	for _, value := range input.Reference {
		reference[bucket(value)]++
	}
	for _, value := range input.Current {
		current[bucket(value)]++
	}
	psi := 0.0
	for index := 0; index < bins; index++ {
		left := (reference[index] + .5) / (float64(len(input.Reference)) + .5*float64(bins))
		right := (current[index] + .5) / (float64(len(input.Current)) + .5*float64(bins))
		psi += (right - left) * math.Log(right/left)
	}
	threshold := input.WarningThreshold
	if threshold <= 0 {
		threshold = .2
	}
	status, action := "stable", "observe"
	if psi >= threshold {
		status, action = "drifted", "alert_and_review"
	}
	return DriftResult{Status: status, PSI: &psi, Action: action}, nil
}
