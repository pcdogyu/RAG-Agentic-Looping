// Package counterresearch measures whether a contrary review adds independent
// evidence and catches errors; agreement or agent count is never confidence.
package counterresearch

import (
	"sort"
	"strings"
	"time"
)

type Evidence struct {
	ID          string    `json:"id"`
	OriginID    string    `json:"origin_id"`
	AvailableAt time.Time `json:"available_at"`
}

type Review struct {
	Status                 string        `json:"status"`
	CompetingMechanisms    []string      `json:"competing_mechanisms"`
	NewEvidenceIDs         []string      `json:"new_evidence_ids"`
	ChallengedClaims       []string      `json:"challenged_claims"`
	IndependentOriginCount int           `json:"independent_origin_count"`
	CandidateErrorsFound   int           `json:"candidate_errors_found"`
	ErrorsFound            int           `json:"errors_found"`
	Latency                time.Duration `json:"latency"`
	Cost                   float64       `json:"cost"`
}

// Finding is an untrusted contrary-model proposal. AssessFindings only accepts
// a finding when it names a baseline claim and cites new, point-in-time,
// independently originated evidence. A candidate challenge is not counted as
// an actual error until a later labelled evaluation confirms it.
type Finding struct {
	ChallengedClaim    string   `json:"challenged_claim"`
	CompetingMechanism string   `json:"competing_mechanism"`
	EvidenceIDs        []string `json:"evidence_ids"`
}

type AblationObservation struct {
	BaselineWrong bool          `json:"baseline_wrong"`
	ErrorFound    bool          `json:"error_found"`
	Latency       time.Duration `json:"latency"`
	Cost          float64       `json:"cost"`
}

type Ablation struct {
	Reviewed           int      `json:"reviewed"`
	BaselineErrors     int      `json:"baseline_errors"`
	ErrorsFound        int      `json:"errors_found"`
	ErrorDiscoveryRate *float64 `json:"error_discovery_rate,omitempty"`
	FalseChallenges    int      `json:"false_challenges"`
	MeanAddedLatencyMS float64  `json:"mean_added_latency_ms"`
	MeanAddedCost      float64  `json:"mean_added_cost"`
}

func Assess(asOf time.Time, baselineEvidence []string, contrary []Evidence, mechanisms []string, errorsFound int, latency time.Duration, cost float64) Review {
	baseline := stringSet(baselineEvidence)
	origins := map[string]bool{}
	ids := make([]string, 0, len(contrary))
	for _, item := range contrary {
		if item.ID == "" || item.OriginID == "" || item.AvailableAt.IsZero() || item.AvailableAt.After(asOf) || baseline[item.ID] {
			continue
		}
		ids = append(ids, item.ID)
		origins[item.OriginID] = true
	}

	sort.Strings(ids)
	status := "no_incremental_evidence"
	if len(ids) > 0 && len(origins) > 0 {
		status = "incremental_evidence"
	}
	if errorsFound > 0 && status == "incremental_evidence" {
		status = "error_found"
	}

	return Review{
		Status:                 status,
		CompetingMechanisms:    cleanStrings(mechanisms),
		NewEvidenceIDs:         ids,
		IndependentOriginCount: len(origins),
		ErrorsFound:            max(0, errorsFound),
		Latency:                latency,
		Cost:                   max(0, cost),
	}
}

// AssessFindings applies the evidence and time gates to untrusted findings.
// baselineClaims must contain the exact claims that the first-pass report made.
func AssessFindings(asOf time.Time, baselineClaims, baselineEvidence []string, contrary []Evidence, findings []Finding, latency time.Duration, cost float64) Review {
	claims := stringSet(baselineClaims)
	baseline := stringSet(baselineEvidence)
	available := map[string]Evidence{}
	for _, item := range contrary {
		if item.ID == "" || item.OriginID == "" || item.AvailableAt.IsZero() || item.AvailableAt.After(asOf) || baseline[item.ID] {
			continue
		}
		available[item.ID] = item
	}
	acceptedClaims, evidenceIDs, mechanisms, origins := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, finding := range findings {
		claim := strings.TrimSpace(finding.ChallengedClaim)
		mechanism := strings.TrimSpace(finding.CompetingMechanism)
		if !claims[claim] || mechanism == "" {
			continue
		}
		findingOrigins := map[string]bool{}
		findingEvidence := []Evidence{}
		for _, id := range finding.EvidenceIDs {
			if item, ok := available[strings.TrimSpace(id)]; ok {
				findingEvidence = append(findingEvidence, item)
				findingOrigins[item.OriginID] = true
			}
		}
		if len(findingEvidence) == 0 || len(findingOrigins) == 0 {
			continue
		}
		acceptedClaims[claim], mechanisms[mechanism] = true, true
		for _, item := range findingEvidence {
			evidenceIDs[item.ID], origins[item.OriginID] = true, true
		}
	}
	review := Review{
		Status: "no_incremental_evidence", CompetingMechanisms: sortedKeys(mechanisms), NewEvidenceIDs: sortedKeys(evidenceIDs),
		ChallengedClaims: sortedKeys(acceptedClaims), IndependentOriginCount: len(origins), CandidateErrorsFound: len(acceptedClaims),
		Latency: latency, Cost: max(0, cost),
	}
	if len(review.NewEvidenceIDs) > 0 {
		review.Status = "incremental_evidence"
	}
	if review.CandidateErrorsFound > 0 {
		review.Status = "candidate_error_found"
	}
	return review
}

// SummarizeAblation reports only labelled error discovery. It deliberately
// keeps unreviewed candidate challenges out of the numerator and denominator.
func SummarizeAblation(values []AblationObservation) Ablation {
	result := Ablation{Reviewed: len(values)}
	for _, item := range values {
		if item.BaselineWrong {
			result.BaselineErrors++
		}
		if item.ErrorFound && item.BaselineWrong {
			result.ErrorsFound++
		} else if item.ErrorFound {
			result.FalseChallenges++
		}
		result.MeanAddedLatencyMS += float64(max(time.Duration(0), item.Latency).Milliseconds())
		result.MeanAddedCost += max(0, item.Cost)
	}
	if result.Reviewed > 0 {
		result.MeanAddedLatencyMS /= float64(result.Reviewed)
		result.MeanAddedCost /= float64(result.Reviewed)
	}
	if result.BaselineErrors > 0 {
		value := float64(result.ErrorsFound) / float64(result.BaselineErrors)
		result.ErrorDiscoveryRate = &value
	}
	return result
}

func sortedKeys(values map[string]bool) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func stringSet(values []string) map[string]bool {
	result := map[string]bool{}
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result[value] = true
		}
	}
	return result
}

func cleanStrings(values []string) []string {
	result := []string{}
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}
