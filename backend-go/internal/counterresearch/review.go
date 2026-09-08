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
	IndependentOriginCount int           `json:"independent_origin_count"`
	ErrorsFound            int           `json:"errors_found"`
	Latency                time.Duration `json:"latency"`
	Cost                   float64       `json:"cost"`
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
