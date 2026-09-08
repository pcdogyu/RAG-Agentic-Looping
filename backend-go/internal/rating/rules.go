package rating

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

type RuleEvaluation struct {
	RuleID   string `json:"rule_id"`
	Status   string `json:"status"`
	Observed any    `json:"observed,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// EvaluateInvalidationRule is deterministic and intentionally small. The rule
// names identify monitored facts; operators define how an observed value
// invalidates a thesis. Missing observations never become numeric zero.
func EvaluateInvalidationRule(rule InvalidationRule, observedValue any, exists bool) RuleEvaluation {
	result := RuleEvaluation{RuleID: rule.ID, Status: "not_triggered", Observed: nil}
	if rule.Operator == "missing" {
		if !exists || valueMissing(observedValue) {
			result.Status, result.Reason = "triggered", "required observation is missing"
		}
		return result
	}
	if !exists || valueMissing(observedValue) {
		result.Status, result.Reason = "unavailable", "observation is missing"
		return result
	}
	result.Observed = observedValue
	switch rule.Operator {
	case "changed":
		if !reflect.DeepEqual(normalizeJSONValue(observedValue), normalizeJSONValue(rule.Threshold)) {
			result.Status, result.Reason = "triggered", "observed value changed from thesis threshold"
		}
	case "eq":
		if reflect.DeepEqual(normalizeJSONValue(observedValue), normalizeJSONValue(rule.Threshold)) {
			result.Status, result.Reason = "triggered", "observed value equals invalidation threshold"
		}
	case "lt", "lte", "gt", "gte":
		left, leftOK := numeric(observedValue)
		right, rightOK := numeric(rule.Threshold)
		if !leftOK || !rightOK {
			result.Status, result.Reason = "unavailable", "numeric operator requires numeric observation and threshold"
			return result
		}
		triggered := rule.Operator == "lt" && left < right || rule.Operator == "lte" && left <= right || rule.Operator == "gt" && left > right || rule.Operator == "gte" && left >= right
		if triggered {
			result.Status, result.Reason = "triggered", fmt.Sprintf("observed value satisfies %s threshold", rule.Operator)
		}
	default:
		result.Status, result.Reason = "unavailable", "unsupported operator"
	}
	return result
}

func numeric(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		result, err := typed.Float64()
		return result, err == nil
	default:
		return 0, false
	}
}

func valueMissing(value any) bool {
	if value == nil {
		return true
	}
	if text, ok := value.(string); ok {
		return text == ""
	}
	return false
}

func normalizeJSONValue(value any) any {
	body, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var result any
	if json.Unmarshal(body, &result) != nil {
		return value
	}
	return result
}
