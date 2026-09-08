package rating

import "testing"

func TestInvalidationRulesKeepMissingSeparateFromZero(t *testing.T) {
	rule := InvalidationRule{ID: "rule-1", Operator: "lt", Threshold: 10.0}
	if result := EvaluateInvalidationRule(rule, nil, false); result.Status != "unavailable" {
		t.Fatalf("missing observation=%#v", result)
	}
	if result := EvaluateInvalidationRule(rule, 0.0, true); result.Status != "triggered" {
		t.Fatalf("numeric zero=%#v", result)
	}
}

func TestInvalidationRuleChangedAndMissing(t *testing.T) {
	changed := EvaluateInvalidationRule(InvalidationRule{ID: "rule-1", Operator: "changed", Threshold: "active"}, "withdrawn", true)
	if changed.Status != "triggered" {
		t.Fatalf("changed=%#v", changed)
	}
	missing := EvaluateInvalidationRule(InvalidationRule{ID: "rule-2", Operator: "missing"}, nil, false)
	if missing.Status != "triggered" {
		t.Fatalf("missing=%#v", missing)
	}
}
