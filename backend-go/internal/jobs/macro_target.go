package jobs

import (
	"strings"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/targetpolicy"
)

// macroTargetIdentity puts a non-tradable impact target onto the shared macro
// vocabulary. It reports ok=false when the label is a person, an institution, an
// instrument or a news clause instead of a macroeconomic or industry object, and
// corrects a loose model type ("other", "sector") when the shared vocabulary
// recognises a more specific flow object such as an export quota or a policy rate.
func macroTargetIdentity(name, targetType string) (string, string, bool) {
	kind, ok := targetpolicy.Classify(strings.TrimSpace(name))
	if !ok {
		return "", "", false
	}
	switch targetType {
	case "", "other", "tradable_asset":
		return name, kind, true
	}
	// A flow object outranks an industry or market word: "柴油出口" stays a supply
	// observation instead of being published as an oil industry target.
	if targetpolicy.FlowKind(kind) && kind != targetType {
		return name, kind, true
	}
	return name, targetType, true
}

// pruneMacroObservationImpacts replays the writer-side target policy over stored
// research impacts. Tradable and asset-backed targets are preserved untouched; a
// bare label that names a person, an institution, an instrument or a news clause
// is removed, and a surviving label takes the target type the shared vocabulary
// derives for it. It returns the kept impacts and the labels it dropped.
func pruneMacroObservationImpacts(impacts []any) ([]any, []string) {
	kept := make([]any, 0, len(impacts))
	dropped := make([]string, 0)
	for _, raw := range impacts {
		impact := objectValue(raw)
		if impact == nil {
			continue
		}
		if stringValue(impact["target_type"]) == "tradable_asset" || strings.TrimSpace(stringValue(impact["asset_id"])) != "" {
			kept = append(kept, impact)
			continue
		}
		name, targetType, ok := macroTargetIdentity(stringValue(impact["target_name"]), stringValue(impact["target_type"]))
		if !ok {
			dropped = append(dropped, strings.TrimSpace(stringValue(impact["target_name"])))
			continue
		}
		impact["target_name"], impact["target_type"] = name, targetType
		kept = append(kept, impact)
	}
	return kept, dropped
}

// macroActionTarget describes an observation generated from a structured event
// action.
type macroActionTarget struct {
	Name, TargetType, ImpactChannel, Relationship string
}

// economicPolicyActionTarget resolves the object a structured action acts upon
// through the shared vocabulary. The actor is only consulted as a last resort and
// must itself name a macro object, otherwise an institution or a person becomes a
// public target.
func economicPolicyActionTarget(action map[string]any) (macroActionTarget, bool) {
	for _, candidate := range []string{
		strings.TrimSpace(stringValue(action["object"])),
		strings.TrimSpace(stringValue(action["scope"])),
		strings.TrimSpace(stringValue(action["actor"])),
	} {
		if candidate == "" || !targetpolicy.PlausibleName(candidate) {
			continue
		}
		kind, ok := targetpolicy.Classify(candidate)
		if !ok {
			continue
		}
		channel, relationship := targetpolicy.ImpactChannelAndRelationship(kind)
		return macroActionTarget{Name: candidate, TargetType: kind, ImpactChannel: channel, Relationship: relationship}, true
	}
	return macroActionTarget{}, false
}
