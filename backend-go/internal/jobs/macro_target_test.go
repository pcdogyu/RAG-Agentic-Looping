package jobs

import (
	"slices"
	"strings"
	"testing"
)

func TestMacroTargetIdentityOnlyAcceptsMacroObjects(t *testing.T) {
	accepted := map[string][2]string{
		"美国CPI|economy":  {"美国CPI", "economy"},
		"半导体|other":      {"半导体", "sector"},
		"柴油出口|sector":    {"柴油出口", "supply_volume"},
		"联邦基金利率|economy": {"联邦基金利率", "interest_rate"},
		"集装箱运价|sector":   {"集装箱运价", "shipping"},
		"日元|other":       {"日元", "fx_rate"},
	}
	for input, want := range accepted {
		parts := strings.SplitN(input, "|", 2)
		name, targetType := parts[0], parts[1]
		gotName, gotType, ok := macroTargetIdentity(name, targetType)
		if !ok {
			t.Errorf("macroTargetIdentity(%q, %q) rejected a valid macro object", name, targetType)
			continue
		}
		if gotName != want[0] || gotType != want[1] {
			t.Errorf("macroTargetIdentity(%q, %q) = %q/%q, want %q/%q", name, targetType, gotName, gotType, want[0], want[1])
		}
	}
	rejected := [][2]string{
		{"美国利率决议", "economy"},
		{"美联储主席鲍威尔", "interest_rate"},
		{"欧央行", "interest_rate"},
		{"特朗普关税", "economy"},
		{"LME铜现货结算价", "commodity_price"},
		{"鲍威尔表示通胀风险仍然存在", "economy"},
		{"某公司宣布重组计划", "sector"},
		{"日美关税协议", "economy"},
	}
	for _, input := range rejected {
		if name, kind, ok := macroTargetIdentity(input[0], input[1]); ok {
			t.Errorf("macroTargetIdentity(%q, %q) = %q/%q, want rejection", input[0], input[1], name, kind)
		}
	}
}

func TestEconomicActionObservationTargetUsesSharedVocabulary(t *testing.T) {
	name, targetType, channel, relationship, ok := economicActionObservationTarget(map[string]any{
		"actor": "美联储", "action_type": "tighten", "object": "联邦基金利率", "scope": "美国",
	})
	if !ok || name != "联邦基金利率" || targetType != "interest_rate" {
		t.Fatalf("economicActionObservationTarget resolved %q/%q/%v, want 联邦基金利率/interest_rate", name, targetType, ok)
	}
	if relationship != "macro_indicator" || channel != "risk_premium" {
		t.Errorf("observation relation/channel = %q/%q, want macro_indicator/risk_premium", relationship, channel)
	}
	// An actor that names an institution must not become a target on its own.
	if _, _, _, _, ok := economicActionObservationTarget(map[string]any{"actor": "欧洲央行", "action_type": "meet"}); ok {
		t.Error("economicActionObservationTarget resolved the institution 欧洲央行 as a target")
	}
	if _, _, _, _, ok := economicActionObservationTarget(map[string]any{"actor": "白宫", "object": "相关举措"}); ok {
		t.Error("economicActionObservationTarget accepted a vague placeholder")
	}
}

func TestSanitizeEventImpactsDropsNonMacroTargets(t *testing.T) {
	values := []eventImpactDraft{
		{TargetType: "economy", TargetName: "美国CPI", DirectionScore: 40},
		{TargetType: "economy", TargetName: "美国利率决议", DirectionScore: -20},
		{TargetType: "interest_rate", TargetName: "美联储主席鲍威尔", DirectionScore: 10},
		{TargetType: "sector", TargetName: "柴油出口", DirectionScore: -30},
		{TargetType: "other", TargetName: "半导体", DirectionScore: 20},
		{TargetType: "sector", TargetName: "某公司宣布重组计划", DirectionScore: 5},
	}
	got := sanitizeEventImpacts(values, map[string]any{})
	if len(got) != 3 {
		t.Fatalf("sanitizeEventImpacts kept %d impacts (%v), want 3", len(got), got)
	}
	want := [][2]string{{"美国CPI", "economy"}, {"柴油出口", "supply_volume"}, {"半导体", "sector"}}
	for index, expected := range want {
		if got[index].TargetName != expected[0] || got[index].TargetType != expected[1] {
			t.Errorf("impact %d = %q/%q, want %q/%q", index, got[index].TargetName, got[index].TargetType, expected[0], expected[1])
		}
	}
}

func TestPruneMacroObservationImpactsKeepsMacroAndIndustryDropsInstitutions(t *testing.T) {
	impacts := []any{
		map[string]any{"target_name": "LME", "target_type": "industry", "direction": "up", "strength": 3},
		map[string]any{"target_name": "白银", "target_type": "macro_economy", "direction": "up", "strength": 3},
		map[string]any{"target_name": "人工智能", "target_type": "industry", "direction": "up", "strength": 3},
		map[string]any{"target_name": "美联储主席鲍威尔", "target_type": "macro_economy", "direction": "up", "strength": 2},
		map[string]any{"target_name": "比特币20251219", "target_type": "industry", "direction": "down", "strength": 3},
		map[string]any{"target_name": "美元/人民币", "target_type": "currency", "direction": "down", "strength": 5},
		map[string]any{"target_name": "柴油出口", "target_type": "industry", "direction": "down", "strength": 4},
		map[string]any{"target_name": "某未映射公司", "target_type": "tradable_asset", "asset_id": "asset-1", "direction": "up", "strength": 4},
	}
	kept, dropped := pruneMacroObservationImpacts(impacts)
	if !slices.Equal(dropped, []string{"LME", "美联储主席鲍威尔", "比特币20251219"}) {
		t.Fatalf("dropped = %#v", dropped)
	}
	types := make([]string, 0, len(kept))
	for _, raw := range kept {
		impact, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("kept entry = %#v", raw)
		}
		types = append(types, impact["target_name"].(string)+":"+impact["target_type"].(string))
	}
	want := []string{
		"白银:macro_economy", "人工智能:industry", "美元/人民币:fx_rate",
		"柴油出口:supply_volume", "某未映射公司:tradable_asset",
	}
	if !slices.Equal(types, want) {
		t.Fatalf("kept targets = %#v want %#v", types, want)
	}
}
