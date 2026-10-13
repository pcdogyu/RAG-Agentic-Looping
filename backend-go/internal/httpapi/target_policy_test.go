package httpapi

import "testing"

func TestMacroTargetPublishableRequiresVocabularyOrMasterData(t *testing.T) {
	published := []canonicalTarget{
		{Key: "industry:rare_earth", Label: "测试行业", TargetType: "sector", Verified: true},
		{Key: "asset:000001", Label: "某基金", TargetType: "other", Verified: true},
	}
	for _, canonical := range published {
		if !macroTargetPublishable(canonical.TargetType, canonical.Label, canonical) {
			t.Errorf("macroTargetPublishable rejected the master-data target %q", canonical.Key)
		}
	}
	unpublished := []canonicalTarget{
		{Key: "economy:美国利率决议", Label: "美国利率决议", TargetType: "economy"},
		{Key: "interest_rate:欧央行", Label: "欧央行", TargetType: "interest_rate"},
		{Key: "economy:日美关税协议", Label: "日美关税协议", TargetType: "economy"},
		{Key: "sector:某公司", Label: "某公司", TargetType: "sector"},
	}
	for _, canonical := range unpublished {
		if macroTargetPublishable(canonical.TargetType, canonical.Label, canonical) {
			t.Errorf("macroTargetPublishable published the free-text target %q", canonical.Key)
		}
	}
	for _, name := range []string{"美国CPI", "联邦基金利率", "半导体", "日元"} {
		canonical := canonicalTarget{Key: "economy:" + name, Label: name, TargetType: "economy"}
		if !macroTargetPublishable(canonical.TargetType, name, canonical) {
			t.Errorf("macroTargetPublishable rejected the valid macro target %q", name)
		}
	}
}

func TestCanonicalizeGoTargetMarksMasterDataIdentity(t *testing.T) {
	taxonomy := map[string]canonicalTarget{"环保设备": {Key: "industry:env_equipment", Label: "环保设备", TargetType: "sector"}}
	taxonomyHit := canonicalizeGoTarget("环保设备", "sector", nil, taxonomy)
	if !taxonomyHit.Verified || taxonomyHit.Key != "industry:env_equipment" {
		t.Errorf("taxonomy hit = %+v, want a verified industry:env_equipment target", taxonomyHit)
	}
	freeText := canonicalizeGoTarget("美国利率决议", "economy", nil, taxonomy)
	if freeText.Verified || freeText.Key != "economy:美国利率决议" {
		t.Errorf("free-text target = %+v, want an unverified economy:美国利率决议 target", freeText)
	}
	if macroTargetPublishable(freeText.TargetType, "美国利率决议", freeText) {
		t.Error("a free-text model label must not be publishable without master data")
	}
	asset := map[string]any{"asset_id": "fund:000001", "name": "某基金", "asset_class": "fund"}
	assetBacked := canonicalizeGoTarget("某基金", "other", asset, taxonomy)
	if !assetBacked.Verified || assetBacked.Key != "fund:000001" {
		t.Errorf("asset-backed target = %+v, want a verified fund:000001 target", assetBacked)
	}
}
