package jobs

import (
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestMappingLaneRegistersRequiredHandler(t *testing.T) {
	lane, err := ValidateBatchFourActivation("mapping", []string{"extract"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, NewMappingHandlers(config.Config{}, nil, nil)); err != nil {
		t.Fatal(err)
	}
}

func TestMappingShortlistUsesExplicitIssuerSymbolAndProduct(t *testing.T) {
	assets := []mappingAsset{
		{ID: "baba", Symbol: "BABA", Name: "Alibaba Group Holding Limited", Aliases: []string{"阿里巴巴"}, Products: []string{"阿里云"}, MarketCap: 10},
		{ID: "ai", Symbol: "AI", Name: "C3.ai, Inc.", Aliases: []string{"C3.ai"}, MarketCap: 5},
	}
	got := shortlistMappingAssets("阿里云企业智能客服增长，AI 行业活跃", assets, 30)
	if len(got) != 1 || got[0].ID != "baba" || got[0].ShortlistRank != 95 {
		t.Fatalf("unexpected shortlist: %#v", got)
	}
	got = shortlistMappingAssets("NASDAQ: AI reports results", assets, 30)
	if len(got) != 1 || got[0].ID != "ai" {
		t.Fatalf("qualified ticker was not shortlisted: %#v", got)
	}
}

func TestProductOwnershipExpandsIssuerListings(t *testing.T) {
	assets := []mappingAsset{
		{ID: "us", Name: "Alibaba", Products: []string{"阿里云"}, IssuerID: "issuer:alibaba", Data: map[string]any{"asset_id": "us"}},
		{ID: "hk", Name: "Alibaba", IssuerID: "issuer:alibaba", Data: map[string]any{"asset_id": "hk"}},
	}
	got := productOwnerCandidates("阿里云市场份额增长", assets)
	if len(got) != 2 || got["us"] == nil || got["hk"] == nil {
		t.Fatalf("issuer listings were not expanded: %#v", got)
	}
}

func TestMappingHintRejectsUnmentionedProxy(t *testing.T) {
	asset := mappingAsset{ID: "nvda", Symbol: "NVDA", Name: "NVIDIA Corporation", Data: map[string]any{"asset_id": "nvda"}}
	hint := mappingHint{AssetID: "nvda", SourceMention: "云计算", Name: asset.Name, Relationship: "entity", Confidence: .9, Rationale: "proxy"}
	if got := validateMappingHint(hint, "云计算投资增长", nil, []mappingAsset{asset}, []mappingAsset{asset}); len(got) != 0 {
		t.Fatalf("unmentioned proxy must be rejected: %#v", got)
	}
}
