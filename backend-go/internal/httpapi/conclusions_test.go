package httpapi

import "testing"

func TestFilterTargetChangesMatchesVisibleIdentityFields(t *testing.T) {
	items := []map[string]any{
		{"label": "能源行业", "symbol": nil, "market": nil, "target_type": "sector"},
		{"label": "Target Corporation", "symbol": "TGT", "market": "US", "target_type": "tradable_asset"},
		{"label": "WTI 原油", "symbol": "CLUSD", "market": "COMMODITY", "target_type": "commodity_price"},
		{"label": "Bitcoin", "symbol": "BTC", "market": "CRYPTO", "target_type": "tradable_asset"},
	}
	tests := []struct {
		query string
		want  int
	}{
		{query: "能源", want: 1},
		{query: "tgt", want: 1},
		{query: "crypto", want: 1},
		{query: "COMMODITY_PRICE", want: 1},
		{query: "missing", want: 0},
		{query: "  ", want: 4},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			if got := len(filterTargetChanges(items, test.query)); got != test.want {
				t.Fatalf("query %q returned %d items, want %d", test.query, got, test.want)
			}
		})
	}
}

func TestRepresentativeImpactUsesStrongestAbsoluteScoreAndKeepsFirstTie(t *testing.T) {
	tests := []struct {
		name    string
		impacts []any
		score   any
		rating  any
	}{
		{
			name: "strongest negative",
			impacts: []any{
				map[string]any{"direction_score": 45.0, "rating": "bullish"},
				map[string]any{"direction_score": -80.0, "rating": "strongly_bearish"},
			},
			score: -80.0, rating: "strongly_bearish",
		},
		{
			name: "first absolute tie",
			impacts: []any{
				map[string]any{"direction_score": 70.0, "rating": "strongly_bullish"},
				map[string]any{"direction_score": -70.0, "rating": "strongly_bearish"},
			},
			score: 70.0, rating: "strongly_bullish",
		},
		{
			name: "legacy normalized score",
			impacts: []any{
				map[string]any{"score": -0.35, "rating": "bearish"},
			},
			score: -35.0, rating: "bearish",
		},
		{name: "no targets", impacts: nil, score: nil, rating: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			score, rating := representativeImpact(test.impacts)
			if score != test.score || rating != test.rating {
				t.Fatalf("got score=%v rating=%v, want score=%v rating=%v", score, rating, test.score, test.rating)
			}
		})
	}
}

func TestSanitizePublishedImpactsRemovesActivityAndRebindsStockPrice(t *testing.T) {
	hood := map[string]any{"asset_id": "hood-id", "symbol": "HOOD", "name": "Robinhood Markets, Inc.", "asset_class": "equity"}
	ibkr := map[string]any{"asset_id": "ibkr-id", "symbol": "IBKR", "name": "Interactive Brokers Group, Inc.", "asset_class": "equity"}
	impacts := []any{
		map[string]any{"target_name": "Interactive Brokers Group, Inc.", "target_type": "tradable_asset", "asset": ibkr, "direction_score": 0.0},
		map[string]any{"target_name": "Robinhood Markets, Inc.", "target_type": "tradable_asset", "asset": hood, "direction_score": 0.0},
		map[string]any{"target_name": "交易量增加", "target_type": "economy", "direction_score": 80.0},
		map[string]any{"target_name": "市场活跃度提升", "target_type": "economy", "direction_score": 75.0},
		map[string]any{"target_name": "零售交易者参与度提高", "target_type": "economy", "direction_score": 90.0},
		map[string]any{"target_name": "Robinhood 股价", "target_type": "economy", "direction_score": 85.0, "evidence_ids": []any{"ev-1"}},
	}

	got := sanitizePublishedImpacts(impacts)
	if len(got) != 2 {
		t.Fatalf("got %d impacts, want 2: %#v", len(got), got)
	}
	hoodImpact := objectValue(got[1])
	if stringValue(hoodImpact["target_type"]) != "tradable_asset" || stringValue(objectValue(hoodImpact["asset"])["symbol"]) != "HOOD" {
		t.Fatalf("Robinhood stock price was not rebound to HOOD: %#v", hoodImpact)
	}
	if numberValue(hoodImpact["direction_score"]) != 85 {
		t.Fatalf("got HOOD score %v, want 85", hoodImpact["direction_score"])
	}
}
