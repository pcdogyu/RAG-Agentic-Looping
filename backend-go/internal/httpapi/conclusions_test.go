package httpapi

import "testing"

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
