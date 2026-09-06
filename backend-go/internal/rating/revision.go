package rating

import "time"

// State is the current fundamental view for one isolated policy/horizon/
// benchmark key. It is separate from the event-signal state exposed by news.
type State struct {
	AssetID        string    `json:"asset_id"`
	PolicyVersion  string    `json:"policy_version"`
	HorizonDays    int       `json:"horizon_days"`
	BenchmarkID    string    `json:"benchmark_id"`
	Rating         string    `json:"rating"`
	ValuationRunID string    `json:"valuation_run_id"`
	EffectiveAt    time.Time `json:"effective_at"`
}

type Revision struct {
	PreviousRating string `json:"previous_rating,omitempty"`
	CurrentRating  string `json:"current_rating,omitempty"`
	Action         string `json:"action"`
	Reason         string `json:"reason,omitempty"`
	ValuationRunID string `json:"valuation_run_id,omitempty"`
}

// Revise maps a newly evaluated fundamental rating to an audit action. The
// caller can persist this atomically later; this pure transition is also used
// by replay tests so event direction scores cannot alter fundamental state.
func Revise(previous *State, next Result) Revision {
	if next.Status != "available" || next.Rating == "" {
		return Revision{Action: "under_review", Reason: next.Reason, ValuationRunID: next.ValuationRunID}
	}
	if previous == nil || previous.Rating == "" {
		return Revision{CurrentRating: next.Rating, Action: "initiated", ValuationRunID: next.ValuationRunID}
	}
	if previous.Rating == next.Rating {
		return Revision{PreviousRating: previous.Rating, CurrentRating: next.Rating, Action: "maintained", ValuationRunID: next.ValuationRunID}
	}
	action := "upgraded"
	if ratingRank(next.Rating) < ratingRank(previous.Rating) {
		action = "downgraded"
	}
	return Revision{PreviousRating: previous.Rating, CurrentRating: next.Rating, Action: action, ValuationRunID: next.ValuationRunID}
}

func ratingRank(value string) int {
	switch value {
	case "strong_sell":
		return 0
	case "sell":
		return 1
	case "hold":
		return 2
	case "buy":
		return 3
	case "strong_buy":
		return 4
	default:
		return -1
	}
}
