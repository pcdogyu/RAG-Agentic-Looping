package httpapi

import "testing"

func targetScopeFixture() []map[string]any {
	return []map[string]any{
		{
			"label": "Older changed target", "observed_at": "2026-09-09T00:00:00Z", "change_detail_id": "change-1",
			"latest_detail": map[string]any{"id": "observation-1"},
			"rating_state":  map[string]any{"previous": "watch", "current": "bullish"},
		},
		{
			"label": "Newest observed target", "observed_at": "2026-09-10T00:00:00Z", "change_detail_id": "change-2",
			"latest_detail": map[string]any{"id": "observation-2"},
			"rating_state":  map[string]any{"previous": "watch", "current": "watch"},
		},
	}
}

func TestScopedTargetChangesKeepsRecentObservationsWithoutWeakeningChangeFilter(t *testing.T) {
	observed := scopedTargetChanges(targetScopeFixture(), "observed", "")
	if len(observed) != 2 || stringValue(observed[0]["label"]) != "Newest observed target" {
		t.Fatalf("observed scope did not retain and sort recent signals: %#v", observed)
	}
	if boolValue(observed[0]["overall_rating_changed"]) || !boolValue(observed[1]["overall_rating_changed"]) {
		t.Fatalf("observed scope did not label rating-change state: %#v", observed)
	}

	changed := scopedTargetChanges(targetScopeFixture(), "changed", "older")
	if len(changed) != 1 || stringValue(changed[0]["label"]) != "Older changed target" {
		t.Fatalf("changed scope no longer returns only real rating changes: %#v", changed)
	}
}

func TestObservedTargetCursorUsesObservationIdentity(t *testing.T) {
	item := targetScopeFixture()[1]
	if field := targetChangeStampField("observed"); field != "observed_at" {
		t.Fatalf("unexpected observed timestamp field %q", field)
	}
	if id := targetChangeCursorID(item, "observed"); id != "observation-2" {
		t.Fatalf("unexpected observed cursor id %q", id)
	}
}
