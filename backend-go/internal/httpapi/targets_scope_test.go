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

func TestPublishableEventImpactRequiresVerifiedRelationForNewMacroPrompts(t *testing.T) {
	verified := map[string]any{"impact_verification": map[string]any{"relation_verified": true}}
	unverified := map[string]any{"impact_verification": map[string]any{"relation_verified": false}}
	if !publishableEventImpact(map[string]any{"prompt_version": "event-research-prompt-v6.0-three-day"}, unverified) {
		t.Fatal("legacy report compatibility was not preserved")
	}
	if publishableEventImpact(map[string]any{"prompt_version": "event-research-prompt-v6.1-macro-observation"}, unverified) {
		t.Fatal("unverified v6.1 observation impact was published")
	}
	if !publishableEventImpact(map[string]any{"prompt_version": "event-research-prompt-v6.2-action-observation"}, verified) {
		t.Fatal("verified v6.2 observation impact was hidden")
	}
	if publishableEventImpact(map[string]any{"prompt_version": "event-research-prompt-v6.3-structured-action-observation"}, unverified) {
		t.Fatal("unverified v6.3 observation impact was published")
	}
}
