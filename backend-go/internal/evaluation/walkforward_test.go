package evaluation

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
)

func walkForwardFixture() ([]Record, WalkForwardConfig) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	records := make([]Record, 0, 180)
	for index := 0; index < 180; index++ {
		signal := start.AddDate(0, 0, index)
		records = append(records, Record{ID: fmtID(index), EventCluster: "event:" + fmtID(index), SignalAt: signal,
			PredictionAvailableAt: signal, FeatureCutoffAt: signal, LabelWindowStart: signal.AddDate(0, 0, 1), LabelWindowEnd: signal.AddDate(0, 0, 2),
			LabelMatureAt: signal.AddDate(0, 0, 3), Status: "mature"})
	}
	return records, WalkForwardConfig{SourceAvailableAsOf: start.AddDate(0, 0, 170), DevelopmentStart: start, DevelopmentEnd: start.AddDate(0, 0, 120), TrainWindowDays: 30,
		CalibrationWindowDays: 15, TestWindowDays: 15, StepDays: 15, EmbargoDays: 2,
		FinalHoldoutStart: start.AddDate(0, 0, 122), FinalHoldoutEnd: start.AddDate(0, 0, 152), FinalLabelCutoff: start.AddDate(0, 0, 160),
		HoldoutReservationID: "holdout-v1", HoldoutReservedAt: start.AddDate(0, 0, -30)}
}

func TestWalkForwardDatasetIsRollingReproducibleAndSealsFinalHoldout(t *testing.T) {
	records, config := walkForwardFixture()
	manifest, err := BuildWalkForwardDataset(records, config)
	if err != nil || len(manifest.Folds) < 2 || manifest.FinalHoldout.IncludedCount == 0 {
		t.Fatalf("manifest=%#v err=%v", manifest, err)
	}
	if !manifest.Folds[1].TrainStart.After(manifest.Folds[0].TrainStart) || slices.Contains(manifest.Folds[1].Train, fmtID(0)) {
		t.Fatalf("training window expanded instead of rolling: first=%#v second=%#v", manifest.Folds[0], manifest.Folds[1])
	}
	reversed := append([]Record{}, records...)
	slices.Reverse(reversed)
	repeated, err := BuildWalkForwardDataset(reversed, config)
	if err != nil || repeated.ID != manifest.ID || repeated.ManifestDigest != manifest.ManifestDigest {
		t.Fatalf("dataset was not reproducible: first=%s repeated=%s err=%v", manifest.ID, repeated.ID, err)
	}
	body, _ := json.Marshal(manifest)
	for _, member := range manifest.finalMembers {
		if strings.Contains(string(body), member.PredictionRunID) {
			t.Fatalf("sealed holdout sample %s leaked through the public manifest", member.PredictionRunID)
		}
	}
	if manifest.FinalHoldout.UsagePolicy != FinalHoldoutUsagePolicy || manifest.FinalHoldout.ManifestDigest == "" {
		t.Fatalf("holdout policy was not frozen: %#v", manifest.FinalHoldout)
	}
	if len(manifest.Folds[0].Members) == 0 || manifest.Folds[0].Members[0].FeatureCutoffAt.IsZero() || manifest.Folds[0].Members[0].LabelMatureAt.IsZero() {
		t.Fatalf("development sample manifest is incomplete: %#v", manifest.Folds[0].Members)
	}
}

func TestWalkForwardPurgesFutureFeaturesLabelOverlapAndTransitiveClusters(t *testing.T) {
	records, config := walkForwardFixture()
	// First fold: Jan 1-31 train, Feb 2-17 calibration, Feb 19-Mar 6 test.
	records[5].ClusterKeys = []string{"shared:a"}
	records[35].ClusterKeys = []string{"shared:a", "shared:b"}
	records[52].ClusterKeys = []string{"shared:b"}
	records[8].FeatureCutoffAt = records[8].SignalAt.Add(time.Hour)
	records[9].PredictionAvailableAt = records[9].LabelWindowStart.Add(time.Hour)
	records[28].LabelWindowEnd = config.DevelopmentStart.AddDate(0, 0, 35)
	manifest, err := BuildWalkForwardDataset(records, config)
	if err != nil {
		t.Fatal(err)
	}
	first := manifest.Folds[0]
	if !slices.Contains(first.Train, records[5].ID) {
		t.Fatalf("canonical earliest cluster member missing from train: %#v", first)
	}
	for _, expected := range []struct{ id, reason string }{
		{records[35].ID, "event_cluster_cross_partition"}, {records[52].ID, "event_cluster_cross_partition"},
		{records[8].ID, "feature_available_after_signal"}, {records[9].ID, "prediction_created_after_label_entry"}, {records[28].ID, "label_window_crosses_partition_boundary"},
	} {
		if !hasExcludedMember(first.Excluded, expected.id, expected.reason) {
			t.Errorf("missing exclusion %s=%s in %#v", expected.id, expected.reason, first.Excluded)
		}
	}
}

func TestFinalHoldoutRejectsDevelopmentClustersAndLateReservation(t *testing.T) {
	records, config := walkForwardFixture()
	records[10].ClusterKeys = []string{"same-fact"}
	records[130].ClusterKeys = []string{"same-fact"}
	manifest, err := BuildWalkForwardDataset(records, config)
	if err != nil {
		t.Fatal(err)
	}
	if !hasExcludedMember(manifest.finalExcluded, records[130].ID, "event_cluster_seen_in_development") {
		t.Fatalf("development fact leaked into final holdout: %#v", manifest.finalExcluded)
	}
	late := config
	late.HoldoutReservedAt = late.FinalHoldoutStart
	if _, err := BuildWalkForwardDataset(records, late); err == nil {
		t.Fatal("holdout reserved after seeing its first signal was accepted")
	}
}

func TestWalkForwardRejectsEmptyFinalHoldoutAndIncompleteCutoffs(t *testing.T) {
	records, config := walkForwardFixture()
	for index := 122; index < 152; index++ {
		records[index].Status = "pending"
	}
	if _, err := BuildWalkForwardDataset(records, config); err == nil || !strings.Contains(err.Error(), "no eligible samples") {
		t.Fatalf("empty final holdout was accepted: %v", err)
	}
	invalid := config
	invalid.SourceAvailableAsOf = invalid.FinalLabelCutoff.Add(-time.Nanosecond)
	if _, err := BuildWalkForwardDataset(records, invalid); err == nil || !strings.Contains(err.Error(), "source_available_as_of") {
		t.Fatalf("unobserved label cutoff was accepted: %v", err)
	}
	invalid = config
	invalid.FinalLabelCutoff = invalid.FinalHoldoutEnd
	if _, err := BuildWalkForwardDataset(records, invalid); err == nil || !strings.Contains(err.Error(), "pre-registered final holdout") {
		t.Fatalf("zero-length holdout label maturity period was accepted: %v", err)
	}
}

func hasExcludedMember(values []SplitMember, id, reason string) bool {
	for _, item := range values {
		if item.PredictionRunID == id && item.ExclusionReason == reason {
			return true
		}
	}
	return false
}
