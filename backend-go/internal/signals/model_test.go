package signals

import (
	"testing"
	"time"
)

func pointer(value float64) *float64 { return &value }

func TestSnapshotBlocksFutureAndPreservesMissing(t *testing.T) {
	asOf := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)
	snapshot := BuildSnapshot(asOf, []string{"news", "price"}, []Feature{{Name: "news", Value: pointer(.8), AvailableAt: asOf.Add(-time.Minute), SourceIDs: []string{"e"}}, {Name: "price", Value: pointer(10), AvailableAt: asOf.Add(time.Minute), SourceIDs: []string{"future"}}})
	if snapshot.Values["news"] == nil || snapshot.Values["price"] != nil || len(snapshot.Missing) != 1 || snapshot.Missing[0] != "price" || len(snapshot.FutureBlocked) != 1 {
		t.Fatalf("snapshot=%#v", snapshot)
	}
}

func TestBinaryModelUsesMatureTrainingRowsAndStaysUncalibrated(t *testing.T) {
	cutoff := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	samples := []Sample{}
	for index := 0; index < 12; index++ {
		value := float64(index) - 5
		samples = append(samples, Sample{ID: string(rune('a' + index)), SignalAt: cutoff.AddDate(0, 0, -20+index), LabelMatureAt: cutoff.AddDate(0, 0, -10+index), Values: map[string]*float64{"surprise": pointer(value)}, Label: value > 0})
	}
	// A future-maturing contradictory row must not influence preprocessing.
	samples = append(samples, Sample{ID: "future", SignalAt: cutoff, LabelMatureAt: cutoff.AddDate(0, 0, 5), Values: map[string]*float64{"surprise": pointer(1000)}, Label: false})
	model, err := FitBinary(samples, []string{"surprise"}, FitOptions{Objective: "excess_up", HorizonSessions: 5, TrainingCutoff: cutoff})
	if err != nil {
		t.Fatal(err)
	}
	if model.SampleCount != 11 {
		t.Fatalf("sample_count=%d", model.SampleCount)
	}
	prediction := model.Predict(BuildSnapshot(cutoff.Add(time.Hour), []string{"surprise"}, []Feature{{Name: "surprise", Value: pointer(2), AvailableAt: cutoff}}))
	if prediction.Status != "uncalibrated" || prediction.Probability != nil || prediction.RawScore == nil || prediction.CalibrationStatus != "uncalibrated" {
		t.Fatalf("prediction=%#v", prediction)
	}
}

func TestCoreFeatureContractPreservesMissingAndFutureValues(t *testing.T) {
	now := time.Now().UTC()
	value := .4
	snapshot := BuildSnapshot(now, CoreFeatureNames(), []Feature{
		{Name: FeatureConsensusSurprise, Value: &value, AvailableAt: now, SourceIDs: []string{"estimate-revision-1"}},
		{Name: FeaturePriceReaction, Value: &value, AvailableAt: now.Add(time.Minute), SourceIDs: []string{"bar-1"}},
	})
	if snapshot.Values[FeatureConsensusSurprise] == nil || len(snapshot.Missing) != 3 || len(snapshot.FutureBlocked) != 1 {
		t.Fatalf("snapshot=%#v", snapshot)
	}
}

func TestBinaryModelVersionTracksTrainingTruthAndRejectsDuplicateClusters(t *testing.T) {
	cutoff := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	samples := make([]Sample, 12)
	for index := range samples {
		value := float64(index)
		samples[index] = Sample{ID: string(rune('a' + index)), EventCluster: string(rune('A' + index)), SignalAt: cutoff.AddDate(0, 0, -20+index), LabelMatureAt: cutoff.AddDate(0, 0, -10+index), Values: map[string]*float64{"feature": pointer(value)}, Label: index >= 6}
	}
	options := FitOptions{Objective: "excess_up", HorizonSessions: 5, TrainingCutoff: cutoff}
	first, err := FitBinary(samples, []string{"feature"}, options)
	if err != nil {
		t.Fatal(err)
	}
	changed := append([]Sample{}, samples...)
	changed[0] = samples[0]
	changed[0].Label = !changed[0].Label
	second, err := FitBinary(changed, []string{"feature"}, options)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version == second.Version {
		t.Fatal("model version did not change when the training truth changed")
	}
	duplicate := append([]Sample{}, samples...)
	duplicate[1].EventCluster = duplicate[0].EventCluster
	if _, err := FitBinary(duplicate, []string{"feature"}, options); err == nil {
		t.Fatal("duplicate event cluster was accepted")
	}
}
