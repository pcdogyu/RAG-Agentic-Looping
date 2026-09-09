package calibration

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestPlattCalibrationIsScopedAndNeverPublishesExactBounds(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	observations := make([]Observation, 40)
	for index := range observations {
		score := float64(index-20) / 5
		observations[index] = Observation{SampleID: fmt.Sprintf("sample-%d", index), Score: score, Label: index >= 20, ObservedAt: base.AddDate(0, 0, index)}
	}
	model, err := FitPlatt("baseline-v1", observations, Scope{AssetClass: "equity", Market: "US", HorizonSessions: 5, EventTypes: []string{"earnings"}})
	if err != nil {
		t.Fatal(err)
	}
	result := model.Apply("baseline-v1", 1000, "equity", "US", 5, "earnings", base.AddDate(0, 0, 50))
	if result.Status != "calibrated" || result.Probability == nil || *result.Probability <= 0 || *result.Probability >= 1 {
		t.Fatalf("result=%#v", result)
	}
	if outside := model.Apply("baseline-v1", 0, "equity", "CN", 5, "earnings", base.AddDate(0, 0, 50)); outside.Status != "unavailable" {
		t.Fatalf("outside=%#v", outside)
	}
	if outside := model.Apply("baseline-v1", 0, "crypto", "US", 5, "earnings", base.AddDate(0, 0, 50)); outside.Status != "unavailable" {
		t.Fatalf("cross-asset calibration was accepted: %#v", outside)
	}
}

func TestCalibrationMetricsReportBinsAndRejectBounds(t *testing.T) {
	metrics, err := Evaluate([]float64{.2, .7, .8, .3}, []bool{false, true, true, false}, 2)
	if err != nil || metrics.SampleCount != 4 || len(metrics.Bins) != 2 || math.IsNaN(metrics.ECE) {
		t.Fatalf("metrics=%#v err=%v", metrics, err)
	}
	if _, err := Evaluate([]float64{0}, []bool{false}, 10); err == nil {
		t.Fatal("exact zero probability accepted")
	}
}

func TestCalibrationVersionTracksLabelsAndIndependentClusters(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	observations := make([]Observation, 40)
	for index := range observations {
		observations[index] = Observation{SampleID: fmt.Sprintf("sample-%d", index), EventCluster: fmt.Sprintf("cluster-%d", index), Score: float64(index), Label: index >= 20, ObservedAt: base.AddDate(0, 0, index)}
	}
	first, err := FitPlatt("baseline-v1", observations, Scope{Market: "US", HorizonSessions: 5})
	if err != nil {
		t.Fatal(err)
	}
	changed := append([]Observation{}, observations...)
	changed[0] = observations[0]
	changed[0].Label = !changed[0].Label
	second, err := FitPlatt("baseline-v1", changed, Scope{Market: "US", HorizonSessions: 5})
	if err != nil {
		t.Fatal(err)
	}
	if first.Version == second.Version {
		t.Fatal("calibration version did not change when labels changed")
	}
	duplicate := append([]Observation{}, observations...)
	duplicate[1].EventCluster = duplicate[0].EventCluster
	if _, err := FitPlatt("baseline-v1", duplicate, Scope{Market: "US", HorizonSessions: 5}); err == nil {
		t.Fatal("duplicate calibration event cluster was accepted")
	}
}
