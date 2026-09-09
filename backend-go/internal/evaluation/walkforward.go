package evaluation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	WalkForwardDatasetContractVersion = "walk-forward-dataset-v1"
	FinalHoldoutUsagePolicy           = "final_evaluation_only_not_model_selection"
)

type Record struct {
	ID                    string    `json:"id"`
	EventCluster          string    `json:"event_cluster"`
	ClusterKeys           []string  `json:"cluster_keys,omitempty"`
	SignalAt              time.Time `json:"signal_at"`
	PredictionAvailableAt time.Time `json:"prediction_available_at"`
	FeatureCutoffAt       time.Time `json:"feature_cutoff_at"`
	LabelWindowStart      time.Time `json:"label_window_start"`
	LabelWindowEnd        time.Time `json:"label_window_end"`
	LabelMatureAt         time.Time `json:"label_mature_at"`
	Status                string    `json:"status"`
	ExclusionReason       string    `json:"exclusion_reason,omitempty"`
}

type SplitMember struct {
	PredictionRunID       string    `json:"prediction_run_id"`
	Partition             string    `json:"partition"`
	IntendedPartition     string    `json:"intended_partition"`
	EventCluster          string    `json:"event_cluster"`
	SignalAt              time.Time `json:"signal_at"`
	PredictionAvailableAt time.Time `json:"prediction_available_at"`
	FeatureCutoffAt       time.Time `json:"feature_cutoff_at"`
	LabelWindowStart      time.Time `json:"label_window_start"`
	LabelWindowEnd        time.Time `json:"label_window_end"`
	LabelMatureAt         time.Time `json:"label_mature_at"`
	ExclusionReason       string    `json:"exclusion_reason,omitempty"`
}

type Fold struct {
	Index             int           `json:"index"`
	Train             []string      `json:"train"`
	Calibration       []string      `json:"calibration"`
	Test              []string      `json:"test"`
	Excluded          []SplitMember `json:"excluded"`
	TrainStart        time.Time     `json:"train_start"`
	TrainCutoff       time.Time     `json:"train_cutoff"`
	CalibrationStart  time.Time     `json:"calibration_start"`
	CalibrationCutoff time.Time     `json:"calibration_cutoff"`
	TestStart         time.Time     `json:"test_start"`
	TestCutoff        time.Time     `json:"test_cutoff"`
	ManifestDigest    string        `json:"manifest_digest"`
	Members           []SplitMember `json:"members"`
}

type WalkForwardConfig struct {
	SourceAvailableAsOf   time.Time `json:"source_available_as_of"`
	DevelopmentStart      time.Time `json:"development_start"`
	DevelopmentEnd        time.Time `json:"development_end"`
	TrainWindowDays       int       `json:"train_window_days"`
	CalibrationWindowDays int       `json:"calibration_window_days"`
	TestWindowDays        int       `json:"test_window_days"`
	StepDays              int       `json:"step_days"`
	EmbargoDays           int       `json:"embargo_days"`
	FinalHoldoutStart     time.Time `json:"final_holdout_start"`
	FinalHoldoutEnd       time.Time `json:"final_holdout_end"`
	FinalLabelCutoff      time.Time `json:"final_label_cutoff"`
	HoldoutReservationID  string    `json:"holdout_reservation_id"`
	HoldoutReservedAt     time.Time `json:"holdout_reserved_at"`
}

type FinalHoldoutDescriptor struct {
	ReservationID  string         `json:"reservation_id"`
	SignalStart    time.Time      `json:"signal_start"`
	SignalEnd      time.Time      `json:"signal_end"`
	LabelCutoff    time.Time      `json:"label_cutoff"`
	ReservedAt     time.Time      `json:"reserved_at"`
	UsagePolicy    string         `json:"usage_policy"`
	IncludedCount  int            `json:"included_count"`
	ExcludedCount  int            `json:"excluded_count"`
	ExclusionCount map[string]int `json:"exclusion_count"`
	ManifestDigest string         `json:"manifest_digest"`
}

type DatasetManifest struct {
	ID                     string                 `json:"id"`
	ContractVersion        string                 `json:"contract_version"`
	LabelDefinitionVersion string                 `json:"label_definition_version"`
	Config                 WalkForwardConfig      `json:"config"`
	CandidateCount         int                    `json:"candidate_count"`
	Folds                  []Fold                 `json:"folds"`
	FinalHoldout           FinalHoldoutDescriptor `json:"final_holdout"`
	ExclusionCount         map[string]int         `json:"exclusion_count"`
	ManifestDigest         string                 `json:"manifest_digest"`
	finalMembers           []SplitMember
	finalExcluded          []SplitMember
}

// WalkForward retains the original in-memory API while applying true rolling
// training windows and strict point-in-time checks. New persisted datasets use
// BuildWalkForwardDataset so their boundaries and exclusions are auditable.
func WalkForward(records []Record, trainWindow, calibrationWindow, testWindow, step, embargo time.Duration) ([]Fold, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("records are required")
	}
	trainDays, err := exactDays(trainWindow)
	if err != nil {
		return nil, err
	}
	calibrationDays, err := exactDays(calibrationWindow)
	if err != nil {
		return nil, err
	}
	testDays, err := exactDays(testWindow)
	if err != nil {
		return nil, err
	}
	stepDays, err := exactDays(step)
	if err != nil {
		return nil, err
	}
	embargoDays, err := nonNegativeExactDays(embargo)
	if err != nil {
		return nil, err
	}
	legacy := append([]Record{}, records...)
	for index := range legacy {
		if legacy[index].FeatureCutoffAt.IsZero() {
			legacy[index].FeatureCutoffAt = legacy[index].SignalAt
		}
		if legacy[index].PredictionAvailableAt.IsZero() {
			legacy[index].PredictionAvailableAt = legacy[index].SignalAt
		}
		if legacy[index].LabelWindowStart.IsZero() {
			legacy[index].LabelWindowStart = legacy[index].SignalAt
		}
		if legacy[index].LabelWindowEnd.IsZero() {
			legacy[index].LabelWindowEnd = legacy[index].LabelMatureAt
		}
		if legacy[index].Status == "" {
			legacy[index].Status = "mature"
		}
	}
	sortRecords(legacy)
	config := WalkForwardConfig{DevelopmentStart: legacy[0].SignalAt.UTC(), DevelopmentEnd: legacy[len(legacy)-1].SignalAt.UTC().Add(time.Nanosecond),
		TrainWindowDays: trainDays, CalibrationWindowDays: calibrationDays, TestWindowDays: testDays, StepDays: stepDays, EmbargoDays: embargoDays}
	components := clusterComponents(legacy)
	return buildFolds(legacy, components, config)
}

func BuildWalkForwardDataset(records []Record, config WalkForwardConfig) (DatasetManifest, error) {
	manifest := DatasetManifest{ContractVersion: WalkForwardDatasetContractVersion, LabelDefinitionVersion: OutcomeLabelDefinitionVersion,
		Config: config, CandidateCount: len(records), Folds: []Fold{}, ExclusionCount: map[string]int{}}
	if err := validateWalkForwardConfig(config); err != nil {
		return DatasetManifest{}, err
	}
	if len(records) == 0 {
		return DatasetManifest{}, fmt.Errorf("records are required")
	}
	ordered := append([]Record{}, records...)
	sortRecords(ordered)
	components := clusterComponents(ordered)
	folds, err := buildFolds(ordered, components, config)
	if err != nil {
		return DatasetManifest{}, err
	}
	manifest.Folds = folds
	usedDevelopmentClusters := map[string]bool{}
	for _, fold := range folds {
		for _, member := range fold.Members {
			usedDevelopmentClusters[member.EventCluster] = true
		}
		for _, item := range fold.Excluded {
			manifest.ExclusionCount[item.ExclusionReason]++
		}
	}
	manifest.finalMembers, manifest.finalExcluded = buildFinalHoldout(ordered, components, config, usedDevelopmentClusters)
	if len(manifest.finalMembers) == 0 {
		return DatasetManifest{}, fmt.Errorf("final holdout has no eligible samples")
	}
	finalExclusions := map[string]int{}
	for _, item := range manifest.finalExcluded {
		finalExclusions[item.ExclusionReason]++
		manifest.ExclusionCount[item.ExclusionReason]++
	}
	manifest.FinalHoldout = FinalHoldoutDescriptor{ReservationID: config.HoldoutReservationID, SignalStart: config.FinalHoldoutStart.UTC(),
		SignalEnd: config.FinalHoldoutEnd.UTC(), LabelCutoff: config.FinalLabelCutoff.UTC(), ReservedAt: config.HoldoutReservedAt.UTC(),
		UsagePolicy: FinalHoldoutUsagePolicy, IncludedCount: len(manifest.finalMembers), ExcludedCount: len(manifest.finalExcluded), ExclusionCount: finalExclusions}
	manifest.FinalHoldout.ManifestDigest = digestValue(struct {
		Reservation string        `json:"reservation"`
		Included    []SplitMember `json:"included"`
		Excluded    []SplitMember `json:"excluded"`
	}{config.HoldoutReservationID, manifest.finalMembers, manifest.finalExcluded})
	manifest.ManifestDigest = datasetDigest(manifest)
	manifest.ID = "dataset-" + manifest.ManifestDigest[:32]
	return manifest, nil
}

func validateWalkForwardConfig(config WalkForwardConfig) error {
	if config.DevelopmentStart.IsZero() || !config.DevelopmentEnd.After(config.DevelopmentStart) {
		return fmt.Errorf("development_start must precede development_end")
	}
	if config.TrainWindowDays < 1 || config.CalibrationWindowDays < 1 || config.TestWindowDays < 1 || config.StepDays < 1 || config.EmbargoDays < 0 {
		return fmt.Errorf("positive rolling windows and non-negative embargo are required")
	}
	if strings.TrimSpace(config.HoldoutReservationID) == "" || config.HoldoutReservedAt.IsZero() || config.FinalHoldoutStart.IsZero() || !config.FinalHoldoutEnd.After(config.FinalHoldoutStart) || !config.FinalLabelCutoff.After(config.FinalHoldoutEnd) {
		return fmt.Errorf("a complete pre-registered final holdout is required")
	}
	if !config.HoldoutReservedAt.Before(config.FinalHoldoutStart) {
		return fmt.Errorf("final holdout must be reserved before its first signal")
	}
	minimumHoldoutStart := config.DevelopmentEnd.AddDate(0, 0, config.EmbargoDays)
	if config.FinalHoldoutStart.Before(minimumHoldoutStart) {
		return fmt.Errorf("final holdout overlaps development data or its embargo")
	}
	if config.SourceAvailableAsOf.IsZero() || config.SourceAvailableAsOf.Before(config.FinalLabelCutoff) {
		return fmt.Errorf("source_available_as_of must reach the pre-registered final label cutoff")
	}
	return nil
}

func buildFolds(records []Record, components map[int]string, config WalkForwardConfig) ([]Fold, error) {
	folds := []Fold{}
	for trainStart := config.DevelopmentStart.UTC(); ; trainStart = trainStart.AddDate(0, 0, config.StepDays) {
		trainEnd := trainStart.AddDate(0, 0, config.TrainWindowDays)
		calibrationStart := trainEnd.AddDate(0, 0, config.EmbargoDays)
		calibrationEnd := calibrationStart.AddDate(0, 0, config.CalibrationWindowDays)
		testStart := calibrationEnd.AddDate(0, 0, config.EmbargoDays)
		testEnd := testStart.AddDate(0, 0, config.TestWindowDays)
		if testEnd.After(config.DevelopmentEnd) {
			break
		}
		fold := Fold{Index: len(folds), Train: []string{}, Calibration: []string{}, Test: []string{}, Excluded: []SplitMember{},
			TrainStart: trainStart, TrainCutoff: trainEnd, CalibrationStart: calibrationStart, CalibrationCutoff: calibrationEnd,
			TestStart: testStart, TestCutoff: testEnd}
		candidates := []SplitMember{}
		for index, record := range records {
			if record.SignalAt.Before(trainStart) || !record.SignalAt.Before(testEnd) {
				continue
			}
			partition, cutoff := foldPartition(record.SignalAt, fold)
			member := splitMember(record, components[index], partition)
			if partition == "" {
				member.Partition, member.ExclusionReason = "excluded", "embargo_gap"
				fold.Excluded = append(fold.Excluded, member)
				continue
			}
			if reason := recordExclusion(record, cutoff); reason != "" {
				member.Partition, member.ExclusionReason = "excluded", reason
				fold.Excluded = append(fold.Excluded, member)
				continue
			}
			candidates = append(candidates, member)
		}
		included, excluded := groupSafeMembers(candidates, nil)
		fold.Excluded = append(fold.Excluded, excluded...)
		fold.Members = included
		for _, member := range included {
			switch member.Partition {
			case "train":
				fold.Train = append(fold.Train, member.PredictionRunID)
			case "calibration":
				fold.Calibration = append(fold.Calibration, member.PredictionRunID)
			case "test":
				fold.Test = append(fold.Test, member.PredictionRunID)
			}
		}
		sortSplitMembers(fold.Excluded)
		if len(fold.Train) == 0 || len(fold.Calibration) == 0 || len(fold.Test) == 0 {
			continue
		}
		fold.Index = len(folds)
		fold.ManifestDigest = digestValue(struct {
			Index    int           `json:"index"`
			Members  []SplitMember `json:"members"`
			Excluded []SplitMember `json:"excluded"`
		}{fold.Index, fold.Members, fold.Excluded})
		folds = append(folds, fold)
	}
	if len(folds) == 0 {
		return nil, fmt.Errorf("no complete walk-forward folds")
	}
	return folds, nil
}

func buildFinalHoldout(records []Record, components map[int]string, config WalkForwardConfig, usedDevelopmentClusters map[string]bool) ([]SplitMember, []SplitMember) {
	candidates, excluded := []SplitMember{}, []SplitMember{}
	for index, record := range records {
		if record.SignalAt.Before(config.FinalHoldoutStart) || !record.SignalAt.Before(config.FinalHoldoutEnd) {
			continue
		}
		member := splitMember(record, components[index], "final_holdout")
		if reason := recordExclusion(record, config.FinalLabelCutoff); reason != "" {
			member.Partition, member.ExclusionReason = "excluded", reason
			excluded = append(excluded, member)
			continue
		}
		candidates = append(candidates, member)
	}
	included, groupedExcluded := groupSafeMembers(candidates, usedDevelopmentClusters)
	excluded = append(excluded, groupedExcluded...)
	sortSplitMembers(excluded)
	return included, excluded
}

func recordExclusion(record Record, partitionCutoff time.Time) string {
	if strings.TrimSpace(record.ID) == "" || record.SignalAt.IsZero() {
		return "invalid_prediction_identity"
	}
	if reason := strings.TrimSpace(record.ExclusionReason); reason != "" {
		return reason
	}
	if record.Status != "mature" {
		if record.Status == "" {
			return "outcome_status_missing"
		}
		return "outcome_" + strings.ToLower(strings.TrimSpace(record.Status))
	}
	if record.FeatureCutoffAt.IsZero() {
		return "feature_cutoff_missing"
	}
	if record.FeatureCutoffAt.After(record.SignalAt) {
		return "feature_available_after_signal"
	}
	if record.LabelWindowStart.IsZero() || record.LabelWindowEnd.IsZero() || record.LabelWindowStart.Before(record.SignalAt) || record.LabelWindowEnd.Before(record.LabelWindowStart) {
		return "invalid_label_window"
	}
	if record.PredictionAvailableAt.IsZero() {
		return "prediction_availability_missing"
	}
	if record.PredictionAvailableAt.After(record.LabelWindowStart) {
		return "prediction_created_after_label_entry"
	}
	if record.LabelWindowEnd.After(partitionCutoff) {
		return "label_window_crosses_partition_boundary"
	}
	if record.LabelMatureAt.IsZero() || record.LabelMatureAt.Before(record.LabelWindowEnd) {
		return "invalid_label_availability"
	}
	if record.LabelMatureAt.After(partitionCutoff) {
		return "label_not_mature_at_partition_cutoff"
	}
	return ""
}

func foldPartition(signal time.Time, fold Fold) (string, time.Time) {
	switch {
	case !signal.Before(fold.TrainStart) && signal.Before(fold.TrainCutoff):
		return "train", fold.TrainCutoff
	case !signal.Before(fold.CalibrationStart) && signal.Before(fold.CalibrationCutoff):
		return "calibration", fold.CalibrationCutoff
	case !signal.Before(fold.TestStart) && signal.Before(fold.TestCutoff):
		return "test", fold.TestCutoff
	default:
		return "", time.Time{}
	}
}

func groupSafeMembers(values []SplitMember, developmentClusters map[string]bool) ([]SplitMember, []SplitMember) {
	sortSplitMembers(values)
	included, excluded := []SplitMember{}, []SplitMember{}
	assigned := map[string]string{}
	seen := map[string]bool{}
	for _, member := range values {
		if developmentClusters != nil && developmentClusters[member.EventCluster] {
			member.Partition, member.ExclusionReason = "excluded", "event_cluster_seen_in_development"
			excluded = append(excluded, member)
			continue
		}
		key := member.EventCluster + "|" + member.Partition
		if seen[key] {
			member.Partition, member.ExclusionReason = "excluded", "duplicate_event_cluster_within_partition"
			excluded = append(excluded, member)
			continue
		}
		if prior, exists := assigned[member.EventCluster]; exists && prior != member.Partition {
			member.Partition, member.ExclusionReason = "excluded", "event_cluster_cross_partition"
			excluded = append(excluded, member)
			continue
		}
		assigned[member.EventCluster], seen[key] = member.Partition, true
		included = append(included, member)
	}
	return included, excluded
}

func splitMember(record Record, cluster, partition string) SplitMember {
	intended := partition
	if intended == "" {
		intended = "embargo"
	}
	return SplitMember{PredictionRunID: record.ID, Partition: partition, IntendedPartition: intended, EventCluster: cluster, SignalAt: record.SignalAt.UTC(), PredictionAvailableAt: record.PredictionAvailableAt.UTC(),
		FeatureCutoffAt: record.FeatureCutoffAt.UTC(), LabelWindowStart: record.LabelWindowStart.UTC(), LabelWindowEnd: record.LabelWindowEnd.UTC(), LabelMatureAt: record.LabelMatureAt.UTC()}
}

func clusterComponents(records []Record) map[int]string {
	parent := make([]int, len(records))
	for index := range parent {
		parent[index] = index
	}
	var find func(int) int
	find = func(value int) int {
		if parent[value] != value {
			parent[value] = find(parent[value])
		}
		return parent[value]
	}
	union := func(left, right int) {
		left, right = find(left), find(right)
		if left != right {
			if left > right {
				left, right = right, left
			}
			parent[right] = left
		}
	}
	owners := map[string]int{}
	for index, record := range records {
		keys := append([]string{}, record.ClusterKeys...)
		if value := strings.TrimSpace(record.EventCluster); value != "" {
			keys = append(keys, value)
		}
		if len(keys) == 0 {
			keys = append(keys, "id:"+strings.TrimSpace(record.ID))
		}
		for _, raw := range keys {
			key := strings.TrimSpace(raw)
			if key == "" {
				continue
			}
			if owner, exists := owners[key]; exists {
				union(index, owner)
			} else {
				owners[key] = index
			}
		}
	}
	members := map[int][]string{}
	for index, record := range records {
		root := find(index)
		members[root] = append(members[root], strings.TrimSpace(record.ID))
	}
	identities := map[int]string{}
	for root, ids := range members {
		sort.Strings(ids)
		identities[root] = "cluster-" + digestValue(ids)[:24]
	}
	result := map[int]string{}
	for index := range records {
		result[index] = identities[find(index)]
	}
	return result
}

func datasetDigest(manifest DatasetManifest) string {
	type digestFold struct {
		Index    int           `json:"index"`
		Members  []SplitMember `json:"members"`
		Excluded []SplitMember `json:"excluded"`
	}
	folds := make([]digestFold, 0, len(manifest.Folds))
	for _, fold := range manifest.Folds {
		folds = append(folds, digestFold{fold.Index, fold.Members, fold.Excluded})
	}
	return digestValue(struct {
		Contract      string            `json:"contract"`
		LabelContract string            `json:"label_contract"`
		Config        WalkForwardConfig `json:"config"`
		Folds         []digestFold      `json:"folds"`
		FinalMembers  []SplitMember     `json:"final_members"`
		FinalExcluded []SplitMember     `json:"final_excluded"`
	}{manifest.ContractVersion, manifest.LabelDefinitionVersion, manifest.Config, folds, manifest.finalMembers, manifest.finalExcluded})
}

func digestValue(value any) string {
	body, _ := json.Marshal(value)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func sortRecords(values []Record) {
	sort.Slice(values, func(i, j int) bool {
		if !values[i].SignalAt.Equal(values[j].SignalAt) {
			return values[i].SignalAt.Before(values[j].SignalAt)
		}
		return values[i].ID < values[j].ID
	})
}

func sortSplitMembers(values []SplitMember) {
	rank := map[string]int{"train": 0, "calibration": 1, "test": 2, "final_holdout": 3, "excluded": 4}
	sort.Slice(values, func(i, j int) bool {
		if rank[values[i].Partition] != rank[values[j].Partition] {
			return rank[values[i].Partition] < rank[values[j].Partition]
		}
		if !values[i].SignalAt.Equal(values[j].SignalAt) {
			return values[i].SignalAt.Before(values[j].SignalAt)
		}
		return values[i].PredictionRunID < values[j].PredictionRunID
	})
}

func exactDays(value time.Duration) (int, error) {
	if value <= 0 || value%(24*time.Hour) != 0 {
		return 0, fmt.Errorf("walk-forward windows must be positive whole days")
	}
	return int(value / (24 * time.Hour)), nil
}

func nonNegativeExactDays(value time.Duration) (int, error) {
	if value < 0 || value%(24*time.Hour) != 0 {
		return 0, fmt.Errorf("embargo must be non-negative whole days")
	}
	return int(value / (24 * time.Hour)), nil
}
