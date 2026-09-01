package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type frozenEventDataset struct {
	Version             int                `json:"version"`
	Dataset             string             `json:"dataset"`
	Minimums            map[string]float64 `json:"minimums"`
	WalkForwardMinimums map[string]float64 `json:"walk_forward_minimums"`
	Assets              []frozenAsset      `json:"assets"`
	Cases               []frozenEventCase  `json:"cases"`
}

type frozenAsset struct {
	ID      string   `json:"asset_id"`
	Symbol  string   `json:"symbol"`
	Name    string   `json:"name"`
	Aliases []string `json:"aliases"`
	Issuer  string   `json:"-"`
}

type frozenEventCase struct {
	ID                string   `json:"id"`
	Title             string   `json:"title"`
	Summary           string   `json:"summary"`
	Symbols           []string `json:"symbols"`
	ExpectedEventType string   `json:"expected_event_type"`
	ExpectedAssets    []string `json:"expected_assets"`
	ClusterID         string   `json:"cluster_id"`
	AsOf              string   `json:"as_of"`
}

type frozenPredictionDataset struct {
	Version  int                    `json:"version"`
	Dataset  string                 `json:"dataset"`
	Minimums map[string]float64     `json:"minimums"`
	Maximums map[string]float64     `json:"maximums"`
	Cases    []frozenPredictionCase `json:"cases"`
}

type frozenPredictionCase struct {
	ID                 string  `json:"id"`
	BullProbability    float64 `json:"bull_probability"`
	BaseProbability    float64 `json:"base_probability"`
	BearProbability    float64 `json:"bear_probability"`
	Direction          int     `json:"direction"`
	Magnitude          float64 `json:"magnitude"`
	Persistence        float64 `json:"persistence"`
	Representativeness float64 `json:"representativeness"`
	MarketConfirmation float64 `json:"market_confirmation"`
	Score              int     `json:"score"`
	Actual             string  `json:"actual"`
}

type evaluatedFrozenEvent struct {
	record    frozenEventCase
	eventType string
	assets    map[string]bool
	issuer    string
}

// RunOfflineEvaluation executes the frozen, network-free quality gates used by
// the Go evolution worker. All suites read versioned fixtures from root/evals.
func RunOfflineEvaluation(root, suite, baselinePath, candidatePath string) (map[string]any, error) {
	switch suite {
	case "fixed-evidence":
		return frozenEvidenceEvaluation(root)
	case "walk-forward":
		return frozenWalkForwardEvaluation(root)
	case "probability-calibration":
		return frozenProbabilityEvaluation(root)
	case "compare-models":
		baseline := map[string]any{}
		candidate := map[string]any{}
		if err := readEvaluationJSON(resolveEvaluationPath(root, baselinePath), &baseline); err != nil {
			return nil, err
		}
		if err := readEvaluationJSON(resolveEvaluationPath(root, candidatePath), &candidate); err != nil {
			return nil, err
		}
		return compareEvaluationMetrics(baseline, candidate, .01), nil
	default:
		return nil, fmt.Errorf("unknown evaluation suite: %s", suite)
	}
}

func WriteEvaluationResult(path string, result map[string]any) error {
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return os.WriteFile(path, body, 0o644)
}

func EvaluationPassed(result map[string]any) bool {
	passed, exists := result["passed"]
	return !exists || boolValue(passed)
}

func frozenEvidenceEvaluation(root string) (map[string]any, error) {
	dataset, err := loadFrozenEventDataset(root)
	if err != nil {
		return nil, err
	}
	stage, err := frozenStageMetrics(dataset, dataset.Cases)
	if err != nil {
		return nil, err
	}
	calibration, err := frozenProbabilityEvaluation(root)
	if err != nil {
		return nil, err
	}
	stageComposite := numberValue(stage["composite_score"])
	delete(stage, "composite_score")
	calibrationComposite := (math.Max(0, numberValue(calibration["brier_skill"])) +
		(1 - numberValue(calibration["expected_calibration_error"])) +
		numberValue(calibration["top_label_accuracy"]) +
		numberValue(calibration["score_probability_consistency"])) / 4
	result := map[string]any{
		"version": dataset.Version, "dataset": dataset.Dataset,
		"stage_composite_score":         roundEvaluation(stageComposite),
		"calibration_samples":           int(numberValue(calibration["samples"])),
		"brier_score":                   numberValue(calibration["brier_score"]),
		"reference_brier_score":         numberValue(calibration["reference_brier_score"]),
		"brier_skill":                   numberValue(calibration["brier_skill"]),
		"expected_calibration_error":    numberValue(calibration["expected_calibration_error"]),
		"top_label_accuracy":            numberValue(calibration["top_label_accuracy"]),
		"score_probability_consistency": numberValue(calibration["score_probability_consistency"]),
		"composite_score":               roundEvaluation(.8*stageComposite + .2*calibrationComposite),
	}
	for key, value := range stage {
		result[key] = value
	}
	result["passed"] = evaluationMeetsMinimums(result, dataset.Minimums) && EvaluationPassed(calibration)
	return result, nil
}

func frozenWalkForwardEvaluation(root string) (map[string]any, error) {
	dataset, err := loadFrozenEventDataset(root)
	if err != nil {
		return nil, err
	}
	ordered := append([]frozenEventCase{}, dataset.Cases...)
	sort.Slice(ordered, func(i, j int) bool {
		return parseEvaluationTime(ordered[i].AsOf).Before(parseEvaluationTime(ordered[j].AsOf))
	})
	split := 0
	if len(ordered) > 0 {
		split = max(1, int(float64(len(ordered))*.7))
	}
	train, heldOut := ordered[:split], ordered[split:]
	trainMetrics, err := frozenStageMetrics(dataset, train)
	if err != nil {
		return nil, err
	}
	heldOutMetrics, err := frozenStageMetrics(dataset, heldOut)
	if err != nil {
		return nil, err
	}
	chronological := len(train) == 0 || len(heldOut) == 0 || parseEvaluationTime(train[len(train)-1].AsOf).Before(parseEvaluationTime(heldOut[0].AsOf))
	minimums := dataset.WalkForwardMinimums
	if len(minimums) == 0 {
		minimums = dataset.Minimums
	}
	return map[string]any{
		"version": dataset.Version, "dataset": dataset.Dataset,
		"train_samples": len(train), "held_out_samples": len(heldOut),
		"chronological_split": chronological, "train_metrics": trainMetrics, "held_out_metrics": heldOutMetrics,
		"passed": chronological && len(heldOut) > 0 && evaluationMeetsMinimums(heldOutMetrics, minimums),
	}, nil
}

func frozenStageMetrics(dataset frozenEventDataset, records []frozenEventCase) (map[string]any, error) {
	assets := frozenEvaluationAssets(dataset.Assets)
	evaluated := make([]evaluatedFrozenEvent, 0, len(records))
	truePositive, falsePositive, falseNegative := 0, 0, 0
	exactAssetHits, eventTypeHits, temporalHits := 0, 0, 0
	for _, record := range records {
		asOf := parseEvaluationTime(record.AsOf)
		if asOf.IsZero() {
			return nil, fmt.Errorf("invalid as_of in frozen case %s", record.ID)
		}
		news := newsRecord{Title: record.Title, Summary: record.Summary, Symbols: record.Symbols, PublishedAt: asOf, ObservedAt: asOf, AsOf: asOf}
		eventType := fallbackExtraction(news).EventType
		actual, issuer := frozenMappedAssets(record, assets)
		expected := evaluationStringSet(record.ExpectedAssets)
		truePositive += setIntersectionSize(expected, actual)
		falsePositive += setDifferenceSize(actual, expected)
		falseNegative += setDifferenceSize(expected, actual)
		if equalStringSets(expected, actual) {
			exactAssetHits++
		}
		if eventType == record.ExpectedEventType {
			eventTypeHits++
		}
		if !asOf.IsZero() {
			temporalHits++
		}
		evaluated = append(evaluated, evaluatedFrozenEvent{record: record, eventType: eventType, assets: actual, issuer: issuer})
	}
	clusterTruePositive, clusterFalsePositive, clusterFalseNegative := 0, 0, 0
	for left := range evaluated {
		for right := left + 1; right < len(evaluated); right++ {
			expectedSame := evaluated[left].record.ClusterID != "" && evaluated[left].record.ClusterID == evaluated[right].record.ClusterID
			predictedSame := frozenSameStory(evaluated[left], evaluated[right])
			switch {
			case expectedSame && predictedSame:
				clusterTruePositive++
			case predictedSame:
				clusterFalsePositive++
			case expectedSame:
				clusterFalseNegative++
			}
		}
	}
	precision := safeEvaluationRatio(truePositive, truePositive+falsePositive, 1)
	recall := safeEvaluationRatio(truePositive, truePositive+falseNegative, 1)
	exactAssetAccuracy := safeEvaluationRatio(exactAssetHits, len(records), 0)
	eventAccuracy := safeEvaluationRatio(eventTypeHits, len(records), 0)
	temporalIntegrity := safeEvaluationRatio(temporalHits, len(records), 0)
	clusterPrecision := safeEvaluationRatio(clusterTruePositive, clusterTruePositive+clusterFalsePositive, 1)
	clusterRecall := safeEvaluationRatio(clusterTruePositive, clusterTruePositive+clusterFalseNegative, 1)
	clusterF1 := 0.0
	if clusterPrecision+clusterRecall > 0 {
		clusterF1 = 2 * clusterPrecision * clusterRecall / (clusterPrecision + clusterRecall)
	}
	composite := .25*precision + .20*recall + .15*exactAssetAccuracy + .15*eventAccuracy + .15*clusterF1 + .10*temporalIntegrity
	return map[string]any{
		"samples": len(records), "mapping_precision": roundEvaluation(precision), "mapping_recall": roundEvaluation(recall),
		"exact_asset_accuracy": roundEvaluation(exactAssetAccuracy), "event_type_accuracy": roundEvaluation(eventAccuracy),
		"cluster_precision": roundEvaluation(clusterPrecision), "cluster_recall": roundEvaluation(clusterRecall),
		"temporal_integrity": roundEvaluation(temporalIntegrity), "composite_score": roundEvaluation(composite),
	}, nil
}

func frozenProbabilityEvaluation(root string) (map[string]any, error) {
	dataset := frozenPredictionDataset{}
	if err := readEvaluationJSON(filepath.Join(root, "evals", "golden_predictions.json"), &dataset); err != nil {
		return nil, err
	}
	if len(dataset.Cases) == 0 {
		return nil, errors.New("frozen probability evaluation requires at least one case")
	}
	labels := []string{"bull", "base", "bear"}
	actualCounts := []int{0, 0, 0}
	type forecast struct {
		probabilities [3]float64
		actual        int
		confidence    float64
		correct       bool
	}
	forecasts := make([]forecast, 0, len(dataset.Cases))
	consistent := 0
	for _, record := range dataset.Cases {
		score := frozenShortTermScore(record)
		probabilities := probabilitiesForEvaluation(score, record.BaseProbability)
		actual := -1
		for index, label := range labels {
			if record.Actual == label {
				actual = index
				break
			}
		}
		if actual < 0 {
			return nil, fmt.Errorf("invalid actual label in frozen case %s", record.ID)
		}
		actualCounts[actual]++
		predicted := 0
		for index := 1; index < len(probabilities); index++ {
			if probabilities[index] > probabilities[predicted] {
				predicted = index
			}
		}
		forecasts = append(forecasts, forecast{probabilities: probabilities, actual: actual, confidence: probabilities[predicted], correct: predicted == actual})
		implied := int(math.Round(100 * (probabilities[0] - probabilities[2])))
		if absInt(record.Score-score) <= 5 && absInt(score-implied) <= 1 {
			consistent++
		}
	}
	samples := float64(len(forecasts))
	brier := 0.0
	for _, item := range forecasts {
		for index, probability := range item.probabilities {
			target := 0.0
			if index == item.actual {
				target = 1
			}
			brier += math.Pow(probability-target, 2) / 3
		}
	}
	brier /= samples
	frequencies := [3]float64{float64(actualCounts[0]) / samples, float64(actualCounts[1]) / samples, float64(actualCounts[2]) / samples}
	reference := 0.0
	for _, item := range forecasts {
		for index, frequency := range frequencies {
			target := 0.0
			if index == item.actual {
				target = 1
			}
			reference += math.Pow(frequency-target, 2) / 3
		}
	}
	reference /= samples
	skill := 0.0
	if reference > 0 {
		skill = 1 - brier/reference
	}
	type calibrationBin struct {
		confidence, correct float64
		count               int
	}
	bins := map[int]calibrationBin{}
	correct := 0
	for _, item := range forecasts {
		bucket := min(9, int(item.confidence*10))
		bin := bins[bucket]
		bin.confidence += item.confidence
		if item.correct {
			bin.correct++
			correct++
		}
		bin.count++
		bins[bucket] = bin
	}
	ece := 0.0
	for _, bin := range bins {
		ece += float64(bin.count) / samples * math.Abs(bin.correct/float64(bin.count)-bin.confidence/float64(bin.count))
	}
	result := map[string]any{
		"version": dataset.Version, "dataset": dataset.Dataset, "samples": len(forecasts),
		"brier_score": roundEvaluation(brier), "reference_brier_score": roundEvaluation(reference),
		"brier_skill": roundEvaluation(skill), "expected_calibration_error": roundEvaluation(ece),
		"top_label_accuracy":            roundEvaluation(float64(correct) / samples),
		"score_probability_consistency": roundEvaluation(float64(consistent) / samples),
	}
	passed := evaluationMeetsMinimums(result, dataset.Minimums)
	for name, maximum := range dataset.Maximums {
		value, ok := numericMetric(result[name])
		passed = passed && ok && value <= maximum
	}
	result["passed"] = passed
	return result, nil
}

func compareEvaluationMetrics(baseline, candidate map[string]any, tolerance float64) map[string]any {
	ignored := map[string]bool{"version": true, "samples": true, "passed": true}
	lowerIsBetter := map[string]bool{"brier_score": true, "expected_calibration_error": true}
	keys := make([]string, 0)
	for key, raw := range baseline {
		if ignored[key] {
			continue
		}
		if _, ok := numericMetric(raw); !ok {
			continue
		}
		if _, ok := numericMetric(candidate[key]); ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	deltas := map[string]float64{}
	regressions := make([]string, 0)
	for _, key := range keys {
		left, _ := numericMetric(baseline[key])
		right, _ := numericMetric(candidate[key])
		delta := right - left
		deltas[key] = roundEvaluation(delta)
		if lowerIsBetter[key] && delta > tolerance || !lowerIsBetter[key] && delta < -tolerance {
			regressions = append(regressions, key)
		}
	}
	candidatePassed := true
	if value, exists := candidate["passed"]; exists {
		candidatePassed = boolValue(value)
	}
	return map[string]any{
		"baseline_dataset": stringValue(baseline["dataset"]), "candidate_dataset": stringValue(candidate["dataset"]),
		"tolerance": tolerance, "metrics_compared": len(keys), "deltas": deltas, "regressions": regressions,
		"passed": len(keys) > 0 && len(regressions) == 0 && candidatePassed,
	}
}

func loadFrozenEventDataset(root string) (frozenEventDataset, error) {
	dataset := frozenEventDataset{}
	err := readEvaluationJSON(filepath.Join(root, "evals", "golden_events.json"), &dataset)
	return dataset, err
}

func frozenEvaluationAssets(values []frozenAsset) []frozenAsset {
	result := append([]frozenAsset{
		{ID: "equity:XNAS:AAPL", Symbol: "AAPL", Name: "Apple Inc", Aliases: []string{"Apple"}, Issuer: "apple"},
		{ID: "crypto:coingecko:ethereum", Symbol: "ETH", Name: "Ethereum", Aliases: []string{"Ether"}, Issuer: "ethereum"},
	}, values...)
	for index := range result {
		if result[index].Issuer == "" {
			result[index].Issuer = result[index].ID
		}
		if result[index].ID == "equity:OTC:MOPHY" || result[index].ID == "equity:XASX:MND" {
			result[index].Issuer = "monadelphous"
		}
	}
	return result
}

func frozenMappedAssets(record frozenEventCase, assets []frozenAsset) (map[string]bool, string) {
	text := record.Title + "\n" + record.Summary + "\n" + strings.Join(record.Symbols, "\n")
	matchedIssuers := map[string]bool{}
	result := map[string]bool{}
	for _, asset := range assets {
		direct := containsStringFold(record.Symbols, asset.Symbol) || explicitSymbol(text, asset.Symbol, false)
		issuerMatch := meaningfulIssuerTerm(asset.Name) && explicitTerm(text, asset.Name)
		if !issuerMatch {
			for _, alias := range asset.Aliases {
				if meaningfulTerm(alias) && explicitTerm(text, alias) {
					issuerMatch = true
					break
				}
			}
		}
		if direct || issuerMatch {
			result[asset.ID] = true
			matchedIssuers[asset.Issuer] = true
		}
	}
	for _, asset := range assets {
		if matchedIssuers[asset.Issuer] {
			result[asset.ID] = true
		}
	}
	issuer := ""
	for _, asset := range assets {
		if result[asset.ID] {
			issuer = asset.Issuer
			break
		}
	}
	return result, issuer
}

func frozenSameStory(left, right evaluatedFrozenEvent) bool {
	if left.eventType != right.eventType {
		return false
	}
	similarity := diceSimilarity(left.record.Title, right.record.Title)
	if left.issuer != "" && right.issuer != "" {
		return left.issuer == right.issuer && similarity >= .58
	}
	if (left.issuer == "") != (right.issuer == "") {
		return false
	}
	return similarity >= .92
}

func frozenShortTermScore(record frozenPredictionCase) int {
	unit := func(value float64) float64 { return math.Max(0, math.Min(1, value)) }
	total := float64(record.Direction) * (45*unit(record.Magnitude) + 25*unit(record.Persistence) + 15*unit(record.Representativeness) + 15*unit(record.MarketConfirmation))
	if total >= 0 {
		return min(100, int(math.Floor(total+.5)))
	}
	return max(-100, int(math.Ceil(total-.5)))
}

func probabilitiesForEvaluation(score int, baseProbability float64) [3]float64 {
	edge := math.Max(-1, math.Min(1, float64(score)/100))
	base := math.Max(0, math.Min(baseProbability, 1-math.Abs(edge)))
	mass := 1 - base
	bull := roundEvaluationPrecision((mass+edge)/2, 8)
	bear := roundEvaluationPrecision((mass-edge)/2, 8)
	base = roundEvaluationPrecision(1-bull-bear, 8)
	return [3]float64{bull, base, bear}
}

func evaluationMeetsMinimums(metrics map[string]any, minimums map[string]float64) bool {
	for name, minimum := range minimums {
		value, ok := numericMetric(metrics[name])
		if !ok || value < minimum {
			return false
		}
	}
	return true
}

func readEvaluationJSON(path string, target any) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func resolveEvaluationPath(root, value string) string {
	if filepath.IsAbs(value) {
		return value
	}
	return filepath.Join(root, filepath.FromSlash(value))
}

func parseEvaluationTime(value string) time.Time {
	result, _ := time.Parse(time.RFC3339, value)
	return result.UTC()
}

func safeEvaluationRatio(numerator, denominator int, empty float64) float64 {
	if denominator == 0 {
		return empty
	}
	return float64(numerator) / float64(denominator)
}

func roundEvaluation(value float64) float64 { return roundEvaluationPrecision(value, 6) }
func roundEvaluationPrecision(value float64, precision int) float64 {
	factor := math.Pow10(precision)
	return math.Round(value*factor) / factor
}

func evaluationStringSet(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

func setIntersectionSize(left, right map[string]bool) int {
	result := 0
	for value := range left {
		if right[value] {
			result++
		}
	}
	return result
}

func setDifferenceSize(left, right map[string]bool) int {
	return len(left) - setIntersectionSize(left, right)
}
func equalStringSets(left, right map[string]bool) bool {
	return len(left) == len(right) && setIntersectionSize(left, right) == len(left)
}
