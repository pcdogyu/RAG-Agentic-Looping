package httpapi

import (
	"encoding/json"
	"math"
	"regexp"
	"strings"
)

var publishedActivityTargets = []string{
	"成交量", "交易量", "市场活跃", "交易活跃", "交易者参与", "投资者参与",
	"tradingvolume", "marketactivity", "tradingactivity", "traderparticipation",
	"investorparticipation", "retailparticipation",
}

var publishedAssetWords = regexp.MustCompile(`[A-Za-z0-9]+|[\p{Han}]{2,}`)

type publishedAssetTarget struct {
	asset  map[string]any
	name   string
	tokens []string
}

// sanitizePublishedImpacts fixes legacy model taxonomy mistakes at the public
// read boundary. Stored reports remain untouched for audit and reprocessing.
func sanitizePublishedImpacts(value any) []any {
	rawImpacts := anySlice(value)
	assets := publishedAssets(rawImpacts)
	result := make([]any, 0, len(rawImpacts))
	indexes := make(map[string]int)

	for _, raw := range rawImpacts {
		impact := deepCloneObject(objectValue(raw))
		if impact == nil {
			continue
		}
		normalizeTargetImpact(impact)
		name := stringValue(impact["target_name"])
		if publishedActivityTarget(name) {
			continue
		}
		if stringValue(impact["target_type"]) != "tradable_asset" {
			if matched := matchPublishedAsset(name, assets); matched != nil {
				impact["target_type"] = "tradable_asset"
				impact["asset"] = matched.asset
				impact["target_name"] = matched.name
			}
		}

		key := publishedImpactKey(impact)
		if index, found := indexes[key]; found {
			existing := objectValue(result[index])
			if preferPublishedImpact(impact, existing) {
				result[index] = impact
			}
			continue
		}
		indexes[key] = len(result)
		result = append(result, impact)
	}
	return result
}

func publishedAssets(impacts []any) []publishedAssetTarget {
	result := make([]publishedAssetTarget, 0)
	seen := make(map[string]bool)
	for _, raw := range impacts {
		asset := objectValue(objectValue(raw)["asset"])
		if asset == nil || !securityAsset(asset) {
			continue
		}
		key := fallbackString(stringValue(asset["asset_id"]), strings.ToLower(stringValue(asset["symbol"])))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		name := fallbackString(stringValue(asset["name"]), stringValue(objectValue(raw)["target_name"]))
		tokens := []string{normalizedTarget(stringValue(asset["symbol"])), normalizedTarget(name)}
		for _, alias := range anySlice(asset["aliases"]) {
			tokens = append(tokens, normalizedTarget(stringValue(alias)))
		}
		for _, word := range publishedAssetWords.FindAllString(name, -1) {
			word = normalizedTarget(word)
			if len([]rune(word)) >= 3 {
				tokens = append(tokens, word)
			}
		}
		result = append(result, publishedAssetTarget{asset: deepCloneObject(asset), name: name, tokens: tokens})
	}
	return result
}

func publishedActivityTarget(name string) bool {
	compact := normalizedTarget(name)
	for _, phrase := range publishedActivityTargets {
		if strings.Contains(compact, normalizedTarget(phrase)) {
			return true
		}
	}
	return false
}

func matchPublishedAsset(name string, assets []publishedAssetTarget) *publishedAssetTarget {
	compact := normalizedTarget(name)
	var matched *publishedAssetTarget
	for index := range assets {
		found := false
		for _, token := range assets[index].tokens {
			if token != "" && strings.Contains(compact, token) {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		if matched != nil {
			return nil
		}
		matched = &assets[index]
	}
	return matched
}

func publishedImpactKey(impact map[string]any) string {
	if asset := objectValue(impact["asset"]); asset != nil {
		if id := stringValue(asset["asset_id"]); id != "" {
			return "asset:" + strings.ToLower(id)
		}
		if symbol := stringValue(asset["symbol"]); symbol != "" {
			return "symbol:" + strings.ToLower(symbol)
		}
	}
	return stringValue(impact["target_type"]) + ":" + normalizedTarget(stringValue(impact["target_name"]))
}

func preferPublishedImpact(candidate, existing map[string]any) bool {
	candidateEvidence := len(anySlice(candidate["evidence_ids"]))
	existingEvidence := len(anySlice(existing["evidence_ids"]))
	if (candidateEvidence > 0) != (existingEvidence > 0) {
		return candidateEvidence > 0
	}
	candidateStrength := math.Abs(numberValue(candidate["direction_score"]))
	existingStrength := math.Abs(numberValue(existing["direction_score"]))
	if candidateStrength != existingStrength {
		return candidateStrength > existingStrength
	}
	return numberValue(candidate["rating_confidence"]) > numberValue(existing["rating_confidence"])
}

func deepCloneObject(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	body, err := json.Marshal(source)
	if err != nil {
		return cloneMap(source)
	}
	var result map[string]any
	if json.Unmarshal(body, &result) != nil {
		return cloneMap(source)
	}
	return result
}
