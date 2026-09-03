package httpapi

import (
	"encoding/json"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type recommendationSnapshot struct {
	ID      string
	AssetID string
	AsOf    time.Time
	Payload map[string]any
}

type recommendationChange struct {
	Previous recommendationSnapshot
	Current  recommendationSnapshot
	Latest   recommendationSnapshot
}

type currentRecommendation struct {
	Previous *recommendationSnapshot
	Latest   recommendationSnapshot
}

func (s *Server) changedTargets(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 100)
	if !ok {
		return
	}
	changes, err := s.latestChangedTargets(r)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "changed target query failed")
		return
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		stamp, id, decodeErr := decodeAssetCursor(raw)
		if decodeErr != nil {
			validationError(w, "cursor", "Input should be a valid cursor")
			return
		}
		filtered := changes[:0]
		for _, item := range changes {
			if item.Current.AsOf.Before(stamp) || (item.Current.AsOf.Equal(stamp) && item.Current.ID < id) {
				filtered = append(filtered, item)
			}
		}
		changes = filtered
	}
	hasMore := len(changes) > limit
	if len(changes) > limit {
		changes = changes[:limit]
	}
	items := make([]any, 0, len(changes))
	for _, item := range changes {
		normalizeRecommendation(item.Previous.Payload)
		normalizeRecommendation(item.Current.Payload)
		normalizeRecommendation(item.Latest.Payload)
		asset, _ := item.Current.Payload["asset"].(map[string]any)
		items = append(items, map[string]any{
			"asset": asset, "recommendation_id": item.Current.ID,
			"latest_recommendation_id": item.Latest.ID, "latest_researched_at": jsonTime(item.Latest.AsOf),
			"changed_at":     jsonTime(item.Current.AsOf),
			"previous":       map[string]any{"signal_status": item.Previous.Payload["signal_status"], "rating": item.Previous.Payload["rating"]},
			"current":        map[string]any{"signal_status": item.Current.Payload["signal_status"], "rating": item.Current.Payload["rating"]},
			"status_changed": stringValue(item.Previous.Payload["signal_status"]) != stringValue(item.Current.Payload["signal_status"]),
			"rating_changed": stringValue(item.Previous.Payload["rating"]) != stringValue(item.Current.Payload["rating"]),
		})
	}
	var next any
	if hasMore && len(changes) > 0 {
		last := changes[len(changes)-1].Current
		next = encodeAssetCursor(last.AsOf, last.ID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s *Server) latestChangedTargets(r *http.Request) ([]recommendationChange, error) {
	rows, err := s.db.Query(r.Context(), `SELECT id,asset_id,as_of,payload::jsonb FROM recommendations ORDER BY asset_id,as_of,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	previous := map[string]recommendationSnapshot{}
	latest := map[string]recommendationSnapshot{}
	changed := map[string][2]recommendationSnapshot{}
	for rows.Next() {
		var item recommendationSnapshot
		var body []byte
		if err = rows.Scan(&item.ID, &item.AssetID, &item.AsOf, &body); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(body, &item.Payload); err != nil {
			return nil, err
		}
		if prior, found := previous[item.AssetID]; found && stringValue(prior.Payload["rating"]) != stringValue(item.Payload["rating"]) {
			changed[item.AssetID] = [2]recommendationSnapshot{prior, item}
		}
		previous[item.AssetID], latest[item.AssetID] = item, item
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	result := make([]recommendationChange, 0, len(changed))
	for assetID, pair := range changed {
		result = append(result, recommendationChange{Previous: pair[0], Current: pair[1], Latest: latest[assetID]})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Current.AsOf.Equal(result[j].Current.AsOf) {
			return result[i].Current.ID > result[j].Current.ID
		}
		return result[i].Current.AsOf.After(result[j].Current.AsOf)
	})
	return result, nil
}

func (s *Server) currentAssetRatings(r *http.Request) ([]map[string]any, error) {
	rows, err := s.db.Query(r.Context(), `SELECT rec.id,rec.asset_id,rec.as_of,rec.payload::jsonb
		FROM recommendations rec JOIN assets a ON a.id=rec.asset_id
		WHERE a.active=true AND a.market IN ('CN','HK','US','CRYPTO')
		ORDER BY rec.asset_id,rec.as_of,rec.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	current := map[string]currentRecommendation{}
	for rows.Next() {
		var item recommendationSnapshot
		var body []byte
		if err = rows.Scan(&item.ID, &item.AssetID, &item.AsOf, &body); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(body, &item.Payload); err != nil {
			return nil, err
		}
		entry := current[item.AssetID]
		if entry.Latest.ID != "" {
			prior := entry.Latest
			entry.Previous = &prior
		}
		entry.Latest = item
		current[item.AssetID] = entry
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	market := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("market")))
	rating := strings.TrimSpace(r.URL.Query().Get("rating"))
	query := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	items := make([]map[string]any, 0, len(current))
	for _, entry := range current {
		normalizeRecommendation(entry.Latest.Payload)
		asset := objectValue(entry.Latest.Payload["asset"])
		if asset == nil || (market != "" && strings.ToUpper(stringValue(asset["market"])) != market) || (rating != "" && stringValue(entry.Latest.Payload["rating"]) != rating) {
			continue
		}
		if query != "" {
			parts := []string{stringValue(asset["symbol"]), stringValue(asset["name"])}
			for _, alias := range anySlice(asset["aliases"]) {
				parts = append(parts, stringValue(alias))
			}
			if !strings.Contains(strings.ToLower(strings.Join(parts, " ")), query) {
				continue
			}
		}
		var previous any
		changeState := "first"
		if entry.Previous != nil {
			normalizeRecommendation(entry.Previous.Payload)
			previous = impactFields(entry.Previous.Payload)
			changeState = "unchanged"
			if stringValue(entry.Previous.Payload["rating"]) != stringValue(entry.Latest.Payload["rating"]) {
				changeState = "changed"
			}
		}
		items = append(items, map[string]any{
			"kind": "asset", "key": entry.Latest.AssetID, "label": asset["name"], "symbol": asset["symbol"], "market": asset["market"],
			"target_type": "tradable_asset", "rated_at": jsonTime(entry.Latest.AsOf), "changed_at": jsonTime(entry.Latest.AsOf),
			"previous": previous, "current": impactFields(entry.Latest.Payload), "change_state": changeState,
			"latest": map[string]any{"rating": entry.Latest.Payload["rating"], "direction_score": entry.Latest.Payload["direction_score"],
				"rating_confidence": entry.Latest.Payload["rating_confidence"], "news_confidence": entry.Latest.Payload["news_confidence"]},
			"latest_detail":    map[string]any{"kind": "asset", "id": entry.Latest.ID, "researched_at": jsonTime(entry.Latest.AsOf)},
			"change_detail_id": entry.Latest.ID,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		left, right := parseAnyTime(items[i]["rated_at"]), parseAnyTime(items[j]["rated_at"])
		if left != nil && right != nil && left.Equal(*right) {
			return stringValue(items[i]["change_detail_id"]) > stringValue(items[j]["change_detail_id"])
		}
		return left != nil && (right == nil || left.After(*right))
	})
	return items, nil
}

type targetObservation struct {
	OccurredAt             time.Time
	Score                  float64
	RatingConfidence       float64
	NewsConfidence         float64
	Persistence            float64
	RealizationProbability float64
	Insufficient           bool
	Provisional            bool
}

type trendScore struct {
	Score         float64
	Rating        string
	Confidence    float64
	Provisional   bool
	EventCount    int
	EligibleCount int
	IgnoredCount  int
	RegimeBreak   bool
}

type targetTrend struct {
	Short, Long, Combined trendScore
}

type canonicalTarget struct {
	Key, Label, TargetType string
}

type macroSnapshot struct {
	Canonical   canonicalTarget
	Impact      map[string]any
	Asset       map[string]any
	Run         map[string]any
	RunID       string
	ChangedAt   time.Time
	Observation targetObservation
	Provisional bool
}

var nonTargetCharacters = regexp.MustCompile(`[^a-z0-9\p{Han}]+`)
var targetWords = regexp.MustCompile(`[A-Za-z0-9.\-]+`)
var parentheticalTicker = regexp.MustCompile(`\([^)]*([A-Za-z]{1,8}|[0-9]{4,8})[^)]*\)`)

var macroTargetTypes = map[string]bool{
	"economy": true, "supply_volume": true, "fx_rate": true,
	"interest_rate": true, "sector": true, "risk_asset": true, "shipping": true, "other": true,
}

func (s *Server) targetChanges(w http.ResponseWriter, r *http.Request) {
	kind := r.URL.Query().Get("kind")
	if kind != "macro" && kind != "asset" {
		validationError(w, "kind", "Input should be 'macro' or 'asset'")
		return
	}
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 100)
	if !ok {
		return
	}
	scope := r.URL.Query().Get("scope")
	if scope == "" {
		scope = "changed"
	}
	if scope != "changed" && scope != "current" {
		validationError(w, "scope", "Input should be 'current' or 'changed'")
		return
	}
	if scope == "current" && kind != "asset" {
		validationError(w, "scope", "current scope is only available for asset targets")
		return
	}
	var items []map[string]any
	var err error
	if scope == "current" {
		items, err = s.currentAssetRatings(r)
	} else if kind == "macro" {
		items, err = s.macroTargetChanges(r)
	} else {
		items, err = s.concreteTargetChanges(r)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "target change query failed")
		return
	}
	if scope == "changed" {
		items = filterTargetChanges(items, r.URL.Query().Get("q"))
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		stamp, id, decodeErr := decodeAssetCursor(raw)
		if decodeErr != nil {
			validationError(w, "cursor", "Input should be a valid cursor")
			return
		}
		filtered := items[:0]
		for _, item := range items {
			stampField := "changed_at"
			if scope == "current" {
				stampField = "rated_at"
			}
			changedAt := parseAnyTime(item[stampField])
			itemID := stringValue(item["change_detail_id"])
			if changedAt != nil && (changedAt.Before(stamp) || (changedAt.Equal(stamp) && itemID < id)) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	hasMore := len(items) > limit
	if len(items) > limit {
		items = items[:limit]
	}
	var next any
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		stampField := "changed_at"
		if scope == "current" {
			stampField = "rated_at"
		}
		if stamp := parseAnyTime(last[stampField]); stamp != nil {
			next = encodeAssetCursor(*stamp, stringValue(last["change_detail_id"]))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func filterTargetChanges(items []map[string]any, query string) []map[string]any {
	needle := strings.ToLower(strings.TrimSpace(query))
	if needle == "" {
		return items
	}
	filtered := make([]map[string]any, 0, len(items))
	for _, item := range items {
		haystack := strings.ToLower(strings.Join([]string{
			stringValue(item["label"]),
			stringValue(item["symbol"]),
			stringValue(item["market"]),
			stringValue(item["target_type"]),
		}, " "))
		if strings.Contains(haystack, needle) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func (s *Server) assetTargetChanges(r *http.Request) ([]map[string]any, error) {
	changes, err := s.latestChangedTargets(r)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(changes))
	for _, item := range changes {
		normalizeRecommendation(item.Previous.Payload)
		normalizeRecommendation(item.Current.Payload)
		normalizeRecommendation(item.Latest.Payload)
		asset, _ := item.Current.Payload["asset"].(map[string]any)
		items = append(items, map[string]any{
			"kind": "asset", "key": item.Current.AssetID, "label": asset["name"], "symbol": asset["symbol"], "market": asset["market"],
			"target_type": "tradable_asset", "changed_at": jsonTime(item.Current.AsOf),
			"previous": impactFields(item.Previous.Payload), "current": impactFields(item.Current.Payload),
			"latest": map[string]any{"rating": item.Latest.Payload["rating"], "direction_score": item.Latest.Payload["direction_score"],
				"rating_confidence": item.Latest.Payload["rating_confidence"], "news_confidence": item.Latest.Payload["news_confidence"]},
			"latest_detail":    map[string]any{"kind": "asset", "id": item.Latest.ID, "researched_at": jsonTime(item.Latest.AsOf)},
			"change_detail_id": item.Current.ID,
		})
	}
	return items, nil
}

func impactFields(value map[string]any) map[string]any {
	return map[string]any{"rating": value["rating"], "direction_score": value["direction_score"], "rating_confidence": value["rating_confidence"]}
}

func (s *Server) macroTargetChanges(r *http.Request) ([]map[string]any, error) {
	return s.eventTargetChanges(r, macroTargetTypes, "macro", func(targetType string, isSecurity bool) bool {
		return !isSecurity
	})
}

var concreteEventTargetTypes = map[string]bool{"commodity_price": true, "tradable_asset": true}

func concreteEventTargetChange(targetType string, isSecurity bool) bool {
	return (targetType == "commodity_price" && !isSecurity) || (targetType == "tradable_asset" && isSecurity)
}

func (s *Server) concreteTargetChanges(r *http.Request) ([]map[string]any, error) {
	assetChanges, err := s.assetTargetChanges(r)
	if err != nil {
		return nil, err
	}
	eventChanges, err := s.eventTargetChanges(r, concreteEventTargetTypes, "asset", concreteEventTargetChange)
	if err != nil {
		return nil, err
	}
	return mergeConcreteTargetChanges(assetChanges, eventChanges), nil
}

func mergeConcreteTargetChanges(groups ...[]map[string]any) []map[string]any {
	latestByKey := map[string]map[string]any{}
	keyOrder := make([]string, 0)
	for _, group := range groups {
		for _, item := range group {
			key := stringValue(item["key"])
			current := latestByKey[key]
			if current == nil {
				keyOrder = append(keyOrder, key)
				latestByKey[key] = item
			} else if targetChangeAfter(item, current) {
				latestByKey[key] = item
			}
		}
	}
	result := make([]map[string]any, 0, len(latestByKey))
	for _, key := range keyOrder {
		result = append(result, latestByKey[key])
	}
	sort.SliceStable(result, func(i, j int) bool { return targetChangeAfter(result[i], result[j]) })
	return result
}

func targetChangeAfter(left, right map[string]any) bool {
	leftTime, rightTime := parseAnyTime(left["changed_at"]), parseAnyTime(right["changed_at"])
	if leftTime != nil && rightTime != nil && leftTime.Equal(*rightTime) {
		return stringValue(left["change_detail_id"]) > stringValue(right["change_detail_id"])
	}
	return leftTime != nil && (rightTime == nil || leftTime.After(*rightTime))
}

func (s *Server) eventTargetChanges(r *http.Request, targetTypes map[string]bool, kind string, include func(string, bool) bool) ([]map[string]any, error) {
	taxonomy, err := s.targetTaxonomy(r)
	if err != nil {
		return nil, err
	}
	masterAssets, err := s.activeSecurityAssets(r)
	if err != nil {
		return nil, err
	}
	securityResolver := newPublishedSecurityResolver(masterAssets)
	securityNames, securitySymbols := securityAssetAliases(masterAssets)
	rows, err := s.db.Query(r.Context(), `SELECT er.id,er.status,er.updated_at,er.payload::jsonb,e.published_at
		FROM event_research_runs er LEFT JOIN news_events e ON e.id=er.event_id ORDER BY er.updated_at,er.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type parsedRun struct {
		id, status string
		updated    time.Time
		published  *time.Time
		payload    map[string]any
	}
	runs := make([]parsedRun, 0)
	aliases := map[string]map[string]any{}
	for rows.Next() {
		var run parsedRun
		var body []byte
		if err = rows.Scan(&run.id, &run.status, &run.updated, &body, &run.published); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(body, &run.payload); err != nil {
			return nil, err
		}
		report := objectValue(run.payload["report"])
		if run.payload["retryable_reason"] != nil || report == nil || (!visibleEventStatus(run.status) && len(anySlice(run.payload["report_history"])) == 0) {
			continue
		}
		normalizeEventReport(report)
		report["impacts"] = securityResolver.resolve(report["impacts"])
		runs = append(runs, run)
		for _, raw := range anySlice(report["impacts"]) {
			impact := objectValue(raw)
			asset := objectValue(impact["asset"])
			targetType := stringValue(impact["target_type"])
			if targetTypes[targetType] && asset != nil && !securityAsset(asset) {
				aliases[targetType+"|"+macroTargetBase(stringValue(impact["target_name"]))] = asset
			}
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	snapshots := map[string][]macroSnapshot{}
	snapshotOrder := make([]string, 0)
	snapshotSeen := map[string]bool{}
	for _, run := range runs {
		report := objectValue(run.payload["report"])
		type impactGroup struct {
			canonical canonicalTarget
			impacts   []map[string]any
			asset     map[string]any
		}
		groups := map[string]*impactGroup{}
		groupOrder := make([]string, 0)
		for _, raw := range anySlice(report["impacts"]) {
			impact := objectValue(raw)
			if impact == nil {
				continue
			}
			// A generic noun or calendar year can coincide with a token in the
			// provider catalogue. It is not enough evidence to publish it as a
			// concrete tradable target.
			if genericPublishedSecurityTarget(stringValue(impact["target_name"])) {
				continue
			}
			targetType := stringValue(impact["target_type"])
			asset := objectValue(impact["asset"])
			isSecurity := securityAsset(asset) || resemblesSecurity(impact, securityNames, securitySymbols)
			if targetType == "economy" && unitedStatesEconomyAlias(stringValue(impact["target_name"])) {
				isSecurity = false
			}
			if !targetTypes[targetType] || !include(targetType, isSecurity) {
				continue
			}
			if asset == nil {
				asset = aliases[targetType+"|"+macroTargetBase(stringValue(impact["target_name"]))]
			}
			canonical := canonicalizeGoTarget(stringValue(impact["target_name"]), targetType, asset, taxonomy)
			group := groups[canonical.Key]
			if group == nil {
				group = &impactGroup{canonical: canonical, asset: asset}
				groups[canonical.Key] = group
				groupOrder = append(groupOrder, canonical.Key)
			}
			group.impacts = append(group.impacts, impact)
			if group.asset == nil {
				group.asset = asset
			}
		}
		occurred := parseAnyTime(run.payload["as_of"])
		if run.published != nil {
			occurred = run.published
		}
		if occurred == nil {
			fallback := run.updated
			occurred = &fallback
		}
		provisional := run.status == "insufficient_evidence" || !boolValue(report["evidence_complete"])
		newsConfidence := numberValue(report["news_confidence"])
		if newsConfidence == 0 {
			newsConfidence = numberValue(report["fact_confidence"])
		}
		for _, key := range groupOrder {
			group := groups[key]
			observation := canonicalObservation(group.impacts, *occurred, newsConfidence, provisional)
			if !snapshotSeen[key] {
				snapshotSeen[key] = true
				snapshotOrder = append(snapshotOrder, key)
			}
			snapshots[key] = append(snapshots[key], macroSnapshot{
				Canonical: group.canonical, Impact: representativeMacroImpact(group.impacts), Asset: group.asset,
				Run: run.payload, RunID: run.id, ChangedAt: run.updated, Observation: observation, Provisional: observation.Provisional,
			})
		}
	}
	output := make([]map[string]any, 0)
	now := time.Now().UTC()
	for _, key := range snapshotOrder {
		values := snapshots[key]
		sort.Slice(values, func(i, j int) bool {
			if values[i].ChangedAt.Equal(values[j].ChangedAt) {
				return values[i].RunID < values[j].RunID
			}
			return values[i].ChangedAt.Before(values[j].ChangedAt)
		})
		var prior *macroSnapshot
		var before, changed *macroSnapshot
		for index := range values {
			current := &values[index]
			if prior != nil && stringValue(prior.Impact["rating"]) != stringValue(current.Impact["rating"]) {
				before, changed = prior, current
			}
			prior = current
		}
		if changed == nil {
			continue
		}
		latest := &values[len(values)-1]
		displayAsset := latest.Asset
		if displayAsset == nil {
			displayAsset = objectValue(latest.Impact["asset"])
		}
		observations := make([]targetObservation, 0, len(values))
		for _, value := range values {
			observations = append(observations, value.Observation)
		}
		report := objectValue(latest.Run["report"])
		output = append(output, map[string]any{
			"kind": kind, "key": key, "label": latest.Canonical.Label, "symbol": valueOrNil(displayAsset, "symbol"),
			"market": valueOrNil(displayAsset, "market"), "target_type": latest.Canonical.TargetType, "changed_at": jsonTime(changed.ChangedAt),
			"previous": macroImpactState(before.Impact, before.Provisional), "current": macroImpactState(changed.Impact, changed.Provisional),
			"latest": map[string]any{"rating": latest.Impact["rating"], "direction_score": latest.Impact["direction_score"],
				"rating_confidence": latest.Impact["rating_confidence"], "provisional": latest.Provisional, "news_confidence": valueOrNil(report, "news_confidence")},
			"trend":            publicTargetTrend(aggregateTargetTrend(observations, now)),
			"latest_detail":    map[string]any{"kind": "event", "id": latest.RunID, "researched_at": jsonTime(latest.ChangedAt)},
			"change_detail_id": changed.RunID,
		})
	}
	sort.SliceStable(output, func(i, j int) bool { return targetChangeAfter(output[i], output[j]) })
	return output, nil
}

func (s *Server) targetTaxonomy(r *http.Request) (map[string]canonicalTarget, error) {
	rows, err := s.db.Query(r.Context(), `SELECT id,name_zh,name_en,aliases::jsonb FROM industries WHERE active=true ORDER BY level DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]canonicalTarget{}
	for rows.Next() {
		var id, zh, en string
		var body []byte
		if err = rows.Scan(&id, &zh, &en, &body); err != nil {
			return nil, err
		}
		var aliases []any
		_ = json.Unmarshal(body, &aliases)
		terms := append([]any{zh, en}, aliases...)
		for _, term := range terms {
			key := compactTarget(stringValue(term))
			if key != "" {
				if _, exists := result[key]; !exists {
					result[key] = canonicalTarget{Key: id, Label: zh, TargetType: "sector"}
				}
			}
		}
	}
	return result, rows.Err()
}

func (s *Server) activeSecurityAssets(r *http.Request) ([]map[string]any, error) {
	rows, err := s.db.Query(r.Context(), `SELECT `+assetJSON+` FROM assets
		WHERE active=true AND asset_class IN ('equity','crypto') ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	assets := make([]map[string]any, 0)
	for rows.Next() {
		var body []byte
		if err = rows.Scan(&body); err != nil {
			return nil, err
		}
		var asset map[string]any
		if json.Unmarshal(body, &asset) != nil {
			continue
		}
		normalizeAsset(asset)
		assets = append(assets, asset)
	}
	return assets, rows.Err()
}

func securityAssetAliases(assets []map[string]any) (map[string]bool, map[string]bool) {
	names, symbols := map[string]bool{}, map[string]bool{}
	for _, asset := range assets {
		for _, term := range append([]any{asset["name"]}, anySlice(asset["aliases"])...) {
			if compact := compactTarget(stringValue(term)); compact != "" {
				names[compact] = true
			}
		}
		if symbol := strings.ToLower(strings.TrimSpace(stringValue(asset["symbol"]))); symbol != "" {
			symbols[symbol] = true
		}
	}
	return names, symbols
}

var preferredPublishedSecurity = map[string]string{
	"nvidia":          "equity:NASDAQ:NVDA",
	"nvidia股价":        "equity:NASDAQ:NVDA",
	"nvda":            "equity:NASDAQ:NVDA",
	"spacex":          "crypto:coingecko:spacex-prestocks-2",
	"spacexprestocks": "crypto:coingecko:spacex-prestocks-2",
}

type publishedSecurityResolver struct {
	assets       []map[string]any
	byID         map[string]map[string]any
	byExactTerm  map[string][]map[string]any
	matchKnown   map[string]bool
	matchedAsset map[string]map[string]any
}

func newPublishedSecurityResolver(assets []map[string]any) *publishedSecurityResolver {
	resolver := &publishedSecurityResolver{
		assets: assets, byID: make(map[string]map[string]any, len(assets)), byExactTerm: map[string][]map[string]any{},
		matchKnown: map[string]bool{}, matchedAsset: map[string]map[string]any{},
	}
	for _, asset := range assets {
		resolver.byID[stringValue(asset["asset_id"])] = asset
		for _, raw := range append([]any{asset["symbol"], asset["name"]}, anySlice(asset["aliases"])...) {
			if term := compactTarget(stringValue(raw)); term != "" {
				resolver.byExactTerm[term] = append(resolver.byExactTerm[term], asset)
			}
		}
	}
	return resolver
}

func resolvePublishedSecurityImpacts(value any, assets []map[string]any) []any {
	return newPublishedSecurityResolver(assets).resolve(value)
}

func (resolver *publishedSecurityResolver) resolve(value any) []any {
	resolved := make([]any, 0, len(anySlice(value)))
	for _, raw := range anySlice(value) {
		impact := deepCloneObject(objectValue(raw))
		if impact == nil {
			continue
		}
		normalizeTargetImpact(impact)
		current := objectValue(impact["asset"])
		asset := resolver.byID[stringValue(valueOrNil(current, "asset_id"))]
		if asset == nil {
			asset = resolver.match(stringValue(impact["target_name"]))
		}
		if asset != nil {
			impact["target_type"] = "tradable_asset"
			impact["target_name"] = fallbackString(stringValue(asset["name"]), stringValue(impact["target_name"]))
			impact["asset"] = deepCloneObject(asset)
		}
		resolved = append(resolved, impact)
	}
	return dedupePublishedSecurityImpacts(resolved)
}

func (resolver *publishedSecurityResolver) match(name string) map[string]any {
	key := compactTarget(name)
	if resolver.matchKnown[key] {
		return resolver.matchedAsset[key]
	}
	resolver.matchKnown[key] = true
	base := key
	for _, suffix := range []string{"stockprice", "shareprice", "股价", "股票"} {
		base = strings.TrimSuffix(base, suffix)
	}
	if preferredID := preferredPublishedSecurity[base]; preferredID != "" {
		resolver.matchedAsset[key] = resolver.byID[preferredID]
		return resolver.matchedAsset[key]
	}
	terms := []string{base}
	for _, token := range targetWords.FindAllString(name, -1) {
		if term := compactTarget(token); term != "" && term != base {
			terms = append(terms, term)
		}
	}
	for _, term := range terms {
		if asset, resolved := selectPublishedMasterAsset(term, resolver.byExactTerm[term]); resolved {
			resolver.matchedAsset[key] = asset
			return asset
		}
	}
	return nil
}

func selectPublishedMasterAsset(target string, candidates []map[string]any) (map[string]any, bool) {
	bestScore := -1.0
	var best map[string]any
	tied := false
	for _, asset := range candidates {
		score := publishedMasterAssetScore(target, asset)
		if score < 0 {
			continue
		}
		if score > bestScore {
			bestScore, best, tied = score, asset, false
		} else if score == bestScore && stringValue(asset["asset_id"]) != stringValue(best["asset_id"]) {
			tied = true
		}
	}
	return best, best != nil && !tied
}

func matchPublishedMasterAsset(name string, assets []map[string]any) map[string]any {
	compact := compactTarget(name)
	base := compact
	for _, suffix := range []string{"stockprice", "shareprice", "股价", "股票"} {
		base = strings.TrimSuffix(base, suffix)
	}
	if preferredID := preferredPublishedSecurity[base]; preferredID != "" {
		for _, asset := range assets {
			if stringValue(asset["asset_id"]) == preferredID {
				return asset
			}
		}
	}
	bestScore := -1.0
	var best map[string]any
	tied := false
	for _, asset := range assets {
		score := publishedMasterAssetScore(base, asset)
		if score < 0 {
			continue
		}
		if score > bestScore {
			bestScore, best, tied = score, asset, false
		} else if score == bestScore && stringValue(asset["asset_id"]) != stringValue(best["asset_id"]) {
			tied = true
		}
	}
	if tied {
		return nil
	}
	return best
}

func publishedMasterAssetScore(target string, asset map[string]any) float64 {
	if target == "" || !securityAsset(asset) || !boolValue(asset["active"]) {
		return -1
	}
	tier := stringValue(asset["association_tier"])
	exact, prefix := false, false
	terms := append([]any{asset["symbol"], asset["name"]}, anySlice(asset["aliases"])...)
	for _, raw := range terms {
		term := compactTarget(stringValue(raw))
		if term == "" {
			continue
		}
		if target == term {
			exact = true
		}
		if len([]rune(target)) >= 4 && len([]rune(term)) >= 4 && (strings.Contains(target, term) || strings.Contains(term, target)) {
			prefix = true
		}
	}
	if tier == "manual_only" || ((tier == "exact_only" || tier == "") && !exact) || (!exact && !prefix) {
		return -1
	}
	score := 100.0
	if exact {
		score += 1000
	}
	if tier == "standard" {
		score += 100
	}
	if stringValue(asset["asset_class"]) == "equity" {
		score += 40
	}
	if stringValue(asset["instrument_type"]) == "common_stock" {
		score += 20
	}
	score += math.Log10(math.Max(1, numberValue(asset["market_cap"])))
	return score
}

func dedupePublishedSecurityImpacts(impacts []any) []any {
	result := make([]any, 0, len(impacts))
	indexes := make(map[string]int)
	for _, raw := range impacts {
		impact := objectValue(raw)
		key := publishedImpactKey(impact)
		if index, found := indexes[key]; found {
			if preferPublishedImpact(impact, objectValue(result[index])) {
				result[index] = impact
			}
			continue
		}
		indexes[key] = len(result)
		result = append(result, impact)
	}
	return result
}

func visibleEventStatus(value string) bool {
	return value == "completed" || value == "insufficient_evidence"
}

func objectValue(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func normalizeEventReport(report map[string]any) {
	if report == nil {
		return
	}
	if report["news_confidence"] == nil {
		report["news_confidence"] = report["fact_confidence"]
	}
	for _, raw := range anySlice(report["impacts"]) {
		normalizeTargetImpact(objectValue(raw))
	}
}

func normalizeTargetImpact(impact map[string]any) {
	if impact == nil {
		return
	}
	if impact["direction_score"] == nil {
		score := math.Round(numberValue(impact["score"]) * 100)
		impact["direction_score"] = math.Max(-100, math.Min(100, score))
	}
	if impact["rating_confidence"] == nil {
		impact["rating_confidence"] = impact["confidence"]
	}
	if asset := objectValue(impact["asset"]); asset != nil {
		normalizeAsset(asset)
	}
}

func valueOrNil(value map[string]any, key string) any {
	if value == nil {
		return nil
	}
	return value[key]
}

func securityAsset(asset map[string]any) bool {
	class := strings.ToLower(stringValue(valueOrNil(asset, "asset_class")))
	return class == "equity" || class == "crypto"
}

func genericPublishedSecurityTarget(value string) bool {
	compact := compactTarget(value)
	if compact == "company" || compact == "companies" {
		return true
	}
	if len(compact) != 4 {
		return false
	}
	for _, current := range compact {
		if current < '0' || current > '9' {
			return false
		}
	}
	year, err := strconv.Atoi(compact)
	return err == nil && year >= 1900 && year <= 2100
}

func resemblesSecurity(impact map[string]any, names, symbols map[string]bool) bool {
	if stringValue(impact["target_type"]) == "tradable_asset" || securityAsset(objectValue(impact["asset"])) {
		return true
	}
	name := stringValue(impact["target_name"])
	if names[compactTarget(name)] {
		return true
	}
	for _, token := range targetWords.FindAllString(name, -1) {
		if len(token) >= 2 && symbols[strings.ToLower(token)] {
			return true
		}
	}
	return false
}

func normalizedTarget(value string) string {
	return nonTargetCharacters.ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), "")
}

func compactTarget(value string) string { return normalizedTarget(value) }

func macroTargetBase(value string) string {
	value = strings.ToLower(parentheticalTicker.ReplaceAllString(value, " "))
	for _, phrase := range []string{"continuous benchmark", "continuous contract", "continuous futures", "连续基准", "连续合约"} {
		value = strings.ReplaceAll(value, phrase, " ")
	}
	return normalizedTarget(value)
}

func stripTargetWrappers(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	words := strings.Fields(value)
	wrappers := map[string]bool{"global": true, "market": true, "markets": true, "sector": true, "industry": true, "sentiment": true}
	for len(words) > 0 && wrappers[words[0]] {
		words = words[1:]
	}
	for len(words) > 0 && wrappers[words[len(words)-1]] {
		words = words[:len(words)-1]
	}
	value = strings.Join(words, " ")
	changed := true
	for changed && value != "" {
		changed = false
		if strings.HasPrefix(value, "全球") {
			value, changed = strings.TrimSpace(strings.TrimPrefix(value, "全球")), true
		}
		for _, suffix := range []string{"市场", "行业", "板块", "领域", "情绪"} {
			if strings.HasSuffix(value, suffix) {
				value, changed = strings.TrimSpace(strings.TrimSuffix(value, suffix)), true
			}
		}
	}
	return value
}

func canonicalizeGoTarget(name, targetType string, asset map[string]any, taxonomy map[string]canonicalTarget) canonicalTarget {
	if asset != nil && stringValue(asset["asset_id"]) != "" {
		if securityAsset(asset) {
			return canonicalTarget{
				Key: stringValue(asset["asset_id"]), Label: fallbackString(stringValue(asset["name"]), strings.TrimSpace(name)), TargetType: "tradable_asset",
			}
		}
		return canonicalTarget{Key: stringValue(asset["asset_id"]), Label: fallbackString(strings.TrimSpace(name), stringValue(asset["asset_id"])), TargetType: targetType}
	}
	normalized := compactTarget(name)
	if normalized == "" {
		normalized = "unknown"
	}
	if targetType == "economy" && unitedStatesEconomyAlias(name) {
		return canonicalTarget{Key: "economy:us", Label: "美国经济", TargetType: "economy"}
	}
	if targetType != "sector" && targetType != "economy" && targetType != "risk_asset" && targetType != "other" {
		return canonicalTarget{Key: targetType + ":" + normalized, Label: fallbackString(strings.TrimSpace(name), normalized), TargetType: targetType}
	}
	unwrapped := compactTarget(stripTargetWrappers(name))
	for _, alias := range []string{"Digital Assets", "Cryptocurrency", "数字资产", "加密货币", "Cryptocurrency Market", "Digital Assets Cryptocurrency", "数字资产 加密货币", "加密货币市场"} {
		if unwrapped == compactTarget(alias) {
			return canonicalTarget{Key: "sector:digital_assets", Label: "数字资产", TargetType: "sector"}
		}
	}
	if matched, found := taxonomy[unwrapped]; found {
		return matched
	}
	return canonicalTarget{Key: targetType + ":" + normalized, Label: fallbackString(strings.TrimSpace(name), normalized), TargetType: targetType}
}

func unitedStatesEconomyAlias(value string) bool {
	switch compactTarget(value) {
	case "美国股市", "美国", "美国经济", "useconomy", "usequitymarket":
		return true
	}
	return false
}

func fallbackString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func representativeMacroImpact(impacts []map[string]any) map[string]any {
	if len(impacts) == 0 {
		return nil
	}
	best := impacts[0]
	for _, candidate := range impacts[1:] {
		candidateTuple := []float64{numberValue(candidate["rating_confidence"]), float64(len(anySlice(candidate["evidence_ids"]))), -float64(len(anySlice(candidate["missing_information"])))}
		bestTuple := []float64{numberValue(best["rating_confidence"]), float64(len(anySlice(best["evidence_ids"]))), -float64(len(anySlice(best["missing_information"])))}
		better := false
		for index := range candidateTuple {
			if candidateTuple[index] != bestTuple[index] {
				better = candidateTuple[index] > bestTuple[index]
				break
			}
		}
		if candidateTuple[0] == bestTuple[0] && candidateTuple[1] == bestTuple[1] && candidateTuple[2] == bestTuple[2] {
			better = normalizedTarget(stringValue(candidate["target_name"])) > normalizedTarget(stringValue(best["target_name"]))
		}
		if better {
			best = candidate
		}
	}
	return best
}

func canonicalObservation(impacts []map[string]any, occurred time.Time, newsConfidence float64, provisional bool) targetObservation {
	weights := make([]float64, len(impacts))
	total, minScore, maxScore, failures := 0.0, math.Inf(1), math.Inf(-1), 0
	for index, impact := range impacts {
		weights[index] = math.Max(0.05, numberValue(impact["rating_confidence"]))
		total += weights[index]
		score := numberValue(impact["direction_score"])
		minScore, maxScore = math.Min(minScore, score), math.Max(maxScore, score)
		if boolValue(impact["technical_failure"]) {
			failures++
		}
	}
	weighted := func(value func(map[string]any) float64) float64 {
		result := 0.0
		for index, impact := range impacts {
			result += value(impact) * weights[index]
		}
		if total == 0 {
			return 0
		}
		return result / total
	}
	insufficient := provisional || maxScore-minScore >= 30 || failures == len(impacts)
	return targetObservation{
		OccurredAt: occurred, Score: weighted(func(value map[string]any) float64 { return numberValue(value["direction_score"]) }),
		RatingConfidence: weighted(func(value map[string]any) float64 { return numberValue(value["rating_confidence"]) }),
		NewsConfidence:   newsConfidence,
		Persistence:      weighted(func(value map[string]any) float64 { return numberValue(valueFromMap(value, "factors", "persistence")) }),
		RealizationProbability: weighted(func(value map[string]any) float64 {
			return numberValue(valueFromMap(value, "factors", "realization_probability"))
		}),
		Insufficient: insufficient, Provisional: insufficient,
	}
}

func macroImpactState(impact map[string]any, provisional bool) map[string]any {
	return map[string]any{"rating": impact["rating"], "direction_score": impact["direction_score"], "rating_confidence": impact["rating_confidence"], "provisional": provisional}
}

func ratingForScore(score float64) string {
	switch {
	case score >= 70:
		return "strongly_bullish"
	case score >= 30:
		return "bullish"
	case score <= -70:
		return "strongly_bearish"
	case score <= -30:
		return "bearish"
	default:
		return "watch"
	}
}

func observationQuality(value targetObservation) float64 {
	return (value.RatingConfidence + value.NewsConfidence + value.Persistence + value.RealizationProbability) / 4
}

func regimeBreak(value targetObservation) bool {
	return !value.Insufficient && !value.Provisional && value.RatingConfidence >= .65 && value.NewsConfidence >= .75 &&
		value.Persistence >= .7 && value.RealizationProbability >= .7 && math.Abs(value.Score) >= 70
}

func decay(age float64, halfLife int) float64 { return math.Pow(.5, age/float64(halfLife)) }

func observationAge(value targetObservation, asOf time.Time) float64 {
	return asOf.Sub(value.OccurredAt).Hours() / 24
}

func roundPlaces(value float64, places int) float64 {
	factor := math.Pow10(places)
	// The rollback API contract applies ties-to-even. Keep the
	// public trend payload byte-for-byte stable at exact half-way boundaries.
	return math.RoundToEven(value*factor) / factor
}

func trendConfidence(values []targetObservation, asOf time.Time, halfLife int) float64 {
	if len(values) == 0 {
		return 0
	}
	total, weighted := 0.0, 0.0
	for _, value := range values {
		weight := decay(observationAge(value, asOf), halfLife)
		total += weight
		weighted += ((value.RatingConfidence + value.NewsConfidence) / 2) * weight
	}
	return roundPlaces(weighted/total, 4)
}

func aggregateTargetTrend(values []targetObservation, asOf time.Time) targetTrend {
	shortValues := make([]targetObservation, 0)
	longValues := make([]targetObservation, 0)
	eligible := make([]targetObservation, 0)
	for _, value := range values {
		age := observationAge(value, asOf)
		if age >= 0 && age <= 7 {
			shortValues = append(shortValues, value)
		}
		if age >= 0 && age <= 90 {
			longValues = append(longValues, value)
			if !value.Insufficient && !value.Provisional && value.RatingConfidence >= .45 {
				eligible = append(eligible, value)
			}
		}
	}
	short := trendScore{EventCount: len(shortValues), EligibleCount: len(shortValues)}
	if len(shortValues) > 0 {
		total, score := 0.0, 0.0
		for _, value := range shortValues {
			weight := decay(observationAge(value, asOf), 3) * math.Max(.05, observationQuality(value))
			total, score = total+weight, score+value.Score*weight
			short.Provisional = short.Provisional || value.Provisional || value.Insufficient || value.RatingConfidence < .45
			short.RegimeBreak = short.RegimeBreak || regimeBreak(value)
		}
		short.Score = roundPlaces(score/total, 2)
		short.Confidence = trendConfidence(shortValues, asOf, 3)
	}
	short.Rating = ratingForScore(short.Score)
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].OccurredAt.Before(eligible[j].OccurredAt) })
	long := trendScore{EventCount: len(longValues), EligibleCount: len(eligible), IgnoredCount: len(longValues) - len(eligible), Provisional: len(eligible) == 0}
	for _, value := range eligible {
		limit := 20.0
		if regimeBreak(value) {
			limit = 45
			long.RegimeBreak = true
		}
		maximum := limit * decay(observationAge(value, asOf), 30) * observationQuality(value)
		step := math.Max(-maximum, math.Min(maximum, value.Score-long.Score))
		long.Score = math.Max(-100, math.Min(100, long.Score+step))
	}
	long.Score, long.Rating, long.Confidence = roundPlaces(long.Score, 2), ratingForScore(roundPlaces(long.Score, 2)), trendConfidence(eligible, asOf, 30)
	combined := long
	if short.EventCount > 0 {
		combined.Score = roundPlaces(.8*long.Score+.2*short.Score, 2)
		combined.Confidence = roundPlaces(.8*long.Confidence+.2*short.Confidence, 4)
	}
	combined.Rating = ratingForScore(combined.Score)
	combined.Provisional = long.Provisional || short.Provisional
	return targetTrend{Short: short, Long: long, Combined: combined}
}

func trendState(value trendScore) map[string]any {
	return map[string]any{"rating": value.Rating, "direction_score": value.Score, "rating_confidence": value.Confidence, "provisional": value.Provisional}
}

func publicTargetTrend(value targetTrend) map[string]any {
	return map[string]any{
		"algorithm_version": "dual-horizon-v1", "short_term": trendState(value.Short), "long_term": trendState(value.Long), "composite": trendState(value.Combined),
		"event_count_90d": value.Long.EventCount, "eligible_event_count_90d": value.Long.EligibleCount,
		"ignored_event_count_90d": value.Long.IgnoredCount, "regime_break": value.Long.RegimeBreak,
	}
}
