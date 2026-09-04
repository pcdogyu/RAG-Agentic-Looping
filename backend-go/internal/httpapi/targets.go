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
	ID         string
	AssetID    string
	EventID    string
	AsOf       time.Time
	OccurredAt time.Time
	UpdatedAt  time.Time
	Payload    map[string]any
}

type recommendationChange struct {
	Previous recommendationSnapshot
	Current  recommendationSnapshot
	Latest   recommendationSnapshot
	Signals  []targetRatingSignal
	State    ratingStateReplay
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
	changedOnly := changes[:0]
	for _, item := range changes {
		if item.State.HasChange {
			changedOnly = append(changedOnly, item)
		}
	}
	changes = changedOnly
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		stamp, id, decodeErr := decodeAssetCursor(raw)
		if decodeErr != nil {
			validationError(w, "cursor", "Input should be a valid cursor")
			return
		}
		filtered := changes[:0]
		for _, item := range changes {
			if item.State.ChangedAt.Before(stamp) || (item.State.ChangedAt.Equal(stamp) && item.State.ChangeDetailID < id) {
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
		if !item.State.HasChange {
			continue
		}
		normalizeRecommendation(item.Previous.Payload)
		normalizeRecommendation(item.Current.Payload)
		normalizeRecommendation(item.Latest.Payload)
		asset, _ := item.Current.Payload["asset"].(map[string]any)
		value := map[string]any{
			"asset": asset, "recommendation_id": item.Current.ID,
			"latest_recommendation_id": item.Latest.ID, "latest_researched_at": jsonTime(item.Latest.AsOf),
			"changed_at":     jsonTime(item.State.ChangedAt),
			"previous":       map[string]any{"signal_status": item.Previous.Payload["signal_status"], "rating": item.Previous.Payload["rating"]},
			"current":        map[string]any{"signal_status": item.Current.Payload["signal_status"], "rating": item.Current.Payload["rating"]},
			"status_changed": stringValue(item.Previous.Payload["signal_status"]) != stringValue(item.Current.Payload["signal_status"]),
			"rating_changed": stringValue(item.Previous.Payload["rating"]) != stringValue(item.Current.Payload["rating"]),
			"rating_state":   ratingStateValue(item.State), "latest_event_signal": latestEventSignalValue(item.State.LatestSignal),
			"overall_rating_changed": true,
		}
		items = append(items, value)
	}
	var next any
	if hasMore && len(changes) > 0 {
		last := changes[len(changes)-1].State
		next = encodeAssetCursor(last.ChangedAt, last.ChangeDetailID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s *Server) latestChangedTargets(r *http.Request) ([]recommendationChange, error) {
	histories, err := s.recommendationHistories(r, false)
	if err != nil {
		return nil, err
	}
	result := make([]recommendationChange, 0, len(histories))
	for _, values := range histories {
		if len(values) == 0 {
			continue
		}
		previous, current, latest := rawRecommendationChange(values)
		signals := recommendationRatingSignals(values)
		state := replayRatingState(signals)
		result = append(result, recommendationChange{Previous: previous, Current: current, Latest: latest, Signals: signals, State: state})
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i].State.ChangedAt, result[j].State.ChangedAt
		if left.IsZero() {
			left = result[i].Latest.AsOf
		}
		if right.IsZero() {
			right = result[j].Latest.AsOf
		}
		if left.Equal(right) {
			return result[i].State.ChangeDetailID > result[j].State.ChangeDetailID
		}
		return left.After(right)
	})
	return result, nil
}

func (s *Server) currentAssetRatings(r *http.Request) ([]map[string]any, error) {
	histories, err := s.recommendationHistories(r, true)
	if err != nil {
		return nil, err
	}
	market := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("market")))
	rating := strings.TrimSpace(r.URL.Query().Get("rating"))
	query := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	items := make([]map[string]any, 0, len(histories))
	for _, values := range histories {
		if len(values) == 0 {
			continue
		}
		latest := values[len(values)-1]
		normalizeRecommendation(latest.Payload)
		asset := objectValue(latest.Payload["asset"])
		state := replayRatingState(recommendationRatingSignals(values))
		if asset == nil || (market != "" && strings.ToUpper(stringValue(asset["market"])) != market) || (rating != "" && state.Current != rating) {
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
		if len(values) > 1 {
			previousSnapshot := values[len(values)-2]
			normalizeRecommendation(previousSnapshot.Payload)
			previous = impactFields(previousSnapshot.Payload)
		}
		changeState := "unchanged"
		if state.EligibleEventCount == 0 {
			changeState = "first"
		} else if state.HasChange {
			changeState = "changed"
		}
		value := map[string]any{
			"kind": "asset", "key": latest.AssetID, "label": asset["name"], "symbol": asset["symbol"], "market": asset["market"],
			"target_type": "tradable_asset", "rated_at": jsonTime(latest.AsOf), "changed_at": jsonTime(latest.AsOf),
			"previous": previous, "current": impactFields(latest.Payload), "change_state": changeState,
			"latest": map[string]any{"rating": latest.Payload["rating"], "direction_score": latest.Payload["direction_score"],
				"rating_confidence": latest.Payload["rating_confidence"], "news_confidence": latest.Payload["news_confidence"]},
			"latest_detail": map[string]any{"kind": "asset", "id": latest.ID, "researched_at": jsonTime(latest.AsOf)}, "change_detail_id": latest.ID,
		}
		applyRatingState(value, recommendationRatingSignals(values))
		delete(value, "_rating_signals")
		items = append(items, value)
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

func (s *Server) recommendationHistories(r *http.Request, activeOnly bool) (map[string][]recommendationSnapshot, error) {
	query := `SELECT rec.id,rec.asset_id,rec.as_of,rec.payload::jsonb,rr.event_id,rr.updated_at,e.published_at
		FROM recommendations rec
		LEFT JOIN research_runs rr ON rr.id=rec.run_id
		LEFT JOIN news_events e ON e.id=rr.event_id`
	if activeOnly {
		query += ` JOIN assets a ON a.id=rec.asset_id WHERE a.active=true AND a.market IN ('CN','HK','US','CRYPTO')`
	}
	query += ` ORDER BY rec.asset_id,rec.as_of,rec.id`
	rows, err := s.db.Query(r.Context(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string][]recommendationSnapshot{}
	for rows.Next() {
		var item recommendationSnapshot
		var body []byte
		var eventID *string
		var updatedAt, publishedAt *time.Time
		if err = rows.Scan(&item.ID, &item.AssetID, &item.AsOf, &body, &eventID, &updatedAt, &publishedAt); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(body, &item.Payload); err != nil {
			return nil, err
		}
		item.EventID = stringPointerValue(eventID)
		item.OccurredAt, item.UpdatedAt = item.AsOf, item.AsOf
		if publishedAt != nil {
			item.OccurredAt = *publishedAt
		}
		if updatedAt != nil {
			item.UpdatedAt = *updatedAt
		}
		result[item.AssetID] = append(result[item.AssetID], item)
	}
	return result, rows.Err()
}

func rawRecommendationChange(values []recommendationSnapshot) (recommendationSnapshot, recommendationSnapshot, recommendationSnapshot) {
	latest := values[len(values)-1]
	previous, current := values[0], latest
	for index := 1; index < len(values); index++ {
		if stringValue(values[index-1].Payload["rating"]) != stringValue(values[index].Payload["rating"]) {
			previous, current = values[index-1], values[index]
		}
	}
	return previous, current, latest
}

func recommendationRatingSignals(values []recommendationSnapshot) []targetRatingSignal {
	result := make([]targetRatingSignal, 0, len(values))
	for _, value := range values {
		normalizeRecommendation(value.Payload)
		impact := objectValue(value.Payload["impact"])
		if impact == nil {
			impact = value.Payload
		}
		confidence := numberValue(value.Payload["rating_confidence"])
		newsConfidence := numberValue(value.Payload["news_confidence"])
		status := stringValue(value.Payload["signal_status"])
		provisional := !boolValue(value.Payload["evidence_complete"]) || boolValue(value.Payload["provisional"]) || boolValue(impact["provisional"]) ||
			status == "insufficient_evidence" || status == "technical_failure" || boolValue(impact["technical_failure"])
		observation := canonicalObservation([]map[string]any{impact}, value.OccurredAt, newsConfidence, provisional)
		result = append(result, targetRatingSignal{
			EventID: value.EventID, Rating: stringValue(value.Payload["rating"]), DirectionScore: numberValue(value.Payload["direction_score"]),
			RatingConfidence: confidence, NewsConfidence: newsConfidence, OccurredAt: value.OccurredAt, EvaluatedAt: value.UpdatedAt,
			DetailKind: "asset", DetailID: value.ID, Eligible: value.EventID != "" && !observation.Insufficient && !observation.Provisional && confidence >= .45,
			SourcePriority: 2, Observation: observation,
		})
	}
	return result
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

type targetRatingSignal struct {
	EventID          string
	Rating           string
	DirectionScore   float64
	RatingConfidence float64
	NewsConfidence   float64
	OccurredAt       time.Time
	EvaluatedAt      time.Time
	DetailKind       string
	DetailID         string
	Eligible         bool
	SourcePriority   int
	Observation      targetObservation
}

type ratingStateReplay struct {
	Previous           string
	Current            string
	ChangedAt          time.Time
	ChangeDetailKind   string
	ChangeDetailID     string
	EligibleEventCount int
	TransitionLimited  bool
	HasChange          bool
	LatestSignal       *targetRatingSignal
	Signals            []targetRatingSignal
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
	EventID     string
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
		changed := items[:0]
		for _, item := range items {
			if ratingStateChanged(item) {
				changed = append(changed, item)
			}
		}
		items = changed
		items = filterTargetChanges(items, r.URL.Query().Get("q"))
	}
	for _, item := range items {
		delete(item, "_rating_signals")
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
		value := map[string]any{
			"kind": "asset", "key": item.Current.AssetID, "label": asset["name"], "symbol": asset["symbol"], "market": asset["market"],
			"target_type": "tradable_asset", "changed_at": jsonTime(item.Current.AsOf),
			"previous": impactFields(item.Previous.Payload), "current": impactFields(item.Current.Payload),
			"latest": map[string]any{"rating": item.Latest.Payload["rating"], "direction_score": item.Latest.Payload["direction_score"],
				"rating_confidence": item.Latest.Payload["rating_confidence"], "news_confidence": item.Latest.Payload["news_confidence"]},
			"latest_detail":    map[string]any{"kind": "asset", "id": item.Latest.ID, "researched_at": jsonTime(item.Latest.AsOf)},
			"change_detail_id": item.Current.ID,
		}
		applyRatingState(value, item.Signals)
		items = append(items, value)
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
	signalsByKey := map[string][]targetRatingSignal{}
	keyOrder := make([]string, 0)
	for _, group := range groups {
		for _, item := range group {
			key := stringValue(item["key"])
			if signals, ok := item["_rating_signals"].([]targetRatingSignal); ok {
				signalsByKey[key] = append(signalsByKey[key], signals...)
			}
			current := latestByKey[key]
			if current == nil {
				keyOrder = append(keyOrder, key)
				latestByKey[key] = item
			} else if latestEventAfter(item, current) {
				latestByKey[key] = item
			}
		}
	}
	result := make([]map[string]any, 0, len(latestByKey))
	for _, key := range keyOrder {
		value := map[string]any{}
		for field, raw := range latestByKey[key] {
			value[field] = raw
		}
		state := applyRatingState(value, signalsByKey[key])
		if len(state.Signals) > 0 {
			observations := make([]targetObservation, 0, len(state.Signals))
			for _, signal := range state.Signals {
				observations = append(observations, signal.Observation)
			}
			value["trend"] = publicTargetTrend(aggregateTargetTrend(observations, time.Now().UTC()))
		}
		if state.LatestSignal != nil {
			value["latest"] = map[string]any{
				"rating": state.LatestSignal.Rating, "direction_score": state.LatestSignal.DirectionScore,
				"rating_confidence": state.LatestSignal.RatingConfidence, "news_confidence": state.LatestSignal.NewsConfidence,
			}
		}
		delete(value, "_rating_signals")
		result = append(result, value)
	}
	sort.SliceStable(result, func(i, j int) bool { return targetChangeAfter(result[i], result[j]) })
	return result
}

func latestEventAfter(left, right map[string]any) bool {
	leftSignal, rightSignal := objectValue(left["latest_event_signal"]), objectValue(right["latest_event_signal"])
	leftDetail, rightDetail := objectValue(leftSignal["detail"]), objectValue(rightSignal["detail"])
	leftTime, rightTime := parseAnyTime(leftDetail["researched_at"]), parseAnyTime(rightDetail["researched_at"])
	if leftTime == nil && rightTime == nil {
		return targetChangeAfter(left, right)
	}
	if leftTime != nil && rightTime != nil && leftTime.Equal(*rightTime) {
		return stringValue(leftDetail["id"]) > stringValue(rightDetail["id"])
	}
	return leftTime != nil && (rightTime == nil || leftTime.After(*rightTime))
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
	rows, err := s.db.Query(r.Context(), `SELECT er.id,er.event_id,er.status,er.updated_at,er.payload::jsonb,e.published_at
		FROM event_research_runs er LEFT JOIN news_events e ON e.id=er.event_id ORDER BY er.updated_at,er.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type parsedRun struct {
		id, eventID, status string
		updated             time.Time
		published           *time.Time
		payload             map[string]any
	}
	runs := make([]parsedRun, 0)
	aliases := map[string]map[string]any{}
	for rows.Next() {
		var run parsedRun
		var body []byte
		if err = rows.Scan(&run.id, &run.eventID, &run.status, &run.updated, &body, &run.published); err != nil {
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
		provisional := run.status == "insufficient_evidence" || !boolValue(report["evidence_complete"]) || boolValue(report["provisional"])
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
				Run: run.payload, RunID: run.id, EventID: run.eventID, ChangedAt: run.updated, Observation: observation, Provisional: observation.Provisional,
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
			before, changed = &values[0], &values[len(values)-1]
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
		value := map[string]any{
			"kind": kind, "key": key, "label": latest.Canonical.Label, "symbol": valueOrNil(displayAsset, "symbol"),
			"market": valueOrNil(displayAsset, "market"), "target_type": latest.Canonical.TargetType, "changed_at": jsonTime(changed.ChangedAt),
			"previous": macroImpactState(before.Impact, before.Provisional), "current": macroImpactState(changed.Impact, changed.Provisional),
			"latest": map[string]any{"rating": latest.Impact["rating"], "direction_score": latest.Impact["direction_score"],
				"rating_confidence": latest.Impact["rating_confidence"], "provisional": latest.Provisional, "news_confidence": valueOrNil(report, "news_confidence")},
			"trend":            publicTargetTrend(aggregateTargetTrend(observations, now)),
			"latest_detail":    map[string]any{"kind": "event", "id": latest.RunID, "researched_at": jsonTime(latest.ChangedAt)},
			"change_detail_id": changed.RunID,
		}
		applyRatingState(value, macroRatingSignals(values))
		output = append(output, value)
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
	impactProvisional := false
	for index, impact := range impacts {
		weights[index] = math.Max(0.05, numberValue(impact["rating_confidence"]))
		total += weights[index]
		score := numberValue(impact["direction_score"])
		minScore, maxScore = math.Min(minScore, score), math.Max(maxScore, score)
		if boolValue(impact["technical_failure"]) {
			failures++
		}
		if boolValue(impact["provisional"]) || stringValue(impact["conclusion_status"]) == "insufficient_evidence" {
			impactProvisional = true
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
	insufficient := provisional || impactProvisional || maxScore-minScore >= 30 || failures == len(impacts)
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

func macroRatingSignals(values []macroSnapshot) []targetRatingSignal {
	result := make([]targetRatingSignal, 0, len(values))
	for _, value := range values {
		confidence := numberValue(value.Impact["rating_confidence"])
		newsConfidence := value.Observation.NewsConfidence
		result = append(result, targetRatingSignal{
			EventID: value.EventID, Rating: stringValue(value.Impact["rating"]), DirectionScore: numberValue(value.Impact["direction_score"]),
			RatingConfidence: confidence, NewsConfidence: newsConfidence, OccurredAt: value.Observation.OccurredAt, EvaluatedAt: value.ChangedAt,
			DetailKind: "event", DetailID: value.RunID, Eligible: value.EventID != "" && !value.Observation.Insufficient && !value.Observation.Provisional && confidence >= .45,
			SourcePriority: 1, Observation: value.Observation,
		})
	}
	return result
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

var ratingScale = []string{"strongly_bearish", "bearish", "watch", "bullish", "strongly_bullish"}

func ratingIndex(value string) int {
	for index, rating := range ratingScale {
		if value == rating {
			return index
		}
	}
	return 2
}

func ratingDistance(left, right int) int {
	if left < right {
		return right - left
	}
	return left - right
}

func normalizedSignalRating(value string, score float64) string {
	for _, rating := range ratingScale {
		if value == rating {
			return value
		}
	}
	return ratingForScore(score)
}

func deduplicateRatingSignals(values []targetRatingSignal) []targetRatingSignal {
	byEvent := map[string]targetRatingSignal{}
	for _, value := range values {
		if strings.TrimSpace(value.EventID) == "" {
			continue
		}
		value.Rating = normalizedSignalRating(value.Rating, value.DirectionScore)
		current, exists := byEvent[value.EventID]
		if !exists || value.SourcePriority > current.SourcePriority || value.SourcePriority == current.SourcePriority && value.EvaluatedAt.After(current.EvaluatedAt) {
			byEvent[value.EventID] = value
		}
	}
	result := make([]targetRatingSignal, 0, len(byEvent))
	for _, value := range byEvent {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].OccurredAt.Equal(result[j].OccurredAt) {
			if result[i].EventID == result[j].EventID {
				return result[i].EvaluatedAt.Before(result[j].EvaluatedAt)
			}
			return result[i].EventID < result[j].EventID
		}
		return result[i].OccurredAt.Before(result[j].OccurredAt)
	})
	return result
}

func replayRatingState(values []targetRatingSignal) ratingStateReplay {
	signals := deduplicateRatingSignals(values)
	state := ratingStateReplay{Previous: "watch", Current: "watch", Signals: signals}
	if len(signals) > 0 {
		latest := signals[len(signals)-1]
		state.LatestSignal = &latest
	}
	currentIndex := ratingIndex("watch")
	for _, signal := range signals {
		if !signal.Eligible {
			continue
		}
		state.EligibleEventCount++
		desiredIndex := ratingIndex(signal.Rating)
		if desiredIndex == currentIndex {
			continue
		}
		previousIndex := currentIndex
		if desiredIndex > currentIndex {
			currentIndex++
		} else {
			currentIndex--
		}
		state.Previous = ratingScale[previousIndex]
		state.Current = ratingScale[currentIndex]
		state.ChangedAt = signal.EvaluatedAt
		state.ChangeDetailKind = signal.DetailKind
		state.ChangeDetailID = signal.DetailID
		state.TransitionLimited = ratingDistance(desiredIndex, previousIndex) > 1
		state.HasChange = true
	}
	return state
}

func ratingStateValue(state ratingStateReplay) map[string]any {
	var changedAt any
	if !state.ChangedAt.IsZero() {
		changedAt = jsonTime(state.ChangedAt)
	}
	return map[string]any{
		"previous":             state.Previous,
		"current":              state.Current,
		"changed_at":           changedAt,
		"algorithm_version":    "step-limited-rating-v1",
		"eligible_event_count": state.EligibleEventCount,
		"transition_limited":   state.TransitionLimited,
	}
}

func latestEventSignalValue(signal *targetRatingSignal) any {
	if signal == nil {
		return nil
	}
	return map[string]any{
		"event_id": signal.EventID, "rating": signal.Rating, "direction_score": signal.DirectionScore,
		"rating_confidence": signal.RatingConfidence, "news_confidence": signal.NewsConfidence,
		"occurred_at": jsonTime(signal.OccurredAt),
		"detail":      map[string]any{"kind": signal.DetailKind, "id": signal.DetailID, "researched_at": jsonTime(signal.EvaluatedAt)},
	}
}

func ratingStateChanged(value map[string]any) bool {
	state := objectValue(value["rating_state"])
	return stringValue(state["previous"]) != stringValue(state["current"])
}

func applyRatingState(value map[string]any, signals []targetRatingSignal) ratingStateReplay {
	state := replayRatingState(signals)
	value["rating_state"] = ratingStateValue(state)
	value["latest_event_signal"] = latestEventSignalValue(state.LatestSignal)
	value["_rating_signals"] = state.Signals
	if state.HasChange {
		value["changed_at"] = jsonTime(state.ChangedAt)
		value["change_detail_id"] = state.ChangeDetailID
	}
	if state.LatestSignal != nil {
		value["latest_detail"] = map[string]any{"kind": state.LatestSignal.DetailKind, "id": state.LatestSignal.DetailID, "researched_at": jsonTime(state.LatestSignal.EvaluatedAt)}
	}
	return state
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
