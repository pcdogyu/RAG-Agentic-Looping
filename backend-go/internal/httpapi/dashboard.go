package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

func (s *Server) newsBoard(w http.ResponseWriter, r *http.Request) {
	perSource, ok := intQuery(w, r.URL.Query(), "per_source", 50, 1, 50)
	if !ok {
		return
	}
	payload, err := s.buildNewsBoard(r, perSource)
	if err != nil {
		writeError(w, 500, "news board query failed")
		return
	}
	writeJSON(w, 200, payload)
}

type newsBoardSourceState struct {
	Source, Status                      string
	Watermark, LastAttempt, LastSuccess *time.Time
	LastError                           *string
	Discovered, New                     int
}
type newsBoardRow struct {
	ID, Source, Quality, Title, Summary, URL string
	Published, Observed                      time.Time
}

func (s *Server) buildNewsBoard(r *http.Request, perSource int) (map[string]any, error) {
	states := map[string]newsBoardSourceState{}
	stateRows, err := s.db.Query(r.Context(), `SELECT source,status,watermark_at,last_attempt_at,last_success_at,last_error,last_discovered_count,last_new_count FROM news_source_states`)
	if err != nil {
		return nil, err
	}
	for stateRows.Next() {
		var state newsBoardSourceState
		if stateRows.Scan(&state.Source, &state.Status, &state.Watermark, &state.LastAttempt, &state.LastSuccess, &state.LastError, &state.Discovered, &state.New) == nil {
			states[state.Source] = state
		}
	}
	stateRows.Close()
	sourceRows, err := s.db.Query(r.Context(), `SELECT source,max(published_at) FROM news_items GROUP BY source ORDER BY max(published_at) DESC,source`)
	if err != nil {
		return nil, err
	}
	type sourceStamp struct {
		Name   string
		Latest *time.Time
	}
	sources := make([]sourceStamp, 0)
	known := map[string]bool{}
	for sourceRows.Next() {
		var value sourceStamp
		if sourceRows.Scan(&value.Name, &value.Latest) == nil {
			sources = append(sources, value)
			known[value.Name] = true
		}
	}
	sourceRows.Close()
	for name, state := range states {
		if !known[name] && (state.Status == "error" || state.Watermark != nil) {
			sources = append(sources, sourceStamp{name, state.Watermark})
		}
	}
	sort.SliceStable(sources, func(i, j int) bool { return compareTimes(sources[i].Latest, sources[j].Latest) > 0 })
	rowsBySource := map[string][]newsBoardRow{}
	selectedIDs := make([]string, 0)
	for _, source := range sources {
		rows, queryErr := s.db.Query(r.Context(), `SELECT id,source,source_quality,title,summary,url,published_at,observed_at FROM news_items WHERE source=$1 ORDER BY published_at DESC,observed_at DESC LIMIT $2`, source.Name, perSource)
		if queryErr != nil {
			continue
		}
		for rows.Next() {
			var item newsBoardRow
			if rows.Scan(&item.ID, &item.Source, &item.Quality, &item.Title, &item.Summary, &item.URL, &item.Published, &item.Observed) == nil {
				rowsBySource[source.Name] = append(rowsBySource[source.Name], item)
				selectedIDs = append(selectedIDs, item.ID)
			}
		}
		rows.Close()
	}
	eventsByNews := map[string][]map[string]any{}
	eventIDs := make([]string, 0)
	if len(selectedIDs) > 0 {
		rows, queryErr := s.db.Query(r.Context(), `SELECT id,payload::jsonb FROM news_events WHERE payload::jsonb->'news_item_ids' ?| $1::text[]`, selectedIDs)
		if queryErr == nil {
			for rows.Next() {
				var id string
				var body []byte
				if rows.Scan(&id, &body) != nil {
					continue
				}
				var event map[string]any
				if json.Unmarshal(body, &event) != nil {
					continue
				}
				eventIDs = append(eventIDs, id)
				for _, raw := range anySlice(event["news_item_ids"]) {
					newsID := fmt.Sprint(raw)
					eventsByNews[newsID] = append(eventsByNews[newsID], event)
				}
			}
			rows.Close()
		}
	}
	runsByEvent := map[string][]map[string]any{}
	if len(eventIDs) > 0 {
		for _, table := range []string{"research_runs", "event_research_runs"} {
			rows, queryErr := s.db.Query(r.Context(), `SELECT event_id,payload::jsonb FROM `+table+` WHERE event_id=ANY($1::text[])`, eventIDs)
			if queryErr != nil {
				continue
			}
			for rows.Next() {
				var eventID string
				var body []byte
				if rows.Scan(&eventID, &body) != nil {
					continue
				}
				var run map[string]any
				if json.Unmarshal(body, &run) == nil {
					runsByEvent[eventID] = append(runsByEvent[eventID], run)
				}
			}
			rows.Close()
		}
	}
	processing := map[string]map[string]any{}
	if len(selectedIDs) > 0 {
		rows, queryErr := s.db.Query(r.Context(), `SELECT news_id,status,last_error,updated_at FROM news_processing WHERE news_id=ANY($1::text[])`, selectedIDs)
		if queryErr == nil {
			for rows.Next() {
				var id, status string
				var lastError *string
				var updated time.Time
				if rows.Scan(&id, &status, &lastError, &updated) == nil {
					processing[id] = map[string]any{"status": status, "last_error": nullableStringPointer(lastError), "updated_at": updated.UTC()}
				}
			}
			rows.Close()
		}
	}
	extraction := s.extractionStatus(r, selectedIDs)
	output := make([]map[string]any, 0, len(sources))
	var lastAttempt, lastSuccess *time.Time
	for _, source := range sources {
		state := states[source.Name]
		if compareTimes(state.LastAttempt, lastAttempt) > 0 {
			lastAttempt = state.LastAttempt
		}
		if compareTimes(state.LastSuccess, lastSuccess) > 0 {
			lastSuccess = state.LastSuccess
		}
		items := make([]map[string]any, 0)
		for _, row := range rowsBySource[source.Name] {
			events := eventsByNews[row.ID]
			sort.SliceStable(events, func(i, j int) bool { return numberValue(events[i]["priority"]) > numberValue(events[j]["priority"]) })
			status, updated, detail := newsBoardStatus(row, events, runsByEvent, processing[row.ID], extraction[row.ID])
			assetItems := make([]map[string]any, 0)
			seenAssets := map[string]bool{}
			eventItems := make([]map[string]any, 0, len(events))
			for _, event := range events {
				eventItems = append(eventItems, map[string]any{"id": event["id"], "headline": event["headline"], "event_type": event["event_type"], "priority": event["priority"]})
				for _, candidateRaw := range anySlice(event["candidates"]) {
					candidate, _ := candidateRaw.(map[string]any)
					asset, _ := candidate["asset"].(map[string]any)
					if asset == nil {
						continue
					}
					id := stringValue(asset["asset_id"])
					if id != "" && !seenAssets[id] {
						seenAssets[id] = true
						assetItems = append(assetItems, map[string]any{"asset_id": id, "symbol": asset["symbol"], "name": asset["name"], "market": asset["market"]})
					}
				}
			}
			items = append(items, map[string]any{"id": row.ID, "title": row.Title, "summary": row.Summary, "url": row.URL, "source_quality": row.Quality, "published_at": jsonTime(row.Published), "observed_at": jsonTime(row.Observed), "status": status, "status_updated_at": jsonTime(updated), "status_detail": detail, "retryable": status == "orphaned" || status == "failed", "events": eventItems, "assets": assetItems})
		}
		output = append(output, map[string]any{"source": source.Name, "latest_published_at": timeOrNil(source.Latest), "item_count": len(items), "items": items, "error": nil, "discovery_status": defaultValue(state.Status, "unchecked"), "last_attempt_at": timeOrNil(state.LastAttempt), "last_success_at": timeOrNil(state.LastSuccess), "watermark_at": timeOrNil(state.Watermark), "last_error": state.LastError, "last_discovered_count": state.Discovered, "last_new_count": state.New})
	}
	return map[string]any{"generated_at": time.Now().UTC(), "last_refresh_at": timeOrNil(lastAttempt), "last_success_at": timeOrNil(lastSuccess), "per_source": perSource, "total_sources": len(output), "sources": output}, nil
}

func (s *Server) extractionStatus(r *http.Request, ids []string) map[string]map[string]any {
	allowed := map[string]bool{}
	for _, id := range ids {
		allowed[id] = true
	}
	result := map[string]map[string]any{}
	raw, err := s.redis.Get(r.Context(), newsExtractionQueueKey).Bytes()
	if err != nil {
		return result
	}
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil {
		return result
	}
	for _, itemRaw := range anySlice(payload["items"]) {
		item, _ := itemRaw.(map[string]any)
		id := stringValue(item["news_id"])
		status := stringValue(item["status"])
		if allowed[id] && (status == "queued" || status == "running" || status == "retrying" || status == "failed") {
			result[id] = item
		}
	}
	return result
}

func newsBoardStatus(row newsBoardRow, events []map[string]any, runs map[string][]map[string]any, processing, extraction map[string]any) (string, time.Time, any) {
	rank := map[string]int{"pending": 0, "orphaned": 1, "failed": 2, "insufficient_evidence": 3, "completed": 4, "dispatch_pending": 5, "queued": 6, "extracting": 7, "mapping": 8, "researching": 9, "revising": 10}
	status := "pending"
	updated := row.Observed.UTC()
	detail := any(nil)
	choose := func(candidate string, stamp *time.Time) {
		if stamp == nil {
			return
		}
		if rank[candidate] > rank[status] || (rank[candidate] == rank[status] && stamp.After(updated)) {
			status = candidate
			updated = stamp.UTC()
		}
	}
	if extraction != nil {
		candidate := "extracting"
		if stringValue(extraction["status"]) == "failed" {
			candidate = "failed"
		}
		choose(candidate, parseAnyTime(extraction["updated_at"]))
		if extraction["error"] != nil {
			detail = extraction["error"]
		}
	}
	if processing != nil {
		mapped := map[string]string{"dispatch_pending": "dispatch_pending", "queued": "queued", "running": "extracting", "retrying": "extracting", "dispatch_failed": "failed", "extraction_failed": "failed"}[stringValue(processing["status"])]
		if mapped != "" {
			choose(mapped, parseAnyTime(processing["updated_at"]))
		}
		if processing["last_error"] != nil {
			detail = processing["last_error"]
		}
	}
	for _, event := range events {
		eventID := stringValue(event["id"])
		for _, stepRaw := range anySlice(event["analysis_steps"]) {
			step, _ := stepRaw.(map[string]any)
			phase, stepStatus := stringValue(step["phase"]), stringValue(step["status"])
			if phase == "asset_mapping" && (stepStatus == "running" || stepStatus == "retrying") || phase == "asset_mapping_queue" && stepStatus == "queued" {
				choose("mapping", parseAnyTime(step["occurred_at"]))
			}
		}
		for _, run := range runs[eventID] {
			candidate := map[string]string{"queued": "researching", "running": "researching", "coalesced": "researching", "verifying": "revising", "completed": "completed", "insufficient_evidence": "insufficient_evidence", "failed": "failed"}[stringValue(run["status"])]
			if candidate != "" {
				choose(candidate, parseAnyTime(run["updated_at"]))
			}
		}
	}
	if len(events) == 0 && processing == nil && extraction == nil {
		status = "orphaned"
		detail = "新闻已入库，但没有抽取任务或关联事件。"
	}
	if detail == nil {
		switch status {
		case "dispatch_pending":
			detail = "新闻已持久化，等待可靠派发到抽取队列。"
		case "queued":
			detail = "抽取任务已创建，等待模型实例执行。"
		}
	}
	return status, updated, detail
}

func (s *Server) analysisLogs(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 10, 1, 50)
	if !ok {
		return
	}
	items, err := s.buildAnalysisLogs(r, limit)
	if err != nil {
		writeError(w, 500, "analysis log query failed")
		return
	}
	writeJSON(w, 200, items)
}

type analysisEntry struct {
	Updated time.Time
	Payload map[string]any
}

func (s *Server) buildAnalysisLogs(r *http.Request, limit int) ([]map[string]any, error) {
	entries := make([]analysisEntry, 0)
	seen := map[string]bool{}
	for _, spec := range []struct {
		table    string
		eventRun bool
	}{{"research_runs", false}, {"event_research_runs", true}} {
		rows, err := s.db.Query(r.Context(), `SELECT payload::jsonb,updated_at FROM `+spec.table+` ORDER BY created_at DESC LIMIT $1`, max(limit*3, 30))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var body []byte
			var stamp time.Time
			if rows.Scan(&body, &stamp) != nil {
				continue
			}
			var run map[string]any
			if json.Unmarshal(body, &run) != nil {
				continue
			}
			eventID := stringValue(run["event_id"])
			event, _ := s.payloadByID(r, "news_events", eventID)
			if eventID != "" {
				seen[eventID] = true
			}
			entries = append(entries, analysisEntry{stamp.UTC(), s.analysisEntry(r, event, run, spec.eventRun)})
		}
		rows.Close()
	}
	rows, err := s.db.Query(r.Context(), `SELECT payload::jsonb,observed_at FROM news_events ORDER BY observed_at DESC LIMIT $1`, max(limit*3, 30))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var body []byte
		var stamp time.Time
		if rows.Scan(&body, &stamp) != nil {
			continue
		}
		var event map[string]any
		if json.Unmarshal(body, &event) != nil || seen[stringValue(event["id"])] {
			continue
		}
		if latest := eventUpdatedAt(event); latest != nil {
			stamp = *latest
		}
		entries = append(entries, analysisEntry{stamp.UTC(), s.analysisEntry(r, event, nil, false)})
	}
	rows.Close()
	sort.Slice(entries, func(i, j int) bool { return entries[i].Updated.After(entries[j].Updated) })
	if len(entries) > limit {
		entries = entries[:limit]
	}
	output := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		output = append(output, entry.Payload)
	}
	return output, nil
}

func (s *Server) analysisEntry(r *http.Request, event, run map[string]any, eventRun bool) map[string]any {
	normalizePythonTimestamps(event)
	normalizePythonTimestamps(run)
	steps := anySlice(valueFrom(run, "analysis_steps"))
	if len(steps) == 0 {
		steps = anySlice(valueFrom(event, "analysis_steps"))
	}
	models := make([]string, 0)
	seen := map[string]bool{}
	for _, raw := range steps {
		step, _ := raw.(map[string]any)
		model := stringValue(step["model"])
		if model != "" && !seen[model] {
			models = append(models, model)
			seen[model] = true
		}
	}
	news := make([]any, 0)
	for _, rawID := range anySlice(valueFrom(event, "news_item_ids")) {
		item, _ := s.payloadByID(r, "news_items", fmt.Sprint(rawID))
		if item != nil {
			news = append(news, map[string]any{"id": item["id"], "title": item["title"], "source": item["source"], "url": item["url"], "published_at": item["published_at"]})
		}
	}
	asset := valueFrom(run, "asset")
	if asset == nil && !eventRun {
		candidates := anySlice(valueFrom(event, "candidates"))
		if len(candidates) > 0 {
			candidate, _ := candidates[0].(map[string]any)
			asset = candidate["asset"]
		}
	}
	id := valueFrom(event, "id")
	runID := any(nil)
	eventRunID := any(nil)
	if run != nil {
		id = run["id"]
		if eventRun {
			eventRunID = run["id"]
		} else {
			runID = run["id"]
		}
	}
	result := any(nil)
	if run != nil {
		if eventRun {
			if report, ok := run["report"].(map[string]any); ok {
				result = map[string]any{"kind": "event_report", "confidence": report["confidence"], "news_confidence": report["news_confidence"], "news_confidence_factors": report["news_confidence_factors"], "evidence_complete": report["evidence_complete"], "summary": report["summary"], "affected_markets": report["affected_markets"], "affected_sectors": report["affected_sectors"], "scenarios": report["scenarios"], "catalysts": report["catalysts"], "risks": report["risks"], "unresolved_questions": report["unresolved_questions"], "scoring_version": report["scoring_version"], "fact_confidence": report["fact_confidence"], "macro_factors": report["macro_factors"], "impacts": report["impacts"], "trade_status": report["trade_status"], "missing_information": report["missing_information"]}
			}
		} else if recommendation, ok := run["recommendation"].(map[string]any); ok {
			result = map[string]any{"kind": "asset_recommendation", "rating": recommendation["rating"], "score": recommendation["score"], "direction_score": recommendation["direction_score"], "raw_score": recommendation["raw_score"], "confidence": recommendation["confidence"], "rating_confidence": recommendation["rating_confidence"], "fact_confidence": recommendation["fact_confidence"], "news_confidence": recommendation["news_confidence"], "news_confidence_factors": recommendation["news_confidence_factors"], "rating_confidence_factors": recommendation["rating_confidence_factors"], "mapping_distance": recommendation["mapping_distance"], "score_source": recommendation["score_source"], "scoring_version": recommendation["scoring_version"], "horizon_unit": recommendation["horizon_unit"], "horizon_days": recommendation["horizon_days"], "evidence_complete": recommendation["evidence_complete"], "directional_evidence_complete": recommendation["directional_evidence_complete"], "signal_status": recommendation["signal_status"], "summary": valueFromMap(recommendation, "thesis", "summary"), "impact": recommendation["impact"]}
		}
	}
	status := valueFrom(run, "status")
	if status == nil {
		status = eventMappingStatus(event)
	}
	updated := valueFrom(run, "updated_at")
	if updated == nil {
		updated = eventUpdatedAt(event)
	}
	if stamp := parseAnyTime(updated); stamp != nil {
		updated = iso(*stamp)
	}
	eventSummary := any(nil)
	if event != nil {
		eventSummary = map[string]any{"id": event["id"], "headline": event["headline"], "event_type": event["event_type"], "direct_impact": event["direct_impact"], "actions": defaultAny(event["actions"], []any{}), "priority": event["priority"]}
	}
	return map[string]any{"id": fmt.Sprint(id), "event_id": valueFrom(event, "id"), "run_id": runID, "event_research_run_id": eventRunID, "status": status, "updated_at": updated, "news": news, "event": eventSummary, "asset": asset, "models": models, "steps": steps, "result": result}
}

func eventUpdatedAt(event map[string]any) *time.Time {
	latest := parseAnyTime(valueFrom(event, "observed_at"))
	for _, raw := range anySlice(valueFrom(event, "analysis_steps")) {
		step, _ := raw.(map[string]any)
		stamp := parseAnyTime(step["occurred_at"])
		if stamp != nil && (latest == nil || stamp.After(*latest)) {
			latest = stamp
		}
	}
	return latest
}

func (s *Server) payloadByID(r *http.Request, table, id string) (map[string]any, error) {
	if id == "" {
		return nil, nil
	}
	var body []byte
	column := "payload"
	if table == "news_items" {
		column = `jsonb_build_object('id',id,'title',title,'source',source,'url',url,'published_at',published_at)`
	}
	err := s.db.QueryRow(r.Context(), `SELECT `+column+`::jsonb FROM `+table+` WHERE id=$1`, id).Scan(&body)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return nil, nil
	}
	return payload, nil
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, 500, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	last := ""
	for {
		snapshot, err := s.streamSnapshot(r)
		if err != nil {
			return
		}
		body, _ := json.Marshal(snapshot)
		current := string(body)
		if current != last {
			_, _ = fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", body)
			last = current
		} else {
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
		}
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *Server) streamSnapshot(r *http.Request) (map[string]any, error) {
	events, err := s.payloadRows(r, `SELECT payload::jsonb FROM news_events ORDER BY published_at DESC,priority DESC LIMIT 30`)
	if err != nil {
		return nil, err
	}
	runs, err := s.payloadRows(r, `SELECT payload::jsonb FROM research_runs ORDER BY created_at DESC LIMIT 10`)
	if err != nil {
		return nil, err
	}
	recommendations, err := s.payloadRows(r, `SELECT payload::jsonb FROM recommendations ORDER BY as_of DESC,id LIMIT 10`)
	if err != nil {
		return nil, err
	}
	eventRuns, err := s.payloadRows(r, `SELECT payload::jsonb FROM event_research_runs ORDER BY created_at DESC LIMIT 10`)
	if err != nil {
		return nil, err
	}
	logs, err := s.buildAnalysisLogs(r, 10)
	if err != nil {
		return nil, err
	}
	return map[string]any{"events": events, "runs": runs, "recommendations": recommendations, "event_research_runs": eventRuns, "analysis_logs": logs}, nil
}
func (s *Server) payloadRows(r *http.Request, query string) ([]any, error) {
	rows, err := s.db.Query(r.Context(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]any, 0)
	for rows.Next() {
		var body []byte
		if rows.Scan(&body) != nil {
			continue
		}
		var item any
		if json.Unmarshal(body, &item) == nil {
			items = append(items, item)
		}
	}
	return items, rows.Err()
}

func eventMappingStatus(event map[string]any) string {
	if event == nil {
		return "not_researched"
	}
	for index := len(anySlice(event["analysis_steps"])) - 1; index >= 0; index-- {
		step, _ := anySlice(event["analysis_steps"])[index].(map[string]any)
		phase, status := stringValue(step["phase"]), stringValue(step["status"])
		if phase == "asset_mapping" && (status == "running" || status == "retrying" || status == "failed") {
			return "mapping_" + status
		}
		if phase == "asset_mapping_queue" && status == "queued" {
			return "mapping_queued"
		}
	}
	if len(anySlice(event["candidates"])) == 0 {
		return "unmapped"
	}
	return "not_researched"
}
func anySlice(value any) []any {
	if typed, ok := value.([]any); ok {
		return typed
	}
	return []any{}
}
func valueFrom(source map[string]any, key string) any {
	if source == nil {
		return nil
	}
	return source[key]
}
func valueFromMap(source map[string]any, first, second string) any {
	nested, _ := source[first].(map[string]any)
	if nested == nil {
		return nil
	}
	return nested[second]
}
func numberValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	}
	return 0
}
func compareTimes(left, right *time.Time) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	if left.After(*right) {
		return 1
	}
	if left.Before(*right) {
		return -1
	}
	return 0
}
