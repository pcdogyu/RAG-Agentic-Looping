package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Server) assets(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `SELECT `+assetJSON+` FROM assets`)
	if err != nil {
		writeError(w, 500, "database query failed")
		return
	}
	defer rows.Close()
	items := make([]any, 0)
	for rows.Next() {
		var body []byte
		if rows.Scan(&body) != nil {
			writeError(w, 500, "stored asset is invalid")
			return
		}
		item, _ := decodeDefault(body, map[string]any{}).(map[string]any)
		normalizeAsset(item)
		items = append(items, item)
	}
	writeJSON(w, 200, items)
}

func (s *Server) news(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	asOf, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols::jsonb,raw_metadata::jsonb FROM news_items
		WHERE $2::timestamptz IS NULL OR (observed_at <= $2 AND published_at <= $2)
		ORDER BY published_at DESC LIMIT $1`, limit, asOf)
	if err != nil {
		writeError(w, 500, "database query failed")
		return
	}
	defer rows.Close()
	items := make([]any, 0)
	for rows.Next() {
		var id, source, quality, title, summary, urlValue, language, hash string
		var published, observed, stamp time.Time
		var symbolsBody, metadataBody []byte
		if rows.Scan(&id, &source, &quality, &title, &summary, &urlValue, &language, &published, &observed, &stamp, &hash, &symbolsBody, &metadataBody) != nil {
			writeError(w, 500, "stored news is invalid")
			return
		}
		var symbols, metadata any
		_ = json.Unmarshal(symbolsBody, &symbols)
		_ = json.Unmarshal(metadataBody, &metadata)
		items = append(items, map[string]any{"id": id, "source": source, "source_quality": quality, "title": title, "summary": summary, "url": urlValue, "language": language, "published_at": jsonTime(published), "observed_at": jsonTime(observed), "as_of": jsonTime(stamp), "content_hash": hash, "symbols": symbols, "raw_metadata": metadata})
	}
	writeJSON(w, 200, items)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	asOf, ok := optionalTimeQuery(w, r, "as_of")
	if !ok {
		return
	}
	s.writePayloadRows(w, r, `SELECT payload::jsonb FROM news_events
		WHERE $2::timestamptz IS NULL OR (observed_at <= $2 AND published_at <= $2)
		ORDER BY published_at DESC,priority DESC LIMIT $1`, []any{limit, asOf})
}

func (s *Server) researchRuns(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	s.writeMappedPayloadRows(w, r, `SELECT payload FROM research_runs ORDER BY created_at DESC LIMIT $1`, []any{limit}, func(item map[string]any) {
		if asset, ok := item["asset"].(map[string]any); ok {
			normalizeAsset(asset)
		}
		if recommendation, ok := item["recommendation"].(map[string]any); ok {
			normalizeRecommendation(recommendation)
		}
	})
}

func (s *Server) researchRun(w http.ResponseWriter, r *http.Request) {
	s.writePayloadByID(w, r, "research_runs", chi.URLParam(r, "runID"), "run not found")
}

func (s *Server) eventResearchRuns(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	s.writePayloadRows(w, r, `SELECT payload::jsonb FROM event_research_runs ORDER BY created_at DESC LIMIT $1`, []any{limit})
}

func (s *Server) eventResearchRun(w http.ResponseWriter, r *http.Request) {
	s.writePayloadByID(w, r, "event_research_runs", chi.URLParam(r, "runID"), "event research run not found")
}

func (s *Server) recommendations(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 100, 1, 500)
	if !ok {
		return
	}
	s.writeMappedPayloadRows(w, r, `SELECT payload::jsonb FROM recommendations ORDER BY as_of DESC,id LIMIT $1`, []any{limit}, normalizeRecommendation)
}

func (s *Server) outcomes(w http.ResponseWriter, r *http.Request) {
	s.writePayloadRows(w, r, `SELECT payload::jsonb FROM outcomes ORDER BY observed_at DESC`, nil)
}

func (s *Server) evolutions(w http.ResponseWriter, r *http.Request) {
	s.writePayloadRows(w, r, `SELECT payload::jsonb FROM evolution_candidates ORDER BY created_at DESC`, nil)
}

func (s *Server) conclusions(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit, ok := intQuery(w, query, "limit", 20, 1, 100)
	if !ok {
		return
	}
	evidence := query.Get("evidence_status")
	if evidence != "" && evidence != "complete" && evidence != "incomplete" {
		validationError(w, "evidence_status", "Input should be '', 'complete' or 'incomplete'")
		return
	}
	var cursorTime any
	var cursorID any
	if raw := query.Get("cursor"); raw != "" {
		stamp, id, err := decodeAssetCursor(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		cursorTime, cursorID = stamp, id
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT payload::jsonb,as_of,id FROM recommendations
		WHERE ($1='' OR rating=$1)
		  AND ($2='' OR lower(payload #>> '{asset,market}')=lower($2))
		  AND ($3='' OR concat_ws(' ',payload #>> '{asset,symbol}',payload #>> '{asset,name}',payload #>> '{thesis,summary}') ILIKE '%'||$3||'%')
		  AND ($4='' OR coalesce((payload->>'evidence_complete')::boolean,false)=($4='complete'))
		  AND ($5::timestamptz IS NULL OR as_of<$5 OR (as_of=$5 AND id<$6::text))
		ORDER BY as_of DESC,id DESC LIMIT $7`, query.Get("rating"), query.Get("market"), strings.TrimSpace(query.Get("q")), evidence,
		cursorTime, cursorID, limit+1)
	if err != nil {
		writeError(w, 500, "conclusions query failed")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	var lastTime time.Time
	var lastID string
	count := 0
	for rows.Next() {
		var body []byte
		var stamp time.Time
		var id string
		if err := rows.Scan(&body, &stamp, &id); err != nil {
			writeError(w, 500, "stored conclusion is invalid")
			return
		}
		count++
		if len(items) == limit {
			continue
		}
		var item map[string]any
		if json.Unmarshal(body, &item) != nil {
			writeError(w, 500, "stored conclusion is invalid")
			return
		}
		publicRecommendation(item)
		items = append(items, item)
		lastTime, lastID = stamp, id
	}
	var next any
	if count > limit && len(items) > 0 {
		next = encodeAssetCursor(lastTime, lastID)
	}
	writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next})
}

func (s *Server) conclusionDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "recommendationID")
	var recommendationBody, runBody, eventBody []byte
	err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM recommendations WHERE id=$1`, id).Scan(&recommendationBody)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "conclusion not found")
		return
	}
	if err != nil {
		writeError(w, 500, "conclusion query failed")
		return
	}
	var recommendation map[string]any
	_ = json.Unmarshal(recommendationBody, &recommendation)
	publicRecommendation(recommendation)
	runID := stringValue(recommendation["run_id"])
	_ = s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM research_runs WHERE id=$1`, runID).Scan(&runBody)
	var run map[string]any
	_ = json.Unmarshal(runBody, &run)
	if eventID := stringValue(run["event_id"]); eventID != "" {
		_ = s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM news_events WHERE id=$1`, eventID).Scan(&eventBody)
	}
	var event map[string]any
	_ = json.Unmarshal(eventBody, &event)
	news, err := s.newsForEvent(r, event)
	if err != nil {
		writeError(w, 500, "conclusion news query failed")
		return
	}
	evidence := defaultAny(run["evidence"], []any{})
	var runValue any
	if len(run) > 0 {
		runValue = run
	}
	var eventValue any
	if len(event) > 0 {
		eventValue = event
	}
	writeJSON(w, 200, map[string]any{"recommendation": recommendation, "run": runValue, "event": eventValue, "news": news, "evidence": evidence})
}

func (s *Server) eventConclusionDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "runID")
	var runBody, eventBody []byte
	err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM event_research_runs WHERE id=$1`, id).Scan(&runBody)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "event conclusion not found")
		return
	}
	if err != nil {
		writeError(w, 500, "event conclusion query failed")
		return
	}
	var run map[string]any
	_ = json.Unmarshal(runBody, &run)
	if run["report"] == nil {
		writeError(w, 404, "event conclusion has no report")
		return
	}
	eventID := stringValue(run["event_id"])
	_ = s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM news_events WHERE id=$1`, eventID).Scan(&eventBody)
	var event map[string]any
	_ = json.Unmarshal(eventBody, &event)
	news, err := s.newsForEvent(r, event)
	if err != nil {
		writeError(w, 500, "event conclusion news query failed")
		return
	}
	writeJSON(w, 200, map[string]any{"run": run, "refresh": publicFullEventResearch(run), "event": event, "report": run["report"], "news": news, "evidence": defaultAny(run["evidence"], []any{})})
}

func (s *Server) newsForEvent(r *http.Request, event map[string]any) ([]any, error) {
	ids, _ := event["news_item_ids"].([]any)
	result := make([]any, 0, len(ids))
	for _, raw := range ids {
		var body []byte
		if err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM news_items WHERE id=$1`, fmt.Sprint(raw)).Scan(&body); err == nil {
			var item any
			if json.Unmarshal(body, &item) == nil {
				result = append(result, item)
			}
		}
	}
	return result, nil
}

func (s *Server) writePayloadRows(w http.ResponseWriter, r *http.Request, query string, args []any) {
	rows, err := s.db.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, 500, "database query failed")
		return
	}
	defer rows.Close()
	items := make([]any, 0)
	for rows.Next() {
		var body []byte
		if rows.Scan(&body) != nil {
			writeError(w, 500, "stored payload is invalid")
			return
		}
		var item any
		if json.Unmarshal(body, &item) != nil {
			writeError(w, 500, "stored payload is invalid")
			return
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		writeError(w, 500, "database query failed")
		return
	}
	writeJSON(w, 200, items)
}

func (s *Server) writeMappedPayloadRows(w http.ResponseWriter, r *http.Request, query string, args []any, transform func(map[string]any)) {
	rows, err := s.db.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, 500, "database query failed")
		return
	}
	defer rows.Close()
	items := make([]any, 0)
	for rows.Next() {
		var body []byte
		if rows.Scan(&body) != nil {
			writeError(w, 500, "stored payload is invalid")
			return
		}
		var item map[string]any
		if json.Unmarshal(body, &item) != nil {
			writeError(w, 500, "stored payload is invalid")
			return
		}
		transform(item)
		items = append(items, item)
	}
	if rows.Err() != nil {
		writeError(w, 500, "database query failed")
		return
	}
	writeJSON(w, 200, items)
}

func (s *Server) writePayloadByID(w http.ResponseWriter, r *http.Request, table, id, notFound string) {
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, 422, "Input should be a valid UUID")
		return
	}
	query := `SELECT payload::jsonb FROM ` + table + ` WHERE id=$1`
	var body []byte
	err := s.db.QueryRow(r.Context(), query, id).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, notFound)
		return
	}
	if err != nil {
		writeError(w, 500, "database query failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(body)
}

func optionalTimeQuery(w http.ResponseWriter, r *http.Request, name string) (any, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return nil, true
	}
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		validationError(w, name, "Input should be a valid datetime")
		return nil, false
	}
	return value, true
}

func decodeAssetCursor(value string) (time.Time, string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("invalid cursor")
	}
	stamp, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", err
	}
	if _, err = uuid.Parse(parts[1]); err != nil {
		return time.Time{}, "", err
	}
	return stamp, parts[1], nil
}
func encodeAssetCursor(stamp time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(iso(stamp) + "|" + id))
}

func jsonTime(value time.Time) string { return pythonTimestamp(value, "Z") }
