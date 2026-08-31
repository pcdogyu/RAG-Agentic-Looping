package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type conclusionCursor struct {
	Time time.Time
	Kind string
	ID   uuid.UUID
}

type conclusionRow struct {
	Kind       string
	ID         uuid.UUID
	OccurredAt time.Time
	Payload    []byte
	RunPayload []byte
	Event      []byte
}

func (s *Server) researchConclusions(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	kind := defaultValue(query.Get("kind"), "all")
	if kind != "all" && kind != "asset" && kind != "event" {
		validationError(w, "kind", "Input should be 'all', 'event' or 'asset'")
		return
	}
	limit, ok := intQuery(w, query, "limit", 20, 1, 100)
	if !ok {
		return
	}
	evidence := query.Get("evidence_status")
	if evidence != "" && evidence != "complete" && evidence != "incomplete" {
		validationError(w, "evidence_status", "Input should be '', 'complete' or 'incomplete'")
		return
	}
	var cursor *conclusionCursor
	if raw := query.Get("cursor"); raw != "" {
		decoded, err := decodeCursor(raw)
		if err != nil {
			writeError(w, http.StatusUnprocessableEntity, "invalid cursor")
			return
		}
		cursor = &decoded
	}
	rows, err := s.queryConclusions(r, kind, query.Get("q"), query.Get("market"), query.Get("rating"), evidence, cursor, limit+1)
	if err != nil {
		slog.Error("conclusions query", "error", err)
		writeError(w, http.StatusInternalServerError, "conclusions query failed")
		return
	}
	items := make([]map[string]any, 0, min(limit, len(rows)))
	for index, row := range rows {
		if index == limit {
			break
		}
		item, err := conclusionItem(row)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "stored conclusion is invalid")
			return
		}
		items = append(items, item)
	}
	var next any
	if len(rows) > limit && len(items) > 0 {
		last := rows[limit-1]
		next = encodeCursor(last.OccurredAt, last.Kind, last.ID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s *Server) queryConclusions(r *http.Request, kind, search, market, rating, evidence string, cursor *conclusionCursor, limit int) ([]conclusionRow, error) {
	var cursorTime any
	var cursorKind int
	var cursorID any
	if cursor != nil {
		cursorTime, cursorKind, cursorID = cursor.Time, kindOrder(cursor.Kind), cursor.ID.String()
	}
	rows, err := s.db.Query(r.Context(), `
		WITH feed AS (
			SELECT 'asset'::text AS kind, r.id, r.as_of AS occurred_at, r.payload::jsonb,
			       rr.payload::jsonb AS run_payload, NULL::jsonb AS event_payload, 1 AS kind_order
			FROM recommendations r
			LEFT JOIN research_runs rr ON rr.id=r.run_id
			WHERE $1 IN ('all','asset')
			  AND (rr.payload->>'retryable_reason') IS NULL
			  AND ($3='' OR lower(r.payload #>> '{asset,market}')=lower($3))
			  AND ($4='' OR r.rating=$4)
			  AND ($5='' OR coalesce((r.payload->>'evidence_complete')::boolean,false)=($5='complete'))
			  AND ($2='' OR concat_ws(' ',r.payload #>> '{asset,symbol}',r.payload #>> '{asset,name}',r.payload #>> '{thesis,summary}') ILIKE '%'||$2||'%')
			UNION ALL
			SELECT 'event', er.id, er.updated_at, er.payload::jsonb, NULL::jsonb, e.payload::jsonb, 0
			FROM event_research_runs er
			LEFT JOIN news_events e ON e.id=er.event_id
			WHERE $1 IN ('all','event') AND $3='' AND $4=''
			  AND er.status IN ('completed','insufficient_evidence')
			  AND (er.payload->>'retryable_reason') IS NULL
			  AND er.payload->'report' IS NOT NULL
			  AND ($5='' OR coalesce((er.payload #>> '{report,evidence_complete}')::boolean,false)=($5='complete'))
			  AND ($2='' OR concat_ws(' ',e.headline,er.payload #>> '{report,summary}',er.payload #>> '{report,affected_markets}',er.payload #>> '{report,affected_sectors}') ILIKE '%'||$2||'%')
		)
		SELECT kind,id,occurred_at,payload,run_payload,event_payload
		FROM feed
		WHERE $6::timestamptz IS NULL OR (occurred_at,kind_order,id) < ($6, $7, $8::text)
		ORDER BY occurred_at DESC,kind_order DESC,id DESC
		LIMIT $9`, kind, strings.TrimSpace(search), strings.TrimSpace(market), strings.TrimSpace(rating), evidence,
		cursorTime, cursorKind, cursorID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []conclusionRow
	for rows.Next() {
		var row conclusionRow
		if err := rows.Scan(&row.Kind, &row.ID, &row.OccurredAt, &row.Payload, &row.RunPayload, &row.Event); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func conclusionItem(row conclusionRow) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal(row.Payload, &payload); err != nil {
		return nil, err
	}
	if row.Kind == "asset" {
		asset, _ := payload["asset"].(map[string]any)
		thesis, _ := payload["thesis"].(map[string]any)
		publicRecommendation(payload)
		return map[string]any{
			"kind": "asset", "id": row.ID, "occurred_at": iso(row.OccurredAt),
			"status": payload["signal_status"], "evidence_complete": boolValue(payload["evidence_complete"]),
			"title": fmt.Sprintf("%v · %v", asset["symbol"], asset["name"]), "summary": thesis["summary"],
			"asset": asset, "event": nil, "recommendation": payload, "report": nil,
		}, nil
	}
	report, _ := payload["report"].(map[string]any)
	var event map[string]any
	_ = json.Unmarshal(row.Event, &event)
	headline := stringValue(event["headline"])
	if headline == "" {
		headline = stringValue(report["summary"])
	}
	impacts := sanitizePublishedImpacts(report["impacts"])
	directionScore, rating := representativeImpact(impacts)
	return map[string]any{
		"kind": "event", "id": row.ID, "occurred_at": iso(row.OccurredAt), "status": payload["status"],
		"evidence_complete": boolValue(report["evidence_complete"]), "title": headline, "summary": report["summary"],
		"asset":          nil,
		"event":          map[string]any{"id": payload["event_id"], "headline": headline, "event_type": defaultAny(event["event_type"], "other")},
		"recommendation": nil,
		"refresh":        publicFullEventResearch(payload),
		"report": map[string]any{"confidence": report["confidence"], "news_confidence": report["news_confidence"],
			"direction_score": directionScore, "rating": rating,
			"impact_count": len(impacts), "affected_markets": defaultAny(report["affected_markets"], []any{}),
			"affected_sectors": defaultAny(report["affected_sectors"], []any{}), "scoring_version": report["scoring_version"]},
	}, nil
}

func representativeImpact(impacts []any) (any, any) {
	var representative map[string]any
	var representativeScore any
	strongest := -1.0
	for _, raw := range impacts {
		impact, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		score, strength, ok := normalizedImpactScore(impact)
		if !ok || strength <= strongest {
			continue
		}
		representative = impact
		representativeScore = score
		strongest = strength
	}
	if representative == nil {
		return nil, nil
	}
	return representativeScore, representative["rating"]
}

func normalizedImpactScore(impact map[string]any) (any, float64, bool) {
	if score, ok := impact["direction_score"].(float64); ok {
		return score, math.Abs(score), true
	}
	if score, ok := impact["score"].(float64); ok {
		points := math.Round(score * 100)
		return points, math.Abs(points), true
	}
	return nil, 0, false
}

func publicRecommendation(payload map[string]any) {
	normalizeRecommendation(payload)
	status := stringValue(payload["signal_status"])
	version := stringValue(payload["scoring_version"])
	directionVerified := boolValue(payload["direction_verified"])
	available := status != "technical_failure" && (version == "llm-direction-v3" || version == "short-term-impact-v1" || (directionVerified && status != "insufficient_evidence"))
	payload["score_available"] = available
	if !available {
		for _, key := range []string{"score", "direction_score", "model_score", "raw_score"} {
			payload[key] = nil
		}
	}
}

func normalizeRecommendation(payload map[string]any) {
	if payload == nil {
		return
	}
	if asset, ok := payload["asset"].(map[string]any); ok {
		normalizeAsset(asset)
	}
	if impact, ok := payload["impact"].(map[string]any); ok {
		if asset, ok := impact["asset"].(map[string]any); ok {
			normalizeAsset(asset)
		}
	}
	if value, ok := payload["direction_score"]; !ok || value == nil {
		payload["direction_score"] = payload["score"]
	}
	if value, ok := payload["rating_confidence"]; !ok || value == nil {
		payload["rating_confidence"] = payload["confidence"]
	}
	if value, ok := payload["news_confidence"]; !ok || value == nil {
		payload["news_confidence"] = payload["fact_confidence"]
	}
	if numberValue(payload["raw_score"]) == 0 && numberValue(payload["score"]) != 0 {
		payload["raw_score"] = payload["score"]
	}
	if value, ok := payload["signal_status"]; !ok || value == nil || stringValue(value) == "" {
		score := numberValue(payload["score"])
		version := stringValue(payload["scoring_version"])
		switch {
		case version == "llm-direction-v3":
			if direction := payload["direction_score"]; direction != nil {
				score = numberValue(direction)
			}
			if math.Abs(score) < 30 {
				payload["signal_status"] = "neutral"
			} else {
				payload["signal_status"] = "directional"
			}
		case version == "short-term-impact-v1":
			if math.Abs(score) < 15 {
				payload["signal_status"] = "neutral"
			} else {
				payload["signal_status"] = "directional"
			}
		case !boolValue(payload["evidence_complete"]):
			payload["signal_status"] = "insufficient_evidence"
		case math.Abs(score) < 20:
			payload["signal_status"] = "neutral"
		default:
			payload["signal_status"] = "directional"
		}
	}
	defaults := map[string]any{
		"news_confidence_version": nil, "news_confidence_factors": nil,
		"rating_confidence_factors": nil, "mapping_distance": float64(5),
		"score_source": "llm", "impact": nil,
	}
	for key, value := range defaults {
		if _, ok := payload[key]; !ok {
			payload[key] = value
		}
	}
}

func normalizeAsset(asset map[string]any) {
	if asset == nil {
		return
	}
	defaults := map[string]any{
		"sector_id": "", "industry_id": "", "raw_sector": "", "raw_industry": "",
		"instrument_type": "", "market_cap": nil, "market_cap_rank": nil, "last_synced_at": nil,
		"association_tier": "standard", "association_reason": "provider_verified",
		"issuer_id": nil, "primary_listing_asset_id": nil, "lot_size": float64(1), "active": true,
	}
	for key, value := range defaults {
		if _, ok := asset[key]; !ok {
			asset[key] = value
		}
	}
	if stamp := parseAnyTime(asset["last_synced_at"]); stamp != nil {
		asset["last_synced_at"] = jsonTime(*stamp)
	}
}

func publicFullEventResearch(run map[string]any) any {
	steps, _ := run["analysis_steps"].([]any)
	for index := len(steps) - 1; index >= 0; index-- {
		step, _ := steps[index].(map[string]any)
		if stringValue(step["phase"]) != "full_event_research" {
			continue
		}
		if stringValue(step["status"]) == "completed" {
			return nil
		}
		metrics, _ := step["metrics"].(map[string]any)
		stage := stringValue(metrics["stage"])
		if stage == "" {
			stage = "event_extraction"
		}
		return map[string]any{"status": step["status"], "stage": stage, "error": metrics["error"]}
	}
	return nil
}

func decodeCursor(value string) (conclusionCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return conclusionCursor{}, err
	}
	parts := strings.SplitN(string(decoded), "|", 3)
	if len(parts) != 3 || (parts[1] != "event" && parts[1] != "asset") {
		return conclusionCursor{}, fmt.Errorf("invalid cursor")
	}
	stamp, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return conclusionCursor{}, err
	}
	id, err := uuid.Parse(parts[2])
	return conclusionCursor{Time: stamp, Kind: parts[1], ID: id}, err
}

func encodeCursor(stamp time.Time, kind string, id uuid.UUID) string {
	raw := iso(stamp) + "|" + kind + "|" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func intQuery(w http.ResponseWriter, values url.Values, name string, fallback, low, high int) (int, bool) {
	raw := values.Get(name)
	if raw == "" {
		return fallback, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < low || value > high {
		validationError(w, name, fmt.Sprintf("Input should be between %d and %d", low, high))
		return 0, false
	}
	return value, true
}

func kindOrder(kind string) int {
	if kind == "asset" {
		return 1
	}
	return 0
}
func defaultValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
func defaultAny(value, fallback any) any {
	if value == nil {
		return fallback
	}
	return value
}
func stringValue(value any) string { result, _ := value.(string); return result }
func boolValue(value any) bool     { result, _ := value.(bool); return result }
func iso(value time.Time) string {
	return pythonTimestamp(value, "+00:00")
}

func pythonTimestamp(value time.Time, suffix string) string {
	value = value.UTC()
	if value.Nanosecond() == 0 {
		return value.Format("2006-01-02T15:04:05") + suffix
	}
	return value.Format("2006-01-02T15:04:05.000000") + suffix
}
