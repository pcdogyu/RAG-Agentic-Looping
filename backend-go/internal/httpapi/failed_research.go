package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

func (s *Server) failedResearchRuns(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 200)
	if !ok {
		return
	}
	rows, err := s.db.Query(r.Context(), `
		WITH asset_originals AS (
			SELECT rr.id,rr.updated_at,rr.payload::jsonb,e.payload::jsonb AS event_payload,
			       coalesce(retries.retry_count,0)::int AS retry_count,retries.latest_retry
			FROM research_runs rr
			LEFT JOIN news_events e ON e.id=rr.event_id
			LEFT JOIN LATERAL (
				SELECT count(*)::int AS retry_count,
				       (array_agg(jsonb_build_object('id',child.id,'status',child.status,'updated_at',child.updated_at)
				          ORDER BY child.updated_at DESC))[1] AS latest_retry
				FROM research_runs child
				WHERE child.payload->>'retry_of_run_id'=rr.id
			) retries ON true
			WHERE (rr.payload->>'retry_of_run_id') IS NULL
			  AND (rr.status='failed' OR rr.payload->>'retryable_reason' IS NOT NULL)
			  AND NOT EXISTS (
				SELECT 1 FROM recommendations rec WHERE rec.run_id=rr.id
				  AND rr.payload->>'retryable_reason' IS NULL
			  )
			  AND NOT EXISTS (
				SELECT 1 FROM research_runs child
				WHERE child.payload->>'retry_of_run_id'=rr.id::text
				  AND child.status IN ('queued','running','verifying')
			  )
			  AND NOT EXISTS (
				SELECT 1 FROM research_runs child
				JOIN recommendations rec ON rec.run_id=child.id
				WHERE child.payload->>'retry_of_run_id'=rr.id
				  AND child.payload->>'retryable_reason' IS NULL
			  )
		), event_failures AS (
			SELECT er.id,er.updated_at,er.payload::jsonb,e.payload::jsonb AS event_payload,
			       coalesce((er.payload->>'retry_count')::int,0),NULL::jsonb
			FROM event_research_runs er
			LEFT JOIN news_events e ON e.id=er.event_id
			WHERE er.status IN ('failed','insufficient_evidence')
			  AND (er.status='failed' OR er.payload->>'retryable_reason' IS NOT NULL)
		), combined AS (
			SELECT 'asset'::text kind,* FROM asset_originals
			UNION ALL SELECT 'event',* FROM event_failures
		)
		SELECT kind,id,updated_at,payload,event_payload,retry_count,latest_retry
		FROM combined ORDER BY updated_at DESC,id DESC LIMIT $1`, limit)
	if err != nil {
		slog.Error("failed research query", "error", err)
		writeError(w, http.StatusInternalServerError, "failed research query failed")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var kind string
		var id any
		var updated time.Time
		var body, eventBody, latestRetry []byte
		var retryCount int
		if err := rows.Scan(&kind, &id, &updated, &body, &eventBody, &retryCount, &latestRetry); err != nil {
			writeError(w, http.StatusInternalServerError, "failed research row is invalid")
			return
		}
		var payload, event map[string]any
		var latest any
		_ = json.Unmarshal(body, &payload)
		_ = json.Unmarshal(eventBody, &event)
		if len(latestRetry) > 0 {
			_ = json.Unmarshal(latestRetry, &latest)
		}
		var asset any
		if kind == "asset" {
			asset = payload["asset"]
		}
		var eventSummary any
		if len(event) > 0 {
			eventSummary = map[string]any{"id": event["id"], "headline": event["headline"]}
		}
		errorValue := payload["error"]
		if errorValue == nil {
			errorValue = payload["retryable_reason"]
		}
		items = append(items, map[string]any{
			"kind": kind, "id": id, "status": payload["status"], "asset": asset,
			"event": eventSummary, "error": errorValue, "updated_at": iso(updated),
			"retry_count": retryCount, "latest_retry": latest,
		})
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "failed research query failed")
		return
	}
	writeJSON(w, http.StatusOK, items)
}
