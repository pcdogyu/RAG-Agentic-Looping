package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

func (s *Server) failedResearchRuns(w http.ResponseWriter, r *http.Request) {
	limit, ok := intQuery(w, r.URL.Query(), "limit", 50, 1, 200)
	if !ok {
		return
	}
	rows, err := s.db.Query(r.Context(), `
		WITH runs AS MATERIALIZED (
			SELECT rr.id,rr.event_id,rr.status,rr.payload::jsonb,rr.created_at,rr.updated_at,
			       nullif(rr.payload->>'retry_of_run_id','') AS retry_of_run_id,
			       nullif(rr.payload->>'retryable_reason','') AS retryable_reason
			FROM research_runs rr
		), recommendation_runs AS MATERIALIZED (
			SELECT run_id FROM recommendations
		), retry_rollup AS (
			SELECT child.retry_of_run_id,count(*)::int AS retry_count,
			       (array_agg(jsonb_build_object(
			          'id',child.id,
			          'status',child.payload->>'status',
			          'updated_at',child.payload->>'updated_at')
			          ORDER BY child.created_at DESC))[1] AS latest_retry,
			       bool_or(child.status IN ('queued','running','verifying')) AS has_active,
			       bool_or(child.retryable_reason IS NULL AND rec.run_id IS NOT NULL) AS has_recommendation
			FROM runs child
			LEFT JOIN recommendation_runs rec ON rec.run_id=child.id
			WHERE child.retry_of_run_id IS NOT NULL
			GROUP BY child.retry_of_run_id
		), asset_originals AS (
			SELECT rr.id,rr.updated_at,rr.payload::jsonb,e.payload::jsonb AS event_payload,
			       coalesce(retries.retry_count,0)::int AS retry_count,retries.latest_retry,
			       coalesce(model_failure.error,'') AS model_error
			FROM runs rr
			LEFT JOIN news_events e ON e.id=rr.event_id
			LEFT JOIN retry_rollup retries ON retries.retry_of_run_id=rr.id
			LEFT JOIN recommendation_runs own_rec ON own_rec.run_id=rr.id
			LEFT JOIN LATERAL (
				SELECT audit.error
				FROM model_call_audits audit
				WHERE audit.entity_id=rr.id::text AND audit.status='failed' AND nullif(audit.error,'') IS NOT NULL
				ORDER BY coalesce(audit.completed_at,audit.started_at) DESC,audit.started_at DESC,audit.id DESC
				LIMIT 1
			) model_failure ON true
			WHERE rr.retry_of_run_id IS NULL
			  AND (rr.status='failed' OR rr.retryable_reason IS NOT NULL)
			  AND NOT (rr.retryable_reason IS NULL AND own_rec.run_id IS NOT NULL)
			  AND NOT coalesce(retries.has_active,false)
			  AND NOT coalesce(retries.has_recommendation,false)
		), event_failures AS (
			SELECT er.id,er.updated_at,er.payload::jsonb,e.payload::jsonb AS event_payload,
			       coalesce((er.payload->>'retry_count')::int,0),NULL::jsonb,
			       coalesce(model_failure.error,'') AS model_error
			FROM event_research_runs er
			LEFT JOIN news_events e ON e.id=er.event_id
			LEFT JOIN LATERAL (
				SELECT audit.error
				FROM model_call_audits audit
				WHERE audit.entity_id=er.id::text AND audit.status='failed' AND nullif(audit.error,'') IS NOT NULL
				ORDER BY coalesce(audit.completed_at,audit.started_at) DESC,audit.started_at DESC,audit.id DESC
				LIMIT 1
			) model_failure ON true
			WHERE er.status IN ('failed','insufficient_evidence')
			  AND (er.status='failed' OR er.payload->>'retryable_reason' IS NOT NULL)
			  AND NOT EXISTS (
				SELECT 1
				FROM event_research_runs replacement
				LEFT JOIN news_events replacement_event ON replacement_event.id=replacement.event_id
				WHERE replacement.id<>er.id
				  AND (
					replacement.event_id=er.event_id
					OR (
						nullif(btrim(e.payload->>'headline'),'') IS NOT NULL
						AND lower(btrim(replacement_event.payload->>'headline'))=lower(btrim(e.payload->>'headline'))
					)
				  )
				  AND (
					replacement.status IN ('queued','running','verifying')
					OR (
						replacement.updated_at>=er.updated_at
						AND replacement.status IN ('completed','insufficient_evidence')
						AND replacement.payload->>'retryable_reason' IS NULL
						AND replacement.payload->'report' IS NOT NULL
					)
				  )
			  )
		), combined AS (
			SELECT 'asset'::text kind,* FROM asset_originals
			UNION ALL SELECT 'event',* FROM event_failures
		)
		SELECT kind,id,updated_at,payload,event_payload,retry_count,latest_retry,model_error
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
		var modelError string
		var retryCount int
		if err := rows.Scan(&kind, &id, &updated, &body, &eventBody, &retryCount, &latestRetry, &modelError); err != nil {
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
			if value, ok := asset.(map[string]any); ok {
				normalizeAsset(value)
			}
		}
		var eventSummary any
		if len(event) > 0 {
			eventSummary = map[string]any{"id": event["id"], "headline": event["headline"]}
		}
		items = append(items, map[string]any{
			"kind": kind, "id": id, "status": payload["status"], "asset": asset,
			"event": eventSummary, "error": failedResearchError(payload, modelError),
			"error_code": payload["retryable_reason"], "updated_at": iso(updated),
			"retry_count": retryCount, "latest_retry": latest,
		})
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "failed research query failed")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func failedResearchError(payload map[string]any, auditedError string) any {
	rawError := strings.TrimSpace(stringValue(payload["error"]))
	reason := strings.TrimSpace(stringValue(payload["retryable_reason"]))
	detail := strings.ToLower(strings.Join([]string{auditedError, rawError, reason}, " "))

	switch {
	case strings.Contains(detail, "timed out waiting for the research inference slot"):
		return "研究模型排队超时：等待可用研究实例超过限制，可重新执行。"
	case strings.Contains(detail, "research_time_limit"):
		return "研究任务超过执行时限，可重新执行。"
	case strings.Contains(detail, "readtimeout"),
		strings.Contains(detail, "read timeout"),
		strings.Contains(detail, "context deadline exceeded"),
		strings.Contains(detail, "deadline exceeded"),
		strings.Contains(detail, "timed out"):
		return "研究模型响应超时：本次生成超过时限，可重新执行。"
	case strings.Contains(detail, "connection refused"),
		strings.Contains(detail, "connection reset"),
		strings.Contains(detail, "no healthy research"),
		strings.Contains(detail, "dial tcp"),
		strings.Contains(detail, "url.error"):
		return "研究模型实例暂不可用：连接失败，可重新执行。"
	case strings.Contains(detail, "llmresponseerror"),
		strings.Contains(detail, "validationerror"),
		strings.Contains(detail, "invalid json"):
		return "研究模型返回格式无效，可重新执行。"
	case strings.Contains(strings.ToLower(reason), "model_llmerror"):
		return "研究模型调用未完成：排队超时或实例暂不可用，可重新执行。"
	case rawError != "":
		return rawError
	case reason != "":
		return reason
	default:
		return nil
	}
}
