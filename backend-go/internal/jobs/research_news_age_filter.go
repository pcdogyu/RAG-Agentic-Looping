package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const researchNewsAgeFilterSetting = "research-news-age-filter"

type ResearchNewsAgeFilter struct {
	Enabled     bool `json:"enabled"`
	MaxAgeHours int  `json:"max_age_hours"`
}

func DefaultResearchNewsAgeFilter() ResearchNewsAgeFilter {
	return ResearchNewsAgeFilter{Enabled: true, MaxAgeHours: 24}
}

func LoadResearchNewsAgeFilter(ctx context.Context, db *pgxpool.Pool) (ResearchNewsAgeFilter, error) {
	filter := DefaultResearchNewsAgeFilter()
	var body []byte
	err := db.QueryRow(ctx, `SELECT payload::jsonb FROM integration_settings WHERE key=$1`, researchNewsAgeFilterSetting).Scan(&body)
	if err != nil {
		return filter, nil
	}
	var stored ResearchNewsAgeFilter
	if json.Unmarshal(body, &stored) == nil {
		filter.Enabled = stored.Enabled
	}
	return filter, nil
}

func SaveResearchNewsAgeFilter(ctx context.Context, db *pgxpool.Pool, enabled bool) (ResearchNewsAgeFilter, error) {
	filter := DefaultResearchNewsAgeFilter()
	filter.Enabled = enabled
	body, _ := json.Marshal(filter)
	_, err := db.Exec(ctx, `INSERT INTO integration_settings(key,payload,updated_at) VALUES($1,$2,now()) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,updated_at=now()`, researchNewsAgeFilterSetting, body)
	return filter, err
}

func ResearchNewsExpired(filter ResearchNewsAgeFilter, publishedAt, now time.Time) bool {
	return filter.Enabled && !publishedAt.IsZero() && !publishedAt.After(now.Add(-time.Duration(filter.MaxAgeHours)*time.Hour))
}

func researchNewsAgeFilterBypass(run map[string]any) bool {
	return boolValue(run["news_age_filter_bypass"])
}

// FilterExpiredAutomaticResearch cancels only unclaimed automatic research jobs.
// Running jobs are deliberately left for the worker's pre-inference guard.
func FilterExpiredAutomaticResearch(ctx context.Context, db *pgxpool.Pool, redisClient *redis.Client) (int, error) {
	filter, err := LoadResearchNewsAgeFilter(ctx, db)
	if err != nil || !filter.Enabled {
		return 0, err
	}
	rows, err := db.Query(ctx, `
		SELECT j.id::text,j.task_type,coalesce(asset_run.id::text,event_run.id::text),
		       coalesce(asset_run.payload,event_run.payload)::jsonb,events.published_at
		FROM go_jobs j
		LEFT JOIN research_runs asset_run ON j.task_type='market_loop.research_asset' AND asset_run.payload->>'celery_task_id'=j.id::text
		LEFT JOIN event_research_runs event_run ON j.task_type='market_loop.research_event' AND event_run.payload->>'celery_task_id'=j.id::text
		JOIN news_events events ON events.id=coalesce(asset_run.event_id,event_run.event_id)
		WHERE j.queue='research' AND j.status IN ('queued','retrying')`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	filtered := 0
	for rows.Next() {
		var taskID, taskType, runID string
		var body []byte
		var publishedAt time.Time
		if rows.Scan(&taskID, &taskType, &runID, &body, &publishedAt) != nil {
			continue
		}
		var run map[string]any
		if json.Unmarshal(body, &run) != nil || researchNewsAgeFilterBypass(run) || !ResearchNewsExpired(filter, publishedAt, time.Now().UTC()) {
			continue
		}
		result, updateErr := db.Exec(ctx, `UPDATE go_jobs SET cancel_requested_at=now(),status='cancelled',completed_at=now(),updated_at=now() WHERE id=$1 AND status IN ('queued','retrying')`, taskID)
		if updateErr != nil || result.RowsAffected() != 1 {
			continue
		}
		markResearchNewsAgeFiltered(run, publishedAt)
		updated, _ := json.Marshal(run)
		table := "research_runs"
		if taskType == researchEventTask {
			table = "event_research_runs"
		}
		if _, err = db.Exec(ctx, fmt.Sprintf(`UPDATE %s SET status='filtered',payload=$2,updated_at=now() WHERE id=$1`, table), runID, updated); err != nil {
			return filtered, err
		}
		updateResearchAgeFilterTracking(ctx, redisClient, taskID)
		filtered++
	}
	return filtered, rows.Err()
}

func markResearchNewsAgeFiltered(run map[string]any, publishedAt time.Time) {
	now := iso(time.Now())
	run["status"], run["retryable_reason"], run["error"] = "filtered", "news_age_filtered", "新闻发布时间超过 24 小时，自动研究已过滤；可手动重试。"
	run["completed_at"], run["updated_at"] = now, now
	appendAnalysisStep(run, analysisStep("research_news_age_filter", "filtered", "go-worker", "新闻发布时间超过 24 小时，自动研究已过滤；可手动重试。", map[string]any{"published_at": iso(publishedAt), "max_age_hours": 24}))
}

func updateResearchAgeFilterTracking(ctx context.Context, redisClient *redis.Client, taskID string) {
	if redisClient == nil {
		return
	}
	key := "market-loop:model-queue:research:tasks"
	raw, _ := redisClient.HGet(ctx, key, taskID).Bytes()
	payload := map[string]any{}
	_ = json.Unmarshal(raw, &payload)
	payload["status"], payload["error"], payload["updated_at"], payload["completed_at"] = "filtered", "新闻发布时间超过 24 小时，自动研究已过滤；可手动重试。", iso(time.Now()), iso(time.Now())
	body, _ := json.Marshal(payload)
	_ = redisClient.HSet(ctx, key, taskID, body).Err()
	_ = redisClient.Expire(ctx, key, modelTaskTTL).Err()
}
