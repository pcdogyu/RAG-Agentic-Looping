package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const newsMaxAge = 48 * time.Hour

type ResearchNewsAgeFilter struct {
	Enabled     bool `json:"enabled"`
	MaxAgeHours int  `json:"max_age_hours"`
}

func DefaultResearchNewsAgeFilter() ResearchNewsAgeFilter {
	return ResearchNewsAgeFilter{Enabled: true, MaxAgeHours: 48}
}

func LoadResearchNewsAgeFilter(ctx context.Context, db *pgxpool.Pool) (ResearchNewsAgeFilter, error) {
	return DefaultResearchNewsAgeFilter(), nil
}

func ResearchNewsExpired(filter ResearchNewsAgeFilter, publishedAt, now time.Time) bool {
	return !publishedAt.IsZero() && publishedAt.Before(now.Add(-newsMaxAge))
}

// newsPublicationSQL resolves the original publication time, never queue time.
// Jobs without a source news/event (for example code evolution or standalone
// fundamental research) are deliberately outside this news-age policy.
const newsPublicationSQL = `CASE j.task_type
    WHEN 'market_loop.extract_news_item' THEN (SELECT published_at FROM news_items WHERE id::text=j.payload->'args'->>1)
    WHEN 'market_loop.retry_news_item' THEN (SELECT published_at FROM news_items WHERE id::text=j.payload->'args'->>0)
    WHEN 'market_loop.reextract_event' THEN (SELECT published_at FROM news_events WHERE id::text=j.payload->'args'->>0)
    WHEN 'market_loop.resolve_event_assets' THEN (SELECT published_at FROM news_events WHERE id::text=j.payload->'args'->>0)
    WHEN 'market_loop.research_event' THEN (SELECT published_at FROM news_events WHERE id::text=j.payload->'args'->>0)
    WHEN 'market_loop.research_asset' THEN (SELECT published_at FROM news_events WHERE id::text=j.payload->'args'->>1)
END`

const newsAgeFilteredMessage = "新闻发布时间超过 48 小时，任务已过滤。"

// DiscardExpiredNewsJobs closes queued/retrying news tasks in bounded batches.
// Terminal rows and unrelated work are retained; the scheduler repeats this
// so an item that ages out while waiting is removed before inference.
func DiscardExpiredNewsJobs(ctx context.Context, db *pgxpool.Pool, limit int) (int64, error) {
	if limit < 1 || limit > 1000 {
		limit = 500
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var count int64
	err = tx.QueryRow(ctx, fmt.Sprintf(`WITH expired AS (
        SELECT j.id FROM go_jobs j
        WHERE j.queue IN ('extract','assist','research')
          AND j.status IN ('queued','retrying')
          AND %s < now()-interval '48 hours'
        ORDER BY j.created_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT $1
    ), cancelled AS (
        UPDATE go_jobs j SET status='cancelled',cancel_requested_at=now(),completed_at=now(),updated_at=now(),
            error=$2,result=jsonb_build_object('status','filtered','reason','news_age_filtered')
        FROM expired WHERE j.id=expired.id
        RETURNING j.id,j.task_type,j.payload
    ), event_runs AS (
        UPDATE event_research_runs r SET status='filtered',updated_at=now(),
            payload=(r.payload::jsonb || jsonb_build_object('status','filtered','retryable_reason','news_age_filtered','error',$2,'completed_at',now(),'updated_at',now()))::json
        FROM cancelled c WHERE c.task_type='market_loop.research_event' AND r.id::text=c.payload->'args'->>1
        RETURNING r.id
    ), asset_runs AS (
        UPDATE research_runs r SET status='filtered',updated_at=now(),
            payload=(r.payload::jsonb || jsonb_build_object('status','filtered','retryable_reason','news_age_filtered','error',$2,'completed_at',now(),'updated_at',now()))::json
        FROM cancelled c WHERE c.task_type='market_loop.research_asset' AND r.id::text=c.payload->'args'->>2
        RETURNING r.id
    ), news_state AS (
        UPDATE news_processing p SET status='cancelled',last_error=$2,completed_at=now(),updated_at=now()
        FROM cancelled c WHERE c.task_type IN ('market_loop.extract_news_item','market_loop.retry_news_item')
          AND p.news_id::text=CASE WHEN c.task_type='market_loop.extract_news_item' THEN c.payload->'args'->>1 ELSE c.payload->'args'->>0 END
        RETURNING p.news_id
    ) SELECT count(*) FROM cancelled`, newsPublicationSQL), limit, newsAgeFilteredMessage).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, tx.Commit(ctx)
}

// DiscardExpiredNewsOutbox prevents old news from being re-enqueued by the
// durable discovery dispatcher, including after a scheduler restart.
func DiscardExpiredNewsOutbox(ctx context.Context, db *pgxpool.Pool) (int64, error) {
	var count int64
	err := db.QueryRow(ctx, `WITH expired AS (
        UPDATE news_processing_outbox o SET status='cancelled',last_error=$1,updated_at=now()
        FROM news_items n WHERE o.news_id=n.id AND o.status IN ('pending','failed','dispatching')
          AND n.published_at<now()-interval '48 hours' RETURNING o.news_id
    ), state AS (
        UPDATE news_processing p SET status='cancelled',last_error=$1,completed_at=now(),updated_at=now()
        FROM expired e WHERE p.news_id=e.news_id AND p.status NOT IN ('completed','cancelled') RETURNING p.news_id
    ) SELECT count(*) FROM expired`, newsAgeFilteredMessage).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// DiscardExpiredClaimedNews catches the race where a job ages out between
// scheduler sweeps and also covers manual retries and already claimed work.
func DiscardExpiredClaimedNews(ctx context.Context, db *pgxpool.Pool, job Job) (bool, error) {
	if job.Queue != "extract" && job.Queue != "assist" && job.Queue != "research" {
		return false, nil
	}
	var publishedAt *time.Time
	err := db.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM go_jobs j WHERE j.id=$1`, newsPublicationSQL), job.ID).Scan(&publishedAt)
	if err != nil {
		return false, err
	}
	if publishedAt == nil || !ResearchNewsExpired(DefaultResearchNewsAgeFilter(), *publishedAt, time.Now().UTC()) {
		return false, nil
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	_, err = tx.Exec(ctx, `UPDATE go_jobs SET cancel_requested_at=now(),error=$2,
        result=jsonb_build_object('status','filtered','reason','news_age_filtered') WHERE id=$1 AND status='running'`, job.ID, newsAgeFilteredMessage)
	if err != nil {
		return false, err
	}
	switch job.TaskType {
	case researchEventTask, researchAssetTask:
		var runID string
		var run map[string]any
		var args taskEnvelope
		if err = json.Unmarshal(job.Payload, &args); err != nil {
			return false, err
		}
		index, table := 2, "research_runs"
		if job.TaskType == researchEventTask {
			index, table = 1, "event_research_runs"
		}
		if len(args.Args) <= index {
			return false, fmt.Errorf("missing research run id")
		}
		runID = fmt.Sprint(args.Args[index])
		var body []byte
		if err = tx.QueryRow(ctx, `SELECT payload::jsonb FROM `+table+` WHERE id::text=$1`, runID).Scan(&body); err != nil {
			return false, err
		}
		if err = json.Unmarshal(body, &run); err != nil {
			return false, err
		}
		markResearchNewsAgeFiltered(run, *publishedAt)
		body, _ = json.Marshal(run)
		if _, err = tx.Exec(ctx, `UPDATE `+table+` SET status='filtered',payload=$2,updated_at=now() WHERE id::text=$1`, runID, body); err != nil {
			return false, err
		}
	case extractTask, retryNewsTask:
		var args taskEnvelope
		if err = json.Unmarshal(job.Payload, &args); err != nil {
			return false, err
		}
		index := 0
		if job.TaskType == extractTask {
			index = 1
		}
		if len(args.Args) > index {
			_, err = tx.Exec(ctx, `UPDATE news_processing SET status='cancelled',last_error=$2,completed_at=now(),updated_at=now() WHERE news_id::text=$1`, fmt.Sprint(args.Args[index]), newsAgeFilteredMessage)
			if err != nil {
				return false, err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func markResearchNewsAgeFiltered(run map[string]any, publishedAt time.Time) {
	now := iso(time.Now())
	run["status"], run["retryable_reason"], run["error"] = "filtered", "news_age_filtered", newsAgeFilteredMessage
	run["completed_at"], run["updated_at"] = now, now
	appendAnalysisStep(run, analysisStep("research_news_age_filter", "filtered", "go-worker", newsAgeFilteredMessage, map[string]any{"published_at": iso(publishedAt), "max_age_hours": 48}))
}
