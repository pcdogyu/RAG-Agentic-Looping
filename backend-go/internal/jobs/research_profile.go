package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/sourcefilter"
)

const (
	researchProfileFast = "fast"
	researchProfileDeep = "deep"
)

func loadSourceFilterConfig(ctx context.Context, db *pgxpool.Pool) (sourcefilter.Config, error) {
	value := sourcefilter.Config{Enabled: true, Blacklist: []string{"天气"}}
	var body []byte
	err := db.QueryRow(ctx, `SELECT payload::jsonb FROM integration_settings WHERE key='source-filter'`).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return value, nil
	}
	if err != nil {
		return value, err
	}
	payload := map[string]any{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return value, err
	}
	value.Enabled = boolDefault(payload["enabled"], true)
	value.Whitelist = stringSlice(payload["whitelist_keywords"])
	value.Blacklist = stringSlice(payload["blacklist_keywords"])
	return value, nil
}

func eventResearchProfile(event map[string]any, manual bool) (string, string, []string) {
	if manual {
		return researchProfileDeep, "manual", stringSlice(event["matched_whitelist_keywords"])
	}
	if stringValue(event["research_profile"]) == researchProfileDeep {
		return researchProfileDeep, "whitelist_match", stringSlice(event["matched_whitelist_keywords"])
	}
	return researchProfileFast, "default_fast", []string{}
}

// ReclassifyQueuedResearchProfiles snapshots the current whitelist routing on
// work that has not started. Existing running and terminal research is never
// rewritten, so audits remain tied to the rule used when execution began.
func ReclassifyQueuedResearchProfiles(ctx context.Context, db *pgxpool.Pool) (int, error) {
	cfg, err := loadSourceFilterConfig(ctx, db)
	if err != nil {
		return 0, err
	}
	rows, err := db.Query(ctx, `
		SELECT j.id::text,j.task_type,j.payload::jsonb,
		       coalesce(er.id::text,rr.id::text,''),coalesce(er.payload::jsonb,rr.payload::jsonb,'{}'::jsonb),
		       coalesce(ev.id::text,''),coalesce(ev.payload::jsonb,'{}'::jsonb),
		       coalesce(titles.values,ARRAY[]::text[])
		FROM go_jobs j
		LEFT JOIN event_research_runs er ON j.task_type='market_loop.research_event' AND er.id::text=j.payload->'args'->>1
		LEFT JOIN research_runs rr ON j.task_type='market_loop.research_asset' AND rr.id::text=j.payload->'args'->>2
		LEFT JOIN news_events ev ON ev.id=coalesce(er.event_id,rr.event_id)
		LEFT JOIN LATERAL (
			SELECT array_agg(ni.title ORDER BY ni.published_at,ni.id) AS values
			FROM jsonb_array_elements_text(coalesce(ev.payload::jsonb->'news_item_ids','[]'::jsonb)) member(news_id)
			JOIN news_items ni ON ni.id::text=member.news_id
		) titles ON true
		WHERE j.queue='research' AND j.status IN ('queued','retrying')
		ORDER BY j.created_at,j.id`)
	if err != nil {
		return 0, err
	}
	type queued struct {
		jobID, taskType, runID, eventID string
		job, run, event                 map[string]any
		titles                          []string
	}
	items := make([]queued, 0)
	for rows.Next() {
		var item queued
		var jobBody, runBody, eventBody []byte
		if err := rows.Scan(&item.jobID, &item.taskType, &jobBody, &item.runID, &runBody, &item.eventID, &eventBody, &item.titles); err != nil {
			rows.Close()
			return 0, err
		}
		_ = json.Unmarshal(jobBody, &item.job)
		_ = json.Unmarshal(runBody, &item.run)
		_ = json.Unmarshal(eventBody, &item.event)
		if headline := stringValue(item.event["headline"]); headline != "" {
			item.titles = append(item.titles, headline)
		}
		items = append(items, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	batch := &pgx.Batch{}
	updated := 0
	for _, item := range items {
		kwargs := objectValue(item.job["kwargs"])
		manual := strings.EqualFold(stringValue(kwargs["source"]), "manual") || boolValue(kwargs["news_age_filter_bypass"]) || item.run["retry_of_run_id"] != nil || numberValue(item.run["retry_count"]) > 0
		eventProfile, eventReason := researchProfileFast, "default_fast"
		matched, matchedBlacklist := []string{}, []string{}
		if cfg.Enabled {
			for _, title := range item.titles {
				decision := sourcefilter.Evaluate(title, cfg)
				matchedBlacklist = uniqueStrings(append(matchedBlacklist, decision.MatchedBlacklist...))
				if decision.Profile == researchProfileDeep {
					eventProfile, eventReason = researchProfileDeep, "whitelist_match"
					matched = uniqueStrings(append(matched, decision.MatchedWhitelist...))
				}
			}
		}
		blocked := len(matchedBlacklist) > 0
		if blocked {
			eventProfile, eventReason = "blocked", "blacklist_match"
		}
		profile, reason := eventProfile, eventReason
		if manual {
			profile, reason = researchProfileDeep, "manual"
		}
		kwargs["research_profile"], kwargs["route_reason"], kwargs["matched_whitelist_keywords"] = profile, reason, matched
		kwargs["matched_blacklist_keywords"] = matchedBlacklist
		item.job["kwargs"] = kwargs
		item.run["research_profile"], item.run["route_reason"], item.run["matched_whitelist_keywords"] = profile, reason, matched
		item.run["matched_blacklist_keywords"] = matchedBlacklist
		if item.eventID != "" {
			item.event["research_profile"], item.event["research_route_reason"], item.event["matched_whitelist_keywords"] = eventProfile, eventReason, matched
			item.event["matched_blacklist_keywords"] = matchedBlacklist
			eventBody, _ := json.Marshal(item.event)
			batch.Queue(`UPDATE news_events SET payload=$2 WHERE id=$1`, item.eventID, eventBody)
		}
		jobBody, _ := json.Marshal(item.job)
		if blocked && !manual {
			item.run["status"], item.run["completed_at"], item.run["updated_at"] = "filtered", iso(time.Now()), iso(time.Now())
			batch.Queue(`UPDATE go_jobs SET payload=$2,status='cancelled',error=NULL,completed_at=now(),updated_at=now() WHERE id=$1 AND status IN ('queued','retrying')`, item.jobID, jobBody)
		} else {
			batch.Queue(`UPDATE go_jobs SET payload=$2,updated_at=now() WHERE id=$1 AND status IN ('queued','retrying')`, item.jobID, jobBody)
		}
		if item.runID != "" {
			runBody, _ := json.Marshal(item.run)
			table := "research_runs"
			if item.taskType == researchEventTask {
				table = "event_research_runs"
			}
			if blocked && !manual {
				batch.Queue(`UPDATE `+table+` SET status='filtered',payload=$2,updated_at=now() WHERE id=$1 AND status IN ('queued','retrying')`, item.runID, runBody)
			} else {
				batch.Queue(`UPDATE `+table+` SET payload=$2,updated_at=now() WHERE id=$1 AND status IN ('queued','retrying')`, item.runID, runBody)
			}
		}
		updated++
	}
	results := db.SendBatch(ctx, batch)
	for index := 0; index < batch.Len(); index++ {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return 0, err
		}
	}
	return updated, results.Close()
}
