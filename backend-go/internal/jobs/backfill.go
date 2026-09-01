package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const backfillAssetMappingsTask = "market_loop.backfill_asset_mappings"

type backfillProgress struct {
	Scanned int `json:"scanned"`
	Queued  int `json:"queued"`
	Skipped int `json:"skipped"`
	Failed  int `json:"failed"`
}

type backfillEvent struct {
	ID         uuid.UUID
	ObservedAt time.Time
	Payload    map[string]any
}

type backfillAsset struct {
	ID              string
	Class           string
	Symbol          string
	Name            string
	AssociationTier string
	Active          bool
}

func NewBackfillHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &ExtractRuntime{cfg: cfg, db: db, redis: redisClient, client: &http.Client{Timeout: cfg.OllamaTimeout}}
	return map[string]Handler{backfillAssetMappingsTask: runtime.backfillAssetMappings}
}

func (runtime *ExtractRuntime) backfillAssetMappings(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, permanentJobError{err}
	}
	days := int(numberValue(envelope.Kwargs["days"]))
	if days < 1 {
		days = 30
	}
	if days > 30 {
		days = 30
	}
	cutoff := parseTime(envelope.Kwargs["cutoff"])
	if cutoff.IsZero() {
		cutoff = time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)
	}
	progress := decodeBackfillProgress(envelope.Kwargs["stats"])
	mappingDepth, err := runtime.activeTrackedTasks(ctx, "assist")
	if err != nil {
		return nil, err
	}
	researchDepth, err := runtime.activeTrackedTasks(ctx, "research")
	if err != nil {
		return nil, err
	}
	if mappingDepth >= 10 || researchDepth >= 12 {
		return nil, runtime.backfillContinuation(days, cutoff, envelope.Kwargs, progress, 60*time.Second, "waiting_for_capacity", map[string]any{
			"mapping_depth": mappingDepth, "research_depth": researchDepth,
		})
	}

	rows, err := runtime.loadBackfillEvents(ctx, cutoff, stringValue(envelope.Kwargs["cursor_time"]), stringValue(envelope.Kwargs["cursor_id"]))
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		progress.Scanned++
		eligible, inspectErr := runtime.eventNeedsMappingBackfill(ctx, row.Payload)
		if inspectErr != nil {
			progress.Failed++
			continue
		}
		if !eligible {
			progress.Skipped++
			continue
		}
		queued, queueErr := runtime.enqueueMapping(ctx, row.Payload, true, true, false)
		if queueErr != nil {
			progress.Failed++
			continue
		}
		if queued {
			progress.Queued++
		} else {
			progress.Skipped++
		}
	}
	if len(rows) < 10 {
		return map[string]any{"scanned": progress.Scanned, "queued": progress.Queued, "skipped": progress.Skipped, "failed": progress.Failed, "status": "completed"}, nil
	}
	last := rows[len(rows)-1]
	kwargs := cloneMap(envelope.Kwargs)
	kwargs["cursor_time"], kwargs["cursor_id"] = iso(last.ObservedAt), last.ID.String()
	return nil, runtime.backfillContinuation(days, cutoff, kwargs, progress, 2*time.Second, "dispatching", nil)
}

func (runtime *ExtractRuntime) backfillContinuation(days int, cutoff time.Time, kwargs map[string]any, progress backfillProgress, delay time.Duration, phase string, extra map[string]any) error {
	next := cloneMap(kwargs)
	next["days"], next["cutoff"], next["stats"] = days, iso(cutoff), progress
	visible := map[string]any{"scanned": progress.Scanned, "queued": progress.Queued, "skipped": progress.Skipped, "failed": progress.Failed, "phase": phase, "days": days}
	for key, value := range extra {
		visible[key] = value
	}
	return &continuationError{Payload: taskEnvelope{Args: []any{}, Kwargs: next}, Progress: visible, Delay: delay}
}

func decodeBackfillProgress(value any) backfillProgress {
	if typed, ok := value.(backfillProgress); ok {
		return typed
	}
	item := objectValue(value)
	return backfillProgress{Scanned: int(numberValue(item["scanned"])), Queued: int(numberValue(item["queued"])), Skipped: int(numberValue(item["skipped"])), Failed: int(numberValue(item["failed"]))}
}

func (runtime *ExtractRuntime) activeTrackedTasks(ctx context.Context, lane string) (int, error) {
	values, err := runtime.redis.HVals(ctx, "market-loop:model-queue:"+lane+":tasks").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}
	active := map[string]bool{"queued": true, "running": true, "retrying": true, "verifying": true, "generating": true, "testing": true, "merging": true, "proposed": true}
	count := 0
	for _, raw := range values {
		var item map[string]any
		if json.Unmarshal([]byte(raw), &item) == nil && active[stringValue(item["status"])] {
			count++
		}
	}
	return count, nil
}

func (runtime *ExtractRuntime) loadBackfillEvents(ctx context.Context, cutoff time.Time, cursorTime, cursorID string) ([]backfillEvent, error) {
	args := []any{cutoff}
	query := `SELECT id,observed_at,payload::jsonb FROM news_events WHERE observed_at >= $1`
	if cursorTime != "" || cursorID != "" {
		stamp, idErr := time.Parse(time.RFC3339Nano, cursorTime)
		id, uuidErr := uuid.Parse(cursorID)
		if idErr != nil || uuidErr != nil {
			return nil, permanentJobError{errors.New("invalid asset mapping backfill cursor")}
		}
		query += ` AND (observed_at > $2 OR (observed_at = $2 AND id > $3))`
		args = append(args, stamp.UTC(), id)
	}
	query += ` ORDER BY observed_at,id LIMIT 10`
	rows, err := runtime.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]backfillEvent, 0, 10)
	for rows.Next() {
		var item backfillEvent
		var payload []byte
		if err := rows.Scan(&item.ID, &item.ObservedAt, &payload); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &item.Payload); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (runtime *ExtractRuntime) eventNeedsMappingBackfill(ctx context.Context, event map[string]any) (bool, error) {
	candidates := anySlice(event["candidates"])
	lowConfidence := len(candidates) == 0
	if len(candidates) > 0 {
		lowConfidence = true
		for _, raw := range candidates {
			candidate := objectValue(raw)
			if numberValue(candidate["relevance"]) >= .65 && numberValue(candidate["mapping_confidence"]) >= .65 {
				lowConfidence = false
				break
			}
		}
	}
	if mappingIsActive(event) {
		return false, nil
	}
	sourceText := stringValue(event["headline"]) + "\n" + strings.Join(stringSlice(event["entities"]), "\n")
	for _, rawID := range stringSlice(event["news_item_ids"]) {
		if newsID, err := uuid.Parse(rawID); err == nil {
			if item, loadErr := runtime.loadNews(ctx, newsID); loadErr == nil {
				sourceText += "\n" + item.Title + "\n" + item.Summary
			}
		}
	}
	invalid, err := runtime.hasInvalidBackfillCandidate(ctx, candidates, sourceText)
	if err != nil {
		return false, err
	}
	return lowConfidence || invalid, nil
}

func mappingIsActive(event map[string]any) bool {
	queued := latestAnalysisStep(event, "asset_mapping_queue")
	if queued == nil || stringValue(queued["status"]) != "queued" {
		return false
	}
	mapping := latestAnalysisStep(event, "asset_mapping")
	if mapping == nil {
		return true
	}
	status := stringValue(mapping["status"])
	return !parseTime(mapping["occurred_at"]).After(parseTime(queued["occurred_at"])) || status == "running" || status == "retrying"
}

func (runtime *ExtractRuntime) hasInvalidBackfillCandidate(ctx context.Context, candidates []any, sourceText string) (bool, error) {
	for _, raw := range candidates {
		assetID := stringValue(objectValue(objectValue(raw)["asset"])["asset_id"])
		var asset backfillAsset
		err := runtime.db.QueryRow(ctx, `SELECT id,asset_class,symbol,name,association_tier,active FROM assets WHERE id=$1`, assetID).
			Scan(&asset.ID, &asset.Class, &asset.Symbol, &asset.Name, &asset.AssociationTier, &asset.Active)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && (!asset.Active || asset.AssociationTier == "manual_only") {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if asset.AssociationTier == "exact_only" {
			eligible, err := runtime.exactOnlyMappingEligible(ctx, sourceText, asset)
			if err != nil {
				return false, err
			}
			if !eligible {
				return true, nil
			}
		}
	}
	return false, nil
}

func (runtime *ExtractRuntime) exactOnlyMappingEligible(ctx context.Context, source string, asset backfillAsset) (bool, error) {
	if asset.Class != "crypto" {
		return false, nil
	}
	parts := strings.Split(asset.ID, ":")
	if coinID := parts[len(parts)-1]; coinID != "" && explicitTerm(source, coinID) {
		return true, nil
	}
	var nameCount, symbolCount int
	err := runtime.db.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE lower(regexp_replace(name,'[^[:alnum:]]','','g'))=lower(regexp_replace($1,'[^[:alnum:]]','','g')))::int,
		count(*) FILTER (WHERE upper(symbol)=upper($2))::int
		FROM assets WHERE active=true AND asset_class='crypto' AND association_tier <> 'manual_only'`, asset.Name, asset.Symbol).Scan(&nameCount, &symbolCount)
	if err != nil {
		return false, err
	}
	if nameCount == 1 && meaningfulTerm(asset.Name) && explicitTerm(source, asset.Name) {
		return true, nil
	}
	if symbolCount != 1 || strings.TrimSpace(asset.Symbol) == "" {
		return false, nil
	}
	escaped := regexp.QuoteMeta(asset.Symbol)
	matched, _ := regexp.MatchString(`(?i)(?:\$`+escaped+`\b|\b`+escaped+`/(?:USD|USDT)\b)`, source)
	return matched, nil
}

func cloneMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
