package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	extractTask            = "market_loop.extract_news_item"
	reextractTask          = "market_loop.reextract_event"
	retryNewsTask          = "market_loop.retry_news_item"
	finalizeExtractionTask = "market_loop.finalize_news_extraction"
	modelTaskTTL           = 48 * time.Hour
	scanStateTTL           = 12 * time.Hour
	scanQueueKey           = "market-loop:scan:news-extraction-queue"
	scanStatusKey          = "market-loop:scan:status"
	scanGateKey            = "market-loop:scan:active"
)

var allowedEventTypes = map[string]bool{
	"earnings": true, "product": true, "regulation": true, "m_and_a": true,
	"management": true, "security": true, "macro": true, "supply_chain": true,
	"tokenomics": true, "other": true,
}

type ExtractRuntime struct {
	cfg    config.Config
	db     *pgxpool.Pool
	redis  *redis.Client
	client *http.Client
}

type taskEnvelope struct {
	Args   []any          `json:"args"`
	Kwargs map[string]any `json:"kwargs"`
}

type newsRecord struct {
	ID            uuid.UUID
	Source        string
	SourceQuality string
	Title         string
	Summary       string
	URL           string
	Language      string
	PublishedAt   time.Time
	ObservedAt    time.Time
	AsOf          time.Time
	Symbols       []string
}

type extractedEvent struct {
	EventType    string           `json:"event_type"`
	Entities     []string         `json:"entities"`
	DirectImpact string           `json:"direct_impact"`
	HorizonDays  int              `json:"horizon_days"`
	Actions      []map[string]any `json:"actions"`
	Novelty      float64          `json:"novelty"`
	Priority     float64          `json:"priority"`
	Search       []string         `json:"search_queries"`
}

type ollamaResponse struct {
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
	PromptTokens     int `json:"prompt_eval_count"`
	CompletionTokens int `json:"eval_count"`
}

func NewExtractHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &ExtractRuntime{cfg: cfg, db: db, redis: redisClient, client: &http.Client{Timeout: cfg.OllamaTimeout}}
	return map[string]Handler{
		extractTask:            runtime.extractNewsItem,
		reextractTask:          runtime.reextractEvent,
		retryNewsTask:          runtime.retryNewsItem,
		finalizeExtractionTask: runtime.finalizeNewsExtraction,
	}
}

func (runtime *ExtractRuntime) extractNewsItem(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, err
	}
	if len(envelope.Args) < 2 {
		return nil, errors.New("extract_news_item requires scan_task_id and news_id")
	}
	scanTaskID, newsID := fmt.Sprint(envelope.Args[0]), fmt.Sprint(envelope.Args[1])
	if active, _ := runtime.redis.Get(ctx, scanGateKey).Result(); active != scanTaskID {
		return map[string]any{"status": "superseded", "news_id": newsID, "event_ids": []any{}}, nil
	}
	_ = runtime.redis.Expire(ctx, scanGateKey, scanStateTTL).Err()
	if runtime.scanItemCancelled(ctx, scanTaskID, newsID) {
		return map[string]any{"status": "cancelled", "news_id": newsID, "event_ids": []any{}}, nil
	}
	return runtime.processNews(ctx, job, newsID, scanTaskID, boolValue(envelope.Kwargs["force_asset_mapping"]), stringValue(envelope.Kwargs["model_instance_id"]))
}

func (runtime *ExtractRuntime) retryNewsItem(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, err
	}
	if len(envelope.Args) < 1 {
		return nil, errors.New("retry_news_item requires news_id")
	}
	return runtime.processNews(ctx, job, fmt.Sprint(envelope.Args[0]), "", boolValue(envelope.Kwargs["force_asset_mapping"]), stringValue(envelope.Kwargs["model_instance_id"]))
}

func (runtime *ExtractRuntime) processNews(ctx context.Context, job Job, rawNewsID, scanTaskID string, forceMapping bool, instanceID string) (result any, returnedErr error) {
	newsID, err := uuid.Parse(rawNewsID)
	if err != nil {
		return nil, fmt.Errorf("invalid news id: %w", err)
	}
	taskID := job.ID.String()
	if runtime.modelTaskCancelled(ctx, taskID) {
		_ = runtime.markNewsProcessing(ctx, newsID, "cancelled", taskID, scanTaskID, job.Attempt, "")
		if scanTaskID != "" {
			runtime.updateScanItem(ctx, scanTaskID, rawNewsID, "cancelled", job.Attempt, "")
		}
		return map[string]any{"status": "cancelled", "news_id": rawNewsID, "event_ids": []any{}}, nil
	}
	news, err := runtime.loadNews(ctx, newsID)
	if err != nil {
		return nil, runtime.failNews(ctx, job, newsID, scanTaskID, err)
	}
	runtime.updateModelTask(ctx, taskID, "running", job.Attempt, rawNewsID, news.Title, news.Source, "", nil)
	if scanTaskID != "" {
		runtime.updateScanItem(ctx, scanTaskID, rawNewsID, "running", job.Attempt, "")
	}
	if err := runtime.markNewsProcessing(ctx, newsID, "running", taskID, scanTaskID, job.Attempt, ""); err != nil {
		return nil, runtime.failNews(ctx, job, newsID, scanTaskID, err)
	}
	event, fallback, err := runtime.ingestNews(ctx, news, instanceID)
	if err != nil {
		if ctx.Err() != nil {
			clean := context.WithoutCancel(ctx)
			_ = runtime.markNewsProcessing(clean, newsID, "cancelled", taskID, scanTaskID, job.Attempt, "")
			runtime.updateModelTask(clean, taskID, "cancelled", job.Attempt, rawNewsID, news.Title, news.Source, "", nil)
			if scanTaskID != "" {
				runtime.updateScanItem(clean, scanTaskID, rawNewsID, "cancelled", job.Attempt, "")
			}
			return map[string]any{"status": "cancelled", "news_id": rawNewsID, "event_ids": []any{}}, nil
		}
		return nil, runtime.failNews(ctx, job, newsID, scanTaskID, err)
	}
	researchQueued, mappingQueued, dispatched := runtime.dispatchEvent(ctx, event, forceMapping, false, false)
	if err := runtime.markNewsProcessing(ctx, newsID, "completed", taskID, scanTaskID, job.Attempt, ""); err != nil {
		return nil, runtime.failNews(ctx, job, newsID, scanTaskID, err)
	}
	metrics := map[string]any{"event_count": 1, "research_queued": researchQueued, "asset_mapping_queued": mappingQueued, "fallback": fallback}
	runtime.updateModelTask(ctx, taskID, "completed", job.Attempt, rawNewsID, news.Title, news.Source, "", metrics)
	if scanTaskID != "" {
		runtime.updateScanItem(ctx, scanTaskID, rawNewsID, "completed", job.Attempt, "")
	}
	return map[string]any{
		"status": "completed", "news_id": rawNewsID, "event_ids": []string{stringValue(event["id"])},
		"provider_errors": []any{}, "research_queued": researchQueued, "asset_mapping_queued": mappingQueued,
		"downstream_dispatched": dispatched,
	}, nil
}

func (runtime *ExtractRuntime) failNews(ctx context.Context, job Job, newsID uuid.UUID, scanTaskID string, cause error) error {
	retrying := job.Attempt < job.MaxAttempts
	status := "extraction_failed"
	modelStatus := "failed"
	if retrying {
		status, modelStatus = "retrying", "retrying"
	}
	message := cause.Error()
	_ = runtime.markNewsProcessing(ctx, newsID, status, job.ID.String(), scanTaskID, job.Attempt, message)
	runtime.updateModelTask(ctx, job.ID.String(), modelStatus, job.Attempt, newsID.String(), "", "", message, nil)
	if scanTaskID != "" {
		runtime.updateScanItem(ctx, scanTaskID, newsID.String(), modelStatus, job.Attempt, message)
	}
	return cause
}

func (runtime *ExtractRuntime) reextractEvent(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, err
	}
	if len(envelope.Args) < 2 {
		return nil, errors.New("reextract_event requires event_id and run_id")
	}
	eventID, err := uuid.Parse(fmt.Sprint(envelope.Args[0]))
	if err != nil {
		return nil, err
	}
	runID, err := uuid.Parse(fmt.Sprint(envelope.Args[1]))
	if err != nil {
		return nil, err
	}
	taskID := job.ID.String()
	if runtime.modelTaskCancelled(ctx, taskID) {
		_ = runtime.updateFullResearch(ctx, runID, "failed", "event_extraction", "完整重新研究已取消，原研报保持不变。", "event refresh cancelled", nil)
		return map[string]any{"status": "cancelled", "event_id": eventID, "run_id": runID}, nil
	}
	owned, err := runtime.fullResearchOwned(ctx, runID, eventID, taskID)
	if err != nil {
		return nil, err
	}
	if !owned {
		return map[string]any{"status": "superseded", "event_id": eventID, "run_id": runID}, nil
	}
	runtime.updateModelTask(ctx, taskID, "running", job.Attempt, eventID.String(), "", "", "", nil)
	if err := runtime.updateFullResearch(ctx, runID, "running", "event_extraction", "正在从全部可用关联新闻重新抽取事件事实。", "", map[string]any{"attempt": job.Attempt}); err != nil {
		return nil, err
	}
	event, err := runtime.loadEvent(ctx, eventID)
	if err != nil {
		return nil, runtime.failReextract(ctx, job, runID, err)
	}
	newsIDs := stringSlice(event["news_item_ids"])
	extracted := make([]map[string]any, 0, len(newsIDs))
	missing := 0
	for _, value := range newsIDs {
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			missing++
			continue
		}
		news, loadErr := runtime.loadNews(ctx, id)
		if loadErr != nil {
			missing++
			continue
		}
		fresh, _, extractErr := runtime.extractEvent(ctx, news, stringValue(envelope.Kwargs["model_instance_id"]))
		if extractErr != nil {
			if ctx.Err() != nil {
				clean := context.WithoutCancel(ctx)
				_ = runtime.updateFullResearch(clean, runID, "failed", "event_extraction", "完整重新研究已取消，原研报保持不变。", "event refresh cancelled", nil)
				runtime.updateModelTask(clean, taskID, "cancelled", job.Attempt, eventID.String(), stringValue(event["headline"]), "", "", nil)
				return map[string]any{"status": "cancelled", "event_id": eventID, "run_id": runID}, nil
			}
			return nil, runtime.failReextract(ctx, job, runID, extractErr)
		}
		extracted = append(extracted, fresh)
	}
	if len(extracted) == 0 {
		return nil, runtime.failReextract(ctx, job, runID, errors.New("event has no source news available for extraction"))
	}
	rebuildEvent(event, extracted, missing)
	if err := runtime.saveEvent(ctx, event); err != nil {
		return nil, runtime.failReextract(ctx, job, runID, err)
	}
	if err := runtime.updateFullResearch(ctx, runID, "running", "asset_mapping", "事件事实已重新抽取，正在强制执行股票映射。", "", map[string]any{"available_news_count": len(extracted), "missing_news_count": missing}); err != nil {
		return nil, runtime.failReextract(ctx, job, runID, err)
	}
	_, mapping, dispatched := runtime.dispatchEvent(ctx, event, true, true, true)
	if !dispatched || mapping == 0 {
		return nil, runtime.failReextract(ctx, job, runID, errors.New("asset mapping could not be queued"))
	}
	runtime.updateModelTask(ctx, taskID, "completed", job.Attempt, eventID.String(), stringValue(event["headline"]), "", "", map[string]any{"news_count": len(extracted), "missing_news_count": missing})
	return map[string]any{"status": "asset_mapping_queued", "event_id": eventID, "run_id": runID, "news_count": len(extracted), "missing_news_count": missing}, nil
}

func (runtime *ExtractRuntime) failReextract(ctx context.Context, job Job, runID uuid.UUID, cause error) error {
	retrying := job.Attempt < job.MaxAttempts
	status, summary := "failed", "事件重新抽取最终失败，已保留原研报。"
	if retrying {
		status, summary = "retrying", "事件重新抽取暂时失败，等待重试。"
	}
	_ = runtime.updateFullResearch(ctx, runID, status, "event_extraction", summary, cause.Error(), map[string]any{"attempt": job.Attempt})
	runtime.updateModelTask(ctx, job.ID.String(), status, job.Attempt, "", "", "", cause.Error(), nil)
	return cause
}

func (runtime *ExtractRuntime) fullResearchOwned(ctx context.Context, runID, eventID uuid.UUID, taskID string) (bool, error) {
	var body []byte
	var storedEventID uuid.UUID
	if err := runtime.db.QueryRow(ctx, `SELECT event_id,payload::jsonb FROM event_research_runs WHERE id=$1`, runID).Scan(&storedEventID, &body); err != nil {
		return false, err
	}
	if storedEventID != eventID {
		return false, nil
	}
	var run map[string]any
	if err := json.Unmarshal(body, &run); err != nil {
		return false, err
	}
	step := latestAnalysisStep(run, "full_event_research")
	if step == nil {
		return true, nil
	}
	owner := stringValue(objectValue(step["metrics"])["task_id"])
	return owner == "" || owner == taskID, nil
}

func (runtime *ExtractRuntime) finalizeNewsExtraction(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, err
	}
	scanTaskID := ""
	if len(envelope.Args) > 0 {
		scanTaskID = fmt.Sprint(envelope.Args[len(envelope.Args)-1])
	}
	active, _ := runtime.redis.Get(ctx, scanGateKey).Result()
	if scanTaskID == "" || active != scanTaskID {
		return map[string]any{"status": "superseded", "scan_task_id": scanTaskID, "events": 0}, nil
	}
	queue := runtime.readRedisObject(ctx, scanQueueKey)
	counts := scanCounts(queue)
	metadata := objectValue(queue["metadata"])
	status := "completed"
	if counts["failed"] > 0 {
		status = "completed_with_errors"
	}
	result := map[string]any{
		"status": status, "discovered": int(numberValue(metadata["discovered"])),
		"accepted": int(numberValue(metadata["accepted"])), "filtered": int(numberValue(metadata["filtered"])),
		"events": counts["completed"], "extraction_completed": counts["completed"], "extraction_failed": counts["failed"],
		"research_queued": 0, "asset_mapping_queued": 0, "executor": "go-worker",
	}
	queue["state"], queue["error"] = status, nil
	runtime.writeRedisObject(ctx, scanQueueKey, queue, scanStateTTL)
	scan := runtime.readRedisObject(ctx, scanStatusKey)
	scan["state"], scan["task_id"], scan["phase"] = "idle", scanTaskID, "completed"
	scan["current"], scan["total"], scan["last_completed_at"] = result["discovered"], result["discovered"], iso(time.Now())
	scan["last_result"], scan["last_error"] = result, nil
	runtime.writeRedisObject(ctx, scanStatusKey, scan, 0)
	_ = runtime.redis.Del(ctx, scanGateKey, "market-loop:scan:pause").Err()
	return result, nil
}

func (runtime *ExtractRuntime) scanItemCancelled(ctx context.Context, scanTaskID, newsID string) bool {
	payload := runtime.readRedisObject(ctx, scanQueueKey)
	if stringValue(payload["scan_task_id"]) != scanTaskID {
		return false
	}
	for _, raw := range anySlice(payload["items"]) {
		item := objectValue(raw)
		if stringValue(item["news_id"]) == newsID {
			return stringValue(item["status"]) == "cancelled"
		}
	}
	return false
}

func (runtime *ExtractRuntime) updateScanItem(ctx context.Context, scanTaskID, newsID, status string, attempt int, errorValue string) {
	payload := runtime.readRedisObject(ctx, scanQueueKey)
	if stringValue(payload["scan_task_id"]) != scanTaskID {
		return
	}
	now := iso(time.Now())
	for _, raw := range anySlice(payload["items"]) {
		item := objectValue(raw)
		if stringValue(item["news_id"]) != newsID || stringValue(item["status"]) == "cancelled" {
			continue
		}
		item["status"], item["attempt"], item["updated_at"] = status, attempt, now
		if errorValue == "" {
			item["error"] = nil
		} else {
			item["error"] = truncateRunes(errorValue, 500)
		}
		if status == "running" {
			if item["started_at"] == nil {
				item["started_at"] = now
			}
			item["completed_at"] = nil
		} else if status == "completed" || status == "failed" || status == "cancelled" {
			item["completed_at"] = now
		}
		break
	}
	counts := scanCounts(payload)
	state := "completed"
	if counts["running"] > 0 {
		state = "running"
	} else if counts["retrying"] > 0 {
		state = "retrying"
	} else if counts["queued"] > 0 {
		state = "queued"
	} else if counts["failed"] > 0 {
		state = "completed_with_errors"
	}
	payload["state"] = state
	runtime.writeRedisObject(ctx, scanQueueKey, payload, scanStateTTL)
}

func scanCounts(payload map[string]any) map[string]int {
	result := map[string]int{"queued": 0, "running": 0, "retrying": 0, "completed": 0, "failed": 0}
	for _, raw := range anySlice(payload["items"]) {
		status := stringValue(objectValue(raw)["status"])
		if _, ok := result[status]; ok {
			result[status]++
		}
	}
	return result
}

func (runtime *ExtractRuntime) readRedisObject(ctx context.Context, key string) map[string]any {
	raw, err := runtime.redis.Get(ctx, key).Bytes()
	if err != nil {
		return map[string]any{}
	}
	result := map[string]any{}
	_ = json.Unmarshal(raw, &result)
	return result
}

func (runtime *ExtractRuntime) writeRedisObject(ctx context.Context, key string, payload map[string]any, ttl time.Duration) {
	body, _ := json.Marshal(payload)
	_ = runtime.redis.Set(ctx, key, body, ttl).Err()
}

func decodeTaskEnvelope(payload json.RawMessage) (taskEnvelope, error) {
	var result taskEnvelope
	if err := json.Unmarshal(payload, &result); err != nil {
		return result, err
	}
	if result.Args == nil {
		result.Args = []any{}
	}
	if result.Kwargs == nil {
		result.Kwargs = map[string]any{}
	}
	return result, nil
}

func (runtime *ExtractRuntime) loadNews(ctx context.Context, id uuid.UUID) (newsRecord, error) {
	var result newsRecord
	var symbols []byte
	err := runtime.db.QueryRow(ctx, `SELECT id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,symbols::jsonb FROM news_items WHERE id=$1`, id).Scan(
		&result.ID, &result.Source, &result.SourceQuality, &result.Title, &result.Summary, &result.URL, &result.Language,
		&result.PublishedAt, &result.ObservedAt, &result.AsOf, &symbols,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, fmt.Errorf("unknown news item: %s", id)
	}
	if err == nil {
		_ = json.Unmarshal(symbols, &result.Symbols)
	}
	return result, err
}

func (runtime *ExtractRuntime) ingestNews(ctx context.Context, news newsRecord, instanceID string) (map[string]any, bool, error) {
	var existingBody []byte
	err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM news_events WHERE payload::jsonb->'news_item_ids' ? $1 LIMIT 1`, news.ID.String()).Scan(&existingBody)
	if err == nil {
		var existing map[string]any
		if json.Unmarshal(existingBody, &existing) == nil {
			return existing, false, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	event, fallback, err := runtime.extractEvent(ctx, news, instanceID)
	if err != nil {
		return nil, fallback, err
	}
	cluster, err := runtime.findCluster(ctx, event)
	if err != nil {
		return nil, fallback, err
	}
	if cluster != nil {
		mergeEvent(cluster, event, news)
		event = cluster
	} else {
		appendAnalysisStep(event, analysisStep("story_clustering", "completed", "persistent-event-cluster:go-v1", fmt.Sprintf("事件簇 %s 已持久化，当前包含 1 篇新闻。", stringValue(event["id"])), map[string]any{"cluster_id": event["id"], "member_count": 1}))
	}
	return event, fallback, runtime.saveEvent(ctx, event)
}

func (runtime *ExtractRuntime) extractEvent(ctx context.Context, news newsRecord, instanceID string) (map[string]any, bool, error) {
	extracted, modelErr := runtime.generateExtraction(ctx, news, instanceID)
	if errors.Is(modelErr, context.Canceled) || errors.Is(modelErr, context.DeadlineExceeded) {
		return nil, false, modelErr
	}
	fallback := modelErr != nil
	if fallback {
		extracted = fallbackExtraction(news)
	}
	normalizeExtraction(&extracted, news)
	assets, err := runtime.matchAssets(ctx, news, extracted)
	if err != nil {
		return nil, fallback, err
	}
	quality := map[string]float64{"official": 1, "primary": .9, "professional": .8, "aggregator": .6, "social": .3}[news.SourceQuality]
	if quality == 0 {
		quality = .6
	}
	steps := []any{analysisStep("news_collection", "completed", "provider", fmt.Sprintf("已采集并归档来自 %s 的新闻。", news.Source), map[string]any{"source": news.Source, "source_quality": news.SourceQuality})}
	if fallback {
		steps = append(steps, analysisStep("event_extraction", "failed", "ollama", fmt.Sprintf("事件提取模型不可用（%T），已切换规则回退。", modelErr), nil))
		steps = append(steps, analysisStep("event_extraction_fallback", "fallback", "keyword-rules:go-v2", fmt.Sprintf("规则引擎已整理为 %s 事件。", extracted.EventType), map[string]any{"entities": len(extracted.Entities)}))
	} else {
		steps = append(steps, analysisStep("event_extraction", "completed", "ollama", fmt.Sprintf("已整理为 %s 事件并生成候选映射查询。", extracted.EventType), map[string]any{"entities": len(extracted.Entities)}))
	}
	steps = append(steps, analysisStep("asset_mapping", ternary(len(assets) > 0, "completed", "unmapped"), "provider-registry:go-v1", fmt.Sprintf("确定性证券映射找到 %d 个候选。", len(assets)), map[string]any{"candidate_count": len(assets)}))
	industries := make([]string, 0)
	for _, raw := range assets {
		candidate := objectValue(raw)
		asset := objectValue(candidate["asset"])
		if industry := stringValue(asset["industry_id"]); industry != "" && !containsString(industries, industry) {
			industries = append(industries, industry)
		}
	}
	eventID := uuid.NewString()
	return map[string]any{
		"id": eventID, "news_item_ids": []string{news.ID.String()}, "headline": news.Title,
		"event_type": extracted.EventType, "entities": extracted.Entities, "actions": extracted.Actions,
		"direct_impact": extracted.DirectImpact, "horizon_days": eventHorizonDays(extracted.EventType),
		"source_quality": news.SourceQuality, "published_at": iso(news.PublishedAt), "observed_at": iso(news.ObservedAt),
		"as_of": iso(news.AsOf), "candidates": assets, "industry_ids": industries,
		"novelty": extracted.Novelty, "priority": math.Min(1, extracted.Priority*quality), "analysis_steps": steps,
	}, fallback, nil
}

func (runtime *ExtractRuntime) generateExtraction(ctx context.Context, news newsRecord, instanceID string) (extractedEvent, error) {
	system := "你是谨慎的跨市场新闻结构化引擎。拒绝猜测，输出结构化事实。"
	prompt := extractionPrompt(news)
	schema := map[string]any{
		"type": "object", "required": []string{"event_type", "direct_impact"}, "properties": map[string]any{
			"event_type":    map[string]any{"type": "string", "enum": []string{"earnings", "product", "regulation", "m_and_a", "management", "security", "macro", "supply_chain", "tokenomics", "other"}},
			"entities":      map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"direct_impact": map[string]any{"type": "string"}, "horizon_days": map[string]any{"type": "integer", "minimum": 1, "maximum": 730},
			"actions": map[string]any{"type": "array", "maxItems": 3, "items": map[string]any{"type": "object"}},
			"novelty": map[string]any{"type": "number", "minimum": 0, "maximum": 1}, "priority": map[string]any{"type": "number", "minimum": 0, "maximum": 1},
			"search_queries": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		},
	}
	messages := []map[string]string{{"role": "system", "content": system}, {"role": "user", "content": prompt + "\n\n只返回符合请求中 format JSON Schema 的 JSON。"}}
	request := map[string]any{
		"model": runtime.cfg.ExtractModel, "messages": messages, "format": schema, "stream": false,
		"keep_alive": ollamaKeepAliveValue(runtime.cfg.OllamaKeepAlive), "options": map[string]any{"temperature": 0, "num_ctx": runtime.cfg.OllamaContextLength, "num_predict": runtime.cfg.OllamaMaxOutput, "num_thread": runtime.cfg.OllamaExtractThreads},
	}
	logicalID := uuid.New()
	var lastErr error
	endpoints := preferredEndpoints(runtime.cfg.ExtractURLs, instanceID, "extract")
	for attempt := 1; attempt <= 2; attempt++ {
		for index, baseURL := range endpoints {
			started := time.Now().UTC()
			body, _ := json.Marshal(request)
			httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/chat", bytes.NewReader(body))
			if err != nil {
				lastErr = err
				continue
			}
			httpRequest.Header.Set("Content-Type", "application/json")
			response, err := runtime.client.Do(httpRequest)
			if err != nil {
				lastErr = err
				runtime.persistModelAudit(ctx, logicalID, news.ID, attempt, "failed", started, messages, schema, "", nil, err.Error(), 0, 0, index)
				continue
			}
			payload, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
				lastErr = fmt.Errorf("ollama extraction returned %s", response.Status)
				runtime.persistModelAudit(ctx, logicalID, news.ID, attempt, "failed", started, messages, schema, string(payload), nil, lastErr.Error(), 0, 0, index)
				continue
			}
			var modelResponse ollamaResponse
			if err := json.Unmarshal(payload, &modelResponse); err != nil {
				lastErr = err
				continue
			}
			var result extractedEvent
			if err := json.Unmarshal([]byte(modelResponse.Message.Content), &result); err != nil {
				lastErr = err
				runtime.persistModelAudit(ctx, logicalID, news.ID, attempt, "failed", started, messages, schema, modelResponse.Message.Content, nil, err.Error(), modelResponse.PromptTokens, modelResponse.CompletionTokens, index)
				continue
			}
			parsed, _ := json.Marshal(result)
			var parsedValue any
			_ = json.Unmarshal(parsed, &parsedValue)
			runtime.persistModelAudit(ctx, logicalID, news.ID, attempt, "completed", started, messages, schema, modelResponse.Message.Content, parsedValue, "", modelResponse.PromptTokens, modelResponse.CompletionTokens, index)
			return result, nil
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no extract model endpoint configured")
	}
	return extractedEvent{}, lastErr
}

func extractionPrompt(news newsRecord) string {
	content := fallbackString(strings.TrimSpace(news.Summary), strings.TrimSpace(news.Title))
	return "从新闻元数据中提取一个可投资研究事件。不要补充新闻中没有的事实。只提取事实框架，不得输出全局影响方向、分数或评级。" +
		"actions 逐项记录主体、动作、对象、范围、action_type 与 action_stage；entities 必须保留新闻明确出现的公司、品牌和品牌产品名称。" +
		"必须阅读完整正文；正文来自上游 HTML 的新闻内容字段，不得按中文或英文句号截断。\n" +
		fmt.Sprintf("标题：%s\n完整正文：%s\n来源：%s\n已标注代码：%v", news.Title, truncateRunes(content, 3000), news.Source, news.Symbols)
}

func ollamaKeepAliveValue(value string) any {
	trimmed := strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return seconds
	}
	return trimmed
}

func (runtime *ExtractRuntime) persistModelAudit(ctx context.Context, logicalID, newsID uuid.UUID, attempt int, status string, started time.Time, messages any, schema any, raw string, parsed any, errorValue string, promptTokens, completionTokens, endpoint int) {
	messagesJSON, _ := json.Marshal(messages)
	schemaJSON, _ := json.Marshal(schema)
	parsedJSON, _ := json.Marshal(parsed)
	metrics, _ := json.Marshal(map[string]any{"endpoint": fmt.Sprintf("extract-%d", endpoint), "lane": "extract"})
	var parsedArgument any
	if parsed != nil {
		parsedArgument = parsedJSON
	}
	var errorArgument any
	if errorValue != "" {
		errorArgument = errorValue
	}
	_, _ = runtime.db.Exec(ctx, `INSERT INTO model_call_audits(
		id,logical_call_id,provider,model,operation,entity_type,entity_id,attempt,status,fidelity,
		started_at,completed_at,duration_ms,prompt_tokens,completion_tokens,input_language,output_language,
		messages,schema_payload,raw_response,parsed_response,error,metrics)
		VALUES($1,$2,'ollama',$3,'event_extraction','news_item',$4,$5,$6,'exact',$7,$8,$9,$10,$11,'other','other',$12,$13,$14,$15,$16,$17)`,
		uuid.New(), logicalID, runtime.cfg.ExtractModel, newsID.String(), attempt, status, started, time.Now().UTC(), time.Since(started).Milliseconds(), nullableInt(promptTokens), nullableInt(completionTokens), messagesJSON, schemaJSON, raw, parsedArgument, errorArgument, metrics)
}

func fallbackExtraction(news newsRecord) extractedEvent {
	text := strings.ToLower(news.Title + " " + news.Summary)
	typeValue := "other"
	// Rule order is part of the contract. A map would make multi-keyword
	// headlines alternate between event types across worker processes.
	rules := []struct {
		eventType string
		keywords  []string
	}{
		{"earnings", []string{"earnings", "revenue", "profit", "业绩", "营收", "利润", "财报"}},
		{"regulation", []string{"regulation", "regulator", "ban", "监管", "处罚", "禁令"}},
		{"product", []string{"launch", "product", "release", "upgrade", "发布", "产品", "获批", "升级"}},
		{"m_and_a", []string{"acquisition", "merger", "takeover", "收购", "合并", "并购"}},
		{"security", []string{"hack", "breach", "exploit", "攻击", "漏洞", "被盗"}},
		{"tokenomics", []string{"unlock", "airdrop", "token", "解锁", "空投", "代币"}},
		{"supply_chain", []string{"supplier", "shortage", "supply chain", "供应商", "短缺", "供应链"}},
	}
	for _, rule := range rules {
		for _, keyword := range rule.keywords {
			if strings.Contains(text, keyword) {
				typeValue = rule.eventType
				break
			}
		}
		if typeValue != "other" {
			break
		}
	}
	return extractedEvent{EventType: typeValue, Entities: append([]string{}, news.Symbols...), DirectImpact: fallbackString(truncateRunes(news.Summary, 400), news.Title), HorizonDays: eventHorizonDays(typeValue), Actions: fallbackActions(news), Novelty: .4, Priority: .45, Search: append([]string{}, news.Symbols...)}
}

func eventHorizonDays(eventType string) int {
	if eventType == "earnings" || eventType == "security" {
		return 30
	}
	if eventType == "m_and_a" {
		return 180
	}
	return 90
}

func preferredEndpoints(values []string, instanceID, lane string) []string {
	if len(values) < 2 || instanceID == "" {
		return values
	}
	prefix := lane + "-"
	if !strings.HasPrefix(instanceID, prefix) {
		return values
	}
	var index int
	if _, err := fmt.Sscanf(strings.TrimPrefix(instanceID, prefix), "%d", &index); err != nil || index < 0 || index >= len(values) {
		return values
	}
	result := make([]string, 0, len(values))
	result = append(result, values[index])
	result = append(result, values[:index]...)
	result = append(result, values[index+1:]...)
	return result
}

func fallbackActions(news newsRecord) []map[string]any {
	source := news.Title + " " + news.Summary
	text := strings.ToLower(source)
	actions := make([]map[string]any, 0, 3)
	add := func(actor, actionType, stage, action, object string, strength float64) {
		actions = append(actions, map[string]any{
			"id": uuid.NewString(), "actor": actor, "action_type": actionType,
			"action_stage": stage, "action": action, "object": object,
			"scope": truncateRunes(source, 240), "strength": strength,
		})
	}
	if containsAny(text, "谴责", "抗议", "condemn", "等同于", "重申立场") {
		add("新闻所述表态方", "condemnation", "statement", "公开谴责或表态", "新闻所述对象", .15)
	}
	if containsAny(text, "制裁", "sanction") {
		stage, strength := "announced", .55
		if containsAny(text, "正式生效", "开始执行", "已实施", "takes effect") {
			stage, strength = "effective", .75
		}
		add("制裁实施方", "sanctions", stage, "实施或宣布制裁", "受制裁方", strength)
	}
	if containsAny(text, "威胁关闭", "警告关闭", "threaten to close") {
		add("新闻所述威胁方", "strait_closure", "threat", "威胁关闭航道", "霍尔木兹海峡", .35)
	} else if containsAny(text, "海峡关闭", "航道中断", "strait closed") {
		add("新闻所述行为方", "strait_closure", "realized", "航道已经关闭或中断", "霍尔木兹海峡", .9)
	}
	if containsAny(text, "恢复谈判", "恢复通航", "重新开放", "resume talks") {
		add("新闻所述参与方", "deescalation", "realized", "恢复谈判或通航", "相关谈判或航道", .85)
	}
	if len(actions) == 0 {
		add("新闻所述主体", "unknown", "unknown", truncateRunes(news.Title, 160), "", .1)
	}
	if len(actions) > 3 {
		actions = actions[:3]
	}
	return actions
}

func containsAny(text string, values ...string) bool {
	for _, value := range values {
		if strings.Contains(text, value) {
			return true
		}
	}
	return false
}

func normalizeExtraction(value *extractedEvent, news newsRecord) {
	if !allowedEventTypes[value.EventType] {
		value.EventType = "other"
	}
	value.Entities = uniqueStrings(value.Entities)
	if strings.TrimSpace(value.DirectImpact) == "" {
		value.DirectImpact = fallbackString(truncateRunes(news.Summary, 500), news.Title)
	}
	if value.HorizonDays < 1 || value.HorizonDays > 730 {
		value.HorizonDays = 90
	}
	value.Novelty = clamp(value.Novelty, 0, 1, .5)
	value.Priority = clamp(value.Priority, 0, 1, .5)
	if len(value.Actions) > 3 {
		value.Actions = value.Actions[:3]
	}
	for _, action := range value.Actions {
		if stringValue(action["id"]) == "" {
			action["id"] = uuid.NewString()
		}
		stage := stringValue(action["action_stage"])
		bounds := map[string][2]float64{"statement": {.1, .2}, "threat": {.25, .4}, "announced": {.5, .7}, "effective": {.7, .85}, "realized": {.85, 1}, "unknown": {.1, .1}}
		if _, ok := bounds[stage]; !ok {
			stage = "unknown"
		}
		action["action_stage"] = stage
		action["strength"] = math.Max(bounds[stage][0], math.Min(bounds[stage][1], numberValue(action["strength"])))
		for _, key := range []string{"actor", "action_type", "action", "object", "scope"} {
			if action[key] == nil {
				action[key] = ""
			}
		}
	}
}

func (runtime *ExtractRuntime) matchAssets(ctx context.Context, news newsRecord, extracted extractedEvent) ([]any, error) {
	rows, err := runtime.db.Query(ctx, `SELECT id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases::jsonb,products::jsonb,competitors::jsonb,sector_id,industry_id,raw_sector,raw_industry,instrument_type,market_cap,market_cap_rank,last_synced_at,issuer_id,primary_listing_asset_id,lot_size,active FROM assets WHERE active=true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	text := news.Title + "\n" + news.Summary
	for _, entity := range extracted.Entities {
		text += "\n" + entity
	}
	results := make([]any, 0)
	for rows.Next() {
		var id, class, market, symbol, name, exchange, currency string
		var aliasesJSON, productsJSON, competitorsJSON []byte
		var sector, industry, rawSector, rawIndustry, instrument string
		var marketCap *float64
		var marketRank *int
		var synced *time.Time
		var issuer, primary *string
		var lot int
		var active bool
		if err := rows.Scan(&id, &class, &market, &symbol, &name, &exchange, &currency, &aliasesJSON, &productsJSON, &competitorsJSON, &sector, &industry, &rawSector, &rawIndustry, &instrument, &marketCap, &marketRank, &synced, &issuer, &primary, &lot, &active); err != nil {
			return nil, err
		}
		aliases, products, competitors := []string{}, []string{}, []string{}
		_ = json.Unmarshal(aliasesJSON, &aliases)
		_ = json.Unmarshal(productsJSON, &products)
		_ = json.Unmarshal(competitorsJSON, &competitors)
		direct := containsStringFold(news.Symbols, symbol) || explicitSymbol(text, symbol, false)
		issuerMatch := meaningfulIssuerTerm(name) && explicitTerm(text, name)
		if !issuerMatch {
			for _, alias := range aliases {
				if meaningfulTerm(alias) && explicitTerm(text, alias) {
					issuerMatch = true
					break
				}
			}
		}
		product := ""
		for _, candidate := range products {
			if meaningfulProduct(candidate) && explicitTerm(text, candidate) {
				product = candidate
				break
			}
		}
		if !direct && !issuerMatch && product == "" {
			continue
		}
		asset := map[string]any{"asset_id": id, "asset_class": class, "market": market, "symbol": symbol, "name": name, "exchange_or_provider": exchange, "currency": currency, "aliases": aliases, "products": products, "competitors": competitors, "sector_id": sector, "industry_id": industry, "raw_sector": rawSector, "raw_industry": rawIndustry, "instrument_type": instrument, "market_cap": marketCap, "market_cap_rank": marketRank, "last_synced_at": synced, "issuer_id": issuer, "primary_listing_asset_id": primary, "lot_size": max(1, lot), "active": active}
		relationship, relevance, confidence, basis, mention := "issuer", .7, .9, []string{"issuer_name", "provider_master"}, name
		if direct {
			relationship, relevance, confidence, basis, mention = "direct", .95, .99, []string{"source_symbol", "provider_master"}, symbol
		} else if product != "" {
			relationship, relevance, confidence, basis, mention = "product_owner", .85, .99, []string{"source_product", "product_owner_master", product}, product
		} else if primary != nil && *primary != "" {
			relationship, relevance, confidence, basis = "cross_listing_issuer", .55, .75, []string{"issuer_name", "provider_master", "explicit_primary_listing"}
		}
		results = append(results, map[string]any{"asset": asset, "relationship": relationship, "relevance": relevance, "rationale": fmt.Sprintf("新闻中的 %s 与 %s 匹配", mention, name), "mapping_confidence": confidence, "identity_basis": basis})
	}
	sort.Slice(results, func(i, j int) bool {
		return numberValue(objectValue(results[i])["relevance"]) > numberValue(objectValue(results[j])["relevance"])
	})
	return results, rows.Err()
}

func (runtime *ExtractRuntime) findCluster(ctx context.Context, event map[string]any) (map[string]any, error) {
	rows, err := runtime.db.Query(ctx, `SELECT payload::jsonb FROM news_events WHERE event_type=$1 AND published_at BETWEEN $2 AND $3 ORDER BY published_at DESC LIMIT 500`, event["event_type"], parseTime(event["published_at"]).Add(-runtime.cfg.EventClusterWindow), parseTime(event["published_at"]).Add(runtime.cfg.EventClusterWindow))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var candidate map[string]any
		if json.Unmarshal(body, &candidate) != nil {
			continue
		}
		similarity := diceSimilarity(stringValue(candidate["headline"]), stringValue(event["headline"]))
		leftAsset, rightAsset := primaryAsset(candidate), primaryAsset(event)
		if leftAsset != nil && rightAsset != nil {
			leftIssuer := fallbackString(stringValue(leftAsset["issuer_id"]), stringValue(leftAsset["asset_id"]))
			rightIssuer := fallbackString(stringValue(rightAsset["issuer_id"]), stringValue(rightAsset["asset_id"]))
			if leftIssuer == rightIssuer && similarity >= .58 {
				return candidate, nil
			}
			continue
		}
		if (leftAsset == nil) != (rightAsset == nil) {
			continue
		}
		if similarity >= .92 || (intersects(stringSlice(candidate["entities"]), stringSlice(event["entities"])) && similarity >= .78) {
			return candidate, nil
		}
	}
	return nil, rows.Err()
}

func (runtime *ExtractRuntime) saveEvent(ctx context.Context, event map[string]any) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = runtime.db.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO UPDATE SET headline=excluded.headline,event_type=excluded.event_type,payload=excluded.payload,priority=excluded.priority,published_at=excluded.published_at,observed_at=excluded.observed_at,as_of=excluded.as_of`,
		event["id"], event["headline"], event["event_type"], body, numberValue(event["priority"]), parseTime(event["published_at"]), parseTime(event["observed_at"]), parseTime(event["as_of"]))
	return err
}

func (runtime *ExtractRuntime) loadEvent(ctx context.Context, id uuid.UUID) (map[string]any, error) {
	var body []byte
	if err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM news_events WHERE id=$1`, id).Scan(&body); err != nil {
		return nil, err
	}
	var result map[string]any
	return result, json.Unmarshal(body, &result)
}

func (runtime *ExtractRuntime) dispatchEvent(ctx context.Context, event map[string]any, forceMapping, refreshReport, forceWebSearch bool) (research, mapping int, dispatched bool) {
	if !runtime.cfg.AutoResearch && !forceMapping {
		return 0, 0, true
	}
	if forceMapping || len(anySlice(event["candidates"])) == 0 {
		queued, err := runtime.enqueueMapping(ctx, event, forceMapping, refreshReport, forceWebSearch)
		return 0, boolInt(queued), err == nil
	}
	queued, err := runtime.enqueueEventResearch(ctx, event, true)
	return boolInt(queued), 0, err == nil
}

func (runtime *ExtractRuntime) enqueueMapping(ctx context.Context, event map[string]any, force, refreshReport, forceWebSearch bool) (bool, error) {
	if !force && hasActiveAnalysisStep(event, "asset_mapping_queue") {
		return false, nil
	}
	taskID := uuid.NewString()
	instanceID := runtime.selectDownstreamInstance(ctx, "assist", len(runtime.cfg.AssistURLs))
	step := analysisStep("asset_mapping_queue", "queued", "go-worker", fmt.Sprintf("确定性映射未找到标的，已创建 %s 二次标的发现任务。", runtime.cfg.AssistModel), map[string]any{"instance_id": instanceID})
	replaceAnalysisStep(event, step)
	if err := runtime.saveEvent(ctx, event); err != nil {
		return false, err
	}
	kwargs := map[string]any{"model_instance_id": instanceID}
	if force {
		kwargs["force_mapping"] = true
	}
	if refreshReport {
		kwargs["refresh_event_report"] = true
	}
	if forceWebSearch {
		kwargs["force_web_search"] = true
	}
	queuedID, err := NewStore(runtime.db).Enqueue(ctx, EnqueueParams{
		ID: uuid.MustParse(taskID), Queue: "assist", TaskType: mappingTask,
		Payload:  taskEnvelope{Args: []any{stringValue(event["id"])}, Kwargs: kwargs},
		Priority: 5, MaxAttempts: 3, DedupeKey: "mapping:" + stringValue(event["id"]),
	})
	if err == nil {
		taskID = queuedID.String()
	}
	if err != nil {
		step["status"] = "failed"
		step["summary"] = fmt.Sprintf("%s 标的发现任务入队失败。", runtime.cfg.AssistModel)
		replaceAnalysisStep(event, step)
		_ = runtime.saveEvent(ctx, event)
		runtime.updateLaneModelTask(ctx, "assist", taskID, "failed", 1, err.Error())
		return false, err
	}
	runtime.recordModelTask(ctx, "assist", taskID, "asset_mapping", stringValue(event["id"]), stringValue(event["headline"]), stringValue(event["event_type"]), ternary(force, "manual", "automatic"), instanceID)
	return true, nil
}

func (runtime *ExtractRuntime) enqueueEventResearch(ctx context.Context, event map[string]any, filterRecentResearch bool) (bool, error) {
	newsAgeFilter, err := LoadResearchNewsAgeFilter(ctx, runtime.db)
	if err != nil {
		return false, err
	}
	if ResearchNewsExpired(newsAgeFilter, parseTime(event["published_at"]), time.Now().UTC()) {
		return false, nil
	}
	if err := runtime.applyRecentResearchFilter(ctx, event, filterRecentResearch); err != nil {
		return false, err
	}
	var existing uuid.UUID
	if err := runtime.db.QueryRow(ctx, `SELECT id FROM event_research_runs WHERE event_id=$1`, event["id"]).Scan(&existing); err == nil {
		return false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	runID, taskID := uuid.New(), uuid.NewString()
	instanceID := runtime.selectDownstreamInstance(ctx, "research", len(runtime.cfg.ResearchURLs))
	steps := append([]any{}, anySlice(event["analysis_steps"])...)
	steps = append(steps, analysisStep("event_research_queue", "queued", "go-worker", "已创建事实框架与逐目标宏观传导研报任务。", map[string]any{"instance_id": instanceID, "priority": 1}))
	now := time.Now().UTC()
	asOf := parseTime(event["as_of"])
	if observed := parseTime(event["observed_at"]); observed.After(asOf) {
		asOf = observed
	}
	payload := map[string]any{"id": runID, "event_id": event["id"], "status": "queued", "as_of": iso(asOf), "historical_replay": false, "filter_recent_research": filterRecentResearch, "news_age_filter_bypass": false, "verification_round": 0, "retry_count": 0, "celery_task_id": taskID, "model_instance_id": instanceID, "retryable_reason": nil, "missing_requirements": []any{}, "contradictions": []any{}, "evidence": []any{}, "report": nil, "report_history": []any{}, "error": nil, "analysis_steps": steps, "created_at": iso(now), "updated_at": iso(now)}
	body, _ := json.Marshal(payload)
	inserted, err := runtime.db.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at) VALUES($1,$2,'queued',$3,$4,$4) ON CONFLICT(event_id) DO NOTHING`, runID, event["id"], body, now)
	if err != nil {
		return false, err
	}
	if inserted.RowsAffected() == 0 {
		return false, nil
	}
	kwargs := map[string]any{"model_instance_id": instanceID, "filter_recent_research": filterRecentResearch}
	_, queueErr := NewStore(runtime.db).Enqueue(ctx, EnqueueParams{ID: uuid.MustParse(taskID), Queue: "research", TaskType: researchEventTask, Payload: map[string]any{"args": []any{stringValue(event["id"]), runID.String()}, "kwargs": kwargs}, Priority: 1, MaxAttempts: 3, DedupeKey: "research-run:" + runID.String()})
	if queueErr != nil {
		payload["status"], payload["error"] = "failed", "event research queue failed"
		failed, _ := json.Marshal(payload)
		_, _ = runtime.db.Exec(ctx, `UPDATE event_research_runs SET status='failed',payload=$2,updated_at=now() WHERE id=$1`, runID, failed)
		return false, queueErr
	}
	runtime.recordModelTask(ctx, "research", taskID, "event_research", runID.String(), stringValue(event["headline"]), stringValue(event["event_type"]), "automatic", instanceID)
	return true, nil
}

func (runtime *ExtractRuntime) selectDownstreamInstance(ctx context.Context, lane string, count int) string {
	if count < 1 {
		count = 1
	}
	value, err := runtime.redis.Incr(ctx, "market-loop:model-instance:"+lane+":round-robin").Result()
	if err != nil {
		value = time.Now().UnixNano()
	}
	return fmt.Sprintf("%s-%d", lane, (value-1)%int64(count))
}

func (runtime *ExtractRuntime) markNewsProcessing(ctx context.Context, newsID uuid.UUID, status, taskID, scanTaskID string, attempt int, errorValue string) error {
	updated, err := runtime.db.Exec(ctx, `UPDATE news_processing SET status=$2::varchar,celery_task_id=$3,scan_task_id=coalesce(nullif($4,''),scan_task_id),attempt_count=greatest(attempt_count,$5),last_error=nullif($6,''),heartbeat_at=now(),updated_at=now(),started_at=CASE WHEN $2::text='running' THEN coalesce(started_at,now()) ELSE started_at END,completed_at=CASE WHEN $2::text IN ('completed','extraction_failed','cancelled') THEN now() ELSE NULL END WHERE news_id=$1`, newsID, status, taskID, scanTaskID, attempt, errorValue)
	if err == nil && updated.RowsAffected() == 0 {
		return fmt.Errorf("news processing state is missing for %s", newsID)
	}
	return err
}

func (runtime *ExtractRuntime) updateFullResearch(ctx context.Context, runID uuid.UUID, status, stage, summary, errorValue string, metrics map[string]any) error {
	var body []byte
	if err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM event_research_runs WHERE id=$1`, runID).Scan(&body); err != nil {
		return err
	}
	var run map[string]any
	if err := json.Unmarshal(body, &run); err != nil {
		return err
	}
	step := latestAnalysisStep(run, "full_event_research")
	if step == nil {
		step = analysisStep("full_event_research", status, "go-worker", summary, map[string]any{})
		appendAnalysisStep(run, step)
	}
	step["status"], step["summary"], step["occurred_at"] = status, summary, iso(time.Now())
	stepMetrics := objectValue(step["metrics"])
	if stepMetrics == nil {
		stepMetrics = map[string]any{}
	}
	for key, value := range metrics {
		stepMetrics[key] = value
	}
	stepMetrics["stage"] = stage
	if errorValue != "" {
		stepMetrics["error"] = errorValue
	}
	step["metrics"] = stepMetrics
	run["updated_at"] = iso(time.Now())
	encoded, _ := json.Marshal(run)
	_, err := runtime.db.Exec(ctx, `UPDATE event_research_runs SET payload=$2,updated_at=now() WHERE id=$1`, runID, encoded)
	return err
}

func (runtime *ExtractRuntime) recordModelTask(ctx context.Context, lane, taskID, kind, entityID, title, subtitle, source, instanceID string) {
	now := iso(time.Now())
	payload := map[string]any{"task_id": taskID, "instance_id": instanceID, "kind": kind, "entity_id": entityID, "title": title, "subtitle": subtitle, "source": source, "status": "queued", "attempt": 1, "task_count": 1, "queued_at": now, "started_at": nil, "completed_at": nil, "updated_at": now, "error": nil, "metrics": map[string]any{}}
	body, _ := json.Marshal(payload)
	key := "market-loop:model-queue:" + lane + ":tasks"
	_ = runtime.redis.HSet(ctx, key, taskID, body).Err()
	_ = runtime.redis.Expire(ctx, key, modelTaskTTL).Err()
}

func (runtime *ExtractRuntime) updateModelTask(ctx context.Context, taskID, status string, attempt int, entityID, title, subtitle, errorValue string, metrics map[string]any) {
	runtime.updateTrackedTask(ctx, "extract", taskID, status, attempt, entityID, title, subtitle, errorValue, metrics)
}

func (runtime *ExtractRuntime) updateLaneModelTask(ctx context.Context, lane, taskID, status string, attempt int, errorValue string) {
	runtime.updateTrackedTask(ctx, lane, taskID, status, attempt, "", "", "", errorValue, nil)
}

func (runtime *ExtractRuntime) updateTrackedTask(ctx context.Context, lane, taskID, status string, attempt int, entityID, title, subtitle, errorValue string, metrics map[string]any) {
	key := "market-loop:model-queue:" + lane + ":tasks"
	raw, _ := runtime.redis.HGet(ctx, key, taskID).Bytes()
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	if payload == nil {
		payload = map[string]any{"task_id": taskID, "kind": ternary(lane == "extract", "news_extraction", "asset_mapping"), "queued_at": iso(time.Now()), "task_count": 1, "metrics": map[string]any{}}
	}
	if stringValue(payload["status"]) == "cancelled" {
		return
	}
	now := iso(time.Now())
	payload["status"], payload["attempt"], payload["updated_at"] = status, attempt, now
	if entityID != "" {
		payload["entity_id"] = entityID
	}
	if title != "" {
		payload["title"] = title
	}
	if subtitle != "" {
		payload["subtitle"] = subtitle
	}
	if errorValue != "" {
		payload["error"] = truncateRunes(errorValue, 500)
	} else if status != "failed" {
		payload["error"] = nil
	}
	if status == "running" && payload["started_at"] == nil {
		payload["started_at"] = now
	}
	if status == "completed" || status == "failed" || status == "cancelled" {
		payload["completed_at"] = now
	}
	storedMetrics := objectValue(payload["metrics"])
	if storedMetrics == nil {
		storedMetrics = map[string]any{}
	}
	for key, value := range metrics {
		storedMetrics[key] = value
	}
	payload["metrics"] = storedMetrics
	body, _ := json.Marshal(payload)
	_ = runtime.redis.HSet(ctx, key, taskID, body).Err()
	_ = runtime.redis.Expire(ctx, key, modelTaskTTL).Err()
}

func (runtime *ExtractRuntime) modelTaskCancelled(ctx context.Context, taskID string) bool {
	raw, err := runtime.redis.HGet(ctx, "market-loop:model-queue:extract:tasks", taskID).Bytes()
	if err != nil {
		return false
	}
	var payload map[string]any
	return json.Unmarshal(raw, &payload) == nil && stringValue(payload["status"]) == "cancelled"
}

func rebuildEvent(event map[string]any, extracted []map[string]any, missing int) {
	lead := extracted[0]
	entities, actions, steps := []string{}, []any{}, []any{}
	seenActions := map[string]bool{}
	priority, novelty := 0.0, 1.0
	published, observed, asOf := parseTime(lead["published_at"]), parseTime(lead["observed_at"]), parseTime(lead["as_of"])
	quality := stringValue(lead["source_quality"])
	for _, item := range extracted {
		entities = append(entities, stringSlice(item["entities"])...)
		for _, raw := range anySlice(item["actions"]) {
			action := objectValue(raw)
			id := stringValue(action["id"])
			if !seenActions[id] {
				seenActions[id] = true
				actions = append(actions, action)
			}
		}
		steps = append(steps, anySlice(item["analysis_steps"])...)
		priority = math.Max(priority, numberValue(item["priority"]))
		novelty = math.Min(novelty, numberValue(item["novelty"]))
		published = minTime(published, parseTime(item["published_at"]))
		observed = minTime(observed, parseTime(item["observed_at"]))
		asOf = maxTime(asOf, parseTime(item["as_of"]))
		if qualityRank(stringValue(item["source_quality"])) > qualityRank(quality) {
			quality = stringValue(item["source_quality"])
		}
	}
	if len(actions) > 3 {
		actions = actions[:3]
	}
	event["headline"], event["event_type"], event["entities"], event["actions"] = lead["headline"], lead["event_type"], uniqueStrings(entities), actions
	event["direct_impact"], event["horizon_days"], event["source_quality"] = lead["direct_impact"], lead["horizon_days"], quality
	event["published_at"], event["observed_at"], event["as_of"] = iso(published), iso(observed), iso(asOf)
	event["candidates"], event["industry_ids"], event["novelty"], event["priority"] = []any{}, []any{}, novelty, priority
	event["analysis_steps"] = append(anySlice(event["analysis_steps"]), steps...)
	appendAnalysisStep(event, analysisStep("full_event_reextraction", "completed", "event-refresh:go-v1", fmt.Sprintf("已从 %d 篇关联新闻重新抽取事件事实，缺失 %d 篇。", len(extracted), missing), map[string]any{"available_news_count": len(extracted), "missing_news_count": missing}))
}

func mergeEvent(existing, fresh map[string]any, news newsRecord) {
	existing["news_item_ids"] = uniqueStrings(append(stringSlice(existing["news_item_ids"]), stringSlice(fresh["news_item_ids"])...))
	existing["entities"] = uniqueStrings(append(stringSlice(existing["entities"]), stringSlice(fresh["entities"])...))
	mergedCandidates := map[string]map[string]any{}
	for _, source := range [][]any{anySlice(existing["candidates"]), anySlice(fresh["candidates"])} {
		for _, raw := range source {
			candidate := objectValue(raw)
			id := stringValue(objectValue(candidate["asset"])["asset_id"])
			if previous := mergedCandidates[id]; previous == nil || numberValue(candidate["relevance"]) > numberValue(previous["relevance"]) {
				mergedCandidates[id] = candidate
			}
		}
	}
	candidates := make([]any, 0, len(mergedCandidates))
	for _, item := range mergedCandidates {
		candidates = append(candidates, item)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return numberValue(objectValue(candidates[i])["relevance"]) > numberValue(objectValue(candidates[j])["relevance"])
	})
	existing["candidates"] = candidates
	existing["priority"] = math.Max(numberValue(existing["priority"]), numberValue(fresh["priority"]))
	existing["novelty"] = math.Min(numberValue(existing["novelty"]), numberValue(fresh["novelty"]))
	existing["published_at"] = iso(minTime(parseTime(existing["published_at"]), parseTime(fresh["published_at"])))
	existing["observed_at"] = iso(minTime(parseTime(existing["observed_at"]), parseTime(fresh["observed_at"])))
	existing["as_of"] = iso(maxTime(parseTime(existing["as_of"]), parseTime(fresh["as_of"])))
	appendAnalysisStep(existing, analysisStep("story_clustering", "completed", "persistent-event-cluster:go-v1", fmt.Sprintf("事件簇 %s 已持久化，当前包含 %d 篇新闻。", stringValue(existing["id"]), len(stringSlice(existing["news_item_ids"]))), map[string]any{"cluster_id": existing["id"], "member_count": len(stringSlice(existing["news_item_ids"])), "latest_source_group": news.Source}))
}

func analysisStep(phase, status, executor, summary string, metrics map[string]any) map[string]any {
	if metrics == nil {
		metrics = map[string]any{}
	}
	return map[string]any{"phase": phase, "status": status, "executor": executor, "model": nil, "summary": summary, "metrics": metrics, "occurred_at": iso(time.Now())}
}

func appendAnalysisStep(payload map[string]any, step map[string]any) {
	payload["analysis_steps"] = append(anySlice(payload["analysis_steps"]), step)
}
func replaceAnalysisStep(payload map[string]any, step map[string]any) {
	steps := anySlice(payload["analysis_steps"])
	for index := len(steps) - 1; index >= 0; index-- {
		if stringValue(objectValue(steps[index])["phase"]) == stringValue(step["phase"]) {
			steps[index] = step
			payload["analysis_steps"] = steps
			return
		}
	}
	payload["analysis_steps"] = append(steps, step)
}
func latestAnalysisStep(payload map[string]any, phase string) map[string]any {
	steps := anySlice(payload["analysis_steps"])
	for index := len(steps) - 1; index >= 0; index-- {
		if step := objectValue(steps[index]); stringValue(step["phase"]) == phase {
			return step
		}
	}
	return nil
}
func hasActiveAnalysisStep(payload map[string]any, phase string) bool {
	step := latestAnalysisStep(payload, phase)
	status := stringValue(step["status"])
	return status == "queued" || status == "completed"
}

func primaryAsset(event map[string]any) map[string]any {
	var best map[string]any
	bestRank, bestRelevance := -1, -1.0
	ranks := map[string]int{"direct": 4, "product_owner": 3, "issuer": 3, "cross_listing_issuer": 2, "entity": 1}
	for _, raw := range anySlice(event["candidates"]) {
		candidate := objectValue(raw)
		rank, relevance := ranks[stringValue(candidate["relationship"])], numberValue(candidate["relevance"])
		if rank > bestRank || (rank == bestRank && relevance > bestRelevance) {
			best, bestRank, bestRelevance = objectValue(candidate["asset"]), rank, relevance
		}
	}
	return best
}

func explicitTerm(text, term string) bool {
	term = strings.TrimSpace(term)
	if !meaningfulTerm(term) {
		return false
	}
	if hasHan(term) {
		return strings.Contains(strings.ToLower(text), strings.ToLower(term))
	}
	pattern := `(?i)(^|[^a-z0-9])` + regexp.QuoteMeta(term) + `([^a-z0-9]|$)`
	matched, _ := regexp.MatchString(pattern, text)
	return matched
}
func explicitSymbol(text, symbol string, allowBare bool) bool {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return false
	}
	variants := []string{symbol}
	if len(symbol) == 5 {
		allDigits := true
		for _, current := range symbol {
			allDigits = allDigits && unicode.IsDigit(current)
		}
		if allDigits {
			variants = append(variants, strings.TrimLeft(symbol, "0"))
		}
	}
	short := len(symbol) <= 3
	for _, candidate := range variants {
		if candidate == "" {
			candidate = "0"
		}
		if !short && explicitTerm(text, candidate) {
			return true
		}
		if allowBare && strings.EqualFold(strings.TrimSpace(text), candidate) {
			return true
		}
		quoted := regexp.QuoteMeta(candidate)
		patterns := []string{
			`(?i)(^|[^a-z0-9])\$\s*` + quoted + `([^a-z0-9]|$)`,
			`(?i)[\(\[]\s*` + quoted + `\s*[\)\]]`,
			`(?i)(^|[^a-z0-9])(nasdaq|nyse|amex|otc|hkex|hkg|sh|sz)\s*:\s*` + quoted + `([^a-z0-9]|$)`,
			`(?i)(^|[^a-z0-9])` + quoted + `\.(ax|l|n|o|oq|pk|us)([^a-z0-9]|$)`,
		}
		for _, pattern := range patterns {
			if matched, _ := regexp.MatchString(pattern, text); matched {
				return true
			}
		}
	}
	return false
}
func meaningfulTerm(value string) bool {
	compact := normalizedText(value)
	if hasHan(value) {
		return len([]rune(compact)) >= 2
	}
	if plausibleCalendarYear(compact) {
		return false
	}
	return len(compact) >= 3 && !map[string]bool{"inc": true, "ltd": true, "group": true, "market": true, "services": true, "company": true, "companies": true}[compact]
}

// plausibleCalendarYear prevents a date in a headline from being treated as a
// security identity. Qualified tickers are still accepted by explicitSymbol.
func plausibleCalendarYear(value string) bool {
	if len(value) != 4 {
		return false
	}
	for _, current := range value {
		if !unicode.IsDigit(current) {
			return false
		}
	}
	year, err := strconv.Atoi(value)
	return err == nil && year >= 1900 && year <= 2100
}
func meaningfulIssuerTerm(value string) bool {
	if !meaningfulTerm(value) {
		return false
	}
	return !map[string]bool{"机器人": true}[normalizedText(value)]
}
func meaningfulProduct(value string) bool {
	if !meaningfulTerm(value) {
		return false
	}
	return !map[string]bool{"game": true, "games": true, "mac": true, "services": true, "云服务": true, "游戏": true}[normalizedText(value)]
}
func hasHan(value string) bool {
	for _, current := range value {
		if unicode.Is(unicode.Han, current) {
			return true
		}
	}
	return false
}
func normalizedText(value string) string {
	var output strings.Builder
	for _, current := range strings.ToLower(value) {
		if unicode.IsLetter(current) || unicode.IsDigit(current) {
			output.WriteRune(current)
		}
	}
	return output.String()
}
func diceSimilarity(left, right string) float64 {
	left, right = normalizedText(left), normalizedText(right)
	if left == right && left != "" {
		return 1
	}
	bigrams := func(value string) map[string]int {
		runes, result := []rune(value), map[string]int{}
		for index := 0; index+1 < len(runes); index++ {
			result[string(runes[index:index+2])]++
		}
		return result
	}
	a, b := bigrams(left), bigrams(right)
	intersection, total := 0, 0
	for key, count := range a {
		total += count
		intersection += min(count, b[key])
	}
	for _, count := range b {
		total += count
	}
	if total == 0 {
		return 0
	}
	return float64(2*intersection) / float64(total)
}

func anySlice(value any) []any             { result, _ := value.([]any); return result }
func objectValue(value any) map[string]any { result, _ := value.(map[string]any); return result }
func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if typed, ok := value.(string); ok {
		return typed
	}
	return fmt.Sprint(value)
}
func numberValue(value any) float64 {
	if typed, ok := value.(float64); ok {
		return typed
	}
	if typed, ok := value.(int); ok {
		return float64(typed)
	}
	return 0
}
func boolValue(value any) bool { result, _ := value.(bool); return result }
func stringSlice(value any) []string {
	result := []string{}
	for _, raw := range anySlice(value) {
		result = append(result, stringValue(raw))
	}
	if typed, ok := value.([]string); ok {
		return typed
	}
	return result
}
func uniqueStrings(values []string) []string {
	result := []string{}
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}
func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func containsStringFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}
func intersects(left, right []string) bool {
	seen := map[string]bool{}
	for _, value := range left {
		seen[normalizedText(value)] = true
	}
	for _, value := range right {
		if seen[normalizedText(value)] {
			return true
		}
	}
	return false
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
func ternary[T any](condition bool, yes, no T) T {
	if condition {
		return yes
	}
	return no
}
func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
func clamp(value, low, high, fallback float64) float64 {
	if math.IsNaN(value) || value < low || value > high {
		return fallback
	}
	return value
}
func iso(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }
func parseTime(value any) time.Time {
	if typed, ok := value.(time.Time); ok {
		return typed.UTC()
	}
	parsed, _ := time.Parse(time.RFC3339Nano, stringValue(value))
	return parsed.UTC()
}
func minTime(left, right time.Time) time.Time {
	if right.Before(left) {
		return right
	}
	return left
}
func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
func qualityRank(value string) int {
	return map[string]int{"social": 0, "aggregator": 1, "professional": 2, "primary": 3, "official": 4}[value]
}
func nullableInt(value int) any {
	if value == 0 {
		return nil
	}
	return value
}
