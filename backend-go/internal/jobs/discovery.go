package jobs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/fernet/fernet-go"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
	"golang.org/x/text/unicode/norm"
)

const (
	ScanNewsTask       = "market_loop.scan_news"
	dispatchOutboxTask = "market_loop.dispatch_news_processing_outbox"
	scanPauseKey       = "market-loop:scan:pause"
)

type discoveryRuntime struct {
	cfg    config.Config
	db     *pgxpool.Pool
	redis  *redis.Client
	client *http.Client
}

type discoveredNews struct {
	ID            uuid.UUID
	Source        string
	Provider      string
	SourceQuality string
	Title         string
	Summary       string
	URL           string
	Language      string
	PublishedAt   time.Time
	ObservedAt    time.Time
	AsOf          time.Time
	ContentHash   string
	Symbols       []string
	Metadata      map[string]any
}

type sourceDiscoveryReport struct {
	Source            string
	Provider          string
	Status            string
	AttemptedAt       time.Time
	DiscoveredCount   int
	LatestPublishedAt *time.Time
	Error             string
}

type discoveryBatch struct {
	Items   []discoveredNews
	Reports []sourceDiscoveryReport
	Errors  []string
}

func NewDiscoveryHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &discoveryRuntime{cfg: cfg, db: db, redis: redisClient, client: &http.Client{Timeout: 45 * time.Second}}
	return map[string]Handler{
		ScanNewsTask:       runtime.scanNews,
		dispatchOutboxTask: runtime.dispatchOutbox,
	}
}

func (runtime *discoveryRuntime) scanNews(ctx context.Context, job Job) (any, error) {
	taskID := job.ID.String()
	owned, err := runtime.claimScanGate(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !owned {
		return map[string]any{"status": "already_running", "discovered": 0, "events": 0}, nil
	}
	stopHeartbeat := runtime.startScanHeartbeat(ctx, taskID)
	defer stopHeartbeat()
	started := time.Now().UTC()
	runtime.updateScanStatus(ctx, map[string]any{
		"state": "running", "task_id": taskID, "phase": "discovering", "paused_from_phase": nil,
		"current": 0, "total": 0, "started_at": iso(started), "next_scan_at": nil, "last_error": nil,
	})
	if err := runtime.waitIfPaused(ctx, taskID, "discovering", 0, 0); err != nil {
		return runtime.supersededScan(taskID), nil
	}
	since := started.Add(-runtime.cfg.NewsDiscoveryLookback)
	active := *runtime
	active.cfg = runtime.effectiveDiscoveryConfig(ctx)
	batch := active.discover(ctx, since, runtime.cfg.ScanBatchSize)
	if !runtime.renewScanGate(ctx, taskID) {
		return runtime.supersededScan(taskID), nil
	}
	watermarks, err := runtime.sourceWatermarks(ctx)
	if err != nil {
		return nil, runtime.failScan(ctx, taskID, err, job.Attempt >= job.MaxAttempts)
	}
	items := filterBySourceWatermark(batch.Items, watermarks, since, runtime.cfg.NewsWatermarkOverlap)
	accepted, filtered, err := runtime.filterNews(ctx, items)
	if err != nil {
		return nil, runtime.failScan(ctx, taskID, err, job.Attempt >= job.MaxAttempts)
	}
	if err := runtime.waitIfPaused(ctx, taskID, "persisting", len(items), len(items)); err != nil {
		return runtime.supersededScan(taskID), nil
	}
	pending, newCounts, err := runtime.persistForExtraction(ctx, taskID, accepted)
	if err != nil {
		return nil, runtime.failScan(ctx, taskID, err, job.Attempt >= job.MaxAttempts)
	}
	if err := runtime.recordSourceReports(ctx, batch.Reports, newCounts); err != nil {
		return nil, runtime.failScan(ctx, taskID, err, job.Attempt >= job.MaxAttempts)
	}
	dispatch, err := runtime.dispatchOutboxLimit(ctx, max(1, len(pending)), pending)
	if err != nil {
		return nil, runtime.failScan(ctx, taskID, err, job.Attempt >= job.MaxAttempts)
	}
	result := map[string]any{
		"status": "completed", "discovered": len(items), "accepted": len(accepted), "filtered": filtered,
		"new": len(pending), "extraction_queued": len(anySlice(dispatch["queued"])), "dispatch_failed": dispatch["failed"],
		"events": 0, "extraction_completed": 0, "extraction_failed": dispatch["failed"],
		"research_queued": 0, "asset_mapping_queued": 0, "source_errors": batch.Errors,
	}
	runtime.completeScan(ctx, taskID, started, result)
	return result, nil
}

func (runtime *discoveryRuntime) effectiveDiscoveryConfig(ctx context.Context) config.Config {
	active := runtime.cfg
	for _, group := range []string{"fmp", "cn_news", "search"} {
		var body []byte
		if err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM integration_settings WHERE key=$1`, "fact-source:"+group).Scan(&body); err != nil {
			continue
		}
		payload := map[string]any{}
		if json.Unmarshal(body, &payload) != nil {
			continue
		}
		switch group {
		case "fmp":
			if value := strings.TrimSpace(stringValue(payload["base_url"])); value != "" {
				active.FMPBaseURL = strings.TrimRight(value, "/")
			}
			if value := discoveryInt(payload["news_lookback_hours"]); value > 0 {
				active.FMPNewsLookback = value
			}
			if boolValue(payload["access_token_disabled"]) {
				active.FMPAccessToken = ""
			} else if encrypted := stringValue(payload["encrypted_access_token"]); encrypted != "" {
				if value, err := decryptDiscoverySecret(encrypted, active.MCPSecretKey); err == nil {
					active.FMPAccessToken = value
				}
			}
		case "cn_news":
			active.AkshareEnabled = boolDefault(payload["akshare_asset_master_enabled"], active.AkshareEnabled)
			if payload["rss_feed_urls"] != nil {
				active.RSSFeeds = stringSlice(payload["rss_feed_urls"])
			}
			if payload["official_rss_feed_urls"] != nil {
				active.OfficialRSSFeeds = stringSlice(payload["official_rss_feed_urls"])
			}
		case "search":
			if value := discoveryInt(payload["timeout_seconds"]); value > 0 {
				active.WebSearchTimeout = time.Duration(value) * time.Second
			}
		}
	}
	return active
}

func (runtime *discoveryRuntime) dispatchOutbox(ctx context.Context, _ Job) (any, error) {
	return runtime.dispatchOutboxLimit(ctx, 50, nil)
}

func (runtime *discoveryRuntime) claimScanGate(ctx context.Context, taskID string) (bool, error) {
	current, err := runtime.redis.Get(ctx, scanGateKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	if current == taskID {
		return runtime.redis.Expire(ctx, scanGateKey, scanStateTTL).Result()
	}
	if current != "" {
		return false, nil
	}
	return runtime.redis.SetNX(ctx, scanGateKey, taskID, scanStateTTL).Result()
}

func (runtime *discoveryRuntime) renewScanGate(ctx context.Context, taskID string) bool {
	current, err := runtime.redis.Get(ctx, scanGateKey).Result()
	if err != nil || current != taskID {
		return false
	}
	ok, _ := runtime.redis.Expire(ctx, scanGateKey, scanStateTTL).Result()
	return ok
}

func (runtime *discoveryRuntime) startScanHeartbeat(ctx context.Context, taskID string) func() {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if !runtime.renewScanGate(heartbeatCtx, taskID) {
					return
				}
				runtime.updateScanStatus(heartbeatCtx, map[string]any{})
			}
		}
	}()
	return cancel
}

func (runtime *discoveryRuntime) waitIfPaused(ctx context.Context, taskID, phase string, current, total int) error {
	if !runtime.renewScanGate(ctx, taskID) {
		return errors.New("scan lease was lost")
	}
	paused, _ := runtime.redis.Get(ctx, scanPauseKey).Result()
	if paused != taskID {
		return nil
	}
	runtime.updateScanStatus(ctx, map[string]any{"state": "paused", "phase": "paused", "paused_from_phase": phase, "current": current, "total": total, "next_scan_at": nil})
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if !runtime.renewScanGate(ctx, taskID) {
				return errors.New("scan lease was lost")
			}
			paused, _ = runtime.redis.Get(ctx, scanPauseKey).Result()
			if paused != taskID {
				runtime.updateScanStatus(ctx, map[string]any{"state": "running", "phase": phase, "paused_from_phase": nil, "current": current, "total": total})
				return nil
			}
		}
	}
}

func (runtime *discoveryRuntime) scanStatus(ctx context.Context) map[string]any {
	payload := map[string]any{"state": "idle", "task_id": nil, "phase": nil, "paused_from_phase": nil, "current": 0, "total": 0, "started_at": nil, "heartbeat_at": nil, "last_completed_at": nil, "next_scan_at": nil, "last_result": nil, "last_error": nil}
	raw, err := runtime.redis.Get(ctx, scanStatusKey).Bytes()
	if err == nil {
		var stored map[string]any
		if json.Unmarshal(raw, &stored) == nil {
			for key, value := range stored {
				payload[key] = value
			}
		}
	}
	return payload
}

func (runtime *discoveryRuntime) updateScanStatus(ctx context.Context, updates map[string]any) map[string]any {
	payload := runtime.scanStatus(ctx)
	for key, value := range updates {
		payload[key] = value
	}
	if state := stringValue(payload["state"]); state == "queued" || state == "running" || state == "retrying" || state == "paused" {
		payload["heartbeat_at"] = iso(time.Now())
	}
	body, _ := json.Marshal(payload)
	_ = runtime.redis.Set(ctx, scanStatusKey, body, 0).Err()
	return payload
}

func (runtime *discoveryRuntime) completeScan(ctx context.Context, taskID string, started time.Time, result map[string]any) {
	if !runtime.renewScanGate(ctx, taskID) {
		return
	}
	completed := time.Now().UTC()
	interval := runtime.cfg.ScanInterval
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	elapsed := completed.Sub(started)
	next := started.Add((time.Duration(elapsed/interval) + 1) * interval)
	runtime.updateScanStatus(ctx, map[string]any{
		"state": "idle", "task_id": taskID, "phase": "completed", "paused_from_phase": nil,
		"current": result["discovered"], "total": result["discovered"], "last_completed_at": iso(completed),
		"next_scan_at": iso(next), "last_result": result, "last_error": nil,
	})
	runtime.compareDelete(ctx, scanGateKey, taskID)
	runtime.compareDelete(ctx, scanPauseKey, taskID)
}

func (runtime *discoveryRuntime) failScan(ctx context.Context, taskID string, cause error, terminal bool) error {
	clean := context.WithoutCancel(ctx)
	if !terminal {
		runtime.updateScanStatus(clean, map[string]any{"state": "retrying", "task_id": taskID, "phase": "retrying", "next_scan_at": nil, "last_error": fmt.Sprintf("%T", cause)})
		_ = runtime.redis.Expire(clean, scanGateKey, scanStateTTL).Err()
		return cause
	}
	next := time.Now().UTC().Add(runtime.cfg.ScanInterval)
	runtime.updateScanStatus(clean, map[string]any{"state": "failed", "task_id": taskID, "phase": "failed", "next_scan_at": iso(next), "last_error": fmt.Sprintf("%T", cause)})
	runtime.compareDelete(clean, scanGateKey, taskID)
	runtime.compareDelete(clean, scanPauseKey, taskID)
	return cause
}

func (runtime *discoveryRuntime) supersededScan(taskID string) map[string]any {
	return map[string]any{"status": "superseded", "task_id": taskID, "discovered": 0, "events": 0}
}

func (runtime *discoveryRuntime) compareDelete(ctx context.Context, key, expected string) {
	_, _ = runtime.redis.Eval(ctx, `if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0`, []string{key}, expected).Result()
}

// DiscoveryScheduler replaces the five-second Celery ensure loop and also
// guarantees that durable outbox rows continue moving during a quiet scan.
type DiscoveryScheduler struct {
	cfg     config.Config
	db      *pgxpool.Pool
	redis   *redis.Client
	store   *Store
	lastOut time.Time
}

func NewDiscoveryScheduler(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) *DiscoveryScheduler {
	return &DiscoveryScheduler{cfg: cfg, db: db, redis: redisClient, store: NewStore(db)}
}

func (scheduler *DiscoveryScheduler) Enabled() bool {
	return completedWorkerLane(scheduler.cfg, "discovery")
}

func (scheduler *DiscoveryScheduler) Tick(ctx context.Context) error {
	if !scheduler.Enabled() {
		return nil
	}
	if time.Since(scheduler.lastOut) >= 30*time.Second {
		_, err := scheduler.store.Enqueue(ctx, EnqueueParams{Queue: "io", TaskType: dispatchOutboxTask, Payload: map[string]any{}, Priority: 4, MaxAttempts: 3, DedupeKey: "news-outbox-dispatch"})
		if err != nil {
			return err
		}
		scheduler.lastOut = time.Now()
	}
	return scheduler.ensureScan(ctx)
}

func (scheduler *DiscoveryScheduler) ensureScan(ctx context.Context) error {
	gate, err := scheduler.redis.Get(ctx, scanGateKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	status := map[string]any{}
	if raw, readErr := scheduler.redis.Get(ctx, scanStatusKey).Bytes(); readErr == nil {
		_ = json.Unmarshal(raw, &status)
	}
	if gate != "" {
		heartbeat := discoveryTime(status["heartbeat_at"])
		if !heartbeat.IsZero() && time.Since(heartbeat) > 10*time.Minute && stringValue(status["state"]) != "paused" {
			_, _ = scheduler.redis.Eval(ctx, `if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0`, []string{scanGateKey}, gate).Result()
			gate = ""
		}
	}
	if gate != "" {
		return nil
	}
	if next := discoveryTime(status["next_scan_at"]); !next.IsZero() && time.Now().UTC().Before(next) {
		return nil
	}
	taskID := uuid.New()
	claimed, err := scheduler.redis.SetNX(ctx, scanGateKey, taskID.String(), scanStateTTL).Result()
	if err != nil || !claimed {
		return err
	}
	_ = scheduler.redis.Del(ctx, scanPauseKey).Err()
	for key, value := range map[string]any{"state": "queued", "task_id": taskID.String(), "phase": "queued", "paused_from_phase": nil, "current": 0, "total": 0, "started_at": nil, "next_scan_at": nil, "last_error": nil, "heartbeat_at": iso(time.Now())} {
		status[key] = value
	}
	body, _ := json.Marshal(status)
	actualID := taskID
	if err = scheduler.redis.Set(ctx, scanStatusKey, body, 0).Err(); err == nil {
		actualID, err = scheduler.store.Enqueue(ctx, EnqueueParams{ID: taskID, Queue: "io", TaskType: ScanNewsTask, Payload: map[string]any{}, Priority: 5, MaxAttempts: 4, DedupeKey: "news-scan"})
	}
	if err != nil {
		_, _ = scheduler.redis.Eval(ctx, `if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0`, []string{scanGateKey}, taskID.String()).Result()
		return err
	}
	if actualID != taskID {
		_, _ = scheduler.redis.Eval(ctx, `if redis.call('get',KEYS[1])==ARGV[1] then redis.call('set',KEYS[1],ARGV[2],'EX',ARGV[3]); return 1 end return 0`, []string{scanGateKey}, taskID.String(), actualID.String(), fmt.Sprint(int(scanStateTTL.Seconds()))).Result()
		status["task_id"] = actualID.String()
		body, _ = json.Marshal(status)
		_ = scheduler.redis.Set(ctx, scanStatusKey, body, 0).Err()
	}
	return nil
}

func (runtime *discoveryRuntime) discover(ctx context.Context, since time.Time, limit int) discoveryBatch {
	type outcome struct {
		batch discoveryBatch
	}
	providers := []func(context.Context, time.Time, int) discoveryBatch{
		runtime.discoverFMP, runtime.discoverRSS, runtime.discoverAkshare, runtime.discoverMCPFeeds,
	}
	results := make(chan outcome, len(providers))
	for _, provider := range providers {
		provider := provider
		go func() { results <- outcome{batch: provider(ctx, since, limit)} }()
	}
	unique := map[string]discoveredNews{}
	seenURLs := map[string]bool{}
	batch := discoveryBatch{}
	for range providers {
		value := (<-results).batch
		batch.Reports = append(batch.Reports, value.Reports...)
		batch.Errors = append(batch.Errors, value.Errors...)
		for _, item := range value.Items {
			item = enrichDiscoveryLineage(item)
			if seenURLs[item.URL] {
				continue
			}
			unique[item.ContentHash] = item
			seenURLs[item.URL] = true
		}
	}
	bySource := map[string][]discoveredNews{}
	for _, item := range unique {
		bySource[item.Source] = append(bySource[item.Source], item)
	}
	for _, items := range bySource {
		sort.Slice(items, func(i, j int) bool { return items[i].PublishedAt.After(items[j].PublishedAt) })
		if len(items) > limit {
			items = items[:limit]
		}
		batch.Items = append(batch.Items, items...)
	}
	sort.Slice(batch.Items, func(i, j int) bool { return batch.Items[i].PublishedAt.After(batch.Items[j].PublishedAt) })
	return batch
}

func (runtime *discoveryRuntime) discoverFMP(ctx context.Context, since time.Time, limit int) discoveryBatch {
	attempted := time.Now().UTC()
	feeds := []struct {
		Endpoint, Source string
		Crypto           bool
	}{{"news/stock-latest", "FMP Stock News", false}, {"news/crypto-latest", "FMP Crypto News", true}, {"news/general-latest", "FMP General News", false}}
	batch := discoveryBatch{}
	if runtime.cfg.FMPAccessToken == "" || !runtime.providerEnabled(ctx, "FMP", true) {
		return batch
	}
	effectiveSince := since
	if boundary := attempted.Add(-time.Duration(runtime.cfg.FMPNewsLookback) * time.Hour); boundary.Before(effectiveSince) {
		effectiveSince = boundary
	}
	for _, feed := range feeds {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, runtime.cfg.FMPBaseURL+"/"+feed.Endpoint, nil)
		query := request.URL.Query()
		query.Set("page", "0")
		query.Set("limit", fmt.Sprint(min(limit, 100)))
		request.URL.RawQuery = query.Encode()
		request.Header.Set("apikey", runtime.cfg.FMPAccessToken)
		response, err := runtime.client.Do(request)
		if err != nil {
			batch.addSourceError(feed.Source, "fmp", attempted, err)
			continue
		}
		var payload any
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&payload)
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 || decodeErr != nil {
			if decodeErr == nil {
				decodeErr = fmt.Errorf("FMP HTTP %d", response.StatusCode)
			}
			batch.addSourceError(feed.Source, "fmp", attempted, decodeErr)
			continue
		}
		items := discoveryObjectItems(payload)
		sourceItems := make([]discoveredNews, 0, len(items))
		for _, raw := range items {
			published := discoveryTime(firstDiscoveryValue(raw, "publishedDate", "date"))
			if published.IsZero() || published.Before(effectiveSince) {
				continue
			}
			title := strings.TrimSpace(stringValue(raw["title"]))
			urlValue := strings.TrimSpace(fallbackString(stringValue(raw["url"]), stringValue(raw["link"])))
			if title == "" || !validDiscoveryURL(urlValue) {
				continue
			}
			symbols := stringSlice(raw["symbols"])
			if symbol := strings.TrimSpace(stringValue(raw["symbol"])); symbol != "" {
				symbols = appendUnique(symbols, symbol)
			}
			sourceItems = append(sourceItems, newDiscoveredNews(feed.Source, "fmp", "aggregator", title, firstDiscoveryString(raw, "text", "snippet"), urlValue, "en", published, symbols, map[string]any{"crypto": feed.Crypto, "site": raw["site"]}))
		}
		batch.Items = append(batch.Items, sourceItems...)
		batch.Reports = append(batch.Reports, healthySourceReport(feed.Source, "fmp", attempted, sourceItems))
	}
	return batch
}

func (runtime *discoveryRuntime) providerEnabled(ctx context.Context, name string, fallback bool) bool {
	var enabled bool
	err := runtime.db.QueryRow(ctx, `SELECT enabled FROM mcp_sources WHERE name=$1`, name).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return fallback
	}
	return err == nil && enabled
}

func (runtime *discoveryRuntime) discoverAkshare(ctx context.Context, since time.Time, limit int) discoveryBatch {
	attempted := time.Now().UTC()
	if !runtime.cfg.AkshareEnabled || runtime.cfg.MarketAdapterURL == "" {
		return discoveryBatch{}
	}
	body, _ := json.Marshal(map[string]any{"since": iso(since), "limit": limit})
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, runtime.cfg.MarketAdapterURL+"/v1/news", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := runtime.client.Do(request)
	if err != nil {
		batch := discoveryBatch{}
		batch.addSourceError("东方财富/AkShare", "akshare", attempted, err)
		return batch
	}
	defer response.Body.Close()
	var payload any
	err = json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&payload)
	if response.StatusCode < 200 || response.StatusCode >= 300 || err != nil {
		if err == nil {
			err = fmt.Errorf("market adapter HTTP %d", response.StatusCode)
		}
		batch := discoveryBatch{}
		batch.addSourceError("东方财富/AkShare", "akshare", attempted, err)
		return batch
	}
	batch := discoveryBatch{}
	grouped := map[string][]discoveredNews{}
	for _, raw := range discoveryObjectItems(payload) {
		published := discoveryTime(firstDiscoveryValue(raw, "published_at", "time", "date"))
		if published.IsZero() || published.Before(since) {
			continue
		}
		source := fallbackString(stringValue(raw["source"]), "东方财富/AkShare")
		item := newDiscoveredNews(source, "akshare", "aggregator", stringValue(raw["title"]), stringValue(raw["summary"]), stringValue(raw["url"]), fallbackString(stringValue(raw["language"]), "zh"), published, nil, map[string]any{"time_normalization": "adapter->UTC:v1"})
		if item.Title != "" && validDiscoveryURL(item.URL) {
			batch.Items = append(batch.Items, item)
			grouped[source] = append(grouped[source], item)
		}
	}
	if len(grouped) == 0 {
		batch.Reports = append(batch.Reports, healthySourceReport("东方财富/AkShare", "akshare", attempted, nil))
	}
	for source, items := range grouped {
		batch.Reports = append(batch.Reports, healthySourceReport(source, "akshare", attempted, items))
	}
	return batch
}

type rssDocument struct {
	Channel struct {
		Title string `xml:"title"`
		Items []struct {
			Title       string `xml:"title"`
			Link        string `xml:"link"`
			Description string `xml:"description"`
			PubDate     string `xml:"pubDate"`
		} `xml:"item"`
	} `xml:"channel"`
	Title   string `xml:"title"`
	Entries []struct {
		Title     string `xml:"title"`
		Summary   string `xml:"summary"`
		Updated   string `xml:"updated"`
		Published string `xml:"published"`
		Links     []struct {
			Href string `xml:"href,attr"`
		} `xml:"link"`
	} `xml:"entry"`
}

func (runtime *discoveryRuntime) discoverRSS(ctx context.Context, since time.Time, limit int) discoveryBatch {
	type feed struct{ URL, Quality string }
	feeds := make([]feed, 0, len(runtime.cfg.RSSFeeds)+len(runtime.cfg.OfficialRSSFeeds))
	for _, value := range runtime.cfg.RSSFeeds {
		feeds = append(feeds, feed{value, "professional"})
	}
	for _, value := range runtime.cfg.OfficialRSSFeeds {
		feeds = append(feeds, feed{value, "official"})
	}
	batch := discoveryBatch{}
	for _, feed := range feeds {
		attempted := time.Now().UTC()
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, feed.URL, nil)
		response, err := runtime.client.Do(request)
		if err != nil {
			batch.addSourceError(feed.URL, "rss", attempted, err)
			continue
		}
		var document rssDocument
		err = xml.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&document)
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 || err != nil {
			if err == nil {
				err = fmt.Errorf("RSS HTTP %d", response.StatusCode)
			}
			batch.addSourceError(feed.URL, "rss", attempted, err)
			continue
		}
		source := fallbackString(strings.TrimSpace(document.Channel.Title), strings.TrimSpace(document.Title))
		if source == "" {
			source = feed.URL
		}
		items := make([]discoveredNews, 0)
		for _, raw := range document.Channel.Items {
			published := discoveryTime(raw.PubDate)
			if published.IsZero() {
				published = attempted
			}
			if published.Before(since) || strings.TrimSpace(raw.Title) == "" || !validDiscoveryURL(raw.Link) {
				continue
			}
			items = append(items, newDiscoveredNews(source, "rss", feed.Quality, raw.Title, raw.Description, raw.Link, "en", published, nil, nil))
		}
		for _, raw := range document.Entries {
			link := ""
			if len(raw.Links) > 0 {
				link = raw.Links[0].Href
			}
			published := discoveryTime(fallbackString(raw.Published, raw.Updated))
			if published.IsZero() {
				published = attempted
			}
			if published.Before(since) || strings.TrimSpace(raw.Title) == "" || !validDiscoveryURL(link) {
				continue
			}
			items = append(items, newDiscoveredNews(source, "rss", feed.Quality, raw.Title, raw.Summary, link, "en", published, nil, nil))
		}
		if len(items) > limit {
			items = items[:limit]
		}
		batch.Items = append(batch.Items, items...)
		batch.Reports = append(batch.Reports, healthySourceReport(source, "rss", attempted, items))
	}
	return batch
}

type discoveryMCPSource struct {
	Name, URL, AuthType string
	AuthHeader, Secret  *string
	Mappings            map[string]map[string]any
}

func (runtime *discoveryRuntime) discoverMCPFeeds(ctx context.Context, since time.Time, limit int) discoveryBatch {
	rows, err := runtime.db.Query(ctx, `SELECT name,url,auth_type,auth_header_name,encrypted_secret,tool_mappings::jsonb FROM mcp_sources WHERE enabled=true AND tool_mappings::jsonb ? 'news_feed' ORDER BY priority DESC`)
	if err != nil {
		return discoveryBatch{Errors: []string{"mcp-news: " + err.Error()}}
	}
	defer rows.Close()
	sources := make([]discoveryMCPSource, 0)
	for rows.Next() {
		var source discoveryMCPSource
		var mappings []byte
		if rows.Scan(&source.Name, &source.URL, &source.AuthType, &source.AuthHeader, &source.Secret, &mappings) == nil {
			_ = json.Unmarshal(mappings, &source.Mappings)
			sources = append(sources, source)
		}
	}
	batch := discoveryBatch{}
	for _, source := range sources {
		attempted := time.Now().UTC()
		items, callErr := runtime.fetchMCPFeed(ctx, source, since, limit)
		if callErr != nil {
			batch.addSourceError(source.Name, "mcp-news", attempted, callErr)
			continue
		}
		batch.Items = append(batch.Items, items...)
		batch.Reports = append(batch.Reports, healthySourceReport(source.Name, "mcp-news", attempted, items))
	}
	return batch
}

func (runtime *discoveryRuntime) fetchMCPFeed(ctx context.Context, source discoveryMCPSource, since time.Time, limit int) ([]discoveredNews, error) {
	mapping := source.Mappings["news_feed"]
	if mapping == nil || stringValue(mapping["tool_name"]) == "" {
		return nil, errors.New("news_feed tool mapping is missing")
	}
	headers := map[string]string{}
	if source.AuthType != "none" && source.Secret != nil {
		secret, err := decryptDiscoverySecret(*source.Secret, runtime.cfg.MCPSecretKey)
		if err != nil {
			return nil, err
		}
		if source.AuthType == "bearer" {
			headers["Authorization"] = "Bearer " + secret
		} else {
			key := "X-API-Key"
			if source.AuthHeader != nil && *source.AuthHeader != "" {
				key = *source.AuthHeader
			}
			headers[key] = secret
		}
	}
	client := &http.Client{Timeout: runtime.cfg.WebSearchTimeout}
	if client.Timeout <= 0 {
		client.Timeout = 20 * time.Second
	}
	initialize := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "rag-agentic-looping-go-discovery", "version": "1"}}}
	_, session, err := discoveryMCPRequest(ctx, client, source.URL, headers, "", initialize)
	if err != nil {
		return nil, err
	}
	_, _, _ = discoveryMCPRequest(ctx, client, source.URL, headers, session, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized", "params": map[string]any{}})
	cursor := ""
	result := make([]discoveredNews, 0)
	seen := map[string]bool{}
	for page := 0; page < 20; page++ {
		arguments := objectValue(mapping["defaults"])
		if arguments == nil {
			arguments = map[string]any{}
		}
		arguments = cloneObject(arguments)
		for canonical, rawTarget := range objectValue(mapping["input_bindings"]) {
			if canonical == "cursor" && cursor != "" {
				arguments[stringValue(rawTarget)] = cursor
			}
		}
		response, _, callErr := discoveryMCPRequest(ctx, client, source.URL, headers, session, map[string]any{"jsonrpc": "2.0", "id": page + 2, "method": "tools/call", "params": map[string]any{"name": mapping["tool_name"], "arguments": arguments}})
		if callErr != nil {
			return nil, callErr
		}
		payload, payloadErr := discoveryMCPResult(response)
		if payloadErr != nil {
			return nil, payloadErr
		}
		items, next, more, reached := normalizeMCPNews(payload, source.Name, fallbackString(stringValue(mapping["output_adapter"]), "news_items_v1"), since)
		for _, item := range items {
			if !seen[item.ContentHash] {
				result = append(result, item)
				seen[item.ContentHash] = true
			}
			if len(result) >= limit {
				return result, nil
			}
		}
		if reached || !more || next == "" || next == cursor {
			break
		}
		cursor = next
	}
	return result, nil
}

func decryptDiscoverySecret(ciphertext, keyValue string) (string, error) {
	key, err := fernet.DecodeKey(keyValue)
	if err != nil {
		return "", errors.New("MCP secret key is invalid")
	}
	value := fernet.VerifyAndDecrypt([]byte(ciphertext), 0, []*fernet.Key{key})
	if value == nil {
		return "", errors.New("stored MCP credential cannot be decrypted")
	}
	return string(value), nil
}

func discoveryMCPRequest(ctx context.Context, client *http.Client, target string, headers map[string]string, session string, payload any) (map[string]any, string, error) {
	body, _ := json.Marshal(payload)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, session, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if session != "" {
		request.Header.Set("Mcp-Session-Id", session)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, session, err
	}
	defer response.Body.Close()
	if value := response.Header.Get("Mcp-Session-Id"); value != "" {
		session = value
	}
	if response.StatusCode == http.StatusAccepted || response.StatusCode == http.StatusNoContent {
		return map[string]any{}, session, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, session, fmt.Errorf("MCP HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(message)))
	}
	var output map[string]any
	if strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		scanner := bufio.NewScanner(io.LimitReader(response.Body, 4<<20))
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data:") && json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &output) == nil {
				break
			}
		}
	} else if json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&output) != nil {
		return nil, session, errors.New("MCP returned invalid JSON")
	}
	if output == nil {
		return nil, session, errors.New("MCP stream returned no JSON-RPC response")
	}
	if rpcError := output["error"]; rpcError != nil {
		return nil, session, fmt.Errorf("MCP error: %v", rpcError)
	}
	return output, session, nil
}

func discoveryMCPResult(response map[string]any) (any, error) {
	result := objectValue(response["result"])
	if boolValue(result["isError"]) {
		return nil, errors.New("MCP tool returned an error")
	}
	if result["structuredContent"] != nil {
		return result["structuredContent"], nil
	}
	values := make([]any, 0)
	for _, raw := range anySlice(result["content"]) {
		item := objectValue(raw)
		textValue := stringValue(item["text"])
		if textValue == "" {
			continue
		}
		var decoded any
		if json.Unmarshal([]byte(textValue), &decoded) == nil {
			values = append(values, decoded)
		} else {
			values = append(values, textValue)
		}
	}
	if len(values) == 1 {
		return values[0], nil
	}
	return values, nil
}

func normalizeMCPNews(payload any, source, adapter string, since time.Time) ([]discoveredNews, string, bool, bool) {
	next, more := "", false
	if object := objectValue(payload); object != nil {
		if data := objectValue(object["data"]); data != nil {
			next, more = stringValue(data["next_cursor"]), boolValue(data["has_more"])
		}
	}
	observed := time.Now().UTC()
	result := make([]discoveredNews, 0)
	reached := false
	for _, raw := range discoveryAdapterItems(payload, adapter) {
		content := strings.TrimSpace(firstDiscoveryString(raw, "content", "introduction"))
		title := strings.TrimSpace(stringValue(raw["title"]))
		if title == "" {
			title = discoveryHeadline(content, 120)
		}
		urlValue := strings.TrimSpace(fallbackString(stringValue(raw["url"]), stringValue(raw["link"])))
		published := discoveryTime(firstDiscoveryValue(raw, "time", "published_at"))
		if !published.IsZero() && published.Before(since) {
			reached = true
			continue
		}
		if title == "" || content == "" || published.IsZero() || !validDiscoveryURL(urlValue) {
			continue
		}
		fingerprint := sha256.Sum256([]byte(strings.ToLower(strings.Join(strings.Fields(content), " "))))
		item := newDiscoveredNews(source, "mcp-news", "professional", title, content, canonicalDiscoveryURL(urlValue), "zh", published, nil, map[string]any{"mcp_source": source, "mcp_adapter": adapter, "upstream_id": raw["id"]})
		item.ObservedAt, item.ContentHash = observed, hex.EncodeToString(fingerprint[:])
		result = append(result, item)
	}
	return result, next, more, reached
}

type sourceFilterConfig struct {
	Enabled   bool
	Whitelist []string
	Blacklist []string
}

func (runtime *discoveryRuntime) filterNews(ctx context.Context, items []discoveredNews) ([]discoveredNews, int, error) {
	configValue := sourceFilterConfig{Enabled: true, Blacklist: []string{"天气"}}
	var body []byte
	err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM integration_settings WHERE key='source-filter'`).Scan(&body)
	if err == nil {
		var payload map[string]any
		if json.Unmarshal(body, &payload) == nil {
			configValue.Enabled = boolDefault(payload["enabled"], true)
			configValue.Whitelist = stringSlice(payload["whitelist_keywords"])
			configValue.Blacklist = stringSlice(payload["blacklist_keywords"])
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, err
	}
	accepted := make([]discoveredNews, 0, len(items))
	filtered := 0
	for _, item := range items {
		allowed, keyword := evaluateDiscoveryTitle(item.Title, configValue)
		if allowed {
			accepted = append(accepted, item)
			continue
		}
		filtered++
		_, err = runtime.db.Exec(ctx, `INSERT INTO news_filter_logs(id,content_hash,source,title,url,matched_keyword,published_at,first_filtered_at,last_filtered_at,hit_count) VALUES($1,$2,$3,$4,$5,$6,$7,now(),now(),1) ON CONFLICT(content_hash) DO UPDATE SET last_filtered_at=now(),hit_count=news_filter_logs.hit_count+1,matched_keyword=excluded.matched_keyword`, uuid.New(), item.ContentHash, item.Source, item.Title, item.URL, keyword, item.PublishedAt)
		if err != nil {
			return nil, filtered, err
		}
	}
	_, err = runtime.db.Exec(ctx, `DELETE FROM news_filter_logs WHERE last_filtered_at < now()-interval '30 days'; DELETE FROM news_filter_logs WHERE id IN (SELECT id FROM news_filter_logs ORDER BY last_filtered_at DESC,id DESC OFFSET 5000)`)
	return accepted, filtered, err
}

func evaluateDiscoveryTitle(title string, cfg sourceFilterConfig) (bool, string) {
	if !cfg.Enabled {
		return true, ""
	}
	candidate := discoveryMatchText(title)
	whitelist := ""
	for _, keyword := range cfg.Whitelist {
		if normalized := discoveryMatchText(keyword); normalized != "" && strings.Contains(candidate, normalized) {
			whitelist = keyword
			break
		}
	}
	for _, keyword := range cfg.Blacklist {
		if normalized := discoveryMatchText(keyword); normalized != "" && strings.Contains(candidate, normalized) {
			return false, keyword
		}
	}
	if whitelist != "" {
		return true, whitelist
	}
	return false, "未命中白名单"
}

func discoveryMatchText(value string) string {
	return strings.ToLower(strings.TrimSpace(norm.NFKC.String(value)))
}

func (runtime *discoveryRuntime) sourceWatermarks(ctx context.Context) (map[string]time.Time, error) {
	rows, err := runtime.db.Query(ctx, `SELECT source,watermark_at FROM news_source_states WHERE watermark_at IS NOT NULL UNION ALL SELECT source,max(published_at) FROM news_items GROUP BY source`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]time.Time{}
	for rows.Next() {
		var source string
		var stamp time.Time
		if err := rows.Scan(&source, &stamp); err != nil {
			return nil, err
		}
		if current, ok := result[source]; !ok || stamp.After(current) {
			result[source] = stamp.UTC()
		}
	}
	return result, rows.Err()
}

func filterBySourceWatermark(items []discoveredNews, watermarks map[string]time.Time, lookback time.Time, overlap time.Duration) []discoveredNews {
	if overlap <= 0 {
		overlap = 10 * time.Minute
	}
	result := make([]discoveredNews, 0, len(items))
	for _, item := range items {
		threshold := lookback
		if watermark, ok := watermarks[item.Source]; ok {
			candidate := watermark.Add(-overlap)
			if candidate.After(threshold) {
				threshold = candidate
			}
		}
		if !item.PublishedAt.Before(threshold) {
			result = append(result, item)
		}
	}
	return result
}

func (runtime *discoveryRuntime) persistForExtraction(ctx context.Context, scanTaskID string, items []discoveredNews) ([]uuid.UUID, map[string]int, error) {
	pending := make([]uuid.UUID, 0, len(items))
	counts := map[string]int{}
	seen := map[uuid.UUID]bool{}
	for _, item := range items {
		tx, err := runtime.db.Begin(ctx)
		if err != nil {
			return nil, counts, err
		}
		var newsID uuid.UUID
		var processed bool
		err = tx.QueryRow(ctx, `SELECT id,exists(SELECT 1 FROM news_events WHERE payload::jsonb->'news_item_ids' ? news_items.id::text) FROM news_items WHERE content_hash=$1 OR url=$2 ORDER BY (content_hash=$1) DESC,observed_at DESC LIMIT 1`, item.ContentHash, item.URL).Scan(&newsID, &processed)
		if errors.Is(err, pgx.ErrNoRows) {
			newsID = item.ID
			metadata, _ := json.Marshal(item.Metadata)
			symbols, _ := json.Marshal(item.Symbols)
			_, err = tx.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(content_hash) DO NOTHING`, newsID, item.Source, item.SourceQuality, item.Title, item.Summary, item.URL, item.Language, item.PublishedAt, item.ObservedAt, item.AsOf, item.ContentHash, symbols, metadata)
			if err == nil {
				err = tx.QueryRow(ctx, `SELECT id FROM news_items WHERE content_hash=$1`, item.ContentHash).Scan(&newsID)
			}
		} else if err == nil && processed {
			_ = tx.Rollback(ctx)
			continue
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, counts, err
		}
		if seen[newsID] {
			_ = tx.Rollback(ctx)
			continue
		}
		seen[newsID] = true
		_, err = tx.Exec(ctx, `INSERT INTO news_processing(news_id,status,scan_task_id,celery_task_id,attempt_count,last_error,queued_at,started_at,completed_at,heartbeat_at,created_at,updated_at) VALUES($1,'dispatch_pending',$2,NULL,0,NULL,NULL,NULL,NULL,now(),now(),now()) ON CONFLICT(news_id) DO UPDATE SET status='dispatch_pending',scan_task_id=excluded.scan_task_id,celery_task_id=NULL,last_error=NULL,queued_at=NULL,started_at=NULL,completed_at=NULL,heartbeat_at=now(),updated_at=now()`, newsID, scanTaskID)
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO news_processing_outbox(id,news_id,status,force_asset_mapping,dispatch_attempts,available_at,dispatched_at,last_error,created_at,updated_at) VALUES($1,$2,'pending',false,0,now(),NULL,NULL,now(),now()) ON CONFLICT(news_id) DO UPDATE SET status='pending',force_asset_mapping=news_processing_outbox.force_asset_mapping,available_at=now(),dispatched_at=NULL,last_error=NULL,updated_at=now()`, uuid.New(), newsID)
		}
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			_ = tx.Rollback(ctx)
		}
		if err != nil {
			return nil, counts, err
		}
		pending = append(pending, newsID)
		counts[item.Source]++
	}
	return pending, counts, nil
}

func (runtime *discoveryRuntime) recordSourceReports(ctx context.Context, reports []sourceDiscoveryReport, newCounts map[string]int) error {
	for _, report := range reports {
		_, err := runtime.db.Exec(ctx, `INSERT INTO news_source_states(source,provider,status,watermark_at,last_attempt_at,last_success_at,last_error,last_discovered_count,last_new_count,consecutive_failures,created_at,updated_at) VALUES($1::varchar(120),$2::varchar(80),$3::varchar(30),$4::timestamptz,$5::timestamptz,CASE WHEN $3::text='healthy' THEN $5::timestamptz ELSE NULL END,NULLIF($6::text,''),$7::integer,$8::integer,CASE WHEN $3::text='healthy' THEN 0 ELSE 1 END,now(),now()) ON CONFLICT(source) DO UPDATE SET provider=excluded.provider,status=excluded.status,watermark_at=CASE WHEN excluded.status='healthy' THEN greatest(news_source_states.watermark_at,excluded.watermark_at) ELSE news_source_states.watermark_at END,last_attempt_at=excluded.last_attempt_at,last_success_at=CASE WHEN excluded.status='healthy' THEN excluded.last_attempt_at ELSE news_source_states.last_success_at END,last_error=CASE WHEN excluded.status='healthy' THEN NULL ELSE excluded.last_error END,last_discovered_count=excluded.last_discovered_count,last_new_count=excluded.last_new_count,consecutive_failures=CASE WHEN excluded.status='healthy' THEN 0 ELSE news_source_states.consecutive_failures+1 END,updated_at=now()`, report.Source, report.Provider, report.Status, report.LatestPublishedAt, report.AttemptedAt, report.Error, report.DiscoveredCount, newCounts[report.Source])
		if err != nil {
			return err
		}
	}
	return nil
}

func (runtime *discoveryRuntime) dispatchOutboxLimit(ctx context.Context, limit int, only []uuid.UUID) (map[string]any, error) {
	if limit < 1 {
		limit = 50
	}
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	query := `SELECT id,news_id,force_asset_mapping,dispatch_attempts+1 FROM news_processing_outbox WHERE status IN ('pending','failed') AND available_at<=now()`
	args := []any{}
	if len(only) > 0 {
		query += ` AND news_id=ANY($1)`
		args = append(args, only)
	}
	query += fmt.Sprintf(` ORDER BY available_at,created_at FOR UPDATE SKIP LOCKED LIMIT %d`, limit)
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	type claim struct {
		OutboxID, NewsID uuid.UUID
		Force            bool
		Attempt          int
	}
	claims := make([]claim, 0)
	for rows.Next() {
		var item claim
		if rows.Scan(&item.OutboxID, &item.NewsID, &item.Force, &item.Attempt) == nil {
			claims = append(claims, item)
		}
	}
	rows.Close()
	for _, item := range claims {
		if _, err = tx.Exec(ctx, `UPDATE news_processing_outbox SET status='dispatching',dispatch_attempts=$2,updated_at=now() WHERE id=$1`, item.OutboxID, item.Attempt); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	queued := make([]any, 0, len(claims))
	failed := 0
	shared := &ExtractRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis, client: runtime.client}
	for _, item := range claims {
		var title, source string
		if err = runtime.db.QueryRow(ctx, `SELECT title,source FROM news_items WHERE id=$1`, item.NewsID).Scan(&title, &source); err != nil {
			runtime.markDispatchFailed(ctx, item.OutboxID, item.NewsID, item.Attempt, err)
			failed++
			continue
		}
		taskID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("news-processing:%s:%d", item.OutboxID, item.Attempt)))
		instanceID := shared.selectDownstreamInstance(ctx, "extract", len(runtime.cfg.ExtractURLs))
		shared.recordModelTask(ctx, "extract", taskID.String(), "news_extraction", item.NewsID.String(), title, source, "durable_outbox", instanceID)
		kwargs := map[string]any{"model_instance_id": instanceID}
		if item.Force {
			kwargs["force_asset_mapping"] = true
		}
		actual, queueErr := NewStore(runtime.db).Enqueue(ctx, EnqueueParams{ID: taskID, Queue: "extract", TaskType: retryNewsTask, Payload: map[string]any{"args": []any{item.NewsID.String()}, "kwargs": kwargs}, Priority: 5, MaxAttempts: 3, DedupeKey: "news:" + item.NewsID.String()})
		if queueErr != nil {
			runtime.markDispatchFailed(ctx, item.OutboxID, item.NewsID, item.Attempt, queueErr)
			shared.updateModelTask(ctx, taskID.String(), "failed", item.Attempt, item.NewsID.String(), title, source, queueErr.Error(), nil)
			failed++
			continue
		}
		if actual != taskID {
			_ = runtime.redis.HDel(ctx, "market-loop:model-queue:extract:tasks", taskID.String()).Err()
		}
		_, err = runtime.db.Exec(ctx, `WITH updated AS (UPDATE news_processing SET status='queued',celery_task_id=$2,attempt_count=greatest(attempt_count,$3),last_error=NULL,queued_at=now(),started_at=NULL,completed_at=NULL,heartbeat_at=now(),updated_at=now() WHERE news_id=$1) UPDATE news_processing_outbox SET status='dispatched',dispatched_at=now(),last_error=NULL,updated_at=now() WHERE id=$4`, item.NewsID, actual, item.Attempt, item.OutboxID)
		if err != nil {
			return nil, err
		}
		queued = append(queued, map[string]any{"news_id": item.NewsID, "task_id": actual})
	}
	return map[string]any{"claimed": len(claims), "queued": queued, "failed": failed}, nil
}

func (runtime *discoveryRuntime) markDispatchFailed(ctx context.Context, outboxID, newsID uuid.UUID, attempt int, cause error) {
	delay := 1 << min(attempt, 8)
	if delay > 300 {
		delay = 300
	}
	detail := truncateRunes(fmt.Sprintf("%T: %v", cause, cause), 500)
	_, _ = runtime.db.Exec(context.WithoutCancel(ctx), `WITH updated AS (UPDATE news_processing SET status='dispatch_failed',attempt_count=greatest(attempt_count,$3),last_error=$4,heartbeat_at=now(),updated_at=now() WHERE news_id=$2) UPDATE news_processing_outbox SET status='failed',available_at=now()+$5::interval,last_error=$4,updated_at=now() WHERE id=$1`, outboxID, newsID, attempt, detail, fmt.Sprintf("%d seconds", delay))
}

func newDiscoveredNews(source, provider, quality, title, summary, rawURL, language string, published time.Time, symbols []string, metadata map[string]any) discoveredNews {
	title, summary, rawURL = strings.TrimSpace(title), strings.TrimSpace(summary), strings.TrimSpace(rawURL)
	digest := sha256.Sum256([]byte(title + "|" + rawURL))
	if metadata == nil {
		metadata = map[string]any{}
	}
	return discoveredNews{ID: uuid.New(), Source: source, Provider: provider, SourceQuality: quality, Title: title, Summary: summary, URL: rawURL, Language: language, PublishedAt: published.UTC(), ObservedAt: time.Now().UTC(), AsOf: published.UTC(), ContentHash: hex.EncodeToString(digest[:]), Symbols: symbols, Metadata: metadata}
}

func enrichDiscoveryLineage(item discoveredNews) discoveredNews {
	canonical := canonicalDiscoveryURL(item.URL)
	parsed, _ := url.Parse(canonical)
	publisher := strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
	if publisher == "" {
		publisher = normalizeDiscoveryText(item.Source)
	}
	original := publisher
	combined := strings.ToLower(item.Source + " " + item.Title + " " + item.Summary)
	for marker, value := range map[string]string{"reuters": "reuters", "路透": "reuters", "bloomberg": "bloomberg", "彭博": "bloomberg", "associated press": "associated-press", "新华社": "xinhua"} {
		if strings.Contains(combined, marker) {
			original = value
			break
		}
	}
	fingerprint := sha256.Sum256([]byte(normalizeDiscoveryText(item.Title) + "|" + normalizeDiscoveryText(item.Summary)))
	metadata := cloneObject(item.Metadata)
	metadata["source_lineage"] = map[string]any{"canonical_url": canonical, "publisher_domain": publisher, "original_source": original, "syndication_group": "origin:" + normalizeDiscoveryText(original), "content_fingerprint": hex.EncodeToString(fingerprint[:])}
	item.Metadata = metadata
	return item
}

func normalizeDiscoveryText(value string) string {
	var builder strings.Builder
	for _, character := range strings.ToLower(norm.NFKC.String(value)) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			builder.WriteRune(character)
		}
	}
	return builder.String()
}

func canonicalDiscoveryURL(value string) string {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil {
		return strings.TrimSpace(value)
	}
	parsed.Scheme, parsed.Host, parsed.Fragment = strings.ToLower(parsed.Scheme), strings.ToLower(parsed.Host), ""
	if strings.HasPrefix(parsed.Host, "www.") {
		parsed.Host = strings.TrimPrefix(parsed.Host, "www.")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	query := parsed.Query()
	for key := range query {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "utm_") || lower == "ref" || lower == "referrer" || lower == "source" || lower == "fbclid" || lower == "gclid" {
			query.Del(key)
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func validDiscoveryURL(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}

func discoveryTime(value any) time.Time {
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

func discoveryObjectItems(payload any) []map[string]any {
	raw := payload
	if object := objectValue(payload); object != nil {
		raw = firstDiscoveryValue(object, "data", "results", "items")
		if nested := objectValue(raw); nested != nil {
			raw = firstDiscoveryValue(nested, "items", "results", "data")
		}
	}
	if object := objectValue(raw); object != nil {
		raw = []any{object}
	}
	result := make([]map[string]any, 0)
	for _, item := range anySlice(raw) {
		if object := objectValue(item); object != nil {
			result = append(result, object)
		}
	}
	return result
}

func discoveryAdapterItems(payload any, adapter string) []map[string]any {
	if adapter == "jin10_flash_v1" {
		if data := objectValue(objectValue(payload)["data"]); data != nil {
			return discoveryObjectItems(data["items"])
		}
	}
	return discoveryObjectItems(payload)
}

func firstDiscoveryValue(value map[string]any, keys ...string) any {
	for _, key := range keys {
		if item := value[key]; item != nil {
			return item
		}
	}
	return nil
}

func firstDiscoveryString(value map[string]any, keys ...string) string {
	for _, key := range keys {
		if item := strings.TrimSpace(stringValue(value[key])); item != "" {
			return item
		}
	}
	return ""
}

func discoveryHeadline(content string, limit int) string {
	compact := strings.Join(strings.Fields(content), " ")
	if compact == "" {
		return ""
	}
	for index, character := range compact {
		if strings.ContainsRune("。！？；.!?", character) {
			compact = compact[:index+len(string(character))]
			break
		}
	}
	return truncateRunes(compact, limit)
}

func cloneObject(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func boolDefault(value any, fallback bool) bool {
	if value == nil {
		return fallback
	}
	result, ok := value.(bool)
	if !ok {
		return fallback
	}
	return result
}

func discoveryInt(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case int64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func (batch *discoveryBatch) addSourceError(source, provider string, attempted time.Time, cause error) {
	detail := truncateRunes(fmt.Sprintf("%T: %v", cause, cause), 500)
	batch.Errors = append(batch.Errors, provider+": "+detail)
	batch.Reports = append(batch.Reports, sourceDiscoveryReport{Source: source, Provider: provider, Status: "error", AttemptedAt: attempted, Error: detail})
}

func healthySourceReport(source, provider string, attempted time.Time, items []discoveredNews) sourceDiscoveryReport {
	report := sourceDiscoveryReport{Source: source, Provider: provider, Status: "healthy", AttemptedAt: attempted, DiscoveredCount: len(items)}
	for _, item := range items {
		if report.LatestPublishedAt == nil || item.PublishedAt.After(*report.LatestPublishedAt) {
			value := item.PublishedAt
			report.LatestPublishedAt = &value
		}
	}
	return report
}
