package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	dispatchEvolutionTask = "market_loop.dispatch_evolve_from_outcomes"
	monitorHealthTask     = "market_loop.monitor_health"
)

type operationsRuntime struct {
	cfg      config.Config
	db       *pgxpool.Pool
	redis    *redis.Client
	store    *Store
	client   *http.Client
	rollback func(context.Context) error
}

func NewOperationsHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &operationsRuntime{
		cfg: cfg, db: db, redis: redisClient, store: NewStore(db),
		client: &http.Client{Timeout: 3 * time.Second},
	}
	runtime.rollback = func(ctx context.Context) error {
		return rollbackLastKnownGood(ctx, cfg, redisClient)
	}
	return map[string]Handler{
		dispatchEvolutionTask: runtime.dispatchEvolution,
		monitorHealthTask:     runtime.monitorHealth,
	}
}

func (runtime *operationsRuntime) dispatchEvolution(ctx context.Context, _ Job) (any, error) {
	if !runtime.cfg.EvolutionEnabled {
		return map[string]any{"status": "disabled"}, nil
	}
	taskID := uuid.New()
	instanceID := runtime.selectCodeInstance(ctx)
	shared := &ExtractRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis, client: runtime.client}
	shared.recordModelTask(ctx, "code", taskID.String(), "code_evolution", "", "定期失败案例代码演进", "根据历史研究结果生成改进方案", "automatic", instanceID)
	queuedID, err := runtime.store.Enqueue(ctx, EnqueueParams{
		ID: taskID, Queue: "code", TaskType: evolveOutcomesTask,
		Payload:  taskEnvelope{Args: []any{}, Kwargs: map[string]any{"model_instance_id": instanceID}},
		Priority: 5, MaxAttempts: 3, DedupeKey: "evolution-task:" + taskID.String(),
	})
	if err != nil {
		shared.updateTrackedTask(context.WithoutCancel(ctx), "code", taskID.String(), "failed", 1, "", "", "", fmt.Sprintf("%T: %v", err, err), nil)
		return nil, permanentJobError{err}
	}
	return map[string]any{"task_id": queuedID.String(), "instance_id": instanceID}, nil
}

func (runtime *operationsRuntime) selectCodeInstance(ctx context.Context) string {
	count := len(runtime.cfg.CodeURLs)
	shared := &ExtractRuntime{cfg: runtime.cfg, redis: runtime.redis}
	preferred := shared.selectDownstreamInstance(ctx, "code", count)
	for _, baseURL := range preferredEndpoints(runtime.cfg.CodeURLs, preferred, "code") {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/tags", nil)
		if err != nil {
			continue
		}
		response, err := runtime.client.Do(request)
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		_ = response.Body.Close()
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			for index, configured := range runtime.cfg.CodeURLs {
				if strings.TrimRight(configured, "/") == strings.TrimRight(baseURL, "/") {
					return fmt.Sprintf("code-%d", index)
				}
			}
		}
	}
	return preferred
}

type healthSnapshot struct {
	FailureRate float64 `json:"failure_rate"`
	Samples     int64   `json:"samples"`
	DataStale   bool    `json:"data_stale"`
	RolledBack  bool    `json:"rolled_back"`
}

func calculateHealth(successes, failures int64, latestNews, now time.Time, scanInterval time.Duration) healthSnapshot {
	total := successes + failures
	rate := 0.0
	if total > 0 {
		rate = float64(failures) / float64(total)
	}
	stale := !latestNews.IsZero() && now.Sub(latestNews) > 3*scanInterval
	return healthSnapshot{FailureRate: rate, Samples: total, DataStale: stale}
}

func (snapshot healthSnapshot) unhealthy() bool {
	return snapshot.Samples >= 10 && snapshot.FailureRate > .10 || snapshot.DataStale
}

func (runtime *operationsRuntime) monitorHealth(ctx context.Context, _ Job) (any, error) {
	successes, err := runtime.redis.Get(ctx, "market-loop:tasks:success").Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	failures, err := runtime.redis.Get(ctx, "market-loop:tasks:failure").Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	var latestNewsValue *time.Time
	if err := runtime.db.QueryRow(ctx, `SELECT max(observed_at) FROM news_items`).Scan(&latestNewsValue); err != nil {
		return nil, err
	}
	var latestNews time.Time
	if latestNewsValue != nil {
		latestNews = *latestNewsValue
	}
	snapshot := calculateHealth(successes, failures, latestNews, time.Now().UTC(), runtime.cfg.ScanInterval)
	if snapshot.unhealthy() && runtime.cfg.EvolutionEnabled && runtime.cfg.EvolutionAutoMerge {
		if err := runtime.rollback(ctx); err != nil {
			runtime.notify(ctx, "系统健康门禁触发，但自动回滚失败；请人工检查。")
		} else {
			snapshot.RolledBack = true
			runtime.notify(ctx, fmt.Sprintf("系统已自动回滚：任务失败率 %.1f%%，数据过期：%s", snapshot.FailureRate*100, ternary(snapshot.DataStale, "是", "否")))
		}
	}
	return snapshot, nil
}

func (runtime *operationsRuntime) notify(ctx context.Context, message string) bool {
	if runtime.cfg.TelegramBotToken == "" || runtime.cfg.TelegramChatID == "" {
		return false
	}
	message = evolutionSecretPatterns[0].ReplaceAllString(message, "[REDACTED]")
	if len([]rune(message)) > 4000 {
		message = string([]rune(message)[:4000])
	}
	body, _ := json.Marshal(map[string]any{"chat_id": runtime.cfg.TelegramChatID, "text": message, "disable_web_page_preview": true})
	endpoint := "https://api.telegram.org/bot" + url.PathEscape(runtime.cfg.TelegramBotToken) + "/sendMessage"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return false
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := runtime.client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	return response.StatusCode >= 200 && response.StatusCode < 300
}

type operationsSchedule struct {
	task       string
	interval   time.Duration
	startDelay bool
}

var operationsSchedules = []operationsSchedule{
	{task: dispatchEvolutionTask, interval: 7 * 24 * time.Hour, startDelay: true},
	{task: monitorHealthTask, interval: 5 * time.Minute},
}

type OperationsScheduler struct {
	cfg   config.Config
	store *Store
	redis *redis.Client
}

func NewOperationsScheduler(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) *OperationsScheduler {
	return &OperationsScheduler{cfg: cfg, store: NewStore(db), redis: redisClient}
}

func (scheduler *OperationsScheduler) Enabled() bool {
	return completedWorkerLane(scheduler.cfg, "operations") && scheduler.cfg.EvolutionEnabled
}

func (scheduler *OperationsScheduler) Tick(ctx context.Context) error {
	if !scheduler.Enabled() {
		return nil
	}
	for _, spec := range operationsSchedules {
		key := "market-loop:go-schedule:" + spec.task
		if spec.startDelay {
			initialized, err := scheduler.redis.SetNX(ctx, key+":initialized", iso(time.Now()), 0).Result()
			if err != nil {
				return err
			}
			if initialized {
				if err := scheduler.redis.Set(ctx, key, iso(time.Now()), spec.interval).Err(); err != nil {
					_ = scheduler.redis.Del(ctx, key+":initialized").Err()
					return err
				}
				continue
			}
		}
		claimed, err := scheduler.redis.SetNX(ctx, key, iso(time.Now()), spec.interval).Result()
		if err != nil {
			return err
		}
		if !claimed {
			continue
		}
		_, err = scheduler.store.Enqueue(ctx, EnqueueParams{Queue: "operations", TaskType: spec.task, Payload: taskEnvelope{Args: []any{}, Kwargs: map[string]any{}}, Priority: 5, MaxAttempts: 3, DedupeKey: "scheduled:" + spec.task})
		if err != nil {
			_ = scheduler.redis.Del(ctx, key).Err()
			return err
		}
	}
	return nil
}
