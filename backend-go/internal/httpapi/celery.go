package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const celeryPrioritySeparator = "\x06\x16"

type celeryMessage struct {
	Body            string         `json:"body"`
	ContentEncoding string         `json:"content-encoding"`
	ContentType     string         `json:"content-type"`
	Headers         map[string]any `json:"headers"`
	Properties      map[string]any `json:"properties"`
}

// publishCelery implements Celery protocol v2 over Kombu's Redis transport.
// The Go API owns admission and persistence while the existing Python workers
// remain the execution plane during the API-language cutover.
func (s *Server) publishCelery(ctx context.Context, task, queue, taskID string, args []any, kwargs map[string]any, priority int) error {
	if taskID == "" {
		taskID = uuid.NewString()
	}
	if args == nil {
		args = []any{}
	}
	if kwargs == nil {
		kwargs = map[string]any{}
	}
	body, err := json.Marshal([]any{args, kwargs, map[string]any{"callbacks": nil, "errbacks": nil, "chain": nil, "chord": nil}})
	if err != nil {
		return err
	}
	deliveryTag := uuid.NewString()
	message := celeryMessage{
		Body:            base64.StdEncoding.EncodeToString(body),
		ContentEncoding: "utf-8",
		ContentType:     "application/json",
		Headers: map[string]any{
			"lang": "py", "task": task, "id": taskID, "shadow": nil, "eta": nil, "expires": nil,
			"group": nil, "group_index": nil, "retries": 0, "timelimit": []any{nil, nil},
			"root_id": taskID, "parent_id": nil, "argsrepr": truncateText(fmt.Sprint(args), 1024),
			"kwargsrepr": truncateText(fmt.Sprint(kwargs), 1024), "origin": "go-api", "ignore_result": false,
			"replaced_task_nesting": 0, "stamped_headers": nil, "stamps": map[string]any{},
		},
		Properties: map[string]any{
			"correlation_id": taskID, "reply_to": uuid.NewString(), "delivery_mode": 2,
			"delivery_info": map[string]any{"exchange": "", "routing_key": queue},
			"priority":      priority, "body_encoding": "base64", "delivery_tag": deliveryTag,
		},
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return s.redis.LPush(ctx, celeryQueueKey(queue, priority), encoded).Err()
}

func celeryQueueKey(queue string, priority int) string {
	// Kombu's Redis transport rounds priorities down to the configured
	// priority_steps (0, 3, 6, 9). Publishing to any other suffix would create
	// a Redis list that Celery workers never poll.
	step := 0
	for _, candidate := range []int{3, 6, 9} {
		if priority >= candidate {
			step = candidate
		}
	}
	if step == 0 {
		return queue
	}
	return queue + celeryPrioritySeparator + strconv.Itoa(step)
}

func (s *Server) waitCelery(ctx context.Context, taskID string, timeout time.Duration) (any, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, errors.New("background task timed out")
		case <-ticker.C:
			raw, err := s.redis.Get(ctx, "celery-task-meta-"+taskID).Bytes()
			if err != nil {
				continue
			}
			var payload map[string]any
			if json.Unmarshal(raw, &payload) != nil {
				continue
			}
			switch strings.ToUpper(stringValue(payload["status"])) {
			case "SUCCESS":
				return payload["result"], nil
			case "FAILURE", "REVOKED":
				return nil, fmt.Errorf("task %s: %s", strings.ToLower(stringValue(payload["status"])), stringValue(payload["exc_type"]))
			}
		}
	}
}

func (s *Server) waitGoJob(ctx context.Context, taskID string, timeout time.Duration) (any, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, errors.New("background task timed out")
		case <-ticker.C:
			var status string
			var result, errorValue []byte
			err := s.db.QueryRow(ctx, `SELECT status,coalesce(result,'null'::jsonb)::jsonb,coalesce(error,'')::text FROM go_jobs WHERE id=$1`, taskID).Scan(&status, &result, &errorValue)
			if err != nil {
				continue
			}
			switch status {
			case "completed":
				var value any
				if json.Unmarshal(result, &value) != nil {
					return nil, errors.New("Go task returned invalid JSON")
				}
				return value, nil
			case "failed", "cancelled":
				return nil, fmt.Errorf("task %s: %s", status, string(errorValue))
			}
		}
	}
}

func (s *Server) purgeCeleryQueue(ctx context.Context, queue string) int64 {
	keys := []string{queue, celeryQueueKey(queue, 3), celeryQueueKey(queue, 6), celeryQueueKey(queue, 9)}
	var purged int64
	for _, key := range keys {
		if count, err := s.redis.LLen(ctx, key).Result(); err == nil {
			purged += count
			if count > 0 {
				_ = s.redis.Del(ctx, key).Err()
			}
		}
	}
	return purged
}

func (s *Server) selectModelInstance(ctx context.Context, lane, preferred string) (string, error) {
	urls := modelURLs(lane)
	model := map[string]string{"extract": s.cfg.ExtractModel, "assist": s.cfg.AssistModel, "research": s.cfg.ResearchModel, "code": s.cfg.CodeModel}[lane]
	if preferred != "" {
		index, ok := instanceIndex(lane, preferred, len(urls))
		if !ok {
			return "", errors.New("model instance not found")
		}
		if models := fetchOllamaModels(ctx, &httpClientTwoSeconds, urls[index]); models == nil || !contains(models, model) {
			return "", errors.New("model instance is unavailable")
		}
		return preferred, nil
	}
	for index, endpoint := range urls {
		if models := fetchOllamaModels(ctx, &httpClientTwoSeconds, endpoint); models != nil && contains(models, model) {
			return fmt.Sprintf("%s-%d", lane, index), nil
		}
	}
	return "", errors.New("no healthy model instance")
}

var httpClientTwoSeconds = http.Client{Timeout: 2 * time.Second}

func instanceIndex(lane, value string, count int) (int, bool) {
	prefix := lane + "-"
	if !strings.HasPrefix(value, prefix) {
		return 0, false
	}
	index, err := strconv.Atoi(strings.TrimPrefix(value, prefix))
	return index, err == nil && index >= 0 && index < count
}

func (s *Server) trackModelTask(ctx context.Context, lane, taskID, kind, entityID, title, subtitle, source, instanceID string) {
	now := iso(time.Now())
	payload := map[string]any{
		"task_id": taskID, "instance_id": nullableString(instanceID), "kind": kind, "entity_id": nullableString(entityID),
		"title": title, "subtitle": subtitle, "source": nullableString(source), "status": "queued", "attempt": 1,
		"task_count": 1, "queued_at": now, "started_at": nil, "completed_at": nil, "updated_at": now,
		"error": nil, "metrics": map[string]any{},
	}
	body, _ := json.Marshal(payload)
	key := "market-loop:model-queue:" + lane + ":tasks"
	_ = s.redis.HSet(ctx, key, taskID, body).Err()
	_ = s.redis.Expire(ctx, key, 48*time.Hour).Err()
	if instanceID != "" {
		assignment, _ := json.Marshal(map[string]any{"task_id": taskID, "instance_id": instanceID, "status": "queued", "updated_at": now})
		assignmentKey := "market-loop:model-instance:" + lane + ":assignments"
		_ = s.redis.HSet(ctx, assignmentKey, taskID, assignment).Err()
		_ = s.redis.Expire(ctx, assignmentKey, 48*time.Hour).Err()
	}
}
