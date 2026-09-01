package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
)

func (s *Server) enqueueGoExtract(ctx context.Context, taskID, taskType string, args []any, kwargs map[string]any, priority int, dedupeKey string) (string, error) {
	return s.enqueueGoModelJob(ctx, "extract", taskID, taskType, args, kwargs, priority, dedupeKey)
}

func (s *Server) enqueueGoModelJob(ctx context.Context, queue, taskID, taskType string, args []any, kwargs map[string]any, priority int, dedupeKey string) (string, error) {
	id, err := uuid.Parse(taskID)
	if err != nil {
		return "", err
	}
	queuedID, err := jobs.NewStore(s.db).Enqueue(ctx, jobs.EnqueueParams{
		ID: id, Queue: queue, TaskType: taskType,
		Payload: map[string]any{"args": args, "kwargs": kwargs}, Priority: int16(priority),
		MaxAttempts: 3, AvailableAt: time.Now().UTC(), DedupeKey: dedupeKey,
	})
	return queuedID.String(), err
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
