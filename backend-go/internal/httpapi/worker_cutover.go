package httpapi

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
)

func (s *Server) goLaneCompleted(lane string) bool {
	for _, value := range s.cfg.WorkerCompletedLanes {
		if value == lane {
			return true
		}
	}
	return false
}

func (s *Server) enqueueGoExtract(ctx context.Context, taskID, taskType string, args []any, kwargs map[string]any, priority int, dedupeKey string) (string, error) {
	id, err := uuid.Parse(taskID)
	if err != nil {
		return "", err
	}
	queuedID, err := jobs.NewStore(s.db).Enqueue(ctx, jobs.EnqueueParams{
		ID: id, Queue: "extract", TaskType: taskType,
		Payload: map[string]any{"args": args, "kwargs": kwargs}, Priority: int16(priority),
		MaxAttempts: 3, AvailableAt: time.Now().UTC(), DedupeKey: dedupeKey,
	})
	return queuedID.String(), err
}
