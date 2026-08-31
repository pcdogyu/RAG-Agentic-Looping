package jobs

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"
)

type Handler func(context.Context, Job) (any, error)

type Worker struct {
	Store        *Store
	ID           string
	Queues       []string
	Lease        time.Duration
	PollInterval time.Duration
	Handlers     map[string]Handler
	active       atomic.Int64
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Store == nil || w.ID == "" {
		return errors.New("worker store and ID are required")
	}
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeat.C:
			if err := w.Store.WorkerHeartbeat(ctx, w.ID, w.Queues, 1, int(w.active.Load())); err != nil {
				slog.Error("worker heartbeat", "error", err)
			}
		default:
		}
		job, err := w.Store.Claim(ctx, w.ID, w.Queues, w.Lease)
		if errors.Is(err, ErrNoJob) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.PollInterval):
				continue
			}
		}
		if err != nil {
			slog.Error("claim job", "error", err)
			time.Sleep(w.PollInterval)
			continue
		}
		w.active.Add(1)
		w.execute(ctx, job)
		w.active.Add(-1)
	}
}

func (w *Worker) execute(parent context.Context, job Job) {
	handler := w.Handlers[job.TaskType]
	if handler == nil {
		_ = w.Store.Fail(parent, job, w.ID, errors.New("unregistered Go task type: "+job.TaskType))
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(w.Lease / 3)
		defer ticker.Stop()
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				alive, err := w.Store.Heartbeat(ctx, job.ID, w.ID, w.Lease)
				if err != nil || !alive {
					cancel()
					return
				}
			}
		}
	}()
	result, err := handler(ctx, job)
	cancel()
	<-done
	if cancelled, cancelErr := w.Store.CancellationRequested(parent, job.ID); cancelErr == nil && cancelled {
		_ = w.Store.CompleteCancellation(parent, job.ID, w.ID)
		return
	}
	if err != nil {
		_ = w.Store.Fail(parent, job, w.ID, err)
		return
	}
	if err := w.Store.Complete(parent, job.ID, w.ID, result); err != nil {
		slog.Error("complete job", "job_id", job.ID, "error", err)
	}
}
