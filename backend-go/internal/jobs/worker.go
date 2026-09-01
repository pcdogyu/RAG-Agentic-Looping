package jobs

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type Handler func(context.Context, Job) (any, error)

type Worker struct {
	Store        *Store
	ID           string
	Queues       []string
	Lease        time.Duration
	PollInterval time.Duration
	Handlers     map[string]Handler
	Concurrency  int
	Redis        *redis.Client
	active       atomic.Int64
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Store == nil || w.ID == "" {
		return errors.New("worker store and ID are required")
	}
	if w.Concurrency <= 0 {
		w.Concurrency = 1
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go w.workerHeartbeat(heartbeatCtx)
	var group sync.WaitGroup
	for index := 0; index < w.Concurrency; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			w.claimLoop(ctx)
		}()
	}
	group.Wait()
	return ctx.Err()
}

func (w *Worker) workerHeartbeat(ctx context.Context) {
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	write := func() {
		if err := w.Store.WorkerHeartbeat(ctx, w.ID, w.Queues, w.Concurrency, int(w.active.Load())); err != nil && ctx.Err() == nil {
			slog.Error("worker heartbeat", "error", err)
		}
	}
	write()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			write()
		}
	}
}

func (w *Worker) claimLoop(ctx context.Context) {
	for ctx.Err() == nil {
		job, err := w.Store.Claim(ctx, w.ID, w.Queues, w.Lease)
		if errors.Is(err, ErrNoJob) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.PollInterval):
				continue
			}
		}
		if err != nil {
			slog.Error("claim job", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.PollInterval):
			}
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
		cause := permanentJobError{errors.New("unregistered Go task type: " + job.TaskType)}
		_ = w.Store.Fail(parent, job, w.ID, cause)
		w.recordResult(parent, "failure")
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
		var permanent interface{ Permanent() bool }
		if job.Attempt >= job.MaxAttempts || errors.As(err, &permanent) && permanent.Permanent() {
			w.recordResult(parent, "failure")
		}
		return
	}
	if err := w.Store.Complete(parent, job.ID, w.ID, result); err != nil {
		slog.Error("complete job", "job_id", job.ID, "error", err)
	} else {
		w.recordResult(parent, "success")
	}
}

func (w *Worker) recordResult(ctx context.Context, result string) {
	if w.Redis != nil {
		_ = w.Redis.Incr(ctx, "market-loop:tasks:"+result).Err()
	}
}
