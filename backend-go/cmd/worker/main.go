package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/platform"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	dependencies, err := platform.Open(ctx, cfg)
	if err != nil {
		slog.Error("open platform", "error", err)
		os.Exit(1)
	}
	defer dependencies.Close()
	if err := migrate.Up(ctx, dependencies.DB); err != nil {
		slog.Error("migrate", "error", err)
		os.Exit(1)
	}
	worker := &jobs.Worker{Store: jobs.NewStore(dependencies.DB), ID: cfg.WorkerID, Queues: cfg.WorkerQueues,
		Lease: cfg.LeaseDuration, PollInterval: cfg.PollInterval, Handlers: map[string]jobs.Handler{}}
	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
