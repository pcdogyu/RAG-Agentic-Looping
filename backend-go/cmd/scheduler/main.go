package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	store := jobs.NewStore(dependencies.DB)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := store.ReconcileExpired(ctx)
			if err != nil {
				slog.Error("reconcile expired leases", "error", err)
			} else if count > 0 {
				slog.Warn("requeued expired leases", "count", count)
			}
			businessCount, businessErr := store.ReconcileResearchBusinessState(ctx, cfg.ResearchHardLimit)
			if businessErr != nil {
				slog.Error("reconcile research state", "error", businessErr)
			} else if businessCount > 0 {
				slog.Warn("reconciled research state", "count", businessCount)
			}
		}
	}
}
