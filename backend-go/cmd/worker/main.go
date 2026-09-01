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
	lane, err := jobs.ValidateBatchFourActivation(cfg.WorkerLane, cfg.WorkerCompletedLanes)
	if err != nil {
		slog.Error("batch 4 activation", "error", err)
		os.Exit(1)
	}
	handlerManifest := map[string]jobs.Handler{}
	if lane.ID == "extract" {
		handlerManifest = jobs.NewExtractHandlers(cfg, nil, nil)
	} else if lane.ID == "mapping" {
		handlerManifest = jobs.NewMappingHandlers(cfg, nil, nil)
	} else if lane.ID == "research" {
		handlerManifest = jobs.NewResearchHandlers(cfg, nil, nil)
	} else if lane.ID == "evolution" {
		handlerManifest = jobs.NewEvolutionHandlers(cfg, nil, nil)
	} else if lane.ID == "discovery" {
		handlerManifest = jobs.NewDiscoveryHandlers(cfg, nil, nil)
	} else if lane.ID == "recovery" {
		handlerManifest = jobs.NewRecoveryHandlers(cfg, nil, nil)
	} else if lane.ID == "outcomes" {
		handlerManifest = jobs.NewOutcomeHandlers(cfg, nil, nil)
	} else if lane.ID == "masterdata" {
		handlerManifest = jobs.NewMasterdataHandlers(cfg, nil, nil)
	}
	if err := jobs.ValidateLaneHandlers(lane, handlerManifest); err != nil {
		slog.Error("batch 4 handler gate", "error", err)
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
	handlers := handlerManifest
	if lane.ID == "extract" {
		handlers = jobs.NewExtractHandlers(cfg, dependencies.DB, dependencies.Redis)
	} else if lane.ID == "mapping" {
		handlers = jobs.NewMappingHandlers(cfg, dependencies.DB, dependencies.Redis)
	} else if lane.ID == "research" {
		handlers = jobs.NewResearchHandlers(cfg, dependencies.DB, dependencies.Redis)
	} else if lane.ID == "evolution" {
		handlers = jobs.NewEvolutionHandlers(cfg, dependencies.DB, dependencies.Redis)
	} else if lane.ID == "discovery" {
		handlers = jobs.NewDiscoveryHandlers(cfg, dependencies.DB, dependencies.Redis)
	} else if lane.ID == "recovery" {
		handlers = jobs.NewRecoveryHandlers(cfg, dependencies.DB, dependencies.Redis)
	} else if lane.ID == "outcomes" {
		handlers = jobs.NewOutcomeHandlers(cfg, dependencies.DB, dependencies.Redis)
	} else if lane.ID == "masterdata" {
		handlers = jobs.NewMasterdataHandlers(cfg, dependencies.DB, dependencies.Redis)
	}
	worker := &jobs.Worker{Store: jobs.NewStore(dependencies.DB), ID: cfg.WorkerID, Queues: []string{lane.GoQueue},
		Lease: cfg.LeaseDuration, PollInterval: cfg.PollInterval, Handlers: handlers, Concurrency: cfg.WorkerConcurrency}
	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
