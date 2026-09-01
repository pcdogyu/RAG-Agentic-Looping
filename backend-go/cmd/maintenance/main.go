package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/platform"
)

func main() {
	task := flag.String("task", "", "compact-research-backlog, reprocess-target-impacts-v2, or seed-assets")
	dryRun := flag.Bool("dry-run", true, "preview backlog compaction without changing research runs")
	batchSize := flag.Int("batch-size", 25, "target-impact replay batch size")
	maxActive := flag.Int("max-active", 50, "target-impact replay active-run ceiling")
	flag.Parse()

	taskTypes := map[string]string{
		"compact-research-backlog":    jobs.CompactResearchBacklogTask,
		"reprocess-target-impacts-v2": jobs.ReprocessTargetImpactsTask,
		"seed-assets":                 jobs.SeedAssetsTask,
	}
	taskType := taskTypes[strings.TrimSpace(*task)]
	if taskType == "" {
		fmt.Fprintln(os.Stderr, "-task must be compact-research-backlog, reprocess-target-impacts-v2, or seed-assets")
		os.Exit(2)
	}
	if *batchSize < 1 || *maxActive < 1 {
		fmt.Fprintln(os.Stderr, "-batch-size and -max-active must be positive")
		os.Exit(2)
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	if _, err := jobs.RequireWorkerLane("maintenance"); err != nil {
		slog.Error("maintenance lane", "error", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
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
	kwargs := map[string]any{}
	if taskType == jobs.CompactResearchBacklogTask {
		kwargs["dry_run"] = *dryRun
	}
	if taskType == jobs.ReprocessTargetImpactsTask {
		kwargs["batch_size"], kwargs["max_active"] = *batchSize, *maxActive
	}
	id, err := jobs.NewStore(dependencies.DB).Enqueue(ctx, jobs.EnqueueParams{
		Queue: "maintenance", TaskType: taskType,
		Payload:  map[string]any{"args": []any{}, "kwargs": kwargs},
		Priority: 5, MaxAttempts: 3, DedupeKey: "maintenance:" + taskType,
	})
	if err != nil {
		slog.Error("enqueue maintenance task", "error", err)
		os.Exit(1)
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"task_id": id, "task": taskType, "status": "queued"})
}
