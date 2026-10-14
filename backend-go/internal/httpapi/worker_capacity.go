package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

// workerHeartbeatFreshness bounds how long a durable worker may miss its 10s
// heartbeat while still counting towards advertised queue capacity.
const workerHeartbeatFreshness = 45 * time.Second

// workerCapacityRow is one durable worker heartbeat as stored in
// go_worker_instances by jobs.Store.WorkerHeartbeat.
type workerCapacityRow struct {
	Queues      []string
	Concurrency int
}

// liveWorkerCapacities aggregates the concurrency that running workers
// advertise per native queue. A worker registered for several queues counts
// towards each of them, and several workers on the same queue add up.
func liveWorkerCapacities(rows []workerCapacityRow) map[string]int {
	capacities := map[string]int{}
	for _, row := range rows {
		if row.Concurrency < 1 {
			continue
		}
		for _, queue := range row.Queues {
			if queue = strings.TrimSpace(queue); queue != "" {
				capacities[queue] += row.Concurrency
			}
		}
	}
	return capacities
}

// liveWorkerCapacities reads the advertised concurrency of every worker with a
// fresh heartbeat. A missing table, a query failure, or a full worker restart
// window all return nil so callers fall back to their configured ceiling.
func (s *Server) liveWorkerCapacities(ctx context.Context) map[string]int {
	rows, err := s.db.Query(ctx, `SELECT queues,concurrency FROM go_worker_instances WHERE heartbeat_at >= $1`,
		time.Now().UTC().Add(-workerHeartbeatFreshness))
	if err != nil {
		slog.Error("worker capacity query failed", "error", err)
		return nil
	}
	defer rows.Close()
	decoded := []workerCapacityRow{}
	for rows.Next() {
		var encoded []byte
		var concurrency int
		if err := rows.Scan(&encoded, &concurrency); err != nil {
			slog.Error("worker capacity scan failed", "error", err)
			return nil
		}
		var queues []string
		if err := json.Unmarshal(encoded, &queues); err != nil {
			slog.Warn("worker capacity queues ignored", "error", err)
			continue
		}
		decoded = append(decoded, workerCapacityRow{Queues: queues, Concurrency: concurrency})
	}
	if err := rows.Err(); err != nil {
		slog.Error("worker capacity iteration failed", "error", err)
		return nil
	}
	return liveWorkerCapacities(decoded)
}

// resolveLaneCapacity prefers the concurrency advertised by live workers and
// falls back to the configured per-instance ceiling when no worker reported it.
func resolveLaneCapacity(observed map[string]int, lane string, advertised int) int {
	if capacity, ok := observed[lane]; ok && capacity > 0 {
		return capacity
	}
	if advertised > 0 {
		return advertised
	}
	return 1
}
