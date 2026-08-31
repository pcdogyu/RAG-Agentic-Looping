package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNoJob = errors.New("no job available")

type Job struct {
	ID                uuid.UUID       `json:"id"`
	Queue             string          `json:"queue"`
	TaskType          string          `json:"task_type"`
	Payload           json.RawMessage `json:"payload"`
	Status            string          `json:"status"`
	Priority          int16           `json:"priority"`
	Attempt           int             `json:"attempt"`
	MaxAttempts       int             `json:"max_attempts"`
	CancelRequestedAt *time.Time      `json:"cancel_requested_at,omitempty"`
}

type EnqueueParams struct {
	ID          uuid.UUID
	Queue       string
	TaskType    string
	Payload     any
	Priority    int16
	MaxAttempts int
	AvailableAt time.Time
	DedupeKey   string
	ParentJobID *uuid.UUID
	DependsOn   []uuid.UUID
}

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Enqueue(ctx context.Context, params EnqueueParams) (uuid.UUID, error) {
	if params.Queue == "" || params.TaskType == "" {
		return uuid.Nil, errors.New("queue and task type are required")
	}
	if params.MaxAttempts <= 0 {
		params.MaxAttempts = 3
	}
	if params.AvailableAt.IsZero() {
		params.AvailableAt = time.Now().UTC()
	}
	payload, err := json.Marshal(params.Payload)
	if err != nil {
		return uuid.Nil, err
	}
	id := params.ID
	if id == uuid.Nil {
		id = uuid.New()
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var dedupe any
	if params.DedupeKey != "" {
		dedupe = params.DedupeKey
	}
	var inserted uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO go_jobs
			(id, queue, task_type, payload, priority, max_attempts, available_at, dedupe_key, parent_job_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT DO NOTHING
		RETURNING id`,
		id, params.Queue, params.TaskType, payload, params.Priority, params.MaxAttempts,
		params.AvailableAt, dedupe, params.ParentJobID).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		if lookupErr := tx.QueryRow(ctx, `SELECT id FROM go_jobs WHERE id=$1`, id).Scan(&inserted); lookupErr != nil {
			if params.DedupeKey == "" {
				return uuid.Nil, lookupErr
			}
			lookupErr = tx.QueryRow(ctx, `
				SELECT id FROM go_jobs
				WHERE queue=$1 AND dedupe_key=$2 AND status IN ('queued','running','retrying')`,
				params.Queue, params.DedupeKey).Scan(&inserted)
			if lookupErr != nil {
				return uuid.Nil, lookupErr
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, err
		}
		return inserted, nil
	}
	if err != nil {
		return uuid.Nil, err
	}
	for _, dependency := range params.DependsOn {
		if _, err := tx.Exec(ctx, `INSERT INTO go_job_dependencies(job_id, depends_on_job_id) VALUES ($1,$2)`, id, dependency); err != nil {
			return uuid.Nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return inserted, nil
}

func (s *Store) Claim(ctx context.Context, workerID string, queues []string, lease time.Duration) (Job, error) {
	if len(queues) == 0 {
		return Job{}, ErrNoJob
	}
	row := s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT j.id
			FROM go_jobs j
			WHERE j.queue = ANY($1)
			  AND j.status IN ('queued','retrying')
			  AND j.available_at <= now()
			  AND j.cancel_requested_at IS NULL
			  AND NOT EXISTS (
				SELECT 1 FROM go_job_dependencies d
				JOIN go_jobs parent ON parent.id=d.depends_on_job_id
				WHERE d.job_id=j.id AND parent.status <> 'completed'
			  )
			-- Keep Kombu/Redis semantics: a smaller number is a higher priority.
			ORDER BY j.priority ASC, j.available_at, j.created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE go_jobs j
		SET status='running', lease_owner=$2, lease_until=now()+$3::interval,
			heartbeat_at=now(), attempt=j.attempt+1, updated_at=now()
		FROM candidate
		WHERE j.id=candidate.id
		RETURNING j.id,j.queue,j.task_type,j.payload,j.status,j.priority,j.attempt,
			j.max_attempts,j.cancel_requested_at`, queues, workerID, interval(lease))
	var job Job
	if err := row.Scan(&job.ID, &job.Queue, &job.TaskType, &job.Payload, &job.Status,
		&job.Priority, &job.Attempt, &job.MaxAttempts, &job.CancelRequestedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrNoJob
		}
		return Job{}, err
	}
	return job, nil
}

func (s *Store) Heartbeat(ctx context.Context, id uuid.UUID, workerID string, lease time.Duration) (bool, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE go_jobs SET heartbeat_at=now(),lease_until=now()+$3::interval,updated_at=now()
		WHERE id=$1 AND lease_owner=$2 AND status='running' AND cancel_requested_at IS NULL`,
		id, workerID, interval(lease))
	return result.RowsAffected() == 1, err
}

func (s *Store) Complete(ctx context.Context, id uuid.UUID, workerID string, result any) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	command, err := s.pool.Exec(ctx, `
		UPDATE go_jobs SET status='completed',result=$3,completed_at=now(),updated_at=now(),
			lease_owner=NULL,lease_until=NULL
		WHERE id=$1 AND lease_owner=$2 AND status='running'`, id, workerID, encoded)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return errors.New("job lease was lost before completion")
	}
	return nil
}

func (s *Store) Fail(ctx context.Context, job Job, workerID string, cause error) error {
	status := "retrying"
	var available time.Time
	var permanent interface{ Permanent() bool }
	if errors.As(cause, &permanent) && permanent.Permanent() {
		status = "failed"
		available = time.Now().UTC()
	} else if job.Attempt >= job.MaxAttempts {
		status = "failed"
		available = time.Now().UTC()
	} else {
		seconds := math.Min(math.Pow(2, float64(job.Attempt)), 300)
		available = time.Now().UTC().Add(time.Duration(seconds) * time.Second)
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE go_jobs SET status=$3,error=$4,available_at=$5,updated_at=now(),
			completed_at=CASE WHEN $3='failed' THEN now() ELSE NULL END,
			lease_owner=NULL,lease_until=NULL
		WHERE id=$1 AND lease_owner=$2 AND status='running'`,
		job.ID, workerID, status, cause.Error(), available)
	return err
}

func (s *Store) Cancel(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE go_jobs SET cancel_requested_at=now(),updated_at=now(),
			status=CASE WHEN status IN ('queued','retrying') THEN 'cancelled' ELSE status END,
			completed_at=CASE WHEN status IN ('queued','retrying') THEN now() ELSE completed_at END
		WHERE id=$1 AND status IN ('queued','retrying','running')`, id)
	return err
}

func (s *Store) CancelQueue(ctx context.Context, queue string) (int64, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE go_jobs SET cancel_requested_at=now(),updated_at=now(),
			status=CASE WHEN status IN ('queued','retrying') THEN 'cancelled' ELSE status END,
			completed_at=CASE WHEN status IN ('queued','retrying') THEN now() ELSE completed_at END
		WHERE queue=$1 AND status IN ('queued','retrying','running')`, queue)
	return result.RowsAffected(), err
}

func (s *Store) CancellationRequested(ctx context.Context, id uuid.UUID) (bool, error) {
	var cancelled bool
	err := s.pool.QueryRow(ctx, `SELECT cancel_requested_at IS NOT NULL FROM go_jobs WHERE id=$1`, id).Scan(&cancelled)
	return cancelled, err
}

func (s *Store) CompleteCancellation(ctx context.Context, id uuid.UUID, workerID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE go_jobs SET status='cancelled',completed_at=now(),updated_at=now(),lease_owner=NULL,lease_until=NULL WHERE id=$1 AND lease_owner=$2 AND status='running' AND cancel_requested_at IS NOT NULL`, id, workerID)
	return err
}

func (s *Store) ReconcileExpired(ctx context.Context) (int64, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE go_jobs
		SET status=CASE
				WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
				WHEN attempt >= max_attempts THEN 'failed'
				ELSE 'retrying'
			END,
			error=CASE WHEN cancel_requested_at IS NULL THEN 'worker lease expired' ELSE error END,
			available_at=now(),updated_at=now(),
			completed_at=CASE WHEN cancel_requested_at IS NOT NULL OR attempt >= max_attempts THEN now() ELSE NULL END,
			lease_owner=NULL,lease_until=NULL
		WHERE status='running' AND lease_until < now()`)
	return result.RowsAffected(), err
}

// ReconcileResearchBusinessState mirrors durable Go job failures into the
// user-facing research rows. A worker process can disappear before its handler
// gets a chance to persist the soft-timeout state; the scheduler closes that
// gap after the job lease is reconciled.
func (s *Store) ReconcileResearchBusinessState(ctx context.Context, hardLimit time.Duration) (int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT j.id,j.task_type,j.status,r.id,r.payload::jsonb
		FROM go_jobs j
		JOIN research_runs r ON r.payload->>'celery_task_id'=j.id::text
		WHERE j.task_type='market_loop.research_asset'
		  AND j.status IN ('retrying','failed')
		  AND r.status IN ('queued','running','verifying')
		UNION ALL
		SELECT j.id,j.task_type,j.status,r.id,r.payload::jsonb
		FROM go_jobs j
		JOIN event_research_runs r ON r.payload->>'celery_task_id'=j.id::text
		WHERE j.task_type='market_loop.research_event'
		  AND j.status IN ('retrying','failed')
		  AND r.status IN ('queued','running','verifying')`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var changed int64
	for rows.Next() {
		var jobID, taskType, jobStatus, runID string
		var body []byte
		if err := rows.Scan(&jobID, &taskType, &jobStatus, &runID, &body); err != nil {
			return changed, err
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			return changed, err
		}
		status := "queued"
		if jobStatus == "failed" {
			status = "failed"
			payload["completed_at"] = iso(time.Now())
			if taskType == researchAssetTask && !parseTime(payload["started_at"]).IsZero() && time.Since(parseTime(payload["started_at"])) >= hardLimit {
				payload["retryable_reason"] = "research_time_limit"
				payload["error"] = fmt.Sprintf("研究超时 / Research timed out: hard limit %s", hardLimit)
				appendAnalysisStep(payload, analysisStep("research_time_limit", "failed", "go-scheduler", fmt.Sprintf("Worker 租约失效后确认单标的研究超过硬时限 %s，已标记为可重试失败。 / The worker lease expired after the %s hard limit.", hardLimit, hardLimit), map[string]any{"hard_limit_seconds": int(hardLimit.Seconds()), "job_id": jobID}))
			} else {
				payload["retryable_reason"] = "model_worker_lease"
				payload["error"] = "Go research worker lease expired / Go 研究 Worker 租约失效"
			}
		}
		payload["status"], payload["updated_at"] = status, iso(time.Now())
		encoded, _ := json.Marshal(payload)
		table := "research_runs"
		if taskType == researchEventTask {
			table = "event_research_runs"
		}
		command, err := s.pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET status=$2,payload=$3,updated_at=now() WHERE id=$1 AND status IN ('queued','running','verifying')", table), runID, status, encoded) //nolint:gosec
		if err != nil {
			return changed, err
		}
		changed += command.RowsAffected()
	}
	return changed, rows.Err()
}

func (s *Store) WorkerHeartbeat(ctx context.Context, id string, queues []string, concurrency, active int) error {
	encoded, _ := json.Marshal(queues)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO go_worker_instances(id,queues,concurrency,active_jobs)
		VALUES($1,$2,$3,$4)
		ON CONFLICT(id) DO UPDATE SET queues=excluded.queues,concurrency=excluded.concurrency,
			active_jobs=excluded.active_jobs,heartbeat_at=now()`, id, encoded, concurrency, active)
	return err
}

func interval(value time.Duration) string {
	return fmt.Sprintf("%f seconds", value.Seconds())
}
