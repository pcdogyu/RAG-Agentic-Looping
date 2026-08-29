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
	id := uuid.New()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var dedupe any
	if params.DedupeKey != "" {
		dedupe = params.DedupeKey
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO go_jobs
			(id, queue, task_type, payload, priority, max_attempts, available_at, dedupe_key, parent_job_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		id, params.Queue, params.TaskType, payload, params.Priority, params.MaxAttempts,
		params.AvailableAt, dedupe, params.ParentJobID)
	if err != nil {
		if params.DedupeKey != "" {
			var existing uuid.UUID
			lookupErr := tx.QueryRow(ctx, `
				SELECT id FROM go_jobs
				WHERE queue=$1 AND dedupe_key=$2 AND status IN ('queued','running','retrying')`,
				params.Queue, params.DedupeKey).Scan(&existing)
			if lookupErr == nil {
				return existing, nil
			}
		}
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
	return id, nil
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
			ORDER BY j.priority DESC, j.available_at, j.created_at
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
	if job.Attempt >= job.MaxAttempts {
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

func (s *Store) ReconcileExpired(ctx context.Context) (int64, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE go_jobs
		SET status=CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'retrying' END,
			error='worker lease expired',available_at=now(),updated_at=now(),
			completed_at=CASE WHEN attempt >= max_attempts THEN now() ELSE NULL END,
			lease_owner=NULL,lease_until=NULL
		WHERE status='running' AND lease_until < now()`)
	return result.RowsAffected(), err
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
