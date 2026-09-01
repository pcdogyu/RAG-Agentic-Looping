ALTER TABLE go_jobs
    ADD COLUMN IF NOT EXISTS started_at timestamptz,
    ADD COLUMN IF NOT EXISTS attempt_started_at timestamptz,
    ADD COLUMN IF NOT EXISTS execution_duration_ms bigint NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS ix_go_jobs_recent_execution
    ON go_jobs(queue, completed_at DESC)
    WHERE status IN ('completed', 'failed');
