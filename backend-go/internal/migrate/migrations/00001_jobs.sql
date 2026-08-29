CREATE TABLE IF NOT EXISTS go_jobs (
    id uuid PRIMARY KEY,
    queue varchar(40) NOT NULL,
    task_type varchar(120) NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status varchar(24) NOT NULL DEFAULT 'queued',
    priority smallint NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 9),
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_owner varchar(160),
    lease_until timestamptz,
    heartbeat_at timestamptz,
    cancel_requested_at timestamptz,
    dedupe_key varchar(240),
    parent_job_id uuid REFERENCES go_jobs(id) ON DELETE SET NULL,
    error text,
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_go_jobs_active_dedupe
    ON go_jobs(queue, dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'running', 'retrying');
CREATE INDEX IF NOT EXISTS ix_go_jobs_claim
    ON go_jobs(queue, status, priority DESC, available_at, created_at)
    WHERE status IN ('queued', 'retrying');
CREATE INDEX IF NOT EXISTS ix_go_jobs_lease
    ON go_jobs(status, lease_until)
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS go_job_dependencies (
    job_id uuid NOT NULL REFERENCES go_jobs(id) ON DELETE CASCADE,
    depends_on_job_id uuid NOT NULL REFERENCES go_jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, depends_on_job_id),
    CHECK (job_id <> depends_on_job_id)
);

CREATE TABLE IF NOT EXISTS go_worker_instances (
    id varchar(160) PRIMARY KEY,
    queues jsonb NOT NULL DEFAULT '[]'::jsonb,
    concurrency integer NOT NULL DEFAULT 1,
    active_jobs integer NOT NULL DEFAULT 0,
    started_at timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS go_workflow_checkpoints (
    workflow_id uuid NOT NULL,
    sequence bigint NOT NULL,
    workflow_type varchar(80) NOT NULL,
    state jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, sequence)
);

CREATE INDEX IF NOT EXISTS ix_recommendations_feed
    ON recommendations(as_of DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_event_research_runs_feed
    ON event_research_runs(status, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_research_runs_failed_feed
    ON research_runs(status, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_news_items_source_feed
    ON news_items(source, published_at DESC, id DESC);
