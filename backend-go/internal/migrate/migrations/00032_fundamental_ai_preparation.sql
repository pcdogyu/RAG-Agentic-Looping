-- Phase 2: durable local-search/AI preparation runs. Model output is stored as
-- a candidate first and can only become governed evidence through a named,
-- versioned policy.
CREATE TABLE IF NOT EXISTS fundamental_ai_batches (
    id uuid PRIMARY KEY,
    market varchar(24) NOT NULL,
    scope varchar(48) NOT NULL,
    requested_count integer NOT NULL,
    task_ids jsonb NOT NULL DEFAULT '[]',
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fundamental_ai_batch_count_check CHECK (requested_count BETWEEN 0 AND 200)
);

CREATE TABLE IF NOT EXISTS fundamental_ai_runs (
    id uuid PRIMARY KEY,
    batch_id uuid REFERENCES fundamental_ai_batches(id) ON DELETE SET NULL,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    task_id uuid NOT NULL,
    status varchar(32) NOT NULL,
    stage varchar(48) NOT NULL,
    policy_version varchar(80) NOT NULL,
    model_version varchar(120) NOT NULL,
    prompt_version varchar(80) NOT NULL,
    as_of timestamptz NOT NULL,
    summary jsonb NOT NULL DEFAULT '{}',
    blockers jsonb NOT NULL DEFAULT '[]',
    idempotency_key varchar(240) NOT NULL UNIQUE,
    started_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fundamental_ai_run_status_check CHECK (status IN ('queued','running','completed','insufficient_data','failed','cancelled'))
);
CREATE INDEX IF NOT EXISTS ix_fundamental_ai_runs_asset
    ON fundamental_ai_runs(asset_id,created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS ix_fundamental_ai_runs_batch
    ON fundamental_ai_runs(batch_id,created_at,id);

CREATE TABLE IF NOT EXISTS fundamental_ai_source_snapshots (
    id varchar(72) PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES fundamental_ai_runs(id) ON DELETE CASCADE,
    query text NOT NULL,
    title text NOT NULL,
    source_name varchar(160) NOT NULL,
    source_class varchar(32) NOT NULL,
    source_url text NOT NULL,
    source_domain varchar(255) NOT NULL,
    published_at timestamptz,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    content_type varchar(160) NOT NULL,
    content_hash varchar(64) NOT NULL,
    content_text text NOT NULL,
    retrieval_status varchar(32) NOT NULL,
    retrieval_detail text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fundamental_ai_source_time_check CHECK (available_at>=observed_at),
    CONSTRAINT fundamental_ai_source_status_check CHECK (retrieval_status IN ('available','unsupported','failed')),
    UNIQUE(run_id,source_url)
);
CREATE INDEX IF NOT EXISTS ix_fundamental_ai_sources_run
    ON fundamental_ai_source_snapshots(run_id,available_at,id);

CREATE TABLE IF NOT EXISTS fundamental_ai_candidates (
    id varchar(72) PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES fundamental_ai_runs(id) ON DELETE CASCADE,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    evidence_type varchar(40) NOT NULL,
    title varchar(240) NOT NULL,
    rationale text NOT NULL,
    values jsonb NOT NULL,
    source_snapshot_ids jsonb NOT NULL,
    evidence_quote text NOT NULL,
    evidence_location varchar(320) NOT NULL,
    status varchar(32) NOT NULL,
    validation jsonb NOT NULL,
    approved_evidence_id varchar(72) REFERENCES analyst_evidence_records(id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fundamental_ai_candidate_status_check CHECK (status IN ('proposed','policy_approved','insufficient_data','rejected')),
    CONSTRAINT fundamental_ai_candidate_values_check CHECK (jsonb_typeof(values)='object'),
    CONSTRAINT fundamental_ai_candidate_sources_check CHECK (jsonb_typeof(source_snapshot_ids)='array')
);
CREATE INDEX IF NOT EXISTS ix_fundamental_ai_candidates_run
    ON fundamental_ai_candidates(run_id,status,evidence_type,id);

ALTER TABLE analyst_evidence_records
    ADD COLUMN IF NOT EXISTS approval_kind varchar(16) NOT NULL DEFAULT 'human',
    ADD COLUMN IF NOT EXISTS policy_version varchar(80) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS provenance jsonb NOT NULL DEFAULT '{}';
ALTER TABLE analyst_evidence_records DROP CONSTRAINT IF EXISTS analyst_evidence_approval_kind_check;
ALTER TABLE analyst_evidence_records ADD CONSTRAINT analyst_evidence_approval_kind_check
    CHECK (approval_kind IN ('human','policy'));

ALTER TABLE fundamental_research_plans
    ADD COLUMN IF NOT EXISTS approval_kind varchar(16) NOT NULL DEFAULT 'human',
    ADD COLUMN IF NOT EXISTS policy_version varchar(80) NOT NULL DEFAULT '';
ALTER TABLE fundamental_research_plans DROP CONSTRAINT IF EXISTS fundamental_research_plan_approval_kind_check;
ALTER TABLE fundamental_research_plans ADD CONSTRAINT fundamental_research_plan_approval_kind_check
    CHECK (approval_kind IN ('human','policy'));

ALTER TABLE evaluation_holdout_reservations
    ADD COLUMN IF NOT EXISTS approval_kind varchar(16) NOT NULL DEFAULT 'human',
    ADD COLUMN IF NOT EXISTS policy_version varchar(80) NOT NULL DEFAULT '';
ALTER TABLE evaluation_holdout_reservations DROP CONSTRAINT IF EXISTS evaluation_holdout_approval_kind_check;
ALTER TABLE evaluation_holdout_reservations ADD CONSTRAINT evaluation_holdout_approval_kind_check
    CHECK (approval_kind IN ('human','policy'));
