-- P1/P2 rating, invalidation, prediction and model-governance contracts.
ALTER TABLE fundamental_rating_states ADD COLUMN IF NOT EXISTS result jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE fundamental_rating_revisions ADD COLUMN IF NOT EXISTS input_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE fundamental_rating_revisions ADD COLUMN IF NOT EXISTS result jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE fundamental_rating_revisions ADD COLUMN IF NOT EXISTS reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE fundamental_rating_revisions ADD COLUMN IF NOT EXISTS evidence_ids jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS rating_invalidation_rules (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    rating_revision_id varchar(64) NOT NULL REFERENCES fundamental_rating_revisions(id) ON DELETE RESTRICT,
    rule_type varchar(80) NOT NULL,
    operator varchar(16) NOT NULL,
    threshold jsonb NOT NULL,
    evidence_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    status varchar(24) NOT NULL DEFAULT 'active',
    active_from timestamptz NOT NULL,
    evaluated_at timestamptz,
    triggered_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rating_invalidation_active ON rating_invalidation_rules(asset_id,status,active_from);

CREATE TABLE IF NOT EXISTS prediction_models (
    version varchar(96) PRIMARY KEY,
    objective varchar(48) NOT NULL,
    market varchar(24) NOT NULL,
    horizon_sessions integer NOT NULL,
    feature_schema jsonb NOT NULL,
    model_payload jsonb NOT NULL,
    training_cutoff timestamptz NOT NULL,
    artifact_digest varchar(128) NOT NULL,
    status varchar(24) NOT NULL,
    scope jsonb NOT NULL DEFAULT '{}'::jsonb,
    approved_by varchar(120),
    approved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS probability_calibrations (
    version varchar(96) PRIMARY KEY,
    model_version varchar(96) NOT NULL REFERENCES prediction_models(version) ON DELETE RESTRICT,
    method varchar(32) NOT NULL,
    parameters jsonb NOT NULL,
    sample_from timestamptz NOT NULL,
    sample_to timestamptz NOT NULL,
    sample_count integer NOT NULL,
    scope jsonb NOT NULL,
    invalidation_conditions jsonb NOT NULL DEFAULT '[]'::jsonb,
    status varchar(24) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS prediction_runs (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    event_id varchar(36) REFERENCES news_events(id) ON DELETE SET NULL,
    signal_available_at timestamptz NOT NULL,
    horizon_sessions integer NOT NULL,
    objective varchar(48) NOT NULL,
    model_version varchar(96) NOT NULL REFERENCES prediction_models(version) ON DELETE RESTRICT,
    calibration_version varchar(96) REFERENCES probability_calibrations(version) ON DELETE RESTRICT,
    status varchar(24) NOT NULL,
    model_status varchar(24) NOT NULL,
    raw_score double precision,
    probabilities jsonb,
    feature_snapshot jsonb NOT NULL,
    exclusion_reason text NOT NULL DEFAULT '',
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outcome_records (
    prediction_run_id varchar(64) PRIMARY KEY REFERENCES prediction_runs(id) ON DELETE RESTRICT,
    entry_at timestamptz,
    exit_at timestamptz,
    label_available_at timestamptz,
    raw_return double precision,
    benchmark_return double precision,
    excess_return double precision,
    net_return double precision,
    execution_cost_bps double precision,
    status varchar(24) NOT NULL,
    exclusion_reason text NOT NULL DEFAULT '',
    data_quality jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
