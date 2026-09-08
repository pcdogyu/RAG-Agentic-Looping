-- Durable P2 model shadow comparisons, drift monitoring, and release audit.
CREATE TABLE IF NOT EXISTS shadow_prediction_comparisons (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    signal_available_at timestamptz NOT NULL,
    incumbent_model_version varchar(96) NOT NULL REFERENCES prediction_models(version) ON DELETE RESTRICT,
    candidate_model_version varchar(96) NOT NULL REFERENCES prediction_models(version) ON DELETE RESTRICT,
    incumbent_run_id varchar(64) NOT NULL REFERENCES prediction_runs(id) ON DELETE RESTRICT,
    candidate_run_id varchar(64) NOT NULL REFERENCES prediction_runs(id) ON DELETE RESTRICT,
    execution_assumptions jsonb NOT NULL,
    status varchar(24) NOT NULL,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(asset_id,signal_available_at,incumbent_model_version,candidate_model_version)
);
CREATE INDEX IF NOT EXISTS ix_shadow_prediction_candidate_time
    ON shadow_prediction_comparisons(candidate_model_version,signal_available_at DESC);

CREATE TABLE IF NOT EXISTS model_governance_checks (
    id varchar(64) PRIMARY KEY,
    subject_type varchar(24) NOT NULL,
    subject_version varchar(96) NOT NULL,
    reference_version varchar(96),
    check_type varchar(32) NOT NULL,
    status varchar(24) NOT NULL,
    action varchar(32) NOT NULL,
    window_start timestamptz,
    window_end timestamptz,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    approved_by varchar(120),
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_model_governance_subject_time
    ON model_governance_checks(subject_type,subject_version,created_at DESC);
CREATE INDEX IF NOT EXISTS ix_model_governance_review
    ON model_governance_checks(status,created_at DESC)
    WHERE status IN ('blocked','review_required','drifted');
