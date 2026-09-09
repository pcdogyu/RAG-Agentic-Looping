-- Phase 2 P2-06/P2-07: immutable layered reports and non-mutating failure drills.
CREATE TABLE IF NOT EXISTS research_quality_reviews (
    id varchar(64) PRIMARY KEY,
    research_run_id varchar(36) NOT NULL REFERENCES research_runs(id) ON DELETE RESTRICT,
    fact_correct boolean,
    relationship_correct boolean,
    citation_supported boolean,
    refusal_appropriate boolean,
    reviewer varchar(120) NOT NULL,
    note text NOT NULL DEFAULT '',
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL,
    CONSTRAINT research_quality_review_label_check CHECK (fact_correct IS NOT NULL OR relationship_correct IS NOT NULL OR citation_supported IS NOT NULL OR refusal_appropriate IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS ix_research_quality_review_run_time
    ON research_quality_reviews(research_run_id,created_at DESC);

CREATE TABLE IF NOT EXISTS evaluation_performance_reports (
    id varchar(64) PRIMARY KEY,
    contract_version varchar(80) NOT NULL,
    dataset_id varchar(64) NOT NULL REFERENCES evaluation_dataset_versions(id) ON DELETE RESTRICT,
    experiment_id varchar(64) NOT NULL REFERENCES evaluation_experiments(id) ON DELETE RESTRICT,
    asset_class varchar(24) NOT NULL,
    market varchar(24) NOT NULL,
    objective varchar(48) NOT NULL,
    horizon_sessions integer NOT NULL,
    report jsonb NOT NULL,
    artifact_digest varchar(64) NOT NULL UNIQUE,
    final_holdout_accessed boolean NOT NULL DEFAULT false,
    created_by varchar(120) NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL,
    CONSTRAINT evaluation_performance_report_scope_check CHECK (asset_class='equity' AND market<>'' AND objective IN ('absolute_up','excess_up') AND horizon_sessions IN (1,5,20)),
    CONSTRAINT evaluation_performance_report_holdout_check CHECK (final_holdout_accessed=false)
);
CREATE INDEX IF NOT EXISTS ix_evaluation_performance_report_scope
    ON evaluation_performance_reports(asset_class,market,objective,horizon_sessions,created_at DESC);

CREATE TABLE IF NOT EXISTS model_failure_drills (
    id varchar(64) PRIMARY KEY,
    contract_version varchar(80) NOT NULL,
    scenario varchar(64) NOT NULL,
    expected_status varchar(32) NOT NULL,
    observed_status varchar(32) NOT NULL,
    expected_action varchar(48) NOT NULL,
    observed_action varchar(48) NOT NULL,
    passed boolean NOT NULL,
    production_state_changed boolean NOT NULL DEFAULT false,
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_by varchar(120) NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL,
    CONSTRAINT model_failure_drill_scenario_check CHECK (scenario IN ('data_source_unavailable','model_timeout','feature_distribution_drift','calibration_expired','manual_rollback_gate')),
    CONSTRAINT model_failure_drill_non_mutating_check CHECK (production_state_changed=false)
);
CREATE INDEX IF NOT EXISTS ix_model_failure_drill_scenario_time
    ON model_failure_drills(scenario,created_at DESC);
