-- Phase 2 M5: one immutable evaluation per pre-registered final holdout.
CREATE TABLE IF NOT EXISTS evaluation_final_holdout_reports (
    id varchar(64) PRIMARY KEY,
    contract_version varchar(80) NOT NULL,
    dataset_id varchar(64) NOT NULL REFERENCES evaluation_dataset_versions(id) ON DELETE RESTRICT,
    holdout_reservation_id varchar(64) NOT NULL UNIQUE REFERENCES evaluation_holdout_reservations(id) ON DELETE RESTRICT,
    experiment_id varchar(64) NOT NULL REFERENCES evaluation_experiments(id) ON DELETE RESTRICT,
    development_report_id varchar(64) NOT NULL REFERENCES evaluation_performance_reports(id) ON DELETE RESTRICT,
    asset_class varchar(24) NOT NULL,
    market varchar(24) NOT NULL,
    objective varchar(48) NOT NULL,
    horizon_sessions integer NOT NULL,
    fold_index integer NOT NULL,
    variant_name varchar(80) NOT NULL,
    variant_kind varchar(80) NOT NULL,
    variant_artifact_digest varchar(64) NOT NULL,
    request_digest varchar(64) NOT NULL,
    report jsonb NOT NULL,
    artifact_digest varchar(64) NOT NULL UNIQUE,
    final_holdout_accessed boolean NOT NULL,
    sample_ids_shown boolean NOT NULL DEFAULT false,
    automatic_model_selection boolean NOT NULL DEFAULT false,
    approved_by varchar(120) NOT NULL,
    approval_reason text NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL,
    CONSTRAINT evaluation_final_holdout_scope_check CHECK (asset_class='equity' AND market<>'' AND objective IN ('absolute_up','excess_up') AND horizon_sessions IN (1,5,20)),
    CONSTRAINT evaluation_final_holdout_variant_check CHECK (fold_index>=0 AND variant_name<>'' AND variant_kind<>'' AND variant_artifact_digest<>''),
    CONSTRAINT evaluation_final_holdout_governance_check CHECK (final_holdout_accessed=true AND sample_ids_shown=false AND automatic_model_selection=false AND approved_by<>'' AND approval_reason<>'')
);
CREATE INDEX IF NOT EXISTS ix_evaluation_final_holdout_scope
    ON evaluation_final_holdout_reports(asset_class,market,objective,horizon_sessions,created_at DESC);
