-- Phase 2 P2-04/P2-05: development-only baseline, ablation and calibration experiments.
CREATE TABLE IF NOT EXISTS evaluation_experiments (
    id varchar(64) PRIMARY KEY,
    contract_version varchar(80) NOT NULL,
    dataset_id varchar(64) NOT NULL REFERENCES evaluation_dataset_versions(id) ON DELETE RESTRICT,
    dataset_manifest_digest varchar(64) NOT NULL,
    asset_class varchar(24) NOT NULL,
    market varchar(24) NOT NULL,
    objective varchar(48) NOT NULL,
    horizon_sessions integer NOT NULL,
    feature_groups jsonb NOT NULL,
    preprocessing_policy varchar(96) NOT NULL,
    report jsonb NOT NULL,
    artifact_digest varchar(64) NOT NULL UNIQUE,
    fold_count integer NOT NULL,
    final_holdout_accessed boolean NOT NULL DEFAULT false,
    created_by varchar(120) NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL,
    CONSTRAINT evaluation_experiment_scope_check CHECK (asset_class='equity' AND market<>'' AND objective IN ('absolute_up','excess_up') AND horizon_sessions IN (1,5,20)),
    CONSTRAINT evaluation_experiment_fold_check CHECK (fold_count>0),
    CONSTRAINT evaluation_experiment_holdout_check CHECK (final_holdout_accessed=false)
);
CREATE INDEX IF NOT EXISTS ix_evaluation_experiment_dataset
    ON evaluation_experiments(dataset_id,created_at DESC);

CREATE TABLE IF NOT EXISTS evaluation_experiment_variants (
    experiment_id varchar(64) NOT NULL REFERENCES evaluation_experiments(id) ON DELETE RESTRICT,
    fold_index integer NOT NULL,
    variant_name varchar(80) NOT NULL,
    variant_kind varchar(80) NOT NULL,
    status varchar(48) NOT NULL,
    reason text NOT NULL DEFAULT '',
    feature_names jsonb NOT NULL,
    training_count integer NOT NULL,
    calibration_count integer NOT NULL,
    test_count integer NOT NULL,
    model_artifact jsonb,
    calibration_artifact jsonb,
    metrics jsonb NOT NULL,
    artifact_digest varchar(64) NOT NULL,
    PRIMARY KEY(experiment_id,fold_index,variant_name),
    CONSTRAINT evaluation_experiment_variant_count_check CHECK (fold_index>=0 AND training_count>=0 AND calibration_count>=0 AND test_count>=0),
    CONSTRAINT evaluation_experiment_variant_status_check CHECK (status IN ('evaluated','evaluated_uncalibrated','unavailable'))
);

CREATE TABLE IF NOT EXISTS evaluation_experiment_predictions (
    experiment_id varchar(64) NOT NULL,
    fold_index integer NOT NULL,
    variant_name varchar(80) NOT NULL,
    prediction_run_id varchar(64) NOT NULL REFERENCES prediction_runs(id) ON DELETE RESTRICT,
    event_cluster varchar(80) NOT NULL,
    signal_available_at timestamptz NOT NULL,
    raw_score double precision NOT NULL,
    probability double precision,
    objective_label boolean NOT NULL,
    correct boolean NOT NULL,
    raw_return double precision,
    excess_return double precision,
    PRIMARY KEY(experiment_id,fold_index,variant_name,prediction_run_id),
    FOREIGN KEY(experiment_id,fold_index,variant_name) REFERENCES evaluation_experiment_variants(experiment_id,fold_index,variant_name) ON DELETE RESTRICT,
    CONSTRAINT evaluation_experiment_probability_check CHECK (probability IS NULL OR (probability>0 AND probability<1))
);
CREATE INDEX IF NOT EXISTS ix_evaluation_experiment_prediction_sample
    ON evaluation_experiment_predictions(prediction_run_id,experiment_id,fold_index);
