-- Phase 2 P2-03: immutable rolling walk-forward manifests and sealed holdouts.
CREATE TABLE IF NOT EXISTS evaluation_holdout_reservations (
    id varchar(64) PRIMARY KEY,
    contract_version varchar(80) NOT NULL,
    label_definition_version varchar(80) NOT NULL,
    asset_class varchar(24) NOT NULL,
    market varchar(24) NOT NULL,
    objective varchar(48) NOT NULL,
    horizon_sessions integer NOT NULL,
    signal_start timestamptz NOT NULL,
    signal_end timestamptz NOT NULL,
    label_cutoff timestamptz NOT NULL,
    usage_policy varchar(80) NOT NULL,
    approved_by varchar(120) NOT NULL,
    request_digest varchar(64) NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL,
    CONSTRAINT evaluation_holdout_scope_check CHECK (asset_class<>'' AND market<>'' AND objective IN ('absolute_up','excess_up') AND horizon_sessions IN (1,5,20)),
    CONSTRAINT evaluation_holdout_time_check CHECK (created_at<signal_start AND signal_start<signal_end AND signal_end<label_cutoff),
    CONSTRAINT evaluation_holdout_usage_check CHECK (usage_policy='final_evaluation_only_not_model_selection')
);
CREATE INDEX IF NOT EXISTS ix_evaluation_holdout_scope
    ON evaluation_holdout_reservations(asset_class,market,objective,horizon_sessions,signal_start);

CREATE TABLE IF NOT EXISTS evaluation_dataset_versions (
    id varchar(64) PRIMARY KEY,
    contract_version varchar(80) NOT NULL,
    label_definition_version varchar(80) NOT NULL,
    holdout_reservation_id varchar(64) NOT NULL REFERENCES evaluation_holdout_reservations(id) ON DELETE RESTRICT,
    asset_class varchar(24) NOT NULL,
    market varchar(24) NOT NULL,
    objective varchar(48) NOT NULL,
    horizon_sessions integer NOT NULL,
    config jsonb NOT NULL,
    manifest jsonb NOT NULL,
    manifest_digest varchar(64) NOT NULL UNIQUE,
    candidate_count integer NOT NULL,
    fold_count integer NOT NULL,
    development_assignment_count integer NOT NULL,
    final_holdout_included_count integer NOT NULL,
    final_holdout_excluded_count integer NOT NULL,
    exclusion_count jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_by varchar(120) NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT evaluation_dataset_scope_check CHECK (asset_class<>'' AND market<>'' AND objective IN ('absolute_up','excess_up') AND horizon_sessions IN (1,5,20)),
    CONSTRAINT evaluation_dataset_count_check CHECK (candidate_count>0 AND fold_count>0 AND development_assignment_count>0 AND final_holdout_included_count>0 AND final_holdout_excluded_count>=0)
);
CREATE INDEX IF NOT EXISTS ix_evaluation_dataset_scope
    ON evaluation_dataset_versions(asset_class,market,objective,horizon_sessions,created_at DESC);

CREATE TABLE IF NOT EXISTS evaluation_dataset_folds (
    dataset_id varchar(64) NOT NULL REFERENCES evaluation_dataset_versions(id) ON DELETE RESTRICT,
    fold_index integer NOT NULL,
    train_start timestamptz NOT NULL,
    train_cutoff timestamptz NOT NULL,
    calibration_start timestamptz NOT NULL,
    calibration_cutoff timestamptz NOT NULL,
    test_start timestamptz NOT NULL,
    test_cutoff timestamptz NOT NULL,
    train_count integer NOT NULL,
    calibration_count integer NOT NULL,
    test_count integer NOT NULL,
    excluded_count integer NOT NULL,
    manifest_digest varchar(64) NOT NULL,
    PRIMARY KEY(dataset_id,fold_index),
    CONSTRAINT evaluation_fold_time_check CHECK (train_start<train_cutoff AND train_cutoff<=calibration_start AND calibration_start<calibration_cutoff AND calibration_cutoff<=test_start AND test_start<test_cutoff),
    CONSTRAINT evaluation_fold_count_check CHECK (train_count>0 AND calibration_count>0 AND test_count>0 AND excluded_count>=0)
);

CREATE TABLE IF NOT EXISTS evaluation_dataset_samples (
    dataset_id varchar(64) NOT NULL REFERENCES evaluation_dataset_versions(id) ON DELETE RESTRICT,
    fold_index integer NOT NULL,
    prediction_run_id varchar(64) NOT NULL REFERENCES prediction_runs(id) ON DELETE RESTRICT,
    partition varchar(24) NOT NULL,
    intended_partition varchar(24) NOT NULL,
    event_cluster varchar(80) NOT NULL,
    signal_available_at timestamptz NOT NULL,
    prediction_available_at timestamptz NOT NULL,
    feature_cutoff_at timestamptz NOT NULL,
    label_window_start timestamptz NOT NULL,
    label_window_end timestamptz NOT NULL,
    label_available_at timestamptz NOT NULL,
    exclusion_reason text NOT NULL DEFAULT '',
    sealed boolean NOT NULL DEFAULT false,
    sample_digest varchar(64) NOT NULL,
    PRIMARY KEY(dataset_id,fold_index,prediction_run_id),
    CONSTRAINT evaluation_sample_partition_check CHECK (partition IN ('train','calibration','test','final_holdout','excluded')),
    CONSTRAINT evaluation_sample_intended_partition_check CHECK (intended_partition IN ('train','calibration','test','final_holdout','embargo')),
    CONSTRAINT evaluation_sample_time_check CHECK (partition='excluded' OR (feature_cutoff_at<=signal_available_at AND prediction_available_at<=label_window_start AND signal_available_at<=label_window_start AND label_window_start<=label_window_end AND label_window_end<=label_available_at)),
    CONSTRAINT evaluation_sample_seal_check CHECK ((intended_partition='final_holdout' AND sealed=true) OR (intended_partition<>'final_holdout' AND sealed=false)),
    CONSTRAINT evaluation_sample_exclusion_check CHECK ((partition='excluded' AND exclusion_reason<>'') OR (partition<>'excluded' AND exclusion_reason=''))
);
CREATE INDEX IF NOT EXISTS ix_evaluation_dataset_samples_partition
    ON evaluation_dataset_samples(dataset_id,fold_index,partition,signal_available_at);
CREATE INDEX IF NOT EXISTS ix_evaluation_dataset_samples_cluster
    ON evaluation_dataset_samples(dataset_id,event_cluster,partition);
