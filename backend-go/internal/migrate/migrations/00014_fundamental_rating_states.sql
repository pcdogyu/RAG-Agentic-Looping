-- P1-07: fundamental ratings are distinct from event-signal states and retain
-- every revision. The current row is keyed by the complete policy scope.
CREATE TABLE IF NOT EXISTS fundamental_rating_states (
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    policy_version varchar(80) NOT NULL,
    horizon_days integer NOT NULL,
    benchmark_id varchar(160) NOT NULL DEFAULT '',
    rating varchar(24) NOT NULL,
    valuation_run_id varchar(64) REFERENCES valuation_runs(id) ON DELETE RESTRICT,
    effective_at timestamptz NOT NULL,
    revision_id varchar(64) NOT NULL,
    PRIMARY KEY(asset_id,policy_version,horizon_days,benchmark_id)
);
CREATE TABLE IF NOT EXISTS fundamental_rating_revisions (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    policy_version varchar(80) NOT NULL,
    horizon_days integer NOT NULL,
    benchmark_id varchar(160) NOT NULL DEFAULT '',
    previous_rating varchar(24),
    current_rating varchar(24),
    action varchar(32) NOT NULL,
    reason text NOT NULL DEFAULT '',
    valuation_run_id varchar(64) REFERENCES valuation_runs(id) ON DELETE RESTRICT,
    effective_at timestamptz NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fundamental_rating_revisions_asset_time ON fundamental_rating_revisions(asset_id,effective_at DESC);
