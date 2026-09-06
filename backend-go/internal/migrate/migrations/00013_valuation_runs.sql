-- P1-04: immutable source-linked valuation scenarios. A valuation range is a
-- model/scenario range, never a calibrated probability or confidence interval.
CREATE TABLE IF NOT EXISTS valuation_runs (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    forecast_version_id varchar(64) NOT NULL REFERENCES forecast_versions(id) ON DELETE RESTRICT,
    model_version varchar(80) NOT NULL,
    status varchar(24) NOT NULL,
    as_of timestamptz NOT NULL,
    input_snapshot json NOT NULL,
    scenarios json NOT NULL,
    sensitivity json NOT NULL,
    result json NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT valuation_runs_status_check CHECK (status IN ('available','unavailable','withdrawn'))
);
CREATE INDEX IF NOT EXISTS ix_valuation_runs_asset_as_of ON valuation_runs(asset_id,as_of DESC);
CREATE INDEX IF NOT EXISTS ix_valuation_runs_forecast ON valuation_runs(forecast_version_id,created_at DESC);
