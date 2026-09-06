-- P1-03: facts, assumptions and deterministic projections remain separate.
CREATE TABLE IF NOT EXISTS forecast_versions (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    parent_version_id varchar(64) REFERENCES forecast_versions(id) ON DELETE RESTRICT,
    model_version varchar(80) NOT NULL,
    status varchar(24) NOT NULL,
    as_of timestamptz NOT NULL,
    input_snapshot json NOT NULL,
    assumptions json NOT NULL,
    projection json NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT forecast_versions_status_check CHECK (status IN ('available','unavailable','withdrawn'))
);
CREATE INDEX IF NOT EXISTS ix_forecast_versions_asset_as_of ON forecast_versions(asset_id,as_of DESC);

CREATE TABLE IF NOT EXISTS event_assumption_links (
    id varchar(64) PRIMARY KEY,
    forecast_version_id varchar(64) NOT NULL REFERENCES forecast_versions(id) ON DELETE CASCADE,
    event_id varchar(36) NOT NULL,
    field varchar(80) NOT NULL,
    fiscal_period_end date,
    old_value double precision,
    new_value double precision NOT NULL,
    delta_value double precision,
    evidence_ids json NOT NULL,
    condition text NOT NULL DEFAULT '',
    approval_status varchar(24) NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT event_assumption_links_approval_check CHECK (approval_status IN ('proposed','approved','rejected','withdrawn'))
);
CREATE INDEX IF NOT EXISTS ix_event_assumption_links_event ON event_assumption_links(event_id,created_at DESC);
