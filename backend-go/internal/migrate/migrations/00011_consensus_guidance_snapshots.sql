-- P1-02 keeps analyst consensus and management guidance as different factual
-- series. Later revisions are append-only, allowing an earnings announcement
-- to be compared only with data available before it was released.
CREATE TABLE IF NOT EXISTS consensus_snapshots (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    metric varchar(48) NOT NULL,
    fiscal_period varchar(16) NOT NULL DEFAULT '',
    fiscal_period_end date NOT NULL,
    accounting_basis varchar(32) NOT NULL DEFAULT 'unknown',
    statistic varchar(24) NOT NULL DEFAULT 'mean',
    estimate_value double precision NOT NULL,
    analyst_count integer,
    currency varchar(16) NOT NULL DEFAULT '',
    unit varchar(32) NOT NULL DEFAULT 'reported',
    published_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    revision_at timestamptz,
    source_name varchar(120) NOT NULL,
    source_url text NOT NULL,
    source_document_id varchar(320) NOT NULL DEFAULT '',
    source_payload json NOT NULL,
    retrieved_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT consensus_snapshots_time_check CHECK (available_at >= published_at),
    CONSTRAINT consensus_snapshots_statistic_check CHECK (statistic IN ('mean','median','low','high','unknown')),
    CONSTRAINT consensus_snapshots_unit_check CHECK (unit IN ('reported','thousands','millions','billions')),
    CONSTRAINT consensus_snapshots_identity UNIQUE (asset_id,metric,fiscal_period_end,accounting_basis,statistic,source_name,source_document_id,published_at)
);
CREATE INDEX IF NOT EXISTS ix_consensus_snapshots_asset_available ON consensus_snapshots(asset_id,available_at DESC,fiscal_period_end DESC);
CREATE INDEX IF NOT EXISTS ix_consensus_snapshots_lookup ON consensus_snapshots(asset_id,metric,fiscal_period_end,available_at DESC);

CREATE TABLE IF NOT EXISTS management_guidance_snapshots (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    metric varchar(48) NOT NULL,
    fiscal_period varchar(16) NOT NULL DEFAULT '',
    fiscal_period_end date NOT NULL,
    accounting_basis varchar(32) NOT NULL DEFAULT 'unknown',
    low_value double precision,
    high_value double precision,
    currency varchar(16) NOT NULL DEFAULT '',
    unit varchar(32) NOT NULL DEFAULT 'reported',
    published_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    revision_at timestamptz,
    source_name varchar(120) NOT NULL,
    source_url text NOT NULL,
    source_document_id varchar(320) NOT NULL DEFAULT '',
    source_payload json NOT NULL,
    retrieved_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT management_guidance_time_check CHECK (available_at >= published_at),
    CONSTRAINT management_guidance_range_check CHECK (low_value IS NOT NULL OR high_value IS NOT NULL),
    CONSTRAINT management_guidance_order_check CHECK (low_value IS NULL OR high_value IS NULL OR low_value <= high_value),
    CONSTRAINT management_guidance_unit_check CHECK (unit IN ('reported','thousands','millions','billions')),
    CONSTRAINT management_guidance_identity UNIQUE (asset_id,metric,fiscal_period_end,accounting_basis,source_name,source_document_id,published_at)
);
CREATE INDEX IF NOT EXISTS ix_management_guidance_asset_available ON management_guidance_snapshots(asset_id,available_at DESC,fiscal_period_end DESC);
CREATE INDEX IF NOT EXISTS ix_management_guidance_lookup ON management_guidance_snapshots(asset_id,metric,fiscal_period_end,available_at DESC);
