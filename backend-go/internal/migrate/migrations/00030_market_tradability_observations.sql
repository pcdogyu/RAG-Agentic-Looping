-- Phase 2 P2-02: immutable point-in-time evidence for close execution.
-- A price row never implies that an order could be filled. Only an explicit,
-- attributable tradability observation can unlock execution simulation.
CREATE TABLE IF NOT EXISTS market_tradability_import_receipts (
    id varchar(72) PRIMARY KEY,
    idempotency_key varchar(200) NOT NULL UNIQUE,
    request_hash varchar(64) NOT NULL,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    source_name varchar(120) NOT NULL,
    source_document_id varchar(320) NOT NULL,
    source_url text NOT NULL,
    license_reference varchar(320) NOT NULL,
    approved_by varchar(160) NOT NULL,
    observation_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    observation_count integer NOT NULL DEFAULT 0,
    inserted_count integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT market_tradability_receipt_counts_check CHECK (
        observation_count >= 0 AND inserted_count >= 0 AND inserted_count <= observation_count
    )
);

CREATE INDEX IF NOT EXISTS ix_market_tradability_receipt_asset
    ON market_tradability_import_receipts(asset_id,available_at DESC);

CREATE TABLE IF NOT EXISTS market_tradability_observations (
    id varchar(72) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    market varchar(32) NOT NULL,
    currency varchar(16) NOT NULL,
    session_date date NOT NULL,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    status varchar(24) NOT NULL,
    execution_point varchar(32) NOT NULL DEFAULT 'market_close',
    buy_executable boolean NOT NULL,
    sell_executable boolean NOT NULL,
    time_precision varchar(24) NOT NULL,
    source_name varchar(120) NOT NULL,
    source_document_id varchar(320) NOT NULL,
    source_url text NOT NULL DEFAULT '',
    import_receipt_id varchar(72) NOT NULL REFERENCES market_tradability_import_receipts(id) ON DELETE RESTRICT,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT market_tradability_status_check CHECK (status IN ('tradable','suspended','limit_up','limit_down','delisted')),
    CONSTRAINT market_tradability_execution_point_check CHECK (execution_point='market_close'),
    CONSTRAINT market_tradability_precision_check CHECK (time_precision IN ('date_only','timestamped')),
    CONSTRAINT market_tradability_time_check CHECK (available_at >= observed_at),
    CONSTRAINT market_tradability_execution_check CHECK (
        (status='tradable' AND buy_executable AND sell_executable)
        OR (status<>'tradable' AND NOT buy_executable AND NOT sell_executable)
    ),
    CONSTRAINT market_tradability_identity UNIQUE (
        asset_id,source_name,source_document_id,session_date,observed_at,status
    )
);

CREATE INDEX IF NOT EXISTS ix_market_tradability_asset_session
    ON market_tradability_observations(asset_id,session_date DESC,available_at DESC);
CREATE INDEX IF NOT EXISTS ix_market_tradability_status
    ON market_tradability_observations(market,status,session_date DESC);
