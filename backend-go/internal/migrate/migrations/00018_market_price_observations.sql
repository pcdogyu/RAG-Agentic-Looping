-- Phase 2 M1.1: immutable provider observations used by outcome labels,
-- benchmark returns and later point-in-time fundamental-rating refreshes.
CREATE TABLE IF NOT EXISTS market_price_observations (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL,
    market varchar(32) NOT NULL,
    currency varchar(16) NOT NULL,
    session_date date NOT NULL,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    price double precision NOT NULL,
    price_field varchar(32) NOT NULL,
    time_precision varchar(24) NOT NULL,
    source_name varchar(120) NOT NULL,
    source_document_id varchar(320) NOT NULL,
    source_url text NOT NULL DEFAULT '',
    metadata json NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT market_price_observations_price_check CHECK (price > 0),
    CONSTRAINT market_price_observations_field_check CHECK (price_field IN ('close','adjusted_close')),
    CONSTRAINT market_price_observations_precision_check CHECK (time_precision IN ('daily_close','timestamped')),
    CONSTRAINT market_price_observations_time_check CHECK (available_at >= observed_at),
    CONSTRAINT market_price_observations_identity UNIQUE (asset_id,source_name,source_document_id,observed_at,price_field,price)
);

CREATE INDEX IF NOT EXISTS ix_market_price_observations_asset_time
    ON market_price_observations(asset_id,observed_at DESC,available_at DESC);
CREATE INDEX IF NOT EXISTS ix_market_price_observations_market_session
    ON market_price_observations(market,session_date DESC);
