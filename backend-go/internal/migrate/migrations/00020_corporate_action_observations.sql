-- Phase 2 M1.2: immutable, point-in-time corporate-action observations.
-- Provider revisions are retained; historical readers select only revisions
-- that were available by their requested cutoff.
CREATE TABLE IF NOT EXISTS corporate_action_observations (
    id varchar(72) PRIMARY KEY,
    event_key varchar(72) NOT NULL,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    market varchar(32) NOT NULL,
    currency varchar(16) NOT NULL,
    action_type varchar(32) NOT NULL,
    effective_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    announcement_at timestamptz,
    record_at timestamptz,
    payment_at timestamptz,
    end_at timestamptz,
    ratio_numerator double precision,
    ratio_denominator double precision,
    cash_amount double precision,
    adjusted_cash_amount double precision,
    old_symbol varchar(80) NOT NULL DEFAULT '',
    new_symbol varchar(80) NOT NULL DEFAULT '',
    time_precision varchar(24) NOT NULL,
    source_name varchar(120) NOT NULL,
    source_document_id varchar(320) NOT NULL,
    source_url text NOT NULL DEFAULT '',
    source_payload json NOT NULL DEFAULT '{}',
    metadata json NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT corporate_action_type_check CHECK (action_type IN ('cash_dividend','split','reverse_split','symbol_change','suspension','delisting')),
    CONSTRAINT corporate_action_precision_check CHECK (time_precision IN ('date_only','timestamped')),
    CONSTRAINT corporate_action_end_check CHECK (end_at IS NULL OR end_at >= effective_at),
    CONSTRAINT corporate_action_terms_check CHECK (
        (action_type='cash_dividend' AND cash_amount>0 AND (adjusted_cash_amount IS NULL OR adjusted_cash_amount>0) AND ratio_numerator IS NULL AND ratio_denominator IS NULL AND old_symbol='' AND new_symbol='') OR
        (action_type='split' AND ratio_numerator>ratio_denominator AND ratio_denominator>0 AND cash_amount IS NULL AND adjusted_cash_amount IS NULL AND old_symbol='' AND new_symbol='') OR
        (action_type='reverse_split' AND ratio_denominator>ratio_numerator AND ratio_numerator>0 AND cash_amount IS NULL AND adjusted_cash_amount IS NULL AND old_symbol='' AND new_symbol='') OR
        (action_type='symbol_change' AND old_symbol<>'' AND new_symbol<>'' AND old_symbol<>new_symbol AND ratio_numerator IS NULL AND ratio_denominator IS NULL AND cash_amount IS NULL AND adjusted_cash_amount IS NULL) OR
        action_type IN ('suspension','delisting')
    )
);

CREATE INDEX IF NOT EXISTS ix_corporate_action_asset_effective
    ON corporate_action_observations(asset_id,effective_at DESC,available_at DESC);
CREATE INDEX IF NOT EXISTS ix_corporate_action_event_revision
    ON corporate_action_observations(source_name,event_key,available_at DESC);
CREATE INDEX IF NOT EXISTS ix_corporate_action_market_type
    ON corporate_action_observations(market,action_type,effective_at DESC);
