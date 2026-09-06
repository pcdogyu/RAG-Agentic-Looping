-- P1-01: immutable, point-in-time financial-statement snapshots.  A row is a
-- provider observation, rather than a derived score, so later restatements can
-- coexist with the original disclosure and historical research can use the
-- information that was actually available at its cutoff.
CREATE TABLE IF NOT EXISTS fundamental_snapshots (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    statement_type varchar(32) NOT NULL,
    fiscal_period varchar(16) NOT NULL DEFAULT '',
    report_period_end date NOT NULL,
    published_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    revision_at timestamptz,
    currency varchar(16) NOT NULL DEFAULT '',
    unit varchar(32) NOT NULL DEFAULT 'reported',
    accounting_standard varchar(32) NOT NULL DEFAULT 'unknown',
    source_name varchar(120) NOT NULL,
    source_url text NOT NULL,
    source_document_id varchar(320) NOT NULL DEFAULT '',
    metrics json NOT NULL,
    source_payload json NOT NULL,
    retrieved_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fundamental_snapshots_statement_type_check CHECK (statement_type IN ('income_statement','balance_sheet','cash_flow')),
    CONSTRAINT fundamental_snapshots_time_check CHECK (available_at >= published_at),
    CONSTRAINT fundamental_snapshots_unit_check CHECK (unit IN ('reported','thousands','millions','billions')),
    CONSTRAINT fundamental_snapshots_identity UNIQUE (asset_id,statement_type,report_period_end,source_name,source_document_id,published_at)
);

CREATE INDEX IF NOT EXISTS ix_fundamental_snapshots_asset_available
    ON fundamental_snapshots(asset_id,available_at DESC,report_period_end DESC);
CREATE INDEX IF NOT EXISTS ix_fundamental_snapshots_asset_statement_period
    ON fundamental_snapshots(asset_id,statement_type,report_period_end DESC,published_at DESC);
CREATE INDEX IF NOT EXISTS ix_fundamental_snapshots_source_document
    ON fundamental_snapshots(source_name,source_document_id);
