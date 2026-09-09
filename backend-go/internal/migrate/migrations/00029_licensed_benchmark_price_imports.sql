-- Licensed total-return index history is imported only through an audited,
-- idempotent administrator workflow. The receipt binds an external approval
-- and licence reference to the exact canonical request without storing any
-- credentials or provider access tokens.
CREATE TABLE IF NOT EXISTS licensed_benchmark_price_import_receipts (
    id varchar(80) PRIMARY KEY,
    idempotency_key varchar(200) NOT NULL UNIQUE,
    request_hash varchar(64) NOT NULL,
    asset_id varchar(160) NOT NULL REFERENCES assets(id),
    vendor_code varchar(80) NOT NULL,
    source_name varchar(120) NOT NULL,
    source_document_id varchar(320) NOT NULL,
    source_url text NOT NULL,
    license_reference varchar(320) NOT NULL,
    approved_by varchar(160) NOT NULL,
    observation_ids json NOT NULL DEFAULT '[]',
    observation_count integer NOT NULL,
    inserted_count integer NOT NULL,
    available_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT licensed_benchmark_price_import_counts_check CHECK (
        observation_count > 0 AND inserted_count >= 0 AND inserted_count <= observation_count
    )
);

CREATE INDEX IF NOT EXISTS ix_licensed_benchmark_price_import_asset_time
    ON licensed_benchmark_price_import_receipts(asset_id,available_at DESC);
