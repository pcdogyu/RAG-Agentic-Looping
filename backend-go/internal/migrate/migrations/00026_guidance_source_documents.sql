-- P1-02 keeps official issuer disclosures separate from analyst consensus and
-- from confirmed numeric management guidance. SEC filings are candidates for
-- human review; discovering a filing never creates guidance or a rating.
CREATE TABLE IF NOT EXISTS guidance_source_documents (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    provider varchar(32) NOT NULL,
    cik varchar(10) NOT NULL,
    accession_number varchar(20) NOT NULL,
    form varchar(16) NOT NULL,
    filing_date date NOT NULL,
    report_date date,
    accepted_at timestamptz NOT NULL,
    source_available_at timestamptz NOT NULL,
    first_observed_at timestamptz NOT NULL,
    filing_index_url text NOT NULL,
    primary_document varchar(300) NOT NULL DEFAULT '',
    primary_document_url text NOT NULL DEFAULT '',
    source_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT guidance_source_documents_provider_check CHECK (provider IN ('sec_edgar')),
    CONSTRAINT guidance_source_documents_cik_check CHECK (cik ~ '^[0-9]{10}$'),
    CONSTRAINT guidance_source_documents_accession_check CHECK (accession_number ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    CONSTRAINT guidance_source_documents_time_check CHECK (source_available_at >= accepted_at AND first_observed_at >= source_available_at),
    CONSTRAINT guidance_source_documents_identity UNIQUE (asset_id,provider,accession_number)
);
CREATE INDEX IF NOT EXISTS ix_guidance_source_documents_asset_available
    ON guidance_source_documents(asset_id,source_available_at DESC,accepted_at DESC);

CREATE TABLE IF NOT EXISTS guidance_source_reviews (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    source_document_id varchar(64) NOT NULL REFERENCES guidance_source_documents(id) ON DELETE RESTRICT,
    decision varchar(32) NOT NULL,
    guidance_snapshot_id varchar(64) REFERENCES management_guidance_snapshots(id) ON DELETE RESTRICT,
    evidence_url text,
    evidence_location varchar(300),
    evidence_excerpt text,
    notes text NOT NULL DEFAULT '',
    reviewed_by varchar(200) NOT NULL,
    reviewed_at timestamptz NOT NULL,
    idempotency_key varchar(160) NOT NULL UNIQUE,
    request_hash varchar(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT guidance_source_reviews_decision_check CHECK (decision IN ('confirmed_guidance','no_guidance','needs_follow_up')),
    CONSTRAINT guidance_source_reviews_confirmation_check CHECK (
        (decision='confirmed_guidance' AND guidance_snapshot_id IS NOT NULL AND evidence_url IS NOT NULL AND evidence_location IS NOT NULL AND evidence_excerpt IS NOT NULL)
        OR (decision<>'confirmed_guidance' AND guidance_snapshot_id IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS ix_guidance_source_reviews_document_time
    ON guidance_source_reviews(source_document_id,reviewed_at DESC,id DESC);
