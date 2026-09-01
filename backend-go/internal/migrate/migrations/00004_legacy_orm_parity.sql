-- Own the last schema elements that were previously created or upgraded by
-- the removed SQLAlchemy runtime. Every statement is safe to run repeatedly.

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

-- Older databases predate the asset identity/taxonomy columns. The Python
-- startup hook added these lazily; Go migrations now own that upgrade path.
ALTER TABLE assets ADD COLUMN IF NOT EXISTS issuer_id varchar(240);
ALTER TABLE assets ADD COLUMN IF NOT EXISTS primary_listing_asset_id varchar(160);
ALTER TABLE assets ADD COLUMN IF NOT EXISTS sector_id varchar(120) DEFAULT '';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS industry_id varchar(160) DEFAULT '';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS raw_sector varchar(160) DEFAULT '';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS raw_industry varchar(200) DEFAULT '';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS instrument_type varchar(40) DEFAULT '';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS market_cap double precision;
ALTER TABLE assets ADD COLUMN IF NOT EXISTS market_cap_rank integer;
ALTER TABLE assets ADD COLUMN IF NOT EXISTS association_tier varchar(30) DEFAULT 'standard';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS association_reason varchar(160) DEFAULT 'provider_verified';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS provider_association_tier varchar(30) DEFAULT 'standard';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS provider_association_reason varchar(160) DEFAULT 'provider_verified';
ALTER TABLE assets ADD COLUMN IF NOT EXISTS last_synced_at timestamptz;
ALTER TABLE assets ADD COLUMN IF NOT EXISTS manual_industry_id varchar(160);
ALTER TABLE assets ADD COLUMN IF NOT EXISTS manual_active boolean;
ALTER TABLE assets ADD COLUMN IF NOT EXISTS manual_association_tier varchar(30);

CREATE INDEX IF NOT EXISTS ix_assets_sector_id ON assets(sector_id);
CREATE INDEX IF NOT EXISTS ix_assets_industry_id ON assets(industry_id);
CREATE INDEX IF NOT EXISTS ix_assets_instrument_type ON assets(instrument_type);
CREATE INDEX IF NOT EXISTS ix_assets_market_cap ON assets(market_cap);
CREATE INDEX IF NOT EXISTS ix_assets_market_cap_rank ON assets(market_cap_rank);
CREATE INDEX IF NOT EXISTS ix_assets_association_tier ON assets(association_tier);
CREATE INDEX IF NOT EXISTS ix_assets_last_synced_at ON assets(last_synced_at);
CREATE INDEX IF NOT EXISTS ix_assets_manual_industry_id ON assets(manual_industry_id);
CREATE INDEX IF NOT EXISTS ix_assets_manual_association_tier ON assets(manual_association_tier);

CREATE TABLE IF NOT EXISTS document_chunks (
    id varchar(36) PRIMARY KEY,
    evidence_id varchar(36) NOT NULL,
    run_id varchar(36) NOT NULL,
    asset_id varchar(160) NOT NULL,
    text text NOT NULL,
    terms json NOT NULL,
    embedding public.vector(384) NOT NULL,
    source_url text NOT NULL,
    source_quality varchar(30) NOT NULL,
    published_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    as_of timestamptz NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ix_document_chunks_evidence_id ON document_chunks(evidence_id);
CREATE INDEX IF NOT EXISTS ix_document_chunks_run_id ON document_chunks(run_id);
CREATE INDEX IF NOT EXISTS ix_document_chunks_asset_id ON document_chunks(asset_id);
CREATE INDEX IF NOT EXISTS ix_document_chunks_source_quality ON document_chunks(source_quality);
CREATE INDEX IF NOT EXISTS ix_document_chunks_published_at ON document_chunks(published_at);
CREATE INDEX IF NOT EXISTS ix_document_chunks_observed_at ON document_chunks(observed_at);
CREATE INDEX IF NOT EXISTS ix_document_chunks_as_of ON document_chunks(as_of);

-- Preserve the last relationship projection observed in deployed databases.
-- It is no longer used by the Go runtime, but remains migration-owned so an
-- upgraded database never depends on removed startup DDL.
CREATE TABLE IF NOT EXISTS asset_relationships (
    source_asset_id varchar(160) NOT NULL,
    target_asset_id varchar(160) NOT NULL,
    relationship_type varchar(40) NOT NULL,
    confidence double precision NOT NULL,
    provenance varchar(120) NOT NULL,
    details json NOT NULL,
    active boolean NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (source_asset_id, target_asset_id, relationship_type)
);

CREATE INDEX IF NOT EXISTS ix_asset_relationships_source_asset_id ON asset_relationships(source_asset_id);
CREATE INDEX IF NOT EXISTS ix_asset_relationships_target_asset_id ON asset_relationships(target_asset_id);
CREATE INDEX IF NOT EXISTS ix_asset_relationships_relationship_type ON asset_relationships(relationship_type);
CREATE INDEX IF NOT EXISTS ix_asset_relationships_active ON asset_relationships(active);

-- These tables were created on first use by langgraph-checkpoint-postgres.
-- Go does not write them, but SQL migrations own their compatibility shape so
-- legacy checkpoint data remains readable and restorable without Python DDL.
CREATE TABLE IF NOT EXISTS checkpoint_migrations (
    v integer PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id text NOT NULL,
    checkpoint_ns text NOT NULL DEFAULT '',
    checkpoint_id text NOT NULL,
    parent_checkpoint_id text,
    type text,
    checkpoint jsonb NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);

CREATE INDEX IF NOT EXISTS checkpoints_thread_id_idx ON checkpoints(thread_id);

CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id text NOT NULL,
    checkpoint_ns text NOT NULL DEFAULT '',
    channel text NOT NULL,
    version text NOT NULL,
    type text NOT NULL,
    blob bytea,
    PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
);

CREATE INDEX IF NOT EXISTS checkpoint_blobs_thread_id_idx ON checkpoint_blobs(thread_id);

CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id text NOT NULL,
    checkpoint_ns text NOT NULL DEFAULT '',
    checkpoint_id text NOT NULL,
    task_id text NOT NULL,
    idx integer NOT NULL,
    channel text NOT NULL,
    type text,
    blob bytea NOT NULL,
    task_path text NOT NULL DEFAULT '',
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);

CREATE INDEX IF NOT EXISTS checkpoint_writes_thread_id_idx ON checkpoint_writes(thread_id);
