-- P0 keeps historical reports immutable.  These tables hold only lineage and
-- versioned policy observations, so they can be backfilled and audited safely.
CREATE TABLE IF NOT EXISTS source_lineage (
    news_item_id varchar(36) PRIMARY KEY,
    canonical_url text NOT NULL,
    original_publisher text,
    original_document_id text,
    syndication_group text,
    parse_status varchar(30) NOT NULL,
    source_chain json NOT NULL DEFAULT '[]'::json,
    payload json NOT NULL DEFAULT '{}'::json,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_source_lineage_group ON source_lineage(syndication_group);
CREATE INDEX IF NOT EXISTS ix_source_lineage_status ON source_lineage(parse_status);

-- Conservative, idempotent 90-day backfill. Unknown origins deliberately do
-- not create independent-source credit.
INSERT INTO source_lineage(news_item_id,canonical_url,original_publisher,original_document_id,syndication_group,parse_status,source_chain,payload,created_at,updated_at)
SELECT n.id,
       COALESCE(NULLIF(n.raw_metadata::jsonb #>> '{source_lineage,canonical_url}',''), n.url),
       NULLIF(n.raw_metadata::jsonb #>> '{source_lineage,original_source}',''),
       NULL,
       NULLIF(n.raw_metadata::jsonb #>> '{source_lineage,syndication_group}',''),
       CASE WHEN COALESCE(n.raw_metadata::jsonb #>> '{source_lineage,original_source}','')='' THEN 'unknown' ELSE 'resolved' END,
       json_build_array(json_build_object('source',n.source,'url',n.url)),
       COALESCE(n.raw_metadata::jsonb #> '{source_lineage}','{}'::jsonb)::json,
       now(),now()
FROM news_items n
WHERE n.published_at >= now()-interval '90 days'
ON CONFLICT(news_item_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS policy_evaluations (
    id varchar(36) PRIMARY KEY,
    event_id varchar(36),
    asset_id varchar(160),
    policy_version varchar(80) NOT NULL,
    policy_mode varchar(16) NOT NULL,
    input_snapshot json NOT NULL,
    legacy_result json NOT NULL,
    policy_result json NOT NULL,
    comparison json NOT NULL,
    code_version varchar(120),
    prompt_version varchar(120),
    model varchar(160),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_policy_evaluations_event ON policy_evaluations(event_id,created_at DESC);
CREATE INDEX IF NOT EXISTS ix_policy_evaluations_asset ON policy_evaluations(asset_id,created_at DESC);

CREATE TABLE IF NOT EXISTS policy_release_approvals (
    policy_version varchar(80) PRIMARY KEY,
    approved_by varchar(160) NOT NULL,
    approved_at timestamptz NOT NULL,
    reviewed_valid_impacts integer NOT NULL,
    shadow_started_at timestamptz NOT NULL,
    note text NOT NULL DEFAULT ''
);
