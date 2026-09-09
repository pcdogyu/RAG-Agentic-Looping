-- Phase 2 M1.2: immutable point-in-time benchmark policy and security-universe history.
-- Nothing is seeded here: a benchmark becomes usable only after an explicit,
-- attributable approval, and failed universe refreshes remain visible.
CREATE TABLE IF NOT EXISTS benchmark_mapping_observations (
    id varchar(72) PRIMARY KEY,
    idempotency_key varchar(160) NOT NULL UNIQUE,
    request_hash varchar(64) NOT NULL,
    scope_type varchar(24) NOT NULL,
    scope_id varchar(160) NOT NULL,
    subject_market varchar(32) NOT NULL,
    subject_currency varchar(16) NOT NULL,
    benchmark_asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    benchmark_market varchar(32) NOT NULL,
    benchmark_currency varchar(16) NOT NULL,
    policy_version varchar(80) NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    source_name varchar(120) NOT NULL,
    source_document_id varchar(320) NOT NULL,
    source_url text NOT NULL DEFAULT '',
    mapping_reason text NOT NULL,
    approved_by varchar(160) NOT NULL,
    metadata json NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT benchmark_mapping_scope_check CHECK (scope_type IN ('asset','industry','market','policy')),
    CONSTRAINT benchmark_mapping_validity_check CHECK (valid_to IS NULL OR valid_to > valid_from),
    CONSTRAINT benchmark_mapping_availability_check CHECK (available_at >= observed_at),
    CONSTRAINT benchmark_mapping_identity_check CHECK (benchmark_market <> '' AND benchmark_currency <> ''),
    CONSTRAINT benchmark_mapping_currency_check CHECK (subject_currency = benchmark_currency)
);

CREATE INDEX IF NOT EXISTS ix_benchmark_mapping_resolution
    ON benchmark_mapping_observations(scope_type,scope_id,valid_from DESC,available_at DESC);
CREATE INDEX IF NOT EXISTS ix_benchmark_mapping_benchmark
    ON benchmark_mapping_observations(benchmark_asset_id,valid_from DESC);

CREATE TABLE IF NOT EXISTS security_universe_snapshots (
    id varchar(72) PRIMARY KEY,
    universe_id varchar(160) NOT NULL,
    market varchar(32) NOT NULL,
    status varchar(24) NOT NULL,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    source_name varchar(120) NOT NULL,
    source_document_id varchar(320) NOT NULL,
    source_url text NOT NULL DEFAULT '',
    eligibility_policy json NOT NULL DEFAULT '{}',
    asset_count integer NOT NULL DEFAULT 0,
    included_count integer NOT NULL DEFAULT 0,
    excluded_count integer NOT NULL DEFAULT 0,
    delisted_count integer NOT NULL DEFAULT 0,
    failure_detail text NOT NULL DEFAULT '',
    metadata json NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT security_universe_snapshot_status_check CHECK (status IN ('completed','failed','rejected')),
    CONSTRAINT security_universe_snapshot_availability_check CHECK (available_at >= observed_at),
    CONSTRAINT security_universe_snapshot_counts_check CHECK (asset_count >= 0 AND included_count >= 0 AND excluded_count >= 0 AND delisted_count >= 0),
    CONSTRAINT security_universe_snapshot_failure_check CHECK ((status='completed' AND failure_detail='') OR (status<>'completed' AND failure_detail<>''))
);

CREATE INDEX IF NOT EXISTS ix_security_universe_snapshot_history
    ON security_universe_snapshots(universe_id,available_at DESC,observed_at DESC);
CREATE INDEX IF NOT EXISTS ix_security_universe_snapshot_market_status
    ON security_universe_snapshots(market,status,available_at DESC);

CREATE TABLE IF NOT EXISTS security_universe_memberships (
    snapshot_id varchar(72) NOT NULL REFERENCES security_universe_snapshots(id) ON DELETE RESTRICT,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    membership_status varchar(24) NOT NULL,
    effective_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    reason_codes json NOT NULL DEFAULT '[]',
    source_identity json NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(snapshot_id,asset_id),
    CONSTRAINT security_universe_membership_status_check CHECK (membership_status IN ('included','excluded','delisted')),
    CONSTRAINT security_universe_membership_availability_check CHECK (available_at >= effective_at)
);

CREATE INDEX IF NOT EXISTS ix_security_universe_membership_asset
    ON security_universe_memberships(asset_id,available_at DESC,effective_at DESC);

-- Outcomes created before the PIT mapping contract used an implicit market
-- switch. Retain those derived values for audit, but never expose them as an
-- approved relative return. This predicate is idempotent on every startup.
UPDATE outcomes
SET payload = (
    payload::jsonb || jsonb_build_object(
        'legacy_benchmark_audit', jsonb_build_object(
            'benchmark_return', payload::jsonb->'benchmark_return',
            'alpha', payload::jsonb->'alpha',
            'benchmark_status', payload::jsonb->'benchmark_status',
            'quarantine_reason', 'pre_pit_mapping_contract'
        ),
        'benchmark_return', NULL,
        'alpha', NULL,
        'benchmark_status', 'unavailable',
        'benchmark_reason', 'legacy_unapproved_mapping',
        'benchmark_mapping_id', '',
        'benchmark_asset_id', ''
    )
)::json
WHERE coalesce(payload::jsonb->>'benchmark_mapping_id','')=''
  AND coalesce(payload::jsonb->>'benchmark_reason','')<>'legacy_unapproved_mapping'
  AND (payload::jsonb->>'benchmark_return' IS NOT NULL OR payload::jsonb->>'alpha' IS NOT NULL);
