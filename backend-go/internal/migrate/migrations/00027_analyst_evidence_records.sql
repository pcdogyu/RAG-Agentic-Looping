-- Phase 2 M1.3: immutable, attributable analyst evidence used by manual and
-- scheduled fundamental research. A non-empty arbitrary string is not proof;
-- governed valuation, benchmark and rating inputs must resolve to a record
-- that was available and approved before the research cutoff.
CREATE TABLE IF NOT EXISTS analyst_evidence_records (
    id varchar(72) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    evidence_type varchar(40) NOT NULL,
    title varchar(240) NOT NULL,
    rationale text NOT NULL,
    values jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    source_name varchar(120) NOT NULL,
    source_document_id varchar(320) NOT NULL,
    source_url text NOT NULL,
    approved_by varchar(160) NOT NULL,
    approved_at timestamptz NOT NULL,
    idempotency_key varchar(200) NOT NULL UNIQUE,
    request_hash varchar(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT analyst_evidence_type_check CHECK (evidence_type IN ('forecast_assumption','valuation_multiple','cost_of_capital','benchmark_expectation','rating_rationale','invalidation_rule')),
    CONSTRAINT analyst_evidence_time_check CHECK (available_at >= observed_at AND approved_at >= available_at),
    CONSTRAINT analyst_evidence_values_object_check CHECK (jsonb_typeof(values)='object')
);

CREATE INDEX IF NOT EXISTS ix_analyst_evidence_asset_available
    ON analyst_evidence_records(asset_id,available_at DESC,approved_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS ix_analyst_evidence_type_available
    ON analyst_evidence_records(evidence_type,available_at DESC,id DESC);

-- Plans approved before this evidence contract only prove that reference
-- strings were non-empty. Preserve their immutable bodies but stop automatic
-- execution until an analyst registers evidence and approves a new revision.
ALTER TABLE fundamental_research_plans
    ADD COLUMN IF NOT EXISTS evidence_contract_version varchar(64);

UPDATE fundamental_research_plans
SET status='review_required',
    last_run_status='review_required',
    last_run_reason='analyst_evidence_registration_required',
    updated_at=now()
WHERE status='approved'
  AND COALESCE(evidence_contract_version,'')='';
