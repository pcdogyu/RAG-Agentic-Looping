-- Phase 2 M1.3: administrators approve immutable research-plan revisions.
-- Runtime status is mutable, but the financial assumptions and valuation
-- scenarios of an approved revision are never rewritten in place.
CREATE TABLE IF NOT EXISTS fundamental_research_plans (
    id varchar(64) PRIMARY KEY,
    asset_id varchar(160) NOT NULL REFERENCES assets(id) ON DELETE RESTRICT,
    forecast_version_id varchar(64) NOT NULL REFERENCES forecast_versions(id) ON DELETE RESTRICT,
    valuation_plan json NOT NULL,
    rating_plan json NOT NULL,
    cadence_hours integer NOT NULL,
    max_price_age_hours integer NOT NULL,
    max_plan_age_days integer NOT NULL,
    status varchar(32) NOT NULL,
    idempotency_key varchar(240) NOT NULL UNIQUE,
    approved_by varchar(160) NOT NULL,
    approved_at timestamptz NOT NULL,
    next_run_at timestamptz NOT NULL,
    last_run_at timestamptz,
    last_run_status varchar(32) NOT NULL DEFAULT '',
    last_run_reason text NOT NULL DEFAULT '',
    last_result json NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fundamental_research_plans_cadence_check CHECK (cadence_hours BETWEEN 1 AND 720),
    CONSTRAINT fundamental_research_plans_price_age_check CHECK (max_price_age_hours BETWEEN 1 AND 336),
    CONSTRAINT fundamental_research_plans_plan_age_check CHECK (max_plan_age_days BETWEEN 1 AND 365),
    CONSTRAINT fundamental_research_plans_status_check CHECK (status IN ('approved','paused','review_required')),
    CONSTRAINT fundamental_research_plans_last_status_check CHECK (last_run_status IN ('','completed','insufficient_data','not_applicable','review_required','data_refresh_failed','technical_failure'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_fundamental_research_plans_active_asset
    ON fundamental_research_plans(asset_id) WHERE status='approved';
CREATE INDEX IF NOT EXISTS ix_fundamental_research_plans_due
    ON fundamental_research_plans(next_run_at,asset_id) WHERE status='approved';
CREATE INDEX IF NOT EXISTS ix_fundamental_research_plans_asset_history
    ON fundamental_research_plans(asset_id,approved_at DESC);
