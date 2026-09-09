-- Phase 2 M2: frozen 1/5/20-session label and execution-research contract.
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS label_definition_version varchar(80) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS objective varchar(48) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS horizon_sessions integer NOT NULL DEFAULT 0;
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS entry_price double precision;
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS exit_price double precision;
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS price_field varchar(32) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS time_precision varchar(24) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS benchmark_asset_id varchar(160) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS benchmark_mapping_id varchar(72) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS alpha_definition varchar(160) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS absolute_label varchar(24) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS relative_label varchar(24) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS objective_label varchar(24) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS risk_adjusted_residual double precision;
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS risk_adjustment_status varchar(48) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS gross_strategy_return double precision;
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS simulation_status varchar(48) NOT NULL DEFAULT '';
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS research_result_only boolean NOT NULL DEFAULT true;
ALTER TABLE outcome_records ADD COLUMN IF NOT EXISTS execution_assumptions jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS ix_outcome_records_maturity
    ON outcome_records(status,label_available_at,horizon_sessions,objective);
