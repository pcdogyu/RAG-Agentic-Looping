-- P3 cross-asset isolation: keep the asset class on every prediction run so
-- market-level model or calibration scopes cannot mix equities, ETFs,
-- commodities and crypto instruments that happen to share a market code.
ALTER TABLE prediction_runs ADD COLUMN IF NOT EXISTS asset_class varchar(32) NOT NULL DEFAULT 'equity';
CREATE INDEX IF NOT EXISTS ix_prediction_runs_segment ON prediction_runs(asset_class,signal_available_at DESC);
