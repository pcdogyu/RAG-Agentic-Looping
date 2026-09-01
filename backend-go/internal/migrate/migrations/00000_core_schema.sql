CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS asset_universe_sync (
    market varchar(20) PRIMARY KEY,
    status varchar(30) NOT NULL,
    asset_count integer NOT NULL,
    industry_count integer NOT NULL,
    added_count integer NOT NULL,
    updated_count integer NOT NULL,
    deactivated_count integer NOT NULL,
    last_error text,
    started_at timestamptz,
    completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS assets (
    id varchar(160) PRIMARY KEY,
    asset_class varchar(20) NOT NULL,
    market varchar(20) NOT NULL,
    symbol varchar(40) NOT NULL,
    name varchar(200) NOT NULL,
    exchange_or_provider varchar(80) NOT NULL,
    currency varchar(10) NOT NULL,
    aliases json NOT NULL,
    products json NOT NULL,
    competitors json NOT NULL,
    lot_size integer NOT NULL,
    active boolean NOT NULL,
    issuer_id varchar(240),
    primary_listing_asset_id varchar(160),
    sector_id varchar(120) DEFAULT '',
    industry_id varchar(160) DEFAULT '',
    raw_sector varchar(160) DEFAULT '',
    raw_industry varchar(200) DEFAULT '',
    instrument_type varchar(40) DEFAULT '',
    market_cap double precision,
    market_cap_rank integer,
    last_synced_at timestamptz,
    manual_industry_id varchar(160),
    manual_active boolean,
    association_tier varchar(30) DEFAULT 'standard',
    association_reason varchar(160) DEFAULT 'provider_verified',
    provider_association_tier varchar(30) DEFAULT 'standard',
    provider_association_reason varchar(160) DEFAULT 'provider_verified',
    manual_association_tier varchar(30)
);

CREATE TABLE IF NOT EXISTS event_research_runs (
    id varchar(36) PRIMARY KEY,
    event_id varchar(36) NOT NULL,
    status varchar(40) NOT NULL,
    payload json NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS evidence (
    id varchar(36) PRIMARY KEY,
    run_id varchar(36) NOT NULL,
    claim text NOT NULL,
    source_url text NOT NULL,
    source_quality varchar(30) NOT NULL,
    published_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    as_of timestamptz NOT NULL,
    payload json NOT NULL
);

CREATE TABLE IF NOT EXISTS evolution_candidates (
    id varchar(36) PRIMARY KEY,
    branch varchar(200) NOT NULL UNIQUE,
    status varchar(30) NOT NULL,
    payload json NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS industries (
    id varchar(160) PRIMARY KEY,
    parent_id varchar(120),
    level integer NOT NULL,
    name_zh varchar(120) NOT NULL,
    name_en varchar(160) NOT NULL,
    aliases json NOT NULL,
    active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS integration_settings (
    key varchar(80) PRIMARY KEY,
    payload json NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS mcp_sources (
    id varchar(36) PRIMARY KEY,
    name varchar(120) NOT NULL,
    url text NOT NULL,
    description text NOT NULL,
    priority integer NOT NULL,
    enabled boolean NOT NULL,
    managed boolean NOT NULL,
    auth_type varchar(30) NOT NULL,
    auth_header_name varchar(120),
    encrypted_secret text,
    discovered_tools json NOT NULL,
    tool_mappings json NOT NULL,
    last_status varchar(30) NOT NULL,
    last_error text,
    last_checked_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS model_call_audits (
    id varchar(36) PRIMARY KEY,
    logical_call_id varchar(36) NOT NULL,
    source_key varchar(500) UNIQUE,
    provider varchar(40) NOT NULL,
    model varchar(160) NOT NULL,
    operation varchar(80) NOT NULL,
    entity_type varchar(50),
    entity_id varchar(160),
    attempt integer NOT NULL,
    status varchar(30) NOT NULL,
    fidelity varchar(30) NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    duration_ms integer,
    prompt_tokens integer,
    completion_tokens integer,
    input_language varchar(20) NOT NULL,
    output_language varchar(20) NOT NULL,
    messages json NOT NULL,
    schema_payload json NOT NULL,
    raw_response text NOT NULL,
    parsed_response json,
    error text,
    metrics json NOT NULL
);

CREATE TABLE IF NOT EXISTS news_events (
    id varchar(36) PRIMARY KEY,
    headline text NOT NULL,
    event_type varchar(40) NOT NULL,
    payload json NOT NULL,
    priority double precision NOT NULL,
    published_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    as_of timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS news_filter_logs (
    id varchar(36) PRIMARY KEY,
    content_hash varchar(64) NOT NULL,
    source varchar(120) NOT NULL,
    title text NOT NULL,
    url text NOT NULL,
    matched_keyword varchar(80) NOT NULL,
    published_at timestamptz NOT NULL,
    first_filtered_at timestamptz NOT NULL,
    last_filtered_at timestamptz NOT NULL,
    hit_count integer NOT NULL
);

CREATE TABLE IF NOT EXISTS news_items (
    id varchar(36) PRIMARY KEY,
    source varchar(120) NOT NULL,
    source_quality varchar(30) NOT NULL,
    title text NOT NULL,
    summary text NOT NULL,
    url text NOT NULL,
    language varchar(10) NOT NULL,
    published_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    as_of timestamptz NOT NULL,
    content_hash varchar(64) NOT NULL,
    symbols json NOT NULL,
    raw_metadata json NOT NULL
);

CREATE TABLE IF NOT EXISTS news_processing (
    news_id varchar(36) PRIMARY KEY,
    status varchar(40) NOT NULL,
    scan_task_id varchar(160),
    celery_task_id varchar(160),
    attempt_count integer NOT NULL,
    last_error text,
    queued_at timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    heartbeat_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS news_processing_outbox (
    id varchar(36) PRIMARY KEY,
    news_id varchar(36) NOT NULL,
    status varchar(40) NOT NULL,
    force_asset_mapping boolean NOT NULL,
    dispatch_attempts integer NOT NULL,
    available_at timestamptz NOT NULL,
    dispatched_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS news_source_states (
    source varchar(120) PRIMARY KEY,
    provider varchar(80) NOT NULL,
    status varchar(30) NOT NULL,
    watermark_at timestamptz,
    last_attempt_at timestamptz,
    last_success_at timestamptz,
    last_error text,
    last_discovered_count integer NOT NULL,
    last_new_count integer NOT NULL,
    consecutive_failures integer NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS outcomes (
    id varchar(36) PRIMARY KEY,
    recommendation_id varchar(36) NOT NULL,
    horizon_days integer NOT NULL,
    observed_at timestamptz NOT NULL,
    payload json NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_orders (
    id varchar(36) PRIMARY KEY,
    recommendation_id varchar(36) NOT NULL,
    asset_id varchar(160) NOT NULL,
    side varchar(10) NOT NULL,
    quantity double precision NOT NULL,
    price double precision NOT NULL,
    currency varchar(10) NOT NULL,
    fee double precision NOT NULL,
    executed_at timestamptz NOT NULL,
    payload json NOT NULL
);

CREATE TABLE IF NOT EXISTS recommendations (
    id varchar(36) PRIMARY KEY,
    run_id varchar(36) NOT NULL,
    asset_id varchar(160) NOT NULL,
    score integer NOT NULL,
    rating varchar(40) NOT NULL,
    confidence double precision NOT NULL,
    as_of timestamptz NOT NULL,
    payload json NOT NULL
);

CREATE TABLE IF NOT EXISTS research_runs (
    id varchar(36) PRIMARY KEY,
    event_id varchar(36),
    asset_id varchar(160) NOT NULL,
    status varchar(40) NOT NULL,
    payload json NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_asset_universe_sync_status ON asset_universe_sync(status);
CREATE INDEX IF NOT EXISTS ix_assets_asset_class ON assets(asset_class);
CREATE INDEX IF NOT EXISTS ix_assets_market ON assets(market);
CREATE INDEX IF NOT EXISTS ix_assets_name ON assets(name);
CREATE INDEX IF NOT EXISTS ix_assets_symbol ON assets(symbol);
CREATE UNIQUE INDEX IF NOT EXISTS ix_event_research_runs_event_id ON event_research_runs(event_id);
CREATE INDEX IF NOT EXISTS ix_event_research_runs_feed ON event_research_runs(status, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_event_research_runs_status ON event_research_runs(status);
CREATE INDEX IF NOT EXISTS ix_evidence_as_of ON evidence(as_of);
CREATE INDEX IF NOT EXISTS ix_evidence_observed_at ON evidence(observed_at);
CREATE INDEX IF NOT EXISTS ix_evidence_published_at ON evidence(published_at);
CREATE INDEX IF NOT EXISTS ix_evidence_run_id ON evidence(run_id);
CREATE INDEX IF NOT EXISTS ix_evidence_source_quality ON evidence(source_quality);
CREATE INDEX IF NOT EXISTS ix_evolution_candidates_status ON evolution_candidates(status);
CREATE INDEX IF NOT EXISTS ix_industries_level ON industries(level);
CREATE INDEX IF NOT EXISTS ix_industries_name_en ON industries(name_en);
CREATE INDEX IF NOT EXISTS ix_industries_name_zh ON industries(name_zh);
CREATE INDEX IF NOT EXISTS ix_industries_parent_id ON industries(parent_id);
CREATE INDEX IF NOT EXISTS ix_mcp_sources_enabled ON mcp_sources(enabled);
CREATE UNIQUE INDEX IF NOT EXISTS ix_mcp_sources_name ON mcp_sources(name);
CREATE INDEX IF NOT EXISTS ix_mcp_sources_priority ON mcp_sources(priority);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_completed_at ON model_call_audits(completed_at);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_entity_id ON model_call_audits(entity_id);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_entity_type ON model_call_audits(entity_type);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_fidelity ON model_call_audits(fidelity);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_input_language ON model_call_audits(input_language);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_logical_call_id ON model_call_audits(logical_call_id);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_model ON model_call_audits(model);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_operation ON model_call_audits(operation);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_output_language ON model_call_audits(output_language);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_provider ON model_call_audits(provider);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_started_at ON model_call_audits(started_at);
CREATE INDEX IF NOT EXISTS ix_model_call_audits_status ON model_call_audits(status);
CREATE INDEX IF NOT EXISTS ix_news_events_as_of ON news_events(as_of);
CREATE INDEX IF NOT EXISTS ix_news_events_event_type ON news_events(event_type);
CREATE INDEX IF NOT EXISTS ix_news_events_priority ON news_events(priority);
CREATE INDEX IF NOT EXISTS ix_news_events_published_at ON news_events(published_at);
CREATE UNIQUE INDEX IF NOT EXISTS ix_news_filter_logs_content_hash ON news_filter_logs(content_hash);
CREATE INDEX IF NOT EXISTS ix_news_filter_logs_last_filtered_at ON news_filter_logs(last_filtered_at);
CREATE INDEX IF NOT EXISTS ix_news_filter_logs_matched_keyword ON news_filter_logs(matched_keyword);
CREATE INDEX IF NOT EXISTS ix_news_filter_logs_published_at ON news_filter_logs(published_at);
CREATE INDEX IF NOT EXISTS ix_news_filter_logs_source ON news_filter_logs(source);
CREATE INDEX IF NOT EXISTS ix_news_items_as_of ON news_items(as_of);
CREATE UNIQUE INDEX IF NOT EXISTS ix_news_items_content_hash ON news_items(content_hash);
CREATE INDEX IF NOT EXISTS ix_news_items_published_at ON news_items(published_at);
CREATE INDEX IF NOT EXISTS ix_news_items_source ON news_items(source);
CREATE INDEX IF NOT EXISTS ix_news_items_source_feed ON news_items(source, published_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_news_processing_celery_task_id ON news_processing(celery_task_id);
CREATE INDEX IF NOT EXISTS ix_news_processing_heartbeat_at ON news_processing(heartbeat_at);
CREATE INDEX IF NOT EXISTS ix_news_processing_scan_task_id ON news_processing(scan_task_id);
CREATE INDEX IF NOT EXISTS ix_news_processing_status ON news_processing(status);
CREATE INDEX IF NOT EXISTS ix_news_processing_updated_at ON news_processing(updated_at);
CREATE INDEX IF NOT EXISTS ix_news_processing_outbox_available_at ON news_processing_outbox(available_at);
CREATE UNIQUE INDEX IF NOT EXISTS ix_news_processing_outbox_news_id ON news_processing_outbox(news_id);
CREATE INDEX IF NOT EXISTS ix_news_processing_outbox_status ON news_processing_outbox(status);
CREATE INDEX IF NOT EXISTS ix_news_processing_outbox_updated_at ON news_processing_outbox(updated_at);
CREATE INDEX IF NOT EXISTS ix_news_source_states_last_attempt_at ON news_source_states(last_attempt_at);
CREATE INDEX IF NOT EXISTS ix_news_source_states_last_success_at ON news_source_states(last_success_at);
CREATE INDEX IF NOT EXISTS ix_news_source_states_status ON news_source_states(status);
CREATE INDEX IF NOT EXISTS ix_news_source_states_updated_at ON news_source_states(updated_at);
CREATE INDEX IF NOT EXISTS ix_news_source_states_watermark_at ON news_source_states(watermark_at);
CREATE INDEX IF NOT EXISTS ix_outcomes_horizon_days ON outcomes(horizon_days);
CREATE INDEX IF NOT EXISTS ix_outcomes_recommendation_id ON outcomes(recommendation_id);
CREATE INDEX IF NOT EXISTS ix_paper_orders_asset_id ON paper_orders(asset_id);
CREATE INDEX IF NOT EXISTS ix_paper_orders_recommendation_id ON paper_orders(recommendation_id);
CREATE INDEX IF NOT EXISTS ix_recommendations_as_of ON recommendations(as_of);
CREATE INDEX IF NOT EXISTS ix_recommendations_asset_id ON recommendations(asset_id);
CREATE INDEX IF NOT EXISTS ix_recommendations_feed ON recommendations(as_of DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_recommendations_rating ON recommendations(rating);
CREATE UNIQUE INDEX IF NOT EXISTS ix_recommendations_run_id ON recommendations(run_id);
CREATE INDEX IF NOT EXISTS ix_research_runs_asset_id ON research_runs(asset_id);
CREATE INDEX IF NOT EXISTS ix_research_runs_event_id ON research_runs(event_id);
CREATE INDEX IF NOT EXISTS ix_research_runs_failed_feed ON research_runs(status, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_research_runs_status ON research_runs(status);

DELETE FROM integration_settings
WHERE key IN (
    SELECT 'mcp-source-group:' || id
    FROM mcp_sources
    WHERE managed = true AND name = 'DuckDuckGo'
);
DELETE FROM mcp_sources WHERE managed = true AND name = 'DuckDuckGo';

INSERT INTO mcp_sources(
    id,name,url,description,priority,enabled,managed,auth_type,auth_header_name,
    encrypted_secret,discovered_tools,tool_mappings,last_status,last_error,
    last_checked_at,created_at,updated_at
) VALUES (
    '10000000-0000-4000-8000-000000000001','SearXNG','http://search-mcp:8080/mcp',
    '内置 Go 联网验证搜索服务',50,true,true,'none',NULL,NULL,'[]'::json,
    '{"web_search":{"tool_name":"web_search","input_bindings":{"query":"query","limit":"limit","language":"language","time_range":"time_range"},"defaults":{},"output_adapter":"search_results_v1"}}'::json,
    'unchecked',NULL,NULL,now(),now()
) ON CONFLICT (name) DO NOTHING;

INSERT INTO mcp_sources(
    id,name,url,description,priority,enabled,managed,auth_type,auth_header_name,
    encrypted_secret,discovered_tools,tool_mappings,last_status,last_error,
    last_checked_at,created_at,updated_at
) VALUES (
    '10000000-0000-4000-8000-000000000002','FMP','http://fmp-mcp:8080/mcp',
    '内置受管的 Financial Modeling Prep 来源',100,true,true,'none',NULL,NULL,'[]'::json,
    '{"quote":{"tool_name":"getQuote","input_bindings":{"symbol":"symbol"},"defaults":{},"output_adapter":"raw_records_v1"},"fundamentals":{"tool_name":"getIncomeStatement","input_bindings":{"symbol":"symbol"},"defaults":{"limit":5,"period":"FY"},"output_adapter":"raw_records_v1"},"filings":{"tool_name":"getFilingsBySymbol","input_bindings":{"symbol":"symbol","from_date":"from_date","to":"to"},"defaults":{"limit":20,"page":0},"output_adapter":"filings_v1"}}'::json,
    'unchecked',NULL,NULL,now(),now()
) ON CONFLICT (name) DO NOTHING;

INSERT INTO integration_settings(key,payload,updated_at)
SELECT 'mcp-source-group:' || id, '{"group_id":"search"}'::json, now()
FROM mcp_sources WHERE name = 'SearXNG'
ON CONFLICT (key) DO NOTHING;

INSERT INTO integration_settings(key,payload,updated_at)
SELECT 'mcp-source-group:' || id, '{"group_id":"fmp"}'::json, now()
FROM mcp_sources WHERE name = 'FMP'
ON CONFLICT (key) DO NOTHING;
