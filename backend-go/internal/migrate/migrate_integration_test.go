package migrate

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUpCreatesFreshGoRuntimeSchema(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "phase3_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") }()

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := Up(ctx, pool); err != nil {
		t.Fatal(err)
	}

	for table, columns := range legacyORMColumns {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, schema+"."+table).Scan(&exists); err != nil || !exists {
			t.Fatalf("table %s was not created: exists=%v err=%v", table, exists, err)
		}
		var actual []string
		if err := pool.QueryRow(ctx, `SELECT array_agg(column_name ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`, schema, table).Scan(&actual); err != nil {
			t.Fatalf("columns for %s: %v", table, err)
		}
		if !slices.Equal(actual, columns) {
			t.Fatalf("table %s columns\n got: %v\nwant: %v", table, actual, columns)
		}
		assertPrimaryKey(t, ctx, pool, schema, table, legacyORMPrimaryKeys[table])
	}
	for _, table := range []string{"go_jobs", "go_job_dependencies", "go_worker_instances", "go_workflow_checkpoints"} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, schema+"."+table).Scan(&exists); err != nil || !exists {
			t.Fatalf("Go runtime table %s was not created: exists=%v err=%v", table, exists, err)
		}
	}
	for _, index := range legacyORMIndexes {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname=$1 AND indexname=$2)`, schema, index).Scan(&exists); err != nil || !exists {
			t.Fatalf("ORM index %s was not created: exists=%v err=%v", index, exists, err)
		}
	}
	for table, column := range legacyORMUniqueColumns {
		var unique bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_index i
				JOIN pg_class t ON t.oid=i.indrelid
				JOIN pg_namespace n ON n.oid=t.relnamespace
				JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=ANY(i.indkey)
				WHERE n.nspname=$1 AND t.relname=$2 AND i.indisunique
				GROUP BY i.indexrelid HAVING array_agg(a.attname ORDER BY a.attname)=ARRAY[$3]::name[]
			)
		`, schema, table, column).Scan(&unique); err != nil || !unique {
			t.Fatalf("unique key %s(%s) was not created: exists=%v err=%v", table, column, unique, err)
		}
	}
	var embeddingType string
	if err := pool.QueryRow(ctx, `SELECT format_type(a.atttypid,a.atttypmod) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname='document_chunks' AND a.attname='embedding'`, schema).Scan(&embeddingType); err != nil || strings.TrimPrefix(embeddingType, "public.") != "vector(384)" {
		t.Fatalf("document_chunks.embedding type=%q err=%v", embeddingType, err)
	}
	var searx, duck int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE name='SearXNG'),count(*) FILTER (WHERE name='DuckDuckGo') FROM mcp_sources`).Scan(&searx, &duck); err != nil {
		t.Fatal(err)
	}
	if searx != 1 || duck != 0 {
		t.Fatalf("unexpected managed MCP seed state: SearXNG=%d DuckDuckGo=%d", searx, duck)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'test','official','title','summary','https://example.com','en',now(),now(),now(),$2,'[]','{}')`, uuid.NewString(), fmt.Sprintf("%064d", 1)); err != nil {
		t.Fatalf("fresh schema rejected a core write: %v", err)
	}

	newsID, eventID := uuid.New(), uuid.New()
	summary := "博通(AVGO.O)2026财年Q4展望营收为348亿美元。"
	if _, err := pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'金十','professional','博通(AVGO.',$2,'https://example.com/avgo','zh',now(),now(),now(),$3,'[]','{"mcp_adapter":"jin10_flash_v1"}')`, newsID, summary, fmt.Sprintf("%064d", 2)); err != nil {
		t.Fatalf("insert malformed headline fixture: %v", err)
	}
	eventPayload := fmt.Sprintf(`{"id":%q,"headline":"博通(AVGO.","news_item_ids":[%q]}`, eventID, newsID)
	if _, err := pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,'博通(AVGO.','earnings',$2,0.5,now(),now(),now())`, eventID, eventPayload); err != nil {
		t.Fatalf("insert malformed event fixture: %v", err)
	}
	decimalNewsID, decimalEventID, ordinaryNewsID := uuid.New(), uuid.New(), uuid.New()
	decimalSummary := "【美国8月非农远高于预期】美国8月季调后非农就业人口录得16.2万人。"
	decimalTitle := "【美国8月非农远高于预期】美国8月季调后非农就业人口录得16."
	if _, err := pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'金十','professional',$2,$3,'https://example.com/payroll','zh',now(),now(),now(),$4,'[]','{"mcp_adapter":"jin10_flash_v1"}')`, decimalNewsID, decimalTitle, decimalSummary, fmt.Sprintf("%064d", 3)); err != nil {
		t.Fatalf("insert decimal headline fixture: %v", err)
	}
	decimalPayload := fmt.Sprintf(`{"id":%q,"headline":%q,"news_item_ids":[%q]}`, decimalEventID, decimalTitle, decimalNewsID)
	if _, err := pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,$2,'macro',$3,0.5,now(),now(),now())`, decimalEventID, decimalTitle, decimalPayload); err != nil {
		t.Fatalf("insert decimal event fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'金十','professional','Independent title.','Independent title. Next sentence','https://example.com/ordinary','en',now(),now(),now(),$2,'[]','{"mcp_adapter":"jin10_flash_v1"}')`, ordinaryNewsID, fmt.Sprintf("%064d", 4)); err != nil {
		t.Fatalf("insert ordinary headline fixture: %v", err)
	}
	if err := Up(ctx, pool); err != nil {
		t.Fatalf("reapply migrations for headline repair: %v", err)
	}
	var repairedNews, repairedEvent, repairedPayload string
	if err := pool.QueryRow(ctx, `SELECT title FROM news_items WHERE id=$1`, newsID).Scan(&repairedNews); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT headline,payload::jsonb->>'headline' FROM news_events WHERE id=$1`, eventID).Scan(&repairedEvent, &repairedPayload); err != nil {
		t.Fatal(err)
	}
	if repairedNews != summary || repairedEvent != summary || repairedPayload != summary {
		t.Fatalf("dotted symbol headline repair failed: news=%q event=%q payload=%q", repairedNews, repairedEvent, repairedPayload)
	}
	if err := pool.QueryRow(ctx, `SELECT title FROM news_items WHERE id=$1`, decimalNewsID).Scan(&repairedNews); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT headline,payload::jsonb->>'headline' FROM news_events WHERE id=$1`, decimalEventID).Scan(&repairedEvent, &repairedPayload); err != nil {
		t.Fatal(err)
	}
	if repairedNews != decimalSummary || repairedEvent != decimalSummary || repairedPayload != decimalSummary {
		t.Fatalf("decimal headline repair failed: news=%q event=%q payload=%q", repairedNews, repairedEvent, repairedPayload)
	}
	var ordinaryTitle string
	if err := pool.QueryRow(ctx, `SELECT title FROM news_items WHERE id=$1`, ordinaryNewsID).Scan(&ordinaryTitle); err != nil {
		t.Fatal(err)
	}
	if ordinaryTitle != "Independent title." {
		t.Fatalf("ordinary short title was unexpectedly expanded: %q", ordinaryTitle)
	}
}

func TestUpUpgradesLegacyAssetTableWithoutRebuildingData(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "legacy_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") }()

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE assets(
		id varchar(160) PRIMARY KEY, asset_class varchar(20) NOT NULL, market varchar(20) NOT NULL,
		symbol varchar(40) NOT NULL, name varchar(200) NOT NULL, exchange_or_provider varchar(80) NOT NULL,
		currency varchar(10) NOT NULL, aliases json NOT NULL, products json NOT NULL, competitors json NOT NULL,
		lot_size integer NOT NULL, active boolean NOT NULL
	); INSERT INTO assets VALUES('legacy-asset','equity','US','OLD','Old Asset','NASDAQ','USD','[]','[]','[]',1,true)`); err != nil {
		t.Fatal(err)
	}
	if err := Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var id, sector, tier string
	if err := pool.QueryRow(ctx, `SELECT id,sector_id,association_tier FROM assets WHERE id='legacy-asset'`).Scan(&id, &sector, &tier); err != nil {
		t.Fatal(err)
	}
	if id != "legacy-asset" || sector != "" || tier != "standard" {
		t.Fatalf("legacy row changed during migration: id=%q sector=%q tier=%q", id, sector, tier)
	}
}

// TestUpPreservesExistingDatabase is deliberately opt-in because it applies
// migrations to a database supplied by the caller. Point it at a disposable
// restore of an existing database, never at the source database itself.
func TestUpPreservesExistingDatabase(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_EXISTING_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_EXISTING_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	before := make(map[string]string)
	for _, table := range legacyDataTables {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+table).Scan(&exists); err != nil {
			t.Fatalf("detect table %s: %v", table, err)
		}
		if !exists {
			continue
		}
		before[table] = tableFingerprint(t, ctx, pool, table)
	}
	if len(before) == 0 {
		t.Fatal("existing database contains none of the legacy data tables")
	}

	if err := Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := Up(ctx, pool); err != nil {
		t.Fatalf("migrations are not idempotent: %v", err)
	}
	for table, fingerprint := range before {
		if after := tableFingerprint(t, ctx, pool, table); after != fingerprint {
			t.Errorf("table %s data changed during migration: before=%s after=%s", table, fingerprint, after)
		}
	}
}

func tableFingerprint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) string {
	t.Helper()
	identifier := pgx.Identifier{table}.Sanitize()
	query := `SELECT count(*)::text || ':' || COALESCE(
		md5(string_agg(md5(row_to_json(data_row)::text), '' ORDER BY md5(row_to_json(data_row)::text))),
		md5('')
	) FROM ` + identifier + ` AS data_row`
	var fingerprint string
	if err := pool.QueryRow(ctx, query).Scan(&fingerprint); err != nil {
		t.Fatalf("fingerprint table %s: %v", table, err)
	}
	return fingerprint
}

func assertPrimaryKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema, table string, expected []string) {
	t.Helper()
	var actual []string
	err := pool.QueryRow(ctx, `
		SELECT array_agg(a.attname ORDER BY key.ordinality)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid=c.conrelid
		JOIN pg_namespace n ON n.oid=t.relnamespace
		JOIN unnest(c.conkey) WITH ORDINALITY AS key(attnum,ordinality) ON true
		JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=key.attnum
		WHERE n.nspname=$1 AND t.relname=$2 AND c.contype='p'
	`, schema, table).Scan(&actual)
	if err != nil || !slices.Equal(actual, expected) {
		t.Fatalf("primary key %s got=%v want=%v err=%v", table, actual, expected, err)
	}
}

var legacyORMColumns = map[string][]string{
	"assets":                   {"id", "asset_class", "market", "symbol", "name", "exchange_or_provider", "currency", "aliases", "products", "competitors", "lot_size", "active", "issuer_id", "primary_listing_asset_id", "sector_id", "industry_id", "raw_sector", "raw_industry", "instrument_type", "market_cap", "market_cap_rank", "last_synced_at", "manual_industry_id", "manual_active", "association_tier", "association_reason", "provider_association_tier", "provider_association_reason", "manual_association_tier"},
	"industries":               {"id", "parent_id", "level", "name_zh", "name_en", "aliases", "active"},
	"asset_universe_sync":      {"market", "status", "asset_count", "industry_count", "added_count", "updated_count", "deactivated_count", "last_error", "started_at", "completed_at"},
	"news_items":               {"id", "source", "source_quality", "title", "summary", "url", "language", "published_at", "observed_at", "as_of", "content_hash", "symbols", "raw_metadata"},
	"news_source_states":       {"source", "provider", "status", "watermark_at", "last_attempt_at", "last_success_at", "last_error", "last_discovered_count", "last_new_count", "consecutive_failures", "created_at", "updated_at"},
	"news_processing":          {"news_id", "status", "scan_task_id", "celery_task_id", "attempt_count", "last_error", "queued_at", "started_at", "completed_at", "heartbeat_at", "created_at", "updated_at"},
	"news_processing_outbox":   {"id", "news_id", "status", "force_asset_mapping", "dispatch_attempts", "available_at", "dispatched_at", "last_error", "created_at", "updated_at"},
	"news_filter_logs":         {"id", "content_hash", "source", "title", "url", "matched_keyword", "published_at", "first_filtered_at", "last_filtered_at", "hit_count"},
	"news_events":              {"id", "headline", "event_type", "payload", "priority", "published_at", "observed_at", "as_of"},
	"research_runs":            {"id", "event_id", "asset_id", "status", "payload", "created_at", "updated_at"},
	"event_research_runs":      {"id", "event_id", "status", "payload", "created_at", "updated_at"},
	"evidence":                 {"id", "run_id", "claim", "source_url", "source_quality", "published_at", "observed_at", "as_of", "payload"},
	"document_chunks":          {"id", "evidence_id", "run_id", "asset_id", "text", "terms", "embedding", "source_url", "source_quality", "published_at", "observed_at", "as_of"},
	"recommendations":          {"id", "run_id", "asset_id", "score", "rating", "confidence", "as_of", "payload"},
	"paper_orders":             {"id", "recommendation_id", "asset_id", "side", "quantity", "price", "currency", "fee", "executed_at", "payload"},
	"outcomes":                 {"id", "recommendation_id", "horizon_days", "observed_at", "payload"},
	"source_lineage":           {"news_item_id", "canonical_url", "original_publisher", "original_document_id", "syndication_group", "parse_status", "source_chain", "payload", "created_at", "updated_at"},
	"policy_evaluations":       {"id", "event_id", "asset_id", "policy_version", "policy_mode", "input_snapshot", "legacy_result", "policy_result", "comparison", "code_version", "prompt_version", "model", "created_at"},
	"policy_release_approvals": {"policy_version", "approved_by", "approved_at", "reviewed_valid_impacts", "shadow_started_at", "note"},
	"evolution_candidates":     {"id", "branch", "status", "payload", "created_at"},
	"model_call_audits":        {"id", "logical_call_id", "source_key", "provider", "model", "operation", "entity_type", "entity_id", "attempt", "status", "fidelity", "started_at", "completed_at", "duration_ms", "prompt_tokens", "completion_tokens", "input_language", "output_language", "messages", "schema_payload", "raw_response", "parsed_response", "error", "metrics"},
	"mcp_sources":              {"id", "name", "url", "description", "priority", "enabled", "managed", "auth_type", "auth_header_name", "encrypted_secret", "discovered_tools", "tool_mappings", "last_status", "last_error", "last_checked_at", "created_at", "updated_at"},
	"integration_settings":     {"key", "payload", "updated_at"},
	"asset_relationships":      {"source_asset_id", "target_asset_id", "relationship_type", "confidence", "provenance", "details", "active", "updated_at"},
	"checkpoint_migrations":    {"v"},
	"checkpoints":              {"thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "type", "checkpoint", "metadata"},
	"checkpoint_blobs":         {"thread_id", "checkpoint_ns", "channel", "version", "type", "blob"},
	"checkpoint_writes":        {"thread_id", "checkpoint_ns", "checkpoint_id", "task_id", "idx", "channel", "type", "blob", "task_path"},
}

var legacyORMPrimaryKeys = map[string][]string{
	"assets": {"id"}, "industries": {"id"}, "asset_universe_sync": {"market"}, "news_items": {"id"},
	"news_source_states": {"source"}, "news_processing": {"news_id"}, "news_processing_outbox": {"id"},
	"news_filter_logs": {"id"}, "news_events": {"id"}, "research_runs": {"id"}, "event_research_runs": {"id"},
	"evidence": {"id"}, "document_chunks": {"id"}, "recommendations": {"id"}, "paper_orders": {"id"},
	"outcomes": {"id"}, "evolution_candidates": {"id"}, "model_call_audits": {"id"}, "mcp_sources": {"id"},
	"source_lineage": {"news_item_id"}, "policy_evaluations": {"id"}, "policy_release_approvals": {"policy_version"},
	"integration_settings": {"key"}, "asset_relationships": {"source_asset_id", "target_asset_id", "relationship_type"},
	"checkpoint_migrations": {"v"}, "checkpoints": {"thread_id", "checkpoint_ns", "checkpoint_id"},
	"checkpoint_blobs":  {"thread_id", "checkpoint_ns", "channel", "version"},
	"checkpoint_writes": {"thread_id", "checkpoint_ns", "checkpoint_id", "task_id", "idx"},
}

var legacyORMUniqueColumns = map[string]string{
	"news_items": "content_hash", "news_processing_outbox": "news_id", "news_filter_logs": "content_hash",
	"event_research_runs": "event_id", "document_chunks": "evidence_id", "recommendations": "run_id",
	"evolution_candidates": "branch", "model_call_audits": "source_key", "mcp_sources": "name",
}

var legacyORMIndexes = []string{
	"ix_asset_universe_sync_status",
	"ix_assets_asset_class", "ix_assets_market", "ix_assets_symbol", "ix_assets_name", "ix_assets_sector_id", "ix_assets_industry_id",
	"ix_assets_instrument_type", "ix_assets_market_cap", "ix_assets_market_cap_rank", "ix_assets_association_tier", "ix_assets_last_synced_at",
	"ix_assets_manual_industry_id", "ix_assets_manual_association_tier",
	"ix_industries_parent_id", "ix_industries_level", "ix_industries_name_zh", "ix_industries_name_en",
	"ix_news_items_source", "ix_news_items_published_at", "ix_news_items_as_of", "ix_news_items_content_hash",
	"ix_news_source_states_status", "ix_news_source_states_watermark_at", "ix_news_source_states_last_attempt_at", "ix_news_source_states_last_success_at", "ix_news_source_states_updated_at",
	"ix_news_processing_status", "ix_news_processing_scan_task_id", "ix_news_processing_celery_task_id", "ix_news_processing_heartbeat_at", "ix_news_processing_updated_at",
	"ix_news_processing_outbox_news_id", "ix_news_processing_outbox_status", "ix_news_processing_outbox_available_at", "ix_news_processing_outbox_updated_at",
	"ix_news_filter_logs_content_hash", "ix_news_filter_logs_source", "ix_news_filter_logs_matched_keyword", "ix_news_filter_logs_published_at", "ix_news_filter_logs_last_filtered_at",
	"ix_news_events_event_type", "ix_news_events_priority", "ix_news_events_published_at", "ix_news_events_as_of",
	"ix_research_runs_event_id", "ix_research_runs_asset_id", "ix_research_runs_status",
	"ix_event_research_runs_event_id", "ix_event_research_runs_status",
	"ix_evidence_run_id", "ix_evidence_source_quality", "ix_evidence_published_at", "ix_evidence_observed_at", "ix_evidence_as_of",
	"ix_document_chunks_evidence_id", "ix_document_chunks_run_id", "ix_document_chunks_asset_id", "ix_document_chunks_source_quality", "ix_document_chunks_published_at", "ix_document_chunks_observed_at", "ix_document_chunks_as_of",
	"ix_recommendations_run_id", "ix_recommendations_asset_id", "ix_recommendations_rating", "ix_recommendations_as_of",
	"ix_paper_orders_recommendation_id", "ix_paper_orders_asset_id", "ix_outcomes_recommendation_id", "ix_outcomes_horizon_days",
	"ix_source_lineage_group", "ix_source_lineage_status", "ix_policy_evaluations_event", "ix_policy_evaluations_asset",
	"ix_evolution_candidates_status",
	"ix_model_call_audits_logical_call_id", "ix_model_call_audits_provider", "ix_model_call_audits_model", "ix_model_call_audits_operation",
	"ix_model_call_audits_entity_type", "ix_model_call_audits_entity_id", "ix_model_call_audits_status", "ix_model_call_audits_fidelity",
	"ix_model_call_audits_started_at", "ix_model_call_audits_completed_at", "ix_model_call_audits_input_language", "ix_model_call_audits_output_language",
	"ix_mcp_sources_name", "ix_mcp_sources_priority", "ix_mcp_sources_enabled",
	"ix_asset_relationships_source_asset_id", "ix_asset_relationships_target_asset_id", "ix_asset_relationships_relationship_type", "ix_asset_relationships_active",
	"checkpoints_thread_id_idx", "checkpoint_blobs_thread_id_idx", "checkpoint_writes_thread_id_idx",
}

// Managed MCP seed rows are intentionally excluded because migrations may add
// or retire managed providers. All business, evidence and checkpoint data is
// fingerprinted byte-for-byte through PostgreSQL's JSON row representation.
var legacyDataTables = []string{
	"assets", "industries", "asset_universe_sync", "news_items", "news_source_states",
	"news_processing", "news_processing_outbox", "news_filter_logs", "news_events",
	"research_runs", "event_research_runs", "evidence", "document_chunks", "recommendations",
	"paper_orders", "outcomes", "evolution_candidates", "model_call_audits", "integration_settings",
	"source_lineage", "policy_evaluations", "policy_release_approvals",
	"asset_relationships", "checkpoint_migrations", "checkpoints", "checkpoint_blobs", "checkpoint_writes",
}
