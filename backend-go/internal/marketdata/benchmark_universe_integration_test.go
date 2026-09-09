package marketdata

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestBenchmarkMappingIsApprovedImmutableAndPointInTimeAgainstIsolatedPostgres(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "benchmark_mapping_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
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
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, values := range [][]string{
		{"equity:XNAS:TARGET", "equity", "US", "TARGET", "Target Inc.", "XNAS", "USD"},
		{"equity:ARCX:BENCH", "etf", "US", "BENCH", "Benchmark ETF", "ARCX", "USD"},
		{"equity:XTKS:JPYBENCH", "etf", "JP", "JPYBENCH", "Yen Benchmark", "XTKS", "JPY"},
	} {
		if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
            VALUES($1,$2,$3,$4,$5,$6,$7,'[]','[]','[]',1,true)`, values[0], values[1], values[2], values[3], values[4], values[5], values[6]); err != nil {
			t.Fatal(err)
		}
	}
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	approvedAt := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	input := BenchmarkMappingSubmission{
		ScopeType: "market", ScopeID: "US", SubjectMarket: "US", SubjectCurrency: "USD", BenchmarkAssetID: "equity:ARCX:BENCH",
		PolicyVersion: "us-policy-v1", ValidFrom: validFrom, SourceName: "Investment Committee", SourceDocumentID: "committee-2026-01",
		SourceURL: "https://user:secret@example.test/policy?token=secret", MappingReason: "broad USD equity total-return policy", ApprovedBy: "reviewer@example.test", IdempotencyKey: "benchmark-us-v1",
	}
	store := NewStore(pool)
	createdMapping, created, err := store.CreateBenchmarkMapping(ctx, input, approvedAt)
	if err != nil || !created {
		t.Fatalf("create mapping created=%v err=%v", created, err)
	}
	if createdMapping.SourceURL != "https://example.test/policy" {
		t.Fatalf("source credentials leaked: %s", createdMapping.SourceURL)
	}
	request := BenchmarkResolutionRequest{AssetID: "equity:XNAS:TARGET", Market: "US", Currency: "USD", EffectiveAt: validFrom.Add(time.Hour), AvailableAsOf: approvedAt.Add(-time.Second)}
	resolution, err := store.ResolveBenchmark(ctx, request)
	if err != nil || resolution.Status != "unavailable" || resolution.Reason != "missing_point_in_time_mapping" {
		t.Fatalf("future approval leaked into cutoff: resolution=%#v err=%v", resolution, err)
	}
	request.AvailableAsOf = approvedAt
	resolution, err = store.ResolveBenchmark(ctx, request)
	if err != nil || resolution.Status != "available" || resolution.Mapping == nil || resolution.Mapping.BenchmarkAssetID != "equity:ARCX:BENCH" {
		t.Fatalf("approved mapping unavailable: resolution=%#v err=%v", resolution, err)
	}
	_, created, err = store.CreateBenchmarkMapping(ctx, input, approvedAt.Add(time.Hour))
	if err != nil || created {
		t.Fatalf("idempotent replay changed mapping: created=%v err=%v", created, err)
	}
	input.MappingReason = "different request"
	if _, _, err = store.CreateBenchmarkMapping(ctx, input, approvedAt.Add(time.Hour)); err == nil || !strings.Contains(err.Error(), "different benchmark mapping") {
		t.Fatalf("idempotency collision was accepted: %v", err)
	}
	input.IdempotencyKey = "currency-mismatch"
	input.MappingReason = "invalid cross currency mapping"
	input.BenchmarkAssetID = "equity:XTKS:JPYBENCH"
	if _, _, err = store.CreateBenchmarkMapping(ctx, input, approvedAt); err == nil || !strings.Contains(err.Error(), "currency") {
		t.Fatalf("cross-currency benchmark was accepted: %v", err)
	}
}

func TestUniverseQueriesKeepFailuresAndHistoricalMembershipAgainstIsolatedPostgres(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "universe_history_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	config, _ := pgxpool.ParseConfig(dsn)
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	assetID := "equity:XNAS:HISTORY"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
        VALUES($1,'equity','US','HISTORY','History Inc.','XNAS','USD','[]','[]','[]',1,false)`, assetID); err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	second := first.Add(24 * time.Hour)
	if _, err = pool.Exec(ctx, `INSERT INTO security_universe_snapshots(id,universe_id,market,status,observed_at,available_at,source_name,source_document_id,eligibility_policy,asset_count,included_count,metadata)
        VALUES('snapshot-ok','market:US','US','completed',$1,$1,'provider','doc-ok','{}',1,1,'{}')`, first); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO security_universe_memberships(snapshot_id,asset_id,membership_status,effective_at,available_at,reason_codes,source_identity)
        VALUES('snapshot-ok',$1,'included',$2,$2,'["provider_snapshot_member"]','{}')`, assetID, first); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO security_universe_snapshots(id,universe_id,market,status,observed_at,available_at,source_name,source_document_id,eligibility_policy,asset_count,failure_detail,metadata)
        VALUES('snapshot-failed','market:US','US','failed',$1,$1,'provider','doc-failed','{}',1,'provider timeout','{}')`, second); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	snapshots, err := store.ListUniverseSnapshots(ctx, "market:US", second, 10)
	if err != nil || len(snapshots) != 2 || snapshots[0].Status != "failed" || snapshots[0].FailureDetail != "provider timeout" {
		t.Fatalf("failure history missing: snapshots=%#v err=%v", snapshots, err)
	}
	memberships, err := store.ListAssetUniverseMemberships(ctx, assetID, first, 10)
	if err != nil || len(memberships) != 1 || memberships[0].MembershipStatus != "included" {
		t.Fatalf("historical membership missing: memberships=%#v err=%v", memberships, err)
	}
	before, err := store.ListUniverseSnapshots(ctx, "market:US", first.Add(-time.Second), 10)
	if err != nil || len(before) != 0 {
		t.Fatalf("future snapshot leaked into cutoff: snapshots=%#v err=%v", before, err)
	}
}
