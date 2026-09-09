package marketdata

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestLicensedBenchmarkImportIsAuditedAtomicAndIdempotentAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "licensed_benchmark_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

	now := time.Date(2026, 9, 10, 12, 34, 56, 0, time.UTC)
	input := LicensedBenchmarkPriceImport{
		VendorCode: ".HSIDV", SourceName: "Licensed Index Vendor", SourceDocumentID: "licensed-export:HSIDV:2026-09-10",
		SourceURL: "https://licensed.example.test/history?access_token=must-not-persist#download", LicenseReference: "agreement-2026-001",
		ApprovedBy: "market-data-owner", IdempotencyKey: "licensed-hsidv-2026-09-10",
		Observations: []LicensedBenchmarkPrice{
			{SessionDate: "2026-09-09", AdjustedClose: 12345.67},
			{SessionDate: "2026-09-08", AdjustedClose: 12200.50},
		},
	}
	receipt, created, err := NewStore(pool).ImportLicensedBenchmarkPrices(ctx, marketpolicy.HKBenchmarkAssetID, input, now)
	if err != nil || !created {
		t.Fatalf("licensed import created=%v err=%v", created, err)
	}
	if receipt.ContractVersion != LicensedBenchmarkPriceImportContractVersion || receipt.AssetID != marketpolicy.HKBenchmarkAssetID || receipt.Market != "HK" || receipt.Currency != "HKD" || receipt.SessionStart != "2026-09-08" || receipt.SessionEnd != "2026-09-09" || receipt.ObservationCount != 2 || receipt.InsertedCount != 2 || len(receipt.ObservationIDs) != 2 || !receipt.AvailableAt.Equal(now) {
		t.Fatalf("invalid licensed import receipt: %#v", receipt)
	}
	if receipt.SourceURL != "https://licensed.example.test/history" || strings.Contains(receipt.SourceURL, "must-not-persist") {
		t.Fatalf("licensed source URL was not sanitized: %q", receipt.SourceURL)
	}

	var count int
	var minAvailable, maxAvailable time.Time
	var minField, minPrecision, minReturnKind, minMethod, minVendor, minReceiptID, minURL string
	var exposedLicense, exposedApprover *string
	if err = pool.QueryRow(ctx, `SELECT count(*),min(available_at),max(available_at),min(price_field),min(time_precision),
		min(metadata::jsonb->>'return_series_kind'),min(metadata::jsonb->>'ingestion_method'),min(metadata::jsonb->>'vendor_code'),
		min(metadata::jsonb->>'audit_receipt_id'),min(metadata::jsonb->>'license_reference'),min(metadata::jsonb->>'approved_by'),min(source_url)
		FROM market_price_observations WHERE asset_id=$1`, marketpolicy.HKBenchmarkAssetID).Scan(&count, &minAvailable, &maxAvailable,
		&minField, &minPrecision, &minReturnKind, &minMethod, &minVendor, &minReceiptID, &exposedLicense, &exposedApprover, &minURL); err != nil {
		t.Fatal(err)
	}
	if count != 2 || !minAvailable.Equal(now) || !maxAvailable.Equal(now) || minField != "adjusted_close" || minPrecision != "daily_close" || minReturnKind != "gross_total_return_index" || minMethod != "licensed_manual_import" || minVendor != ".HSIDV" || minReceiptID != receipt.ID || exposedLicense != nil || exposedApprover != nil || minURL != "https://licensed.example.test/history" {
		t.Fatalf("invalid imported observations count=%d available=%s/%s field=%s precision=%s return=%s method=%s vendor=%s receipt=%s exposed_license=%v exposed_approver=%v url=%s",
			count, minAvailable, maxAvailable, minField, minPrecision, minReturnKind, minMethod, minVendor, minReceiptID, exposedLicense, exposedApprover, minURL)
	}

	input.Observations[0], input.Observations[1] = input.Observations[1], input.Observations[0]
	replayed, created, err := NewStore(pool).ImportLicensedBenchmarkPrices(ctx, marketpolicy.HKBenchmarkAssetID, input, now.Add(time.Hour))
	if err != nil || created || replayed.ID != receipt.ID || !replayed.AvailableAt.Equal(now) {
		t.Fatalf("licensed import replay receipt=%#v created=%v err=%v", replayed, created, err)
	}
	input.Observations[0].AdjustedClose++
	if _, _, err = NewStore(pool).ImportLicensedBenchmarkPrices(ctx, marketpolicy.HKBenchmarkAssetID, input, now.Add(2*time.Hour)); err == nil || !strings.Contains(err.Error(), "idempotency key is already bound") {
		t.Fatalf("changed replay was not rejected: %v", err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM market_price_observations WHERE asset_id=$1`, marketpolicy.HKBenchmarkAssetID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("rejected replay changed observations count=%d err=%v", count, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM licensed_benchmark_price_import_receipts WHERE asset_id=$1`, marketpolicy.HKBenchmarkAssetID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("unexpected receipt count=%d err=%v", count, err)
	}
	receipts, err := NewStore(pool).ListLicensedBenchmarkPriceImports(ctx, marketpolicy.HKBenchmarkAssetID, 20)
	if err != nil || len(receipts) != 1 || receipts[0].ID != receipt.ID || receipts[0].SessionStart != "2026-09-08" || receipts[0].SessionEnd != "2026-09-09" || receipts[0].LicenseReference != "agreement-2026-001" || receipts[0].ApprovedBy != "market-data-owner" || receipts[0].ObservationIDs != nil {
		t.Fatalf("licensed import receipt audit query=%#v err=%v", receipts, err)
	}
	emptyReceipts, err := NewStore(pool).ListLicensedBenchmarkPriceImports(ctx, marketpolicy.CNBenchmarkAssetID, 20)
	if err != nil || len(emptyReceipts) != 0 {
		t.Fatalf("empty canonical receipt query=%#v err=%v", emptyReceipts, err)
	}

	input.IdempotencyKey = "wrong-vendor-code"
	input.VendorCode = "HSI"
	if _, _, err = NewStore(pool).ImportLicensedBenchmarkPrices(ctx, marketpolicy.HKBenchmarkAssetID, input, now); err == nil || !strings.Contains(err.Error(), "canonical benchmark identity") {
		t.Fatalf("wrong vendor code was not rejected: %v", err)
	}
	input.IdempotencyKey = "ordinary-etf"
	input.VendorCode = "2800"
	if _, _, err = NewStore(pool).ImportLicensedBenchmarkPrices(ctx, "etf:XHKG:2800", input, now); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("unknown/noncanonical asset was not rejected: %v", err)
	}

	for _, table := range []string{"benchmark_mapping_observations", "prediction_runs", "forecast_versions", "valuation_runs", "fundamental_rating_states", "fundamental_research_plans"} {
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("licensed import mutated %s count=%d err=%v", table, count, err)
		}
	}
}
