package fundamentalresearch

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentals"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestPreparationBuildsSourceLinkedFactsWithoutInventingApprovalAgainstPostgres(t *testing.T) {
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
	schema := "fundamental_preparation_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

	const assetID = "equity:XNAS:PREP"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES($1,'equity','US','PREP','Preparation Inc.','XNAS','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	period := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)
	published := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	store := fundamentals.NewStore(pool)
	snapshots := []fundamentals.Snapshot{
		{ID: "prep-income", AssetID: assetID, StatementType: fundamentals.IncomeStatement, FiscalPeriod: "FY", ReportPeriodEnd: period, PublishedAt: published, AvailableAt: published, Currency: "USD", Unit: "reported", AccountingStd: "US-GAAP", SourceName: "test", SourceURL: "https://example.com/income", SourceDocumentID: "income-2025", Metrics: map[string]any{"revenue": 1000.0, "operatingIncome": 200.0, "incomeBeforeTax": 180.0, "incomeTaxExpense": 36.0, "weightedAverageShsOutDil": 100.0}, SourcePayload: map[string]any{}, RetrievedAt: published.Add(time.Hour)},
		{ID: "prep-balance", AssetID: assetID, StatementType: fundamentals.BalanceSheet, FiscalPeriod: "FY", ReportPeriodEnd: period, PublishedAt: published, AvailableAt: published, Currency: "USD", Unit: "reported", AccountingStd: "US-GAAP", SourceName: "test", SourceURL: "https://example.com/balance", SourceDocumentID: "balance-2025", Metrics: map[string]any{"cashAndCashEquivalents": 100.0, "totalDebt": 50.0, "totalAssets": 1500.0}, SourcePayload: map[string]any{}, RetrievedAt: published.Add(time.Hour)},
		{ID: "prep-cash", AssetID: assetID, StatementType: fundamentals.CashFlow, FiscalPeriod: "FY", ReportPeriodEnd: period, PublishedAt: published, AvailableAt: published, Currency: "USD", Unit: "reported", AccountingStd: "US-GAAP", SourceName: "test", SourceURL: "https://example.com/cash", SourceDocumentID: "cash-2025", Metrics: map[string]any{"depreciationAndAmortization": 20.0, "capitalExpenditure": -30.0, "changeInWorkingCapital": -5.0, "operatingCashFlow": 180.0, "freeCashFlow": 150.0}, SourcePayload: map[string]any{}, RetrievedAt: published.Add(time.Hour)},
	}
	for _, snapshot := range snapshots {
		if _, err = store.Save(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
	}
	future := snapshots[0]
	future.ID, future.PublishedAt, future.AvailableAt, future.RevisionAt = "prep-income-future", published.Add(48*time.Hour), published.Add(48*time.Hour), nil
	future.SourceDocumentID = "income-2025-future-revision"
	future.Metrics = map[string]any{"revenue": 9999.0, "operatingIncome": 200.0, "incomeBeforeTax": 180.0, "incomeTaxExpense": 36.0, "weightedAverageShsOutDil": 100.0}
	future.RetrievedAt = future.AvailableAt.Add(time.Hour)
	if _, err = store.Save(ctx, future); err != nil {
		t.Fatal(err)
	}

	cutoff := published.Add(24 * time.Hour)
	result, err := NewPreparationService(pool).Prepare(ctx, assetID, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "analyst_review_required" || result.StatementPeriodEnd == nil || !result.StatementPeriodEnd.Equal(period) {
		t.Fatalf("preparation status=%s reason=%s period=%v", result.Status, result.Reason, result.StatementPeriodEnd)
	}
	assertPreparationValue(t, "revenue", result.FactualInputs.Revenue, 1000)
	assertPreparationValue(t, "operating_margin", result.FactualInputs.OperatingMargin, .2)
	assertPreparationValue(t, "tax_rate", result.FactualInputs.TaxRate, .2)
	assertPreparationValue(t, "depreciation", result.FactualInputs.Depreciation, 20)
	assertPreparationValue(t, "capex", result.FactualInputs.Capex, 30)
	assertPreparationValue(t, "change_nwc", result.FactualInputs.ChangeNWC, 5)
	assertPreparationValue(t, "diluted_shares", result.FactualInputs.DilutedShares, 100)
	if len(result.SnapshotIDs) != 3 || len(result.MissingFields) != 0 || len(result.FieldLineage) != 7 {
		t.Fatalf("snapshot_ids=%v missing=%v lineage=%v", result.SnapshotIDs, result.MissingFields, result.FieldLineage)
	}
	if result.FieldLineage["capex"].Transform != "absolute_cash_outflow" || result.FieldLineage["change_nwc"].Transform != "invert_provider_cash_flow_sign" {
		t.Fatalf("cash-flow sign lineage was not explicit: %#v", result.FieldLineage)
	}
	if result.Controls.AutomaticAssumptions || result.Controls.AutomaticValuation || result.Controls.AutomaticRating || !result.Controls.AnalystApproval {
		t.Fatalf("preparation controls=%#v", result.Controls)
	}
	if result.WorkflowTemplate == nil || len(result.WorkflowTemplate.Forecast.Assumptions) != 0 || len(result.WorkflowTemplate.Valuation.DCFScenarios) != 0 || len(result.WorkflowTemplate.Valuation.MultipleScenarios) != 0 || result.WorkflowTemplate.Rating.BenchmarkReturn != nil {
		t.Fatalf("preparation invented governed inputs: %#v", result.WorkflowTemplate)
	}
}

func assertPreparationValue(t *testing.T, name string, actual *float64, expected float64) {
	t.Helper()
	if actual == nil || math.Abs(*actual-expected) > 1e-9 {
		t.Fatalf("%s=%v expected=%v", name, actual, expected)
	}
}
