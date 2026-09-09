package fundamentalresearch

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/forecast"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/rating"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/valuation"
)

func TestScheduledWorkflowRevaluesApprovedForecastAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "scheduled_fundamental_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

	const assetID = "equity:XNAS:SCHEDULED"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES($1,'equity','US','SCHEDULED','Scheduled Inc.','XNAS','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	const incomeID, balanceID, cashFlowID = "scheduled-income-snapshot", "scheduled-balance-snapshot", "scheduled-cash-flow-snapshot"
	if _, err = pool.Exec(ctx, `INSERT INTO fundamental_snapshots(id,asset_id,statement_type,fiscal_period,report_period_end,published_at,available_at,currency,unit,accounting_standard,source_name,source_url,source_document_id,metrics,source_payload,retrieved_at) VALUES
        ($1,$4,'income_statement','FY','2025-12-31',$5,$5,'USD','millions','US-GAAP','SEC','https://example.com/scheduled-income','10-k-2025-income','{"revenue":1000,"grossProfit":500,"operatingIncome":200,"netIncome":160,"ebitda":220,"weightedAverageShsOutDil":100}','{}',$5),
        ($2,$4,'balance_sheet','FY','2025-12-31',$5,$5,'USD','millions','US-GAAP','SEC','https://example.com/scheduled-balance','10-k-2025-balance','{"cashAndCashEquivalents":100,"totalDebt":50,"totalAssets":1500}','{}',$5),
        ($3,$4,'cash_flow','FY','2025-12-31',$5,$5,'USD','millions','US-GAAP','SEC','https://example.com/scheduled-cash','10-k-2025-cash','{"operatingCashFlow":180,"capitalExpenditure":30,"freeCashFlow":150}','{}',$5)`, incomeID, balanceID, cashFlowID, assetID, base.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	revenue, margin, tax, depreciation, capex, nwc, shares := 1000.0, .2, .2, 20.0, 30.0, 5.0, 100.0
	forecastVersion, _, err := forecast.NewStore(pool).Create(ctx, forecast.Submission{AssetID: assetID, AsOf: base, Inputs: forecast.Inputs{Currency: "USD", Unit: "millions", Revenue: &revenue, OperatingMargin: &margin, TaxRate: &tax, Depreciation: &depreciation, Capex: &capex, ChangeNWC: &nwc, DilutedShares: &shares}, FundamentalSnapshotIDs: []string{incomeID, balanceID, cashFlowID}})
	if err != nil || forecastVersion.Status != "available" {
		t.Fatalf("forecast=%#v err=%v", forecastVersion, err)
	}
	price := marketdata.PriceObservation{AssetID: assetID, Market: "US", Currency: "USD", ObservedAt: base.Add(-30 * time.Minute), AvailableAt: base, Price: 20, PriceField: "adjusted_close", TimePrecision: "timestamped", SourceName: "test-market", SourceDocumentID: "price-series:" + assetID}
	if _, err = marketdata.NewStore(pool).Save(ctx, price); err != nil {
		t.Fatal(err)
	}
	benchmark := .05
	submission := PlanSubmission{AssetID: assetID, ForecastVersionID: forecastVersion.ID, Valuation: ValuationPlan{MultipleScenarios: []valuation.MultipleScenario{{Name: "base", PriceEarningsMultiple: 20, ComparableEvidenceIDs: []string{"comparable-set-1"}}}}, Rating: ScheduledRatingPlan{Policy: rating.DefaultUSPolicy(), BenchmarkReturn: &benchmark, BenchmarkEvidenceID: "benchmark-expectation-1", ReasonCodes: []string{"approved_periodic_review"}, EvidenceIDs: []string{incomeID, balanceID, cashFlowID}}, CadenceHours: 24, MaxPriceAgeHours: 120, MaxPlanAgeDays: 90, ApprovedBy: "integration-test", IdempotencyKey: "scheduled-plan-request-1"}
	approvedAt := base
	plan, created, err := NewPlanStore(pool).Approve(ctx, submission, approvedAt)
	if err != nil || !created || plan.Status != "approved" {
		t.Fatalf("plan=%#v created=%v err=%v", plan, created, err)
	}
	repeated, created, err := NewPlanStore(pool).Approve(ctx, submission, approvedAt.Add(time.Minute))
	if err != nil || created || repeated.ID != plan.ID {
		t.Fatalf("idempotent plan=%#v created=%v err=%v", repeated, created, err)
	}
	due, err := NewPlanStore(pool).Due(ctx, approvedAt, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("due=%#v err=%v", due, err)
	}
	result, err := NewPlanStore(pool).Run(ctx, plan, approvedAt.Add(30*time.Minute))
	if err != nil || result.Status != "completed" || result.Valuation == nil || result.Rating == nil || result.Price == nil {
		t.Fatalf("scheduled result=%#v err=%v", result, err)
	}
	manual, err := New(pool).Run(ctx, Input{
		AssetID: assetID,
		AsOf:    result.AsOf,
		Forecast: ForecastPlan{
			Inputs:                 forecast.Inputs{Currency: "USD", Unit: "millions", Revenue: &revenue, OperatingMargin: &margin, TaxRate: &tax, Depreciation: &depreciation, Capex: &capex, ChangeNWC: &nwc, DilutedShares: &shares},
			FundamentalSnapshotIDs: []string{incomeID, balanceID, cashFlowID},
		},
		Valuation: ValuationPlan{MultipleScenarios: []valuation.MultipleScenario{{Name: "base", PriceEarningsMultiple: 20, ComparableEvidenceIDs: []string{"comparable-set-1"}}}},
		Rating: RatingPlan{
			Policy: rating.DefaultUSPolicy(), AsOfPrice: &result.Price.Price, AsOfPriceEvidenceID: result.Price.ID,
			BenchmarkReturn: &benchmark, BenchmarkEvidenceID: "benchmark-expectation-1",
			ReasonCodes: []string{"scheduled_fundamental_review", "approved_periodic_review"}, EvidenceIDs: []string{incomeID, balanceID, cashFlowID},
		},
	})
	if err != nil || manual.Status != "available" || manual.Valuation == nil || manual.Rating == nil {
		t.Fatalf("manual parity result=%#v err=%v", manual, err)
	}
	if manual.Forecast.ID != result.ForecastVersionID || manual.Valuation.ID != result.Valuation.ID || manual.Rating.Created || manual.Rating.State == nil || result.Rating.State == nil || manual.Rating.State.ValuationRunID != result.Rating.State.ValuationRunID {
		t.Fatalf("manual and scheduled paths diverged: manual=%#v scheduled=%#v", manual, result)
	}
	if err = NewPlanStore(pool).Record(ctx, plan, result, approvedAt.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var revisions int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM fundamental_rating_revisions WHERE asset_id=$1`, assetID).Scan(&revisions); err != nil || revisions != 1 {
		t.Fatalf("rating revisions=%d err=%v", revisions, err)
	}
	const revisedSnapshotID = "scheduled-fundamental-revision"
	newAvailable := approvedAt.Add(time.Hour)
	if _, err = pool.Exec(ctx, `INSERT INTO fundamental_snapshots(id,asset_id,statement_type,fiscal_period,report_period_end,published_at,available_at,currency,unit,accounting_standard,source_name,source_url,source_document_id,metrics,source_payload,retrieved_at) VALUES($1,$2,'income_statement','FY','2026-03-31',$3,$3,'USD','millions','US-GAAP','SEC','https://example.com/scheduled-q1','10-q-2026','{}','{}',$3)`, revisedSnapshotID, assetID, newAvailable); err != nil {
		t.Fatal(err)
	}
	review, err := NewPlanStore(pool).Run(ctx, plan, newAvailable.Add(time.Minute))
	if err != nil || review.Status != "review_required" || review.Reason != "new_financial_statement_requires_forecast_review" {
		t.Fatalf("new financial statement did not stop automation: result=%#v err=%v", review, err)
	}
	paused, err := NewPlanStore(pool).Pause(ctx, assetID, newAvailable.Add(2*time.Minute))
	if err != nil || !paused {
		t.Fatalf("pause=%v err=%v", paused, err)
	}
	staleResult, err := NewPlanStore(pool).Run(ctx, plan, newAvailable.Add(3*time.Minute))
	if err != nil || staleResult.Status != "not_applicable" || staleResult.Reason != "scheduled_research_plan_is_not_approved" {
		t.Fatalf("paused plan executed: result=%#v err=%v", staleResult, err)
	}
	if err = NewPlanStore(pool).Record(ctx, plan, result, newAvailable.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	history, err := NewPlanStore(pool).History(ctx, assetID, 1)
	if err != nil || len(history) != 1 || history[0].Status != "paused" {
		t.Fatalf("stale result reactivated paused plan: history=%#v err=%v", history, err)
	}
}
