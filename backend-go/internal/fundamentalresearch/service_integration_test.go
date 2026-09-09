package fundamentalresearch

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/analystevidence"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/forecast"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/rating"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/valuation"
)

func TestWorkflowCompletesWithoutNewsAgainstIsolatedPostgres(t *testing.T) {
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
	schema := "fundamental_workflow_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	const assetID = "equity:XNAS:ACME"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES($1,'equity','US','ACME','Acme Inc.','XNAS','USD','[]','[]','[]',1,true)`, assetID); err != nil {
		t.Fatal(err)
	}
	asOf := time.Date(2026, 9, 8, 14, 0, 0, 0, time.UTC)
	const snapshotID = "fundamental-workflow-snapshot"
	if _, err = pool.Exec(ctx, `INSERT INTO fundamental_snapshots(id,asset_id,statement_type,fiscal_period,report_period_end,published_at,available_at,currency,unit,accounting_standard,source_name,source_url,source_document_id,metrics,source_payload,retrieved_at) VALUES($1,$2,'income_statement','FY','2025-12-31',$3,$3,'USD','millions','US-GAAP','SEC','https://example.com/acme','10-k-2025','{}','{}',$3)`, snapshotID, assetID, asOf.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	revenue, margin, tax, depreciation, capex, nwc, shares := 1000.0, .2, .2, 20.0, 30.0, 5.0, 100.0
	price, benchmark := 20.0, .05
	priceRecord := marketdata.PriceObservation{AssetID: assetID, Market: "US", Currency: "USD", ObservedAt: asOf.Add(-2 * time.Hour), AvailableAt: asOf.Add(-time.Hour), Price: price, PriceField: "adjusted_close", TimePrecision: "timestamped", SourceName: "test-market", SourceDocumentID: "price-series:" + assetID}
	if err = priceRecord.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if _, err = marketdata.NewStore(pool).Save(ctx, priceRecord); err != nil {
		t.Fatal(err)
	}
	evidenceStore := analystevidence.NewStore(pool)
	comparable, _, err := evidenceStore.Create(ctx, workflowEvidenceSubmission(assetID, analystevidence.ValuationMultiple, "workflow-comparable", map[string]any{"selected_multiple": 20.0}, asOf), asOf)
	if err != nil {
		t.Fatal(err)
	}
	benchmarkEvidence, _, err := evidenceStore.Create(ctx, workflowEvidenceSubmission(assetID, analystevidence.BenchmarkExpectation, "workflow-benchmark", map[string]any{"benchmark_id": rating.DefaultUSPolicy().BenchmarkID, "expected_return": benchmark}, asOf), asOf)
	if err != nil {
		t.Fatal(err)
	}
	rationale, _, err := evidenceStore.Create(ctx, workflowEvidenceSubmission(assetID, analystevidence.RatingRationale, "workflow-rationale", map[string]any{"reason_codes": []any{"scheduled_fundamental_review"}}, asOf), asOf)
	if err != nil {
		t.Fatal(err)
	}
	assumptionEvidence, _, err := evidenceStore.Create(ctx, workflowEvidenceSubmission(assetID, analystevidence.ForecastAssumption, "workflow-assumption", map[string]any{"field": "revenue_growth", "value": .1}, asOf), asOf)
	if err != nil {
		t.Fatal(err)
	}
	capitalEvidence, _, err := evidenceStore.Create(ctx, workflowEvidenceSubmission(assetID, analystevidence.CostOfCapital, "workflow-capital-cost", map[string]any{"wacc": .1}, asOf), asOf)
	if err != nil {
		t.Fatal(err)
	}
	invalidationEvidence, _, err := evidenceStore.Create(ctx, workflowEvidenceSubmission(assetID, analystevidence.InvalidationRule, "workflow-invalidation", map[string]any{"rule_type": "revenue_growth"}, asOf), asOf)
	if err != nil {
		t.Fatal(err)
	}
	directForecast := forecast.Submission{AssetID: assetID, AsOf: asOf, Inputs: forecast.Inputs{Currency: "USD", Unit: "millions", Revenue: &revenue, OperatingMargin: &margin, TaxRate: &tax, Depreciation: &depreciation, Capex: &capex, ChangeNWC: &nwc, DilutedShares: &shares}, FundamentalSnapshotIDs: []string{snapshotID}, Assumptions: []forecast.Assumption{{Field: "revenue_growth", Value: .1, EvidenceIDs: []string{assumptionEvidence.ID}, Approved: true}}}
	if err = ValidateForecastSubmissionEvidence(ctx, pool, directForecast); err != nil {
		t.Fatalf("registered direct forecast evidence was rejected: %v", err)
	}
	invalidForecast := directForecast
	invalidForecast.Assumptions = append([]forecast.Assumption{}, directForecast.Assumptions...)
	invalidForecast.Assumptions[0].EvidenceIDs = []string{"arbitrary-string"}
	if err = ValidateForecastSubmissionEvidence(ctx, pool, invalidForecast); err == nil {
		t.Fatal("unregistered direct forecast evidence was accepted")
	}
	directValuation := valuation.Submission{AssetID: assetID, AsOf: asOf, NetDebtSnapshotID: snapshotID, DCFScenarios: []valuation.DCFScenario{{Name: "base-dcf", WACC: .1, TerminalGrowth: .03, ProjectionYears: 5, CostOfCapitalEvidenceIDs: []string{capitalEvidence.ID}}}, MultipleScenarios: []valuation.MultipleScenario{{Name: "base-multiple", PriceEarningsMultiple: 20, ComparableEvidenceIDs: []string{comparable.ID}}}}
	if err = ValidateValuationSubmissionEvidence(ctx, pool, directValuation); err != nil {
		t.Fatalf("registered direct valuation evidence was rejected: %v", err)
	}
	invalidValuation := directValuation
	invalidValuation.MultipleScenarios = append([]valuation.MultipleScenario{}, directValuation.MultipleScenarios...)
	invalidValuation.MultipleScenarios[0].ComparableEvidenceIDs = []string{"arbitrary-string"}
	if err = ValidateValuationSubmissionEvidence(ctx, pool, invalidValuation); err == nil {
		t.Fatal("unregistered direct valuation evidence was accepted")
	}
	directRating := rating.Submission{AssetID: assetID, Policy: rating.DefaultUSPolicy(), AsOfPrice: &price, AsOfPriceEvidenceID: priceRecord.ID, BenchmarkReturn: &benchmark, BenchmarkEvidenceID: benchmarkEvidence.ID, EffectiveAt: asOf, ReasonCodes: []string{"scheduled_fundamental_review"}, EvidenceIDs: []string{snapshotID, rationale.ID}, InvalidationRules: []rating.InvalidationRule{{RuleType: "revenue_growth", Operator: "lt", Threshold: 0, EvidenceIDs: []string{invalidationEvidence.ID}}}}
	if err = ValidateRatingSubmissionEvidence(ctx, pool, directRating); err != nil {
		t.Fatalf("registered direct rating evidence was rejected: %v", err)
	}
	invalidRating := directRating
	invalidRating.EvidenceIDs = []string{snapshotID, "arbitrary-string"}
	if err = ValidateRatingSubmissionEvidence(ctx, pool, invalidRating); err == nil {
		t.Fatal("unregistered direct rating evidence was accepted")
	}
	defaultPolicyRating := directRating
	defaultPolicyRating.Policy = rating.Policy{}
	defaultPolicyRating.BenchmarkEvidenceID = "arbitrary-string"
	if err = ValidateRatingSubmissionEvidence(ctx, pool, defaultPolicyRating); err == nil {
		t.Fatal("default relative policy bypassed benchmark evidence validation")
	}
	input := Input{AssetID: assetID, AsOf: asOf, Forecast: ForecastPlan{Inputs: forecast.Inputs{Currency: "USD", Unit: "millions", Revenue: &revenue, OperatingMargin: &margin, TaxRate: &tax, Depreciation: &depreciation, Capex: &capex, ChangeNWC: &nwc, DilutedShares: &shares}, FundamentalSnapshotIDs: []string{snapshotID}}, Valuation: ValuationPlan{MultipleScenarios: []valuation.MultipleScenario{{Name: "base", PriceEarningsMultiple: 20, ComparableEvidenceIDs: []string{comparable.ID}}}}, Rating: RatingPlan{Policy: rating.DefaultUSPolicy(), AsOfPrice: &price, AsOfPriceEvidenceID: priceRecord.ID, BenchmarkReturn: &benchmark, BenchmarkEvidenceID: benchmarkEvidence.ID, ReasonCodes: []string{"scheduled_fundamental_review"}, EvidenceIDs: []string{snapshotID, rationale.ID}}}
	wrongPrice := price + 1
	wrong := input
	wrong.Rating.AsOfPrice = &wrongPrice
	if _, err = New(pool).Run(ctx, wrong); err == nil || !strings.Contains(err.Error(), "evidence gate") {
		t.Fatalf("mismatched price evidence was accepted: %v", err)
	}
	var forecastsBeforeValidRun int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM forecast_versions WHERE asset_id=$1`, assetID).Scan(&forecastsBeforeValidRun); err != nil || forecastsBeforeValidRun != 0 {
		t.Fatalf("failed evidence gate wrote forecast versions: count=%d err=%v", forecastsBeforeValidRun, err)
	}
	result, err := New(pool).Run(ctx, input)
	if err != nil || result.Status != "available" || result.Forecast.Status != "available" || result.Valuation == nil || result.Valuation.Status != "available" || result.Rating == nil || result.Rating.Result.Rating != "strong_buy" {
		t.Fatalf("workflow=%#v err=%v", result, err)
	}
	if result.ScheduleDraft == nil || result.ScheduleDraft.ForecastVersionID != result.Forecast.ID || result.ScheduleDraft.ApprovedBy != "" {
		t.Fatalf("schedule draft=%#v", result.ScheduleDraft)
	}
	if result.ScheduleDraftControls == nil || !result.ScheduleDraftControls.ApprovalRequired || result.ScheduleDraftControls.AutomaticApproval || result.ScheduleDraftControls.RuntimePriceField != "adjusted_close" {
		t.Fatalf("schedule draft controls=%#v", result.ScheduleDraftControls)
	}
	if result.ScheduleDraft.Rating.BenchmarkEvidenceID != input.Rating.BenchmarkEvidenceID || len(result.ScheduleDraft.Valuation.MultipleScenarios) != 1 {
		t.Fatalf("schedule draft lost governed inputs: %#v", result.ScheduleDraft)
	}
	repeated, err := New(pool).Run(ctx, input)
	if err != nil || repeated.Forecast.ID != result.Forecast.ID || repeated.Valuation == nil || repeated.Valuation.ID != result.Valuation.ID || repeated.Rating == nil || repeated.Rating.Created {
		t.Fatalf("repeated workflow=%#v err=%v", repeated, err)
	}
	missing := input
	missing.AsOf = asOf.Add(time.Hour)
	missing.Forecast.Inputs.Revenue = nil
	insufficient, err := New(pool).Run(ctx, missing)
	if err != nil || insufficient.Status != "insufficient_data" || insufficient.Reason != "missing_required_financial_inputs" || insufficient.Valuation != nil || insufficient.Rating != nil {
		t.Fatalf("insufficient workflow=%#v err=%v", insufficient, err)
	}
	if insufficient.ScheduleDraft != nil {
		t.Fatalf("insufficient workflow produced schedule draft: %#v", insufficient.ScheduleDraft)
	}
	if insufficient.ScheduleDraftControls != nil {
		t.Fatalf("insufficient workflow produced schedule draft controls: %#v", insufficient.ScheduleDraftControls)
	}
	const cryptoID = "crypto:coingecko:bitcoin"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active) VALUES($1,'crypto','CRYPTO','BTC','Bitcoin','coingecko','USD','[]','[]','[]',1,true)`, cryptoID); err != nil {
		t.Fatal(err)
	}
	cryptoInput := input
	cryptoInput.AssetID = cryptoID
	unsupported, err := New(pool).Run(ctx, cryptoInput)
	if err != nil || unsupported.Status != "not_applicable" || unsupported.MarketPolicy.FundamentalSupported || unsupported.Forecast.ID != "" || unsupported.Valuation != nil || unsupported.Rating != nil {
		t.Fatalf("crypto workflow=%#v err=%v", unsupported, err)
	}
}

func workflowEvidenceSubmission(assetID, evidenceType, key string, values map[string]any, cutoff time.Time) analystevidence.Submission {
	return analystevidence.Submission{
		AssetID: assetID, EvidenceType: evidenceType, Title: key, Rationale: "integration test reviewed evidence",
		Values: values, ObservedAt: cutoff.Add(-2 * time.Hour), AvailableAt: cutoff.Add(-time.Hour),
		SourceName: "integration-test", SourceDocumentID: key, SourceURL: "https://example.test/" + key,
		ApprovedBy: "integration-test", IdempotencyKey: key,
	}
}
