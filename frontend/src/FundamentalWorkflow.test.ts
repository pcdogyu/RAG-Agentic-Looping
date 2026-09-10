import { describe, expect, it } from "vitest";

import {
	analystEvidenceBody,
	analystEvidenceTemplate,
	fundamentalScheduleBody,
	fundamentalWorkflowBody,
} from "./FundamentalWorkflow";

const assetID = "equity:XNAS:AAPL";
const now = new Date("2026-09-10T12:00:00Z");

function analystEvidence(overrides: Record<string, unknown> = {}) {
	return JSON.stringify({
		asset_id: assetID,
		evidence_type: "valuation_multiple",
		title: "同业估值样本",
		rationale: "基于同市场同业务口径的可比公司中位数，由分析师复核。",
		values: { selected_multiple: 20 },
		observed_at: "2026-09-09T10:00:00Z",
		available_at: "2026-09-09T11:00:00Z",
		source_name: "Licensed Research",
		source_document_id: "comparable-2026-09-09",
		source_url: "https://user:secret@example.test/report?token=secret#page=2",
		approved_by: "analyst-1",
		...overrides,
	});
}

function workflow(overrides: Record<string, unknown> = {}) {
	return JSON.stringify({
		asset_id: assetID,
		as_of: "2026-09-10T10:00:00Z",
		forecast: {
			inputs: { currency: "USD", unit: "millions", revenue: 1000, operating_margin: 0.2, tax_rate: 0.2, depreciation: 40, capex: 30, change_nwc: 10, diluted_shares: 100 },
			fundamental_snapshot_ids: ["income-1", "balance-1", "cashflow-1"],
			assumptions: [{ field: "revenue_growth", value: 0.1, evidence_ids: ["evidence-assumption"], approved: true }],
		},
		valuation: {
			dcf_scenarios: [],
			multiple_scenarios: [{ name: "base", price_earnings_multiple: 20, comparable_evidence_ids: ["evidence-multiple"] }],
		},
		rating: {
			policy: { version: "fundamental-rating-v1", market: "US", asset_class: "equity", horizon_days: 365, benchmark_id: "equity:AMEX:SPY", relative_required: true, strong_buy_floor: 0.2, buy_floor: 0.05, sell_ceiling: -0.05, strong_sell_ceiling: -0.2 },
			as_of_price: 225,
			as_of_price_evidence_id: "price-1",
			benchmark_return: 0.08,
			benchmark_evidence_id: "evidence-benchmark",
			reason_codes: ["approved_periodic_review"],
			changed_assumptions: { revenue_growth: 0.1 },
			evidence_ids: ["income-1", "evidence-rationale"],
			invalidation_rules: [{ rule_type: "revenue_growth", operator: "lt", threshold: 0, evidence_ids: ["evidence-invalidation"] }],
		},
		...overrides,
	});
}

function schedule(overrides: Record<string, unknown> = {}) {
	const source = JSON.parse(workflow()) as Record<string, unknown>;
	const rating = { ...(source.rating as Record<string, unknown>) };
	delete rating.as_of_price;
	delete rating.as_of_price_evidence_id;
	return JSON.stringify({
		asset_id: assetID,
		forecast_version_id: "forecast-1",
		valuation: source.valuation,
		rating,
		cadence_hours: 24,
		max_price_age_hours: 120,
		max_plan_age_days: 90,
		approved_by: "analyst-1",
		...overrides,
	});
}

describe("guided analyst evidence", () => {
	it("generates typed templates without inventing values or approval", () => {
		const multiple = JSON.parse(analystEvidenceTemplate(assetID, "valuation_multiple"));
		expect(multiple).toMatchObject({ asset_id: assetID, evidence_type: "valuation_multiple", values: { selected_multiple: null }, approved_by: "" });
		const benchmark = JSON.parse(analystEvidenceTemplate(assetID, "benchmark_expectation", "equity:AMEX:SPY"));
		expect(benchmark.values).toEqual({ benchmark_id: "equity:AMEX:SPY", expected_return: null });
	});

	it("normalizes a valid record and strips URL credentials before submission", () => {
		const preview = analystEvidenceBody(assetID, analystEvidence(), now);
		expect(preview.evidenceType).toBe("valuation_multiple");
		expect(preview.valueSummary).toBe("selected_multiple=20");
		expect(preview.body.source_url).toBe("https://example.test/report");
	});

	it("rejects mismatched assets, bad time order, future evidence and invalid typed values", () => {
		expect(() => analystEvidenceBody(assetID, analystEvidence({ asset_id: "equity:XNAS:MSFT" }), now)).toThrow("当前标的一致");
		expect(() => analystEvidenceBody(assetID, analystEvidence({ available_at: "2026-09-09T09:00:00Z" }), now)).toThrow("不能早于");
		expect(() => analystEvidenceBody(assetID, analystEvidence({ available_at: "2026-09-11T09:00:00Z" }), now)).toThrow("不能位于未来");
		expect(() => analystEvidenceBody(assetID, analystEvidence({ values: { selected_multiple: null } }), now)).toThrow("有限数值");
		expect(() => analystEvidenceBody(assetID, analystEvidence({ evidence_type: "cost_of_capital", values: { wacc: 1 } }), now)).toThrow("0 和 1");
		expect(() => analystEvidenceBody(assetID, analystEvidence({ unexpected: true }), now)).toThrow("不受支持字段");
	});
});

describe("guided no-news workflow", () => {
	it("previews complete facts, assumptions, valuation and governed evidence", () => {
		const preview = fundamentalWorkflowBody(assetID, workflow(), now);
		expect(preview).toMatchObject({ snapshotCount: 3, assumptionCount: 1, dcfScenarioCount: 0, multipleScenarioCount: 1 });
		expect(preview.evidenceIDs).toEqual(["evidence-assumption", "evidence-benchmark", "evidence-invalidation", "evidence-multiple", "evidence-rationale", "income-1"]);
		expect(preview.body.asset_id).toBe(assetID);
	});

	it("does not turn missing facts into zero and requires explicitly approved assumptions", () => {
		const missing = JSON.parse(workflow()) as Record<string, unknown>;
		((missing.forecast as Record<string, unknown>).inputs as Record<string, unknown>).revenue = null;
		expect(() => fundamentalWorkflowBody(assetID, JSON.stringify(missing), now)).toThrow("缺失值请保持 null 而不是填 0");
		const unapproved = JSON.parse(workflow()) as Record<string, unknown>;
		(((unapproved.forecast as Record<string, unknown>).assumptions as Array<Record<string, unknown>>)[0]).approved = false;
		expect(() => fundamentalWorkflowBody(assetID, JSON.stringify(unapproved), now)).toThrow("明确为 true");
	});

	it("rejects empty valuation, invalid rating thresholds, future as-of and asset mismatch", () => {
		const emptyValuation = JSON.parse(workflow()) as Record<string, unknown>;
		emptyValuation.valuation = { dcf_scenarios: [], multiple_scenarios: [] };
		expect(() => fundamentalWorkflowBody(assetID, JSON.stringify(emptyValuation), now)).toThrow("至少需要一个");
		const thresholds = JSON.parse(workflow()) as Record<string, unknown>;
		((thresholds.rating as Record<string, unknown>).policy as Record<string, unknown>).buy_floor = 0.3;
		expect(() => fundamentalWorkflowBody(assetID, JSON.stringify(thresholds), now)).toThrow("阈值顺序");
		expect(() => fundamentalWorkflowBody(assetID, workflow({ as_of: "2026-09-11T00:00:00Z" }), now)).toThrow("不能位于未来");
		expect(() => fundamentalWorkflowBody(assetID, workflow({ asset_id: "equity:XNAS:MSFT" }), now)).toThrow("当前标的一致");
		expect(() => fundamentalWorkflowBody(assetID, workflow({ unexpected: true }), now)).toThrow("不受支持字段");
	});
});

describe("guided schedule approval", () => {
	it("previews the same governed valuation and rating with bounded cadence", () => {
		const preview = fundamentalScheduleBody(assetID, schedule());
		expect(preview).toMatchObject({ forecastVersionID: "forecast-1", cadenceHours: 24, maxPriceAgeHours: 120, maxPlanAgeDays: 90, multipleScenarioCount: 1 });
		expect(preview.body.approved_by).toBe("analyst-1");
	});

	it("requires a real approver and refuses out-of-range or cross-asset plans", () => {
		expect(() => fundamentalScheduleBody(assetID, schedule({ approved_by: "" }))).toThrow("approved_by");
		expect(() => fundamentalScheduleBody(assetID, schedule({ cadence_hours: 721 }))).toThrow("1—720");
		expect(() => fundamentalScheduleBody(assetID, schedule({ asset_id: "equity:XNAS:MSFT" }))).toThrow("当前标的一致");
	});
});
