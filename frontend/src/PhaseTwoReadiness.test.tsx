import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { isOutcomeEvaluationTerminalState, PhaseTwoReadinessPanel, phaseTwoOutcomeEvaluationStatusLabel, phaseTwoPendingReasonLabel, phaseTwoReadinessStatusLabel, type PhaseTwoReadinessReport } from "./PhaseTwoReadiness";

describe("phase two readiness presentation", () => {
	it("keeps engineering, human input, and natural maturity states distinct", () => {
		expect(phaseTwoReadinessStatusLabel("engineering_gap")).toBe("工程缺口");
		expect(phaseTwoReadinessStatusLabel("waiting_human_input")).toBe("等待人工输入");
		expect(phaseTwoReadinessStatusLabel("waiting_natural_maturity")).toBe("等待自然成熟");
		expect(phaseTwoPendingReasonLabel("awaiting_price_sessions")).toBe("等待足够交易日复权价格");
	});

	it("recognizes only final outcome evaluation task states", () => {
		expect(isOutcomeEvaluationTerminalState("completed")).toBe(true);
		expect(isOutcomeEvaluationTerminalState(" FAILED ")).toBe(true);
		expect(isOutcomeEvaluationTerminalState("cancelled")).toBe(true);
		expect(isOutcomeEvaluationTerminalState("running")).toBe(false);
		expect(isOutcomeEvaluationTerminalState("pending")).toBe(false);
		expect(phaseTwoOutcomeEvaluationStatusLabel("completed_with_warnings")).toBe("已完成（有警告）");
	});

	it("separates prediction labels from safe legacy outcome warnings", () => {
		const report: PhaseTwoReadinessReport = {
			version: "phase-two-readiness-v1", as_of: "2026-09-10T00:00:00Z", overall_status: "blocked",
			completed_gates: 0, total_blocking_gates: 0, automatic_completion: false,
			facts: {
				pending_prediction_labels: 111, licensed_benchmark_import_receipts: 0, tradability_import_receipts: 0,
				latest_outcome_evaluation: { job_id: "job-warning", status: "completed", summary_status: "completed_with_warnings", selected: 111, matured: 0, pending: 111, unavailable: 0, excluded: 0, failed: 0, pending_reasons: { awaiting_price_sessions: 111 }, warning_count: 10, legacy_recommendation_outcomes: { created: 0, pending: 28, skipped: 499, failed: 10 } },
			},
			gates: [],
		};
		const html = renderToStaticMarkup(createElement(PhaseTwoReadinessPanel, { report }));
		expect(html).toContain("已完成（有警告）");
		expect(html).toContain("同任务存在 10 条分支级警告");
		expect(html).toContain("预测标签失败 0；历史推荐结果失败 10");
		expect(html).toContain("原始失败文本和内部地址不在此页面返回或展示");
	});

	it("renders blocked truth and actionable dependencies without claiming completion", () => {
		const report: PhaseTwoReadinessReport = {
			version: "phase-two-readiness-v1", as_of: "2026-09-10T00:00:00Z", overall_status: "blocked",
			completed_gates: 0, total_blocking_gates: 2, automatic_completion: false,
			facts: {
				pending_prediction_labels: 66, licensed_benchmark_import_receipts: 0, tradability_import_receipts: 0,
				latest_outcome_evaluation: { job_id: "job-1", status: "completed", created_at: "2026-09-10T00:00:00Z", completed_at: "2026-09-10T00:01:00Z", selected: 66, matured: 0, pending: 66, unavailable: 0, excluded: 0, failed: 0, pending_reasons: { awaiting_price_sessions: 66 } },
			},
			gates: [
				{ id: "analyst_evidence", stage: "M1", title: "真实分析师证据", status: "waiting_human_input", current: 0, required: 1, unit: "条", blocking: true, external_input: true, dependencies: [], action: "登记真实证据", route: "/fundamental", authority: "database_or_server_configuration" },
				{ id: "final_holdout_evaluation", stage: "M5", title: "一次性最终留出评估", status: "engineering_gap", current: 0, required: 1, unit: "项", blocking: true, external_input: false, dependencies: ["layered_performance_report"], action: "实现一次性评估", authority: "database_or_server_configuration" },
			],
		};
		const html = renderToStaticMarkup(createElement(PhaseTwoReadinessPanel, { report }));
		expect(html).toContain("尚未完成");
		expect(html).toContain("等待人工输入");
		expect(html).toContain("工程缺口");
		expect(html).toContain("只能等待真实交易日经过");
		expect(html).toContain("最近一次真实结果评估");
		expect(html).toContain("等待足够交易日复权价格");
		expect(html).toContain("awaiting_price_sessions");
		expect(html).toContain("href=\"#/fundamental\"");
		expect(html).not.toContain("可人工验收");
	});

	it("renders a guarded manual maturity recheck action and progress state", () => {
		const report: PhaseTwoReadinessReport = {
			version: "phase-two-readiness-v1", as_of: "2026-09-10T00:00:00Z", overall_status: "blocked",
			completed_gates: 0, total_blocking_gates: 0, automatic_completion: false,
			facts: { pending_prediction_labels: 1, licensed_benchmark_import_receipts: 0, tradability_import_receipts: 0 },
			gates: [],
		};
		const html = renderToStaticMarkup(createElement(PhaseTwoReadinessPanel, { report, onEvaluateOutcomes: () => undefined, outcomeEvaluationBusy: true, outcomeEvaluationMessage: "任务 job-1 已入队" }));
		expect(html).toContain("检查中…");
		expect(html).toContain("disabled");
		expect(html).toContain("不允许提前成熟");
		expect(html).toContain("复用同一任务 ID");
		expect(html).toContain("任务 job-1 已入队");
	});

	it("keeps an older API response readable while the additive field rolls out", () => {
		const report: PhaseTwoReadinessReport = {
			version: "phase-two-readiness-v1", as_of: "2026-09-10T00:00:00Z", overall_status: "blocked",
			completed_gates: 0, total_blocking_gates: 0, automatic_completion: false,
			facts: { pending_prediction_labels: 0, licensed_benchmark_import_receipts: 0, tradability_import_receipts: 0 },
			gates: [],
		};
		const html = renderToStaticMarkup(createElement(PhaseTwoReadinessPanel, { report }));
		expect(html).toContain("最近一次真实结果评估");
		expect(html).toContain("尚未运行");
	});
});
