import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { PhaseTwoReadinessPanel, phaseTwoReadinessStatusLabel, type PhaseTwoReadinessReport } from "./PhaseTwoReadiness";

describe("phase two readiness presentation", () => {
	it("keeps engineering, human input, and natural maturity states distinct", () => {
		expect(phaseTwoReadinessStatusLabel("engineering_gap")).toBe("工程缺口");
		expect(phaseTwoReadinessStatusLabel("waiting_human_input")).toBe("等待人工输入");
		expect(phaseTwoReadinessStatusLabel("waiting_natural_maturity")).toBe("等待自然成熟");
	});

	it("renders blocked truth and actionable dependencies without claiming completion", () => {
		const report: PhaseTwoReadinessReport = {
			version: "phase-two-readiness-v1", as_of: "2026-09-10T00:00:00Z", overall_status: "blocked",
			completed_gates: 0, total_blocking_gates: 2, automatic_completion: false,
			facts: { pending_prediction_labels: 66, licensed_benchmark_import_receipts: 0, tradability_import_receipts: 0 },
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
		expect(html).toContain("href=\"#/fundamental\"");
		expect(html).not.toContain("可人工验收");
	});
});
