import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
	PhaseTwoEvaluationWorkbench,
	evaluationPipelinePreview,
	evaluationPipelineTemplate,
	type EvaluationInventory,
} from "./PhaseTwoEvaluationWorkbench";

const now = new Date("2026-06-01T00:00:00Z");

function inventory(): EvaluationInventory {
	return {
		holdouts: [{ id: "holdout-1", asset_class: "equity", market: "US", objective: "absolute_up", horizon_sessions: 5,
			signal_start: "2026-02-01T00:00:00Z", signal_end: "2026-03-01T00:00:00Z", label_cutoff: "2026-04-01T00:00:00Z", approved_by: "reviewer", created_at: "2026-01-01T00:00:00Z" }],
		datasets: [{ manifest: { id: "dataset-1", manifest_digest: "dataset-digest", config: { holdout_reservation_id: "holdout-1", development_start: "2024-01-01T00:00:00Z", development_end: "2025-12-01T00:00:00Z" },
			final_holdout: { reservation_id: "holdout-1", label_cutoff: "2026-04-01T00:00:00Z", included_count: 120, excluded_count: 4 }, folds: [{ index: 0 }, { index: 1 }] },
			asset_class: "equity", market: "US", objective: "absolute_up", horizon_sessions: 5, created_by: "builder", created_at: "2026-04-02T00:00:00Z" }],
		experiments: [{ experiment: { id: "experiment-1", dataset_id: "dataset-1", artifact_digest: "experiment-digest", final_holdout_accessed: false,
			folds: [{ index: 0, variants: [] }, { index: 1, variants: [{ name: "logistic_full", kind: "interpretable_logistic", status: "evaluated", feature_names: ["signal"], artifact_digest: "variant-digest", model: { version: "model-1" }, metrics: { sample_count: 80, accuracy: .62 } }] }] },
			created_by: "experimenter", created_at: "2026-04-03T00:00:00Z" }],
		performanceReports: [{ report: { id: "performance-1", dataset_id: "dataset-1", experiment_id: "experiment-1", artifact_digest: "performance-digest", final_holdout_accessed: false, selection_decision: "no_automatic_model_selection" }, created_by: "reporter", created_at: "2026-04-04T00:00:00Z" }],
		finalReports: [],
	};
}

describe("phase two evaluation pipeline prevalidation", () => {
	it("keeps generated holdout and final drafts incomplete until a human supplies decisions", () => {
		const data = inventory();
		const holdout = JSON.parse(evaluationPipelineTemplate("holdout", data));
		expect(holdout.market).toBe("");
		expect(holdout.horizon_sessions).toBe(0);
		expect(holdout.approved_by).toBe("");
		const final = JSON.parse(evaluationPipelineTemplate("final", data));
		expect(final.dataset_id).toBe("dataset-1");
		expect(final.fold_index).toBe(1);
		expect(final.variant_name).toBe("");
		expect(final.variant_artifact_digest).toBe("");
		expect(final.approved_by).toBe("");
	});

	it("accepts only a genuinely future pre-registration window", () => {
		const raw = JSON.stringify({ asset_class: "equity", market: "us", objective: "absolute_up", horizon_sessions: 5,
			signal_start: "2026-02-01T00:00:00Z", signal_end: "2026-03-01T00:00:00Z", label_cutoff: "2026-04-01T00:00:00Z", approved_by: " reviewer " });
		const preview = evaluationPipelinePreview("holdout", raw, inventory(), new Date("2026-01-01T00:00:00Z"));
		expect(preview.body.market).toBe("US");
		expect(preview.body.approved_by).toBe("reviewer");
		expect(() => evaluationPipelinePreview("holdout", raw, inventory(), now)).toThrow("不能事后预注册");
		expect(() => evaluationPipelinePreview("holdout", JSON.stringify({ ...JSON.parse(raw), invented: true }), inventory(), new Date("2026-01-01T00:00:00Z"))).toThrow("未知字段");
	});

	it("checks dataset maturity, inventory identity, rolling windows and embargo", () => {
		const body = { holdout_reservation_id: "holdout-1", available_as_of: "2026-04-02T00:00:00Z", development_start: "2024-01-01T00:00:00Z", development_end: "2025-12-01T00:00:00Z",
			train_window_days: 180, calibration_window_days: 60, test_window_days: 30, step_days: 30, embargo_days: 5, created_by: "builder" };
		const preview = evaluationPipelinePreview("dataset", JSON.stringify(body), inventory(), now);
		expect(preview.body.holdout_reservation_id).toBe("holdout-1");
		expect(() => evaluationPipelinePreview("dataset", JSON.stringify({ ...body, holdout_reservation_id: "holdout-missing" }), inventory(), now)).toThrow("当前服务器快照");
		expect(() => evaluationPipelinePreview("dataset", JSON.stringify({ ...body, available_as_of: "2026-03-01T00:00:00Z" }), inventory(), now)).toThrow("尚未达到");
		expect(() => evaluationPipelinePreview("dataset", JSON.stringify({ ...body, development_end: "2026-01-30T00:00:00Z", embargo_days: 5 }), inventory(), now)).toThrow("重叠");
	});

	it("locks the exact latest-fold artifact and refuses a second final evaluation", () => {
		const data = inventory();
		const body = { dataset_id: "dataset-1", experiment_id: "experiment-1", development_report_id: "performance-1", fold_index: 1,
			variant_name: "logistic_full", variant_artifact_digest: "variant-digest", approved_by: "independent reviewer", approval_reason: "locked after development report" };
		const preview = evaluationPipelinePreview("final", JSON.stringify(body), data, now);
		expect(preview.summary).toContain("120 个密封样本");
		expect(preview.body.variant_artifact_digest).toBe("variant-digest");
		expect(() => evaluationPipelinePreview("final", JSON.stringify({ ...body, variant_artifact_digest: "changed" }), data, now)).toThrow("精确匹配");
		expect(() => evaluationPipelinePreview("final", JSON.stringify({ ...body, fold_index: 0 }), data, now)).toThrow("最新开发折");
		const alreadyEvaluated = { ...data, finalReports: [{ id: "final-1", dataset_id: "dataset-1", holdout_reservation_id: "holdout-1", experiment_id: "experiment-1", development_report_id: "performance-1", variant_name: "logistic_full", status: "evaluated", sealed_sample_count: 120, evaluated_sample_count: 120, sample_ids_shown: false, automatic_model_selection: false, evaluated_at: "2026-05-01T00:00:00Z" }] };
		expect(() => evaluationPipelinePreview("final", JSON.stringify(body), alreadyEvaluated, now)).toThrow("已经执行过一次");
	});

	it("renders all five manual stages and the irreversible confirmation language", () => {
		const html = renderToStaticMarkup(createElement(PhaseTwoEvaluationWorkbench, { apiBase: "", token: "" }));
		expect(html).toContain("最终留出人工操作台");
		expect(html).toContain("1. 预注册未来留出");
		expect(html).toContain("5. 执行一次最终评估");
		expect(html).toContain("每一步独立确认");
		expect(html).toContain("不会自动选模或连锁执行");
		expect(html).toContain("当前生产库没有评估流水线记录");
	});
});
