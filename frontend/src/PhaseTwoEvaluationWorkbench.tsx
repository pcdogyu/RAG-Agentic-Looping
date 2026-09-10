import { useCallback, useEffect, useMemo, useState } from "react";

export type EvaluationPipelineStage = "holdout" | "dataset" | "experiment" | "performance" | "final";

export type EvaluationHoldout = {
	id: string;
	asset_class: string;
	market: string;
	objective: string;
	horizon_sessions: number;
	signal_start: string;
	signal_end: string;
	label_cutoff: string;
	approved_by: string;
	created_at: string;
};

export type EvaluationDataset = {
	manifest: {
		id: string;
		manifest_digest: string;
		config: { holdout_reservation_id: string; development_start: string; development_end: string };
		final_holdout: { reservation_id: string; label_cutoff: string; included_count: number; excluded_count: number };
		folds: Array<{ index: number }>;
	};
	asset_class: string;
	market: string;
	objective: string;
	horizon_sessions: number;
	created_by: string;
	created_at: string;
};

export type EvaluationVariant = {
	name: string;
	kind: string;
	status: string;
	feature_names: string[];
	artifact_digest: string;
	model?: { version: string };
	calibrator?: { version: string };
	metrics: { sample_count: number; accuracy?: number; probability?: { brier_score: number } };
};

export type EvaluationExperiment = {
	experiment: {
		id: string;
		dataset_id: string;
		artifact_digest: string;
		final_holdout_accessed: boolean;
		folds: Array<{ index: number; variants: EvaluationVariant[] }>;
	};
	created_by: string;
	created_at: string;
};

export type EvaluationPerformanceReport = {
	report: {
		id: string;
		dataset_id: string;
		experiment_id: string;
		artifact_digest: string;
		final_holdout_accessed: boolean;
		selection_decision: string;
	};
	created_by: string;
	created_at: string;
};

export type FinalHoldoutReport = {
	id: string;
	dataset_id: string;
	holdout_reservation_id: string;
	experiment_id: string;
	development_report_id: string;
	variant_name: string;
	status: string;
	sealed_sample_count: number;
	evaluated_sample_count: number;
	sample_ids_shown: boolean;
	automatic_model_selection: boolean;
	evaluated_at: string;
};

export type EvaluationInventory = {
	holdouts: EvaluationHoldout[];
	datasets: EvaluationDataset[];
	experiments: EvaluationExperiment[];
	performanceReports: EvaluationPerformanceReport[];
	finalReports: FinalHoldoutReport[];
};

export type EvaluationPipelinePreview = {
	stage: EvaluationPipelineStage;
	body: Record<string, unknown>;
	summary: string;
	irreversible: boolean;
};

const stages: Array<{ id: EvaluationPipelineStage; label: string; endpoint: string; responseKey: string }> = [
	{ id: "holdout", label: "1. 预注册未来留出", endpoint: "/go/evaluation-holdouts", responseKey: "reservation" },
	{ id: "dataset", label: "2. 构建滚动数据集", endpoint: "/go/evaluation-datasets", responseKey: "dataset" },
	{ id: "experiment", label: "3. 运行开发实验", endpoint: "/go/evaluation-experiments", responseKey: "experiment" },
	{ id: "performance", label: "4. 生成开发报告", endpoint: "/go/evaluation-performance-reports", responseKey: "performance_report" },
	{ id: "final", label: "5. 执行一次最终评估", endpoint: "/go/evaluation-final-holdouts", responseKey: "final_holdout_evaluation" },
];

const emptyInventory: EvaluationInventory = { holdouts: [], datasets: [], experiments: [], performanceReports: [], finalReports: [] };

function objectValue(raw: string) {
	const value = JSON.parse(raw) as unknown;
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("必须是一个 JSON 对象");
	return value as Record<string, unknown>;
}

function exactKeys(value: Record<string, unknown>, allowed: string[]) {
	const unexpected = Object.keys(value).filter((key) => !allowed.includes(key));
	if (unexpected.length) throw new Error(`存在未知字段：${unexpected.join("、")}`);
}

function textField(value: Record<string, unknown>, key: string) {
	const result = typeof value[key] === "string" ? value[key].trim() : "";
	if (!result) throw new Error(`${key} 必填`);
	return result;
}

function integerField(value: Record<string, unknown>, key: string, minimum: number) {
	const result = value[key];
	if (!Number.isInteger(result) || (result as number) < minimum) throw new Error(`${key} 必须是不小于 ${minimum} 的整数`);
	return result as number;
}

function instantField(value: Record<string, unknown>, key: string) {
	const raw = textField(value, key);
	if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(raw)) throw new Error(`${key} 必须是带时区的 RFC3339 时间`);
	const date = new Date(raw);
	if (!Number.isFinite(date.getTime())) throw new Error(`${key} 时间无效`);
	return date;
}

function datasetID(item: EvaluationDataset) { return item.manifest.id; }
function experimentID(item: EvaluationExperiment) { return item.experiment.id; }
function performanceID(item: EvaluationPerformanceReport) { return item.report.id; }

export function evaluationPipelineTemplate(stage: EvaluationPipelineStage, inventory: EvaluationInventory) {
	const latestHoldout = inventory.holdouts[0];
	const latestDataset = inventory.datasets[0];
	const latestExperiment = inventory.experiments[0];
	const latestPerformance = inventory.performanceReports[0];
	switch (stage) {
	case "holdout":
		return JSON.stringify({ asset_class: "equity", market: "", objective: "absolute_up", horizon_sessions: 0, signal_start: "", signal_end: "", label_cutoff: "", approved_by: "" }, null, 2);
	case "dataset":
		return JSON.stringify({ holdout_reservation_id: latestHoldout?.id || "", available_as_of: "", development_start: "", development_end: "", train_window_days: 0, calibration_window_days: 0, test_window_days: 0, step_days: 0, embargo_days: 0, created_by: "" }, null, 2);
	case "experiment":
		return JSON.stringify({ dataset_id: latestDataset ? datasetID(latestDataset) : "", created_by: "" }, null, 2);
	case "performance":
		return JSON.stringify({ experiment_id: latestExperiment ? experimentID(latestExperiment) : "", created_by: "" }, null, 2);
	case "final": {
		const report = latestPerformance?.report;
		const experiment = inventory.experiments.find((item) => experimentID(item) === report?.experiment_id)?.experiment;
		const foldIndex = experiment?.folds.reduce((maximum, fold) => Math.max(maximum, fold.index), -1) ?? -1;
		return JSON.stringify({ dataset_id: report?.dataset_id || "", experiment_id: report?.experiment_id || "", development_report_id: report?.id || "", fold_index: foldIndex,
			variant_name: "", variant_artifact_digest: "", approved_by: "", approval_reason: "" }, null, 2);
	}
	}
}

export function evaluationPipelinePreview(stage: EvaluationPipelineStage, raw: string, inventory: EvaluationInventory, now = new Date()): EvaluationPipelinePreview {
	const value = objectValue(raw);
	if (stage === "holdout") {
		exactKeys(value, ["asset_class", "market", "objective", "horizon_sessions", "signal_start", "signal_end", "label_cutoff", "approved_by"]);
		const assetClass = textField(value, "asset_class").toLowerCase();
		const market = textField(value, "market").toUpperCase();
		const objective = textField(value, "objective").toLowerCase();
		const horizon = integerField(value, "horizon_sessions", 1);
		const start = instantField(value, "signal_start"), end = instantField(value, "signal_end"), cutoff = instantField(value, "label_cutoff");
		if (assetClass !== "equity") throw new Error("asset_class 目前只允许 equity");
		if (!(["absolute_up", "excess_up"].includes(objective))) throw new Error("objective 只允许 absolute_up 或 excess_up");
		if (![1, 5, 20].includes(horizon)) throw new Error("horizon_sessions 只允许 1、5 或 20");
		if (start.getTime() <= now.getTime()) throw new Error("signal_start 必须晚于当前时间，不能事后预注册");
		if (!(start < end && end < cutoff)) throw new Error("必须满足 signal_start < signal_end < label_cutoff");
		const approvedBy = textField(value, "approved_by");
		return { stage, irreversible: true, summary: `${market} · ${objective} · ${horizon} 日 · ${start.toISOString()} 至 ${end.toISOString()}，审批人 ${approvedBy}`,
			body: { asset_class: assetClass, market, objective, horizon_sessions: horizon, signal_start: start.toISOString(), signal_end: end.toISOString(), label_cutoff: cutoff.toISOString(), approved_by: approvedBy } };
	}
	if (stage === "dataset") {
		exactKeys(value, ["holdout_reservation_id", "available_as_of", "development_start", "development_end", "train_window_days", "calibration_window_days", "test_window_days", "step_days", "embargo_days", "created_by"]);
		const holdoutID = textField(value, "holdout_reservation_id");
		const holdout = inventory.holdouts.find((item) => item.id === holdoutID);
		if (!holdout) throw new Error("holdout_reservation_id 不在当前服务器快照中");
		const available = instantField(value, "available_as_of"), developmentStart = instantField(value, "development_start"), developmentEnd = instantField(value, "development_end");
		const train = integerField(value, "train_window_days", 1), calibration = integerField(value, "calibration_window_days", 1);
		const test = integerField(value, "test_window_days", 1), step = integerField(value, "step_days", 1), embargo = integerField(value, "embargo_days", 0);
		if (available > now) throw new Error("available_as_of 不能位于未来");
		if (available < new Date(holdout.label_cutoff)) throw new Error("available_as_of 尚未达到预注册标签截止");
		if (!(developmentStart < developmentEnd)) throw new Error("development_start 必须早于 development_end");
		if (developmentStart.getTime() + (train + calibration + test + 2 * embargo) * 86400000 > developmentEnd.getTime()) throw new Error("开发期不足以形成一个完整的训练、校准和未来测试折");
		if (developmentEnd.getTime() + embargo * 86400000 > new Date(holdout.signal_start).getTime()) throw new Error("开发期及 embargo 与最终留出信号期重叠");
		const createdBy = textField(value, "created_by");
		return { stage, irreversible: true, summary: `${holdoutID} · 训练/校准/测试 ${train}/${calibration}/${test} 天 · 步长 ${step} 天 · embargo ${embargo} 天`,
			body: { holdout_reservation_id: holdoutID, available_as_of: available.toISOString(), development_start: developmentStart.toISOString(), development_end: developmentEnd.toISOString(), train_window_days: train, calibration_window_days: calibration, test_window_days: test, step_days: step, embargo_days: embargo, created_by: createdBy } };
	}
	if (stage === "experiment") {
		exactKeys(value, ["dataset_id", "created_by"]);
		const id = textField(value, "dataset_id");
		if (!inventory.datasets.some((item) => datasetID(item) === id)) throw new Error("dataset_id 不在当前服务器快照中");
		const createdBy = textField(value, "created_by");
		return { stage, irreversible: true, summary: `${id} · 开发折基线、消融和独立校准 · 创建人 ${createdBy}`, body: { dataset_id: id, created_by: createdBy } };
	}
	if (stage === "performance") {
		exactKeys(value, ["experiment_id", "created_by"]);
		const id = textField(value, "experiment_id");
		if (!inventory.experiments.some((item) => experimentID(item) === id)) throw new Error("experiment_id 不在当前服务器快照中");
		const createdBy = textField(value, "created_by");
		return { stage, irreversible: true, summary: `${id} · 仅开发集分层报告 · 创建人 ${createdBy}`, body: { experiment_id: id, created_by: createdBy } };
	}
	exactKeys(value, ["dataset_id", "experiment_id", "development_report_id", "fold_index", "variant_name", "variant_artifact_digest", "approved_by", "approval_reason"]);
	const dataset = inventory.datasets.find((item) => datasetID(item) === textField(value, "dataset_id"));
	const experiment = inventory.experiments.find((item) => experimentID(item) === textField(value, "experiment_id"));
	const development = inventory.performanceReports.find((item) => performanceID(item) === textField(value, "development_report_id"));
	if (!dataset || !experiment || !development) throw new Error("数据集、实验或开发报告不在当前服务器快照中");
	if (experiment.experiment.dataset_id !== dataset.manifest.id || development.report.dataset_id !== dataset.manifest.id || development.report.experiment_id !== experiment.experiment.id) throw new Error("数据集、实验和开发报告不属于同一条链路");
	if (development.report.final_holdout_accessed || development.report.selection_decision !== "no_automatic_model_selection") throw new Error("开发报告必须未访问最终留出且未自动选模");
	if (new Date(dataset.manifest.final_holdout.label_cutoff) > now) throw new Error("最终留出尚未达到预注册标签截止");
	if (inventory.finalReports.some((item) => item.holdout_reservation_id === dataset.manifest.final_holdout.reservation_id)) throw new Error("该预注册最终留出已经执行过一次评估");
	const foldIndex = integerField(value, "fold_index", 0);
	const latest = experiment.experiment.folds.reduce((selected, fold) => !selected || fold.index > selected.index ? fold : selected, undefined as { index: number; variants: EvaluationVariant[] } | undefined);
	if (!latest || latest.index !== foldIndex) throw new Error("fold_index 必须是最新开发折");
	const variantName = textField(value, "variant_name"), digest = textField(value, "variant_artifact_digest");
	const variant = latest.variants.find((item) => item.name === variantName);
	if (!variant || variant.artifact_digest !== digest) throw new Error("变体名称和制品摘要必须精确匹配冻结实验");
	if (!["evaluated", "evaluated_uncalibrated"].includes(variant.status)) throw new Error("所选变体不可评估");
	if (!variant.model && !(["simple_rule", "llm_direction_score"].includes(variant.kind) && variant.feature_names.length === 1)) throw new Error("所选变体没有可重放的冻结评分制品");
	const approvedBy = textField(value, "approved_by"), reason = textField(value, "approval_reason");
	return { stage, irreversible: true, summary: `${dataset.manifest.id} · 最新折 ${foldIndex} · 人工锁定 ${variantName} · ${dataset.manifest.final_holdout.included_count} 个密封样本`,
		body: { dataset_id: dataset.manifest.id, experiment_id: experiment.experiment.id, development_report_id: development.report.id, fold_index: foldIndex,
			variant_name: variantName, variant_artifact_digest: digest, approved_by: approvedBy, approval_reason: reason } };
}

function newRequestID(stage: EvaluationPipelineStage) {
	return globalThis.crypto?.randomUUID?.() || `${stage}-${Date.now()}`;
}

function itemID(stage: EvaluationPipelineStage, item: unknown) {
	const record = item as Record<string, unknown>;
	if (stage === "dataset") return ((record.manifest as Record<string, unknown> | undefined)?.id as string) || "已创建";
	if (stage === "experiment") return ((record.experiment as Record<string, unknown> | undefined)?.id as string) || "已创建";
	if (stage === "performance") return ((record.report as Record<string, unknown> | undefined)?.id as string) || "已创建";
	return (record.id as string) || "已创建";
}

export function PhaseTwoEvaluationWorkbench({ apiBase, token, onChanged }: { apiBase: string; token: string; onChanged?: () => void }) {
	const [inventory, setInventory] = useState<EvaluationInventory>(emptyInventory);
	const [stage, setStage] = useState<EvaluationPipelineStage>("holdout");
	const [draft, setDraft] = useState("");
	const [preview, setPreview] = useState<EvaluationPipelinePreview>();
	const [confirmed, setConfirmed] = useState(false);
	const [finalPhrase, setFinalPhrase] = useState("");
	const [requestID, setRequestID] = useState(() => newRequestID("holdout"));
	const [loading, setLoading] = useState(false);
	const [message, setMessage] = useState("先刷新服务器库存；每一步都必须单独预校验和确认，不会自动串联执行。");
	const load = useCallback(async () => {
		setPreview(undefined); setConfirmed(false); setFinalPhrase("");
		if (!token) { setInventory(emptyInventory); return; }
		setLoading(true);
		try {
			const paths = ["evaluation-holdouts", "evaluation-datasets", "evaluation-experiments", "evaluation-performance-reports", "evaluation-final-holdouts"];
			const responses = await Promise.all(paths.map((path) => fetch(`${apiBase}/go/${path}?limit=50`, { headers: { "X-Admin-Token": token } })));
			const bodies = await Promise.all(responses.map((response) => response.json().catch(() => ({}))));
			const failed = responses.findIndex((response) => !response.ok);
			if (failed >= 0) throw new Error((bodies[failed] as { detail?: string }).detail || `HTTP ${responses[failed].status}`);
			setInventory({ holdouts: bodies[0].items || [], datasets: bodies[1].items || [], experiments: bodies[2].items || [], performanceReports: bodies[3].items || [], finalReports: bodies[4].items || [] });
			setMessage("已读取五段服务器库存；选择阶段并生成草稿，生成草稿不会发送请求。");
		} catch (reason) {
			setInventory(emptyInventory);
			setMessage(`库存读取失败：${reason instanceof Error ? reason.message : "未知错误"}`);
		} finally { setLoading(false); }
	}, [apiBase, token]);
	useEffect(() => { void load(); }, [load]);
	const selectedStage = stages.find((item) => item.id === stage)!;
	const latestVariants = useMemo(() => {
		if (stage !== "final") return [];
		try {
			const body = objectValue(draft);
			const experiment = inventory.experiments.find((item) => experimentID(item) === body.experiment_id)?.experiment;
			const latest = experiment?.folds.reduce((selected, fold) => !selected || fold.index > selected.index ? fold : selected, undefined as { index: number; variants: EvaluationVariant[] } | undefined);
			return latest?.variants.filter((variant) => ["evaluated", "evaluated_uncalibrated"].includes(variant.status) &&
				(Boolean(variant.model) || (["simple_rule", "llm_direction_score"].includes(variant.kind) && variant.feature_names.length === 1))) || [];
		} catch { return []; }
	}, [draft, inventory.experiments, stage]);
	function resetForEdit(nextDraft = draft) {
		setDraft(nextDraft); setPreview(undefined); setConfirmed(false); setFinalPhrase(""); setRequestID(newRequestID(stage));
	}
	function changeStage(next: EvaluationPipelineStage) {
		setStage(next); setDraft(""); setPreview(undefined); setConfirmed(false); setFinalPhrase(""); setRequestID(newRequestID(next));
	}
	function generateTemplate() { resetForEdit(evaluationPipelineTemplate(stage, inventory)); setMessage("草稿已在浏览器生成，尚未发送；请补全并核对真实输入。"); }
	function validate() {
		try { const result = evaluationPipelinePreview(stage, draft, inventory); setPreview(result); setConfirmed(false); setFinalPhrase(""); setMessage(`预校验通过：${result.summary}。尚未提交。`); }
		catch (reason) { setPreview(undefined); setConfirmed(false); setFinalPhrase(""); setMessage(`预校验失败：${reason instanceof Error ? reason.message : "JSON 无效"}`); }
	}
	function useVariant(variant: EvaluationVariant) {
		try {
			const body = objectValue(draft); body.variant_name = variant.name; body.variant_artifact_digest = variant.artifact_digest;
			resetForEdit(JSON.stringify(body, null, 2)); setMessage(`已由人工选择 ${variant.name} 并写入精确制品摘要；仍需重新预校验和确认。`);
		} catch { setMessage("请先生成有效的最终评估草稿。"); }
	}
	async function submit() {
		if (!preview || !confirmed || preview.stage !== stage || (stage === "final" && finalPhrase !== "执行一次最终评估")) return;
		setLoading(true);
		try {
			const response = await fetch(`${apiBase}${selectedStage.endpoint}`, { method: "POST", headers: { "Content-Type": "application/json", "X-Admin-Token": token, "Idempotency-Key": requestID }, body: JSON.stringify(preview.body) });
			const payload = await response.json().catch(() => ({})) as Record<string, unknown> & { detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			setMessage(`${selectedStage.label}已写入：${itemID(stage, payload[selectedStage.responseKey])}。不会自动执行下一阶段。`);
			setPreview(undefined); setConfirmed(false); setFinalPhrase(""); setDraft(""); setRequestID(newRequestID(stage));
			await load(); onChanged?.();
		} catch (reason) { setMessage(`提交失败：${reason instanceof Error ? reason.message : "未知错误"}`); }
		finally { setLoading(false); }
	}
	return <section className="evaluation-workbench">
		<header><div><span>MANUAL EVALUATION PIPELINE</span><h2>最终留出人工操作台</h2><p>预注册 → 数据集 → 开发实验 → 开发报告 → 一次最终评估。每一步独立确认，系统不会自动选模或连锁执行；最终一步还必须输入确认短语。</p></div><button type="button" disabled={!token || loading} onClick={() => void load()}>{loading ? "处理中…" : "刷新五段库存"}</button></header>
		<div className="evaluation-inventory">
			<article><span>预注册留出</span><strong>{inventory.holdouts.length}</strong><small>{inventory.holdouts[0]?.id || "尚无真实记录"}</small></article>
			<article><span>滚动数据集</span><strong>{inventory.datasets.length}</strong><small>{inventory.datasets[0] ? datasetID(inventory.datasets[0]) : "依赖成熟标签"}</small></article>
			<article><span>开发实验</span><strong>{inventory.experiments.length}</strong><small>{inventory.experiments[0] ? experimentID(inventory.experiments[0]) : "依赖数据集"}</small></article>
			<article><span>开发报告</span><strong>{inventory.performanceReports.length}</strong><small>{inventory.performanceReports[0] ? performanceID(inventory.performanceReports[0]) : "依赖实验"}</small></article>
			<article><span>最终报告</span><strong>{inventory.finalReports.length}</strong><small>{inventory.finalReports[0]?.id || "同一留出仅一次"}</small></article>
		</div>
		<div className="evaluation-editor">
			<label>当前人工阶段<select aria-label="评估流水线阶段" value={stage} onChange={(event) => changeStage(event.target.value as EvaluationPipelineStage)}>{stages.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}</select></label>
			<div className="evaluation-actions"><button type="button" disabled={!token || loading} onClick={generateTemplate}>生成当前阶段草稿</button><button type="button" disabled={!token || loading || !draft.trim()} onClick={validate}>预校验当前阶段</button></div>
			<label>精确 API 请求 JSON<textarea aria-label="评估流水线 JSON" rows={14} value={draft} onChange={(event) => resetForEdit(event.target.value)} placeholder="先刷新库存并生成草稿；空值必须由真实操作员填写。" /></label>
			{stage === "final" && latestVariants.length > 0 && <div className="evaluation-variants"><strong>最新开发折的可评估变体（必须人工选择）</strong>{latestVariants.map((variant) => <button type="button" key={`${variant.name}:${variant.artifact_digest}`} onClick={() => useVariant(variant)}>{variant.name} · {variant.status} · n={variant.metrics.sample_count}</button>)}</div>}
			{preview && <div className="evaluation-preview"><strong>预校验通过</strong><span>{preview.summary}</span><small>幂等键：{requestID}</small><label><input type="checkbox" checked={confirmed} onChange={(event) => setConfirmed(event.target.checked)} />我已复核真实输入，并确认写入不可变记录</label>{stage === "final" && <label>最终确认短语<input aria-label="最终评估确认短语" value={finalPhrase} onChange={(event) => setFinalPhrase(event.target.value)} placeholder="执行一次最终评估" /></label>}<button type="button" disabled={loading || !confirmed || (stage === "final" && finalPhrase !== "执行一次最终评估")} onClick={() => void submit()}>{stage === "final" ? "执行且仅执行一次最终评估" : `确认${selectedStage.label}`}</button></div>}
			<p className="evaluation-message">{message}</p>
		</div>
		<div className="evaluation-audit"><h3>最近不可变记录</h3>{[
			...inventory.holdouts.slice(0, 3).map((item) => ({ kind: "留出", id: item.id, detail: `${item.market} · ${item.objective} · 截止 ${new Date(item.label_cutoff).toLocaleString("zh-CN")}` })),
			...inventory.datasets.slice(0, 3).map((item) => ({ kind: "数据集", id: datasetID(item), detail: `密封 ${item.manifest.final_holdout.included_count} · 折次 ${item.manifest.folds.length}` })),
			...inventory.experiments.slice(0, 3).map((item) => ({ kind: "实验", id: experimentID(item), detail: `折次 ${item.experiment.folds.length} · 未访问留出 ${String(!item.experiment.final_holdout_accessed)}` })),
			...inventory.performanceReports.slice(0, 3).map((item) => ({ kind: "开发报告", id: performanceID(item), detail: item.report.selection_decision })),
			...inventory.finalReports.slice(0, 3).map((item) => ({ kind: "最终报告", id: item.id, detail: `${item.variant_name} · ${item.status} · ${item.evaluated_sample_count}/${item.sealed_sample_count}` })),
		].map((item) => <article key={`${item.kind}:${item.id}`}><span>{item.kind}</span><code>{item.id}</code><small>{item.detail}</small></article>)}{inventory.holdouts.length + inventory.datasets.length + inventory.experiments.length + inventory.performanceReports.length + inventory.finalReports.length === 0 && <div className="page-empty">当前生产库没有评估流水线记录；这不代表已完成验收。</div>}</div>
	</section>;
}
