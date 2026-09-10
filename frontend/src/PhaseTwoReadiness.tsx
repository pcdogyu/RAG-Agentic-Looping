import { useCallback, useEffect, useRef, useState } from "react";

import { PhaseTwoEvaluationWorkbench } from "./PhaseTwoEvaluationWorkbench";

export type PhaseTwoReadinessGate = {
	id: string;
	stage: string;
	title: string;
	status: string;
	current: number;
	required: number;
	unit: string;
	blocking: boolean;
	external_input: boolean;
	dependencies: string[];
	action: string;
	route?: string;
	authority: string;
};

export type PhaseTwoReadinessReport = {
	version: string;
	as_of: string;
	overall_status: string;
	completed_gates: number;
	total_blocking_gates: number;
	automatic_completion: boolean;
	facts: {
		pending_prediction_labels: number;
		licensed_benchmark_import_receipts: number;
		tradability_import_receipts: number;
		latest_outcome_evaluation?: {
			job_id: string;
			status: string;
			summary_status?: string;
			created_at?: string;
			completed_at?: string;
			selected: number;
			matured: number;
			pending: number;
			unavailable: number;
			excluded: number;
			failed: number;
			pending_reasons: Record<string, number>;
			warning_count?: number;
			legacy_recommendation_outcomes?: { created: number; pending: number; skipped: number; failed: number };
		};
	};
	gates: PhaseTwoReadinessGate[];
};
const pendingReasonLabels: Record<string, string> = {
	awaiting_price_sessions: "等待足够交易日复权价格",
	active_suspension: "有效停牌中",
	awaiting_post_suspension_sessions: "等待复牌后足够交易日",
	symbol_change_continuity_grace: "代码变更连续性观察期",
};

const statusLabels: Record<string, string> = {
	completed: "已满足",
	waiting_human_input: "等待人工输入",
	waiting_natural_maturity: "等待自然成熟",
	blocked_by_dependency: "依赖未满足",
	ready_for_manual_action: "可人工执行",
	engineering_gap: "工程缺口",
};
const outcomeEvaluationStatusLabels: Record<string, string> = {
	not_run: "尚未运行",
	queued: "排队中",
	running: "运行中",
	retrying: "重试中",
	completed: "已完成",
	completed_with_warnings: "已完成（有警告）",
	failed: "失败",
	cancelled: "已取消",
};

export function phaseTwoReadinessStatusLabel(status: string) {
	return statusLabels[status] || status || "未知";
}

export function phaseTwoPendingReasonLabel(reason: string) {
	return pendingReasonLabels[reason] || reason || "未提供原因";
}

export function phaseTwoOutcomeEvaluationStatusLabel(status: string) {
	return outcomeEvaluationStatusLabels[status] || status || "未知";
}

export function isOutcomeEvaluationTerminalState(state: string) {
	return ["COMPLETED", "FAILED", "CANCELLED"].includes(state.trim().toUpperCase());
}

function waitForOutcomeEvaluationPoll(signal: AbortSignal) {
	return new Promise<void>((resolve, reject) => {
		const onAbort = () => {
			window.clearTimeout(timer);
			reject(new DOMException("Aborted", "AbortError"));
		};
		const timer = window.setTimeout(() => {
			signal.removeEventListener("abort", onAbort);
			resolve();
		}, 1000);
		signal.addEventListener("abort", onAbort, { once: true });
	});
}

type PhaseTwoReadinessPanelProps = {
	report: PhaseTwoReadinessReport;
	onEvaluateOutcomes?: () => void;
	outcomeEvaluationBusy?: boolean;
	outcomeEvaluationMessage?: string;
};

export function PhaseTwoReadinessPanel({ report, onEvaluateOutcomes, outcomeEvaluationBusy = false, outcomeEvaluationMessage = "" }: PhaseTwoReadinessPanelProps) {
	const completed = report.gates.filter((gate) => gate.status === "completed").length;
	const latest = report.facts.latest_outcome_evaluation || { job_id: "", status: "not_run", selected: 0, matured: 0, pending: 0, unavailable: 0, excluded: 0, failed: 0, pending_reasons: {} };
	const summaryStatus = latest.summary_status || latest.status;
	const legacy = latest.legacy_recommendation_outcomes || { created: 0, pending: 0, skipped: 0, failed: 0 };
	return <>
		<div className="readiness-summary">
			<article><span>总体状态</span><strong>{report.overall_status === "eligible_for_human_acceptance" ? "可人工验收" : "尚未完成"}</strong><small>系统不会自动标记第二期完成</small></article>
			<article><span>已满足门禁</span><strong>{completed}/{report.gates.length}</strong><small>阻断门禁 {report.completed_gates}/{report.total_blocking_gates}</small></article>
			<article><span>待成熟标签</span><strong>{report.facts.pending_prediction_labels}</strong><small>只能等待真实交易日经过</small></article>
			<article><span>授权导入回执</span><strong>{report.facts.licensed_benchmark_import_receipts + report.facts.tradability_import_receipts}</strong><small>持牌基准 + 可交易状态</small></article>
		</div>
		<section className="readiness-outcome-evaluation">
			<header><div><span>LATEST OUTCOME EVALUATION</span><h2>最近一次真实结果评估</h2></div><div className="readiness-outcome-actions"><strong className={summaryStatus === "completed_with_warnings" ? "warning" : ""}>{phaseTwoOutcomeEvaluationStatusLabel(summaryStatus)}</strong>{onEvaluateOutcomes && <button type="button" disabled={outcomeEvaluationBusy} onClick={onEvaluateOutcomes}>{outcomeEvaluationBusy ? "检查中…" : "重新检查成熟标签"}</button>}</div></header>
			<p className="readiness-outcome-note">严格按冻结的 1/5/20 个交易日门槛检查，不允许提前成熟；已有活动任务时复用同一任务 ID。</p>
			{outcomeEvaluationMessage && <p className="readiness-outcome-message" role="status">{outcomeEvaluationMessage}</p>}
			{(latest.warning_count || 0) > 0 && <div className="readiness-outcome-warning" role="status"><strong>同任务存在 {latest.warning_count} 条分支级警告</strong><span>预测标签失败 {latest.failed}；历史推荐结果失败 {legacy.failed}。原始失败文本和内部地址不在此页面返回或展示。</span></div>}
			{latest.status === "not_run" ? <p>生产库还没有同源结果任务记录。</p> : <>
				<div className="readiness-outcome-counts">
					<span>选中 <b>{latest.selected}</b></span><span>成熟 <b>{latest.matured}</b></span><span>等待 <b>{latest.pending}</b></span><span>不可用 <b>{latest.unavailable}</b></span><span>排除 <b>{latest.excluded}</b></span><span>失败 <b>{latest.failed}</b></span>
				</div>
				<div className="readiness-outcome-reasons">
					{Object.entries(latest.pending_reasons).length > 0 ? Object.entries(latest.pending_reasons).sort(([left], [right]) => left.localeCompare(right)).map(([reason, count]) => <span key={reason}>{phaseTwoPendingReasonLabel(reason)} <b>{count}</b><small>{reason}</small></span>) : <span>没有待成熟原因</span>}
				</div>
				<footer><span>任务 {latest.job_id}</span><time>{latest.completed_at || latest.created_at || "时间不可用"}</time></footer>
			</>}
		</section>
		<div className="readiness-gates">
			{report.gates.map((gate) => <article className={`readiness-gate ${gate.status}`} key={gate.id}>
				<header><span>{gate.stage} · {gate.id}</span><strong>{phaseTwoReadinessStatusLabel(gate.status)}</strong></header>
				<h3>{gate.title}</h3>
				<p><b>{gate.current}</b> / {gate.required} {gate.unit}</p>
				<small>{gate.action}</small>
				{gate.dependencies.length > 0 && <small>依赖：{gate.dependencies.join("、")}</small>}
				<footer><span>{gate.external_input ? "需要真实人工/外部输入" : "系统事实或工程动作"}</span>{gate.route && <a href={`#${gate.route}`}>前往处理</a>}</footer>
			</article>)}
		</div>
	</>;
}

export default function PhaseTwoReadinessPage({ apiBase }: { apiBase: string }) {
	const [report, setReport] = useState<PhaseTwoReadinessReport>();
	const [loading, setLoading] = useState(false);
	const [outcomeEvaluationBusy, setOutcomeEvaluationBusy] = useState(false);
	const [outcomeEvaluationMessage, setOutcomeEvaluationMessage] = useState("");
	const outcomeEvaluationAbort = useRef<AbortController | undefined>(undefined);
	const [message, setMessage] = useState("就绪度按生产事实计算；下方人工操作台仍必须逐步预校验并确认才会写入。");
	const load = useCallback(async () => {
		setLoading(true);
		try {
			const response = await fetch(`${apiBase}/go/phase-two/readiness`);
			const payload = await response.json().catch(() => ({})) as PhaseTwoReadinessReport & { detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			setReport(payload);
			setMessage("已按数据库与服务器配置重新计算；代码存在不等于真实验收完成。");
		} catch (reason) {
			setReport(undefined);
			setMessage(`读取失败：${reason instanceof Error ? reason.message : "未知错误"}`);
		} finally {
			setLoading(false);
		}
	}, [apiBase]);
	useEffect(() => { void load(); }, [load]);
	useEffect(() => () => outcomeEvaluationAbort.current?.abort(), []);
	async function evaluateOutcomes() {
		if (outcomeEvaluationBusy) return;
		outcomeEvaluationAbort.current?.abort();
		const controller = new AbortController();
		outcomeEvaluationAbort.current = controller;
		setOutcomeEvaluationBusy(true);
		setOutcomeEvaluationMessage("正在提交真实结果评估任务…");
		try {
			const response = await fetch(`${apiBase}/go/outcome-labels/evaluate`, { method: "POST", signal: controller.signal });
			const payload = await response.json().catch(() => ({})) as { task_id?: string; early_maturity_allowed?: boolean };
			if (!response.ok) throw new Error(`提交失败（HTTP ${response.status}）`);
			if (!payload.task_id) throw new Error("提交失败：服务端未返回任务 ID");
			if (payload.early_maturity_allowed !== false) throw new Error("提交失败：服务端未确认禁止提前成熟");
			const taskID = payload.task_id;
			setOutcomeEvaluationMessage(`任务 ${taskID} 已入队，正在等待最终状态…`);
			for (let attempt = 0; attempt < 300; attempt += 1) {
				const statusResponse = await fetch(`${apiBase}/go/outcome-labels/evaluations/${encodeURIComponent(taskID)}`, { signal: controller.signal });
				const statusPayload = await statusResponse.json().catch(() => ({})) as { status?: string; summary_status?: string; warning_count?: number };
				if (!statusResponse.ok) throw new Error(`状态查询失败（HTTP ${statusResponse.status}）`);
				const state = (statusPayload.status || "pending").toUpperCase();
				if (isOutcomeEvaluationTerminalState(state)) {
					await load();
					setOutcomeEvaluationMessage(state === "COMPLETED" ? `任务 ${taskID} 已完成${statusPayload.summary_status === "completed_with_warnings" ? `，含 ${statusPayload.warning_count || 0} 条安全汇总警告` : ""}；成熟度统计已刷新。` : `任务 ${taskID} 已${state === "CANCELLED" ? "取消" : "失败"}；未展示内部错误详情，请检查服务日志。`);
					return;
				}
				await waitForOutcomeEvaluationPoll(controller.signal);
			}
			setOutcomeEvaluationMessage(`任务 ${taskID} 仍在后台运行；可稍后重新核验生产事实，不会重复创建活动任务。`);
		} catch (reason) {
			if (reason instanceof DOMException && reason.name === "AbortError") return;
			setOutcomeEvaluationMessage(reason instanceof Error ? reason.message : "真实结果评估失败，请检查服务状态。");
		} finally {
			if (outcomeEvaluationAbort.current === controller) {
				outcomeEvaluationAbort.current = undefined;
				setOutcomeEvaluationBusy(false);
			}
		}
	}
	return <section className="app-page readiness-page">
		<div className="page-heading"><div><span>PHASE II READINESS</span><h1>第二期生产就绪度</h1><p>按真实证据、自然成熟、人工审批和工程能力分别显示门禁；不以演示数据或代码数量代替验收。</p></div></div>
		<div className="readiness-toolbar"><button type="button" disabled={loading} onClick={() => void load()}>{loading ? "正在核验…" : "重新核验生产事实"}</button><span>{message}</span></div>
		{report ? <PhaseTwoReadinessPanel report={report} onEvaluateOutcomes={() => void evaluateOutcomes()} outcomeEvaluationBusy={outcomeEvaluationBusy} outcomeEvaluationMessage={outcomeEvaluationMessage} /> : <div className="page-empty">尚未取得生产就绪度快照。</div>}
		<PhaseTwoEvaluationWorkbench apiBase={apiBase} onChanged={() => void load()} />
	</section>;
}
