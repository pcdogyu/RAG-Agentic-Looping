import { FormEvent, useCallback, useEffect, useState } from "react";

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
			created_at?: string;
			completed_at?: string;
			selected: number;
			matured: number;
			pending: number;
			unavailable: number;
			excluded: number;
			failed: number;
			pending_reasons: Record<string, number>;
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

const tokenKey = "market-loop-admin-token";
const statusLabels: Record<string, string> = {
	completed: "已满足",
	waiting_human_input: "等待人工输入",
	waiting_natural_maturity: "等待自然成熟",
	blocked_by_dependency: "依赖未满足",
	ready_for_manual_action: "可人工执行",
	engineering_gap: "工程缺口",
};

function readToken() {
	if (typeof window === "undefined") return "";
	try { return window.sessionStorage.getItem(tokenKey) || ""; } catch { return ""; }
}

export function phaseTwoReadinessStatusLabel(status: string) {
	return statusLabels[status] || status || "未知";
}

export function phaseTwoPendingReasonLabel(reason: string) {
	return pendingReasonLabels[reason] || reason || "未提供原因";
}

export function PhaseTwoReadinessPanel({ report }: { report: PhaseTwoReadinessReport }) {
	const completed = report.gates.filter((gate) => gate.status === "completed").length;
	const latest = report.facts.latest_outcome_evaluation || { job_id: "", status: "not_run", selected: 0, matured: 0, pending: 0, unavailable: 0, excluded: 0, failed: 0, pending_reasons: {} };
	return <>
		<div className="readiness-summary">
			<article><span>总体状态</span><strong>{report.overall_status === "eligible_for_human_acceptance" ? "可人工验收" : "尚未完成"}</strong><small>系统不会自动标记第二期完成</small></article>
			<article><span>已满足门禁</span><strong>{completed}/{report.gates.length}</strong><small>阻断门禁 {report.completed_gates}/{report.total_blocking_gates}</small></article>
			<article><span>待成熟标签</span><strong>{report.facts.pending_prediction_labels}</strong><small>只能等待真实交易日经过</small></article>
			<article><span>授权导入回执</span><strong>{report.facts.licensed_benchmark_import_receipts + report.facts.tradability_import_receipts}</strong><small>持牌基准 + 可交易状态</small></article>
		</div>
		<section className="readiness-outcome-evaluation">
			<header><div><span>LATEST OUTCOME EVALUATION</span><h2>最近一次真实结果评估</h2></div><strong>{latest.status === "not_run" ? "尚未运行" : latest.status}</strong></header>
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
	const [token, setToken] = useState(readToken);
	const [draft, setDraft] = useState("");
	const [report, setReport] = useState<PhaseTwoReadinessReport>();
	const [loading, setLoading] = useState(false);
	const [message, setMessage] = useState("输入管理员令牌后读取生产事实；就绪度快照本身只读，下方人工操作台必须逐步预校验并确认才会写入。");
	const load = useCallback(async () => {
		if (!token) {
			setReport(undefined);
			return;
		}
		setLoading(true);
		try {
			const response = await fetch(`${apiBase}/go/phase-two/readiness`, { headers: { "X-Admin-Token": token } });
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
	}, [apiBase, token]);
	useEffect(() => { void load(); }, [load]);
	function unlock(event: FormEvent) {
		event.preventDefault();
		const value = draft.trim();
		if (!value) return;
		window.sessionStorage.setItem(tokenKey, value);
		setToken(value);
		setDraft("");
	}
	function lock() {
		window.sessionStorage.removeItem(tokenKey);
		setToken("");
		setReport(undefined);
	}
	return <section className="app-page readiness-page">
		<div className="page-heading"><div><span>PHASE II READINESS</span><h1>第二期生产就绪度</h1><p>按真实证据、自然成熟、人工审批和工程能力分别显示门禁；不以演示数据或代码数量代替验收。</p></div></div>
		{token ? <div className="admin-unlock unlocked"><span>管理员核验与人工操作已解锁，本次浏览器会话有效。</span><button type="button" onClick={lock}>锁定</button></div> : <form className="admin-unlock" onSubmit={unlock}><label>管理员令牌<input type="password" value={draft} onChange={(event) => setDraft(event.target.value)} autoComplete="off" /></label><button type="submit">解锁管理员操作</button></form>}
		<div className="readiness-toolbar"><button type="button" disabled={!token || loading} onClick={() => void load()}>{loading ? "正在核验…" : "重新核验生产事实"}</button><span>{message}</span></div>
		{report ? <PhaseTwoReadinessPanel report={report} /> : <div className="page-empty">尚未取得管理员就绪度快照。</div>}
		{token && <PhaseTwoEvaluationWorkbench apiBase={apiBase} token={token} onChanged={() => void load()} />}
	</section>;
}
