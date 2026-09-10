import { FormEvent, useCallback, useEffect, useState } from "react";

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
	};
	gates: PhaseTwoReadinessGate[];
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

export function PhaseTwoReadinessPanel({ report }: { report: PhaseTwoReadinessReport }) {
	const completed = report.gates.filter((gate) => gate.status === "completed").length;
	return <>
		<div className="readiness-summary">
			<article><span>总体状态</span><strong>{report.overall_status === "eligible_for_human_acceptance" ? "可人工验收" : "尚未完成"}</strong><small>系统不会自动标记第二期完成</small></article>
			<article><span>已满足门禁</span><strong>{completed}/{report.gates.length}</strong><small>阻断门禁 {report.completed_gates}/{report.total_blocking_gates}</small></article>
			<article><span>待成熟标签</span><strong>{report.facts.pending_prediction_labels}</strong><small>只能等待真实交易日经过</small></article>
			<article><span>授权导入回执</span><strong>{report.facts.licensed_benchmark_import_receipts + report.facts.tradability_import_receipts}</strong><small>持牌基准 + 可交易状态</small></article>
		</div>
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
	const [message, setMessage] = useState("输入管理员令牌后读取生产事实；本页不会创建数据、批准模型或触发任务。");
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
		{token ? <div className="admin-unlock unlocked"><span>管理员只读核验已解锁，本次浏览器会话有效。</span><button type="button" onClick={lock}>锁定</button></div> : <form className="admin-unlock" onSubmit={unlock}><label>管理员令牌<input type="password" value={draft} onChange={(event) => setDraft(event.target.value)} autoComplete="off" /></label><button type="submit">解锁只读核验</button></form>}
		<div className="readiness-toolbar"><button type="button" disabled={!token || loading} onClick={() => void load()}>{loading ? "正在核验…" : "重新核验生产事实"}</button><span>{message}</span></div>
		{report ? <PhaseTwoReadinessPanel report={report} /> : <div className="page-empty">尚未取得管理员就绪度快照。</div>}
	</section>;
}
