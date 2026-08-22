import { useCallback, useEffect, useMemo, useState } from "react";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

type Candidate = {
  asset: { asset_id: string; symbol: string; name: string; market: string };
  relationship: string;
  relevance: number;
  rationale: string;
};

type EventItem = {
  id: string;
  headline: string;
  event_type: string;
  source_quality: string;
  priority: number;
  published_at: string;
  candidates: Candidate[];
};

type Run = {
  id: string;
  status: string;
  asset: { symbol: string; name: string };
  verification_round: number;
  updated_at: string;
};

type Recommendation = {
  id: string;
  asset: { symbol: string; name: string; market: string };
  score: number;
  rating: string;
  confidence: number;
  evidence_complete: boolean;
  thesis: { summary: string; catalysts: string[]; risks: string[] };
  generated_at: string;
};

type Portfolio = {
  cash_usd: number;
  nav_usd: number;
  crypto_weight: number;
  positions: Array<{
    asset: { symbol: string; name: string };
    market_value_usd: number;
    unrealized_pnl_usd: number;
    weight: number;
  }>;
};

type Snapshot = { events: EventItem[]; runs: Run[]; recommendations: Recommendation[] };
type ScanStatus = {
  state: string;
  task_id: string | null;
  phase: string | null;
  current: number;
  total: number;
  started_at: string | null;
  last_completed_at: string | null;
  next_scan_at: string | null;
  last_result: Record<string, unknown> | null;
  last_error: string | null;
  interval_seconds: number;
  server_time: string;
};

export type HealthStatus = {
  ollama: boolean;
  models: string[];
};

export type ModelConnectionState = "checking" | "offline" | "available" | "missing";

const ollamaModels = ["qwen2.5:3b", "qwen2.5:7b", "qwen2.5-coder:7b"] as const;

type AnalysisStep = {
  phase: string;
  status: string;
  executor: string;
  model: string | null;
  summary: string;
  metrics: Record<string, unknown>;
  occurred_at: string;
};

type AnalysisLog = {
  id: string;
  event_id: string | null;
  run_id: string | null;
  event_research_run_id: string | null;
  status: string;
  updated_at: string;
  news: Array<{ id: string; title: string; source: string; url: string; published_at: string }>;
  event: { headline: string; event_type: string; direct_impact: string; priority: number } | null;
  asset: { symbol: string; name: string; market: string } | null;
  models: string[];
  steps: AnalysisStep[];
  result: {
    kind: "asset_recommendation";
    rating: string;
    score: number;
    confidence: number;
    evidence_complete: boolean;
    summary: string;
  } | {
    kind: "event_report";
    confidence: number;
    evidence_complete: boolean;
    summary: string;
    affected_markets: string[];
    affected_sectors: string[];
    scenarios: string[];
    catalysts: string[];
    risks: string[];
    unresolved_questions: string[];
  } | null;
};

type DashboardSnapshot = Snapshot & { analysis_logs: AnalysisLog[] };

const labels: Record<string, string> = {
  strongly_bullish: "强烈看多",
  bullish: "看多",
  watch: "观察",
  bearish: "看空",
  strongly_bearish: "强烈看空",
  insufficient_evidence: "证据不足",
  completed: "已完成",
  running: "研究中",
  verifying: "验证中",
  queued: "排队中",
  failed: "失败",
  unmapped: "未映射标的",
  not_researched: "尚未深研",
  mapping_queued: "标的映射排队中",
  mapping_running: "标的映射中",
  mapping_retrying: "标的映射重试中",
  mapping_failed: "标的映射失败",
};

const phaseLabels: Record<string, string> = {
  news_collection: "新闻采集与归档",
  event_extraction: "事件模型提取",
  event_extraction_fallback: "规则回退提取",
  asset_mapping: "证券主数据映射",
  asset_mapping_queue: "7B 标的发现入队",
  research_queue: "研究任务入队",
  evidence_gathering: "证据收集与检索",
  report_drafting: "研究报告生成",
  verification: "证据与引用校验",
  report_revision: "研究报告修订",
  cloud_verification: "高影响云复核",
  finalization: "评级与置信度定稿",
  research_failed: "研究任务失败",
  event_research_queue: "中性事件研报入队",
  event_evidence_gathering: "事件证据收集",
  event_report_drafting: "中性事件研报生成",
  event_report_verification: "事件证据与引用校验",
  event_report_revision: "中性事件研报修订",
  event_report_finalization: "中性事件研报定稿",
  event_research_failed: "中性事件研报失败",
};

export function analysisPendingText(status: string) {
  if (status.startsWith("mapping_")) {
    return status === "mapping_failed"
      ? "7B 标的发现失败，系统未生成或猜测证券代码。"
      : "7B 正在识别并验证新闻中明确提及的证券标的。";
  }
  if (status === "unmapped") return "该新闻尚未映射到可研究标的。";
  return "研究任务正在排队或处理中，结果会自动更新。";
}

function isScanning(state: string) {
  return state === "queued" || state === "running" || state === "retrying";
}

export function modelConnectionState(
  health: HealthStatus | null,
  modelName: string,
): ModelConnectionState {
  if (!health) return "checking";
  if (!health.ollama) return "offline";
  const expected = modelName.toLocaleLowerCase();
  return health.models.some((name) => name.toLocaleLowerCase() === expected)
    ? "available"
    : "missing";
}

export function formatCountdown(totalSeconds: number) {
  const safe = Math.max(0, Math.ceil(totalSeconds));
  const minutes = Math.floor(safe / 60);
  const seconds = safe % 60;
  return `${String(minutes).padStart(2, "0")}分${String(seconds).padStart(2, "0")}秒`;
}

export function scanButtonText(status: ScanStatus | null, serverNowMs: number) {
  if (!status) return "准备扫描…";
  if (isScanning(status.state)) {
    if (status.phase === "extracting" && status.total > 0) {
      return `扫描中 · 事件归纳 ${status.current}/${status.total}`;
    }
    return status.state === "retrying" ? "扫描中 · 正在重试" : "扫描中";
  }
  if (status.state === "failed") return "扫描失败 · 点击重试";
  if (status.next_scan_at) {
    const remaining = (Date.parse(status.next_scan_at) - serverNowMs) / 1000;
    if (remaining <= 0) return "即将开始扫描…";
    return `距离下一次扫描 ${formatCountdown(remaining)}`;
  }
  return "立即扫描";
}

function money(value: number) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(value);
}

function time(value: string) {
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(
    new Date(value),
  );
}

export default function App() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot>({
    events: [], runs: [], recommendations: [], analysis_logs: [],
  });
  const [portfolio, setPortfolio] = useState<Portfolio | null>(null);
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [scanStatus, setScanStatus] = useState<ScanStatus | null>(null);
  const [serverOffset, setServerOffset] = useState(0);
  const [clock, setClock] = useState(Date.now());
  const [expandedLog, setExpandedLog] = useState<string | null>(null);
  const [selected, setSelected] = useState<Recommendation | null>(null);

  const applyScanStatus = useCallback((status: ScanStatus) => {
    setScanStatus(status);
    setServerOffset(Date.parse(status.server_time) - Date.now());
  }, []);

  const refresh = useCallback(async () => {
    const [events, runs, recommendations, portfolioData, healthData, scanData, analysisLogs] = await Promise.all([
      fetch(`${API}/api/v1/events?limit=30`).then((r) => r.json()),
      fetch(`${API}/api/v1/research-runs?limit=20`).then((r) => r.json()),
      fetch(`${API}/api/v1/recommendations?limit=20`).then((r) => r.json()),
      fetch(`${API}/api/v1/portfolio`).then((r) => r.json()),
      fetch(`${API}/health`).then((r) => r.json() as Promise<HealthStatus>),
      fetch(`${API}/api/v1/scan/status`).then((r) => r.json()),
      fetch(`${API}/api/v1/analysis-logs?limit=10`).then((r) => r.json()),
    ]);
    setSnapshot({ events, runs, recommendations, analysis_logs: analysisLogs });
    setPortfolio(portfolioData);
    setHealth(healthData);
    applyScanStatus(scanData);
  }, [applyScanStatus]);

  useEffect(() => {
    refresh().catch(() => undefined);
    const stream = new EventSource(`${API}/api/v1/stream`);
    stream.addEventListener("snapshot", (event) => {
      const incoming = JSON.parse((event as MessageEvent).data) as DashboardSnapshot;
      setSnapshot((current) => ({
        events: incoming.events,
        runs: incoming.runs,
        recommendations: incoming.recommendations,
        analysis_logs: incoming.analysis_logs || current.analysis_logs,
      }));
    });
    const portfolioTimer = window.setInterval(() => {
      fetch(`${API}/api/v1/portfolio`).then((r) => r.json()).then(setPortfolio).catch(() => undefined);
    }, 15000);
    const healthTimer = window.setInterval(() => {
      fetch(`${API}/health`)
        .then((r) => r.json() as Promise<HealthStatus>)
        .then(setHealth)
        .catch(() => setHealth(null));
    }, 30000);
    const scanTimer = window.setInterval(() => {
      fetch(`${API}/api/v1/scan/status`).then((r) => r.json()).then(applyScanStatus).catch(() => undefined);
    }, 2000);
    const clockTimer = window.setInterval(() => setClock(Date.now()), 1000);
    return () => {
      stream.close();
      window.clearInterval(portfolioTimer);
      window.clearInterval(healthTimer);
      window.clearInterval(scanTimer);
      window.clearInterval(clockTimer);
    };
  }, [applyScanStatus, refresh]);

  useEffect(() => {
    if (snapshot.analysis_logs.length && !snapshot.analysis_logs.some((item) => item.id === expandedLog)) {
      setExpandedLog(snapshot.analysis_logs[0].id);
    }
  }, [expandedLog, snapshot.analysis_logs]);

  async function scan() {
    if (scanStatus && isScanning(scanStatus.state)) return;
    setScanStatus((current) => current ? { ...current, state: "queued", phase: "queued" } : current);
    try {
      const response = await fetch(`${API}/api/v1/scan`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ background: true }),
      });
      if (!response.ok) throw new Error("scan request failed");
      const queued = await response.json() as { scan: ScanStatus };
      applyScanStatus(queued.scan);
    } catch {
      setScanStatus((current) => current ? {
        ...current, state: "failed", phase: "failed", last_error: "request failed",
      } : current);
    }
  }

  const scanBusy = Boolean(scanStatus && isScanning(scanStatus.state));
  const scanLabel = scanButtonText(scanStatus, clock + serverOffset);

  async function research(candidate: Candidate, eventId: string) {
    await fetch(`${API}/api/v1/research`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ asset_id: candidate.asset.asset_id, event_id: eventId, background: true }),
    });
  }

  const confidenceAverage = useMemo(() => {
    if (!snapshot.recommendations.length) return 0;
    return snapshot.recommendations.reduce((sum, item) => sum + item.confidence, 0) /
      snapshot.recommendations.length;
  }, [snapshot.recommendations]);

  return (
    <main>
      <header>
        <div>
          <p className="eyebrow">EVIDENCE-FIRST RESEARCH OS</p>
          <h1>Market Loop <span>Agent</span></h1>
          <p className="subhead">跨市场新闻发现、证据验证与模拟组合</p>
        </div>
        <div className="header-actions">
          <div className="health-cluster">
            <div
              className={`status ${health ? (health.ollama ? "online" : "offline") : "checking"}`}
              aria-label={`Ollama ${health ? (health.ollama ? "在线" : "离线") : "检测中"}`}
            >
              <i /> Ollama
            </div>
            <div className="model-statuses" aria-label="千问模型连接状态">
              {ollamaModels.map((model) => (
                <ModelStatus key={model} health={health} model={model} />
              ))}
            </div>
          </div>
          <button
            onClick={scan}
            disabled={scanBusy}
            aria-live="polite"
            title={!scanBusy && scanStatus?.next_scan_at ? "点击可提前扫描" : undefined}
          >
            {scanLabel}
          </button>
        </div>
      </header>

      <section className="metrics">
        <Metric label="组合净值" value={portfolio ? money(portfolio.nav_usd) : "—"} note="模拟资金" />
        <Metric label="可用现金" value={portfolio ? money(portfolio.cash_usd) : "—"} note="最低保留 10%" />
        <Metric label="事件队列" value={String(snapshot.events.length)} note="最近 30 条" />
        <Metric label="平均置信度" value={`${Math.round(confidenceAverage * 100)}%`} note="独立于方向评分" />
      </section>

      <section className="grid">
        <div className="panel events-panel">
          <PanelTitle title="市场事件" meta="10 分钟增量循环" />
          <div className="feed">
            {snapshot.events.length === 0 && <Empty text="尚无事件，配置数据源后启动扫描。" />}
            {snapshot.events.map((item) => (
              <article className="event" key={item.id}>
                <div className="event-top">
                  <span className="event-type">{item.event_type}</span>
                  <span className="muted">优先级 {Math.round(item.priority * 100)}</span>
                </div>
                <h3>{item.headline}</h3>
                <p className="muted">{time(item.published_at)} · {item.source_quality}</p>
                <div className="candidates">
                  {item.candidates.slice(0, 4).map((candidate) => (
                    <button className="candidate" key={candidate.asset.asset_id} onClick={() => research(candidate, item.id)}>
                      <strong>{candidate.asset.symbol}</strong>
                      <span>{candidate.asset.name}</span>
                      <em>{Math.round(candidate.relevance * 100)}%</em>
                    </button>
                  ))}
                </div>
              </article>
            ))}
          </div>
        </div>

        <div className="panel runs-panel">
          <PanelTitle title="Agent 轨迹" meta="可恢复状态图" />
          <div className="runs">
            {snapshot.runs.length === 0 && <Empty text="还没有研究任务。" />}
            {snapshot.runs.map((run) => (
              <div className="run" key={run.id}>
                <i className={run.status} />
                <div><strong>{run.asset.symbol}</strong><span>{run.asset.name}</span></div>
                <div><strong>{labels[run.status] || run.status}</strong><span>验证 {run.verification_round}/2</span></div>
                <span className="muted">{time(run.updated_at)}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="panel recommendations-panel">
          <PanelTitle title="最新建议" meta="证据门控评级" />
          <div className="recommendations">
            {snapshot.recommendations.length === 0 && <Empty text="完成一次深度研究后将在这里生成建议。" />}
            {snapshot.recommendations.map((item) => (
              <button className="recommendation" key={item.id} onClick={() => setSelected(item)}>
                <div className="ticker">
                  <strong>{item.asset.symbol}</strong>
                  <span>{item.asset.name}</span>
                </div>
                <div className={`score ${item.score > 19 ? "positive" : item.score < -19 ? "negative" : "neutral"}`}>
                  {item.score > 0 ? "+" : ""}{item.score}
                </div>
                <div>
                  <strong>{labels[item.rating] || item.rating}</strong>
                  <span className="muted">置信度 {Math.round(item.confidence * 100)}%</span>
                </div>
                <span className={`evidence ${item.evidence_complete ? "verified" : "limited"}`}>
                  {item.evidence_complete ? "已验证" : "证据有限"}
                </span>
              </button>
            ))}
          </div>
        </div>
      </section>

      <section className="panel trace-panel">
        <PanelTitle title="分析链路" meta="模型、证据与校验审计" />
        <div className="trace-list">
          {snapshot.analysis_logs.length === 0 && <Empty text="完成新闻扫描后，这里会显示可审计的分析过程。" />}
          {snapshot.analysis_logs.map((log) => {
            const open = expandedLog === log.id;
            return (
              <article className={`trace-card ${open ? "open" : ""}`} key={log.id}>
                <button className="trace-summary" onClick={() => setExpandedLog(log.id)} aria-expanded={open}>
                  <div>
                    <span className="event-type">{log.event?.event_type || "RESEARCH"}</span>
                    <strong>{log.event?.headline || log.news[0]?.title || `${log.asset?.symbol || "未知标的"} 独立研究`}</strong>
                  </div>
                  <div className="trace-asset">
                    <strong>{log.asset?.symbol || (log.event_research_run_id ? "EVENT" : "—")}</strong>
                    <span>{log.asset?.name || (log.event_research_run_id ? "中性事件研报" : "未映射主标的")}</span>
                  </div>
                  <div className="trace-confidence">
                    <strong>{log.result ? `${Math.round(log.result.confidence * 100)}%` : "—"}</strong>
                    <span>置信度</span>
                  </div>
                  <span className={`trace-state ${log.status}`}>{labels[log.status] || log.status}</span>
                  <i>{open ? "−" : "+"}</i>
                </button>

                {open && (
                  <div className="trace-detail">
                    <div className="trace-context">
                      <div>
                        <h3>新闻来源</h3>
                        {log.news.length === 0 && <p className="muted">此研究没有关联新闻事件。</p>}
                        {log.news.map((item) => (
                          <a href={item.url} target="_blank" rel="noreferrer" key={item.id}>
                            <strong>{item.title}</strong>
                            <span>{item.source} · {time(item.published_at)}</span>
                          </a>
                        ))}
                      </div>
                      <div>
                        <h3>执行模型</h3>
                        <div className="model-list">
                          {log.models.length
                            ? log.models.map((model) => <span key={model}>{model}</span>)
                            : <span>确定性规则</span>}
                        </div>
                        {log.event?.direct_impact && <p>{log.event.direct_impact}</p>}
                      </div>
                    </div>

                    <div className="trace-timeline">
                      {log.steps.map((step, index) => (
                        <div className={`trace-step ${step.status}`} key={`${step.phase}-${step.occurred_at}-${index}`}>
                          <i />
                          <div>
                            <div className="trace-step-title">
                              <strong>{phaseLabels[step.phase] || step.phase}</strong>
                              <span>{step.model || step.executor}</span>
                              <time>{time(step.occurred_at)}</time>
                            </div>
                            <p>{step.summary}</p>
                          </div>
                        </div>
                      ))}
                    </div>

                    {log.result?.kind === "asset_recommendation" ? (
                      <div className="trace-result">
                        <div>
                          <span>最终结果</span>
                          <strong>{labels[log.result.rating] || log.result.rating}</strong>
                        </div>
                        <div><span>方向分数</span><strong>{log.result.score > 0 ? "+" : ""}{log.result.score}</strong></div>
                        <div><span>置信度</span><strong>{Math.round(log.result.confidence * 100)}%</strong></div>
                        <div><span>证据</span><strong>{log.result.evidence_complete ? "完整" : "不足"}</strong></div>
                        <p>{log.result.summary}</p>
                      </div>
                    ) : log.result?.kind === "event_report" ? (
                      <div className="trace-result event-report-result">
                        <div>
                          <span>最终结果</span>
                          <strong>中性事件研报</strong>
                        </div>
                        <div><span>置信度</span><strong>{Math.round(log.result.confidence * 100)}%</strong></div>
                        <div><span>证据</span><strong>{log.result.evidence_complete ? "完整" : "不足"}</strong></div>
                        <div>
                          <span>影响范围</span>
                          <strong>{[...log.result.affected_markets, ...log.result.affected_sectors].join(" · ") || "待确认"}</strong>
                        </div>
                        <p>{log.result.summary}</p>
                      </div>
                    ) : (
                      <div className="trace-pending">
                        {analysisPendingText(log.status)}
                      </div>
                    )}
                  </div>
                )}
              </article>
            );
          })}
        </div>
      </section>

      {selected && (
        <div className="modal-backdrop" onClick={() => setSelected(null)}>
          <article className="modal" onClick={(event) => event.stopPropagation()}>
            <button className="close" onClick={() => setSelected(null)}>×</button>
            <p className="eyebrow">{selected.asset.market} · {selected.asset.symbol}</p>
            <h2>{selected.asset.name}</h2>
            <div className="modal-score"><strong>{selected.score}</strong><span>{labels[selected.rating]}</span></div>
            <p>{selected.thesis.summary}</p>
            <h3>催化剂</h3>
            <ul>{selected.thesis.catalysts.map((item) => <li key={item}>{item}</li>)}</ul>
            <h3>风险</h3>
            <ul>{selected.thesis.risks.map((item) => <li key={item}>{item}</li>)}</ul>
            <footer>置信度 {Math.round(selected.confidence * 100)}% · 仅用于研究和模拟</footer>
          </article>
        </div>
      )}
    </main>
  );
}

function Metric({ label, value, note }: { label: string; value: string; note: string }) {
  return <div className="metric"><span>{label}</span><strong>{value}</strong><small>{note}</small></div>;
}

function PanelTitle({ title, meta }: { title: string; meta: string }) {
  return <div className="panel-title"><h2>{title}</h2><span>{meta}</span></div>;
}

function Empty({ text }: { text: string }) {
  return <div className="empty"><i>◇</i><p>{text}</p></div>;
}

const modelStateLabels: Record<ModelConnectionState, string> = {
  checking: "检测中",
  offline: "离线",
  available: "可用",
  missing: "未安装",
};

function ModelStatus({
  health,
  model,
}: {
  health: HealthStatus | null;
  model: string;
}) {
  const state = modelConnectionState(health, model);
  return (
    <div
      className={`model-status ${state}`}
      aria-label={`${model} ${modelStateLabels[state]}`}
    >
      <i />
      <span>{model}</span>
    </div>
  );
}
