import { useCallback, useEffect, useMemo, useState } from "react";

import BuildFooter from "./BuildFooter";
import { AppRoute, RoutedPage, routeFromHash, TopNavigation } from "./AppPages";

const API = import.meta.env.VITE_API_URL || "";

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
  paused_from_phase: string | null;
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
export type Theme = "dark" | "light";

export type TrackedConnection = {
  failures: number;
  state: ModelConnectionState;
};

export type HealthTrackingState = {
  ollama: TrackedConnection;
  models: Record<string, TrackedConnection>;
};

const ollamaModels = ["qwen2.5:3b", "qwen2.5:7b", "qwen2.5:14b", "qwen2.5-coder:7b"] as const;
const healthFailureThreshold = 3;
const themeStorageKey = "market-loop-theme";

export function normalizeTheme(value: string | null): Theme {
  return value === "light" ? "light" : "dark";
}

export function createInitialHealthTracking(): HealthTrackingState {
  return {
    ollama: { failures: 0, state: "checking" },
    models: Object.fromEntries(
      ollamaModels.map((model) => [model, { failures: 0, state: "checking" }]),
    ),
  };
}

function advanceConnection(
  current: TrackedConnection,
  available: boolean,
  failureState: Extract<ModelConnectionState, "offline" | "missing">,
): TrackedConnection {
  if (available) return { failures: 0, state: "available" };
  const failures = current.failures + 1;
  return {
    failures,
    state: failures >= healthFailureThreshold ? failureState : "checking",
  };
}

export function updateHealthTracking(
  current: HealthTrackingState,
  health: HealthStatus | null,
): HealthTrackingState {
  const ollamaAvailable = health?.ollama === true;
  const installedModels = new Set(
    ollamaAvailable ? health.models.map((name) => name.toLocaleLowerCase()) : [],
  );
  return {
    ollama: advanceConnection(current.ollama, ollamaAvailable, "offline"),
    models: Object.fromEntries(ollamaModels.map((model) => {
      const available = ollamaAvailable && installedModels.has(model.toLocaleLowerCase());
      return [
        model,
        advanceConnection(
          current.models[model],
          available,
          ollamaAvailable ? "missing" : "offline",
        ),
      ];
    })),
  };
}

function isHealthStatus(value: unknown): value is HealthStatus {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<HealthStatus>;
  return typeof candidate.ollama === "boolean"
    && Array.isArray(candidate.models)
    && candidate.models.every((model) => typeof model === "string");
}

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

export function formatCountdown(totalSeconds: number) {
  const safe = Math.max(0, Math.ceil(totalSeconds));
  const minutes = Math.floor(safe / 60);
  const seconds = safe % 60;
  return `${String(minutes).padStart(2, "0")}分${String(seconds).padStart(2, "0")}秒`;
}

export function scanButtonText(status: ScanStatus | null, serverNowMs: number) {
  if (!status) return "准备扫描…";
  if (status.state === "paused") {
    if (status.total > 0) return `继续扫描 · 已暂停 ${status.current}/${status.total}`;
    return "继续扫描 · 已暂停";
  }
  if (isScanning(status.state)) {
    if (status.phase === "extracting" && status.total > 0) {
      return `暂停 · 事件归纳 ${status.current}/${status.total}`;
    }
    if (status.phase === "queuing" && status.total > 0) {
      return `暂停 · 研究入队 ${status.current}/${status.total}`;
    }
    if (status.state === "retrying") return "暂停扫描 · 正在重试";
    return status.state === "queued" ? "暂停扫描 · 排队中" : "暂停扫描 · 新闻发现";
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
  const [healthTracking, setHealthTracking] = useState(createInitialHealthTracking);
  const [theme, setTheme] = useState<Theme>(() => {
    if (typeof window === "undefined") return "dark";
    try {
      return normalizeTheme(window.localStorage.getItem(themeStorageKey));
    } catch {
      return "dark";
    }
  });
  const [scanStatus, setScanStatus] = useState<ScanStatus | null>(null);
  const [serverOffset, setServerOffset] = useState(0);
  const [clock, setClock] = useState(Date.now());
  const [expandedLog, setExpandedLog] = useState<string | null>(null);
  const [selected, setSelected] = useState<Recommendation | null>(null);
  const [scanActionPending, setScanActionPending] = useState(false);
  const [route, setRoute] = useState<AppRoute>(() => (
    typeof window !== "undefined" ? routeFromHash(window.location.hash) : "home"
  ));

  const applyScanStatus = useCallback((status: ScanStatus) => {
    setScanStatus(status);
    setServerOffset(Date.parse(status.server_time) - Date.now());
  }, []);

  const pollHealth = useCallback(async (signal?: AbortSignal) => {
    let healthData: HealthStatus | null = null;
    try {
      const response = await fetch(`${API}/health`, { signal });
      if (!response.ok) throw new Error(`Health request failed with ${response.status}`);
      const payload: unknown = await response.json();
      if (!isHealthStatus(payload)) throw new Error("Health response is invalid");
      healthData = payload;
    } catch {
      if (signal?.aborted) return;
      healthData = null;
    }
    if (signal?.aborted) return;
    setHealthTracking((current) => updateHealthTracking(current, healthData));
  }, []);

  const refresh = useCallback(async () => {
    const [events, runs, recommendations, portfolioData, scanData, analysisLogs] = await Promise.all([
      fetch(`${API}/api/v1/events?limit=30`).then((r) => r.json()),
      fetch(`${API}/api/v1/research-runs?limit=20`).then((r) => r.json()),
      fetch(`${API}/api/v1/recommendations?limit=20`).then((r) => r.json()),
      fetch(`${API}/api/v1/portfolio`).then((r) => r.json()),
      fetch(`${API}/api/v1/scan/status`).then((r) => r.json()),
      fetch(`${API}/api/v1/analysis-logs?limit=10`).then((r) => r.json()),
    ]);
    setSnapshot({ events, runs, recommendations, analysis_logs: analysisLogs });
    setPortfolio(portfolioData);
    applyScanStatus(scanData);
  }, [applyScanStatus]);

  useEffect(() => {
    const healthController = new AbortController();
    refresh().catch(() => undefined);
    pollHealth(healthController.signal);
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
      pollHealth(healthController.signal);
    }, 60000);
    const scanTimer = window.setInterval(() => {
      fetch(`${API}/api/v1/scan/status`).then((r) => r.json()).then(applyScanStatus).catch(() => undefined);
    }, 2000);
    const clockTimer = window.setInterval(() => setClock(Date.now()), 1000);
    return () => {
      healthController.abort();
      stream.close();
      window.clearInterval(portfolioTimer);
      window.clearInterval(healthTimer);
      window.clearInterval(scanTimer);
      window.clearInterval(clockTimer);
    };
  }, [applyScanStatus, pollHealth, refresh]);

  useEffect(() => {
    const handleHashChange = () => setRoute(routeFromHash(window.location.hash));
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    document.querySelector('meta[name="theme-color"]')?.setAttribute(
      "content",
      theme === "light" ? "#f2f7f5" : "#07110f",
    );
    try {
      window.localStorage.setItem(themeStorageKey, theme);
    } catch {
      // The selected theme still applies when storage is unavailable.
    }
  }, [theme]);

  useEffect(() => {
    if (snapshot.analysis_logs.length && !snapshot.analysis_logs.some((item) => item.id === expandedLog)) {
      setExpandedLog(snapshot.analysis_logs[0].id);
    }
  }, [expandedLog, snapshot.analysis_logs]);

  async function scan() {
    if (scanActionPending) return;
    const action = scanStatus?.state === "paused"
      ? "resume"
      : scanStatus && isScanning(scanStatus.state)
        ? "pause"
        : "start";
    setScanActionPending(true);
    setScanStatus((current) => {
      if (!current) return current;
      if (action === "pause") {
        return {
          ...current,
          state: "paused",
          paused_from_phase: current.phase,
          phase: "paused",
        };
      }
      if (action === "resume") {
        return {
          ...current,
          state: "running",
          phase: current.paused_from_phase || "discovering",
          paused_from_phase: null,
        };
      }
      return { ...current, state: "queued", phase: "queued" };
    });
    try {
      const endpoint = action === "start" ? "/api/v1/scan" : `/api/v1/scan/${action}`;
      const response = await fetch(`${API}${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: action === "start" ? JSON.stringify({ background: true }) : undefined,
      });
      if (!response.ok) {
        const latest = await fetch(`${API}/api/v1/scan/status`).then((item) => item.json());
        applyScanStatus(latest);
        return;
      }
      const queued = await response.json() as { scan: ScanStatus };
      applyScanStatus(queued.scan);
    } catch {
      if (action === "start") {
        setScanStatus((current) => current ? {
          ...current, state: "failed", phase: "failed", last_error: "request failed",
        } : current);
      }
    } finally {
      setScanActionPending(false);
    }
  }

  const scanBusy = Boolean(scanStatus && isScanning(scanStatus.state));
  const scanPaused = scanStatus?.state === "paused";
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

  const sharedHeader = <>
    <header>
      <div>
        <p className="eyebrow">EVIDENCE-FIRST RESEARCH OS</p>
        <h1>Market Loop <span>Agent</span></h1>
        <p className="subhead">跨市场新闻发现、证据验证与模拟组合</p>
      </div>
      <div className="header-actions">
        <div className="health-cluster">
          <div
            className={`status ${healthTracking.ollama.state === "available" ? "online" : healthTracking.ollama.state}`}
            aria-label={`Ollama ${ollamaStateLabels[healthTracking.ollama.state]}`}
          >
            <i /> Ollama
          </div>
          <div className="model-statuses" aria-label="千问模型连接状态">
            {ollamaModels.map((model) => (
              <ModelStatus key={model} state={healthTracking.models[model].state} model={model} />
            ))}
          </div>
        </div>
        <div className="theme-switcher" role="group" aria-label="主题切换">
          <button type="button" className={theme === "dark" ? "active" : undefined} aria-pressed={theme === "dark"} onClick={() => setTheme("dark")}>深色</button>
          <button type="button" className={theme === "light" ? "active" : undefined} aria-pressed={theme === "light"} onClick={() => setTheme("light")}>浅色</button>
        </div>
        <button
          type="button"
          onClick={scan}
          disabled={scanActionPending}
          className={scanPaused ? "paused" : scanBusy ? "scanning" : undefined}
          aria-pressed={scanPaused}
          aria-live="polite"
          title={scanPaused ? "点击继续当前扫描" : scanBusy ? "点击暂停；当前新闻处理完成后生效" : scanStatus?.next_scan_at ? "点击可提前扫描" : undefined}
        >
          {scanActionPending ? "正在切换…" : scanLabel}
        </button>
      </div>
    </header>
    <TopNavigation current={route} />
  </>;

  if (route !== "home") {
    return <main>{sharedHeader}<RoutedPage route={route} apiBase={API} /><BuildFooter /></main>;
  }

  return (
    <main>
      {sharedHeader}

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
      <BuildFooter />
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

const ollamaStateLabels: Record<ModelConnectionState, string> = {
  ...modelStateLabels,
  available: "在线",
};

function ModelStatus({
  state,
  model,
}: {
  state: ModelConnectionState;
  model: string;
}) {
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
