import { FormEvent, useCallback, useEffect, useState } from "react";

import AnalysisPage, { type AnalysisLog } from "./AnalysisPage";
import ModelLogsPage from "./ModelLogs";

export type AppRoute = "home" | "source-filter" | "sources" | "news" | "queue" | "analysis" | "conclusions" | "model-logs" | "search" | "weknora";

export const navigationGroups: Record<"left" | "right", Array<{ route: AppRoute; label: string }>> = {
  left: [
    { route: "home", label: "首页" },
    { route: "source-filter", label: "数据源过滤" },
    { route: "sources", label: "数据源" },
    { route: "news", label: "新闻" },
    { route: "queue", label: "队列" },
    { route: "analysis", label: "分析链路" },
    { route: "conclusions", label: "结论" },
  ],
  right: [
    { route: "model-logs", label: "模型日志" },
    { route: "search", label: "搜索引擎" },
    { route: "weknora", label: "WeKnora" },
  ],
};

const routes = [...navigationGroups.left, ...navigationGroups.right];

export function routeFromHash(hash: string): AppRoute {
  const candidate = hash.replace(/^#\/?/, "") as AppRoute;
  return routes.some((item) => item.route === candidate) ? candidate : "home";
}

export function TopNavigation({ current }: { current: AppRoute }) {
  const links = (items: Array<{ route: AppRoute; label: string }>) => items.map((item) => (
    <a
      key={item.route}
      href={`#/${item.route}`}
      aria-current={current === item.route ? "page" : undefined}
    >
      {item.label}
    </a>
  ));
  return (
    <nav className="top-navigation" aria-label="主导航">
      <div className="navigation-group left">{links(navigationGroups.left)}</div>
      <div className="navigation-group right">{links(navigationGroups.right)}</div>
    </nav>
  );
}

const tokenKey = "market-loop-admin-token";

function readToken() {
  if (typeof window === "undefined") return "";
  try { return window.sessionStorage.getItem(tokenKey) || ""; } catch { return ""; }
}

function AdminUnlock({ token, onToken }: { token: string; onToken: (value: string) => void }) {
  const [draft, setDraft] = useState("");
  function unlock(event: FormEvent) {
    event.preventDefault();
    const value = draft.trim();
    if (!value) return;
    window.sessionStorage.setItem(tokenKey, value);
    onToken(value);
    setDraft("");
  }
  if (token) {
    return (
      <div className="admin-unlock unlocked">
        <span>管理员功能已解锁，本次浏览器会话有效。</span>
        <button type="button" onClick={() => {
          window.sessionStorage.removeItem(tokenKey); onToken("");
        }}>锁定</button>
      </div>
    );
  }
  return (
    <form className="admin-unlock" onSubmit={unlock}>
      <label>管理员令牌<input type="password" value={draft} onChange={(e) => setDraft(e.target.value)} /></label>
      <button type="submit">解锁</button>
      <small>令牌仅保存在 sessionStorage，关闭标签页后失效。</small>
    </form>
  );
}

function PageHeading({ eyebrow, title, copy }: { eyebrow: string; title: string; copy: string }) {
  return <div className="page-heading"><p className="eyebrow">{eyebrow}</p><h2>{title}</h2><p>{copy}</p></div>;
}

export const queueRefreshIntervalMs = 5000;
export const queueDesktopColumns = 5;
export const newsBoardRefreshIntervalMs = 5000;
export const newsSourceDesktopColumns = 3;

export function formatQueueDuration(value: number | null | undefined) {
  if (value == null) return "—";
  const totalSeconds = Math.max(0, Math.floor(value / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}时${minutes}分`;
  if (minutes > 0) return `${minutes}分${seconds}秒`;
  return `${seconds}秒`;
}

export type ResearchQueueItem = {
  asset_id: string;
  symbol: string;
  name: string;
  market: string;
  asset_class: string;
  status: "queued" | "running" | "verifying";
  task_count: number;
  queued_at: string;
  representative_queued_at: string;
  started_at: string | null;
  completed_at: string | null;
  queue_duration_ms: number | null;
  execution_duration_ms: number | null;
  updated_at: string;
};

export type NewsExtractionQueueItem = {
  task_id: string;
  news_id: string;
  title: string;
  source: string;
  published_at: string;
  status: "queued" | "running" | "retrying" | "failed";
  attempt: number;
  queued_at: string;
  started_at: string | null;
  completed_at: string | null;
  queue_duration_ms: number | null;
  execution_duration_ms: number | null;
  updated_at: string;
  error: string | null;
};

export type ModelInferenceQueueItem = {
  lane: string;
  model: string;
  purpose: string;
  binding: string;
  task_enabled: boolean;
  threads: number;
  capacity: number;
  queued: number;
  running: number;
  available: number;
  observable: boolean;
  state: "idle" | "queued" | "running" | "unavailable";
};

export type ModelQueueTask = {
  task_id: string;
  kind: string;
  entity_id: string | null;
  title: string;
  subtitle: string;
  source: string | null;
  status: string;
  attempt: number;
  task_count: number;
  queued_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
  queue_duration_ms: number | null;
  execution_duration_ms: number | null;
  error: string | null;
  metrics: Record<string, unknown>;
};

export type ModelQueueOverviewItem = {
  id: "extract" | "research" | "assist" | "code";
  model: string;
  purpose: string;
  binding: string;
  enabled: boolean;
  state: string;
  threads: number;
  capacity: number;
  available: number;
  observable: boolean;
  counts: {
    queued: number;
    running: number;
    retrying: number;
    verifying: number;
    waiting_for_model: number;
    completed: number;
    failed: number;
  };
  metrics: {
    average_queue_duration_ms: number | null;
    average_execution_duration_ms: number | null;
    longest_wait_ms: number | null;
    estimated_clear_ms: number | null;
    queue_duration_sample_count: number;
    execution_duration_sample_count: number;
  };
  total_tasks: number;
  truncated: boolean;
  tasks: ModelQueueTask[];
  error: string | null;
};

type ModelQueueOverviewResponse = {
  generated_at: string;
  queues: ModelQueueOverviewItem[];
};

const queueStatusLabels: Record<ResearchQueueItem["status"], string> = {
  queued: "排队中",
  running: "研究中",
  verifying: "验证中",
};

const extractionStatusLabels: Record<NewsExtractionQueueItem["status"], string> = {
  queued: "排队中",
  running: "抽取中",
  retrying: "重试中",
  failed: "失败",
};

export function NewsExtractionList({ items }: { items: NewsExtractionQueueItem[] }) {
  if (!items.length) return <div className="page-empty">当前没有待抽取或失败的新闻。</div>;
  return <div className="extraction-list" data-columns={queueDesktopColumns}>{items.map((item) => (
    <article className={`extraction-item ${item.status}`} key={item.task_id} title={item.title}>
      <div className="extraction-item-heading">
        <span className="extraction-status"><i />{extractionStatusLabels[item.status]}</span>
        {item.attempt > 1 && <small>第 {item.attempt} 次尝试</small>}
      </div>
      <strong>{item.title}</strong>
      <div className="extraction-item-meta">
        <span>{item.source}</span>
        <time dateTime={item.published_at}>{new Date(item.published_at).toLocaleString("zh-CN")}</time>
      </div>
      <div className="queue-card-timing">
        <span>排队 {formatQueueDuration(item.queue_duration_ms)}</span>
        <span>执行 {formatQueueDuration(item.execution_duration_ms)}</span>
      </div>
      {item.error && <small className="extraction-error" title={item.error}>{item.error}</small>}
    </article>
  ))}</div>;
}

export function QueueGrid({ items }: { items: ResearchQueueItem[] }) {
  if (!items.length) return <div className="page-empty">当前没有排队或处理中的标的。</div>;
  return <div className="queue-grid" data-columns={queueDesktopColumns}>{items.map((item) => (
    <article
      className={`queue-card ${item.status}`}
      key={item.asset_id}
      title={`${item.symbol} · ${item.name} · ${queueStatusLabels[item.status]}`}
    >
      <span className="queue-card-market">{item.market} · {item.asset_class}</span>
      <strong>{item.symbol}</strong>
      <p>{item.name}</p>
      <div className="queue-card-state-row">
        <span className="queue-card-status"><i />{queueStatusLabels[item.status]}</span>
        {item.task_count > 1 && <small>{item.task_count} 个任务</small>}
      </div>
      <div className="queue-card-timing">
        <span>排队 {formatQueueDuration(item.queue_duration_ms)}</span>
        <span>执行 {formatQueueDuration(item.execution_duration_ms)}</span>
      </div>
      <time dateTime={item.queued_at}>{new Date(item.queued_at).toLocaleString("zh-CN")}</time>
    </article>
  ))}</div>;
}

const inferenceStateLabels: Record<ModelInferenceQueueItem["state"], string> = {
  idle: "空闲",
  queued: "有请求排队",
  running: "推理中",
  unavailable: "状态不可用",
};

export function ModelInferenceQueuePanel({ item }: { item: ModelInferenceQueueItem }) {
  const emptyMessage = item.task_enabled
    ? "当前没有等待或运行中的模型请求。"
    : `${item.binding}；推理通道已就绪。`;
  return <section className="model-queue-panel inference-queue-panel">
    <header>
      <div><p className="eyebrow">MODEL INFERENCE</p><h3>{item.model} {item.purpose}队列</h3></div>
      <span className={`model-queue-state ${item.state}`}>{inferenceStateLabels[item.state]}</span>
    </header>
    <div className="queue-metrics inference-queue-metrics" aria-live="polite">
      <span>排队<strong>{item.queued}</strong></span>
      <span>运行<strong>{item.running}</strong></span>
      <span>可用槽位<strong>{item.available}/{item.capacity}</strong></span>
      <span>CPU 线程<strong>{item.threads}</strong></span>
    </div>
    {!item.observable && <div className="page-error">Redis 队列状态暂时不可用。</div>}
    {item.observable && item.queued === 0 && item.running === 0 && <div className="page-empty">{emptyMessage}</div>}
    {item.observable && (item.queued > 0 || item.running > 0) && <div className="inference-queue-activity">
      <span>正在占用 {item.running} 个推理槽位</span>
      <span>等待进入模型 {item.queued} 个请求</span>
    </div>}
  </section>;
}

const modelQueueStateLabels: Record<string, string> = {
  idle: "空闲",
  queued: "排队中",
  running: "处理中",
  failed: "有失败",
  disabled: "未启用",
  unavailable: "不可用",
};

const modelTaskStatusLabels: Record<string, string> = {
  queued: "排队中",
  proposed: "待执行",
  running: "处理中",
  generating: "生成方案",
  retrying: "重试中",
  verifying: "验证中",
  testing: "测试中",
  merging: "合并中",
  failed: "失败",
  rejected: "已拒绝",
  rolled_back: "已回滚",
  completed: "已完成",
  merged: "已合并",
  insufficient_evidence: "证据不足",
};

const modelQueueEyebrows: Record<ModelQueueOverviewItem["id"], string> = {
  extract: "NEWS EXTRACTION",
  research: "ASSET RESEARCH",
  assist: "ASSET MAPPING",
  code: "CODE EVOLUTION",
};

function queueMetricValue(value: unknown) {
  return typeof value === "number" || typeof value === "string" ? String(value) : "—";
}

function taskSourceLabel(source: string | null) {
  if (source === "automatic") return "自动任务";
  if (source === "manual") return "手动任务";
  if (source === "candidate") return "演进候选";
  return source || "业务任务";
}

export function ModelQueueTaskGrid({ queue }: { queue: ModelQueueOverviewItem }) {
  if (!queue.enabled && queue.id === "code") {
    return <div className="page-empty">代码演进未启用（EVOLUTION_ENABLED=false）。</div>;
  }
  if (!queue.tasks.length) {
    return <div className="page-empty">当前没有等待、运行或最近失败的{queue.purpose}任务。</div>;
  }
  return <div className="model-task-grid" data-queue={queue.id}>{queue.tasks.map((task) => {
    const isMapping = task.kind === "asset_mapping";
    const isEvolution = task.kind === "code_evolution";
    const branch = queueMetricValue(task.metrics.branch);
    return <article className={`model-task-card ${task.status}`} key={task.task_id} title={task.title}>
      <div className="model-task-heading">
        <span className="model-task-status"><i />{modelTaskStatusLabels[task.status] ?? task.status}</span>
        <small>{taskSourceLabel(task.source)}</small>
      </div>
      <h4>{task.title}</h4>
      <p>{task.subtitle || task.kind}</p>
      {task.task_count > 1 && <div className="model-task-count">合并 {task.task_count} 个任务</div>}
      {isMapping && <div className="model-task-results mapping-results">
        <span>提出<strong>{queueMetricValue(task.metrics.proposed_count)}</strong></span>
        <span>通过<strong>{queueMetricValue(task.metrics.verified_count)}</strong></span>
        <span>拒绝<strong>{queueMetricValue(task.metrics.rejected_count)}</strong></span>
      </div>}
      {isEvolution && <div className="model-task-evolution">
        <span>目标：{queueMetricValue(task.metrics.target_metric)}</span>
        <span title={branch}>分支：{branch}</span>
      </div>}
      <div className="model-task-meta">
        <span>第 {Math.max(1, task.attempt)} 次尝试</span>
        <span>排队 {formatQueueDuration(task.queue_duration_ms)}</span>
        <span>执行 {formatQueueDuration(task.execution_duration_ms)}</span>
      </div>
      {task.error && <details className="model-task-error">
        <summary>最近错误</summary>
        <p>{task.error}</p>
      </details>}
      <time dateTime={task.updated_at}>{new Date(task.updated_at).toLocaleString("zh-CN")}</time>
    </article>;
  })}</div>;
}

export function UnifiedModelQueuePanel({ queue }: { queue: ModelQueueOverviewItem }) {
  const secondary = queue.counts.retrying + queue.counts.verifying;
  return <section className={`model-queue-panel unified-model-queue-panel ${queue.id}`}>
    <header>
      <div>
        <p className="eyebrow">{modelQueueEyebrows[queue.id]}</p>
        <h3>{queue.model} {queue.purpose}队列</h3>
        <small>{queue.binding}</small>
      </div>
      <span className={`model-queue-state ${queue.state}`}>{modelQueueStateLabels[queue.state] ?? queue.state}</span>
    </header>
    <div className="queue-metrics unified-queue-metrics" aria-live="polite">
      <span>待处理<strong>{queue.counts.queued}</strong></span>
      <span>运行<strong>{queue.counts.running}</strong></span>
      <span>重试/验证<strong>{secondary}</strong></span>
      <span>完成/失败<strong>{queue.counts.completed}/{queue.counts.failed}</strong></span>
      <span title={`样本 ${queue.metrics.queue_duration_sample_count}`}>平均排队<strong>{formatQueueDuration(queue.metrics.average_queue_duration_ms)}</strong></span>
      <span title={`样本 ${queue.metrics.execution_duration_sample_count}`}>平均执行<strong>{formatQueueDuration(queue.metrics.average_execution_duration_ms)}</strong></span>
    </div>
    <div className="model-queue-runtime">
      <span>模型等待<strong>{queue.counts.waiting_for_model}</strong></span>
      <span>槽位<strong>{queue.available}/{queue.capacity}</strong></span>
      <span>CPU<strong>{queue.threads} 线程</strong></span>
      <span>最长等待<strong>{formatQueueDuration(queue.metrics.longest_wait_ms)}</strong></span>
      <span>预计清空<strong>{formatQueueDuration(queue.metrics.estimated_clear_ms)}</strong></span>
    </div>
    {!queue.observable && <div className="page-error">模型推理槽位状态暂时不可用。</div>}
    {queue.error && <div className="page-error">{queue.error}</div>}
    {queue.truncated && <div className="page-message">队列过长，当前显示前 500 张任务卡。</div>}
    <ModelQueueTaskGrid queue={queue} />
  </section>;
}

export function QueuePage({ apiBase }: { apiBase: string }) {
  const [overview, setOverview] = useState<ModelQueueOverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const loadQueues = useCallback(async (signal?: AbortSignal, showLoading = false) => {
    if (showLoading) setLoading(true);
    try {
      const response = await fetch(`${apiBase}/api/v1/model-queue-overview?limit=500`, { signal });
      if (!response.ok) throw new Error(`模型队列请求失败（HTTP ${response.status}）`);
      setOverview(await response.json() as ModelQueueOverviewResponse);
      setError("");
    } catch (reason) {
      if (signal?.aborted) return;
      setError(reason instanceof Error ? reason.message : "模型队列请求失败");
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [apiBase]);

  useEffect(() => {
    const controller = new AbortController();
    void loadQueues(controller.signal, true);
    const timer = window.setInterval(
      () => void loadQueues(controller.signal),
      queueRefreshIntervalMs,
    );
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [loadQueues]);

  return <section className="app-page queue-page">
    <PageHeading eyebrow="ACTIVE MODEL PIPELINES" title="队列" copy="分别查看四个本地模型的业务任务与推理通道；页面每 5 秒自动更新。" />
    <div className="queue-toolbar">
      <span>四个模型队列独立加载；任一服务异常不会遮挡其他队列。</span>
      <button
        type="button"
        disabled={loading}
        onClick={() => void loadQueues(undefined, true)}
      >
        {loading ? "刷新中…" : "立即刷新"}
      </button>
    </div>
    {error && <div className="page-error">{error}</div>}
    {!overview && loading && <div className="page-message">正在读取四个模型队列…</div>}
    <div className="model-queue-columns">
      {overview?.queues.map((queue) => <UnifiedModelQueuePanel queue={queue} key={queue.id} />)}
    </div>
  </section>;
}

export type NewsBoardStatus =
  | "extracting"
  | "mapping"
  | "researching"
  | "revising"
  | "completed"
  | "insufficient_evidence"
  | "failed"
  | "pending";

export type NewsBoardItem = {
  id: string;
  title: string;
  summary: string;
  url: string;
  source_quality: string;
  published_at: string;
  observed_at: string;
  status: NewsBoardStatus;
  status_updated_at: string;
  events: Array<{ id: string; headline: string; event_type: string; priority: number }>;
  assets: Array<{ asset_id: string; symbol: string; name: string; market: string }>;
};

export type NewsBoardSource = {
  source: string;
  latest_published_at: string;
  item_count: number;
  items: NewsBoardItem[];
  error: string | null;
};

type NewsBoardResponse = {
  generated_at: string;
  per_source: number;
  total_sources: number;
  sources: NewsBoardSource[];
};

export const newsBoardStatusLabels: Record<NewsBoardStatus, string> = {
  extracting: "抽取中",
  mapping: "股票映射中",
  researching: "研究中",
  revising: "修订中",
  completed: "已完成",
  insufficient_evidence: "证据不足",
  failed: "失败",
  pending: "待处理",
};

const eventTypeLabels: Record<string, string> = {
  earnings: "业绩",
  product: "产品",
  regulation: "监管",
  m_and_a: "并购",
  management: "管理层",
  security: "安全",
  macro: "宏观",
  supply_chain: "供应链",
  tokenomics: "代币经济",
  other: "其他",
};

const sourceQualityLabels: Record<string, string> = {
  official: "官方",
  primary: "一手来源",
  professional: "专业财经",
  aggregator: "聚合来源",
  social: "社交来源",
};

export function NewsSourcePanel({ group }: { group: NewsBoardSource }) {
  return <section className="news-source-panel">
    <header>
      <div><p className="eyebrow">NEWS SOURCE</p><h3>{group.source}</h3></div>
      <span>最新 {group.item_count}/50 条</span>
    </header>
    {group.error && <div className="page-error">{group.error}</div>}
    {!group.error && !group.items.length && <div className="page-empty">该来源暂无新闻。</div>}
    <div className="news-source-items">
      {group.items.map((item) => {
        const eventType = item.events[0]?.event_type;
        return <article className={`news-board-item ${item.status}`} key={item.id}>
          <div className="news-board-item-heading">
            <span className="news-event-type">{eventType ? (eventTypeLabels[eventType] ?? eventType) : "待归类"}</span>
            <span className={`news-processing-status ${item.status}`}><i />{newsBoardStatusLabels[item.status]}</span>
          </div>
          <h4><a href={item.url} target="_blank" rel="noreferrer" title={item.title}>{item.title}</a></h4>
          <div className="news-board-meta">
            <time dateTime={item.published_at}>{new Date(item.published_at).toLocaleString("zh-CN")}</time>
            <span>{sourceQualityLabels[item.source_quality] ?? item.source_quality}</span>
          </div>
          {!!item.assets.length && <div className="news-board-assets" aria-label="关联标的">
            {item.assets.slice(0, 5).map((asset) => <span key={asset.asset_id} title={`${asset.name} · ${asset.market}`}>{asset.symbol}</span>)}
            {item.assets.length > 5 && <small>+{item.assets.length - 5}</small>}
          </div>}
        </article>;
      })}
    </div>
  </section>;
}

export function NewsPage({ apiBase }: { apiBase: string }) {
  const [board, setBoard] = useState<NewsBoardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = useCallback(async (signal?: AbortSignal, showLoading = false) => {
    if (showLoading) setLoading(true);
    try {
      const response = await fetch(`${apiBase}/api/v1/news-board?per_source=50`, { signal });
      if (!response.ok) throw new Error(`新闻看板请求失败（HTTP ${response.status}）`);
      setBoard(await response.json() as NewsBoardResponse);
      setError("");
    } catch (reason) {
      if (signal?.aborted) return;
      setError(reason instanceof Error ? reason.message : "新闻看板请求失败");
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [apiBase]);

  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal, true);
    const timer = window.setInterval(() => void load(controller.signal), newsBoardRefreshIntervalMs);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [load]);

  return <section className="app-page news-page">
    <PageHeading eyebrow="LIVE NEWS PIPELINE" title="新闻" copy="按来源查看最新 50 条新闻及其抽取、股票映射、研究和修订状态；页面每 5 秒自动更新。" />
    <div className="news-board-toolbar">
      <span>{board ? `${board.total_sources} 个来源 · 每来源最新 ${board.per_source} 条` : "正在读取新闻来源…"}</span>
      <button type="button" disabled={loading} onClick={() => void load(undefined, true)}>{loading ? "刷新中…" : "立即刷新"}</button>
    </div>
    {error && <div className="page-error">{error}</div>}
    {!board && loading && <div className="page-message">正在读取新闻状态…</div>}
    {board && !board.sources.length && <div className="page-empty">当前没有已入库新闻。</div>}
    {board && <div className="news-source-grid" data-columns={newsSourceDesktopColumns}>{board.sources.map((group) => <NewsSourcePanel group={group} key={group.source} />)}</div>}
  </section>;
}

type Recommendation = {
  id: string;
  run_id: string;
  asset: { symbol: string; name: string; market: string };
  rating: string;
  score: number;
  confidence: number;
  evidence_complete: boolean;
  as_of: string;
  bull_probability: number;
  base_probability: number;
  bear_probability: number;
  thesis: {
    summary: string;
    historical_context: string;
    financials_and_growth: string;
    products_or_protocol: string;
    competition: string;
    valuation_or_tokenomics: string;
    catalysts: string[];
    risks: string[];
    invalidation_conditions: string[];
  };
};

type ConclusionDetail = {
  recommendation: Recommendation;
  event: { headline: string } | null;
  news: Array<{ id: string; title: string; url: string; source: string }>;
  evidence: Array<{ id: string; claim: string; source_name: string; source_url: string; excerpt: string }>;
};

type ConclusionReference = { label: string; url: string; source: string };

export type FailedResearch = {
  kind: "asset" | "event";
  id: string;
  status: string;
  asset: { symbol: string; name: string; market: string } | null;
  event: { id: string; headline: string } | null;
  error: string | null;
  updated_at: string;
  retry_count: number;
  latest_retry: { id: string; status: string; updated_at: string } | null;
};

export function failedResearchRetryPath(item: Pick<FailedResearch, "kind" | "id">) {
  return item.kind === "asset"
    ? `/api/v1/research-runs/${item.id}/retry`
    : `/api/v1/event-research-runs/${item.id}/retry`;
}

function canonicalReferenceUrl(value: string): string {
  try {
    const url = new URL(value);
    url.hash = "";
    url.hostname = url.hostname.toLocaleLowerCase().replace(/^www\./, "");
    for (const key of [...url.searchParams.keys()]) {
      const normalized = key.toLocaleLowerCase();
      if (normalized.startsWith("utm_") || ["fbclid", "gclid", "ref", "referrer", "source"].includes(normalized)) {
        url.searchParams.delete(key);
      }
    }
    url.searchParams.sort();
    url.pathname = url.pathname === "/" ? "/" : url.pathname.replace(/\/+$/, "");
    return url.toString();
  } catch {
    return value.trim();
  }
}

function normalizedReferenceText(value: string): string {
  return value.normalize("NFKC").toLocaleLowerCase().replace(/[^a-z0-9\u3400-\u9fff]+/g, "");
}

export function conclusionReferences(
  detail: Pick<ConclusionDetail, "news" | "evidence">,
): ConclusionReference[] {
  const candidates: ConclusionReference[] = [
    ...detail.news.map((item) => ({ label: item.title, url: item.url, source: item.source })),
    ...detail.evidence.map((item) => ({
      label: item.claim,
      url: item.source_url,
      source: item.source_name,
    })),
  ];
  const seenUrls = new Set<string>();
  const seenLabels = new Set<string>();
  return candidates.filter((item) => {
    const urlKey = canonicalReferenceUrl(item.url);
    const labelKey = `${normalizedReferenceText(item.label)}|${normalizedReferenceText(item.source)}`;
    if ((urlKey && seenUrls.has(urlKey)) || (labelKey && seenLabels.has(labelKey))) return false;
    if (urlKey) seenUrls.add(urlKey);
    if (labelKey) seenLabels.add(labelKey);
    return true;
  });
}

const ratingLabels: Record<string, string> = {
  strongly_bullish: "强烈看多", bullish: "看多", watch: "观察", bearish: "看空", strongly_bearish: "强烈看空",
};

export function ConclusionScore({
  score, rating, confidence, evidenceComplete,
}: {
  score: number; rating: string; confidence: number; evidenceComplete: boolean;
}) {
  return <div className="conclusion-score">
    <strong>方向评分：{score > 0 ? "+" : ""}{score}</strong>
    <span>评级：{ratingLabels[rating] || rating}</span>
    <small>置信度 {Math.round(confidence * 100)}% · {evidenceComplete ? "证据完整" : "证据不足"}</small>
  </div>;
}

export function ConclusionsPage({ apiBase }: { apiBase: string }) {
  const [filters, setFilters] = useState({ q: "", market: "", rating: "", evidence_status: "" });
  const [items, setItems] = useState<Recommendation[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [selected, setSelected] = useState<ConclusionDetail | null>(null);
  const [error, setError] = useState("");
  const [failedItems, setFailedItems] = useState<FailedResearch[]>([]);
  const [retryingId, setRetryingId] = useState("");
  const [retryMessage, setRetryMessage] = useState("");

  async function load(append = false) {
    const params = new URLSearchParams({ ...filters, limit: "20" });
    if (append && cursor) params.set("cursor", cursor);
    try {
      const response = await fetch(`${apiBase}/api/v1/conclusions?${params}`);
      if (!response.ok) throw new Error("结论请求失败");
      const payload = await response.json() as { items: Recommendation[]; next_cursor: string | null };
      setItems((current) => append ? [...current, ...payload.items] : payload.items);
      setCursor(payload.next_cursor); setError("");
    } catch (reason) { setError(reason instanceof Error ? reason.message : "结论请求失败"); }
  }
  async function loadFailures() {
    try {
      const response = await fetch(`${apiBase}/api/v1/failed-research-runs?limit=50`);
      if (!response.ok) throw new Error("失败研究记录请求失败");
      setFailedItems(await response.json() as FailedResearch[]);
    } catch (reason) {
      setRetryMessage(reason instanceof Error ? reason.message : "失败研究记录请求失败");
    }
  }
  useEffect(() => { load(); loadFailures(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  async function retry(item: FailedResearch) {
    setRetryingId(item.id); setRetryMessage("");
    try {
      const response = await fetch(`${apiBase}${failedResearchRetryPath(item)}`, { method: "POST" });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "重新执行失败");
      setRetryMessage(`已重新排队：${item.asset?.symbol || item.event?.headline || item.id}`);
      await loadFailures();
    } catch (reason) {
      setRetryMessage(reason instanceof Error ? reason.message : "重新执行失败");
    } finally { setRetryingId(""); }
  }

  async function open(item: Recommendation) {
    const response = await fetch(`${apiBase}/api/v1/conclusions/${item.id}`);
    if (response.ok) setSelected(await response.json() as ConclusionDetail);
  }
  return (
    <section className="app-page conclusions-page">
      <PageHeading eyebrow="RESEARCH OUTCOMES" title="研究结论" copy="仅展示最终标的建议；关联新闻和证据保留为可追溯依据。" />
      <form className="page-filters" onSubmit={(e) => { e.preventDefault(); load(); }}>
        <input aria-label="搜索结论" placeholder="标的、代码或核心观点" value={filters.q} onChange={(e) => setFilters({ ...filters, q: e.target.value })} />
        <select aria-label="市场" value={filters.market} onChange={(e) => setFilters({ ...filters, market: e.target.value })}><option value="">全部市场</option><option value="US">美股</option><option value="CN">A股</option><option value="HK">港股</option><option value="CRYPTO">加密</option></select>
        <select aria-label="评级" value={filters.rating} onChange={(e) => setFilters({ ...filters, rating: e.target.value })}><option value="">全部评级</option>{Object.entries(ratingLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select>
        <select aria-label="证据状态" value={filters.evidence_status} onChange={(e) => setFilters({ ...filters, evidence_status: e.target.value })}><option value="">全部证据</option><option value="complete">证据完整</option><option value="incomplete">证据不足</option></select>
        <button>筛选</button>
      </form>
      {error && <div className="page-error">{error}</div>}
      <section className="failed-research-panel">
        <div className="failed-research-heading"><div><p className="eyebrow">RETRY QUEUE</p><h3>历史失败研究</h3></div><button type="button" onClick={loadFailures}>刷新</button></div>
        <p className="failed-research-copy">重新执行会创建新任务，保留原失败记录，并使用当前数据源和模型配置。</p>
        {retryMessage && <div className={retryMessage.includes("失败") ? "page-error" : "page-message"}>{retryMessage}</div>}
        <div className="failed-research-list">
          {failedItems.map((item) => {
            const retryActive = ["queued", "running", "verifying"].includes(item.latest_retry?.status || "");
            return <article key={`${item.kind}-${item.id}`} className="failed-research-item">
              <div><span>{item.kind === "asset" ? "标的研究" : "事件研报"} · {new Date(item.updated_at).toLocaleString("zh-CN")}</span><strong>{item.asset ? `${item.asset.symbol} · ${item.asset.name}` : item.event?.headline || item.id}</strong><p>{item.error || "未记录错误详情"}</p>{item.latest_retry && <small>最近重跑：{item.latest_retry.status} · {new Date(item.latest_retry.updated_at).toLocaleString("zh-CN")}</small>}</div>
              <button type="button" disabled={retryingId === item.id || retryActive} onClick={() => retry(item)}>{retryingId === item.id ? "正在排队…" : retryActive ? "重跑中" : "重新执行"}</button>
            </article>;
          })}
          {!failedItems.length && !retryMessage && <div className="page-empty">当前没有失败研究。</div>}
        </div>
      </section>
      <div className="conclusion-list">
        {items.map((item) => <button type="button" className="conclusion-card" key={item.id} onClick={() => open(item)}>
          <div><span>{item.asset.market} · {new Date(item.as_of).toLocaleString("zh-CN")}</span><strong>{item.asset.symbol} · {item.asset.name}</strong><p>{item.thesis.summary}</p></div>
          <ConclusionScore score={item.score} rating={item.rating} confidence={item.confidence} evidenceComplete={item.evidence_complete} />
        </button>)}
        {!items.length && !error && <div className="page-empty">当前筛选范围内没有最终标的建议。</div>}
      </div>
      {cursor && <button className="load-more" type="button" onClick={() => load(true)}>加载更多</button>}
      {selected && <div className="modal-backdrop" onClick={() => setSelected(null)}><article className="modal conclusion-modal" onClick={(e) => e.stopPropagation()}>
        <button className="close" onClick={() => setSelected(null)}>×</button>
        <p className="eyebrow">{selected.recommendation.asset.market} · {selected.recommendation.asset.symbol}</p><h2>{selected.recommendation.asset.name}</h2>
        <div className="probability-grid"><span>看多 <strong>{Math.round(selected.recommendation.bull_probability * 100)}%</strong></span><span>基准 <strong>{Math.round(selected.recommendation.base_probability * 100)}%</strong></span><span>看空 <strong>{Math.round(selected.recommendation.bear_probability * 100)}%</strong></span></div>
        <h3>核心观点</h3><p>{selected.recommendation.thesis.summary}</p>
        {selected.recommendation.thesis.historical_context && <><h3>历史背景</h3><p>{selected.recommendation.thesis.historical_context}</p></>}
        <h3>催化剂</h3><ul>{selected.recommendation.thesis.catalysts.map((item) => <li key={item}>{item}</li>)}</ul>
        <h3>风险</h3><ul>{selected.recommendation.thesis.risks.map((item) => <li key={item}>{item}</li>)}</ul>
        <h3>失效条件</h3><ul>{selected.recommendation.thesis.invalidation_conditions.map((item) => <li key={item}>{item}</li>)}</ul>
        {selected.event && <><h3>关联事件</h3><p>{selected.event.headline}</p></>}
        <h3>新闻与证据</h3><div className="evidence-links">{conclusionReferences(selected).map((item) => <a key={`${item.url}-${item.label}`} href={item.url} target="_blank" rel="noreferrer"><strong>{item.label}</strong><span>{item.source}</span></a>)}</div>
      </article></div>}
    </section>
  );
}

type SourceFilterConfig = {
  enabled: boolean;
  whitelist_keywords: string[];
  blacklist_keywords: string[];
  retained_log_count: number;
  last_filtered_at: string | null;
  updated_at: string | null;
};

type SourceFilterLog = {
  id: string;
  source: string;
  title: string;
  url: string;
  matched_keyword: string;
  published_at: string;
  first_filtered_at: string;
  last_filtered_at: string;
  hit_count: number;
};

const defaultSourceFilter: SourceFilterConfig = {
  enabled: true,
  whitelist_keywords: [],
  blacklist_keywords: ["天气"],
  retained_log_count: 0,
  last_filtered_at: null,
  updated_at: null,
};

export function parseFilterKeywords(value: string) {
  const seen = new Set<string>();
  return value.split(/[\r\n,，]+/).map((item) => item.trim()).filter((item) => {
    const key = item.toLocaleLowerCase();
    if (!item || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function SourceFilterPage({ apiBase }: { apiBase: string }) {
  const [config, setConfig] = useState<SourceFilterConfig>(defaultSourceFilter);
  const [enabled, setEnabled] = useState(true);
  const [whitelist, setWhitelist] = useState("");
  const [blacklist, setBlacklist] = useState("天气");
  const [logs, setLogs] = useState<SourceFilterLog[]>([]);
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(true);

  function applyConfig(payload: SourceFilterConfig) {
    setConfig(payload);
    setEnabled(payload.enabled);
    setWhitelist(payload.whitelist_keywords.join("\n"));
    setBlacklist(payload.blacklist_keywords.join("\n"));
  }
  async function load() {
    setLoading(true);
    try {
      const [configResponse, logResponse] = await Promise.all([
        fetch(`${apiBase}/api/v1/source-filter`),
        fetch(`${apiBase}/api/v1/source-filter/logs?limit=100`),
      ]);
      if (!configResponse.ok || !logResponse.ok) throw new Error("读取过滤配置失败");
      applyConfig(await configResponse.json() as SourceFilterConfig);
      setLogs((await logResponse.json() as { items: SourceFilterLog[] }).items);
      setMessage("");
    } catch (reason) {
      setMessage(reason instanceof Error ? reason.message : "读取过滤配置失败");
    } finally { setLoading(false); }
  }
  useEffect(() => { load(); }, [apiBase]); // eslint-disable-line react-hooks/exhaustive-deps
  async function save(event: FormEvent) {
    event.preventDefault();
    const response = await fetch(`${apiBase}/api/v1/source-filter`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        enabled,
        whitelist_keywords: parseFilterKeywords(whitelist),
        blacklist_keywords: parseFilterKeywords(blacklist),
      }),
    });
    const body = await response.json();
    if (!response.ok) { setMessage(body.detail?.[0]?.msg || body.detail || "保存失败"); return; }
    applyConfig(body as SourceFilterConfig);
    setMessage("过滤规则已保存，将从下一轮新闻扫描开始生效。");
  }
  async function reset() {
    if (!window.confirm("恢复默认过滤规则？白名单将清空，黑名单恢复为“天气”。")) return;
    const response = await fetch(`${apiBase}/api/v1/source-filter`, { method: "DELETE" });
    if (!response.ok) { setMessage("恢复默认失败。"); return; }
    applyConfig(await response.json() as SourceFilterConfig);
    setMessage("已恢复默认过滤规则。");
  }
  const whitelistCount = parseFilterKeywords(whitelist).length;
  const blacklistCount = parseFilterKeywords(blacklist).length;
  return <section className="app-page source-filter-page">
    <PageHeading eyebrow="PRE-RESEARCH GATE" title="数据源过滤" copy="在新闻进入事件提取和研究队列前检查标题，减少与投资无关的模型调用。" />
    <div className="filter-metrics">
      <div><span>过滤状态</span><strong className={enabled ? "enabled" : "disabled"}>{enabled ? "已启用" : "已关闭"}</strong></div>
      <div><span>白名单</span><strong>{whitelistCount}</strong><small>优先放行</small></div>
      <div><span>黑名单</span><strong>{blacklistCount}</strong><small>命中忽略</small></div>
      <div><span>保留记录</span><strong>{config.retained_log_count}</strong><small>最近 30 天</small></div>
    </div>
    <form className="source-filter-form" onSubmit={save}>
      <label className="filter-master-switch"><input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} /><span><strong>启用新闻标题过滤</strong><small>关闭后所有新标题均进入原有流程。</small></span></label>
      <div className="keyword-panels">
        <label><span><strong>白名单关键字</strong><small>{whitelistCount} / 200 · 命中后优先放行</small></span><textarea aria-label="白名单关键字" value={whitelist} placeholder="例如：苹果供应链" onChange={(e) => setWhitelist(e.target.value)} /></label>
        <label><span><strong>黑名单关键字</strong><small>{blacklistCount} / 200 · 命中后忽略</small></span><textarea aria-label="黑名单关键字" value={blacklist} placeholder="例如：天气" onChange={(e) => setBlacklist(e.target.value)} /></label>
      </div>
      <p className="filter-note">每行或逗号分隔一个关键字；忽略英文大小写，仅匹配新闻扫描标题。白名单和黑名单同时命中时，白名单优先。</p>
      <div className="card-actions"><button type="submit">保存规则</button><button type="button" onClick={load}>刷新</button><button type="button" className="danger" onClick={reset}>恢复默认</button></div>
    </form>
    {message && <div className={message.includes("失败") ? "page-error" : "page-message"}>{message}</div>}
    <section className="filter-audit" aria-labelledby="filter-audit-title">
      <div className="filter-audit-heading"><div><p className="eyebrow">FILTER AUDIT</p><h3 id="filter-audit-title">最近过滤记录</h3></div><span>{loading ? "读取中…" : `${logs.length} 条`}</span></div>
      <div className="filter-log-list">{logs.map((item) => <article key={item.id}>
        <div><span>{item.source} · {new Date(item.last_filtered_at).toLocaleString("zh-CN")}</span><h4><a href={item.url} target="_blank" rel="noreferrer">{item.title}</a></h4></div>
        <div><strong>命中：{item.matched_keyword}</strong><small>累计 {item.hit_count} 次</small></div>
      </article>)}</div>
      {!loading && logs.length === 0 && <div className="page-empty">还没有新闻标题被过滤。</div>}
    </section>
  </section>;
}

type McpSource = {
  id: string; name: string; url: string; description: string; priority: number; enabled: boolean;
  managed: boolean; auth_type: string; auth_header_name: string | null; secret_configured: boolean;
  discovered_tools: Array<{ name: string; description: string; input_schema: unknown; output_schema: unknown }>;
  tool_mappings: Record<string, unknown>; last_status: string; last_error: string | null;
  group_id: string;
};

type SourceDraft = {
  name: string; url: string; description: string; priority: number; enabled: boolean;
  auth_type: string; auth_header_name: string; secret: string; clear_secret: boolean;
  tool_mappings: string; group_id: string;
};

export type FactSourceGroup = {
  id: string; badge: string; name: string; description: string; tone: string;
  status: string; configured_count: number; mcp_count: number; config_source: string;
  config: Record<string, unknown>; mcp_sources: McpSource[];
};

export type GroupDraft = Record<string, string | number | boolean>;

const blankSource: SourceDraft = {
  name: "", url: "", description: "", priority: 50, enabled: true,
  auth_type: "none", auth_header_name: "X-API-Key", secret: "", clear_secret: false,
  tool_mappings: "{}", group_id: "other",
};

export const factSourceGroupDefinitions = [
  { id: "fmp", badge: "US", name: "FMP 美股数据", description: "美股行情、财务报表、估值指标与公司基础数据", tone: "amber" },
  { id: "sec", badge: "OFFICIAL", name: "SEC 官方文件", description: "SEC EDGAR 监管文件与公司申报记录", tone: "cyan" },
  { id: "cn_news", badge: "CN / NEWS", name: "A股与新闻", description: "AkShare 主数据、市场新闻、公告与 RSS 事实来源", tone: "amber" },
  { id: "crypto", badge: "CRYPTO", name: "数字资产", description: "CoinGecko、DeFiLlama 与 CCXT Kraken 交叉验证", tone: "cyan" },
  { id: "search", badge: "WEB / SEARCH", name: "网络搜索与交叉验证", description: "跨市场网页搜索、独立来源验证与实时补充证据", tone: "mint" },
] as const;

export const factSourceGroupOptions = [
  ...factSourceGroupDefinitions.map(({ id, name }) => ({ id, name })),
  { id: "other", name: "其他数据源" },
];

const initialFactGroups: FactSourceGroup[] = factSourceGroupDefinitions.map((item) => ({
  ...item,
  status: "checking",
  configured_count: 0,
  mcp_count: 0,
  config_source: "environment",
  config: {},
  mcp_sources: [],
}));

function groupDraft(group: FactSourceGroup): GroupDraft {
  const config = group.config;
  if (group.id === "fmp") return {
    base_url: String(config.base_url || ""),
    access_token: "",
    clear_access_token: false,
    rate_limit_per_minute: Number(config.rate_limit_per_minute || 240),
    news_lookback_hours: Number(config.news_lookback_hours || 12),
  };
  if (group.id === "sec") return { identity: String(config.identity || "") };
  if (group.id === "cn_news") return {
    akshare_asset_master_enabled: Boolean(config.akshare_asset_master_enabled),
    akshare_ipv4_only: Boolean(config.akshare_ipv4_only),
    rss_feed_urls: Array.isArray(config.rss_feed_urls) ? config.rss_feed_urls.join("\n") : "",
    official_rss_feed_urls: Array.isArray(config.official_rss_feed_urls) ? config.official_rss_feed_urls.join("\n") : "",
  };
  if (group.id === "crypto") return {
    coingecko_base_url: String(config.coingecko_base_url || ""),
    defillama_base_url: String(config.defillama_base_url || ""),
  };
  if (group.id === "search") return { timeout_seconds: Number(config.timeout_seconds || 20) };
  return {};
}

function sourceDraft(source?: McpSource): SourceDraft {
  return source ? {
    name: source.name, url: source.url, description: source.description, priority: source.priority,
    enabled: source.enabled, auth_type: source.auth_type,
    auth_header_name: source.auth_header_name || "X-API-Key", secret: "", clear_secret: false,
    tool_mappings: JSON.stringify(source.tool_mappings, null, 2), group_id: source.group_id,
  } : { ...blankSource };
}

function splitUrls(value: string | number | boolean | undefined) {
  return String(value || "").split(/\r?\n|,/).map((item) => item.trim()).filter(Boolean);
}

function groupSavePayload(groupId: string, draft: GroupDraft) {
  if (groupId === "fmp") return {
    base_url: draft.base_url,
    access_token: draft.access_token || null,
    clear_access_token: Boolean(draft.clear_access_token),
    rate_limit_per_minute: Number(draft.rate_limit_per_minute),
    news_lookback_hours: Number(draft.news_lookback_hours),
  };
  if (groupId === "sec") return { identity: draft.identity || "" };
  if (groupId === "cn_news") return {
    akshare_asset_master_enabled: Boolean(draft.akshare_asset_master_enabled),
    akshare_ipv4_only: Boolean(draft.akshare_ipv4_only),
    rss_feed_urls: splitUrls(draft.rss_feed_urls),
    official_rss_feed_urls: splitUrls(draft.official_rss_feed_urls),
  };
  if (groupId === "crypto") return {
    coingecko_base_url: draft.coingecko_base_url,
    defillama_base_url: draft.defillama_base_url,
  };
  return { timeout_seconds: Number(draft.timeout_seconds) };
}

export function firstUnhealthyGroup(groups: Array<Pick<FactSourceGroup, "id" | "status">>) {
  return groups.find((group) => group.status !== "healthy")?.id || null;
}

export function NativeConfigEditor({ group, draft, onDraft }: {
  group: FactSourceGroup; draft: GroupDraft;
  onDraft: (value: GroupDraft) => void;
}) {
  if (group.id === "fmp") return <div className="native-config-fields">
    <label>FMP REST 地址<input type="url" value={String(draft.base_url || "")} onChange={(e) => onDraft({ ...draft, base_url: e.target.value })} /></label>
    <label>新 REST Token<input type="password" value={String(draft.access_token || "")} placeholder={group.config.access_token_configured ? "已配置，留空则保留" : "尚未配置"} onChange={(e) => onDraft({ ...draft, access_token: e.target.value })} /></label>
    <label>每分钟请求上限<input type="number" min="1" max="300" value={Number(draft.rate_limit_per_minute)} onChange={(e) => onDraft({ ...draft, rate_limit_per_minute: Number(e.target.value) })} /></label>
    <label>新闻回看小时<input type="number" min="1" max="168" value={Number(draft.news_lookback_hours)} onChange={(e) => onDraft({ ...draft, news_lookback_hours: Number(e.target.value) })} /></label>
    <label className="inline-check danger-check"><input type="checkbox" checked={Boolean(draft.clear_access_token)} onChange={(e) => onDraft({ ...draft, clear_access_token: e.target.checked })} />清除 REST Token</label>
    <p className="config-note">REST Token：{group.config.access_token_configured ? `已配置（${group.config.access_token_source}）` : "未配置"}。独立 FMP MCP 的上游 Token 由服务器环境管理，修改后需要部署更新。</p>
  </div>;
  if (group.id === "sec") return <div className="native-config-fields single">
    <label>SEC Identity<input value={String(draft.identity || "")} placeholder="机构/姓名 contact@example.com" onChange={(e) => onDraft({ ...draft, identity: e.target.value })} /></label>
  </div>;
  if (group.id === "cn_news") return <div className="native-config-fields">
    <label className="inline-check"><input type="checkbox" checked={Boolean(draft.akshare_asset_master_enabled)} onChange={(e) => onDraft({ ...draft, akshare_asset_master_enabled: e.target.checked })} />启用 AkShare 主数据</label>
    <label className="inline-check"><input type="checkbox" checked={Boolean(draft.akshare_ipv4_only)} onChange={(e) => onDraft({ ...draft, akshare_ipv4_only: e.target.checked })} />AkShare 仅使用 IPv4</label>
    <label>RSS 地址（每行一个）<textarea value={String(draft.rss_feed_urls || "")} onChange={(e) => onDraft({ ...draft, rss_feed_urls: e.target.value })} /></label>
    <label>官方 RSS 地址（每行一个）<textarea value={String(draft.official_rss_feed_urls || "")} onChange={(e) => onDraft({ ...draft, official_rss_feed_urls: e.target.value })} /></label>
  </div>;
  if (group.id === "crypto") return <div className="native-config-fields">
    <label>CoinGecko 地址<input type="url" value={String(draft.coingecko_base_url || "")} onChange={(e) => onDraft({ ...draft, coingecko_base_url: e.target.value })} /></label>
    <label>DeFiLlama 地址<input type="url" value={String(draft.defillama_base_url || "")} onChange={(e) => onDraft({ ...draft, defillama_base_url: e.target.value })} /></label>
    <label>CCXT 交叉验证<input value="Kraken · 固定只读" readOnly /></label>
  </div>;
  if (group.id === "search") return <div className="native-config-fields single">
    <label>搜索与 MCP 超时（秒）<input type="number" min="2" max="120" value={Number(draft.timeout_seconds)} onChange={(e) => onDraft({ ...draft, timeout_seconds: Number(e.target.value) })} /></label>
  </div>;
  return <p className="config-note">此组没有内置配置，仅管理自定义 MCP 来源。</p>;
}

export function SourcesPage({ apiBase }: { apiBase: string }) {
  const [groups, setGroups] = useState<FactSourceGroup[]>(initialFactGroups);
  const [drafts, setDrafts] = useState<Record<string, GroupDraft>>({});
  const [expanded, setExpanded] = useState<string[]>([]);
  const [editing, setEditing] = useState<string | "new" | null>(null);
  const [draft, setDraft] = useState<SourceDraft>(sourceDraft());
  const [message, setMessage] = useState("");
  const [groupMessages, setGroupMessages] = useState<Record<string, string>>({});
  const headers = { "Content-Type": "application/json" };
  async function load() {
    const response = await fetch(`${apiBase}/api/v1/admin/fact-source-groups`, { headers });
    if (!response.ok) { setMessage("无法读取事实数据源配置。"); return; }
    const payload = await response.json() as FactSourceGroup[];
    setGroups(payload);
    setDrafts(Object.fromEntries(payload.map((group) => [group.id, groupDraft(group)])));
    setExpanded((current) => {
      if (current.length) return current;
      const firstGroup = firstUnhealthyGroup(payload);
      return firstGroup ? [firstGroup] : [];
    });
    setMessage("");
  }
  useEffect(() => { load(); }, [apiBase]); // eslint-disable-line react-hooks/exhaustive-deps
  function setGroupMessage(groupId: string, value: string) { setGroupMessages((current) => ({ ...current, [groupId]: value })); }
  function toggleGroup(groupId: string) { setExpanded((current) => current.includes(groupId) ? current.filter((item) => item !== groupId) : [...current, groupId]); }
  async function action(id: string, kind: "test" | "discover") {
    const owner = groups.find((group) => group.mcp_sources.some((item) => item.id === id));
    if (owner) setGroupMessage(owner.id, "正在连接 MCP 来源…");
    const response = await fetch(`${apiBase}/api/v1/admin/mcp-sources/${id}/${kind}`, { method: "POST", headers });
    const body = await response.json();
    if (owner) setGroupMessage(owner.id, response.ok ? `${kind === "test" ? "连接测试" : "工具发现"}完成。` : body.detail || "操作失败");
    await load();
  }
  async function toggle(item: McpSource) {
    const response = await fetch(`${apiBase}/api/v1/admin/mcp-sources/${item.id}/enabled`, { method: "PATCH", headers, body: JSON.stringify({ enabled: !item.enabled }) });
    setGroupMessage(item.group_id, response.ok ? `来源已${item.enabled ? "关闭" : "启用"}。` : "更新来源状态失败。");
    if (response.ok) await load();
  }
  async function save(event: FormEvent) {
    event.preventDefault();
    if (draft.clear_secret && !window.confirm("确认清除该 MCP 来源的现有凭据？清除后需要重新配置才能恢复认证。")) return;
    try {
      const payload = { ...draft, tool_mappings: JSON.parse(draft.tool_mappings), secret: draft.secret || null };
      const url = editing === "new" ? `${apiBase}/api/v1/admin/mcp-sources` : `${apiBase}/api/v1/admin/mcp-sources/${editing}`;
      const response = await fetch(url, { method: editing === "new" ? "POST" : "PUT", headers, body: JSON.stringify(payload) });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || "保存失败");
      setEditing(null);
      setExpanded((current) => current.includes(draft.group_id) ? current : [...current, draft.group_id]);
      setGroupMessage(draft.group_id, "来源配置已保存并热生效。");
      await load();
    } catch (reason) { setGroupMessage(draft.group_id, reason instanceof Error ? reason.message : "保存失败"); }
  }
  async function remove(item: McpSource) {
    if (item.managed || !window.confirm(`删除数据源 ${item.name}？`)) return;
    const response = await fetch(`${apiBase}/api/v1/admin/mcp-sources/${item.id}`, { method: "DELETE", headers });
    setGroupMessage(item.group_id, response.ok ? "来源已删除。" : "删除来源失败。");
    if (response.ok) await load();
  }
  async function saveGroup(group: FactSourceGroup) {
    if (group.id === "fmp" && drafts[group.id]?.clear_access_token
      && !window.confirm("确认清除 FMP REST Token？清除后新的 FMP REST 请求将停止使用该凭据。")) return;
    const response = await fetch(`${apiBase}/api/v1/admin/fact-source-groups/${group.id}`, { method: "PUT", headers, body: JSON.stringify(groupSavePayload(group.id, drafts[group.id] || {})) });
    const body = await response.json();
    setGroupMessage(group.id, response.ok ? "配置已保存，新任务将使用最新配置。" : body.detail || "保存失败");
    if (response.ok) await load();
  }
  async function testGroup(group: FactSourceGroup) {
    setGroupMessage(group.id, "正在测试组内配置与来源…");
    const response = await fetch(`${apiBase}/api/v1/admin/fact-source-groups/${group.id}/test`, { method: "POST", headers });
    const body = await response.json();
    setGroupMessage(group.id, response.ok && body.ok ? "组内配置与来源连接正常。" : body.detail || body.native?.detail || "部分配置或来源测试失败。");
    await load();
  }
  async function resetGroup(group: FactSourceGroup) {
    if (!window.confirm(`恢复 ${group.name} 的环境默认配置？数据库覆盖将被删除。`)) return;
    const response = await fetch(`${apiBase}/api/v1/admin/fact-source-groups/${group.id}`, { method: "DELETE", headers });
    setGroupMessage(group.id, response.ok ? "已恢复环境默认配置。" : "恢复默认失败。");
    if (response.ok) await load();
  }
  return <section className="app-page sources-page">
    <PageHeading eyebrow="RESEARCH DATA FABRIC" title="数据源" copy="按事实领域统一查看内置配置、运行状态和所属 MCP；保存后从下一项任务开始生效。" />
    {message && <div className="page-message">{message}</div>}
    <div className="page-toolbar"><button type="button" onClick={() => { setEditing("new"); setDraft(sourceDraft()); }}>新增 MCP 来源</button><button type="button" onClick={load}>刷新</button></div>
    <div className="fact-source-groups">{groups.map((group) => {
      const open = expanded.includes(group.id);
      const groupDraftValue = drafts[group.id] || groupDraft(group);
      return <article className={`fact-source-group ${group.tone} ${open ? "open" : ""}`} key={group.id}>
        <button type="button" className="fact-group-summary" aria-expanded={open} onClick={() => toggleGroup(group.id)}>
          <span className="fact-source-badge">{group.badge}</span>
          <span className="fact-group-title"><strong>{group.name}</strong><small>{group.description}</small></span>
          <span className="fact-group-counts"><small>{group.configured_count} 项配置</small><small>{group.mcp_count} 个 MCP</small></span>
          <span className={`group-status ${group.status}`}><i />{{ healthy: "正常", failed: "异常", pending: "待配置", checking: "检测中" }[group.status] || group.status}</span>
          <i className="group-chevron">{open ? "−" : "+"}</i>
        </button>
        {open && <div className="fact-group-detail">
          {groupMessages[group.id] && <div className={groupMessages[group.id].includes("失败") || groupMessages[group.id].includes("异常") ? "page-error" : "page-message"}>{groupMessages[group.id]}</div>}
          <section className="native-config-panel">
            <div className="group-section-heading"><div><span>NATIVE CONFIG</span><h4>内置配置</h4></div><small>{group.config_source === "database" ? "数据库覆盖" : "环境配置"}</small></div>
            <NativeConfigEditor group={group} draft={groupDraftValue} onDraft={(value) => setDrafts((current) => ({ ...current, [group.id]: value }))} />
            {group.id !== "other" && <div className="card-actions"><button type="button" onClick={() => testGroup(group)}>测试配置</button><button type="button" onClick={() => saveGroup(group)}>保存</button><button type="button" className="danger" onClick={() => resetGroup(group)}>恢复环境默认</button></div>}
          </section>
          <section className="group-mcp-panel">
            <div className="group-section-heading"><div><span>STREAMABLE HTTP</span><h4>MCP 来源</h4></div><small>{group.mcp_count} 个来源</small></div>
            <div className="source-list">{group.mcp_sources.map((item) => <article className="source-card" key={item.id}>
              <div className="source-card-main"><div><span className={`health-dot ${item.last_status}`} /> <strong>{item.name}</strong>{item.managed && <small>内置</small>}<p>{item.description || item.url}</p><code>{item.url}</code></div><div><span>优先级 {item.priority}</span><span>{item.discovered_tools.length} 个工具</span><span>{item.secret_configured ? "凭据已配置" : "无凭据"}</span></div></div>
              {item.last_error && <p className="page-error">{item.last_error}</p>}
              <div className="card-actions"><button type="button" onClick={() => toggle(item)}>{item.enabled ? "关闭" : "启用"}</button><button type="button" onClick={() => action(item.id, "test")}>连接测试</button><button type="button" onClick={() => action(item.id, "discover")}>工具发现</button><button type="button" onClick={() => { setEditing(item.id); setDraft(sourceDraft(item)); }}>编辑</button>{!item.managed && <button type="button" className="danger" onClick={() => remove(item)}>删除</button>}</div>
              {item.discovered_tools.length > 0 && <details><summary>已发现工具与 Schema</summary>{item.discovered_tools.map((tool) => <pre key={tool.name}>{tool.name}\n{tool.description}\n{JSON.stringify(tool.input_schema, null, 2)}</pre>)}</details>}
            </article>)}{group.mcp_sources.length === 0 && <div className="group-empty">此组尚无 MCP 来源，可使用“新增 MCP 来源”添加。</div>}</div>
          </section>
        </div>}
      </article>;
    })}</div>
    {editing && <div className="modal-backdrop" onClick={() => setEditing(null)}><form className="modal source-editor" onSubmit={save} onClick={(e) => e.stopPropagation()}><button type="button" className="close" onClick={() => setEditing(null)}>×</button><h2>{editing === "new" ? "新增数据源" : "编辑数据源"}</h2>
      <label>所属事实组<select required value={draft.group_id} disabled={editing !== "new" && groups.some((group) => group.mcp_sources.some((item) => item.id === editing && item.managed))} onChange={(e) => setDraft({ ...draft, group_id: e.target.value })}>{factSourceGroupOptions.map((group) => <option key={group.id} value={group.id}>{group.name}</option>)}</select></label><label>名称<input required value={draft.name} onChange={(e) => setDraft({ ...draft, name: e.target.value })} /></label><label>Streamable HTTP URL<input required type="url" value={draft.url} onChange={(e) => setDraft({ ...draft, url: e.target.value })} /></label><label>描述<textarea value={draft.description} onChange={(e) => setDraft({ ...draft, description: e.target.value })} /></label><label>优先级<input type="number" min="0" max="1000" value={draft.priority} onChange={(e) => setDraft({ ...draft, priority: Number(e.target.value) })} /></label><label>认证<select value={draft.auth_type} onChange={(e) => setDraft({ ...draft, auth_type: e.target.value })}><option value="none">无</option><option value="bearer">Bearer</option><option value="api_key_header">API Key Header</option></select></label>{draft.auth_type === "api_key_header" && <label>Header 名称<input value={draft.auth_header_name} onChange={(e) => setDraft({ ...draft, auth_header_name: e.target.value })} /></label>}{draft.auth_type !== "none" && <><label>新凭据<input type="password" value={draft.secret} placeholder="留空则保留现有凭据" onChange={(e) => setDraft({ ...draft, secret: e.target.value })} /></label><label className="inline-check"><input type="checkbox" checked={draft.clear_secret} onChange={(e) => setDraft({ ...draft, clear_secret: e.target.checked })} />清除现有凭据</label></>}<label>用途映射 JSON<textarea className="json-editor" value={draft.tool_mappings} onChange={(e) => setDraft({ ...draft, tool_mappings: e.target.value })} /></label><button type="submit">保存配置</button>
    </form></div>}
  </section>;
}

type SearchItem = { title: string; url: string; snippet: string; source: string; sources?: string[]; domain: string; published_at: string | null };

export function searchSourceLabel(item: Pick<SearchItem, "source" | "sources">): string {
  const sources = item.sources?.length ? item.sources : [item.source];
  return [...new Set(sources)].join(" + ");
}

export function isSearchSource(item: Pick<McpSource, "enabled" | "tool_mappings">): boolean {
  return item.enabled && ("web_search" in item.tool_mappings || "news_search" in item.tool_mappings);
}

export function SearchPage({ apiBase }: { apiBase: string }) {
  const [sources, setSources] = useState<McpSource[]>([]);
  const [query, setQuery] = useState(""); const [sourceId, setSourceId] = useState("");
  const [language, setLanguage] = useState("zh-CN"); const [timeRange, setTimeRange] = useState(""); const [limit, setLimit] = useState(10);
  const [items, setItems] = useState<SearchItem[]>([]); const [errors, setErrors] = useState<Array<{ source: string; error: string }>>([]); const [loading, setLoading] = useState(false);
  const headers = { "Content-Type": "application/json" };
  useEffect(() => { fetch(`${apiBase}/api/v1/admin/mcp-sources`, { headers }).then((r) => r.ok ? r.json() : []).then(setSources); }, [apiBase]); // eslint-disable-line react-hooks/exhaustive-deps
  async function search(event: FormEvent) { event.preventDefault(); setLoading(true); setErrors([]); try { const response = await fetch(`${apiBase}/api/v1/admin/search`, { method: "POST", headers, body: JSON.stringify({ query, source_id: sourceId || null, language, time_range: timeRange, limit }) }); const payload = await response.json(); if (!response.ok) throw new Error(payload.detail || "搜索失败"); setItems(payload.items); setErrors(payload.errors); } catch (reason) { setErrors([{ source: "系统", error: reason instanceof Error ? reason.message : "搜索失败" }]); } finally { setLoading(false); } }
  return <section className="app-page search-page"><PageHeading eyebrow="NETWORK VERIFICATION" title="搜索引擎" copy="通过已启用 MCP 来源手动验证本地模型结论，结果始终保留原始链接。" /><form className="search-form" onSubmit={search}><input required aria-label="搜索查询" placeholder="输入需要验证的问题" value={query} onChange={(e) => setQuery(e.target.value)} /><select aria-label="搜索来源" value={sourceId} onChange={(e) => setSourceId(e.target.value)}><option value="">全部启用来源</option>{sources.filter(isSearchSource).map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select><select aria-label="语言" value={language} onChange={(e) => setLanguage(e.target.value)}><option value="zh-CN">中文</option><option value="en">英文</option><option value="all">不限</option></select><select aria-label="时间范围" value={timeRange} onChange={(e) => setTimeRange(e.target.value)}><option value="">不限时间</option><option value="day">24 小时</option><option value="week">一周</option><option value="month">一月</option><option value="year">一年</option></select><label>结果数<input type="number" min="1" max="20" value={limit} onChange={(e) => setLimit(Number(e.target.value))} /></label><button disabled={loading}>{loading ? "正在搜索…" : "搜索验证"}</button></form>{errors.map((item) => <div className="page-error" key={`${item.source}-${item.error}`}>{item.source}: {item.error}</div>)}<div className="search-results">{items.map((item) => <article key={item.url}><span>{searchSourceLabel(item)} · {item.domain}{item.published_at ? ` · ${new Date(item.published_at).toLocaleString("zh-CN")}` : ""}</span><h3><a href={item.url} target="_blank" rel="noreferrer">{item.title}</a></h3><p>{item.snippet}</p></article>)}</div></section>;
}

export function WeknoraPage({ apiBase }: { apiBase: string }) {
  const [token, setToken] = useState(readToken); const [url, setUrl] = useState("http://10.15.0.28/"); const [draft, setDraft] = useState(url); const [message, setMessage] = useState(""); const [failed, setFailed] = useState(false);
  useEffect(() => { fetch(`${apiBase}/api/v1/integrations/weknora`).then((r) => r.json()).then((payload: { url: string }) => { setUrl(payload.url); setDraft(payload.url); }).catch(() => setMessage("无法读取 WeKnora 配置，已使用默认地址。")); }, [apiBase]);
  const headers = { "Content-Type": "application/json", "X-Admin-Token": token };
  async function save() { const response = await fetch(`${apiBase}/api/v1/admin/integrations/weknora`, { method: "PUT", headers, body: JSON.stringify({ url: draft }) }); if (response.ok) { setUrl((await response.json()).url); setFailed(false); setMessage("WeKnora 地址已保存。"); } else setMessage("保存失败，请检查管理员令牌和 URL。"); }
  async function test() { const response = await fetch(`${apiBase}/api/v1/admin/integrations/weknora/test`, { method: "POST", headers, body: JSON.stringify({ url: draft }) }); const payload = await response.json(); setMessage(payload.ok ? `连接成功（HTTP ${payload.status_code}）。` : `连接失败：${payload.error || payload.status_code}`); }
  return <section className="app-page weknora-page"><PageHeading eyebrow="LOCAL KNOWLEDGE WORKBENCH" title="WeKnora" copy="内嵌本地知识库工作台；若服务禁止 iframe，可在新窗口中继续。" /><div className="weknora-toolbar"><a href={url} target="_blank" rel="noreferrer">新窗口打开</a><span>{failed ? "内嵌加载失败，请使用“新窗口打开”。" : "若下方为空白或提示拒绝连接，请使用“新窗口打开”。"}</span></div><div className="weknora-frame"><iframe title="WeKnora 本地知识库" src={url} onError={() => setFailed(true)} /></div><AdminUnlock token={token} onToken={setToken} />{token && <div className="integration-editor"><label>WeKnora URL<input type="url" value={draft} onChange={(e) => setDraft(e.target.value)} /></label><button type="button" onClick={test}>连接测试</button><button type="button" onClick={save}>保存</button></div>}{message && <div className="page-message">{message}</div>}</section>;
}

export function RoutedPage({
  route, apiBase, analysisLogs,
}: {
  route: Exclude<AppRoute, "home">; apiBase: string; analysisLogs: AnalysisLog[];
}) {
  if (route === "model-logs") return <ModelLogsPage apiBase={apiBase} onBack={() => { window.location.hash = "/home"; }} embedded />;
  if (route === "source-filter") return <SourceFilterPage apiBase={apiBase} />;
  if (route === "conclusions") return <ConclusionsPage apiBase={apiBase} />;
  if (route === "sources") return <SourcesPage apiBase={apiBase} />;
  if (route === "news") return <NewsPage apiBase={apiBase} />;
  if (route === "queue") return <QueuePage apiBase={apiBase} />;
  if (route === "analysis") return <AnalysisPage logs={analysisLogs} />;
  if (route === "search") return <SearchPage apiBase={apiBase} />;
  return <WeknoraPage apiBase={apiBase} />;
}
