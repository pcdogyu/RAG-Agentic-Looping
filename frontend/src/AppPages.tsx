import { FormEvent, useCallback, useEffect, useRef, useState } from "react";

import AnalysisPage, { type AnalysisLog } from "./AnalysisPage";
import ModelLogsPage from "./ModelLogs";
import { TargetTrendSummary, type TargetTrend } from "./TargetTrendSummary";

export type AppRoute = "home" | "source-filter" | "sources" | "asset-universe" | "news" | "queue" | "analysis" | "conclusions" | "targets" | "model-logs" | "search" | "weknora";

export const navigationGroups: Record<"left" | "right", Array<{ route: AppRoute; label: string }>> = {
  left: [
    { route: "home", label: "首页" },
    { route: "source-filter", label: "数据源过滤" },
    { route: "sources", label: "数据源" },
    { route: "asset-universe", label: "资产主数据" },
    { route: "news", label: "新闻" },
    { route: "queue", label: "队列" },
    { route: "analysis", label: "分析链路" },
    { route: "conclusions", label: "结论" },
    { route: "targets", label: "标的" },
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
export const researchViewsRefreshIntervalMs = 5000;
export const newsSourceDesktopColumns = 3;
export const liveSnapshotEventName = "market-loop:snapshot";

export function subscribeLiveRefresh(refresh: () => void, fallbackMs: number) {
  let lastLiveUpdate = Date.now();
  const refreshWhenVisible = () => {
    if (document.visibilityState !== "hidden") refresh();
  };
  const onSnapshot = () => {
    lastLiveUpdate = Date.now();
    refreshWhenVisible();
  };
  const onVisibility = () => {
    if (document.visibilityState === "visible") {
      lastLiveUpdate = Date.now();
      refresh();
    }
  };
  window.addEventListener(liveSnapshotEventName, onSnapshot);
  document.addEventListener("visibilitychange", onVisibility);
  const timer = window.setInterval(() => {
    if (Date.now() - lastLiveUpdate >= fallbackMs) refreshWhenVisible();
  }, fallbackMs);
  return () => {
    window.removeEventListener(liveSnapshotEventName, onSnapshot);
    document.removeEventListener("visibilitychange", onVisibility);
    window.clearInterval(timer);
  };
}

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
  instance_id?: string | null;
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

type ModelQueueCounts = {
  queued: number;
  running: number;
  retrying: number;
  verifying: number;
  waiting_for_model: number;
  completed: number;
  failed: number;
};

type ModelQueueMetrics = {
  average_queue_duration_ms: number | null;
  average_execution_duration_ms: number | null;
  longest_wait_ms: number | null;
  estimated_clear_ms: number | null;
  queue_duration_sample_count: number;
  execution_duration_sample_count: number;
  execution_p50_ms: number | null;
  execution_p90_ms: number | null;
  throughput_per_hour: number | null;
};

export type ModelQueueInstanceItem = {
  id: string;
  healthy: boolean;
  model_available: boolean;
  state: string;
  capacity: number;
  available: number;
  observable: boolean;
  counts: ModelQueueCounts;
  metrics: ModelQueueMetrics;
  total_tasks: number;
  truncated: boolean;
  tasks: ModelQueueTask[];
};

type ModelQueueInstanceSummary = Pick<
  ModelQueueInstanceItem,
  "id" | "healthy" | "model_available"
> & Partial<Omit<ModelQueueInstanceItem, "id" | "healthy" | "model_available">>;

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
  instance_count: number;
  per_instance_concurrency: number;
  observable: boolean;
  instances: ModelQueueInstanceSummary[];
  counts: ModelQueueCounts;
  metrics: ModelQueueMetrics;
  total_tasks: number;
  truncated: boolean;
  tasks: ModelQueueTask[];
  error: string | null;
};

export function modelQueueInstances(
  queue: ModelQueueOverviewItem,
): ModelQueueInstanceItem[] {
  const summaries = queue.instances.length ? queue.instances : [{
    id: `${queue.id}-0`,
    healthy: true,
    model_available: true,
  }];
  return summaries.map((instance, index) => ({
    id: instance.id,
    healthy: instance.healthy,
    model_available: instance.model_available,
    state: instance.state ?? queue.state,
    capacity: instance.capacity ?? queue.per_instance_concurrency ?? queue.capacity,
    available: instance.available ?? queue.available,
    observable: instance.observable ?? queue.observable,
    counts: instance.counts ?? queue.counts,
    metrics: instance.metrics ?? queue.metrics,
    total_tasks: instance.total_tasks ?? queue.total_tasks,
    truncated: instance.truncated ?? queue.truncated,
    tasks: instance.tasks ?? queue.tasks.filter(
      (task) => task.instance_id === instance.id || (!task.instance_id && index === 0),
    ),
  }));
}

export type ModelQueuePanelItem = {
  queue: ModelQueueOverviewItem;
  instance: ModelQueueInstanceItem;
};

export function modelQueuePanelColumns(
  queues: ModelQueueOverviewItem[],
): [ModelQueuePanelItem[], ModelQueuePanelItem[]] {
  const buildColumn = (queueIds: ModelQueueOverviewItem["id"][]) => queueIds.flatMap(
    (queueId) => queues
      .filter((queue) => queue.id === queueId)
      .flatMap((queue) => modelQueueInstances(queue).map((instance) => ({ queue, instance }))),
  );
  return [
    buildColumn(["extract", "assist"]),
    buildColumn(["research", "code"]),
  ];
}

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

const cancellableTaskStatuses = new Set(["queued", "running", "retrying", "verifying"]);

export function ModelQueueTaskGrid({
  queue,
  tasks = queue.tasks,
  onCancel,
  onRetry,
  cancellingTaskId,
  retryingTaskId,
}: {
  queue: ModelQueueOverviewItem;
  tasks?: ModelQueueTask[];
  onCancel?: (task: ModelQueueTask) => void;
  onRetry?: (task: ModelQueueTask) => void;
  cancellingTaskId?: string | null;
  retryingTaskId?: string | null;
}) {
  if (!queue.enabled && queue.id === "code") {
    return <div className="page-empty">代码演进未启用（EVOLUTION_ENABLED=false）。</div>;
  }
  if (!tasks.length) {
    return <div className="page-empty">当前没有等待、运行或最近失败的{queue.purpose}任务。</div>;
  }
  return <div className="model-task-grid" data-queue={queue.id}>{tasks.map((task) => {
    const isMapping = task.kind === "asset_mapping";
    const isEvolution = task.kind === "code_evolution";
    const isCancellable = (
      queue.id === "research" && ["asset_research", "event_research"].includes(task.kind)
    ) || (
      queue.id === "assist" && task.kind === "asset_mapping"
    );
    const canCancel = isCancellable && cancellableTaskStatuses.has(task.status);
    const cancelNoun = queue.id === "assist" ? "股票映射任务" : "研究";
    const branch = queueMetricValue(task.metrics.branch);
    return <article className={`model-task-card ${task.status}`} key={task.task_id} title={task.title}>
      <div className="model-task-heading">
        <span className="model-task-status"><i />{modelTaskStatusLabels[task.status] ?? task.status}</span>
        <div className="model-task-heading-actions">
          <small>{taskSourceLabel(task.source)}</small>
          {canCancel && <button
            type="button"
            className="model-task-cancel"
            aria-label={`取消 ${task.title} 的${cancelNoun}`}
            title={queue.id === "assist" ? "取消当前股票映射任务" : "取消该标的研究"}
            disabled={cancellingTaskId === task.task_id}
            onClick={() => onCancel?.(task)}
          >×</button>}
        </div>
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
      {task.error && <div className="model-task-error-row">
        <details className="model-task-error">
          <summary>最近错误</summary>
          <p>{task.error}</p>
        </details>
        {queue.id !== "code" && <button
          type="button"
          className="model-task-retry"
          title="以最高优先级插队重试"
          disabled={retryingTaskId === task.task_id}
          onClick={() => onRetry?.(task)}
        >{retryingTaskId === task.task_id ? "重试中…" : "手动重试"}</button>}
      </div>}
      <time dateTime={task.updated_at}>{new Date(task.updated_at).toLocaleString("zh-CN")}</time>
    </article>;
  })}</div>;
}

export function UnifiedModelQueuePanel({
  queue,
  instance,
  filterRecentResearch = true,
  onFilterRecentResearchChange,
  onCancelTask,
  onRetryTask,
  onRetryAll,
  onClear,
  cancellingTaskId,
  retryingTaskId,
  retryingAll,
  clearing,
}: {
  queue: ModelQueueOverviewItem;
  instance?: ModelQueueInstanceItem;
  filterRecentResearch?: boolean;
  onFilterRecentResearchChange?: (value: boolean) => void;
  onCancelTask?: (task: ModelQueueTask) => void;
  onRetryTask?: (task: ModelQueueTask) => void;
  onRetryAll?: () => void;
  onClear?: () => void;
  cancellingTaskId?: string | null;
  retryingTaskId?: string | null;
  retryingAll?: boolean;
  clearing?: boolean;
}) {
  const activeInstance = instance ?? modelQueueInstances(queue)[0];
  const secondary = activeInstance.counts.retrying + activeInstance.counts.verifying;
  const activeCount = activeInstance.counts.queued + activeInstance.counts.running + secondary;
  const clearableCount = activeCount + activeInstance.counts.failed;
  const retryableCount = activeInstance.tasks.filter((task) => task.error).length;
  const ready = activeInstance.healthy && activeInstance.model_available;
  const state = ready ? activeInstance.state : "unavailable";
  return <section className={`model-queue-panel unified-model-queue-panel ${queue.id}`}>
    <header>
      <div>
        <p className="eyebrow">{modelQueueEyebrows[queue.id]}</p>
        <h3>{queue.model} {queue.purpose}队列 · {activeInstance.id}</h3>
        <small>{queue.binding} · {ready ? "实例可用" : (activeInstance.healthy ? "模型缺失" : "实例离线")}</small>
      </div>
      <div className="model-queue-header-actions">
        {queue.id === "assist" && <label className="model-queue-filter-toggle" title="手动重试时过滤过去 48 小时已经研究过的行业和标的">
          <input
            type="checkbox"
            checked={filterRecentResearch}
            onChange={(event) => onFilterRecentResearchChange?.(event.target.checked)}
          />
          <span>过滤 48h 已研究</span>
        </label>}
        <button
          type="button"
          className="model-queue-retry"
          title="重试当前队列中的全部错误任务"
          disabled={retryingAll || retryableCount === 0}
          onClick={onRetryAll}
        >{retryingAll ? "重试中…" : "重试"}</button>
        <span className={`model-queue-state ${state}`}>{modelQueueStateLabels[state] ?? state}</span>
        <button
          type="button"
          className="model-queue-clear"
          disabled={clearing || clearableCount === 0}
          onClick={onClear}
        >{clearing ? "清空中…" : "清空"}</button>
      </div>
    </header>
    <div className="queue-metrics unified-queue-metrics" aria-live="polite">
      <span>待处理<strong>{activeInstance.counts.queued}</strong></span>
      <span>运行<strong>{activeInstance.counts.running}</strong></span>
      <span>重试/验证<strong>{secondary}</strong></span>
      <span>完成/失败<strong>{activeInstance.counts.completed}/{activeInstance.counts.failed}</strong></span>
      <span title={`样本 ${activeInstance.metrics.queue_duration_sample_count}`}>平均排队<strong>{formatQueueDuration(activeInstance.metrics.average_queue_duration_ms)}</strong></span>
      {queue.id === "assist"
        ? <span title="过去 4 小时完成任务的实际吞吐">近4h吞吐<strong>{activeInstance.metrics.throughput_per_hour === null ? "—" : `${activeInstance.metrics.throughput_per_hour.toFixed(1)}/时`}</strong></span>
        : <span title={`近 4 小时终态样本 ${activeInstance.metrics.execution_duration_sample_count}`}>近4h平均执行<strong>{formatQueueDuration(activeInstance.metrics.average_execution_duration_ms)}</strong></span>}
    </div>
    <div className={`model-queue-runtime ${queue.id === "research" ? "research" : "standard"}`}>
      <span>模型等待<strong>{activeInstance.counts.waiting_for_model}</strong></span>
      <span>槽位<strong>{activeInstance.available}/{activeInstance.capacity}</strong></span>
      <span>实例并发<strong>{activeInstance.capacity} 路</strong></span>
      <span>CPU<strong>{queue.threads} 线程</strong></span>
      <span>最长等待<strong>{formatQueueDuration(activeInstance.metrics.longest_wait_ms)}</strong></span>
      <span>预计清空<strong>{formatQueueDuration(activeInstance.metrics.estimated_clear_ms)}</strong></span>
      {queue.id === "research" && <>
        <span>P50<strong>{formatQueueDuration(activeInstance.metrics.execution_p50_ms)}</strong></span>
        <span>P90<strong>{formatQueueDuration(activeInstance.metrics.execution_p90_ms)}</strong></span>
        <span>近24h吞吐<strong>{activeInstance.metrics.throughput_per_hour === null ? "—" : `${activeInstance.metrics.throughput_per_hour.toFixed(1)}/时`}</strong></span>
      </>}
    </div>
    {!activeInstance.observable && <div className="page-error">模型推理槽位状态暂时不可用。</div>}
    {queue.error && <div className="page-error">{queue.error}</div>}
    {activeInstance.truncated && <div className="page-message">队列过长，当前显示前 500 张任务卡。</div>}
    <ModelQueueTaskGrid
      queue={queue}
      tasks={activeInstance.tasks}
      onCancel={onCancelTask}
      onRetry={onRetryTask}
      cancellingTaskId={cancellingTaskId}
      retryingTaskId={retryingTaskId}
    />
  </section>;
}

export function removeTasksFromQueueOverview(
  current: ModelQueueOverviewResponse,
  queueId: ModelQueueOverviewItem["id"],
  predicate: (task: ModelQueueTask) => boolean,
): ModelQueueOverviewResponse {
  return {
    ...current,
    queues: current.queues.map((queue) => {
      if (queue.id !== queueId) return queue;
      const removed = queue.tasks.filter(predicate);
      if (!removed.length) return queue;
      const counts = { ...queue.counts };
      const removeFromCounts = (source: ModelQueueCounts, tasks: ModelQueueTask[]) => {
        const next = { ...source };
        for (const task of tasks) {
          const field = ["queued", "proposed"].includes(task.status) ? "queued"
            : ["running", "generating", "testing", "merging"].includes(task.status) ? "running"
              : task.status === "retrying" ? "retrying"
                : task.status === "verifying" ? "verifying"
                  : ["failed", "rejected", "rolled_back"].includes(task.status) ? "failed" : null;
          if (field) next[field] = Math.max(0, next[field] - 1);
        }
        return next;
      };
      const nextCounts = removeFromCounts(counts, removed);
      return {
        ...queue,
        counts: nextCounts,
        total_tasks: nextCounts.queued + nextCounts.running + nextCounts.retrying
          + nextCounts.verifying + nextCounts.completed + nextCounts.failed,
        tasks: queue.tasks.filter((task) => !predicate(task)),
        instances: modelQueueInstances(queue).map((instance) => {
          const instanceRemoved = instance.tasks.filter(predicate);
          if (!instanceRemoved.length) return instance;
          const instanceCounts = removeFromCounts(instance.counts, instanceRemoved);
          return {
            ...instance,
            counts: instanceCounts,
            total_tasks: instanceCounts.queued + instanceCounts.running
              + instanceCounts.retrying + instanceCounts.verifying
              + instanceCounts.completed + instanceCounts.failed,
            tasks: instance.tasks.filter((task) => !predicate(task)),
          };
        }),
      };
    }),
  };
}

type CancelledTaskTombstone = {
  queueId: ModelQueueOverviewItem["id"];
  countField: "queued" | "running" | "retrying" | "verifying" | null;
  maxCount: number;
  cancelledAt: number;
};

function taskActiveCountField(task: ModelQueueTask): CancelledTaskTombstone["countField"] {
  if (["queued", "proposed"].includes(task.status)) return "queued";
  if (["running", "generating", "testing", "merging"].includes(task.status)) return "running";
  if (task.status === "retrying") return "retrying";
  if (task.status === "verifying") return "verifying";
  return null;
}

export function applyCancelledTaskTombstone(
  current: ModelQueueOverviewResponse,
  taskId: string,
  tombstone: CancelledTaskTombstone,
): { overview: ModelQueueOverviewResponse; settled: boolean } {
  const queue = current.queues.find((item) => item.id === tombstone.queueId);
  if (!queue) return { overview: current, settled: false };
  if (queue.tasks.some((task) => task.task_id === taskId)) {
    return {
      overview: removeTasksFromQueueOverview(
        current,
        tombstone.queueId,
        (task) => task.task_id === taskId,
      ),
      settled: false,
    };
  }
  if (!tombstone.countField) return { overview: current, settled: true };
  if (queue.counts[tombstone.countField] <= tombstone.maxCount) {
    return { overview: current, settled: true };
  }
  const queues = current.queues.map((item) => {
    if (item.id !== tombstone.queueId) return item;
    const counts = {
      ...item.counts,
      [tombstone.countField as string]: tombstone.maxCount,
    };
    return {
      ...item,
      counts,
      total_tasks: counts.queued + counts.running + counts.retrying
        + counts.verifying + counts.completed + counts.failed,
    };
  });
  return { overview: { ...current, queues }, settled: false };
}

export function modelTaskRetryRequest(
  queue: Pick<ModelQueueOverviewItem, "id">,
  task: Pick<ModelQueueTask, "task_id" | "kind" | "entity_id" | "instance_id">,
  filterRecentResearch: boolean,
): RequestInit {
  return {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      task_id: task.task_id,
      kind: task.kind,
      entity_id: task.entity_id,
      instance_id: task.instance_id,
      ...(queue.id === "assist" ? { filter_recent_research: filterRecentResearch } : {}),
    }),
  };
}

export function modelQueueRetryRequest(
  queue: Pick<ModelQueueOverviewItem, "id">,
  filterRecentResearch: boolean,
): RequestInit {
  if (queue.id !== "assist") return { method: "POST" };
  return {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ filter_recent_research: filterRecentResearch }),
  };
}

export function QueuePage({ apiBase }: { apiBase: string }) {
  const [overview, setOverview] = useState<ModelQueueOverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [cancellingTaskId, setCancellingTaskId] = useState<string | null>(null);
  const [retryingTaskId, setRetryingTaskId] = useState<string | null>(null);
  const [retryingQueueId, setRetryingQueueId] = useState<string | null>(null);
  const [clearingQueueId, setClearingQueueId] = useState<string | null>(null);
  const [filterRecentResearch, setFilterRecentResearch] = useState(true);
  const requestInFlight = useRef(false);
  const cancelledTaskIds = useRef(new Map<string, CancelledTaskTombstone>());

  const loadQueues = useCallback(async (signal?: AbortSignal, showLoading = false) => {
    if (requestInFlight.current) return;
    requestInFlight.current = true;
    if (showLoading) setLoading(true);
    try {
      const response = await fetch(`${apiBase}/api/v1/model-queue-overview?limit=500`, { signal });
      if (!response.ok) throw new Error(`模型队列请求失败（HTTP ${response.status}）`);
      let next = await response.json() as ModelQueueOverviewResponse;
      const snapshotTime = Date.parse(next.generated_at);
      for (const [taskId, tombstone] of cancelledTaskIds.current) {
        const applied = applyCancelledTaskTombstone(next, taskId, tombstone);
        next = applied.overview;
        if (applied.settled && Number.isFinite(snapshotTime) && snapshotTime >= tombstone.cancelledAt) {
          cancelledTaskIds.current.delete(taskId);
        }
      }
      setOverview(next);
      setError("");
    } catch (reason) {
      if (signal?.aborted) return;
      setError(reason instanceof Error ? reason.message : "模型队列请求失败");
    } finally {
      requestInFlight.current = false;
      if (!signal?.aborted) setLoading(false);
    }
  }, [apiBase]);

  const removeQueueTasks = useCallback((
    queueId: ModelQueueOverviewItem["id"],
    predicate: (task: ModelQueueTask) => boolean,
  ) => {
    setOverview((current) => current
      ? removeTasksFromQueueOverview(current, queueId, predicate)
      : current);
  }, []);

  const cancelModelTask = useCallback(async (
    queue: ModelQueueOverviewItem,
    task: ModelQueueTask,
  ) => {
    setCancellingTaskId(task.task_id);
    setActionMessage("");
    setError("");
    const countField = taskActiveCountField(task);
    cancelledTaskIds.current.set(task.task_id, {
      queueId: queue.id,
      countField,
      maxCount: countField ? Math.max(0, queue.counts[countField] - 1) : 0,
      cancelledAt: Date.now(),
    });
    removeQueueTasks(queue.id, (item) => item.task_id === task.task_id);
    try {
      const response = await fetch(`${apiBase}/api/v1/model-queues/${queue.id}/tasks/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ task_id: task.task_id, kind: task.kind, entity_id: task.entity_id }),
      });
      if (!response.ok) throw new Error(`取消${queue.purpose}失败（HTTP ${response.status}）`);
      const result = await response.json() as { cancelled: number };
      setActionMessage(`已取消“${task.title}”的 ${result.cancelled} 个活动${queue.purpose}任务。`);
    } catch (reason) {
      cancelledTaskIds.current.delete(task.task_id);
      setError(reason instanceof Error ? reason.message : `取消${queue.purpose}失败`);
      void loadQueues(undefined, false);
    } finally {
      setCancellingTaskId(null);
    }
  }, [apiBase, loadQueues, removeQueueTasks]);

  const clearModelQueue = useCallback(async (
    queue: ModelQueueOverviewItem,
    instance: ModelQueueInstanceItem,
  ) => {
    const activeCount = instance.counts.queued + instance.counts.running
      + instance.counts.retrying + instance.counts.verifying;
    const clearableCount = activeCount + instance.counts.failed;
    if (!clearableCount) return;
    const actionId = `${queue.id}:${instance.id}`;
    setClearingQueueId(actionId);
    setActionMessage("");
    setError("");
    try {
      const response = await fetch(`${apiBase}/api/v1/model-queues/${queue.id}/instances/${instance.id}/clear`, { method: "POST" });
      if (!response.ok) throw new Error(`清空${queue.purpose}队列失败（HTTP ${response.status}）`);
      const result = await response.json() as { cancelled: number };
      removeQueueTasks(queue.id, (task) => task.instance_id === instance.id && [
        "queued", "proposed", "running", "generating", "retrying", "verifying", "testing", "merging",
        "failed", "rejected", "rolled_back",
      ].includes(task.status));
      setActionMessage(`已清空 ${queue.model} ${instance.id} 的 ${result.cancelled} 个当前${queue.purpose}任务；其他实例不受影响。`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : `清空${queue.purpose}队列失败`);
    } finally {
      setClearingQueueId(null);
    }
  }, [apiBase, removeQueueTasks]);

  const retryModelTask = useCallback(async (
    queue: ModelQueueOverviewItem,
    task: ModelQueueTask,
  ) => {
    setRetryingTaskId(task.task_id);
    setActionMessage("");
    setError("");
    try {
      const response = await fetch(`${apiBase}/api/v1/model-queues/${queue.id}/tasks/retry`, modelTaskRetryRequest(
        queue,
        task,
        filterRecentResearch,
      ));
      if (!response.ok) throw new Error(`手动重试失败（HTTP ${response.status}）`);
      setActionMessage(`已将“${task.title}”插入 ${queue.model} 队列最前方重试。`);
      await loadQueues();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "手动重试失败");
    } finally {
      setRetryingTaskId(null);
    }
  }, [apiBase, filterRecentResearch, loadQueues]);

  const retryModelQueue = useCallback(async (
    queue: ModelQueueOverviewItem,
    instance: ModelQueueInstanceItem,
  ) => {
    const retryableCount = instance.tasks.filter((task) => task.error).length;
    if (!retryableCount || !window.confirm(
      `确认重试 ${queue.model} ${instance.id} 当前 ${retryableCount} 个错误任务？成功任务不会重复执行。`,
    )) return;
    const actionId = `${queue.id}:${instance.id}`;
    setRetryingQueueId(actionId);
    setActionMessage("");
    setError("");
    try {
      const response = await fetch(
        `${apiBase}/api/v1/model-queues/${queue.id}/instances/${instance.id}/retry`,
        modelQueueRetryRequest(queue, filterRecentResearch),
      );
      if (!response.ok) throw new Error(`批量重试失败（HTTP ${response.status}）`);
      const result = await response.json() as { retried: number; skipped: number };
      setActionMessage(
        `已重试 ${queue.model} ${instance.id} 的 ${result.retried} 个错误任务${result.skipped ? `，跳过 ${result.skipped} 个已失效任务` : ""}。`,
      );
      await loadQueues();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "批量重试失败");
    } finally {
      setRetryingQueueId(null);
    }
  }, [apiBase, filterRecentResearch, loadQueues]);

  useEffect(() => {
    const controller = new AbortController();
    void loadQueues(controller.signal, true);
    const unsubscribe = subscribeLiveRefresh(
      () => void loadQueues(controller.signal),
      queueRefreshIntervalMs,
    );
    return () => {
      controller.abort();
      unsubscribe();
    };
  }, [loadQueues]);

  return <section className="app-page queue-page">
    <PageHeading eyebrow="ACTIVE MODEL PIPELINES" title="队列" copy="分别查看四条业务队列及其独立推理通道；实时事件触发更新，连接中断时每 5 秒回退刷新。" />
    <div className="queue-toolbar">
      <span>四条业务队列独立加载；任一服务异常不会遮挡其他队列。</span>
      <button
        type="button"
        disabled={loading}
        onClick={() => void loadQueues(undefined, true)}
      >
        {loading ? "刷新中…" : "立即刷新"}
      </button>
    </div>
    {error && <div className="page-error">{error}</div>}
    {actionMessage && <div className="page-message">{actionMessage}</div>}
    {!overview && loading && <div className="page-message">正在读取四条业务队列…</div>}
    <div className="model-queue-columns">
      {overview && modelQueuePanelColumns(overview.queues).map((column, columnIndex) => (
        <div
          className="model-queue-column"
          data-queue-column={columnIndex === 0 ? "extract-assist" : "research-code"}
          key={columnIndex === 0 ? "extract-assist" : "research-code"}
        >
          {column.map(({ queue, instance }) => {
            const actionId = `${queue.id}:${instance.id}`;
            return <UnifiedModelQueuePanel
              queue={queue}
              instance={instance}
              filterRecentResearch={filterRecentResearch}
              onFilterRecentResearchChange={setFilterRecentResearch}
              key={actionId}
              onCancelTask={(task) => void cancelModelTask(queue, task)}
              onRetryTask={(task) => void retryModelTask(queue, task)}
              onRetryAll={() => void retryModelQueue(queue, instance)}
              onClear={() => void clearModelQueue(queue, instance)}
              cancellingTaskId={cancellingTaskId}
              retryingTaskId={retryingTaskId}
              retryingAll={retryingQueueId === actionId}
              clearing={clearingQueueId === actionId}
            />;
          })}
        </div>
      ))}
    </div>
  </section>;
}

export type NewsBoardStatus =
  | "dispatch_pending"
  | "queued"
  | "extracting"
  | "mapping"
  | "researching"
  | "revising"
  | "completed"
  | "insufficient_evidence"
  | "failed"
  | "orphaned"
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
  status_detail?: string | null;
  retryable?: boolean;
  events: Array<{ id: string; headline: string; event_type: string; priority: number }>;
  assets: Array<{ asset_id: string; symbol: string; name: string; market: string }>;
};

export type NewsBoardSource = {
  source: string;
  latest_published_at: string | null;
  item_count: number;
  items: NewsBoardItem[];
  error: string | null;
  discovery_status?: "healthy" | "error" | "unchecked" | string;
  last_attempt_at?: string | null;
  last_success_at?: string | null;
  watermark_at?: string | null;
  last_error?: string | null;
  last_discovered_count?: number;
  last_new_count?: number;
};

type NewsBoardResponse = {
  generated_at: string;
  last_refresh_at: string | null;
  last_success_at: string | null;
  per_source: number;
  total_sources: number;
  sources: NewsBoardSource[];
};

export const newsBoardStatusLabels: Record<NewsBoardStatus, string> = {
  dispatch_pending: "待入队",
  queued: "已入队",
  extracting: "抽取中",
  mapping: "股票映射中",
  researching: "研究中",
  revising: "修订中",
  completed: "已完成",
  insufficient_evidence: "证据不足",
  failed: "失败",
  orphaned: "入队中断",
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

type NewsRetryState = { status: "pending" | "queued" | "error"; error?: string };

const discoveryStatusLabels: Record<string, string> = {
  healthy: "抓取正常",
  error: "抓取异常",
  unchecked: "等待首次抓取",
};

export function formatNewsRefreshTime(value?: string | null) {
  return value ? new Date(value).toLocaleString("zh-CN") : "尚未成功";
}

export function NewsSourcePanel({ group, retryStates = {}, onRetry }: {
  group: NewsBoardSource;
  retryStates?: Record<string, NewsRetryState>;
  onRetry?: (item: NewsBoardItem) => void;
}) {
  return <section className="news-source-panel">
    <header>
      <div><p className="eyebrow">NEWS SOURCE</p><h3>{group.source}</h3></div>
      <div className="news-source-refresh">
        <span className={`news-source-health ${group.discovery_status ?? "unchecked"}`}>
          <i />{discoveryStatusLabels[group.discovery_status ?? "unchecked"] ?? group.discovery_status}
        </span>
        <small>成功 {formatNewsRefreshTime(group.last_success_at)}</small>
        <small>本轮发现 {group.last_discovered_count ?? 0} · 新增 {group.last_new_count ?? 0}</small>
        <span className="news-source-count">最新 {group.item_count}/50 条</span>
      </div>
    </header>
    {group.error && <div className="page-error">{group.error}</div>}
    {group.last_error && <div className="page-error">数据源错误：{group.last_error}</div>}
    {!group.error && !group.items.length && <div className="page-empty">该来源暂无新闻。</div>}
    <div className="news-source-items">
      {group.items.map((item) => {
        const eventType = item.events[0]?.event_type;
        const retryState = retryStates[item.id];
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
          {item.status_detail && <small className="news-processing-detail">{item.status_detail}</small>}
          {!!item.assets.length && <div className="news-board-assets" aria-label="关联标的">
            {item.assets.slice(0, 5).map((asset) => <span key={asset.asset_id} title={`${asset.name} · ${asset.market}`}>{asset.symbol}</span>)}
            {item.assets.length > 5 && <small>+{item.assets.length - 5}</small>}
          </div>}
          {item.retryable && onRetry && <div className="news-retry-action">
            <button type="button" disabled={retryState?.status === "pending" || retryState?.status === "queued"} onClick={() => onRetry(item)}>
              {retryState?.status === "pending" ? "重新入队中…" : retryState?.status === "queued" ? "已重新入队" : "重新处理"}
            </button>
            {retryState?.status === "error" && <small>{retryState.error}</small>}
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
  const [refreshMessage, setRefreshMessage] = useState("");
  const [retryStates, setRetryStates] = useState<Record<string, NewsRetryState>>({});

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
    const unsubscribe = subscribeLiveRefresh(
      () => void load(controller.signal),
      newsBoardRefreshIntervalMs,
    );
    return () => {
      controller.abort();
      unsubscribe();
    };
  }, [load]);

  async function retry(item: NewsBoardItem) {
    if (["pending", "queued"].includes(retryStates[item.id]?.status || "")) return;
    setRetryStates((current) => ({ ...current, [item.id]: { status: "pending" } }));
    try {
      const response = await fetch(`${apiBase}/api/v1/news/${encodeURIComponent(item.id)}/retry`, { method: "POST" });
      const payload = await response.json() as { detail?: unknown };
      if (!response.ok) throw new Error(typeof payload.detail === "string" ? payload.detail : "新闻重新入队失败");
      setRetryStates((current) => ({ ...current, [item.id]: { status: "queued" } }));
      await load();
    } catch (reason) {
      setRetryStates((current) => ({ ...current, [item.id]: { status: "error", error: reason instanceof Error ? reason.message : "新闻重新入队失败" } }));
    }
  }

  async function refreshSources() {
    if (loading) return;
    setLoading(true);
    try {
      const response = await fetch(`${apiBase}/api/v1/scan`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ background: true }),
      });
      const payload = await response.json() as { status?: string; detail?: unknown };
      if (!response.ok) {
        throw new Error(typeof payload.detail === "string" ? payload.detail : `新闻抓取请求失败（HTTP ${response.status}）`);
      }
      setRefreshMessage(payload.status === "already_queued" ? "新闻抓取正在执行，完成后自动更新。" : "新闻抓取已排队。");
      await load();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "新闻抓取请求失败");
    } finally {
      setLoading(false);
    }
  }

  return <section className="app-page news-page">
    <PageHeading eyebrow="LIVE NEWS PIPELINE" title="新闻" copy="按来源查看最新 50 条新闻及其抽取、股票映射、研究和修订状态；实时事件触发更新，断线时自动回退轮询。" />
    <div className="news-board-toolbar">
      <div className="news-board-refresh-summary">
        <span>{board ? `${board.total_sources} 个来源 · 每来源最新 ${board.per_source} 条` : "正在读取新闻来源…"}</span>
        {board && <small>最近成功刷新：{formatNewsRefreshTime(board.last_success_at)}</small>}
      </div>
      <button type="button" disabled={loading} onClick={() => void refreshSources()}>{loading ? "刷新中…" : "立即刷新"}</button>
    </div>
    {error && <div className="page-error">{error}</div>}
    {refreshMessage && <div className="page-message">{refreshMessage}</div>}
    {!board && loading && <div className="page-message">正在读取新闻状态…</div>}
    {board && !board.sources.length && <div className="page-empty">当前没有已入库新闻。</div>}
    {board && <div className="news-source-grid" data-columns={newsSourceDesktopColumns}>{board.sources.map((group) => <NewsSourcePanel group={group} retryStates={retryStates} onRetry={(item) => void retry(item)} key={group.source} />)}</div>}
  </section>;
}

type ScoringFactor = {
  value: number;
  reason: string;
  evidence_ids: string[];
};

type SystemConfidenceFactor = {
  value: number;
  reason: string;
  evidence_ids: string[];
};

export type Recommendation = {
  id: string;
  run_id: string;
  asset: { asset_id: string; symbol: string; name: string; market: string };
  rating: string;
  score: number | null;
  direction_score?: number | null;
  confidence: number;
  rating_confidence?: number;
  news_confidence?: number;
  evidence_complete: boolean;
  directional_evidence_complete?: boolean;
  direction_verified?: boolean;
  signal_status?: "technical_failure" | "insufficient_evidence" | "neutral" | "directional";
  model_score?: number | null;
  model_direction?: "bullish" | "neutral" | "bearish" | null;
  model_rating?: string | null;
  model_confidence?: number | null;
  raw_score?: number | null;
  score_available?: boolean;
  evidence_strength?: number;
  mapping_confidence?: number;
  primary_gate_reason?: string | null;
  gate_reasons?: string[];
  horizon_days?: number;
  horizon_unit?: "calendar_days" | "trading_sessions";
  impact_factors?: {
    direction: number;
    magnitude: ScoringFactor;
    persistence: ScoringFactor;
    representativeness: ScoringFactor;
    market_confirmation: ScoringFactor;
  };
  confidence_factors?: {
    direction_clarity: ScoringFactor;
    source_reliability: ScoringFactor;
    magnitude_certainty: ScoringFactor;
    market_context_completeness: ScoringFactor;
  };
  fact_confidence?: number;
  news_confidence_factors?: {
    source_reliability: SystemConfidenceFactor;
    originality: SystemConfidenceFactor;
    cross_verification: SystemConfidenceFactor;
    clarity: SystemConfidenceFactor;
    timeliness_completeness: SystemConfidenceFactor;
  };
  rating_confidence_factors?: {
    mapping_strength: SystemConfidenceFactor;
    causality_certainty: SystemConfidenceFactor;
    historical_pattern: SystemConfidenceFactor;
    impact_scale: SystemConfidenceFactor;
    timing_certainty: SystemConfidenceFactor;
    market_consistency: SystemConfidenceFactor;
  };
  mapping_distance?: number;
  score_source?: "llm" | "rule_fallback";
  evidence_warnings?: string[];
  scoring_version?: string;
  calibration_version?: string;
  claim_assessments?: Array<{
    claim: string;
    claim_kind: string;
    stance: number;
    verdict: "supported" | "contradicted" | "unrelated" | "insufficient";
    evidence_ids: string[];
    confidence: number;
    reason: string;
  }>;
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

export type ChangedTarget = {
  asset: {
    asset_id: string;
    symbol: string;
    name: string;
    market: string;
  };
  recommendation_id: string;
  latest_recommendation_id?: string;
  latest_researched_at?: string;
  changed_at: string;
  previous: { signal_status: string; rating: string };
  current: { signal_status: string; rating: string };
  status_changed: boolean;
  rating_changed: boolean;
};

export type ConclusionDetail = {
  recommendation: Recommendation;
  event: { headline: string } | null;
  news: Array<{ id: string; title: string; url: string; source: string }>;
  evidence: Array<{ id: string; claim: string; source_name: string; source_url: string; excerpt: string }>;
};

export type EventTargetImpact = {
  target_type: string;
  target_name: string;
  asset: { asset_id: string; symbol: string; name: string; market: string } | null;
  direction_score: number;
  rating: string;
  rating_confidence: number;
  horizon_days: number;
  horizon_unit: string;
  transmission_path: string[];
  rationale: string;
  missing_information: string[];
};

export type EventConclusionDetail = {
  run: { id: string; status: string; updated_at: string };
  refresh?: EventResearchRefresh | null;
  event: { id: string; headline: string; event_type: string } | null;
  report: {
    summary: string;
    affected_markets: string[];
    affected_sectors: string[];
    scenarios: string[];
    catalysts: string[];
    risks: string[];
    unresolved_questions: string[];
    confidence: number;
    evidence_complete: boolean;
    news_confidence: number;
    impacts: EventTargetImpact[];
    macro_factors: Array<{ id: string; name: string; description: string; strength: number }>;
    missing_information: string[];
  };
  news: Array<{ id: string; title: string; url: string; source: string }>;
  evidence: Array<{ id: string; claim: string; source_name: string; source_url: string; excerpt: string }>;
};

export type ResearchConclusionItem = {
  kind: "asset" | "event";
  id: string;
  occurred_at: string;
  status: string;
  evidence_complete: boolean;
  title: string;
  summary: string;
  asset: Recommendation["asset"] | null;
  event: { id: string; headline: string; event_type: string } | null;
  recommendation: Recommendation | null;
  refresh?: EventResearchRefresh | null;
  report: {
    confidence: number;
    news_confidence: number;
    direction_score: number | null;
    rating: string | null;
    impact_count: number;
    affected_markets: string[];
    affected_sectors: string[];
    scoring_version: string;
  } | null;
};

export type EventResearchRefresh = {
  status: "queued" | "running" | "retrying" | "failed";
  stage: "event_extraction" | "asset_mapping" | "deep_research" | "web_search";
  error: string | null;
};

export type TargetChange = {
  kind: "macro" | "asset";
  key: string;
  label: string;
  symbol: string | null;
  market: string | null;
  target_type: string;
  changed_at: string;
  previous: { rating: string; direction_score: number | null; rating_confidence: number | null } | null;
  current: { rating: string; direction_score: number | null; rating_confidence: number | null };
  latest: { rating: string; direction_score: number | null; rating_confidence: number | null; news_confidence: number | null };
  trend?: TargetTrend;
  latest_detail: { kind: "event" | "asset"; id: string; researched_at: string };
  change_detail_id: string;
  rated_at?: string;
  change_state?: "first" | "changed" | "unchanged";
};

export function changedTargetLatestRecommendationId(item: ChangedTarget) {
  return item.latest_recommendation_id || item.recommendation_id;
}

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

export type FailedResearchBulkRetryResponse = {
  requested: number;
  retried: number;
  skipped: number;
  failed: number;
  results: Array<{
    kind: "asset" | "event";
    source_run_id: string;
    run_id: string | null;
    task_id: string | null;
    status: "queued" | "skipped" | "failed";
    detail: string | null;
  }>;
};

export const failedResearchBulkRetryPath = "/api/v1/failed-research-runs/retry";

export type ConclusionResearchResponse = {
  task_id: string;
  run_id: string;
  source_recommendation_id: string;
  status: "queued";
};

export type EventConclusionResearchResponse = {
  task_id: string;
  run_id: string;
  source_run_id: string;
  status: "queued";
  stage: "event_extraction";
};

export const conclusionResearchPath = (recommendationId: string) =>
  `/api/v1/conclusions/${encodeURIComponent(recommendationId)}/research`;

export async function researchConclusion(
  apiBase: string,
  recommendationId: string,
  request: typeof fetch = fetch,
): Promise<ConclusionResearchResponse> {
  const response = await request(`${apiBase}${conclusionResearchPath(recommendationId)}`, { method: "POST" });
  const payload = await response.json() as Partial<ConclusionResearchResponse> & { detail?: unknown };
  if (!response.ok) {
    const structured = payload.detail && typeof payload.detail === "object"
      ? payload.detail as { message?: unknown; active_run_id?: unknown }
      : null;
    const message = structured && typeof structured.message === "string" ? structured.message : null;
    const activeRun = structured && typeof structured.active_run_id === "string" ? structured.active_run_id : null;
    const detail = typeof payload.detail === "string"
      ? payload.detail
      : message
        ? `${message}${activeRun ? `（活动任务 ${activeRun}）` : ""}`
        : "重新调研失败";
    throw new Error(detail);
  }
  return payload as ConclusionResearchResponse;
}

export const eventConclusionResearchPath = (runId: string) =>
  `/api/v1/event-conclusions/${encodeURIComponent(runId)}/research`;

export async function researchEventConclusion(
  apiBase: string,
  runId: string,
  request: typeof fetch = fetch,
): Promise<EventConclusionResearchResponse> {
  const response = await request(`${apiBase}${eventConclusionResearchPath(runId)}`, { method: "POST" });
  const payload = await response.json() as Partial<EventConclusionResearchResponse> & { detail?: unknown };
  if (!response.ok) {
    const structured = payload.detail && typeof payload.detail === "object"
      ? payload.detail as { message?: unknown; active_run_id?: unknown }
      : null;
    const message = structured && typeof structured.message === "string" ? structured.message : null;
    const activeRun = structured && typeof structured.active_run_id === "string" ? structured.active_run_id : null;
    const detail = typeof payload.detail === "string"
      ? payload.detail
      : message
        ? `${message}${activeRun ? `（活动任务 ${activeRun}）` : ""}`
        : "事件重新调研失败";
    throw new Error(detail);
  }
  return payload as EventConclusionResearchResponse;
}

export function failedResearchRetryPath(
  item: Pick<FailedResearch, "kind" | "id">,
  instanceId?: string,
) {
  const path = item.kind === "asset"
    ? `/api/v1/research-runs/${item.id}/retry`
    : `/api/v1/event-research-runs/${item.id}/retry`;
  return instanceId ? `${path}?instance_id=${encodeURIComponent(instanceId)}` : path;
}

export function availableResearchInstances(queues: ModelQueueOverviewItem[]) {
  const research = queues.find((queue) => queue.id === "research" && queue.enabled);
  return research
    ? modelQueueInstances(research).filter(
      (instance) => instance.healthy && instance.model_available,
    )
    : [];
}

export async function retryAllFailedResearch(
  apiBase: string,
  request: typeof fetch = fetch,
): Promise<FailedResearchBulkRetryResponse> {
  const response = await request(`${apiBase}${failedResearchBulkRetryPath}`, { method: "POST" });
  const payload = await response.json() as FailedResearchBulkRetryResponse & { detail?: string };
  if (!response.ok) throw new Error(payload.detail || "全部重试失败");
  return payload;
}

export function failedResearchBulkRetryMessage(payload: FailedResearchBulkRetryResponse) {
  return `批量重试完成：已排队 ${payload.retried} 条，跳过 ${payload.skipped} 条，失败 ${payload.failed} 条。`;
}

export function failedResearchAfterBulkRetry(
  items: FailedResearch[],
  payload: FailedResearchBulkRetryResponse,
) {
  const queuedIds = new Set(
    payload.results
      .filter((item) => item.status === "queued")
      .map((item) => item.source_run_id),
  );
  return items.filter((item) => !queuedIds.has(item.id));
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
  strongly_bullish: "强烈看多", bullish: "看多", watch: "中性", bearish: "看空", strongly_bearish: "强烈看空",
};

export function recommendationRatingLabel(value: string) {
  const normalized = value.trim() === "官网" ? "watch" : value.trim();
  return ratingLabels[normalized] || normalized;
}

const signalStatusLabels: Record<string, string> = {
  technical_failure: "技术失败",
  insufficient_evidence: "方向证据不足",
  neutral: "中性",
  directional: "方向信号",
};

const modelDirectionLabels: Record<string, string> = {
  bullish: "看多 / Bullish",
  neutral: "中性 / Neutral",
  bearish: "看空 / Bearish",
};

const modelRatingLabels: Record<string, string> = {
  strongly_bullish: "强烈看多 / Strongly bullish",
  bullish: "看多 / Bullish",
  watch: "中性 / Neutral",
  bearish: "看空 / Bearish",
  strongly_bearish: "强烈看空 / Strongly bearish",
};

export function ConclusionScore({
  score, rating, confidence, evidenceComplete, directionalEvidenceComplete, signalStatus,
  factConfidence, horizonDays, horizonUnit, scoringVersion, directionScore,
  newsConfidence, ratingConfidence, scoreSource, compact = false,
}: {
  score: number | null;
  directionScore?: number | null;
  rating: string;
  confidence: number;
  newsConfidence?: number;
  ratingConfidence?: number;
  evidenceComplete: boolean;
  directionalEvidenceComplete?: boolean;
  signalStatus?: string;
  factConfidence?: number;
  horizonDays?: number;
  horizonUnit?: string;
  scoringVersion?: string;
  scoreSource?: "llm" | "rule_fallback";
  compact?: boolean;
}) {
  const isV3 = scoringVersion === "llm-direction-v3";
  const publishedScore = directionScore ?? score ?? 0;
  const resolvedStatus = signalStatus || (
    isV3
      ? (Math.abs(publishedScore) < 30 ? "neutral" : "directional")
      : (!evidenceComplete ? "insufficient_evidence" : (Math.abs(publishedScore) < 20 ? "neutral" : "directional"))
  );
  const shortTerm = scoringVersion === "short-term-impact-v1" || horizonUnit === "trading_sessions";
  const scoreBlocked = !isV3 && (score === null || resolvedStatus === "insufficient_evidence" || resolvedStatus === "technical_failure");
  const positiveThreshold = isV3 ? 30 : 15;
  const scoreTone = scoreBlocked ? "neutral" : publishedScore <= -positiveThreshold ? "negative" : publishedScore >= positiveThreshold ? "positive" : "neutral";
  return <div className={`conclusion-score ${scoreTone}`}>
    <strong>{scoreBlocked ? "暂不评分" : `${isV3 ? "方向分" : shortTerm ? "影响分" : "发布分"}：${publishedScore > 0 ? "+" : ""}${publishedScore}`}</strong>
    <span>{!compact && `${signalStatusLabels[resolvedStatus] || resolvedStatus} · `}{isV3 ? "五级评级" : shortTerm ? "五档评级" : "评级"}：{recommendationRatingLabel(rating)}{isV3 && scoreSource === "rule_fallback" ? " · 规则回退" : ""}</span>
    {isV3
      ? <small>新闻可信度 {Math.round((newsConfidence ?? factConfidence ?? 0) * 100)}% · 评级置信度 {Math.round((ratingConfidence ?? confidence) * 100)}% · 未来 {horizonDays ?? 90} 个自然日</small>
      : shortTerm && !scoreBlocked
      ? <small>新闻事实置信度 {Math.round((factConfidence ?? confidence) * 100)}% · 评级置信度 {Math.round(confidence * 100)}% · 未来 1–{horizonDays ?? 3} 个交易日</small>
      : compact
      ? <small>{scoreBlocked ? "参考置信度" : "发布置信度"} {Math.round(confidence * 100)}%</small>
      : <small>
        {scoreBlocked ? "门禁后参考置信度" : "发布置信度"} {Math.round(confidence * 100)}% · 资料覆盖{evidenceComplete ? "完整" : "不足"}
        {directionalEvidenceComplete !== undefined && ` · 方向证据${directionalEvidenceComplete ? "通过" : "未通过"}`}
      </small>}
  </div>;
}

export function ModelOpinion({
  direction,
  rating,
  confidence,
}: {
  direction?: string | null;
  rating?: string | null;
  confidence?: number | null;
}) {
  if (!direction && !rating && typeof confidence !== "number") return null;
  return <section className="model-opinion">
    <h3>7B 模型原始意见 / 7B model raw opinion</h3>
    <p>这是模型基于当前输入给出的独立原始意见；证据质量核验会降低最终置信度，但不会隐藏或归零方向评分。 / Evidence quality checks may reduce final confidence without hiding or zeroing the directional score.</p>
    <div className="model-opinion-grid">
      <span>方向 / Direction<strong>{direction ? modelDirectionLabels[direction] || direction : "未留存 / Not retained"}</strong></span>
      <span>五档评级 / Rating<strong>{rating ? modelRatingLabels[rating === "官网" ? "watch" : rating] || recommendationRatingLabel(rating) : "未留存 / Not retained"}</strong></span>
      <span>原始置信度 / Confidence<strong>{typeof confidence === "number" ? `${Math.round(confidence * 100)}%` : "未留存 / Not retained"}</strong></span>
    </div>
  </section>;
}

export type ConclusionResearchState = {
  status: "pending" | "queued" | "error";
  error?: string;
};

export function eventRefreshResearchState(
  refresh?: EventResearchRefresh | null,
): ConclusionResearchState | undefined {
  if (!refresh) return undefined;
  if (refresh.status === "failed") {
    return { status: "error", error: refresh.error || "完整重新研究失败" };
  }
  return { status: "queued" };
}

export function ResearchAgainButton({
  state,
  onResearch,
  label = "重新调研",
}: {
  state?: ConclusionResearchState;
  onResearch: () => void;
  label?: string;
}) {
  const pending = state?.status === "pending";
  const queued = state?.status === "queued";
  return <div className="conclusion-research-action">
    <button
      type="button"
      className="research-again"
      disabled={pending || queued}
      onClick={onResearch}
    >{pending ? `${label}中…` : queued ? "已进入队列" : label}</button>
    {state?.status === "error" && <small role="alert">{state.error || `${label}失败`}</small>}
  </div>;
}

function factorValue(value: number) {
  return Number.isFinite(value) ? String(Number(value.toFixed(3))) : "0";
}

function signedFactorValue(value: number) {
  return `${value > 0 ? "+" : ""}${factorValue(value)}`;
}

function roundHalfUp(value: number) {
  return value >= 0 ? Math.floor(value + 0.5) : Math.ceil(value - 0.5);
}

function FactorEvidence({ factor }: { factor: ScoringFactor }) {
  return <>
    {factor.reason && <p>{factor.reason}</p>}
    {!!factor.evidence_ids?.length && <small>证据：{factor.evidence_ids.join("、")}</small>}
  </>;
}

function SystemFactorEvidence({ factor }: { factor: SystemConfidenceFactor }) {
  return <>
    {factor.reason && <p>{factor.reason}</p>}
    {!!factor.evidence_ids?.length && <small>依据证据：{factor.evidence_ids.join("、")}</small>}
  </>;
}

export function V3ConfidenceDetails({ recommendation }: { recommendation: Recommendation }) {
  const news = recommendation.news_confidence_factors;
  const rating = recommendation.rating_confidence_factors;
  if (!news && !rating) return null;
  const newsDefinitions: Array<[string, string, number, SystemConfidenceFactor]> = news ? [
    ["S", "信息源可靠性", 30, news.source_reliability],
    ["P", "原始性", 20, news.originality],
    ["V", "多源交叉验证", 20, news.cross_verification],
    ["C", "信息明确程度", 15, news.clarity],
    ["T", "时效性与完整性", 15, news.timeliness_completeness],
  ] : [];
  const ratingDefinitions: Array<[string, string, number, SystemConfidenceFactor]> = rating ? [
    ["M", "标的映射强度", 25, rating.mapping_strength],
    ["C", "因果确定性", 20, rating.causality_certainty],
    ["H", "历史规律", 15, rating.historical_pattern],
    ["I", "影响规模", 15, rating.impact_scale],
    ["T", "时间确定性", 10, rating.timing_certainty],
    ["K", "市场一致性", 15, rating.market_consistency],
  ] : [];
  const factorGrid = (definitions: Array<[string, string, number, SystemConfidenceFactor]>) => <div className="score-factor-grid confidence-factor-grid">
    {definitions.map(([code, label, weight, factor]) => <article key={`${code}-${label}`}>
      <span>{code} · {label}</span>
      <strong>{Math.round(factor.value * weight)} / {weight}</strong>
      <SystemFactorEvidence factor={factor} />
    </article>)}
  </div>;
  return <section className="short-term-score-details v3-confidence-details">
    {news && <><h3>新闻可信度五因子</h3><p className="score-formula">30%S + 20%P + 20%V + 15%C + 15%T</p>{factorGrid(newsDefinitions)}</>}
    {rating && <><h3>评级置信度六因子</h3><p className="score-formula">25%M + 20%C + 15%H + 15%I + 10%T + 15%K · 映射距离 L{recommendation.mapping_distance ?? 5}</p>{factorGrid(ratingDefinitions)}</>}
  </section>;
}

export function ShortTermScoreDetails({ recommendation }: { recommendation: Recommendation }) {
  const impact = recommendation.impact_factors;
  const confidence = recommendation.confidence_factors;
  const warnings = recommendation.evidence_warnings || [];
  if (!impact && !confidence && !warnings.length) return null;
  const impactDefinitions: Array<[string, string, number, ScoringFactor]> = impact ? [
    ["M", "变动幅度", 45, impact.magnitude],
    ["T", "持续性", 25, impact.persistence],
    ["I", "标的代表性", 15, impact.representativeness],
    ["C", "市场确认", 15, impact.market_confirmation],
  ] : [];
  const confidenceDefinitions: Array<[string, string, number, ScoringFactor]> = confidence ? [
    ["A", "方向明确度", 40, confidence.direction_clarity],
    ["R", "事实与来源可靠度", 25, confidence.source_reliability],
    ["Q", "幅度分类确定性", 20, confidence.magnitude_certainty],
    ["K", "趋势及市场信息完整度", 15, confidence.market_context_completeness],
  ] : [];
  const impactSubtotal = impactDefinitions.reduce((total, [, , weight, factor]) => total + factor.value * weight, 0);
  const calculatedImpactScore = impact ? roundHalfUp(impact.direction * impactSubtotal) : 0;
  return <section className="short-term-score-details">
    {impact && <>
      <h3>短线影响因子</h3>
      <p className="score-formula">S = D × (45M + 25T + 15I + 15C)，D = {impact.direction > 0 ? "+1" : impact.direction < 0 ? "-1" : "0"}</p>
      <p className="score-calculation-summary">因子合计 {factorValue(impactSubtotal)}；乘以方向并四舍五入后为 <strong>{signedFactorValue(calculatedImpactScore)}</strong></p>
      <div className="score-factor-grid impact-factor-grid">
        {impactDefinitions.map(([code, label, weight, factor]) => <article key={code}>
          <span>{code} · {label}</span>
          <strong>{factorValue(factor.value)} × {weight} × {impact.direction} = {signedFactorValue(factor.value * weight * impact.direction)}</strong>
          <FactorEvidence factor={factor} />
        </article>)}
      </div>
    </>}
    {confidence && <>
      <h3>评级置信度因素</h3>
      <p className="score-formula">置信度 = 40%A + 25%R + 20%Q + 15%K</p>
      <div className="score-factor-grid confidence-factor-grid">
        {confidenceDefinitions.map(([code, label, weight, factor]) => <article key={code}>
          <span>{code} · {label}</span>
          <strong>{Math.round(factor.value * weight)} / {weight}</strong>
          <FactorEvidence factor={factor} />
        </article>)}
      </div>
    </>}
    {!!warnings.length && <>
      <h3>证据质量提示</h3>
      <ul className="evidence-warnings">{warnings.map((warning, index) => <li key={`${warning}-${index}`}>{warning}</li>)}</ul>
    </>}
  </section>;
}

export type GateReasonExplanation = {
  title: string;
  explanation: string;
};

const gateReasonExplanations: Record<string, GateReasonExplanation> = {
  summary: {
    title: "核心观点缺失 / Summary missing",
    explanation: "报告没有形成可由证据验证的核心结论，因此不能发布方向评分。 / No evidence-verifiable core conclusion was produced, so no directional score can be published.",
  },
  products_or_protocol: {
    title: "产品、主营业务或协议影响缺失 / Products, business, or protocol impact missing",
    explanation: "股票场景中此项指产品或主营业务，不是要求存在区块链协议。报告需要说明事件如何影响收入、成本、订单、用户或竞争力；该部分为空或仅写证据不足时会被过滤。 / For equities, this means products or core business, not a blockchain protocol. The report must explain the event&apos;s effect on revenue, costs, orders, users, or competitiveness.",
  },
  valuation_or_tokenomics: {
    title: "估值或代币经济分析缺失 / Valuation or tokenomics analysis missing",
    explanation: "股票需要估值影响分析，加密资产需要代币经济分析；没有证据支持的相关分析时不发布评分。 / Equities require valuation impact analysis and crypto assets require tokenomics analysis; unsupported or missing analysis blocks publication.",
  },
  risks: {
    title: "风险分析缺失 / Risks missing",
    explanation: "报告未给出可验证的反向风险，无法判断观点是否只考虑了单边信息。 / The report lacks verifiable downside risks, so the system cannot confirm that the thesis considered both sides.",
  },
  invalidation_conditions: {
    title: "失效条件缺失 / Invalidation conditions missing",
    explanation: "报告未说明出现什么事实时应推翻当前观点，因此观点不可检验。 / The report does not state which facts would invalidate the thesis, making it non-falsifiable.",
  },
  "evidence citations": {
    title: "缺少证据引用 / Evidence citations missing",
    explanation: "结论没有绑定到可追溯的证据记录，无法核验观点来源。 / The conclusion is not linked to traceable evidence records, so its source cannot be verified.",
  },
  "one official source or two independent sources": {
    title: "来源数量或独立性不足 / One official source or two independent sources required",
    explanation: "至少需要一个直接相关的官方来源，或两个相互独立的来源；同一报道的转载不算两个来源。 / At least one directly relevant official source or two independent sources are required; syndicated copies count as one source.",
  },
  "claim-level evidence strength is below the publication threshold": {
    title: "观点级证据强度低于发布门槛 / Claim-level evidence strength below publication threshold",
    explanation: "逐条观点的证据覆盖率、来源质量或复核置信度不足，系统保留研究内容但不发布方向评分。 / Claim coverage, source quality, or verification confidence is insufficient; the research is retained without a published directional score.",
  },
  "direction weakened below the publication threshold after evidence gating": {
    title: "门禁折减后方向强度不足 / Direction below publication threshold after evidence gating",
    explanation: "方向信号经过证据强度和标的映射可信度折减后低于看多或看空门槛，因此不发布方向评分。 / After evidence-strength and asset-mapping adjustments, the signal falls below the bullish or bearish publication threshold.",
  },
  "claim stances do not support the deterministic direction": {
    title: "观点立场不支持程序方向 / Claim stances do not support the deterministic direction",
    explanation: "逐观点证据的多空倾向与程序计算方向不一致，方向复核未通过。 / Evidence-backed claim stances do not align with the calculated direction, so directional verification failed.",
  },
  "semantic evidence verifier unavailable": {
    title: "语义证据复核不可用 / Semantic evidence verifier unavailable",
    explanation: "负责逐观点核验证据的模型或服务未完成复核，当前不能发布评分。 / The claim-level verification model or service did not complete, so a score cannot be published.",
  },
  "high-impact independent cloud review rejected": {
    title: "高影响独立复核未通过 / High-impact independent review rejected",
    explanation: "高影响方向需要额外独立复核，本次复核未批准。 / High-impact directions require an additional independent review, which did not approve this conclusion.",
  },
  "high-impact cloud verifier unavailable": {
    title: "高影响独立复核不可用 / High-impact independent verifier unavailable",
    explanation: "高影响结论所需的独立复核服务不可用，因此按保守规则不发布。 / The independent verifier required for a high-impact conclusion was unavailable, so publication was blocked.",
  },
  "point-in-time boundary violation": {
    title: "时间边界违规 / Point-in-time boundary violation",
    explanation: "证据晚于研究截止时间，存在使用未来信息的风险。 / Evidence is later than the research cutoff, creating look-ahead risk.",
  },
};

export function explainGateReason(reason: string): GateReasonExplanation {
  const exact = gateReasonExplanations[reason];
  if (exact) return exact;
  if (reason.startsWith("asset mapping confidence ")) return {
    title: "标的映射可信度不足 / Asset mapping confidence below threshold",
    explanation: "事件与该证券之间的映射可信度低于 65%，不足以发布该标的的方向评分。 / Confidence that the event maps to this security is below 65%, so its directional score is withheld.",
  };
  if (reason.startsWith("unsupported claim:") || reason.startsWith("contradicted claim:")) return {
    title: "观点缺少支持或存在矛盾 / Unsupported or contradicted claim",
    explanation: "至少一条重要观点没有被引用证据支持，或与证据相矛盾。 / At least one material claim is unsupported by, or contradicts, its cited evidence.",
  };
  if (reason.startsWith("unknown evidence ids:")) return {
    title: "引用了无效证据 / Unknown evidence citation",
    explanation: "报告引用了当前研究记录中不存在的证据 ID，无法完成追溯。 / The report cites evidence IDs that do not exist in this research record.",
  };
  if (/^(model|semantic_verifier|cloud_verifier)_/.test(reason)) return {
    title: "研究依赖发生技术错误 / Research dependency failed",
    explanation: "模型或复核服务发生技术异常，本次结果按失败处理且不发布评分。 / A model or verification dependency failed; this run is treated as a technical failure with no published score.",
  };
  return {
    title: "其他门禁原因 / Other gate reason",
    explanation: "该原因尚无专用翻译，请结合下方原始原因排查。 / No dedicated translation is available yet; use the raw reason below for diagnosis.",
  };
}

function GateReasonItem({ reason }: { reason: string }) {
  const detail = explainGateReason(reason);
  return <li>
    <strong>{detail.title}</strong>
    <span>{detail.explanation}</span>
    <code>{reason}</code>
  </li>;
}

export function GateReasons({
  primaryReason,
  allReasons = [],
}: {
  primaryReason?: string | null;
  allReasons?: string[];
}) {
  const reasons = [...new Set(allReasons.filter(Boolean))];
  const blockingReason = primaryReason || reasons[0];
  if (!blockingReason) return null;
  return <>
    <h3>门禁原因 / Primary gate reason</h3>
    <ul className="gate-reasons"><GateReasonItem reason={blockingReason} /></ul>
    {!!reasons.length && <>
      <h3>所有门禁原因 / All gate reasons</h3>
      <ul className="gate-reasons">{reasons.map((item) => <GateReasonItem key={item} reason={item} />)}</ul>
    </>}
  </>;
}

export function recommendationAssetKey(recommendation: Pick<Recommendation, "asset">) {
  return recommendation.asset.asset_id || `${recommendation.asset.market}:${recommendation.asset.symbol}`;
}

export function ConclusionCard({
  item,
  researchState,
  onOpen,
  onResearch,
}: {
  item: Recommendation;
  researchState?: ConclusionResearchState;
  onOpen: () => void;
  onResearch: () => void;
}) {
  return <article className="conclusion-card">
    <button type="button" className="conclusion-card-details" onClick={onOpen} aria-label={`查看 ${item.asset.symbol} 研究详情`}>
      <div className="conclusion-card-copy"><span>{item.asset.market} · {new Date(item.as_of).toLocaleString("zh-CN")}</span><strong>{item.asset.symbol} · {item.asset.name}</strong><p>{item.thesis.summary}</p></div>
      <ConclusionScore
        score={item.score}
        directionScore={item.direction_score}
        rating={item.rating}
        confidence={item.confidence}
        newsConfidence={item.news_confidence}
        ratingConfidence={item.rating_confidence}
        evidenceComplete={item.evidence_complete}
        directionalEvidenceComplete={item.directional_evidence_complete}
        signalStatus={item.signal_status}
        factConfidence={item.fact_confidence}
        horizonDays={item.horizon_days}
        horizonUnit={item.horizon_unit}
        scoringVersion={item.scoring_version}
        scoreSource={item.score_source}
        compact
      />
    </button>
    <ResearchAgainButton state={researchState} onResearch={onResearch} />
  </article>;
}

const eventConclusionStatusLabels: Record<string, string> = {
  completed: "已完成",
  insufficient_evidence: "证据不足",
};

export function EventConclusionCard({
  item,
  researchState,
  onOpen,
  onResearch,
}: {
  item: ResearchConclusionItem;
  researchState?: ConclusionResearchState;
  onOpen: () => void;
  onResearch: () => void;
}) {
  const report = item.report;
  const score = report?.direction_score;
  const rating = report?.rating;
  const scoreTone = score === null || score === undefined ? "neutral" : score < 0 ? "negative" : score > 0 ? "positive" : "neutral";
  return <article className="conclusion-card event-conclusion-card">
    <button type="button" className="conclusion-card-details" onClick={onOpen} aria-label={`查看 ${item.title} 事件研报`}>
      <div className="conclusion-card-copy">
        <span>{item.event?.event_type ?? "other"} · {new Date(item.occurred_at).toLocaleString("zh-CN")}</span>
        <strong>{item.title}</strong>
        <p>{item.summary}</p>
      </div>
    </button>
    <div className="event-conclusion-side">
      <button type="button" className={`event-conclusion-summary ${scoreTone}`} onClick={onOpen} aria-label={`查看 ${item.title} 事件研报评分`}>
        <strong>{score === null || score === undefined
          ? "— · 暂无评级"
          : `${score > 0 ? "+" : ""}${score} · ${rating ? recommendationRatingLabel(rating) : "暂无评级"}`}</strong>
        <span>影响目标 {report?.impact_count ?? 0} 个</span>
        <small>新闻可信度 {Math.round((report?.news_confidence ?? 0) * 100)}% · 研报置信度 {Math.round((report?.confidence ?? 0) * 100)}%</small>
      </button>
      <ResearchAgainButton state={researchState} onResearch={onResearch} label="重新研究" />
    </div>
  </article>;
}

export const changedTargetDesktopColumns = 4;

export function ChangedTargetGrid({
  items,
  researchStates = {},
  onResearch = () => undefined,
  onOpen = () => undefined,
  detailLoadingId = "",
}: {
  items: ChangedTarget[];
  researchStates?: Record<string, ConclusionResearchState>;
  onResearch?: (item: ChangedTarget) => void;
  onOpen?: (item: ChangedTarget) => void;
  detailLoadingId?: string;
}) {
  return <div className="target-change-grid" data-columns={changedTargetDesktopColumns}>
    {items.map((item) => <article className="target-change-card" key={item.asset.asset_id}>
      <header>
        <span>{item.asset.market} · {new Date(item.changed_at).toLocaleString("zh-CN")}</span>
        <div className="target-change-symbol-row">
          <button
            type="button"
            className="target-change-identity"
            aria-label={`查看 ${item.asset.symbol} 最近一次调研`}
            aria-busy={detailLoadingId === item.asset.asset_id}
            disabled={detailLoadingId === item.asset.asset_id}
            onClick={() => onOpen(item)}
          >
            <strong>{item.asset.symbol}</strong>
            <small>{detailLoadingId === item.asset.asset_id ? "正在加载最近调研…" : item.asset.name}</small>
          </button>
          <ResearchAgainButton state={researchStates[item.asset.asset_id]} onResearch={() => onResearch(item)} />
        </div>
      </header>
      <div className="target-change-field changed">
        <span>评级</span>
        <strong>{recommendationRatingLabel(item.previous.rating)} → {recommendationRatingLabel(item.current.rating)}</strong>
      </div>
    </article>)}
  </div>;
}

export function ChangedTargetsContent({
  items, loading, error, onRetry, researchStates = {}, onResearch = () => undefined,
  onOpen = () => undefined, detailLoadingId = "",
}: {
  items: ChangedTarget[];
  loading: boolean;
  error: string;
  onRetry: () => void;
  researchStates?: Record<string, ConclusionResearchState>;
  onResearch?: (item: ChangedTarget) => void;
  onOpen?: (item: ChangedTarget) => void;
  detailLoadingId?: string;
}) {
  return <>
    {error && <div className="page-error target-change-error"><span>{error}</span><button type="button" onClick={onRetry}>重试</button></div>}
    {!items.length && !error && (loading
      ? <div className="page-message">正在加载标的评级变化…</div>
      : <div className="page-empty">当前没有评级发生变化的标的。</div>)}
    {!!items.length && <ChangedTargetGrid items={items} researchStates={researchStates} onResearch={onResearch} onOpen={onOpen} detailLoadingId={detailLoadingId} />}
  </>;
}

export function ConclusionDetailModal({ detail, onClose }: { detail: ConclusionDetail; onClose: () => void }) {
  const isV3 = detail.recommendation.scoring_version === "llm-direction-v3";
  const isShortTerm = detail.recommendation.scoring_version === "short-term-impact-v1"
    || detail.recommendation.horizon_unit === "trading_sessions";
  return <div className="modal-backdrop" onClick={onClose}>
    <article className="modal conclusion-modal" onClick={(event) => event.stopPropagation()}>
      <button type="button" className="close" aria-label="关闭调研详情" onClick={onClose}>×</button>
      <p className="eyebrow">{detail.recommendation.asset.market} · {detail.recommendation.asset.symbol} · {new Date(detail.recommendation.as_of).toLocaleString("zh-CN")}</p>
      <h2>{detail.recommendation.asset.name}</h2>
      <ConclusionScore
        score={detail.recommendation.score}
        directionScore={detail.recommendation.direction_score}
        rating={detail.recommendation.rating}
        confidence={detail.recommendation.confidence}
        newsConfidence={detail.recommendation.news_confidence}
        ratingConfidence={detail.recommendation.rating_confidence}
        evidenceComplete={detail.recommendation.evidence_complete}
        directionalEvidenceComplete={detail.recommendation.directional_evidence_complete}
        signalStatus={detail.recommendation.signal_status}
        factConfidence={detail.recommendation.fact_confidence}
        horizonDays={detail.recommendation.horizon_days}
        horizonUnit={detail.recommendation.horizon_unit}
        scoringVersion={detail.recommendation.scoring_version}
        scoreSource={detail.recommendation.score_source}
      />
      <p className="score-explanation">{isV3
        ? "方向分是模型唯一数值判断；五级评级、新闻可信度和评级置信度均由系统独立计算，缺失信息只降低对应置信因子。"
        : isShortTerm
        ? "影响分按 D × (45M + 25T + 15I + 15C) 计算；证据质量核验只降低置信度，不改变方向或隐藏评分。"
        : "该历史结论沿用原评分与证据门禁规则；证据不足记录继续暂不评分，供追溯和重新调研。"}</p>
      {!isV3 && <ModelOpinion direction={detail.recommendation.model_direction} rating={detail.recommendation.model_rating} confidence={detail.recommendation.model_confidence} />}
      {isV3
        ? <>
          <V3ConfidenceDetails recommendation={detail.recommendation} />
          <div className="probability-grid"><span>牛市 <strong>{Math.round(detail.recommendation.bull_probability * 100)}%</strong></span><span>中性 <strong>{Math.round(detail.recommendation.base_probability * 100)}%</strong></span><span>熊市 <strong>{Math.round(detail.recommendation.bear_probability * 100)}%</strong></span></div>
        </>
        : isShortTerm
        ? <ShortTermScoreDetails recommendation={detail.recommendation} />
        : <>
          <div className="probability-grid"><span>看多 <strong>{Math.round(detail.recommendation.bull_probability * 100)}%</strong></span><span>基准 <strong>{Math.round(detail.recommendation.base_probability * 100)}%</strong></span><span>看空 <strong>{Math.round(detail.recommendation.bear_probability * 100)}%</strong></span></div>
          <div className="research-gate-grid">
            <span>程序原始分<strong>{detail.recommendation.score_available === false || ["technical_failure", "insufficient_evidence"].includes(detail.recommendation.signal_status || "") ? "—" : <>{(detail.recommendation.raw_score ?? detail.recommendation.score ?? 0) > 0 ? "+" : ""}{detail.recommendation.raw_score ?? detail.recommendation.score ?? 0}</>}</strong></span>
            <span>证据强度<strong>{Math.round((detail.recommendation.evidence_strength ?? (detail.recommendation.evidence_complete ? 1 : 0)) * 100)}%</strong></span>
            <span>映射可信度<strong>{Math.round((detail.recommendation.mapping_confidence ?? 1) * 100)}%</strong></span>
            <span>研究期限<strong>{detail.recommendation.horizon_days ?? 90} 天</strong></span>
          </div>
          <GateReasons primaryReason={detail.recommendation.primary_gate_reason} allReasons={detail.recommendation.gate_reasons} />
        </>}
      <h3>核心观点</h3><p>{detail.recommendation.thesis.summary}</p>
      {detail.recommendation.thesis.historical_context && <><h3>历史背景</h3><p>{detail.recommendation.thesis.historical_context}</p></>}
      <h3>催化剂</h3><ul>{detail.recommendation.thesis.catalysts.map((item) => <li key={item}>{item}</li>)}</ul>
      <h3>风险</h3><ul>{detail.recommendation.thesis.risks.map((item) => <li key={item}>{item}</li>)}</ul>
      <h3>失效条件</h3><ul>{detail.recommendation.thesis.invalidation_conditions.map((item) => <li key={item}>{item}</li>)}</ul>
      {!!detail.recommendation.claim_assessments?.length && <><h3>{isV3 || isShortTerm ? "逐观点证据核验" : "逐观点证据门禁"}</h3><div className="claim-assessments">{detail.recommendation.claim_assessments.map((item, index) => <article key={`${item.claim_kind}-${index}`}><span>{item.claim_kind} · {item.verdict}</span><strong>{item.claim}</strong><small>证据核验 {Math.round(item.confidence * 100)}%{item.reason ? ` · ${item.reason}` : ""}</small></article>)}</div></>}
      {detail.event && <><h3>关联事件</h3><p>{detail.event.headline}</p></>}
      <h3>新闻与证据</h3><div className="evidence-links">{conclusionReferences(detail).map((item) => <a key={`${item.url}-${item.label}`} href={item.url} target="_blank" rel="noreferrer"><strong>{item.label}</strong><span>{item.source}</span></a>)}</div>
    </article>
  </div>;
}

export function ResearchDetailLoadingModal({ title, onClose }: { title: string; onClose: () => void }) {
  return <div className="modal-backdrop" onClick={onClose}>
    <article className="modal research-detail-loading-modal" aria-busy="true" aria-live="polite" onClick={(event) => event.stopPropagation()}>
      <button type="button" className="close" aria-label="关闭研究报告加载提示" onClick={onClose}>×</button>
      <p className="eyebrow">RESEARCH REPORT</p>
      <h2>{title}</h2>
      <div className="page-message">正在加载研究报告… / Loading research report…</div>
    </article>
  </div>;
}

const targetTypeLabels: Record<string, string> = {
  economy: "宏观经济",
  supply_volume: "供给量",
  commodity_price: "商品价格",
  fx_rate: "汇率",
  interest_rate: "利率",
  sector: "行业",
  tradable_asset: "具体标的",
  risk_asset: "风险资产",
  shipping: "航运",
  other: "其他",
};

const missingInformationDescriptions: Record<string, [chinese: string, english: string]> = {
  target_direction: ["目标影响方向尚未明确", "The target impact direction is not yet established"],
  transmission_evidence: ["缺少影响传导路径的证据", "Evidence for the impact transmission path is missing"],
  action_stage: ["事件行动阶段尚未明确", "The event action stage is not yet established"],
  impact_evidence: ["缺少对目标影响的直接证据", "Direct evidence for the target impact is missing"],
  evidence_gate: ["证据质量核验尚未通过", "Evidence quality verification has not passed"],
  industry_only_mapping: ["仅映射到行业，尚未确认具体可交易标的", "Only the industry is mapped; no specific tradable asset is confirmed"],
  sanction_scope: ["制裁范围尚未明确", "The sanction scope is not yet established"],
  whether_oil_exports_are_targeted: ["尚未确认制裁是否针对石油出口", "It is not yet confirmed whether oil exports are targeted"],
  secondary_sanctions: ["二级制裁范围尚未明确", "The scope of secondary sanctions is not yet established"],
  effective_date: ["生效日期尚未明确", "The effective date is not yet established"],
  affected_target: ["受影响目标尚未明确", "The affected target is not yet established"],
  transmission_path: ["影响传导路径尚未明确", "The impact transmission path is not yet established"],
  tradable_asset_path: ["尚未确认对应的可交易标的", "The corresponding tradable asset has not yet been confirmed"],
  "实际的制裁范围、生效日、支付结算、港口航运、实际供应或市场反应": [
    "实际制裁范围、生效日期、支付结算、港口航运、实际供应或市场反应尚待确认",
    "The actual sanction scope, effective date, payment settlement, port shipping, physical supply, or market reaction remains unconfirmed",
  ],
};

export function describeMissingInformation(value: string) {
  const raw = value.trim();
  const detail = missingInformationDescriptions[raw] ?? missingInformationDescriptions[raw.toLowerCase()];
  if (detail) return `${detail[0]} / ${detail[1]}`;
  if (/\p{Script=Han}/u.test(raw)) {
    return `${raw || "相关信息尚待确认"} / Additional verified information is required for this item`;
  }
  const readable = raw.replace(/[_:]+/g, " ").replace(/\s+/g, " ").trim() || "related information";
  return `缺少相关信息：${readable} / Missing information: ${readable}`;
}

export function EventConclusionDetailModal({ detail, onClose }: { detail: EventConclusionDetail; onClose: () => void }) {
  const report = detail.report;
  return <div className="modal-backdrop" onClick={onClose}>
    <article className="modal conclusion-modal event-conclusion-modal" onClick={(event) => event.stopPropagation()}>
      <button type="button" className="close" aria-label="关闭事件研报详情" onClick={onClose}>×</button>
      <p className="eyebrow">{detail.event?.event_type ?? "other"} · {new Date(detail.run.updated_at).toLocaleString("zh-CN")}</p>
      <h2>{detail.event?.headline ?? "事件研报"}</h2>
      <div className="event-report-metrics">
        <span>研究状态<strong>{eventConclusionStatusLabels[detail.run.status] ?? detail.run.status}</strong></span>
        <span>新闻可信度<strong>{Math.round(report.news_confidence * 100)}%</strong></span>
        <span>研报置信度<strong>{Math.round(report.confidence * 100)}%</strong></span>
        <span>影响目标<strong>{report.impacts.length}</strong></span>
      </div>
      {!report.evidence_complete && <div className="page-message">该报告可追溯，但资料覆盖不足，不应视为可直接交易的确定性结论。</div>}
      <h3>事件结论</h3><p>{report.summary}</p>
      {(report.affected_markets.length > 0 || report.affected_sectors.length > 0) && <div className="event-report-scope">
        <span>市场：{report.affected_markets.join("、") || "未明确"}</span>
        <span>行业：{report.affected_sectors.join("、") || "未明确"}</span>
      </div>}
      {!!report.impacts.length && <><h3>目标影响</h3><div className="event-impact-grid">{report.impacts.map((impact, index) => <article key={`${impact.target_type}-${impact.target_name}-${index}`}>
        <span>{targetTypeLabels[impact.target_type] ?? impact.target_type}{impact.asset?.symbol ? ` · ${impact.asset.symbol}` : ""}</span>
        <strong>{impact.target_name}</strong>
        <div><b>{recommendationRatingLabel(impact.rating)}</b><b>{impact.direction_score > 0 ? "+" : ""}{impact.direction_score}</b><b>置信度 {Math.round(impact.rating_confidence * 100)}%</b></div>
        <small>未来 {impact.horizon_days} 个自然日</small>
        {impact.rationale && <p>{impact.rationale}</p>}
        {!!impact.transmission_path.length && <ol>{impact.transmission_path.map((step, stepIndex) => <li key={`${step}-${stepIndex}`}>{step}</li>)}</ol>}
        {!!impact.missing_information.length && <small>缺失 / Missing：{impact.missing_information.map(describeMissingInformation).join("；")}</small>}
      </article>)}</div></>}
      {!!report.macro_factors.length && <><h3>宏观因子</h3><div className="macro-factor-list">{report.macro_factors.map((factor) => <article key={factor.id}><strong>{factor.name}</strong><span>{Math.round(factor.strength * 100)}%</span><p>{factor.description}</p></article>)}</div></>}
      {!!report.scenarios.length && <><h3>情景</h3><ul>{report.scenarios.map((item) => <li key={item}>{item}</li>)}</ul></>}
      {!!report.catalysts.length && <><h3>催化剂</h3><ul>{report.catalysts.map((item) => <li key={item}>{item}</li>)}</ul></>}
      {!!report.risks.length && <><h3>风险</h3><ul>{report.risks.map((item) => <li key={item}>{item}</li>)}</ul></>}
      {!!report.unresolved_questions.length && <><h3>待确认问题</h3><ul>{report.unresolved_questions.map((item) => <li key={item}>{item}</li>)}</ul></>}
      {!!report.missing_information.length && <><h3>缺失信息 / Missing information</h3><ul>{report.missing_information.map((item) => <li key={item}>{describeMissingInformation(item)}</li>)}</ul></>}
      <h3>新闻与证据</h3><div className="evidence-links">{conclusionReferences(detail).map((item) => <a key={`${item.url}-${item.label}`} href={item.url} target="_blank" rel="noreferrer"><strong>{item.label}</strong><span>{item.source}</span></a>)}</div>
    </article>
  </div>;
}

export function TargetChangeGrid({
  items,
  onOpen,
  detailLoadingId = "",
  researchStates = {},
  onResearch,
}: {
  items: TargetChange[];
  onOpen: (item: TargetChange) => void;
  detailLoadingId?: string;
  researchStates?: Record<string, ConclusionResearchState>;
  onResearch?: (item: TargetChange) => void;
}) {
  return <div className="target-change-grid unified-target-change-grid" data-columns={changedTargetDesktopColumns}>
    {items.map((item) => {
      const score = item.latest.direction_score;
      const newsConfidence = item.latest.news_confidence;
      const confidence = item.latest.rating_confidence;
      return <article className={`target-change-card ${item.kind}`} key={item.key}>
        <header>
          <span>{item.target_type === "commodity_price" || item.kind === "macro" ? (targetTypeLabels[item.target_type] ?? item.target_type) : item.market} · {new Date(item.changed_at).toLocaleString("zh-CN")}</span>
          <div className="target-change-symbol-row">
            <button
              type="button"
              className="target-change-identity"
              title={item.label}
              aria-label={`查看 ${item.symbol || item.label} 最新研究`}
              aria-busy={detailLoadingId === item.key}
              disabled={detailLoadingId === item.key}
              onClick={() => onOpen(item)}
            >
              <strong>{item.symbol || item.label}</strong>
              {item.symbol && <small>{item.label}</small>}
            </button>
          </div>
        </header>
        <div className="target-change-field changed"><span>{item.latest_detail.kind === "event" ? "最近事件评级变化" : "评级变化"}</span><div className="target-change-rating-row"><strong>{recommendationRatingLabel(item.previous?.rating || item.current.rating)} → {recommendationRatingLabel(item.current.rating)}</strong><b className={score === null ? "neutral" : score < 0 ? "negative" : score > 0 ? "positive" : "neutral"} title="最新方向分">{score === null ? "—" : `${score > 0 ? "+" : ""}${score}`}</b></div></div>
        {item.trend && <TargetTrendSummary trend={item.trend} />}
        <div className={`target-change-latest${onResearch ? " with-research" : ""}`}>
          <span>新闻可信度<strong>{newsConfidence === null ? "—" : `${Math.round(newsConfidence * 100)}%`}</strong></span>
          <span>评级置信度<strong>{confidence === null ? "—" : `${Math.round(confidence * 100)}%`}</strong></span>
          {onResearch && <ResearchAgainButton state={researchStates[targetChangeResearchKey(item)]} onResearch={() => onResearch(item)} />}
        </div>
      </article>;
    })}
  </div>;
}

export function targetChangeResearchKey(item: TargetChange) {
  return item.latest_detail.kind === "event"
    ? `event:${item.latest_detail.id}`
    : `asset:${item.key}`;
}

export const targetChangeSearchDebounceMs = 300;

export function shouldSkipTargetChangeRefresh(silent: boolean, inFlight: boolean) {
  return silent && inFlight;
}

export function buildTargetChangeQuery(
  kind: "macro" | "asset",
  query: string,
  cursor: string | null = null,
) {
  const params = new URLSearchParams({ kind, limit: "50" });
  const normalizedQuery = query.trim();
  if (normalizedQuery) params.set("q", normalizedQuery);
  if (cursor) params.set("cursor", cursor);
  return params.toString();
}

function TargetChangeSection({
  apiBase,
  kind,
  title,
  copy,
  onOpen,
  detailLoadingId,
  researchStates,
  onResearch,
  query,
}: {
  apiBase: string;
  kind: "macro" | "asset";
  title: string;
  copy: string;
  onOpen: (item: TargetChange) => void;
  detailLoadingId: string;
  researchStates: Record<string, ConclusionResearchState>;
  onResearch?: (item: TargetChange) => void;
  query: string;
}) {
  const [items, setItems] = useState<TargetChange[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState("");
  const cursorRef = useRef<string | null>(null);
  const requestIdRef = useRef(0);
  const inFlightRef = useRef(false);

  const load = useCallback(async (append = false, silent = false) => {
    if (shouldSkipTargetChangeRefresh(silent, inFlightRef.current)) return;
    const requestId = ++requestIdRef.current;
    inFlightRef.current = true;
    const params = buildTargetChangeQuery(kind, query, append ? cursorRef.current : null);
    if (append) setLoadingMore(true);
    else {
      setLoadingMore(false);
      if (!silent) setLoading(true);
    }
    try {
      const response = await fetch(`${apiBase}/api/v1/target-changes?${params}`);
      if (!response.ok) throw new Error(`${title}请求失败`);
      const payload = await response.json() as { items: TargetChange[]; next_cursor: string | null };
      if (requestId !== requestIdRef.current) return;
      setItems((current) => append ? [...current, ...payload.items] : payload.items);
      cursorRef.current = payload.next_cursor;
      setCursor(payload.next_cursor);
      setError("");
    } catch (reason) {
      if (requestId !== requestIdRef.current) return;
      setError(reason instanceof Error ? reason.message : `${title}请求失败`);
    } finally {
      if (requestId === requestIdRef.current) {
        if (append) setLoadingMore(false); else if (!silent) setLoading(false);
        inFlightRef.current = false;
      }
    }
  }, [apiBase, kind, query, title]);

  useEffect(() => {
    cursorRef.current = null;
    setCursor(null);
    setItems([]);
    setError("");
    setLoadingMore(false);
    void load();
    return subscribeLiveRefresh(
      () => void load(false, true),
      researchViewsRefreshIntervalMs,
    );
  }, [load]);

  return <section className={`target-change-section ${kind}`}>
    <header><div><p className="eyebrow">{kind === "macro" ? "MACRO / SECTOR" : "INSTRUMENT TARGETS"}</p><h2>{title}</h2><p>{copy}</p></div><button type="button" disabled={loading} onClick={() => void load()}>{loading ? "刷新中…" : "刷新"}</button></header>
    {error && <div className="page-error target-change-error"><span>{error}</span><button type="button" onClick={() => void load()}>重试</button></div>}
    {!items.length && !error && (loading ? <div className="page-message">正在加载{title}…</div> : <div className="page-empty">{query ? `未找到与“${query}”匹配的评级变化。` : "当前没有最近评级变化。"}</div>)}
    {!!items.length && <TargetChangeGrid items={items} onOpen={onOpen} detailLoadingId={detailLoadingId} researchStates={researchStates} onResearch={onResearch} />}
    {cursor && <button className="load-more" type="button" disabled={loadingMore} onClick={() => void load(true)}>{loadingMore ? "正在加载…" : "加载更多"}</button>}
  </section>;
}

export function ChangedTargetsPage({ apiBase }: { apiBase: string }) {
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [detailLoadingId, setDetailLoadingId] = useState("");
  const [detailError, setDetailError] = useState("");
  const [selectedAsset, setSelectedAsset] = useState<ConclusionDetail | null>(null);
  const [selectedEvent, setSelectedEvent] = useState<EventConclusionDetail | null>(null);
  const [researchStates, setResearchStates] = useState<Record<string, ConclusionResearchState>>({});
  const researchInFlight = useRef(new Set<string>());

  useEffect(() => {
    const timer = window.setTimeout(
      () => setDebouncedQuery(searchQuery.trim()),
      targetChangeSearchDebounceMs,
    );
    return () => window.clearTimeout(timer);
  }, [searchQuery]);

  async function openLatestResearch(item: TargetChange) {
    if (detailLoadingId) return;
    setDetailLoadingId(item.key);
    setDetailError("");
    try {
      const path = item.latest_detail.kind === "event"
        ? `/api/v1/event-conclusions/${item.latest_detail.id}`
        : `/api/v1/conclusions/${item.latest_detail.id}`;
      const response = await fetch(`${apiBase}${path}`);
      if (!response.ok) throw new Error("最近一次调研加载失败");
      if (item.latest_detail.kind === "event") setSelectedEvent(await response.json() as EventConclusionDetail);
      else setSelectedAsset(await response.json() as ConclusionDetail);
    } catch (reason) {
      setDetailError(reason instanceof Error ? reason.message : "最近一次调研加载失败");
    } finally {
      setDetailLoadingId("");
    }
  }

  async function researchAgain(item: TargetChange) {
    const stateKey = targetChangeResearchKey(item);
    if (researchInFlight.current.has(stateKey) || researchStates[stateKey]?.status === "queued") return;
    researchInFlight.current.add(stateKey);
    setResearchStates((current) => ({ ...current, [stateKey]: { status: "pending" } }));
    try {
      if (item.latest_detail.kind === "event") await researchEventConclusion(apiBase, item.latest_detail.id);
      else await researchConclusion(apiBase, item.latest_detail.id);
      setResearchStates((current) => ({ ...current, [stateKey]: { status: "queued" } }));
    } catch (reason) {
      setResearchStates((current) => ({ ...current, [stateKey]: { status: "error", error: reason instanceof Error ? reason.message : "重新调研失败" } }));
    } finally {
      researchInFlight.current.delete(stateKey);
    }
  }

  return <section className="app-page targets-page">
    <PageHeading eyebrow="RATING CHANGES" title="标的评级变化" copy="左侧追踪宏观经济、行业及跨资产目标，右侧追踪具体证券与商品价格；仅展示最近一次五级评级变化。" />
    {detailError && <div className="page-error target-detail-error"><span>{detailError}</span></div>}
    <form className="page-filters target-search" role="search" onSubmit={(event) => { event.preventDefault(); setDebouncedQuery(searchQuery.trim()); }}>
      <input type="search" aria-label="搜索评级变化" placeholder="搜索宏观、行业、代码或标的名称" value={searchQuery} onChange={(event) => setSearchQuery(event.target.value)} />
      {searchQuery && <button type="button" onClick={() => { setSearchQuery(""); setDebouncedQuery(""); }}>清除</button>}
    </form>
    <div className="target-change-split">
      <TargetChangeSection apiBase={apiBase} kind="macro" title="宏观经济与行业变化" copy="经济、行业、汇率、利率、供给、航运与风险资产。" query={debouncedQuery} onOpen={(item) => void openLatestResearch(item)} detailLoadingId={detailLoadingId} researchStates={researchStates} onResearch={(item) => void researchAgain(item)} />
      <TargetChangeSection apiBase={apiBase} kind="asset" title="具体标的变化" copy="股票、加密资产与商品价格的最新五级评级变化及研究结论。" query={debouncedQuery} onOpen={(item) => void openLatestResearch(item)} detailLoadingId={detailLoadingId} researchStates={researchStates} onResearch={(item) => void researchAgain(item)} />
    </div>
    {selectedAsset && <ConclusionDetailModal detail={selectedAsset} onClose={() => setSelectedAsset(null)} />}
    {selectedEvent && <EventConclusionDetailModal detail={selectedEvent} onClose={() => setSelectedEvent(null)} />}
  </section>;
}

export function ConclusionsPage({ apiBase }: { apiBase: string }) {
  const [filters, setFilters] = useState({ kind: "all", q: "", market: "", rating: "" });
  const [items, setItems] = useState<ResearchConclusionItem[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [selectedAsset, setSelectedAsset] = useState<ConclusionDetail | null>(null);
  const [selectedEvent, setSelectedEvent] = useState<EventConclusionDetail | null>(null);
  const [detailLoadingId, setDetailLoadingId] = useState("");
  const [detailLoadingTitle, setDetailLoadingTitle] = useState("");
  const [detailError, setDetailError] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [failedItems, setFailedItems] = useState<FailedResearch[]>([]);
  const [researchInstances, setResearchInstances] = useState<ModelQueueInstanceItem[]>([]);
  const [failuresLoading, setFailuresLoading] = useState(true);
  const [retryingId, setRetryingId] = useState("");
  const [retryingAll, setRetryingAll] = useState(false);
  const [retryMessage, setRetryMessage] = useState("");
  const [retryMessageError, setRetryMessageError] = useState(false);
  const [researchStates, setResearchStates] = useState<Record<string, ConclusionResearchState>>({});
  const researchInFlight = useRef(new Set<string>());
  const conclusionsLoadAbort = useRef<AbortController | null>(null);
  const detailLoadAbort = useRef<AbortController | null>(null);
  const filtersRef = useRef(filters);
  const cursorRef = useRef<string | null>(null);

  const load = useCallback(async (append = false, silent = false) => {
    if (silent && conclusionsLoadAbort.current) return;
    conclusionsLoadAbort.current?.abort();
    const controller = new AbortController();
    conclusionsLoadAbort.current = controller;
    const params = new URLSearchParams({ ...filtersRef.current, limit: "20" });
    if (append && cursorRef.current) params.set("cursor", cursorRef.current);
    if (append) setLoadingMore(true); else if (!silent) setLoading(true);
    try {
      const response = await fetch(`${apiBase}/api/v1/research-conclusions?${params}`, { signal: controller.signal });
      if (!response.ok) throw new Error("结论请求失败");
      const payload = await response.json() as { items: ResearchConclusionItem[]; next_cursor: string | null };
      setItems((current) => append ? [...current, ...payload.items] : payload.items);
      setResearchStates((current) => {
        const next = { ...current };
        for (const item of payload.items) {
          if (item.kind !== "event") continue;
          const state = eventRefreshResearchState(item.refresh);
          if (state) next[item.id] = state;
          else if (next[item.id]?.status === "queued") delete next[item.id];
        }
        return next;
      });
      cursorRef.current = payload.next_cursor;
      setCursor(payload.next_cursor); setError("");
    } catch (reason) {
      if (reason instanceof Error && reason.name === "AbortError") return;
      setError(reason instanceof Error ? reason.message : "结论请求失败");
    } finally {
      if (conclusionsLoadAbort.current === controller) {
        conclusionsLoadAbort.current = null;
        if (append) setLoadingMore(false); else if (!silent) setLoading(false);
      }
    }
  }, [apiBase]);
  async function loadFailures() {
    setFailuresLoading(true);
    try {
      const queueRequest = fetch(`${apiBase}/api/v1/model-queue-overview?limit=1`)
        .then(async (response) => response.ok
          ? await response.json() as { queues: ModelQueueOverviewItem[] }
          : null)
        .catch(() => null);
      const [response, overview] = await Promise.all([
        fetch(`${apiBase}/api/v1/failed-research-runs?limit=50`),
        queueRequest,
      ]);
      if (!response.ok) throw new Error("失败研究记录请求失败");
      setFailedItems(await response.json() as FailedResearch[]);
      setResearchInstances(overview ? availableResearchInstances(overview.queues) : []);
    } catch (reason) {
      setRetryMessage(reason instanceof Error ? reason.message : "失败研究记录请求失败");
      setRetryMessageError(true);
    } finally {
      setFailuresLoading(false);
    }
  }
  useEffect(() => {
    void load();
    void loadFailures();
    const unsubscribe = subscribeLiveRefresh(
      () => void load(false, true),
      researchViewsRefreshIntervalMs,
    );
    return () => {
      unsubscribe();
      conclusionsLoadAbort.current?.abort();
      detailLoadAbort.current?.abort();
    };
  }, [load]); // eslint-disable-line react-hooks/exhaustive-deps

  async function retry(item: FailedResearch, instanceId?: string) {
    setRetryingId(item.id); setRetryMessage(""); setRetryMessageError(false);
    try {
      const response = await fetch(`${apiBase}${failedResearchRetryPath(item, instanceId)}`, { method: "POST" });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "重新执行失败");
      setRetryMessage(`${instanceId ? `已送入 ${instanceId}` : "已重新排队"}：${item.asset?.symbol || item.event?.headline || item.id}`);
      setRetryMessageError(false);
      await loadFailures();
    } catch (reason) {
      setRetryMessage(reason instanceof Error ? reason.message : "重新执行失败");
      setRetryMessageError(true);
    } finally { setRetryingId(""); }
  }

  async function retryAll() {
    setRetryingAll(true); setRetryMessage(""); setRetryMessageError(false);
    try {
      const payload = await retryAllFailedResearch(apiBase);
      setFailedItems((current) => failedResearchAfterBulkRetry(current, payload));
      setRetryMessage(failedResearchBulkRetryMessage(payload));
      setRetryMessageError(payload.failed > 0);
      await loadFailures();
    } catch (reason) {
      setRetryMessage(reason instanceof Error ? reason.message : "全部重试失败");
      setRetryMessageError(true);
    } finally { setRetryingAll(false); }
  }

  async function open(item: ResearchConclusionItem) {
    if (detailLoadAbort.current) return;
    const controller = new AbortController();
    detailLoadAbort.current = controller;
    setDetailLoadingId(item.id);
    setDetailLoadingTitle(item.title);
    setDetailError("");
    const path = item.kind === "event"
      ? `/api/v1/event-conclusions/${item.id}`
      : `/api/v1/conclusions/${item.id}`;
    try {
      const response = await fetch(`${apiBase}${path}`, { signal: controller.signal });
      if (!response.ok) throw new Error("研究报告加载失败");
      if (item.kind === "event") setSelectedEvent(await response.json() as EventConclusionDetail);
      else setSelectedAsset(await response.json() as ConclusionDetail);
    } catch (reason) {
      if (!(reason instanceof Error && reason.name === "AbortError")) {
        setDetailError(reason instanceof Error ? reason.message : "研究报告加载失败");
      }
    } finally {
      if (detailLoadAbort.current === controller) {
        detailLoadAbort.current = null;
        setDetailLoadingId("");
      }
    }
  }

  function closeDetailLoading() {
    detailLoadAbort.current?.abort();
    detailLoadAbort.current = null;
    setDetailLoadingId("");
  }

  async function researchAgain(item: Recommendation) {
    const assetId = recommendationAssetKey(item);
    if (researchInFlight.current.has(assetId) || researchStates[assetId]?.status === "queued") return;
    researchInFlight.current.add(assetId);
    setResearchStates((current) => ({ ...current, [assetId]: { status: "pending" } }));
    try {
      await researchConclusion(apiBase, item.id);
      setResearchStates((current) => ({ ...current, [assetId]: { status: "queued" } }));
    } catch (reason) {
      setResearchStates((current) => ({
        ...current,
        [assetId]: { status: "error", error: reason instanceof Error ? reason.message : "重新调研失败" },
      }));
    } finally {
      researchInFlight.current.delete(assetId);
    }
  }

  async function researchEventAgain(item: ResearchConclusionItem) {
    const runId = item.id;
    if (researchInFlight.current.has(runId) || researchStates[runId]?.status === "queued") return;
    researchInFlight.current.add(runId);
    setResearchStates((current) => ({ ...current, [runId]: { status: "pending" } }));
    try {
      await researchEventConclusion(apiBase, runId);
      setResearchStates((current) => ({ ...current, [runId]: { status: "queued" } }));
      await load(false, true);
    } catch (reason) {
      setResearchStates((current) => ({
        ...current,
        [runId]: { status: "error", error: reason instanceof Error ? reason.message : "完整重新研究失败" },
      }));
    } finally {
      researchInFlight.current.delete(runId);
    }
  }
  return (
    <section className="app-page conclusions-page">
      <PageHeading eyebrow="RESEARCH OUTCOMES" title="研究结论" copy="新研究按事件类型使用 30、90 或 180 个自然日评级周期；新闻可信度与评级置信度独立计算。" />
      <form className="page-filters" onSubmit={(e) => { e.preventDefault(); filtersRef.current = filters; cursorRef.current = null; setCursor(null); void load(); }}>
        <select aria-label="结论类型" value={filters.kind} onChange={(e) => setFilters({ ...filters, kind: e.target.value })}><option value="all">全部结论</option><option value="event">事件研报</option><option value="asset">具体标的</option></select>
        <input aria-label="搜索结论" placeholder="事件、标的、代码或核心观点" value={filters.q} onChange={(e) => setFilters({ ...filters, q: e.target.value })} />
        <select aria-label="市场" value={filters.market} onChange={(e) => setFilters({ ...filters, market: e.target.value })}><option value="">全部市场</option><option value="US">美股</option><option value="CN">A股</option><option value="HK">港股</option><option value="CRYPTO">加密</option></select>
        <select aria-label="评级" value={filters.rating} onChange={(e) => setFilters({ ...filters, rating: e.target.value })}><option value="">全部评级</option>{Object.entries(ratingLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select>
        <button disabled={loading}>{loading ? "筛选中…" : "筛选"}</button>
      </form>
      <div className="conclusions-split" data-layout="65-35">
        <section className="research-results-panel successful-research-panel">
          <div className="research-results-heading"><div><p className="eyebrow">SUCCESSFUL RESEARCH</p><h3>成功研究</h3></div></div>
          <div className="research-results-scroll successful-research-scroll">
            {error && <div className="page-error">{error}</div>}
            <div className="conclusion-list">
              {items.map((item) => item.kind === "asset" && item.recommendation
                ? <ConclusionCard
                  key={`${item.kind}-${item.id}`}
                  item={item.recommendation}
                  researchState={researchStates[recommendationAssetKey(item.recommendation)]}
                  onOpen={() => void open(item)}
                  onResearch={() => void researchAgain(item.recommendation as Recommendation)}
                />
                : <EventConclusionCard
                  key={`${item.kind}-${item.id}`}
                  item={item}
                  researchState={researchStates[item.id] ?? eventRefreshResearchState(item.refresh)}
                  onOpen={() => void open(item)}
                  onResearch={() => void researchEventAgain(item)}
                />)}
              {!items.length && !error && (loading
                ? <div className="page-message">正在加载研究结论…</div>
                : <div className="page-empty">当前筛选范围内没有事件或标的结论。</div>)}
            </div>
            {cursor && <button className="load-more" type="button" disabled={loadingMore} onClick={() => void load(true)}>{loadingMore ? "正在加载…" : "加载更多"}</button>}
          </div>
        </section>
        <section className="research-results-panel failed-research-panel">
          <div className="research-results-heading failed-research-heading"><div><p className="eyebrow">RETRY QUEUE</p><h3>历史失败研究</h3></div><div className="failed-research-actions"><button type="button" disabled={retryingAll || !!retryingId || failuresLoading || !failedItems.length} onClick={retryAll}>{retryingAll ? "全部重试中…" : "全部重试"}</button><button type="button" disabled={retryingAll || failuresLoading} onClick={loadFailures}>刷新</button></div></div>
          <p className="failed-research-copy">重新执行会创建新任务，保留原失败记录，并使用当前数据源和模型配置。</p>
          {retryMessage && <div className={retryMessageError ? "page-error" : "page-message"}>{retryMessage}</div>}
          <div className="research-results-scroll failed-research-list">
            {failedItems.map((item) => {
              const retryActive = ["queued", "running", "verifying"].includes(item.latest_retry?.status || "");
              return <article key={`${item.kind}-${item.id}`} className="failed-research-item">
                <div><span>{item.kind === "asset" ? "标的研究" : "事件研报"} · {new Date(item.updated_at).toLocaleString("zh-CN")}</span><strong>{item.asset ? `${item.asset.symbol} · ${item.asset.name}` : item.event?.headline || item.id}</strong><p>{item.error || "未记录错误详情"}</p>{item.latest_retry && <small>最近重跑：{item.latest_retry.status} · {new Date(item.latest_retry.updated_at).toLocaleString("zh-CN")}</small>}</div>
                <div className="failed-research-controls">
                  <button type="button" disabled={retryingAll || retryingId === item.id || retryActive} onClick={() => retry(item)}>{retryingId === item.id ? "正在排队…" : retryActive ? "重跑中" : "重新执行"}</button>
                  <div className="failed-research-queues" aria-label={`可用研究队列 ${researchInstances.length} 个`}>
                    <span>可用队列 {researchInstances.length}</span>
                    {researchInstances.map((instance) => <button
                      type="button"
                      key={instance.id}
                      title={`送入 ${instance.id}；当前排队 ${instance.counts.queued} 条`}
                      disabled={retryingAll || retryingId === item.id || retryActive}
                      onClick={() => retry(item, instance.id)}
                    >{instance.id}</button>)}
                  </div>
                </div>
              </article>;
            })}
            {!failedItems.length && !retryMessage && (failuresLoading
              ? <div className="page-message">正在加载历史失败研究…</div>
              : <div className="page-empty">当前没有失败研究。</div>)}
          </div>
        </section>
      </div>
      {detailError && <div className="page-error conclusion-detail-error">{detailError}</div>}
      {detailLoadingId && <ResearchDetailLoadingModal title={detailLoadingTitle || "研究报告"} onClose={closeDetailLoading} />}
      {selectedAsset && <ConclusionDetailModal detail={selectedAsset} onClose={() => setSelectedAsset(null)} />}
      {selectedEvent && <EventConclusionDetailModal detail={selectedEvent} onClose={() => setSelectedEvent(null)} />}
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
  rescan_allowed: boolean;
};

export function sourceFilterRescanPath(logId: string) {
  return `/api/v1/source-filter/logs/${encodeURIComponent(logId)}/rescan`;
}

export function SourceFilterAuditRow({
  item,
  busy,
  onRescan,
}: {
  item: SourceFilterLog;
  busy: boolean;
  onRescan: (item: SourceFilterLog) => void;
}) {
  return <article>
    <div><span>{item.source} · {new Date(item.last_filtered_at).toLocaleString("zh-CN")}</span><h4><a href={item.url} target="_blank" rel="noreferrer">{item.title}</a></h4></div>
    <div className="filter-log-result">
      <div><strong>过滤原因：{item.matched_keyword}</strong><small>累计 {item.hit_count} 次</small></div>
      {item.rescan_allowed && <button type="button" disabled={busy} onClick={() => onRescan(item)}>{busy ? "重新扫描中…" : "重新扫描"}</button>}
    </div>
  </article>;
}

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
  const [rescanningIds, setRescanningIds] = useState<Set<string>>(new Set());

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
    if (!window.confirm("恢复默认过滤规则？白名单将清空，过滤保持启用，因此所有新新闻都会被忽略；黑名单恢复为“天气”。")) return;
    const response = await fetch(`${apiBase}/api/v1/source-filter`, { method: "DELETE" });
    if (!response.ok) { setMessage("恢复默认失败。"); return; }
    applyConfig(await response.json() as SourceFilterConfig);
    setMessage("已恢复默认过滤规则。");
  }
  async function rescan(item: SourceFilterLog) {
    setRescanningIds((current) => new Set(current).add(item.id));
    try {
      const response = await fetch(`${apiBase}${sourceFilterRescanPath(item.id)}`, {
        method: "POST",
      });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || "重新扫描失败");
      setLogs((current) => current.filter((value) => value.id !== item.id));
      setConfig((current) => ({
        ...current,
        retained_log_count: Math.max(0, current.retained_log_count - 1),
      }));
      setMessage(`已将“${item.title}”重新送入 3B 抽取、7B 股票映射和后续研究队列。`);
    } catch (reason) {
      setMessage(reason instanceof Error ? reason.message : "重新扫描失败");
    } finally {
      setRescanningIds((current) => {
        const next = new Set(current);
        next.delete(item.id);
        return next;
      });
    }
  }
  const whitelistCount = parseFilterKeywords(whitelist).length;
  const blacklistCount = parseFilterKeywords(blacklist).length;
  return <section className="app-page source-filter-page">
    <PageHeading eyebrow="PRE-RESEARCH GATE" title="数据源过滤" copy="在新闻进入事件提取和研究队列前检查标题，减少与投资无关的模型调用。" />
    <div className="filter-metrics">
      <div><span>过滤状态</span><strong className={enabled ? "enabled" : "disabled"}>{enabled ? "已启用" : "已关闭"}</strong></div>
      <div><span>白名单</span><strong>{whitelistCount}</strong><small>命中才准入</small></div>
      <div><span>黑名单</span><strong>{blacklistCount}</strong><small>命中即否决</small></div>
      <div><span>保留记录</span><strong>{config.retained_log_count}</strong><small>最近 30 天</small></div>
    </div>
    <form className="source-filter-form" onSubmit={save}>
      <label className="filter-master-switch"><input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} /><span><strong>启用新闻标题过滤</strong><small>关闭后所有新标题均进入原有流程。</small></span></label>
      <div className="keyword-panels">
        <label><span><strong>白名单关键字</strong><small>{whitelistCount} / 200 · 命中才允许进入 3B</small></span><textarea aria-label="白名单关键字" value={whitelist} placeholder="例如：苹果供应链" onChange={(e) => setWhitelist(e.target.value)} /></label>
        <label><span><strong>黑名单关键字</strong><small>{blacklistCount} / 200 · 命中即禁止进入 3B</small></span><textarea aria-label="黑名单关键字" value={blacklist} placeholder="例如：天气" onChange={(e) => setBlacklist(e.target.value)} /></label>
      </div>
      <p className="filter-note">每行或逗号分隔一个关键字；忽略英文大小写，仅匹配新闻扫描标题。启用后必须命中白名单且不能命中黑名单，黑名单拥有否决权。</p>
      {!loading && enabled && whitelistCount === 0 && <div className="page-error">白名单为空：所有新新闻都会被忽略，不会进入 3B。</div>}
      <div className="card-actions"><button type="submit">保存规则</button><button type="button" onClick={load}>刷新</button><button type="button" className="danger" onClick={reset}>恢复默认</button></div>
    </form>
    {message && <div className={message.includes("失败") ? "page-error" : "page-message"}>{message}</div>}
    <section className="filter-audit" aria-labelledby="filter-audit-title">
      <div className="filter-audit-heading"><div><p className="eyebrow">FILTER AUDIT</p><h3 id="filter-audit-title">最近过滤记录</h3></div><span>{loading ? "读取中…" : `${logs.length} 条`}</span></div>
      <div className="filter-log-list">{logs.map((item) => <SourceFilterAuditRow key={item.id} item={item} busy={rescanningIds.has(item.id)} onRescan={rescan} />)}</div>
      {!loading && logs.length === 0 && <div className="page-empty">还没有新闻标题被过滤。</div>}
    </section>
  </section>;
}

export type McpSource = {
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

export function mcpSourceCredentialLabel(source: Pick<McpSource, "auth_type" | "secret_configured">): string {
  if (source.auth_type === "none") return "无需凭据";
  return source.secret_configured ? "凭据已配置" : "待录入凭据";
}

export function mcpSourceSetupLabel(source: Pick<McpSource, "auth_type" | "secret_configured" | "discovered_tools" | "last_status">): string {
  if (source.auth_type !== "none" && !source.secret_configured) return "配置凭据后发现工具";
  if (source.discovered_tools.length === 0) return "待工具发现";
  if (source.last_status === "healthy") return "连接已验证";
  if (source.last_status === "failed") return "连接异常";
  return "工具已发现，待连接测试";
}

export function canProbeMcpSource(source: Pick<McpSource, "auth_type" | "secret_configured">): boolean {
  return source.auth_type === "none" || source.secret_configured;
}

export function canEnableMcpSource(source: Pick<McpSource, "enabled" | "auth_type" | "secret_configured" | "discovered_tools" | "last_status">): boolean {
  if (source.enabled || source.auth_type === "none") return true;
  return source.secret_configured && source.discovered_tools.length > 0 && source.last_status === "healthy";
}

export function McpSourceCard({ item, onToggle, onAction, onEdit, onRemove }: {
  item: McpSource;
  onToggle: (source: McpSource) => void;
  onAction: (id: string, kind: "test" | "discover") => void;
  onEdit: (source: McpSource) => void;
  onRemove: (source: McpSource) => void;
}) {
  const canProbe = canProbeMcpSource(item);
  const canEnable = canEnableMcpSource(item);
  return <article className="source-card">
    <div className="source-card-main"><div><span className={`health-dot ${item.last_status}`} /> <strong>{item.name}</strong>{item.managed && <small>内置</small>}<p>{item.description || item.url}</p><code>{item.url}</code></div><div><span>优先级 {item.priority}</span><span>{item.discovered_tools.length} 个工具</span><span>{mcpSourceCredentialLabel(item)}</span><span>{mcpSourceSetupLabel(item)}</span></div></div>
    {item.last_error && <p className="page-error">{item.last_error}</p>}
    <div className="card-actions"><button type="button" disabled={!canEnable} onClick={() => onToggle(item)}>{item.enabled ? "关闭" : "启用"}</button><button type="button" disabled={!canProbe} onClick={() => onAction(item.id, "test")}>连接测试</button><button type="button" disabled={!canProbe} onClick={() => onAction(item.id, "discover")}>工具发现</button><button type="button" onClick={() => onEdit(item)}>编辑</button>{!item.managed && <button type="button" className="danger" onClick={() => onRemove(item)}>删除</button>}</div>
    {item.discovered_tools.length > 0 && <details><summary>已发现工具与 Schema</summary>{item.discovered_tools.map((tool) => <pre key={tool.name}>{tool.name}\n{tool.description}\n{JSON.stringify(tool.input_schema, null, 2)}</pre>)}</details>}
  </article>;
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
    const succeeded = response.ok && body.source?.last_status !== "failed";
    if (owner) setGroupMessage(owner.id, succeeded ? `${kind === "test" ? "连接测试" : "工具发现"}完成。` : body.source?.last_error || body.detail || "操作失败");
    await load();
  }
  async function toggle(item: McpSource) {
    const response = await fetch(`${apiBase}/api/v1/admin/mcp-sources/${item.id}/enabled`, { method: "PATCH", headers, body: JSON.stringify({ enabled: !item.enabled }) });
    const body = await response.json();
    setGroupMessage(item.group_id, response.ok ? `来源已${item.enabled ? "关闭" : "启用"}。` : body.detail || "更新来源状态失败。");
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
          {groupMessages[group.id] && <div className={["失败", "异常", "请先", "Error"].some((token) => groupMessages[group.id].includes(token)) ? "page-error" : "page-message"}>{groupMessages[group.id]}</div>}
          <section className="native-config-panel">
            <div className="group-section-heading"><div><span>NATIVE CONFIG</span><h4>内置配置</h4></div><small>{group.config_source === "database" ? "数据库覆盖" : "环境配置"}</small></div>
            <NativeConfigEditor group={group} draft={groupDraftValue} onDraft={(value) => setDrafts((current) => ({ ...current, [group.id]: value }))} />
            {group.id !== "other" && <div className="card-actions"><button type="button" onClick={() => testGroup(group)}>测试配置</button><button type="button" onClick={() => saveGroup(group)}>保存</button><button type="button" className="danger" onClick={() => resetGroup(group)}>恢复环境默认</button></div>}
          </section>
          <section className="group-mcp-panel">
            <div className="group-section-heading"><div><span>STREAMABLE HTTP</span><h4>MCP 来源</h4></div><small>{group.mcp_count} 个来源</small></div>
            <div className="source-list">{group.mcp_sources.map((item) => <McpSourceCard key={item.id} item={item} onToggle={toggle} onAction={action} onEdit={(source) => { setEditing(source.id); setDraft(sourceDraft(source)); }} onRemove={remove} />)}{group.mcp_sources.length === 0 && <div className="group-empty">此组尚无 MCP 来源，可使用“新增 MCP 来源”添加。</div>}</div>
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

type UniverseAsset = {
  asset_id: string;
  market: string;
  symbol: string;
  name: string;
  aliases: string[];
  sector_id: string;
  industry_id: string;
  raw_sector: string;
  raw_industry: string;
  instrument_type: string;
  market_cap: number | null;
  market_cap_rank: number | null;
  association_tier: "standard" | "exact_only" | "manual_only";
  association_reason: string;
  active: boolean;
  last_synced_at: string | null;
};

type IndustryItem = {
  industry_id: string;
  parent_id: string | null;
  level: number;
  name_zh: string;
  name_en: string;
  asset_count: number;
};

type UniverseMarketStatus = {
  market: string;
  status: string;
  asset_count: number;
  industry_count: number;
  classified_count: number;
  unclassified_count: number;
  classification_rate: number;
  last_error: string | null;
  completed_at: string | null;
  association_tier_counts: Record<string, number>;
};

const universeMarketLabels: Record<string, string> = {
  CN: "A 股",
  HK: "港股",
  US: "美股",
  CRYPTO: "加密资产",
};

const associationReasonLabels: Record<string, string> = {
  provider_verified: "供应商验证",
  coingecko_market_cap_top_500: "CoinGecko 市值前 500",
  coingecko_long_tail_exact_identity: "长尾币种，仅精确身份",
  stable_or_wrapped_manual_only: "稳定币或封装币，仅手动研究",
  ambiguous_crypto_identity_manual_only: "代码或名称存在歧义，仅手动研究",
  manual_override: "人工覆盖",
};

function universeTime(value: string | null) {
  return value ? new Date(value).toLocaleString("zh-CN") : "尚未同步";
}

export function AssetUniversePage({ apiBase }: { apiBase: string }) {
  const [token, setToken] = useState(readToken);
  const [assets, setAssets] = useState<UniverseAsset[]>([]);
  const [industries, setIndustries] = useState<IndustryItem[]>([]);
  const [statuses, setStatuses] = useState<UniverseMarketStatus[]>([]);
  const [activeCounts, setActiveCounts] = useState<Record<string, number>>({});
  const [query, setQuery] = useState("");
  const [market, setMarket] = useState("");
  const [industryId, setIndustryId] = useState("");
  const [associationTier, setAssociationTier] = useState("");
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [editing, setEditing] = useState<UniverseAsset | null>(null);
  const [aliasDraft, setAliasDraft] = useState("");
  const [industryDraft, setIndustryDraft] = useState("");
  const [activeDraft, setActiveDraft] = useState(true);
  const [tierDraft, setTierDraft] = useState("auto");
  const [researchingId, setResearchingId] = useState("");
  const limit = 100;

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    const params = new URLSearchParams({ offset: String(offset), limit: String(limit) });
    if (query.trim()) params.set("q", query.trim());
    if (market) params.set("market", market);
    if (industryId) params.set("industry_id", industryId);
    if (associationTier) params.set("association_tier", associationTier);
    try {
      const [assetResponse, industryResponse, statusResponse] = await Promise.all([
        fetch(`${apiBase}/api/v1/asset-universe?${params}`),
        fetch(`${apiBase}/api/v1/industries${market ? `?market=${encodeURIComponent(market)}` : ""}`),
        fetch(`${apiBase}/api/v1/asset-universe/status`),
      ]);
      if (!assetResponse.ok || !industryResponse.ok || !statusResponse.ok) {
        throw new Error("资产主数据读取失败");
      }
      const assetPayload = await assetResponse.json() as { items: UniverseAsset[]; total: number };
      const statusPayload = await statusResponse.json() as {
        markets: UniverseMarketStatus[];
        active_counts: Record<string, number>;
      };
      setAssets(assetPayload.items);
      setTotal(assetPayload.total);
      setIndustries(await industryResponse.json() as IndustryItem[]);
      setStatuses(statusPayload.markets);
      setActiveCounts(statusPayload.active_counts || {});
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "资产主数据读取失败");
    } finally {
      setLoading(false);
    }
  }, [apiBase, associationTier, industryId, market, offset, query]);

  useEffect(() => { void load(); }, [load]);

  async function queueAdminAction(path: "refresh" | "backfill", selectedMarket = "") {
    setError("");
    setMessage("");
    try {
      const params = new URLSearchParams();
      if (path === "refresh" && selectedMarket) params.set("market", selectedMarket);
      if (path === "backfill") params.set("days", "30");
      const response = await fetch(`${apiBase}/api/v1/admin/asset-universe/${path}${params.size ? `?${params}` : ""}`, {
        method: "POST",
        headers: { "X-Admin-Token": token },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "任务入队失败");
      setMessage(path === "refresh"
        ? `${selectedMarket ? universeMarketLabels[selectedMarket] : "全市场"}同步已进入队列（${payload.task_id}）。`
        : `最近 ${payload.days} 天的低置信度映射已进入回补队列。`);
      window.setTimeout(() => { void load(); }, 1000);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "任务入队失败");
    }
  }

  function beginEdit(asset: UniverseAsset) {
    setEditing(asset);
    setAliasDraft(asset.aliases.join("\n"));
    setIndustryDraft(asset.industry_id || "");
    setActiveDraft(asset.active);
    setTierDraft("auto");
  }

  async function saveEdit(event: FormEvent) {
    event.preventDefault();
    if (!editing) return;
    setError("");
    const response = await fetch(`${apiBase}/api/v1/admin/assets/${encodeURIComponent(editing.asset_id)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json", "X-Admin-Token": token },
      body: JSON.stringify({
        aliases: aliasDraft.split("\n").map((item) => item.trim()).filter(Boolean),
        industry_id: industryDraft,
        active: activeDraft,
        association_tier: tierDraft,
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      setError(payload.detail || "保存资产修订失败");
      return;
    }
    setEditing(null);
    setMessage(`${payload.symbol} 的主数据修订已保存。`);
    await load();
  }

  async function researchAsset(asset: UniverseAsset) {
    setResearchingId(asset.asset_id);
    setError("");
    try {
      const response = await fetch(`${apiBase}/api/v1/research`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ asset_id: asset.asset_id, background: true }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || "研究任务入队失败");
      setMessage(`${asset.symbol} 研究已进入队列（${payload.run_id}）。完成后将在“标的评级”显示。`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "研究任务入队失败");
    } finally {
      setResearchingId("");
    }
  }

  const statusByMarket = new Map(statuses.map((item) => [item.market, item]));
  const levelTwoIndustries = industries.filter((item) => item.level === 2);
  return (
    <section className="app-page asset-universe-page">
      <PageHeading
        eyebrow="SECURITY & INDUSTRY MASTER"
        title="资产与行业主数据"
        copy="维护 A 股、港股、美股公司证券及 CoinGecko 全量加密资产目录；统一行业用于跨市场比较，并保留上游原始行业供追溯。"
      />
      <div className="universe-status-grid">
        {Object.entries(universeMarketLabels).map(([key, label]) => {
          const status = statusByMarket.get(key);
          return <article className={`universe-status ${status?.status || "pending"}`} key={key}>
            <span>{label}</span>
            <strong>{(activeCounts[key] || status?.asset_count || 0).toLocaleString()}</strong>
            <small>行业 {(status?.classified_count || 0).toLocaleString()} / {(activeCounts[key] || status?.asset_count || 0).toLocaleString()} · {((status?.classification_rate || 0) * 100).toLocaleString("zh-CN", { maximumFractionDigits: 1 })}%</small>
            <small>标准 {(status?.association_tier_counts?.standard || 0).toLocaleString()} · 精确 {(status?.association_tier_counts?.exact_only || 0).toLocaleString()} · 手动 {(status?.association_tier_counts?.manual_only || 0).toLocaleString()}</small>
            <small>{status?.status === "failed" ? `失败：${status.last_error}` : universeTime(status?.completed_at || null)}</small>
            <button type="button" disabled={!token || status?.status === "running"} onClick={() => void queueAdminAction("refresh", key)}>{status?.status === "running" ? "同步中…" : "同步此市场"}</button>
          </article>;
        })}
      </div>
      <AdminUnlock token={token} onToken={setToken} />
      <div className="universe-actions">
        <button type="button" disabled={!token} onClick={() => void queueAdminAction("refresh")}>同步全部市场</button>
        <button type="button" disabled={!token} onClick={() => void queueAdminAction("backfill")}>回补最近 30 天映射</button>
        <span>行业新闻自动关联每市场市值靠前代表股，关系标记为 industry_peer，最多 8 只。</span>
      </div>
      {message && <div className="page-message">{message}</div>}
      {error && <div className="page-error">{error}</div>}
      <form className="universe-filters" onSubmit={(event) => { event.preventDefault(); setOffset(0); void load(); }}>
        <input aria-label="搜索资产" placeholder="代码或公司/资产名称" value={query} onChange={(event) => { setQuery(event.target.value); setOffset(0); }} />
        <select aria-label="市场" value={market} onChange={(event) => { setMarket(event.target.value); setIndustryId(""); setOffset(0); }}>
          <option value="">全部市场</option>
          {Object.entries(universeMarketLabels).map(([key, label]) => <option key={key} value={key}>{label}</option>)}
        </select>
        <select aria-label="行业" value={industryId} onChange={(event) => { setIndustryId(event.target.value); setOffset(0); }}>
          <option value="">全部行业</option>
          {levelTwoIndustries.map((item) => <option key={item.industry_id} value={item.industry_id}>{item.name_zh}（{item.asset_count}）</option>)}
        </select>
        <select aria-label="关联层级" value={associationTier} onChange={(event) => { setAssociationTier(event.target.value); setOffset(0); }}><option value="">全部关联层级</option><option value="standard">标准自动关联</option><option value="exact_only">仅精确命中</option><option value="manual_only">仅手动研究</option></select>
        <button type="submit">查询</button>
        <button type="button" onClick={() => void load()}>刷新状态</button>
      </form>
      <div className="universe-table-wrap">
        <table className="universe-table">
          <thead><tr><th>市场 / 代码</th><th>名称与别名</th><th>统一行业 / 原始行业</th><th>关联层级</th><th>类型</th><th>同步时间</th><th>操作</th></tr></thead>
          <tbody>
            {assets.map((asset) => <tr key={asset.asset_id} className={asset.active ? "" : "inactive"}>
              <td><small>{universeMarketLabels[asset.market] || asset.market}</small><strong>{asset.symbol}</strong></td>
              <td><strong>{asset.name}</strong><small>{asset.aliases.slice(0, 3).join(" · ") || "无别名"}</small></td>
              <td><strong>{levelTwoIndustries.find((item) => item.industry_id === asset.industry_id)?.name_zh || "待归类"}</strong><small>{asset.raw_industry ? `原始：${asset.raw_industry}` : "未取得原始行业"}</small></td>
              <td><strong>{asset.association_tier === "standard" ? "标准" : asset.association_tier === "exact_only" ? "仅精确" : "仅手动"}</strong><small>{associationReasonLabels[asset.association_reason] || asset.association_reason}</small></td>
              <td>{asset.instrument_type || "—"}</td>
              <td>{universeTime(asset.last_synced_at)}</td>
              <td><div className="universe-row-actions"><button type="button" disabled={researchingId === asset.asset_id || !asset.active} onClick={() => void researchAsset(asset)}>{researchingId === asset.asset_id ? "入队中…" : "研究"}</button><button type="button" disabled={!token} onClick={() => beginEdit(asset)}>编辑</button></div></td>
            </tr>)}
          </tbody>
        </table>
        {!loading && !assets.length && <div className="page-empty">没有符合条件的资产。</div>}
        {loading && <div className="page-empty">正在读取资产主数据…</div>}
      </div>
      <div className="universe-pagination">
        <span>共 {total.toLocaleString()} 条，当前 {offset + 1}–{Math.min(offset + limit, total)}</span>
        <button type="button" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - limit))}>上一页</button>
        <button type="button" disabled={offset + limit >= total} onClick={() => setOffset(offset + limit)}>下一页</button>
      </div>
      {editing && <div className="modal-backdrop" role="presentation" onMouseDown={() => setEditing(null)}>
        <form className="universe-editor" onSubmit={saveEdit} onMouseDown={(event) => event.stopPropagation()}>
          <div><span>主数据修订</span><button type="button" onClick={() => setEditing(null)}>关闭</button></div>
          <h3>{editing.symbol} · {editing.name}</h3>
          <label>别名（每行一个）<textarea value={aliasDraft} onChange={(event) => setAliasDraft(event.target.value)} /></label>
          <label>统一行业<select value={industryDraft} onChange={(event) => setIndustryDraft(event.target.value)}><option value="">待归类</option>{levelTwoIndustries.map((item) => <option key={item.industry_id} value={item.industry_id}>{item.name_zh} / {item.name_en}</option>)}</select></label>
          <label className="inline-check"><input type="checkbox" checked={activeDraft} onChange={(event) => setActiveDraft(event.target.checked)} />参与映射</label>
          <label>关联层级<select value={tierDraft} onChange={(event) => setTierDraft(event.target.value)}><option value="auto">跟随供应商自动分层</option><option value="standard">标准自动关联</option><option value="exact_only">仅精确命中</option><option value="manual_only">仅手动研究</option></select></label>
          <button type="submit">保存修订</button>
        </form>
      </div>}
    </section>
  );
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
  if (route === "targets") return <ChangedTargetsPage apiBase={apiBase} />;
  if (route === "sources") return <SourcesPage apiBase={apiBase} />;
  if (route === "asset-universe") return <AssetUniversePage apiBase={apiBase} />;
  if (route === "news") return <NewsPage apiBase={apiBase} />;
  if (route === "queue") return <QueuePage apiBase={apiBase} />;
  if (route === "analysis") return <AnalysisPage logs={analysisLogs} />;
  if (route === "search") return <SearchPage apiBase={apiBase} />;
  return <WeknoraPage apiBase={apiBase} />;
}
