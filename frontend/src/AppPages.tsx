import { FormEvent, useCallback, useEffect, useRef, useState } from "react";

import AnalysisPage, { type AnalysisLog } from "./AnalysisPage";
import {
	analystEvidenceBody,
	analystEvidenceTemplate,
	analystEvidenceTypeLabels,
	analystEvidenceTypes,
	fundamentalScheduleBody,
	fundamentalWorkflowBody,
	type AnalystEvidencePreview,
	type AnalystEvidenceType,
	type SchedulePreview,
	type WorkflowPreview,
} from "./FundamentalWorkflow";
import ModelLogsPage from "./ModelLogs";
import PhaseTwoReadinessPage from "./PhaseTwoReadiness";
import { TargetTrendSummary, type TargetTrend } from "./TargetTrendSummary";

export type AppRoute = "home" | "source-filter" | "sources" | "asset-universe" | "news" | "queue" | "analysis" | "conclusions" | "targets" | "fundamental" | "readiness" | "model-logs" | "policy" | "search" | "weknora";

export const navigationGroups: Record<"left" | "right", Array<{ route: AppRoute; label: string }>> = {
  left: [
    { route: "home", label: "首页" },
    { route: "source-filter", label: "新闻准入与分流" },
    { route: "sources", label: "数据源" },
    { route: "news", label: "新闻" },
    { route: "queue", label: "队列" },
    { route: "analysis", label: "分析链路" },
    { route: "conclusions", label: "结论" },
    { route: "targets", label: "标的" },
    { route: "fundamental", label: "基本面与预测" },
		{ route: "readiness", label: "二期就绪度" },
  ],
  right: [
    { route: "model-logs", label: "模型日志" },
    { route: "policy", label: "策略影子期" },
    { route: "asset-universe", label: "资产主数据" },
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
  run_id?: string | null;
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
  research_profile?: "fast" | "deep" | string;
  route_reason?: string;
  waiting_for_deep_slot?: boolean;
  escalated_to_deep?: boolean;
};

type ModelQueueCounts = {
  queued: number;
  running: number;
  retrying: number;
  verifying: number;
  waiting_for_model: number;
  completed: number;
  failed: number;
  filtered?: number;
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
  deep_capacity?: number;
  observable: boolean;
  instances: ModelQueueInstanceSummary[];
  counts: ModelQueueCounts;
  metrics: ModelQueueMetrics;
  total_tasks: number;
  truncated: boolean;
  tasks: ModelQueueTask[];
  error: string | null;
  news_age_filter?: { enabled: boolean; max_age_hours: number };
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

export function modelQueueAggregateInstance(
  queue: ModelQueueOverviewItem,
): ModelQueueInstanceItem {
  const instances = modelQueueInstances(queue);
  return {
    id: "全部实例",
    healthy: instances.every((instance) => instance.healthy),
    model_available: instances.every((instance) => instance.model_available),
    state: queue.state,
    capacity: queue.capacity,
    available: queue.available,
    observable: queue.observable,
    counts: queue.counts,
    metrics: queue.metrics,
    total_tasks: queue.total_tasks,
    truncated: queue.truncated,
    tasks: queue.tasks,
  };
}

export function modelQueuePanelColumns(
  queues: ModelQueueOverviewItem[],
): [ModelQueuePanelItem[], ModelQueuePanelItem[]] {
  const buildColumn = (queueIds: ModelQueueOverviewItem["id"][]) => queueIds.flatMap(
    (queueId) => queues
      .filter((queue) => queue.id === queueId)
      .flatMap((queue) => (queue.id === "research"
        ? [{ queue, instance: modelQueueAggregateInstance(queue) }]
        : modelQueueInstances(queue).map((instance) => ({ queue, instance })))),
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
  filtered: "新闻已过滤",
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
      {queue.id === "research" && <div className="model-task-count">{task.waiting_for_deep_slot ? "等待深度槽" : task.research_profile === "deep" ? "深度" : "快速"}{task.escalated_to_deep ? " · 快速失败后升级" : ""}</div>}
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
  filterRecentResearch = false,
  onFilterRecentResearchChange,
  onResearchNewsAgeFilterChange,
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
  onResearchNewsAgeFilterChange?: (value: boolean) => void;
  onCancelTask?: (task: ModelQueueTask) => void;
  onRetryTask?: (task: ModelQueueTask) => void;
  onRetryAll?: () => void;
  onClear?: () => void;
  cancellingTaskId?: string | null;
  retryingTaskId?: string | null;
  retryingAll?: boolean;
  clearing?: boolean;
}) {
  const activeInstance = instance ?? (queue.id === "research"
    ? modelQueueAggregateInstance(queue)
    : modelQueueInstances(queue)[0]);
  const researchAggregate = queue.id === "research" && activeInstance.id === "全部实例";
  const researchInstances = researchAggregate ? modelQueueInstances(queue) : [];
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
        <small>{queue.binding} · {researchAggregate
          ? `${researchInstances.filter((item) => item.healthy && item.model_available).length}/${researchInstances.length} 实例可用`
          : (ready ? "实例可用" : (activeInstance.healthy ? "模型缺失" : "实例离线"))}</small>
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
        {queue.id === "research" && <label className="model-queue-filter-toggle" title="开启后自动研究会过滤新闻发布时间超过 24 小时的任务；手动重试不受影响">
          <input
            type="checkbox"
            checked={queue.news_age_filter?.enabled ?? true}
            onChange={(event) => onResearchNewsAgeFilterChange?.(event.target.checked)}
          />
          <span>过滤 24h 新闻</span>
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
      <span>{queue.id === "research" ? "近24h完成/失败" : "完成/失败"}<strong>{activeInstance.counts.completed}/{activeInstance.counts.failed}</strong></span>
      {queue.id === "research" && <span>已过滤<strong>{activeInstance.counts.filtered ?? 0}</strong></span>}
      <span title={`样本 ${activeInstance.metrics.queue_duration_sample_count}`}>平均排队<strong>{formatQueueDuration(activeInstance.metrics.average_queue_duration_ms)}</strong></span>
      {queue.id === "assist"
        ? <span title="过去 4 小时完成任务的实际吞吐">近4h吞吐<strong>{activeInstance.metrics.throughput_per_hour === null ? "—" : `${activeInstance.metrics.throughput_per_hour.toFixed(1)}/时`}</strong></span>
        : <span title={`近 4 小时终态样本 ${activeInstance.metrics.execution_duration_sample_count}`}>近4h平均执行<strong>{formatQueueDuration(activeInstance.metrics.average_execution_duration_ms)}</strong></span>}
    </div>
    <div className={`model-queue-runtime ${queue.id === "research" ? "research" : "standard"}`}>
      <span>模型等待<strong>{activeInstance.counts.waiting_for_model}</strong></span>
      <span>槽位<strong>{activeInstance.available}/{activeInstance.capacity}</strong></span>
      <span>实例并发<strong>{researchAggregate ? `${queue.instance_count} × ${queue.per_instance_concurrency}` : activeInstance.capacity} 路</strong></span>
      {queue.id === "research" && <span>总/深度并发<strong>{queue.capacity} / {queue.deep_capacity ?? 1}</strong></span>}
      <span>CPU<strong>{queue.threads} 线程</strong></span>
      <span>最长等待<strong>{formatQueueDuration(activeInstance.metrics.longest_wait_ms)}</strong></span>
      <span>预计清空<strong>{formatQueueDuration(activeInstance.metrics.estimated_clear_ms)}</strong></span>
      {queue.id === "research" && <>
        <span>P50<strong>{formatQueueDuration(activeInstance.metrics.execution_p50_ms)}</strong></span>
        <span>P90<strong>{formatQueueDuration(activeInstance.metrics.execution_p90_ms)}</strong></span>
        <span>近24h吞吐<strong>{activeInstance.metrics.throughput_per_hour === null ? "—" : `${activeInstance.metrics.throughput_per_hour.toFixed(1)}/时`}</strong></span>
      </>}
    </div>
    {researchAggregate && <div className="research-instance-status" aria-label="研究实例实时槽位">
      {researchInstances.map((item) => {
        const instanceReady = item.healthy && item.model_available;
        return <span className={instanceReady ? "healthy" : "unavailable"} key={item.id}>
          <i />{item.id} · 运行 {item.counts.running} · 槽位 {item.available}/{item.capacity}
        </span>;
      })}
    </div>}
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
  task: Pick<ModelQueueTask, "task_id" | "run_id" | "kind" | "entity_id" | "instance_id">,
  filterRecentResearch: boolean,
): RequestInit {
  return {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      task_id: task.task_id,
      run_id: task.run_id,
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
  const [filterRecentResearch, setFilterRecentResearch] = useState(false);
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

  const updateResearchNewsAgeFilter = useCallback(async (enabled: boolean) => {
    setActionMessage("");
    setError("");
    try {
      const response = await fetch(`${apiBase}/api/v1/model-queues/research/news-age-filter`, {
        method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ enabled }),
      });
      if (!response.ok) throw new Error(`更新新闻过滤失败（HTTP ${response.status}）`);
      const result = await response.json() as { discarded: number };
      setActionMessage(enabled
        ? `已开启过滤 24h 新闻，已过滤 ${result.discarded} 个未开始自动研究任务。`
        : "已关闭过滤 24h 新闻；后续自动研究不再按新闻发布时间过滤。");
      await loadQueues();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "更新新闻过滤失败");
    }
  }, [apiBase, loadQueues]);

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
              onResearchNewsAgeFilterChange={(enabled) => void updateResearchNewsAgeFilter(enabled)}
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

type ResearchAssessment = {
  score: number;
  reason: string;
  evidence_ids: string[];
  action_ids: string[];
  missing_information: string[];
  cap_reasons?: string[];
};

type TargetEvaluation = {
  object_relevance: ResearchAssessment;
  evidence_sufficiency: ResearchAssessment;
  transmission_certainty: ResearchAssessment;
  impact_support: ResearchAssessment;
  timing_persistence: ResearchAssessment;
};

type ResearchClaim = {
  claim_type: "fact" | "inference";
  text: string;
  evidence_ids: string[];
  action_ids: string[];
  missing_information: string[];
};

type TransmissionStep = {
  source_node: string;
  mechanism: string;
  target_node: string;
  basis_type: "fact" | "inference";
  evidence_ids: string[];
  action_ids: string[];
  missing_information: string[];
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
  news_credibility_score?: number;
  report_confidence?: number;
  report_confidence_score?: number;
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
  claim_status?: ClaimStatus;
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
  prompt_version?: string;
  target_evaluation_version?: string;
  report_confidence_version?: string;
  target_evaluation_score?: number;
  target_evaluation?: TargetEvaluation;
  model_target_evaluation?: TargetEvaluation;
  impact?: {
    conclusion_status?: string;
    impact_channel?: string;
    claims?: ResearchClaim[];
    transmission_steps?: TransmissionStep[];
    target_evaluation_score?: number;
    target_evaluation?: TargetEvaluation;
    model_target_evaluation?: TargetEvaluation;
    applied_caps?: string[];
    conditional_impact?: boolean;
    conditional_information?: string[];
  };
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
  bull_probability: number | null;
  base_probability: number | null;
  bear_probability: number | null;
  event_signal?: { status: string; direction_score: number; rating: string; signal_available_at?: string };
  event_signal_state?: RatingState;
  evidence_quality?: { score: number; status: string; rule_version: string };
  fundamental_rating?: { status: string; rating: string | null; reason?: string; policy_version?: string; effective_at?: string };
  short_term_prediction?: { status: string; probability?: number | null; probabilities?: { up?: number; status?: string } | null; calibration?: unknown; calibration_version?: string; reason?: string };
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

export type ClaimStatus = {
  statement_occurrence: "documented" | "unknown";
  claimed_event_truth: "corroborated" | "single_source" | "unverified";
  realization_status: "realized" | "effective" | "announced" | "threat" | "statement" | "unknown";
  independent_source_groups: number;
  unknown_lineage_evidence: number;
};

export type RatingState = {
  previous: string;
  current: string;
  changed_at: string | null;
  algorithm_version: "step-limited-rating-v1";
  eligible_event_count: number;
  transition_limited: boolean;
};

export type LatestEventSignal = {
  event_id: string;
  rating: string;
  direction_score: number;
  rating_confidence: number;
  news_confidence: number;
  occurred_at: string;
  detail: { kind: "event" | "asset"; id: string; researched_at: string };
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
  rating_state?: RatingState;
  event_signal_state?: RatingState;
  latest_event_signal?: LatestEventSignal | null;
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
  news_confidence?: number;
  news_credibility_score?: number;
  horizon_days: number;
  horizon_unit: string;
  transmission_path: string[];
  rationale: string;
  missing_information: string[];
  conditional_impact?: boolean;
  conditional_information?: string[];
  conclusion_status?: "directional" | "neutral_supported" | "insufficient_evidence";
  impact_channel?: "supply" | "demand" | "revenue" | "cost" | "profit" | "cash_flow" | "valuation" | "risk_premium";
  claims?: ResearchClaim[];
  transmission_steps?: TransmissionStep[];
  target_evaluation_score?: number;
  target_evaluation?: TargetEvaluation;
  model_target_evaluation?: TargetEvaluation;
  applied_caps?: string[];
  impact_verification?: {
    quality?: {
      structurally_valid: boolean;
      evidence_complete: boolean;
      missing_information: string[];
      conditional_information: string[];
      contradictions: string[];
    };
  };
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
    report_confidence?: number;
    report_confidence_score?: number;
    evidence_complete: boolean;
    news_confidence: number;
    news_credibility_score?: number;
    claim_status?: ClaimStatus;
    prompt_version?: string;
    target_evaluation_version?: string;
    report_confidence_version?: string;
    direction_score?: number;
    rating?: string;
    signal_available?: boolean;
    report_confidence_reason?: "no_valid_target" | null;
    impacts: EventTargetImpact[];
    macro_factors: Array<{ id: string; name: string; description: string; strength: number }>;
    missing_information: string[];
    conditional_information?: string[];
		counter_research?: {
			enabled: boolean;
			status: string;
			candidate_errors_found?: number;
			confirmed_errors_found?: number | null;
			challenged_claims?: string[];
			competing_mechanisms?: string[];
			independent_origin_count?: number;
			latency_ms?: number;
			confidence_effect?: "none";
		};
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
    report_confidence?: number;
    report_confidence_score?: number;
    news_confidence: number;
    news_credibility_score?: number;
    direction_score: number | null;
    rating: string | null;
    signal_available?: boolean;
    report_confidence_reason?: "no_valid_target" | null;
    impact_count: number;
    affected_markets: string[];
    affected_sectors: string[];
    scoring_version: string;
    prompt_version?: string;
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
  observed_at?: string;
  overall_rating_changed?: boolean;
  previous: { rating: string; direction_score: number | null; rating_confidence: number | null } | null;
  current: { rating: string; direction_score: number | null; rating_confidence: number | null };
  latest: { rating: string; direction_score: number | null; rating_confidence: number | null; news_confidence: number | null };
  rating_state?: RatingState;
  event_signal_state?: RatingState;
  latest_event_signal?: LatestEventSignal | null;
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

export const failedResearchDismissPath = (item: Pick<FailedResearch, "kind" | "id">) =>
  `/api/v1/failed-research-runs/${encodeURIComponent(item.kind)}/${encodeURIComponent(item.id)}`;

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
  newsConfidence, ratingConfidence, newsCredibilityScore, reportConfidenceScore, scoreSource, compact = false,
}: {
  score: number | null;
  directionScore?: number | null;
  rating: string;
  confidence: number;
  newsConfidence?: number;
  ratingConfidence?: number;
  newsCredibilityScore?: number;
  reportConfidenceScore?: number;
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
    <span>{!compact && `${signalStatusLabels[resolvedStatus] || resolvedStatus} · `}本次事件信号：{recommendationRatingLabel(rating)}{isV3 && scoreSource === "rule_fallback" ? " · 规则回退" : ""}</span>
    {isV3
      ? <small>{typeof newsCredibilityScore === "number" || typeof reportConfidenceScore === "number"
        ? `新闻可信度 ${newsCredibilityScore ?? Math.round((newsConfidence ?? factConfidence ?? 0) * 100)}/100 · 研报置信度 ${reportConfidenceScore ?? Math.round((ratingConfidence ?? confidence) * 100)}/100 · 未来 ${horizonDays ?? 90} 个自然日`
        : `新闻可信度 ${Math.round((newsConfidence ?? factConfidence ?? 0) * 100)}% · 评级置信度 ${Math.round((ratingConfidence ?? confidence) * 100)}% · 未来 ${horizonDays ?? 90} 个自然日`}</small>
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

const statementOccurrenceLabels: Record<string, string> = { documented: "已记录声明", unknown: "声明状态未知" };
const claimedEventTruthLabels: Record<string, string> = { corroborated: "多源印证", single_source: "单一可追溯来源", unverified: "来源血缘未确认", unknown: "声明状态未知" };
const realizationStatusLabels: Record<string, string> = { realized: "已兑现", effective: "已生效", announced: "已宣布", threat: "威胁/拟议", statement: "仅声明", unknown: "行动阶段未知" };

export function ClaimStatusDetails({ value }: { value?: ClaimStatus }) {
  if (!value) return null;
  return <section className="short-term-score-details">
    <h3>事件事实状态</h3>
    <p>声明：<strong>{statementOccurrenceLabels[value.statement_occurrence] ?? value.statement_occurrence}</strong>；事实支持：<strong>{claimedEventTruthLabels[value.claimed_event_truth] ?? value.claimed_event_truth}</strong>；行动兑现：<strong>{realizationStatusLabels[value.realization_status] ?? value.realization_status}</strong></p>
    <small>可确认独立来源组 {value.independent_source_groups} 个；血缘未知证据 {value.unknown_lineage_evidence} 条。声明被记录不等于行动已兑现。</small>
  </section>;
}

const targetEvaluationLabels: Array<[keyof TargetEvaluation, string, number]> = [
  ["object_relevance", "对象相关性", 20],
  ["evidence_sufficiency", "证据充分度", 25],
  ["transmission_certainty", "传导确定性", 25],
  ["impact_support", "影响支持度", 20],
  ["timing_persistence", "时点持续性", 10],
];

const evaluationCapLabels: Record<string, string> = {
  no_valid_support_id: "无有效支持 ID，归零",
  no_target_specific_evidence: "无目标专属证据，归零",
  source_independence_gate: "缺少官方来源或两个独立来源组，最高 49",
  missing_transmission_steps: "缺少传导步骤，归零",
  incomplete_transmission_step: "关键传导条件缺失，传导确定性最高 39",
  insufficient_evidence: "结论证据不足，影响支持度最高 39",
  missing_economic_endpoint: "缺少经济或财务终点，最高 39",
  unknown_action_stage: "动作阶段未知，最高 20",
  unresolved_contradiction: "存在未解决矛盾，最高 39",
};

export function TargetEvaluationDetails({ value, modelValue, score }: { value?: TargetEvaluation; modelValue?: TargetEvaluation; score?: number }) {
  if (!value) return null;
  return <section className="short-term-score-details target-evaluation-details">
    <h3>标的五项综合评价{typeof score === "number" ? ` · ${score}/100` : ""}</h3>
    <p className="score-formula">20% 对象相关性 + 25% 证据充分度 + 25% 传导确定性 + 20% 影响支持度 + 10% 时点持续性</p>
    <div className="score-factor-grid confidence-factor-grid">
      {targetEvaluationLabels.map(([key, label, weight]) => {
        const assessment = value[key];
        const rawScore = modelValue?.[key]?.score;
        return <article key={key}>
          <span>{label} · 权重 {weight}%</span>
          <strong>{assessment.score}/100{typeof rawScore === "number" && rawScore !== assessment.score ? `（模型原始 ${rawScore}）` : ""}</strong>
          {assessment.reason && <p>{assessment.reason}</p>}
          {!!assessment.cap_reasons?.length && <small>程序封顶：{assessment.cap_reasons.map((reason) => evaluationCapLabels[reason] ?? reason).join("；")}</small>}
          {!!assessment.evidence_ids?.length && <small>证据：{assessment.evidence_ids.join("、")}</small>}
          {!!assessment.action_ids?.length && <small>动作：{assessment.action_ids.join("、")}</small>}
        </article>;
      })}
    </div>
  </section>;
}

export function ResearchReasoningDetails({ claims, steps }: { claims?: ResearchClaim[]; steps?: TransmissionStep[] }) {
  if (!claims?.length && !steps?.length) return null;
  return <section className="research-reasoning-details">
    {!!claims?.length && <><h3>事实与推断</h3><div className="claim-assessments">{claims.map((claim, index) => <article key={`${claim.claim_type}-${index}`}>
      <span>{claim.claim_type === "fact" ? "事实" : "推断"}</span><strong>{claim.text}</strong>
      <small>{[...(claim.evidence_ids || []), ...(claim.action_ids || [])].join("、") || "无有效支持 ID"}</small>
      {!!claim.missing_information?.length && <small>缺失：{claim.missing_information.map(describeMissingInformation).join("；")}</small>}
    </article>)}</div></>}
    {!!steps?.length && <><h3>逐步传导链</h3><ol className="transmission-step-list">{steps.map((step, index) => <li key={`${step.source_node}-${step.target_node}-${index}`}>
      <strong>{step.source_node} → {step.target_node}</strong><span>{step.mechanism} · {step.basis_type === "fact" ? "事实" : "推断"}</span>
      <small>{[...(step.evidence_ids || []), ...(step.action_ids || [])].join("、") || "无有效支持 ID"}</small>
    </li>)}</ol></>}
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
        <strong>{report?.signal_available === false
          ? "本次事件信号：0 · 观望"
          : score === null || score === undefined
            ? "本次事件信号：0 · 观望"
            : `本次事件信号：${score > 0 ? "+" : ""}${score} · ${rating ? recommendationRatingLabel(rating) : "观望"}`}</strong>
        <span>影响目标 {report?.impact_count ?? 0} 个</span>
        <small>新闻可信度 {Math.round((report?.news_confidence ?? 0) * 100)}% · 研报置信度 {Math.round((report?.confidence ?? 0) * 100)}%{report?.report_confidence_reason === "no_valid_target" ? "（无有效影响目标）" : ""}</small>
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
    {items.map((item) => {
      const previousRating = item.event_signal_state?.previous || item.rating_state?.previous || item.previous.rating;
      const currentRating = item.event_signal_state?.current || item.rating_state?.current || item.current.rating;
      const eventSignal = item.latest_event_signal;
      const changedAt = item.rating_state?.changed_at || item.changed_at;
      return <article className="target-change-card" key={item.asset.asset_id}>
      <header>
        <span>{item.asset.market} · {new Date(changedAt).toLocaleString("zh-CN")}</span>
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
        <span>事件信号状态变化</span>
        <strong>{recommendationRatingLabel(previousRating)} → {recommendationRatingLabel(currentRating)}</strong>
      </div>
      {eventSignal && <div className="target-change-field">
        <span>最新新闻信号</span>
        <div className="target-change-rating-row"><strong>{recommendationRatingLabel(eventSignal.rating)}</strong><b className={eventSignal.direction_score < 0 ? "negative" : eventSignal.direction_score > 0 ? "positive" : "neutral"} title="本次事件原始方向分">{eventSignal.direction_score > 0 ? "+" : ""}{eventSignal.direction_score}</b></div>
      </div>}
    </article>;
    })}
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
        newsCredibilityScore={detail.recommendation.news_credibility_score}
        reportConfidenceScore={detail.recommendation.report_confidence_score}
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
		? (detail.recommendation.target_evaluation
		  ? "方向分由模型判断；五级评级、五项评价封顶、新闻可信度和研报置信度均由系统确定性计算。"
		  : "方向分是模型唯一数值判断；五级评级、新闻可信度和评级置信度均由系统独立计算，缺失信息只降低对应置信因子。")
        : isShortTerm
        ? "影响分按 D × (45M + 25T + 15I + 15C) 计算；证据质量核验只降低置信度，不改变方向或隐藏评分。"
        : "该历史结论沿用原评分与证据门禁规则；证据不足记录继续暂不评分，供追溯和重新调研。"}</p>
      {!isV3 && <ModelOpinion direction={detail.recommendation.model_direction} rating={detail.recommendation.model_rating} confidence={detail.recommendation.model_confidence} />}
      {isV3
		? <>
		  <V3ConfidenceDetails recommendation={detail.recommendation} />
		  <ClaimStatusDetails value={detail.recommendation.claim_status} />
		  <TargetEvaluationDetails value={detail.recommendation.target_evaluation} modelValue={detail.recommendation.model_target_evaluation} score={detail.recommendation.target_evaluation_score} />
		  {!!detail.recommendation.impact?.conditional_information?.length && <section className="short-term-score-details"><h3>条件与情景边界</h3><ul>{detail.recommendation.impact.conditional_information.map((item) => <li key={item}>{item}</li>)}</ul></section>}
		  <ResearchReasoningDetails claims={detail.recommendation.impact?.claims} steps={detail.recommendation.impact?.transmission_steps} />
		  <div className="probability-grid"><span>短期预测 <strong>未校准</strong></span><span>概率 <strong>暂不显示</strong></span><span>基本面评级 <strong>未建立</strong></span></div>
        </>
        : isShortTerm
        ? <ShortTermScoreDetails recommendation={detail.recommendation} />
        : <>
          <div className="probability-grid"><span>短期预测 <strong>未校准</strong></span><span>概率 <strong>暂不显示</strong></span><span>基本面评级 <strong>未建立</strong></span></div>
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

const impactChannelLabels: Record<string, string> = {
  supply: "供给", demand: "需求", revenue: "收入", cost: "成本", profit: "利润", cash_flow: "现金流", valuation: "估值", risk_premium: "风险溢价",
};

const conclusionStatusLabels: Record<string, string> = {
  directional: "方向成立", neutral_supported: "证据支持中性", insufficient_evidence: "证据不足",
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
        <span>新闻可信度<strong>{report.news_credibility_score ?? Math.round(report.news_confidence * 100)}/100</strong></span>
        <span>研报置信度<strong>{report.report_confidence_score ?? Math.round((report.report_confidence ?? report.confidence) * 100)}/100{report.report_confidence_reason === "no_valid_target" ? " · 无有效影响目标" : ""}</strong></span>
        <span>影响目标<strong>{report.impacts.length}</strong></span>
      </div>
      {!report.evidence_complete && <div className="page-message">该报告可追溯，但资料覆盖不足，不应视为可直接交易的确定性结论。</div>}
		{report.counter_research?.enabled && <div className="page-message">反方研究：{report.counter_research.status} · 候选错误 {report.counter_research.candidate_errors_found ?? 0} · 独立来源 {report.counter_research.independent_origin_count ?? 0}。候选反证不改变置信度，确认错误需独立真值复核。</div>}
		{!!report.counter_research?.challenged_claims?.length && <details><summary>反方研究候选</summary>{report.counter_research.challenged_claims.map((claim, index) => <p key={`${claim}-${index}`}>{claim}{report.counter_research?.competing_mechanisms?.[index] ? `：${report.counter_research.competing_mechanisms[index]}` : ""}</p>)}</details>}
      <h3>事件结论</h3><p>{report.summary}</p>
	  <ClaimStatusDetails value={report.claim_status} />
      {(report.affected_markets.length > 0 || report.affected_sectors.length > 0) && <div className="event-report-scope">
        <span>市场：{report.affected_markets.join("、") || "未明确"}</span>
        <span>行业：{report.affected_sectors.join("、") || "未明确"}</span>
      </div>}
      {!report.impacts.length && <div className="page-empty">当前证据未确认具体影响标的；系统未创建虚假中性目标。</div>}
      {!!report.impacts.length && <><h3>目标影响</h3><div className="event-impact-grid">{report.impacts.map((impact, index) => <article key={`${impact.target_type}-${impact.target_name}-${index}`}>
        <span>{targetTypeLabels[impact.target_type] ?? impact.target_type}{impact.asset?.symbol ? ` · ${impact.asset.symbol}` : ""}</span>
        <strong>{impact.target_name}</strong>
        <div><b>本次事件信号：{recommendationRatingLabel(impact.rating)}</b><b>{impact.direction_score > 0 ? "+" : ""}{impact.direction_score}</b><b>目标评价 {impact.target_evaluation_score ?? Math.round(impact.rating_confidence * 100)}/100</b></div>
        {impact.news_credibility_score !== undefined && <small>目标证据可信度：{impact.news_credibility_score}/100</small>}
        {(impact.conclusion_status || impact.impact_channel) && <small>{conclusionStatusLabels[impact.conclusion_status ?? ""] ?? impact.conclusion_status ?? ""}{impact.impact_channel ? ` · ${impactChannelLabels[impact.impact_channel] ?? impact.impact_channel}` : ""}</small>}
        {impact.impact_verification?.quality && <small>目标证据门禁：{impact.impact_verification.quality.evidence_complete ? "通过" : "未通过"}</small>}
        <small>未来 {impact.horizon_days} 个自然日</small>
        {impact.rationale && <p>{impact.rationale}</p>}
        {!!impact.transmission_path.length && <ol>{impact.transmission_path.map((step, stepIndex) => <li key={`${step}-${stepIndex}`}>{step}</li>)}</ol>}
        <TargetEvaluationDetails value={impact.target_evaluation} modelValue={impact.model_target_evaluation} score={impact.target_evaluation_score} />
        <ResearchReasoningDetails claims={impact.claims} steps={impact.transmission_steps} />
        {!!impact.conditional_information?.length && <small>条件 / Conditions：{impact.conditional_information.join("；")}</small>}
        {!!impact.missing_information.length && <small>缺失 / Missing：{impact.missing_information.map(describeMissingInformation).join("；")}</small>}
      </article>)}</div></>}
      {!!report.macro_factors.length && <><h3>宏观因子</h3><div className="macro-factor-list">{report.macro_factors.map((factor) => <article key={factor.id}><strong>{factor.name}</strong><span>{Math.round(factor.strength * 100)}%</span><p>{factor.description}</p></article>)}</div></>}
      {!!report.scenarios.length && <><h3>情景</h3><ul>{report.scenarios.map((item) => <li key={item}>{item}</li>)}</ul></>}
      {!!report.catalysts.length && <><h3>催化剂</h3><ul>{report.catalysts.map((item) => <li key={item}>{item}</li>)}</ul></>}
      {!!report.risks.length && <><h3>风险</h3><ul>{report.risks.map((item) => <li key={item}>{item}</li>)}</ul></>}
      {!!report.unresolved_questions.length && <><h3>待确认问题</h3><ul>{report.unresolved_questions.map((item) => <li key={item}>{item}</li>)}</ul></>}
      {!!report.conditional_information?.length && <><h3>条件与情景边界</h3><ul>{report.conditional_information.map((item) => <li key={item}>{item}</li>)}</ul></>}
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
      const eventSignal = item.latest_event_signal;
      const score = eventSignal?.direction_score ?? item.latest.direction_score;
      const newsConfidence = eventSignal?.news_confidence ?? item.latest.news_confidence;
      const confidence = eventSignal?.rating_confidence ?? item.latest.rating_confidence;
      const previousRating = item.event_signal_state?.previous || item.rating_state?.previous || item.previous?.rating || item.current.rating;
      const currentRating = item.event_signal_state?.current || item.rating_state?.current || item.current.rating;
      const overallRatingChanged = item.overall_rating_changed ?? previousRating !== currentRating;
      const changedAt = item.rating_state?.changed_at || item.observed_at || item.changed_at;
      return <article className={`target-change-card ${item.kind}`} key={item.key}>
        <header>
          <span>{item.target_type === "commodity_price" || item.kind === "macro" ? (targetTypeLabels[item.target_type] ?? item.target_type) : item.market} · {new Date(changedAt).toLocaleString("zh-CN")}</span>
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
        <div className={`target-change-field ${overallRatingChanged ? "changed" : "unchanged"}`}><span>{overallRatingChanged ? "事件信号状态变化" : "总体评级（未变）"}</span><div className="target-change-rating-row"><strong>{overallRatingChanged ? `${recommendationRatingLabel(previousRating)} → ${recommendationRatingLabel(currentRating)}` : recommendationRatingLabel(currentRating)}</strong>{overallRatingChanged ? ((item.event_signal_state || item.rating_state)?.transition_limited && <em className="target-change-limited">单步限制</em>) : <em className="target-change-limited">未触发变更</em>}</div></div>
        <div className="target-change-field"><span>最新新闻信号</span><div className="target-change-rating-row"><strong>{recommendationRatingLabel(eventSignal?.rating || item.latest.rating)}</strong><b className={score === null ? "neutral" : score < 0 ? "negative" : score > 0 ? "positive" : "neutral"} title="本次事件原始方向分">{score === null ? "—" : `${score > 0 ? "+" : ""}${score}`}</b></div></div>
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
  const params = new URLSearchParams({ kind, scope: "observed", limit: "50" });
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
    {!items.length && !error && (loading ? <div className="page-message">正在加载{title}…</div> : <div className="page-empty">{query ? `未找到与“${query}”匹配的评级观测。` : "当前没有最近评级观测。"}</div>)}
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
    <PageHeading eyebrow="RATING CHANGES" title="标的评级变化" copy="左侧追踪宏观经济、行业及跨资产目标，右侧追踪具体证券与商品价格；同时展示最近观测，未满足证据门槛的新闻信号不会改变总体评级。" />
    {detailError && <div className="page-error target-detail-error"><span>{detailError}</span></div>}
    <form className="page-filters target-search" role="search" onSubmit={(event) => { event.preventDefault(); setDebouncedQuery(searchQuery.trim()); }}>
      <input type="search" aria-label="搜索评级变化" placeholder="搜索宏观、行业、代码或标的名称" value={searchQuery} onChange={(event) => setSearchQuery(event.target.value)} />
      {searchQuery && <button type="button" onClick={() => { setSearchQuery(""); setDebouncedQuery(""); }}>清除</button>}
    </form>
    <div className="target-change-split">
      <TargetChangeSection apiBase={apiBase} kind="macro" title="宏观经济与行业变化" copy="经济、行业、汇率、利率、供给、航运与风险资产的评级变化与最近观测。" query={debouncedQuery} onOpen={(item) => void openLatestResearch(item)} detailLoadingId={detailLoadingId} researchStates={researchStates} onResearch={(item) => void researchAgain(item)} />
      <TargetChangeSection apiBase={apiBase} kind="asset" title="具体标的变化" copy="股票、加密资产与商品价格的总体评级变化、最近观测及最新新闻信号。" query={debouncedQuery} onOpen={(item) => void openLatestResearch(item)} detailLoadingId={detailLoadingId} researchStates={researchStates} onResearch={(item) => void researchAgain(item)} />
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
  const [dismissingId, setDismissingId] = useState("");
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

  async function dismissFailure(item: FailedResearch) {
    const dismissKey = `${item.kind}-${item.id}`;
    setDismissingId(dismissKey); setRetryMessage(""); setRetryMessageError(false);
    setFailedItems((current) => current.filter((candidate) => candidate.id !== item.id || candidate.kind !== item.kind));
    try {
      const response = await fetch(`${apiBase}${failedResearchDismissPath(item)}`, { method: "DELETE" });
      const payload = await response.json() as { detail?: string };
      if (!response.ok) throw new Error(payload.detail || "关闭失败研究记录失败");
    } catch (reason) {
      setRetryMessage(reason instanceof Error ? reason.message : "关闭失败研究记录失败");
      setRetryMessageError(true);
      await loadFailures();
    } finally { setDismissingId(""); }
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
                <button type="button" className="failed-research-dismiss" aria-label={`关闭失败研究 ${item.asset?.symbol || item.event?.headline || item.id}`} title="关闭并从失败列表隐藏" disabled={dismissingId === `${item.kind}-${item.id}`} onClick={() => void dismissFailure(item)}>×</button>
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
  mode?: string;
  default_research_profile?: string;
  rule_version?: string;
  normalization_warnings?: Array<{ field: string; value: string; code: string; message: string }>;
  routing_stats?: { fast_24h: number; deep_24h: number; blocked_24h: number };
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
  routing_stats: { fast_24h: 0, deep_24h: 0, blocked_24h: 0 },
};

type FilterKeywordIssue = { field: "whitelist_keywords" | "blacklist_keywords"; index: number; value: string; code: string; message: string };

function normalizeFilterKeyword(value: string) {
  return value.normalize("NFKC").trim().replace(/\s+/g, " ");
}

function analyzeKeywordList(value: string, field: FilterKeywordIssue["field"]) {
  const seen = new Set<string>();
  const keywords: string[] = [];
  const issues: FilterKeywordIssue[] = [];
  let duplicates = 0;
  value.split(/[\r\n,，]+/).forEach((raw, index) => {
    const item = normalizeFilterKeyword(raw);
    if (!item) return;
    if (/[\u0000-\u001f\u007f-\u009f]/.test(item)) {
      issues.push({ field, index, value: raw, code: "invalid_character", message: "关键字包含不可用的控制字符" });
      return;
    }
    if (Array.from(item).length > 80) {
      issues.push({ field, index, value: raw, code: "too_long", message: "关键字不能超过 80 个字符" });
      return;
    }
    const key = item.toLocaleLowerCase();
    if (seen.has(key)) { duplicates += 1; return; }
    seen.add(key);
    keywords.push(item);
  });
  if (keywords.length > 200) issues.push({ field, index: -1, value: "", code: "too_many", message: "每组关键字不能超过 200 个" });
  return { keywords, issues, duplicates };
}

export function validateFilterKeywords(whitelist: string, blacklist: string) {
  const white = analyzeKeywordList(whitelist, "whitelist_keywords");
  const black = analyzeKeywordList(blacklist, "blacklist_keywords");
  const blackKeys = new Set(black.keywords.map((item) => item.toLocaleLowerCase()));
  const conflicts = white.keywords.filter((item) => blackKeys.has(item.toLocaleLowerCase()));
  const issues = [...white.issues, ...black.issues];
  conflicts.forEach((value, index) => {
    issues.push({ field: "whitelist_keywords", index, value, code: "cross_list_conflict", message: `“${value}”同时存在于白名单和黑名单` });
    issues.push({ field: "blacklist_keywords", index: black.keywords.findIndex((item) => item.toLocaleLowerCase() === value.toLocaleLowerCase()), value, code: "cross_list_conflict", message: `“${value}”同时存在于白名单和黑名单` });
  });
  return { whitelist: white.keywords, blacklist: black.keywords, whitelistDuplicates: white.duplicates, blacklistDuplicates: black.duplicates, conflicts, issues };
}

export function parseFilterKeywords(value: string) {
  return analyzeKeywordList(value, "whitelist_keywords").keywords;
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
    setWhitelist(payload.whitelist_keywords.join(", "));
    setBlacklist(payload.blacklist_keywords.join(", "));
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
    const validation = validateFilterKeywords(whitelist, blacklist);
    if (validation.issues.length > 0) {
      setMessage(validation.issues.map((item) => item.message).join("；"));
      return;
    }
    const response = await fetch(`${apiBase}/api/v1/source-filter`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        enabled,
        whitelist_keywords: validation.whitelist,
        blacklist_keywords: validation.blacklist,
      }),
    });
    const body = await response.json();
    if (!response.ok) { setMessage(body.errors?.map((item: FilterKeywordIssue) => item.message).join("；") || body.detail?.[0]?.msg || body.detail || "保存失败"); return; }
    applyConfig(body as SourceFilterConfig);
    const cleaned = (body as SourceFilterConfig).normalization_warnings?.length || 0;
    setMessage(`规则已保存，未领取研究任务已重新分类。${cleaned ? ` 已自动清理 ${cleaned} 个重复项。` : ""}`);
  }
  async function reset() {
    if (!window.confirm("恢复默认规则？白名单将清空，所有非黑名单新闻使用快速研究；黑名单恢复为“天气”。")) return;
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
  const validation = validateFilterKeywords(whitelist, blacklist);
  const whitelistCount = validation.whitelist.length;
  const blacklistCount = validation.blacklist.length;
  const duplicateCount = validation.whitelistDuplicates + validation.blacklistDuplicates;
  const hasErrors = validation.issues.length > 0;
  return <section className="app-page source-filter-page">
    <PageHeading eyebrow="NEWS ADMISSION & RESEARCH ROUTING" title="新闻准入与研究分流" copy="黑名单控制新闻准入；白名单只决定是否进入 Qwen2.5 7B 深度研究。" />
    <div className="filter-metrics">
      <div><span>过滤状态</span><strong className={enabled ? "enabled" : "disabled"}>{enabled ? "已启用" : "已关闭"}</strong></div>
      <div><span>快速研究 / 24h</span><strong>{config.routing_stats?.fast_24h || 0}</strong><small>未命中白名单</small></div>
      <div><span>深度研究 / 24h</span><strong>{config.routing_stats?.deep_24h || 0}</strong><small>命中白名单</small></div>
      <div><span>黑名单拦截 / 24h</span><strong>{config.routing_stats?.blocked_24h || 0}</strong><small>黑名单优先</small></div>
    </div>
    <form className="source-filter-form" onSubmit={save}>
      <label className="filter-master-switch"><input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} /><span><strong>启用新闻标题准入与分流</strong><small>关闭后不拦截新闻，全部使用快速研究。</small></span></label>
      <div className="keyword-panels">
        <label className={validation.issues.some((item) => item.field === "whitelist_keywords") ? "invalid" : ""}><span><strong>白名单关键字</strong><small>{whitelistCount} / 200 · 重复 {validation.whitelistDuplicates} · 冲突 {validation.conflicts.length}</small></span><textarea aria-label="白名单关键字" aria-invalid={validation.issues.some((item) => item.field === "whitelist_keywords")} value={whitelist} placeholder="例如：苹果供应链, MS" onChange={(e) => setWhitelist(e.target.value)} /></label>
        <label className={validation.issues.some((item) => item.field === "blacklist_keywords") ? "invalid" : ""}><span><strong>黑名单关键字</strong><small>{blacklistCount} / 200 · 重复 {validation.blacklistDuplicates} · 冲突 {validation.conflicts.length}</small></span><textarea aria-label="黑名单关键字" aria-invalid={validation.issues.some((item) => item.field === "blacklist_keywords")} value={blacklist} placeholder="例如：天气" onChange={(e) => setBlacklist(e.target.value)} /></label>
      </div>
      <p className="filter-note">支持英文逗号、中文逗号和换行分隔；保存后统一显示为英文逗号加空格。短英文、数字和证券代码按完整边界匹配；同时命中时黑名单优先。</p>
      {!loading && enabled && whitelistCount === 0 && <div className="page-message">白名单为空：所有非黑名单新闻使用快速研究。</div>}
      {duplicateCount > 0 && <div className="page-message">检测到 {duplicateCount} 个重复项，保存时会自动去重。</div>}
      {hasErrors && <div className="page-error">{validation.issues.map((item) => item.message).join("；")}</div>}
      <div className="card-actions"><button type="submit" disabled={hasErrors}>保存规则</button><button type="button" onClick={load}>刷新</button><button type="button" className="danger" onClick={reset}>恢复默认</button></div>
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
	asset_class?: string;
  market: string;
  symbol: string;
  name: string;
	exchange_or_provider?: string;
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

export function fundamentalAssetSearchTerm(value: string) {
	const normalized = value.trim();
	if (!normalized) return "";
	return normalized.includes(":") ? normalized.split(":").at(-1)?.trim() || normalized : normalized;
}

function normalizedAssetVenue(value: string) {
	const normalized = value.trim().toUpperCase();
	const aliases: Record<string, string> = {
		XNAS: "NASDAQ", NASDAQ: "NASDAQ", XNYS: "NYSE", NYSE: "NYSE",
		ARCX: "NYSE_ARCA", NYSE_ARCA: "NYSE_ARCA", XASE: "AMEX", AMEX: "AMEX",
	};
	return aliases[normalized] || normalized;
}

export function selectFundamentalAssetCandidate(value: string, items: UniverseAsset[]) {
	const normalized = value.trim().toLocaleLowerCase();
	const term = fundamentalAssetSearchTerm(value).toLocaleLowerCase();
	if (!normalized || !term) throw new Error("请输入资产代码、名称或规范资产 ID");
	const exactID = items.find((item) => item.asset_id.toLocaleLowerCase() === normalized);
	if (exactID) return exactID;
	const exactSymbol = items.filter((item) => item.symbol.trim().toLocaleLowerCase() === term);
	const inputParts = value.trim().split(":").map((part) => part.trim()).filter(Boolean);
	if (inputParts.length >= 3) {
		const inputClass = inputParts[0].toLocaleLowerCase();
		const inputVenue = normalizedAssetVenue(inputParts.at(-2) || "");
		const typed = exactSymbol.filter((item) => (item.asset_class || item.asset_id.split(":")[0]).trim().toLocaleLowerCase() === inputClass);
		const venueMatched = typed.filter((item) => normalizedAssetVenue(item.exchange_or_provider || item.asset_id.split(":").at(-2) || "") === inputVenue);
		if (venueMatched.length === 1) return venueMatched[0];
		if (typed.length === 1) return typed[0];
	}
	if (exactSymbol.length === 1) return exactSymbol[0];
	const listedEquities = exactSymbol.filter((item) => (item.asset_class || item.asset_id.split(":")[0]).trim().toLocaleLowerCase() === "equity");
	if (listedEquities.length === 1) return listedEquities[0];
	const exactNameOrAlias = items.filter((item) => item.name.trim().toLocaleLowerCase() === normalized
		|| item.aliases.some((alias) => alias.trim().toLocaleLowerCase() === normalized));
	if (exactNameOrAlias.length === 1) return exactNameOrAlias[0];
	if (items.length === 1) return items[0];
	if (exactSymbol.length > 1 || exactNameOrAlias.length > 1 || items.length > 1) throw new Error("匹配到多个资产，请输入更精确的规范资产 ID");
	throw new Error(`未找到资产：${value.trim()}`);
}

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
      headers: { "Content-Type": "application/json" },
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
            <button type="button" disabled={status?.status === "running"} onClick={() => void queueAdminAction("refresh", key)}>{status?.status === "running" ? "同步中…" : "同步此市场"}</button>
          </article>;
        })}
      </div>
      <div className="universe-actions">
        <button type="button" disabled={loading} onClick={() => void queueAdminAction("refresh")}>同步全部市场</button>
        <button type="button" disabled={loading} onClick={() => void queueAdminAction("backfill")}>回补最近 30 天映射</button>
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
              <td><div className="universe-row-actions"><button type="button" disabled={researchingId === asset.asset_id || !asset.active} onClick={() => void researchAsset(asset)}>{researchingId === asset.asset_id ? "入队中…" : "研究"}</button><button type="button" onClick={() => beginEdit(asset)}>编辑</button></div></td>
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

type ResearchPolicyStatus = {
  configured_mode: "shadow" | "enforce";
  active_mode: "shadow" | "enforce";
  version: string;
  prediction_mode: "unavailable";
  minimum_days: number;
  minimum_reviews: number;
  shadow_started_at?: string;
  shadow_age_days?: number;
  valid_impacts: number;
  reviewed_impacts: number;
  ready_for_approval: boolean;
  approved: boolean;
};

type ResearchPolicyEvaluation = {
  id: string;
  event_id: string | null;
  asset_id: string | null;
  headline: string | null;
  asset_name: string | null;
  symbol: string | null;
  event_signal: { status: string; direction_score: number; rating: string };
  evidence_quality: { score: number; status: string };
  created_at: string;
  decision: "accepted" | "rejected" | null;
  reviewer: string | null;
  evidence: Array<{ claim?: string; excerpt?: string; source_name?: string }>;
};

export function ResearchPolicyPage({ apiBase }: { apiBase: string }) {
  const [status, setStatus] = useState<ResearchPolicyStatus | null>(null);
  const [items, setItems] = useState<ResearchPolicyEvaluation[]>([]);
  const [reviewer, setReviewer] = useState("");
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(true);
  const headers = { "Content-Type": "application/json" };
  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const response = await fetch(`${apiBase}/go/research-policy/evaluations?limit=50`);
      if (!response.ok) throw new Error("无法读取影子策略状态");
      const payload = await response.json() as { items: ResearchPolicyEvaluation[]; policy: ResearchPolicyStatus };
      setItems(payload.items || []); setStatus(payload.policy);
    } catch (error) { setMessage(error instanceof Error ? error.message : "无法读取影子策略状态"); }
    finally { setLoading(false); }
  }, [apiBase]);
  useEffect(() => { void refresh(); }, [refresh]);
  async function review(id: string, decision: "accepted" | "rejected") {
    if (!reviewer.trim()) { setMessage("请先填写复核人。 "); return; }
    const response = await fetch(`${apiBase}/go/research-policy/reviews`, { method: "POST", headers, body: JSON.stringify({ policy_evaluation_id: id, reviewer: reviewer.trim(), decision, note: "" }) });
    if (!response.ok) { setMessage("保存复核失败，请检查记录状态或服务器访问配置。"); return; }
    setMessage("人工复核已保存。 "); await refresh();
  }
  async function approve() {
    if (!reviewer.trim()) { setMessage("请先填写批准人。 "); return; }
    const response = await fetch(`${apiBase}/go/research-policy/approve`, { method: "POST", headers, body: JSON.stringify({ approved_by: reviewer.trim(), note: "P0 shadow gate approved" }) });
    setMessage(response.ok ? "已写入不可变批准记录。" : "尚未满足 14 天和 100 条人工复核门槛。");
    if (response.ok) await refresh();
  }
  return <section className="app-page policy-page">
    <PageHeading eyebrow="P0 EVIDENCE GOVERNANCE" title="策略影子期" copy="事件信号与证据门禁先在影子模式下审计。系统不会自动切换至强制门禁。" />
    <div className="research-gate-grid">
      <span>当前模式<strong>{status?.active_mode === "enforce" ? "强制" : "影子"}</strong></span>
      <span>影子时长<strong>{status?.shadow_age_days ?? 0} / {status?.minimum_days ?? 14} 天</strong></span>
      <span>人工复核<strong>{status?.reviewed_impacts ?? 0} / {status?.minimum_reviews ?? 100}</strong></span>
      <span>批准状态<strong>{status?.approved ? "已批准" : "待批准"}</strong></span>
    </div>
    <p className="score-explanation">版本 {status?.version || "p0-evidence-v1"} · 事件影响分不等于投资评级；短期概率仅在独立校准有效时显示。</p>
    <div className="integration-editor"><label>复核人<input value={reviewer} onChange={(event) => setReviewer(event.target.value)} placeholder="姓名或工号" /></label><button type="button" disabled={!status?.ready_for_approval} onClick={() => void approve()}>批准切换资格</button><small>批准只记录资格；仍需将服务配置显式改为 enforce 才会启用。</small></div>
    {message && <div className="page-message">{message}</div>}
    {loading ? <div className="page-empty">正在读取影子评估…</div> : <div className="conclusion-list">{items.length === 0 ? <div className="page-empty">暂无可人工复核的有效定向影响。</div> : items.map((item) => <article className="conclusion-card" key={item.id}><span>{new Date(item.created_at).toLocaleString("zh-CN")} · {item.symbol || item.asset_id || item.event_id || "事件"}</span><h3>{item.headline || item.asset_name || "事件信号"}</h3><p>事件信号：{item.event_signal?.rating || "观望"} · {item.event_signal?.direction_score ?? 0}；证据状态：{item.evidence_quality?.status || "unknown"} · 质量 {Math.round((item.evidence_quality?.score || 0) * 100)}%</p>{item.evidence?.length > 0 && <details><summary>证据快照（最多 5 条）</summary>{item.evidence.map((evidence, index) => <p key={index}>{evidence.claim || evidence.excerpt || "证据"}{evidence.source_name ? ` · ${evidence.source_name}` : ""}</p>)}</details>}{item.decision ? <small>已由 {item.reviewer || "管理员"} 复核：{item.decision === "accepted" ? "接受" : "驳回"}</small> : <div><button type="button" onClick={() => void review(item.id, "accepted")}>接受</button><button type="button" onClick={() => void review(item.id, "rejected")}>驳回</button></div>}</article>)}</div>}
  </section>;
}

type FundamentalBundle = {
  fundamentals?: { items?: Array<{ id?: string; statement_type?: string; available_at?: string; source?: { provider?: string; url?: string } }> };
	prices?: { items?: Array<{ id?: string; price?: number; price_field?: string; observed_at?: string; available_at?: string; currency?: string; source_name?: string; source_url?: string }> };
	tradability?: {
		contract_version?: string;
		items?: Array<{ id?: string; session_date?: string; observed_at?: string; available_at?: string; status?: string; buy_executable?: boolean; sell_executable?: boolean; source_name?: string; source_document_id?: string; source_url?: string }>;
		resolved_by_session?: Record<string, { status?: string; reason?: string; source_count?: number }>;
	};
	consensus?: {
		items?: Array<{ id?: string; metric?: string; fiscal_period_end?: string; statistic?: string; estimate_value?: number; analyst_count?: number; currency?: string; available_at?: string; source_name?: string }>;
		revisions?: Array<{ current_id?: string; metric?: string; fiscal_period_end?: string; statistic?: string; previous_value?: number; current_value?: number; absolute_change?: number; direction?: string; observed_at?: string; individual_analyst_behavior_status?: string }>;
		provider_publication_time_available?: boolean;
		historical_backfill?: boolean;
		automatic_rating?: boolean;
	};
	guidance?: {
		items?: Array<{ id?: string; metric?: string; fiscal_period_end?: string; low_value?: number; high_value?: number; currency?: string; available_at?: string; source_name?: string; source_url?: string }>;
		revisions?: Array<{ current_id?: string; metric?: string; fiscal_period_end?: string; previous_low?: number; previous_high?: number; current_low?: number; current_high?: number; direction?: string; range_change?: string; changed_bounds?: string[]; available_at?: string }>;
		guidance_is_consensus?: boolean;
		automatic_rating?: boolean;
	};
	guidanceSources?: {
		items?: Array<{ id?: string; accession_number?: string; form?: string; filing_date?: string; report_date?: string; accepted_at?: string; filing_index_url?: string; primary_document_url?: string; latest_review?: { decision?: string; reviewed_by?: string; reviewed_at?: string; guidance_snapshot_id?: string } }>;
		review_status_counts?: Record<string, number>;
		candidate_is_guidance?: boolean;
		human_review_required?: boolean;
		automatic_extraction?: boolean;
		sync_status?: string;
		sync_reason?: string;
	};
	preparation?: {
		status?: string;
		reason?: string;
		statement_period_end?: string;
		fundamental_snapshot_ids?: string[];
		missing_fields?: string[];
		analyst_inputs_required?: string[];
		factual_inputs?: Record<string, unknown>;
		field_lineage?: Record<string, { snapshot_id?: string; metrics?: string[]; transform?: string }>;
		workflow_template?: Record<string, unknown>;
		controls?: { automatic_assumptions?: boolean; automatic_valuation?: boolean; automatic_rating?: boolean; analyst_approval_required?: boolean };
	};
	analystEvidence?: {
		items?: Array<{ id?: string; evidence_type?: string; title?: string; rationale?: string; values?: Record<string, unknown>; observed_at?: string; available_at?: string; approved_by?: string; approved_at?: string; approval_kind?: string; policy_version?: string; source_name?: string; source_url?: string }>;
	};
	benchmarkMapping?: {
		resolution?: { status?: string; reason?: string; mapping?: { id?: string; scope_type?: string; scope_id?: string; benchmark_asset_id?: string; source_name?: string; mapping_reason?: string; approved_by?: string; available_at?: string } };
	};
  forecasts?: { items?: Array<{ id?: string; model_version?: string; status?: string; as_of?: string; assumptions?: unknown[] }> };
  valuations?: { items?: Array<{ id?: string; model_version?: string; status?: string; as_of?: string; result?: { status?: string; reason?: string; currency?: string; range?: { low?: number; high?: number } } }> };
  ratings?: { items?: Array<{
    result?: { status?: string; rating?: string; reason?: string; horizon_days?: number; benchmark_id?: string; policy_version?: string; valuation_run_id?: string };
    state?: { effective_at?: string };
    revision?: { previous_rating?: string; current_rating?: string; action?: string; reason?: string };
    reason_codes?: string[];
    changed_assumptions?: Record<string, unknown>;
    evidence_ids?: string[];
  }> };
  predictions?: { items?: Array<{ status?: string; probability?: number; signal_available_at?: string; horizon_sessions?: number; model_version?: string; calibration_version?: string; exclusion_reason?: string }> };
	marketPolicy?: { asset_class?: string; market?: string; currency?: string; policy?: { version?: string; fundamental_method?: string; fundamental_supported?: boolean; prediction_supported?: boolean; prediction_scope?: string; benchmark_id?: string; benchmark_policy?: string; reason?: string; required_inputs?: string[] } };
  schedule?: { items?: Array<{ id?: string; status?: string; forecast_version_id?: string; cadence_hours?: number; next_run_at?: string; last_run_status?: string; last_run_reason?: string; approved_by?: string; approved_at?: string; approval_kind?: string; policy_version?: string }> };
};

type FundamentalAIPreparation = {
	asset_id?: string;
	status?: string;
	run?: {
		id?: string;
		task_id?: string;
		status?: string;
		stage?: string;
		policy_version?: string;
		model_version?: string;
		summary?: Record<string, unknown>;
		blockers?: string[];
		created_at?: string;
		updated_at?: string;
	};
	sources?: Array<{ id?: string; title?: string; source_name?: string; source_class?: string; source_url?: string; published_at?: string; available_at?: string; content_type?: string; content_hash?: string; retrieval_status?: string; retrieval_detail?: string }>;
	candidates?: Array<{ id?: string; evidence_type?: string; title?: string; rationale?: string; values?: Record<string, unknown>; source_snapshot_ids?: string[]; evidence_quote?: string; evidence_location?: string; status?: string; validation?: Record<string, unknown>; approved_evidence_id?: string }>;
	search_snippets_are_evidence?: boolean;
	automatic_model_release?: boolean;
};
const fundamentalAIStageLabels: Record<string, string> = { queued: "等待", data_sync: "数据同步", local_search: "本地搜索", original_fetch: "原文抓取", ai_reasoning: "AI 推理", counterevidence: "反证检查", policy_validation: "政策校验", completed: "已完成", failed: "失败" };

type LicensedBenchmarkImportReceipt = {
	id?: string;
	asset_id?: string;
	vendor_code?: string;
	source_name?: string;
	source_document_id?: string;
	source_url?: string;
	license_reference?: string;
	approved_by?: string;
	session_start?: string;
	session_end?: string;
	observation_count?: number;
	inserted_count?: number;
	available_at?: string;
};

type TradabilityImportReceipt = {
	id?: string;
	asset_id?: string;
	market?: string;
	currency?: string;
	source_name?: string;
	source_document_id?: string;
	source_url?: string;
	license_reference?: string;
	approved_by?: string;
	session_start?: string;
	session_end?: string;
	observation_count?: number;
	inserted_count?: number;
	available_at?: string;
};

type TradabilityImportPreview = {
	observationCount: number;
	sessionStart: string;
	sessionEnd: string;
	statusCounts: Record<string, number>;
};

const tradabilityStatuses = new Set(["tradable", "suspended", "limit_up", "limit_down", "delisted"]);

export function tradabilityStatusLabel(status?: string) {
	return ({ tradable: "可交易", suspended: "停牌", limit_up: "涨停", limit_down: "跌停", delisted: "退市", conflict: "来源冲突", unknown: "证据缺失" } as Record<string, string>)[status || ""] || status || "证据缺失";
}

export function tradabilityImportTemplate(assetID: string) {
	const identity = assetID.trim().split(":");
	const assetClass = identity[0]?.toLowerCase();
	if ((assetClass !== "equity" && assetClass !== "etf") || identity.length < 3 || identity.some((part) => !part.trim())) return "";
	return JSON.stringify({
		source_name: "", source_document_id: "", source_url: "", license_reference: "", approved_by: "",
	}, null, 2);
}

function validUTCDateOnly(value: string) {
	if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
	const parsed = new Date(`${value}T00:00:00.000Z`);
	return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

export function tradabilityImportBody(assetID: string, metadataJSON: string, observationsText: string, now = new Date()) {
	if (!tradabilityImportTemplate(assetID)) throw new Error("可交易状态只支持股票或 ETF 资产");
	if (Number.isNaN(now.getTime())) throw new Error("当前校验时间无效");
	let metadata: Record<string, unknown>;
	try {
		const parsed = JSON.parse(metadataJSON) as unknown;
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error();
		metadata = parsed as Record<string, unknown>;
	} catch {
		throw new Error("授权信息必须是有效 JSON 对象");
	}
	const required = ["source_name", "source_document_id", "source_url", "license_reference", "approved_by"] as const;
	const normalized = Object.fromEntries(required.map((field) => [field, String(metadata[field] || "").trim()])) as Record<(typeof required)[number], string>;
	for (const field of required) {
		if (!normalized[field]) throw new Error(`${field} 不能为空`);
	}
	const maximumLengths = { source_name: 120, source_document_id: 320, license_reference: 320, approved_by: 160 } as const;
	for (const field of Object.keys(maximumLengths) as Array<keyof typeof maximumLengths>) {
		if (normalized[field].length > maximumLengths[field]) throw new Error(`${field} 超过长度限制`);
	}
	try {
		const sourceURL = new URL(normalized.source_url);
		if (sourceURL.protocol !== "https:") throw new Error();
		if (sourceURL.username || sourceURL.password || sourceURL.search || sourceURL.hash) throw new Error("credentials");
	} catch (error) {
		if (error instanceof Error && error.message === "credentials") throw new Error("source_url 不得包含用户名、密码、查询参数或片段");
		throw new Error("source_url 必须是绝对 HTTPS 地址");
	}

	type Observation = { session_date: string; source_observed_at: string; status: string };
	let rawObservations: Array<Record<string, unknown>>;
	const trimmed = observationsText.trim();
	if (!trimmed) throw new Error("必须提供可交易状态观测");
	if (trimmed.startsWith("[")) {
		try {
			const parsed = JSON.parse(trimmed) as unknown;
			if (!Array.isArray(parsed)) throw new Error();
			rawObservations = parsed as Array<Record<string, unknown>>;
		} catch {
			throw new Error("观测 JSON 必须是数组");
		}
	} else {
		const lines = trimmed.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
		if (lines[0]?.toLowerCase().replace(/\s/g, "") === "session_date,source_observed_at,status") lines.shift();
		rawObservations = lines.map((line) => {
			const cells = line.split(",").map((cell) => cell.trim());
			if (cells.length !== 3) throw new Error("CSV 每行必须只有 session_date,source_observed_at,status 三列");
			return { session_date: cells[0], source_observed_at: cells[1], status: cells[2] };
		});
	}
	if (rawObservations.length < 1 || rawObservations.length > 1000) throw new Error("一次必须提供 1—1000 条观测");
	const today = now.toISOString().slice(0, 10);
	const seen = new Set<string>();
	const observations: Observation[] = rawObservations.map((item) => {
		if (!item || typeof item !== "object" || Array.isArray(item)) throw new Error("每条观测必须是对象");
		const sessionDate = String(item.session_date || "").trim();
		if (!validUTCDateOnly(sessionDate) || sessionDate > today) throw new Error("session_date 必须是非未来的有效 YYYY-MM-DD 日期");
		if (seen.has(sessionDate)) throw new Error("同一导入中 session_date 不能重复");
		seen.add(sessionDate);
		const sourceObservedAt = String(item.source_observed_at || "").trim();
		const dateOnly = validUTCDateOnly(sourceObservedAt);
		const timestamped = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(sourceObservedAt);
		const observedTime = dateOnly ? new Date(`${sourceObservedAt}T00:00:00.000Z`) : new Date(sourceObservedAt);
		if ((!dateOnly && !timestamped) || Number.isNaN(observedTime.getTime()) || observedTime.getTime() > now.getTime()) throw new Error("source_observed_at 必须是非未来的 YYYY-MM-DD 或 RFC3339 时间");
		const status = String(item.status || "").trim().toLowerCase();
		if (!tradabilityStatuses.has(status)) throw new Error("status 只能是 tradable、suspended、limit_up、limit_down 或 delisted");
		return { session_date: sessionDate, source_observed_at: sourceObservedAt, status };
	});
	observations.sort((left, right) => left.session_date.localeCompare(right.session_date));
	return { ...normalized, observations };
}

export function scheduleDraftJSON(payload: { status?: string; schedule_draft?: Record<string, unknown> }) {
	if (payload.status !== "available" || !payload.schedule_draft) return "";
	return JSON.stringify(payload.schedule_draft, null, 2);
}

export function benchmarkMappingDraftJSON(assetID: string, marketPolicy: FundamentalBundle["marketPolicy"], validFrom = new Date()) {
	const market = marketPolicy?.market?.trim().toUpperCase();
	const currency = marketPolicy?.currency?.trim().toUpperCase();
	const benchmarkAssetID = marketPolicy?.policy?.benchmark_id?.trim();
	const policyVersion = marketPolicy?.policy?.version?.trim();
	if (!assetID.trim() || !market || !currency || !benchmarkAssetID || !policyVersion || Number.isNaN(validFrom.getTime())) return "";
	return JSON.stringify({
		scope_type: "market", scope_id: market, subject_market: market, subject_currency: currency,
		benchmark_asset_id: benchmarkAssetID, policy_version: policyVersion, valid_from: validFrom.toISOString(),
		source_name: "", source_document_id: "", source_url: "", mapping_reason: "", approved_by: "",
		metadata: { approval_mode: "human", draft_for_asset_id: assetID.trim() },
	}, null, 2);
}

const licensedTotalReturnVendorCodes: Record<string, string[]> = {
	"index:CSI:H00300": ["H00300", ".CSIH00300", "CSIR0300", "CSI 300 Total Return Index"],
	"index:HSI:HSIDV": ["HSIDV", ".HSIDV", "HSIRH", "HSIRH.HI", "Hang Seng Index Gross Total Return Index"],
};

export function licensedBenchmarkImportTemplate(benchmarkAssetID: string) {
	const canonical = benchmarkAssetID.trim();
	const vendorCode = canonical === "index:CSI:H00300" ? "H00300" : canonical === "index:HSI:HSIDV" ? ".HSIDV" : "";
	if (!vendorCode) return "";
	return JSON.stringify({
		vendor_code: vendorCode, source_name: "", source_document_id: "", source_url: "",
		license_reference: "", approved_by: "",
	}, null, 2);
}

export function licensedBenchmarkImportBody(benchmarkAssetID: string, metadataJSON: string, observationsText: string) {
	const canonical = benchmarkAssetID.trim();
	const allowedCodes = licensedTotalReturnVendorCodes[canonical];
	if (!allowedCodes) throw new Error("当前策略基准不是可持牌导入的规范中证/恒生总回报指数");
	let metadata: Record<string, unknown>;
	try {
		const parsed = JSON.parse(metadataJSON) as unknown;
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error();
		metadata = parsed as Record<string, unknown>;
	} catch {
		throw new Error("授权信息必须是有效 JSON 对象");
	}
	const required = ["vendor_code", "source_name", "source_document_id", "source_url", "license_reference", "approved_by"] as const;
	const normalized = Object.fromEntries(required.map((field) => [field, String(metadata[field] || "").trim()])) as Record<(typeof required)[number], string>;
	for (const field of required) {
		if (!normalized[field]) throw new Error(`${field} 不能为空`);
	}
	if (!allowedCodes.some((value) => value.toLowerCase() === normalized.vendor_code.toLowerCase())) throw new Error("vendor_code 与当前规范基准身份不一致");
	try {
		const sourceURL = new URL(normalized.source_url);
		if (sourceURL.protocol !== "https:") throw new Error();
	} catch {
		throw new Error("source_url 必须是绝对 HTTPS 地址");
	}

	type Observation = { session_date: string; adjusted_close: number };
	let rawObservations: Array<Record<string, unknown>>;
	const trimmed = observationsText.trim();
	if (!trimmed) throw new Error("必须提供持牌总回报观测");
	if (trimmed.startsWith("[")) {
		try {
			const parsed = JSON.parse(trimmed) as unknown;
			if (!Array.isArray(parsed)) throw new Error();
			rawObservations = parsed as Array<Record<string, unknown>>;
		} catch {
			throw new Error("观测 JSON 必须是数组");
		}
	} else {
		const lines = trimmed.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
		if (lines[0]?.toLowerCase().replace(/\s/g, "") === "session_date,adjusted_close") lines.shift();
		rawObservations = lines.map((line) => {
			const cells = line.split(",").map((cell) => cell.trim());
			if (cells.length !== 2) throw new Error("CSV 每行必须只有 session_date,adjusted_close 两列");
			return { session_date: cells[0], adjusted_close: cells[1] };
		});
	}
	if (rawObservations.length < 1 || rawObservations.length > 1000) throw new Error("一次必须提供 1—1000 条观测");
	const seen = new Set<string>();
	const observations: Observation[] = rawObservations.map((item) => {
		if (!item || typeof item !== "object" || Array.isArray(item)) throw new Error("每条观测必须是对象");
		const sessionDate = String(item.session_date || "").trim();
		const parsedDate = new Date(`${sessionDate}T00:00:00.000Z`);
		if (!/^\d{4}-\d{2}-\d{2}$/.test(sessionDate) || Number.isNaN(parsedDate.getTime()) || parsedDate.toISOString().slice(0, 10) !== sessionDate) throw new Error("session_date 必须是有效 YYYY-MM-DD 日期");
		if (seen.has(sessionDate)) throw new Error("同一导入中 session_date 不能重复");
		seen.add(sessionDate);
		const adjustedClose = Number(item.adjusted_close);
		if (!Number.isFinite(adjustedClose) || adjustedClose <= 0) throw new Error("adjusted_close 必须是正有限数");
		return { session_date: sessionDate, adjusted_close: adjustedClose };
	});
	observations.sort((left, right) => left.session_date.localeCompare(right.session_date));
	return { ...normalized, observations };
}

const fundamentalBundleEndpoints = [
	{ key: "fundamentals", route: "fundamentals", label: "财务快照" },
	{ key: "preparation", route: "fundamental-research", suffix: "/preparation", label: "事实准备包" },
	{ key: "analystEvidence", route: "analyst-evidence", label: "分析师证据" },
	{ key: "prices", route: "market-prices", query: "price_field=adjusted_close&limit=20", label: "复权价格" },
	{ key: "tradability", route: "market-tradability", query: "limit=100", label: "可交易状态" },
	{ key: "consensus", route: "consensus", label: "一致预期" },
	{ key: "guidance", route: "consensus", suffix: "/guidance", label: "管理层指引" },
	{ key: "guidanceSources", route: "consensus", suffix: "/guidance-sources", label: "披露候选" },
	{ key: "forecasts", route: "forecasts", label: "预测版本" },
	{ key: "valuations", route: "valuations", label: "估值运行" },
	{ key: "ratings", route: "ratings", label: "基本面评级" },
	{ key: "predictions", route: "predictions", label: "短期预测" },
	{ key: "marketPolicy", route: "market-policies", label: "市场策略" },
	{ key: "benchmarkMapping", route: "benchmark-mappings", label: "PIT 基准映射" },
	{ key: "schedule", route: "fundamental-research", suffix: "/schedule", label: "定时研究" },
] as const;

type FundamentalBundleRead = { bundle: FundamentalBundle; warnings: string[] };

async function responseDetail(response: Response) {
	const payload = await response.json().catch(() => ({})) as { detail?: string };
	return payload.detail || `HTTP ${response.status}`;
}

export async function resolveFundamentalAssetID(apiBase: string, value: string) {
	const term = fundamentalAssetSearchTerm(value);
	if (!term) throw new Error("请输入资产代码、名称或规范资产 ID");
	const params = new URLSearchParams({ q: term, limit: "50", active: "true" });
	const response = await fetch(`${apiBase}/api/v1/asset-universe?${params}`);
	if (!response.ok) throw new Error(`资产解析失败：${await responseDetail(response)}`);
	const payload = await response.json() as { items?: UniverseAsset[] };
	return selectFundamentalAssetCandidate(value, payload.items || []).asset_id;
}

export async function readFundamentalBundle(apiBase: string, canonical: string): Promise<FundamentalBundleRead> {
	const path = encodeURIComponent(canonical);
	const results = await Promise.all(fundamentalBundleEndpoints.map(async (item) => {
		const url = `${apiBase}/go/${item.route}/${path}${"suffix" in item ? item.suffix : ""}?${"query" in item ? item.query : "limit=20"}`;
		try {
			const response = await fetch(url);
			if (!response.ok) throw new Error(await responseDetail(response));
			return { key: item.key, value: await response.json(), warning: "" };
		} catch (error) {
			return { key: item.key, value: undefined, warning: `${item.label}：${error instanceof Error ? error.message : "读取失败"}` };
		}
	}));
	const bundle: FundamentalBundle = {};
	const warnings: string[] = [];
	for (const result of results) {
		if (result.warning) warnings.push(result.warning);
		else (bundle as Record<string, unknown>)[result.key] = result.value;
	}
	if (Object.keys(bundle).length === 0) throw new Error("所有基本面数据接口均读取失败");
	return { bundle, warnings };
}

export function buildFundamentalPreparationDrafts(canonical: string, source: FundamentalBundle, evidenceType: AnalystEvidenceType, now = new Date()) {
	const benchmarkID = source.marketPolicy?.policy?.benchmark_id?.trim() || "";
	const analystEvidence = analystEvidenceTemplate(canonical, evidenceType, benchmarkID);
	const benchmarkMapping = benchmarkMappingDraftJSON(canonical, source.marketPolicy, now);
	const licensedBenchmarkMetadata = licensedBenchmarkImportTemplate(benchmarkID);
	const tradabilityMetadata = tradabilityImportTemplate(canonical);
	const workflow = source.preparation?.workflow_template ? JSON.stringify(source.preparation.workflow_template, null, 2) : "";
	const generated: string[] = [];
	if (analystEvidence) generated.push("分析师证据模板");
	if (benchmarkMapping) generated.push("PIT 基准映射草稿");
	if (licensedBenchmarkMetadata) generated.push("持牌总回报模板");
	if (tradabilityMetadata) generated.push("可交易状态模板");
	if (workflow) generated.push("财务事实研究草稿");
	return { analystEvidence, benchmarkMapping, licensedBenchmarkMetadata, tradabilityMetadata, workflow, generated };
}

type FundamentalPreparationTask = { label: string; taskID: string };

async function waitForFundamentalPreparationTask(apiBase: string, task: FundamentalPreparationTask, onPoll?: () => Promise<void>) {
	for (let attempt = 0; attempt < 2100; attempt += 1) {
		const response = await fetch(`${apiBase}/api/v1/tasks/${encodeURIComponent(task.taskID)}`);
		if (!response.ok) throw new Error(`${task.label}状态读取失败：${await responseDetail(response)}`);
		const payload = await response.json() as { state?: string; error?: string };
		if (onPoll) await onPoll();
		const state = String(payload.state || "PENDING").toUpperCase();
		if (state === "COMPLETED" || state === "SUCCESS" || state === "SUCCEEDED") return;
		if (["FAILED", "CANCELLED", "CANCELED"].includes(state)) throw new Error(`${task.label}失败：${payload.error || state}`);
		await new Promise((resolve) => globalThis.setTimeout(resolve, 1000));
	}
	throw new Error(`${task.label}等待超过 35 分钟；任务仍在后台运行`);
}

export function FundamentalResearchPage({ apiBase }: { apiBase: string }) {
  const [assetID, setAssetID] = useState("AAPL");
  const [bundle, setBundle] = useState<FundamentalBundle>({});
  const [message, setMessage] = useState("输入代码、名称或规范资产 ID 后读取；缺失字段保持为空，不按零处理。");
  const [loading, setLoading] = useState(false);
  const [workflowJSON, setWorkflowJSON] = useState("");
  const [scheduleJSON, setScheduleJSON] = useState("");
	const [analystEvidenceJSON, setAnalystEvidenceJSON] = useState("");
	const [analystEvidenceType, setAnalystEvidenceType] = useState<AnalystEvidenceType>("forecast_assumption");
	const [analystEvidencePreview, setAnalystEvidencePreview] = useState<AnalystEvidencePreview>();
	const [analystEvidenceConfirmed, setAnalystEvidenceConfirmed] = useState(false);
	const [analystEvidenceRequestID, setAnalystEvidenceRequestID] = useState(() => globalThis.crypto?.randomUUID?.() || `analyst-evidence-${Date.now()}`);
	const [workflowPreview, setWorkflowPreview] = useState<WorkflowPreview>();
	const [workflowConfirmed, setWorkflowConfirmed] = useState(false);
	const [schedulePreview, setSchedulePreview] = useState<SchedulePreview>();
	const [scheduleConfirmed, setScheduleConfirmed] = useState(false);
	const [benchmarkMappingJSON, setBenchmarkMappingJSON] = useState("");
	const [benchmarkMappingRequestID, setBenchmarkMappingRequestID] = useState(() => globalThis.crypto?.randomUUID?.() || `benchmark-mapping-${Date.now()}`);
	const [licensedBenchmarkMetadataJSON, setLicensedBenchmarkMetadataJSON] = useState("");
	const [licensedBenchmarkObservations, setLicensedBenchmarkObservations] = useState("");
	const [licensedBenchmarkRequestID, setLicensedBenchmarkRequestID] = useState(() => globalThis.crypto?.randomUUID?.() || `licensed-benchmark-${Date.now()}`);
	const [licensedBenchmarkConfirmed, setLicensedBenchmarkConfirmed] = useState(false);
	const [licensedBenchmarkReceipts, setLicensedBenchmarkReceipts] = useState<LicensedBenchmarkImportReceipt[]>([]);
	const [licensedBenchmarkAuditLoaded, setLicensedBenchmarkAuditLoaded] = useState(false);
	const [tradabilityMetadataJSON, setTradabilityMetadataJSON] = useState("");
	const [tradabilityObservations, setTradabilityObservations] = useState("");
	const [tradabilityRequestID, setTradabilityRequestID] = useState(() => globalThis.crypto?.randomUUID?.() || `tradability-${Date.now()}`);
	const [tradabilityPreview, setTradabilityPreview] = useState<TradabilityImportPreview>();
	const [tradabilityConfirmed, setTradabilityConfirmed] = useState(false);
	const [tradabilityReceipts, setTradabilityReceipts] = useState<TradabilityImportReceipt[]>([]);
	const [tradabilityAuditLoaded, setTradabilityAuditLoaded] = useState(false);
	const [guidanceReviewJSON, setGuidanceReviewJSON] = useState("");
	const [guidanceReviewRequestID, setGuidanceReviewRequestID] = useState(() => globalThis.crypto?.randomUUID?.() || `guidance-review-${Date.now()}`);
  const [scheduleRequestID, setScheduleRequestID] = useState(() => globalThis.crypto?.randomUUID?.() || `fundamental-schedule-${Date.now()}`);
	const [autoPreparing, setAutoPreparing] = useState(false);
	const [aiPreparation, setAIPreparation] = useState<FundamentalAIPreparation>();
	function applyBundleRead(canonical: string, result: FundamentalBundleRead, completedMessage = "") {
		setAssetID(canonical);
		setBundle(result.bundle);
		const warningText = result.warnings.length ? ` 部分数据未就绪：${result.warnings.join("；")}。` : "";
		setMessage(`${completedMessage || `已解析为 ${canonical} 并读取可用数据。`}${warningText}`);
	}
  async function load(event?: FormEvent, completedMessage = "") {
    event?.preventDefault();
		const input = assetID.trim();
		if (!input) return;
		setLicensedBenchmarkReceipts([]); setLicensedBenchmarkAuditLoaded(false);
		setTradabilityReceipts([]); setTradabilityAuditLoaded(false); setTradabilityPreview(undefined); setTradabilityConfirmed(false);
		setAnalystEvidencePreview(undefined); setAnalystEvidenceConfirmed(false); setWorkflowPreview(undefined); setWorkflowConfirmed(false); setSchedulePreview(undefined); setScheduleConfirmed(false);
    setLoading(true); setMessage("");
    try {
			const canonical = await resolveFundamentalAssetID(apiBase, input);
			applyBundleRead(canonical, await readFundamentalBundle(apiBase, canonical), completedMessage);
    } catch (error) {
			setBundle({});
      setMessage(`${completedMessage ? `${completedMessage} ` : ""}读取刷新失败：${error instanceof Error ? error.message : "未知错误"}`);
    } finally { setLoading(false); }
	}
	async function oneClickPrepare() {
		const input = assetID.trim();
		if (!input || loading) return;
		setLoading(true); setAutoPreparing(true); setMessage("AI 研究 1/3：正在解析规范资产…");
		setLicensedBenchmarkReceipts([]); setLicensedBenchmarkAuditLoaded(false);
		setTradabilityReceipts([]); setTradabilityAuditLoaded(false);
		try {
			const canonical = await resolveFundamentalAssetID(apiBase, input);
			setAssetID(canonical);
			const encoded = encodeURIComponent(canonical);
			setMessage("AI 研究 2/3：数据同步、本地搜索、原文抓取、Ollama 推理和政策校验正在服务端执行…");
			const requestID = globalThis.crypto?.randomUUID?.() || `fundamental-ai-${Date.now()}`;
			const response = await fetch(`${apiBase}/go/fundamental-research/${encoded}/ai-prepare`, { method: "POST", headers: { "Idempotency-Key": requestID } });
			const queued = await response.json().catch(() => ({})) as { task_id?: string; run_id?: string; detail?: string };
			if (!response.ok) throw new Error(queued.detail || `HTTP ${response.status}`);
			if (!queued.task_id) throw new Error("服务端未返回 AI 任务 ID");
			await waitForFundamentalPreparationTask(apiBase, { label: "AI 研究", taskID: queued.task_id }, async () => {
				try {
					const progressResponse = await fetch(`${apiBase}/go/fundamental-research/${encoded}/ai-preparation`);
					if (!progressResponse.ok) return;
					const progress = await progressResponse.json() as FundamentalAIPreparation;
					setAIPreparation(progress);
					const stage = progress.run?.stage || "queued";
					setMessage(`AI 研究进度：${fundamentalAIStageLabels[stage] || stage}…`);
				} catch {
					// Task polling remains authoritative when the optional progress read is transiently unavailable.
				}
			});
			setMessage("AI 研究 3/3：正在读取来源、候选和政策结果…");
			const aiResponse = await fetch(`${apiBase}/go/fundamental-research/${encoded}/ai-preparation`);
			const ai = await aiResponse.json().catch(() => ({})) as FundamentalAIPreparation & { detail?: string };
			if (!aiResponse.ok) throw new Error(ai.detail || `HTTP ${aiResponse.status}`);
			setAIPreparation(ai);
			const refreshed = await readFundamentalBundle(apiBase, canonical);
			setBundle(refreshed.bundle);
			const sources = ai.sources?.filter((item) => item.retrieval_status === "available").length || 0;
			const approved = ai.candidates?.filter((item) => !!item.approved_evidence_id).length || 0;
			const blockers = ai.run?.blockers || [];
			setMessage(`AI 研究${ai.run?.status === "completed" ? "完成" : "已结束"}：${canonical}；可核验原文 ${sources} 份，政策放行证据 ${approved} 条。${blockers.length ? ` 仍有阻塞：${blockers.join("、")}。` : ""}${refreshed.warnings.length ? ` 读取警告：${refreshed.warnings.join("；")}。` : ""}`);
		} catch (error) {
			setMessage(`AI 研究失败：${error instanceof Error ? error.message : "未知错误"}`);
		} finally {
			setAutoPreparing(false); setLoading(false);
		}
	}
	function resetAnalystEvidenceApproval() {
		setAnalystEvidencePreview(undefined);
		setAnalystEvidenceConfirmed(false);
		setAnalystEvidenceRequestID(globalThis.crypto?.randomUUID?.() || `analyst-evidence-${Date.now()}`);
	}
	function loadAnalystEvidenceTemplate() {
		const template = analystEvidenceTemplate(assetID, analystEvidenceType, bundle.marketPolicy?.policy?.benchmark_id || "");
		if (!template) {
			setMessage("请选择受支持的分析师证据类型并先读取规范资产。");
			return;
		}
		setAnalystEvidenceJSON(template);
		resetAnalystEvidenceApproval();
		setMessage(`已生成${analystEvidenceTypeLabels[analystEvidenceType]}模板；所有数值、时点、来源、理由和批准人都保持待人工填写。`);
	}
	function previewAnalystEvidence() {
		try {
			const preview = analystEvidenceBody(assetID, analystEvidenceJSON);
			setAnalystEvidencePreview(preview);
			setAnalystEvidenceConfirmed(false);
			setMessage(`分析师证据预校验通过：${analystEvidenceTypeLabels[preview.evidenceType]} · ${preview.valueSummary} · 可得 ${new Date(preview.availableAt).toLocaleString("zh-CN")}。尚未提交。`);
		} catch (error) {
			setAnalystEvidencePreview(undefined);
			setAnalystEvidenceConfirmed(false);
			setMessage(`分析师证据预校验失败：${error instanceof Error ? error.message : "JSON 无效"}`);
		}
	}
	async function registerAnalystEvidence() {
		const canonical = assetID.trim();
		if (!canonical || !analystEvidencePreview || !analystEvidenceConfirmed) return;
		setLoading(true); setMessage("");
		try {
			const body = analystEvidenceBody(canonical, analystEvidenceJSON).body;
			const response = await fetch(`${apiBase}/go/analyst-evidence`, { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": analystEvidenceRequestID }, body: JSON.stringify(body) });
			const payload = await response.json() as { id?: string; detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			const completedMessage = `分析师证据已不可变登记：${payload.id || "已保存"}。请将该 ID 引用到对应研究输入。`;
			setAnalystEvidenceJSON("");
			resetAnalystEvidenceApproval();
			await load(undefined, completedMessage);
		} catch (error) {
			setMessage(`分析师证据登记失败：${error instanceof Error ? error.message : "JSON 或请求无效"}`);
		} finally { setLoading(false); }
	}
	function loadBenchmarkMappingTemplate() {
		const draft = benchmarkMappingDraftJSON(assetID, bundle.marketPolicy);
		if (!draft) {
			setMessage("当前资产没有完整的市场、币种、策略版本或规范基准身份，不能生成审批草稿。");
			return;
		}
		setBenchmarkMappingJSON(draft);
		setBenchmarkMappingRequestID(globalThis.crypto?.randomUUID?.() || `benchmark-mapping-${Date.now()}`);
		setMessage("已生成从当前时点生效的市场级基准映射草稿；请补充真实来源、理由和批准人。系统不会自动提交或倒填历史。");
	}
	async function approveBenchmarkMapping() {
		const canonical = assetID.trim();
		if (!canonical || !benchmarkMappingJSON.trim()) return;
		setLoading(true); setMessage("");
		try {
			const body = JSON.parse(benchmarkMappingJSON) as Record<string, unknown>;
			const expectedMarket = bundle.marketPolicy?.market?.trim().toUpperCase();
			const expectedCurrency = bundle.marketPolicy?.currency?.trim().toUpperCase();
			const expectedBenchmark = bundle.marketPolicy?.policy?.benchmark_id?.trim();
			if (String(body.subject_market || "").trim().toUpperCase() !== expectedMarket) throw new Error("subject_market 必须与当前资产市场一致");
			if (String(body.subject_currency || "").trim().toUpperCase() !== expectedCurrency) throw new Error("subject_currency 必须与当前资产币种一致");
			if (String(body.benchmark_asset_id || "").trim() !== expectedBenchmark) throw new Error("benchmark_asset_id 必须与当前市场策略一致");
			for (const field of ["source_name", "source_document_id", "mapping_reason", "approved_by"] as const) {
				if (!String(body[field] || "").trim()) throw new Error(`${field} 不能为空`);
			}
			const response = await fetch(`${apiBase}/go/benchmark-mappings`, { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": benchmarkMappingRequestID }, body: JSON.stringify(body) });
			const payload = await response.json() as { created?: boolean; mapping?: { id?: string }; detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			const completedMessage = `PIT 基准映射${payload.created ? "已不可变批准" : "已幂等读回"}：${payload.mapping?.id || "已保存"}。不会自动生成历史映射。`;
			setBenchmarkMappingRequestID(globalThis.crypto?.randomUUID?.() || `benchmark-mapping-${Date.now()}`);
			await load(undefined, completedMessage);
		} catch (error) {
			setMessage(`基准映射批准失败：${error instanceof Error ? error.message : "JSON 或请求无效"}`);
		} finally { setLoading(false); }
	}
	async function syncCanonicalBenchmarkPrice() {
		const benchmarkAssetID = bundle.marketPolicy?.policy?.benchmark_id?.trim();
		if (!benchmarkAssetID) return;
		setLoading(true); setMessage("");
		try {
			const response = await fetch(`${apiBase}/go/market-prices/${encodeURIComponent(benchmarkAssetID)}/sync?lookback_days=14`, { method: "POST" });
			const payload = await response.json() as { task_id?: string; detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			setMessage(`规范基准 ${benchmarkAssetID} 的复权行情同步任务已排队：${payload.task_id || "等待 Worker"}。该操作不批准基准映射。`);
		} catch (error) {
			setMessage(`规范基准行情同步失败：${error instanceof Error ? error.message : "未知错误"}`);
		} finally { setLoading(false); }
	}
	function loadLicensedBenchmarkTemplate() {
		const benchmarkAssetID = bundle.marketPolicy?.policy?.benchmark_id?.trim() || "";
		const template = licensedBenchmarkImportTemplate(benchmarkAssetID);
		if (!template) {
			setMessage("当前策略基准不是可持牌导入的中证 H00300 或恒生 HSIDV 总回报指数。");
			return;
		}
		setLicensedBenchmarkMetadataJSON(template);
		setLicensedBenchmarkObservations("session_date,adjusted_close\n");
		setLicensedBenchmarkConfirmed(false);
		setLicensedBenchmarkRequestID(globalThis.crypto?.randomUUID?.() || `licensed-benchmark-${Date.now()}`);
		setMessage(`已生成 ${benchmarkAssetID} 持牌导入模板；只可填写真实授权来源、许可证引用、审批人和原始总回报观测。`);
	}
	async function importLicensedBenchmarkPrices() {
		const benchmarkAssetID = bundle.marketPolicy?.policy?.benchmark_id?.trim() || "";
		if (!benchmarkAssetID || !licensedBenchmarkMetadataJSON.trim() || !licensedBenchmarkObservations.trim() || !licensedBenchmarkConfirmed) return;
		setLoading(true); setMessage("");
		try {
			const body = licensedBenchmarkImportBody(benchmarkAssetID, licensedBenchmarkMetadataJSON, licensedBenchmarkObservations);
			const response = await fetch(`${apiBase}/go/market-prices/${encodeURIComponent(benchmarkAssetID)}/licensed-import`, {
				method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": licensedBenchmarkRequestID }, body: JSON.stringify(body),
			});
			const payload = await response.json() as { created?: boolean; detail?: string; receipt?: LicensedBenchmarkImportReceipt };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			setMessage(`持牌总回报行情${payload.created ? "已不可变导入" : "已幂等读回"}：${payload.receipt?.observation_count ?? 0} 条，首次新增 ${payload.receipt?.inserted_count ?? 0} 条，回执 ${payload.receipt?.id || "已保存"}。该操作不批准 PIT 映射。`);
			if (payload.receipt?.id) setLicensedBenchmarkReceipts((items) => [payload.receipt!, ...items.filter((item) => item.id !== payload.receipt?.id)]);
			setLicensedBenchmarkAuditLoaded(true);
			setLicensedBenchmarkObservations("");
			setLicensedBenchmarkConfirmed(false);
			setLicensedBenchmarkRequestID(globalThis.crypto?.randomUUID?.() || `licensed-benchmark-${Date.now()}`);
		} catch (error) {
			setMessage(`持牌总回报行情导入失败：${error instanceof Error ? error.message : "JSON、CSV 或请求无效"}`);
		} finally { setLoading(false); }
	}
	async function loadLicensedBenchmarkReceipts() {
		const benchmarkAssetID = bundle.marketPolicy?.policy?.benchmark_id?.trim() || "";
		if (!benchmarkAssetID || !licensedBenchmarkImportTemplate(benchmarkAssetID)) return;
		setLoading(true); setMessage("");
		try {
			const response = await fetch(`${apiBase}/go/market-prices/${encodeURIComponent(benchmarkAssetID)}/licensed-imports?limit=20`);
			const payload = await response.json() as { items?: LicensedBenchmarkImportReceipt[]; detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			const items = payload.items || [];
			setLicensedBenchmarkReceipts(items); setLicensedBenchmarkAuditLoaded(true);
			setMessage(items.length ? `已读取 ${benchmarkAssetID} 最近 ${items.length} 份管理员导入回执。` : `${benchmarkAssetID} 尚无持牌导入回执；系统没有自动导入数据。`);
		} catch (error) {
			setMessage(`持牌导入回执读取失败：${error instanceof Error ? error.message : "未知错误"}`);
		} finally { setLoading(false); }
	}
	function resetTradabilityApproval() {
		setTradabilityPreview(undefined);
		setTradabilityConfirmed(false);
		setTradabilityRequestID(globalThis.crypto?.randomUUID?.() || `tradability-${Date.now()}`);
	}
	function loadTradabilityTemplate() {
		const canonical = assetID.trim();
		const template = tradabilityImportTemplate(canonical);
		if (!template) {
			setMessage("可交易状态导入只支持规范股票或 ETF 资产。");
			return;
		}
		setTradabilityMetadataJSON(template);
		setTradabilityObservations("session_date,source_observed_at,status\n");
		resetTradabilityApproval();
		setMessage(`已生成 ${canonical} 可交易状态导入模板；请上传或粘贴真实授权来源文件，再执行本地预校验。`);
	}
	async function loadTradabilityFile(file?: File) {
		if (!file) return;
		if (file.size > 1024 * 1024) {
			setMessage("可交易状态文件超过 1 MiB；单次最多 1000 条，请拆分后导入。");
			return;
		}
		try {
			const body = await file.text();
			setTradabilityObservations(body);
			resetTradabilityApproval();
			setMessage(`已在浏览器读取 ${file.name}；尚未上传到服务器，请继续预校验。`);
		} catch {
			setMessage("无法读取可交易状态文件，请改用 UTF-8 CSV/JSON 或直接粘贴内容。");
		}
	}
	function previewTradabilityImport() {
		try {
			const body = tradabilityImportBody(assetID, tradabilityMetadataJSON, tradabilityObservations);
			const statusCounts: Record<string, number> = {};
			for (const item of body.observations) statusCounts[item.status] = (statusCounts[item.status] || 0) + 1;
			const preview = {
				observationCount: body.observations.length,
				sessionStart: body.observations[0]?.session_date || "",
				sessionEnd: body.observations.at(-1)?.session_date || "",
				statusCounts,
			};
			setTradabilityPreview(preview);
			setTradabilityConfirmed(false);
			setMessage(`预校验通过：${preview.observationCount} 条，覆盖 ${preview.sessionStart} 至 ${preview.sessionEnd}。数据尚未发送，请核对状态分布后人工确认。`);
		} catch (error) {
			setTradabilityPreview(undefined);
			setTradabilityConfirmed(false);
			setMessage(`可交易状态预校验失败：${error instanceof Error ? error.message : "JSON、CSV 或授权信息无效"}`);
		}
	}
	async function importTradability() {
		const canonical = assetID.trim();
		if (!canonical || !tradabilityPreview || !tradabilityConfirmed) return;
		setLoading(true); setMessage("");
		try {
			const body = tradabilityImportBody(canonical, tradabilityMetadataJSON, tradabilityObservations);
			const response = await fetch(`${apiBase}/go/market-tradability/${encodeURIComponent(canonical)}/import`, {
				method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": tradabilityRequestID }, body: JSON.stringify(body),
			});
			const payload = await response.json() as { created?: boolean; detail?: string; receipt?: TradabilityImportReceipt };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			if (payload.receipt?.id) setTradabilityReceipts((items) => [payload.receipt!, ...items.filter((item) => item.id !== payload.receipt?.id)]);
			setTradabilityAuditLoaded(true);
			setTradabilityObservations("");
			setTradabilityPreview(undefined);
			setTradabilityConfirmed(false);
			setTradabilityRequestID(globalThis.crypto?.randomUUID?.() || `tradability-${Date.now()}`);
			let refreshed = false;
			try {
				const publicResponse = await fetch(`${apiBase}/go/market-tradability/${encodeURIComponent(canonical)}?limit=100`);
				if (publicResponse.ok) {
					const tradability = await publicResponse.json() as FundamentalBundle["tradability"];
					setBundle((current) => ({ ...current, tradability }));
					refreshed = true;
				}
			} catch { /* The immutable receipt above remains authoritative even if the public refresh fails. */ }
			setMessage(`可交易状态${payload.created ? "已不可变导入" : "已幂等读回"}：${payload.receipt?.observation_count ?? 0} 条，首次新增 ${payload.receipt?.inserted_count ?? 0} 条，回执 ${payload.receipt?.id || "已保存"}。不会自动运行结果评价或评级。${refreshed ? "" : "公共决议刷新失败，请重新读取标的。"}`);
		} catch (error) {
			setMessage(`可交易状态导入失败：${error instanceof Error ? error.message : "请求无效"}`);
		} finally { setLoading(false); }
	}
	async function loadTradabilityReceipts() {
		const canonical = assetID.trim();
		if (!canonical || !tradabilityImportTemplate(canonical)) return;
		setLoading(true); setMessage("");
		try {
			const response = await fetch(`${apiBase}/go/market-tradability/${encodeURIComponent(canonical)}/imports?limit=20`);
			const payload = await response.json() as { items?: TradabilityImportReceipt[]; detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			const items = payload.items || [];
			setTradabilityReceipts(items); setTradabilityAuditLoaded(true);
			setMessage(items.length ? `已读取 ${canonical} 最近 ${items.length} 份可交易状态导入回执。` : `${canonical} 尚无可交易状态导入回执；没有回执不能推断已有覆盖。`);
		} catch (error) {
			setMessage(`可交易状态回执读取失败：${error instanceof Error ? error.message : "未知错误"}`);
		} finally { setLoading(false); }
	}
	async function syncMarketPrices() {
		const canonical = assetID.trim();
		if (!canonical) return;
		setLoading(true); setMessage("");
		try {
			const response = await fetch(`${apiBase}/go/market-prices/${encodeURIComponent(canonical)}/sync?lookback_days=14`, { method: "POST" });
			const payload = await response.json() as { task_id?: string; detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			setMessage(`真实复权价格同步任务已排队：${payload.task_id || "等待 Worker"}。完成后重新读取即可引用不可变价格证据。`);
		} catch (error) {
			setMessage(`复权价格同步失败：${error instanceof Error ? error.message : "未知错误"}`);
		} finally { setLoading(false); }
	}
	async function syncConsensus() {
		const canonical = assetID.trim();
		if (!canonical) return;
		setLoading(true); setMessage("");
		try {
			const response = await fetch(`${apiBase}/go/consensus/${encodeURIComponent(canonical)}/sync?limit=10`, { method: "POST" });
			const payload = await response.json() as { task_id?: string; detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			setMessage(`一致预期首次观测任务已排队：${payload.task_id || "等待 Worker"}。完成后重新读取即可查看；不会倒填历史。`);
		} catch (error) {
			setMessage(`一致预期同步失败：${error instanceof Error ? error.message : "未知错误"}`);
		} finally { setLoading(false); }
	}
	async function syncGuidanceSources() {
		const canonical = assetID.trim();
		if (!canonical) return;
		setLoading(true); setMessage("");
		try {
			const response = await fetch(`${apiBase}/go/consensus/${encodeURIComponent(canonical)}/guidance-sources/sync?limit=40`, { method: "POST" });
			const payload = await response.json() as { task_id?: string; detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			setMessage(`SEC 官方披露候选同步任务已排队：${payload.task_id || "等待 Worker"}。候选不会自动变成管理层指引。`);
		} catch (error) {
			setMessage(`SEC 披露同步失败：${error instanceof Error ? error.message : "未知错误"}`);
		} finally { setLoading(false); }
	}
	async function reviewGuidanceSource() {
		const canonical = assetID.trim();
		if (!canonical || !guidanceReviewJSON.trim()) return;
		setLoading(true); setMessage("");
		try {
			const body = JSON.parse(guidanceReviewJSON) as Record<string, unknown>;
			const sourceDocumentID = typeof body.source_document_id === "string" ? body.source_document_id.trim() : "";
			if (!sourceDocumentID) throw new Error("source_document_id 不能为空");
			delete body.source_document_id;
			const response = await fetch(`${apiBase}/go/consensus/${encodeURIComponent(canonical)}/guidance-sources/${encodeURIComponent(sourceDocumentID)}/reviews`, { method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": guidanceReviewRequestID }, body: JSON.stringify(body) });
			const payload = await response.json() as { detail?: string };
			if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
			setGuidanceReviewRequestID(globalThis.crypto?.randomUUID?.() || `guidance-review-${Date.now()}`);
			await load(undefined, "披露候选核验已保存；只有 confirmed_guidance 才会生成来源化指引快照。");
		} catch (error) {
			setMessage(`指引核验失败：${error instanceof Error ? error.message : "JSON 或请求无效"}`);
		} finally { setLoading(false); }
	}
	function resetWorkflowApproval() {
		setWorkflowPreview(undefined);
		setWorkflowConfirmed(false);
	}
	function previewWorkflowInput() {
		try {
			const preview = fundamentalWorkflowBody(assetID, workflowJSON);
			setWorkflowPreview(preview);
			setWorkflowConfirmed(false);
			setMessage(`研究输入预校验通过：${preview.snapshotCount} 份财务快照、${preview.assumptionCount} 项批准假设、${preview.dcfScenarioCount + preview.multipleScenarioCount} 个估值情景、${preview.evidenceIDs.length} 个唯一证据引用。服务器仍会逐项核对真实记录和值。`);
		} catch (error) {
			setWorkflowPreview(undefined);
			setWorkflowConfirmed(false);
			setMessage(`研究输入预校验失败：${error instanceof Error ? error.message : "JSON 无效"}`);
		}
	}
  async function runWorkflow() {
    const canonical = assetID.trim();
    if (!canonical || !workflowPreview || !workflowConfirmed) return;
    setLoading(true); setMessage("");
    try {
      const body = fundamentalWorkflowBody(canonical, workflowJSON).body;
      const response = await fetch(`${apiBase}/go/fundamental-research/${encodeURIComponent(canonical)}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      const payload = await response.json() as { status?: string; reason?: string; detail?: string; schedule_draft?: Record<string, unknown>; schedule_draft_controls?: { approval_required?: boolean; automatic_approval?: boolean; runtime_price_field?: string } };
      if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
		const draftJSON = scheduleDraftJSON(payload);
		if (draftJSON) {
			setScheduleJSON(draftJSON);
			setScheduleRequestID(globalThis.crypto?.randomUUID?.() || `fundamental-schedule-${Date.now()}`);
			setSchedulePreview(undefined); setScheduleConfirmed(false);
			await load(undefined, "基本面研究已完成；同源定时计划草稿已载入。请填写 approved_by、预校验并复核后再批准，系统不会自动启用计划。");
		} else {
			await load(undefined, `工作流未生成结论：${payload.reason || payload.status || "数据不足"}`);
		}
    } catch (error) {
      setMessage(`工作流失败：${error instanceof Error ? error.message : "JSON 或请求无效"}`);
    } finally { setLoading(false); }
  }
	function loadPreparationTemplate() {
		const template = bundle.preparation?.workflow_template;
		if (!template) {
			setMessage("当前没有可用的事实准备包；请先同步并读取同一报告期的三张财务表。");
			return;
		}
		setWorkflowJSON(JSON.stringify(template, null, 2));
		resetWorkflowApproval();
		setMessage("已载入事实模板；估值情景、基准预期、原因码和失效规则仍需分析师补充并审核。");
	}
	function resetScheduleApproval() {
		setSchedulePreview(undefined);
		setScheduleConfirmed(false);
		setScheduleRequestID(globalThis.crypto?.randomUUID?.() || `fundamental-schedule-${Date.now()}`);
	}
	function previewScheduleInput() {
		try {
			const preview = fundamentalScheduleBody(assetID, scheduleJSON);
			setSchedulePreview(preview);
			setScheduleConfirmed(false);
			setMessage(`定时计划预校验通过：预测 ${preview.forecastVersionID} · 每 ${preview.cadenceHours} 小时 · 价格最长 ${preview.maxPriceAgeHours} 小时 · 计划最长 ${preview.maxPlanAgeDays} 天。尚未批准。`);
		} catch (error) {
			setSchedulePreview(undefined);
			setScheduleConfirmed(false);
			setMessage(`定时计划预校验失败：${error instanceof Error ? error.message : "JSON 无效"}`);
		}
	}
  async function approveSchedule() {
    const canonical = assetID.trim();
    if (!canonical || !schedulePreview || !scheduleConfirmed) return;
    setLoading(true); setMessage("");
    try {
      const body = fundamentalScheduleBody(canonical, scheduleJSON).body;
      const response = await fetch(`${apiBase}/go/fundamental-research/${encodeURIComponent(canonical)}/schedule`, { method: "PUT", headers: { "Content-Type": "application/json", "Idempotency-Key": scheduleRequestID }, body: JSON.stringify(body) });
      const payload = await response.json() as { detail?: string };
      if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
      resetScheduleApproval();
      await load(undefined, "定时基本面研究计划已批准；系统只复用显式批准的预测和估值参数。");
    } catch (error) {
      setMessage(`计划批准失败：${error instanceof Error ? error.message : "JSON 或请求无效"}`);
    } finally { setLoading(false); }
  }
  async function pauseSchedule() {
    const canonical = assetID.trim();
    if (!canonical) return;
    setLoading(true); setMessage("");
    try {
      const response = await fetch(`${apiBase}/go/fundamental-research/${encodeURIComponent(canonical)}/schedule`, { method: "DELETE" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      await load(undefined, "定时基本面研究计划已暂停。");
    } catch (error) { setMessage(`暂停失败：${error instanceof Error ? error.message : "未知错误"}`); }
    finally { setLoading(false); }
  }
  const rating = bundle.ratings?.items?.[0];
  const prediction = bundle.predictions?.items?.[0];
  const valuation = bundle.valuations?.items?.[0];
  const forecast = bundle.forecasts?.items?.[0];
  const schedule = bundle.schedule?.items?.[0];
	const analystEvidenceItems = bundle.analystEvidence?.items || [];
	const benchmarkResolution = bundle.benchmarkMapping?.resolution;
	const benchmarkMapping = benchmarkResolution?.mapping;
	const canonicalBenchmarkID = bundle.marketPolicy?.policy?.benchmark_id?.trim() || "";
	const licensedBenchmarkAvailable = !!licensedBenchmarkImportTemplate(canonicalBenchmarkID);
	const tradabilityImportAvailable = !!tradabilityImportTemplate(assetID);
	const tradabilityItems = bundle.tradability?.items || [];
	const tradabilitySessions = Object.entries(bundle.tradability?.resolved_by_session || {}).sort(([left], [right]) => right.localeCompare(left));
	const latestTradability = tradabilitySessions[0];
	const tradabilityPreviewSummary = tradabilityPreview ? Object.entries(tradabilityPreview.statusCounts).sort(([left], [right]) => left.localeCompare(right)).map(([status, count]) => `${tradabilityStatusLabel(status)} ${count}`).join(" · ") : "";
	const preparation = bundle.preparation;
	const prices = bundle.prices?.items || [];
	const latestPrice = prices[0];
	const consensus = bundle.consensus;
	const consensusItems = consensus?.items || [];
	const guidance = bundle.guidance;
	const guidanceItems = guidance?.items || [];
	const guidanceSources = bundle.guidanceSources;
	const guidanceSourceItems = guidanceSources?.items || [];
  const valuationRange = valuation?.result?.range;
  const changedAssumptions = Object.entries(rating?.changed_assumptions || {});
  return <section className="app-page fundamental-page">
    <PageHeading eyebrow="FUNDAMENTAL & SIGNAL WORKBENCH" title="基本面评级与短期预测" copy="事件信号、基本面评级和固定期限概率彼此独立；只有通过独立校准的概率才显示数值。" />
    <form className="page-toolbar" onSubmit={load}>
		<input aria-label="资产代码、名称或规范 ID" value={assetID} onChange={(event) => { setAssetID(event.target.value); setBundle({}); setAIPreparation(undefined); setAnalystEvidenceJSON(""); setBenchmarkMappingJSON(""); setLicensedBenchmarkMetadataJSON(""); setLicensedBenchmarkObservations(""); setTradabilityMetadataJSON(""); setTradabilityObservations(""); setWorkflowJSON(""); setScheduleJSON(""); resetTradabilityApproval(); resetAnalystEvidenceApproval(); resetWorkflowApproval(); resetScheduleApproval(); }} />
      <button type="submit" disabled={loading}>{loading ? "读取中…" : "读取"}</button>
		<button type="button" disabled={loading || !assetID.trim()} onClick={() => void oneClickPrepare()}>{autoPreparing ? "AI 研究中…" : "一键 AI 研究"}</button>
		<small>服务端自动完成数据同步、本地搜索、原文抓取、AI 推理、反证和政策校验；外部许可、SEC 身份、自然成熟和最终治理批准不会被伪造或越过。</small>
    </form>
	{aiPreparation && <section className="fundamental-ai-preparation" aria-label="AI 研究进度">
		<header><div><span>FUNDAMENTAL AI · {aiPreparation.run?.policy_version || "fundamental-ai-policy-v1"}</span><h2>{fundamentalAIStageLabels[aiPreparation.run?.stage || ""] || aiPreparation.run?.stage || aiPreparation.status || "尚未运行"}</h2></div><strong>{aiPreparation.run?.status || aiPreparation.status || "not_run"}</strong></header>
		<div className="readiness-outcome-counts"><span>原文 <b>{aiPreparation.sources?.length || 0}</b></span><span>候选 <b>{aiPreparation.candidates?.length || 0}</b></span><span>已放行 <b>{aiPreparation.candidates?.filter((item) => !!item.approved_evidence_id).length || 0}</b></span><span>资料不足 <b>{aiPreparation.candidates?.filter((item) => item.status === "insufficient_data" || item.status === "rejected").length || 0}</b></span></div>
		{!!aiPreparation.run?.blockers?.length && <p className="readiness-outcome-warning">阻塞：{aiPreparation.run.blockers.join("、")}</p>}
		{!!aiPreparation.sources?.length && <details><summary>搜索与原文抓取</summary>{aiPreparation.sources.map((source) => <p key={source.id}>{source.source_class || "public_web"} · {source.retrieval_status || "unknown"} · {source.source_url ? <a href={source.source_url} target="_blank" rel="noreferrer">{source.title || source.source_name || source.source_url}</a> : source.title || "无地址"}{source.content_hash ? ` · ${source.content_hash.slice(0, 12)}` : ""}</p>)}</details>}
		{!!aiPreparation.candidates?.length && <details><summary>AI 建议与政策结果</summary>{aiPreparation.candidates.map((candidate) => <p key={candidate.id}>{candidate.evidence_type || "unknown"} · {candidate.status || "unknown"} · {candidate.title || "未命名"}{candidate.approved_evidence_id ? ` · policy 证据 ${candidate.approved_evidence_id}` : ""}</p>)}</details>}
		<small>Search-MCP 摘要只发现链接，不作证据；AI 不自我批准最终留出集、模型晋级或发布。</small>
	</section>}
	<details className="fundamental-manual-fallback">
		<summary>高级人工兜底工具</summary>
		<div className="integration-editor">
		<label>分析师证据类型<select aria-label="分析师证据类型" value={analystEvidenceType} onChange={(event) => { setAnalystEvidenceType(event.target.value as AnalystEvidenceType); resetAnalystEvidenceApproval(); }}>{analystEvidenceTypes.map((type) => <option key={type} value={type}>{analystEvidenceTypeLabels[type]}</option>)}</select></label>
		<button type="button" disabled={loading || !assetID.trim()} onClick={loadAnalystEvidenceTemplate}>生成分析师证据模板</button>
		<label>分析师证据登记<textarea aria-label="分析师证据 JSON" rows={10} value={analystEvidenceJSON} onChange={(event) => { setAnalystEvidenceJSON(event.target.value); resetAnalystEvidenceApproval(); }} placeholder='先按类型生成模板；所有数值、observed_at、available_at、来源、理由和 approved_by 必须由分析师真实填写。' /></label>
		<button type="button" disabled={loading || !analystEvidenceJSON.trim()} onClick={previewAnalystEvidence}>预校验分析师证据</button>
		{analystEvidencePreview && <small>预览：{analystEvidenceTypeLabels[analystEvidencePreview.evidenceType]} · {analystEvidencePreview.title} · {analystEvidencePreview.valueSummary} · 服务器仍会核对资产和批准时点</small>}
		<label className="licensed-import-confirmation"><input type="checkbox" disabled={!analystEvidencePreview} checked={analystEvidenceConfirmed} onChange={(event) => setAnalystEvidenceConfirmed(event.target.checked)} />我确认数值、来源时点、分析理由和批准身份真实有效，并理解登记后记录不可变</label>
		<button type="button" disabled={loading || !analystEvidencePreview || !analystEvidenceConfirmed} onClick={() => void registerAnalystEvidence()}>登记不可变分析师证据</button>
		<label>PIT 基准映射审批<textarea aria-label="PIT 基准映射 JSON" rows={8} value={benchmarkMappingJSON} onChange={(event) => { setBenchmarkMappingJSON(event.target.value); setBenchmarkMappingRequestID(globalThis.crypto?.randomUUID?.() || `benchmark-mapping-${Date.now()}`); }} placeholder='先生成草稿，再填写 source_name、source_document_id、source_url、mapping_reason 和 approved_by；默认从当前时点生效。' /></label>
		<button type="button" disabled={loading || !bundle.marketPolicy?.policy?.benchmark_id} onClick={loadBenchmarkMappingTemplate}>生成市场级基准映射草稿</button>
		<button type="button" disabled={loading || !benchmarkMappingJSON.trim()} onClick={() => void approveBenchmarkMapping()}>批准不可变 PIT 基准映射</button>
		<button type="button" disabled={loading || !bundle.marketPolicy?.policy?.benchmark_id} onClick={() => void syncCanonicalBenchmarkPrice()}>同步规范基准复权行情</button>
		<small>基准映射必须有真实来源、理由和批准人；草稿默认不回填历史。同步基准行情只写事实，不会自动批准映射。</small>
		<label>持牌总回报授权信息<textarea aria-label="持牌总回报授权信息 JSON" rows={7} value={licensedBenchmarkMetadataJSON} onChange={(event) => { setLicensedBenchmarkMetadataJSON(event.target.value); setLicensedBenchmarkConfirmed(false); setLicensedBenchmarkRequestID(globalThis.crypto?.randomUUID?.() || `licensed-benchmark-${Date.now()}`); }} placeholder="由模板填写 vendor_code、真实来源、文档 ID、HTTPS 地址、许可证引用和批准人。" /></label>
		<label>持牌总回报观测<textarea aria-label="持牌总回报观测 CSV 或 JSON" rows={7} value={licensedBenchmarkObservations} onChange={(event) => { setLicensedBenchmarkObservations(event.target.value); setLicensedBenchmarkConfirmed(false); setLicensedBenchmarkRequestID(globalThis.crypto?.randomUUID?.() || `licensed-benchmark-${Date.now()}`); }} placeholder={'CSV: session_date,adjusted_close\n2026-09-09,12345.67；或粘贴同字段 JSON 数组。'} /></label>
		<button type="button" disabled={loading || !licensedBenchmarkAvailable} onClick={loadLicensedBenchmarkTemplate}>生成持牌总回报导入模板</button>
		<label className="licensed-import-confirmation"><input type="checkbox" checked={licensedBenchmarkConfirmed} onChange={(event) => setLicensedBenchmarkConfirmed(event.target.checked)} />我确认这些观测来自有权使用的规范总回报数据，且审批与许可证引用真实有效</label>
		<button type="button" disabled={loading || !licensedBenchmarkMetadataJSON.trim() || !licensedBenchmarkObservations.trim() || !licensedBenchmarkConfirmed} onClick={() => void importLicensedBenchmarkPrices()}>人工导入持牌总回报行情</button>
		<button type="button" disabled={loading || !licensedBenchmarkAvailable} onClick={() => void loadLicensedBenchmarkReceipts()}>读取持牌导入回执</button>
		<small>只允许有授权、可追溯的 H00300/HSIDV 总回报数据；页面先校验身份、HTTPS、日期、正数和重复日。导入不会自动批准基准映射、生成评级或倒填可获得时间。</small>
		<label>可交易状态授权信息<textarea aria-label="可交易状态授权信息 JSON" rows={7} value={tradabilityMetadataJSON} onChange={(event) => { setTradabilityMetadataJSON(event.target.value); resetTradabilityApproval(); }} placeholder="由模板填写真实来源、文档 ID、无凭据 HTTPS 地址、许可证引用和批准人。" /></label>
		<label>选择可交易状态文件<input aria-label="选择可交易状态文件" type="file" accept=".csv,.json,text/csv,application/json" onChange={(event) => void loadTradabilityFile(event.target.files?.[0])} /></label>
		<label>可交易状态观测<textarea aria-label="可交易状态观测 CSV 或 JSON" rows={8} value={tradabilityObservations} onChange={(event) => { setTradabilityObservations(event.target.value); resetTradabilityApproval(); }} placeholder={'CSV: session_date,source_observed_at,status\n2026-09-09,2026-09-09T20:00:00Z,tradable；状态仅可为 tradable、suspended、limit_up、limit_down、delisted。'} /></label>
		<button type="button" disabled={loading || !tradabilityImportAvailable} onClick={loadTradabilityTemplate}>生成可交易状态导入模板</button>
		<button type="button" disabled={loading || !tradabilityMetadataJSON.trim() || !tradabilityObservations.trim()} onClick={previewTradabilityImport}>预校验可交易状态文件</button>
		{tradabilityPreview && <small>预览：{tradabilityPreview.observationCount} 条 · {tradabilityPreview.sessionStart} 至 {tradabilityPreview.sessionEnd} · {tradabilityPreviewSummary}</small>}
		<label className="licensed-import-confirmation"><input type="checkbox" disabled={!tradabilityPreview} checked={tradabilityConfirmed} onChange={(event) => setTradabilityConfirmed(event.target.checked)} />我已核对预览，并确认状态文件有权使用、来源时点真实、许可证引用和批准人有效</label>
		<button type="button" disabled={loading || !tradabilityPreview || !tradabilityConfirmed} onClick={() => void importTradability()}>人工导入不可变可交易状态</button>
		<button type="button" disabled={loading || !tradabilityImportAvailable} onClick={() => void loadTradabilityReceipts()}>读取可交易状态导入回执</button>
		<small>只支持股票/ETF，单批 1—1000 条；页面拒绝未来日期、重复交易日、未知状态、非 HTTPS 和 URL 凭据。服务器决定 available_at；导入不自动评价结果、生成评级或放开执行。</small>
		<label>无新闻基本面研究输入<textarea aria-label="基本面研究 JSON" rows={10} value={workflowJSON} onChange={(event) => { setWorkflowJSON(event.target.value); resetWorkflowApproval(); }} placeholder='载入事实模板后补充显式批准的 forecast、valuation、rating 输入；不会自动补造假设或价格。' /></label>
		<button type="button" disabled={loading || !preparation?.workflow_template} onClick={loadPreparationTemplate}>载入财务事实模板</button>
		<button type="button" disabled={loading} onClick={() => void syncMarketPrices()}>同步真实复权价格</button>
		<button type="button" disabled={loading} onClick={() => void syncConsensus()}>同步一致预期</button>
		<button type="button" disabled={loading} onClick={() => void syncGuidanceSources()}>同步 SEC 披露候选</button>
		<label>管理层指引人工核验<textarea aria-label="管理层指引核验 JSON" rows={8} value={guidanceReviewJSON} onChange={(event) => { setGuidanceReviewJSON(event.target.value); setGuidanceReviewRequestID(globalThis.crypto?.randomUUID?.() || `guidance-review-${Date.now()}`); }} placeholder='粘贴 source_document_id、decision、reviewed_by；确认指引时另填 guidance、evidence_url、evidence_location、evidence_excerpt。' /></label>
		<button type="button" disabled={loading || !guidanceReviewJSON.trim()} onClick={() => void reviewGuidanceSource()}>保存指引核验</button>
		<button type="button" disabled={loading || !workflowJSON.trim()} onClick={previewWorkflowInput}>预校验研究输入</button>
		{workflowPreview && <small>预览：截至 {new Date(workflowPreview.asOf).toLocaleString("zh-CN")} · {workflowPreview.snapshotCount} 份财务快照 · {workflowPreview.assumptionCount} 项假设 · {workflowPreview.dcfScenarioCount + workflowPreview.multipleScenarioCount} 个估值情景 · {workflowPreview.evidenceIDs.length} 个唯一证据引用</small>}
		<label className="licensed-import-confirmation"><input type="checkbox" disabled={!workflowPreview} checked={workflowConfirmed} onChange={(event) => setWorkflowConfirmed(event.target.checked)} />我确认假设、估值、基准预期、评级理由和失效规则均引用真实已批准证据</label>
		<button type="button" disabled={loading || !workflowPreview || !workflowConfirmed} onClick={() => void runWorkflow()}>运行基本面研究</button>
		<label>定时研究批准计划<textarea aria-label="定时基本面研究计划 JSON" rows={10} value={scheduleJSON} onChange={(event) => { setScheduleJSON(event.target.value); resetScheduleApproval(); }} placeholder='人工研究成功后载入同源草稿；填写 approved_by，预校验后再批准。运行时价格由真实复权行情重新读取。' /></label>
		<button type="button" disabled={loading || !scheduleJSON.trim()} onClick={previewScheduleInput}>预校验定时研究计划</button>
		{schedulePreview && <small>预览：预测 {schedulePreview.forecastVersionID} · 每 {schedulePreview.cadenceHours} 小时 · 价格最长 {schedulePreview.maxPriceAgeHours} 小时 · 计划最长 {schedulePreview.maxPlanAgeDays} 天 · {schedulePreview.evidenceIDs.length} 个唯一证据引用</small>}
		<label className="licensed-import-confirmation"><input type="checkbox" disabled={!schedulePreview} checked={scheduleConfirmed} onChange={(event) => setScheduleConfirmed(event.target.checked)} />我确认计划复用本次人工研究的同源预测、估值和证据，并由填写的 approved_by 批准启用</label>
		<button type="button" disabled={loading || !schedulePreview || !scheduleConfirmed} onClick={() => void approveSchedule()}>批准定时研究</button>
      <button type="button" disabled={loading || schedule?.status !== "approved"} onClick={() => void pauseSchedule()}>暂停定时研究</button>
      <small>人工研究成功后会自动载入同源计划草稿，但 approved_by 保持空白且不会自动批准；计划运行时重新读取真实复权价。出现新财报、计划过期或缺少复权价时自动停止并等待复核。</small>
		</div>
	</details>
    {message && <div className="page-message">{message}</div>}
    <div className="metric-grid">
		<article><span>财务快照</span><strong>{bundle.fundamentals?.items?.length ?? 0}</strong><small>严格按 available_at 截止</small></article>
		<article><span>已批准分析师证据</span><strong>{analystEvidenceItems.length}</strong><small>任意字符串不能作为估值、基准或评级证据</small></article>
		<article><span>人工研究门禁</span><strong>三段预校验</strong><small>证据登记 → 研究运行 → 计划批准均需再次确认</small></article>
		<article><span>PIT 基准映射</span><strong>{benchmarkResolution?.status === "available" ? "已批准" : "不可用"}</strong><small>{benchmarkMapping?.benchmark_asset_id || benchmarkResolution?.reason || bundle.marketPolicy?.policy?.benchmark_id || "等待市场策略"}</small></article>
		<article><span>持牌总回报导入</span><strong>{licensedBenchmarkAvailable ? licensedBenchmarkAuditLoaded ? `${licensedBenchmarkReceipts.length} 份回执` : "人工入口已就绪" : "当前基准不适用"}</strong><small>{canonicalBenchmarkID || "读取资产后核对规范基准"} · 不自动获取或批准数据</small></article>
		<article><span>可交易状态证据</span><strong>{latestTradability ? tradabilityStatusLabel(latestTradability[1].status) : "不可用"}</strong><small>{latestTradability ? `${latestTradability[0]} · ${latestTradability[1].source_count ?? 0} 个来源` : `${tradabilityItems.length} 条观测 · 禁止从价格推断`}</small></article>
		<article><span>复权价格证据</span><strong>{typeof latestPrice?.price === "number" ? `${latestPrice.price} ${latestPrice.currency || ""}` : "不可用"}</strong><small>{latestPrice?.id || "可按需同步真实复权价格，不自动写入研究假设"}</small></article>
		<article><span>一致预期返回</span><strong>{consensusItems.length}</strong><small>仅返回首次观测后可用的数据</small></article>
		<article><span>管理层指引</span><strong>{guidanceItems.length}</strong><small>与分析师一致预期分开保存</small></article>
		<article><span>官方披露候选</span><strong>{guidanceSourceItems.length}</strong><small>候选不等于管理层指引</small></article>
		<article><span>分析准备</span><strong>{preparation?.status === "analyst_review_required" ? "待人工审核" : preparation?.status || "不可用"}</strong><small>{preparation?.statement_period_end || preparation?.reason || "等待同报告期财务表"}</small></article>
      <article><span>预测版本</span><strong>{bundle.forecasts?.items?.length ?? 0}</strong><small>假设与证据可追溯</small></article>
      <article><span>估值运行</span><strong>{bundle.valuations?.items?.length ?? 0}</strong><small>情景区间不是概率</small></article>
      <article><span>基本面评级</span><strong>{rating?.result?.rating || rating?.result?.status || "不可用"}</strong><small>{rating?.result?.reason || rating?.state?.effective_at || "等待完整输入"}</small></article>
		<article><span>短期预测</span><strong>{prediction?.status || "不可用"}</strong><small>{prediction?.status === "calibrated" && typeof prediction.probability === "number" ? `${Math.round(prediction.probability * 100)}%` : "未校准时不显示概率"}</small></article>
		<article><span>资产政策</span><strong>{bundle.marketPolicy?.asset_class || "不可用"} · {bundle.marketPolicy?.market || "—"}</strong><small>{bundle.marketPolicy?.policy?.fundamental_supported ? "基本面可用" : "基本面不适用"} · {bundle.marketPolicy?.policy?.prediction_supported ? "短期预测可用" : "短期预测不适用"}</small></article>
      <article><span>定时研究</span><strong>{schedule?.status || "未配置"}</strong><small>{schedule?.last_run_reason || schedule?.last_run_status || schedule?.next_run_at || "人工研究成功后自动载入同源计划草稿，仍需人工显式批准"}</small></article>
    </div>
	<div className="conclusion-list">
		<article className="conclusion-card">
			<span>时点基准治理</span>
			<h3>{benchmarkResolution?.status === "available" ? `${benchmarkMapping?.benchmark_asset_id || "基准"} 已批准` : "尚无当时可用的基准映射"}</h3>
			<p>{benchmarkMapping ? `${benchmarkMapping.scope_type || "scope"}:${benchmarkMapping.scope_id || "—"} · ${benchmarkMapping.mapping_reason || "已记录批准理由"}` : `策略候选 ${bundle.marketPolicy?.policy?.benchmark_id || "未配置"}；身份存在不代表映射已经批准。`}</p>
			<small>{benchmarkMapping ? `来源 ${benchmarkMapping.source_name || "—"} · 批准人 ${benchmarkMapping.approved_by || "—"} · 可用时间 ${benchmarkMapping.available_at || "—"}` : `状态 ${benchmarkResolution?.reason || "missing_point_in_time_mapping"} · 自动批准：关闭`}</small>
		</article>
		<article className="conclusion-card">
			<span>持牌导入回执</span>
			<h3>{licensedBenchmarkAuditLoaded ? licensedBenchmarkReceipts.length ? `${licensedBenchmarkReceipts.length} 批已审计导入` : "尚无导入回执" : "等待主动读取"}</h3>
			<p>{licensedBenchmarkReceipts.length ? `最近覆盖 ${licensedBenchmarkReceipts[0]?.session_start || "—"} 至 ${licensedBenchmarkReceipts[0]?.session_end || "—"}，共 ${licensedBenchmarkReceipts[0]?.observation_count ?? 0} 条。` : "回执历史不会通过公开行情接口返回；没有回执不能推断已经获得或导入持牌数据。"}</p>
			<small>许可证与审批详情仅在管理页面显示 · 覆盖范围从实际不可变行情观测计算</small>
			{licensedBenchmarkReceipts.length > 0 && <details><summary>最近 20 批导入审计</summary>{licensedBenchmarkReceipts.map((item) => <p key={item.id}>{item.session_start || "—"} 至 {item.session_end || "—"} · {item.observation_count ?? 0} 条 / 首次新增 {item.inserted_count ?? 0} · {item.vendor_code || "—"} · {item.source_url ? <a href={item.source_url} target="_blank" rel="noreferrer">{item.source_name || item.source_document_id || "来源"}</a> : item.source_name || "—"} · 许可证 {item.license_reference || "—"} · 批准人 {item.approved_by || "—"} · {item.available_at ? new Date(item.available_at).toLocaleString("zh-CN") : "—"}</p>)}</details>}
		</article>
		<article className="conclusion-card">
			<span>可交易状态决议</span>
			<h3>{latestTradability ? `${latestTradability[0]} · ${tradabilityStatusLabel(latestTradability[1].status)}` : "尚无当时可用的状态证据"}</h3>
			<p>{latestTradability ? `${latestTradability[1].source_count ?? 0} 个来源 · ${latestTradability[1].reason || "按最新来源一致性决议"}` : "没有观测时保持 unknown，不会依据收盘价、涨跌幅或公司行动推断可成交。"}</p>
			<small>只有同一交易日所有来源的最新事实一致为可交易，入场和出场才可能启用执行模拟</small>
			{tradabilitySessions.length > 0 && <details><summary>最近交易日决议</summary>{tradabilitySessions.slice(0, 20).map(([session, resolution]) => <p key={session}>{session} · {tradabilityStatusLabel(resolution.status)} · {resolution.source_count ?? 0} 个来源 · {resolution.reason || "—"}</p>)}</details>}
			{tradabilityItems.length > 0 && <details><summary>公共来源事实</summary>{tradabilityItems.slice(0, 30).map((item) => <p key={item.id}>{item.session_date?.slice(0, 10) || "—"} · {tradabilityStatusLabel(item.status)} · {item.source_url ? <a href={item.source_url} target="_blank" rel="noreferrer">{item.source_name || item.source_document_id || "来源"}</a> : item.source_name || "—"} · 可得 {item.available_at ? new Date(item.available_at).toLocaleString("zh-CN") : "—"}</p>)}</details>}
		</article>
		<article className="conclusion-card">
			<span>可交易状态导入回执</span>
			<h3>{tradabilityAuditLoaded ? tradabilityReceipts.length ? `${tradabilityReceipts.length} 批已审计导入` : "尚无导入回执" : "等待主动读取"}</h3>
			<p>{tradabilityReceipts.length ? `最近覆盖 ${tradabilityReceipts[0]?.session_start || "—"} 至 ${tradabilityReceipts[0]?.session_end || "—"}，共 ${tradabilityReceipts[0]?.observation_count ?? 0} 条。` : "许可证与批准信息仅通过管理接口读取；没有回执不能推断已经取得或导入状态数据。"}</p>
			<small>覆盖范围来自实际不可变观测 · 导入后不自动运行结果评价或评级</small>
			{tradabilityReceipts.length > 0 && <details><summary>最近 20 批状态审计</summary>{tradabilityReceipts.map((item) => <p key={item.id}>{item.session_start || "—"} 至 {item.session_end || "—"} · {item.observation_count ?? 0} 条 / 首次新增 {item.inserted_count ?? 0} · {item.market || "—"} {item.currency || ""} · {item.source_url ? <a href={item.source_url} target="_blank" rel="noreferrer">{item.source_name || item.source_document_id || "来源"}</a> : item.source_name || "—"} · 许可证 {item.license_reference || "—"} · 批准人 {item.approved_by || "—"} · {item.available_at ? new Date(item.available_at).toLocaleString("zh-CN") : "—"}</p>)}</details>}
		</article>
		<article className="conclusion-card">
			<span>无新闻人工操作闭环</span>
			<h3>证据模板 → 研究预校验 → 同源计划审批</h3>
			<p>模板只声明字段和数据类型，所有数值、时点、来源、假设、估值与批准身份都必须由分析师填写；null 不会被替换成零。</p>
			<small>浏览器预校验不替代服务器 PIT、资产归属、证据类型和值一致性硬门禁 · 不自动批准</small>
		</article>
		<article className="conclusion-card">
			<span>分析师证据登记</span>
			<h3>{analystEvidenceItems.length ? `${analystEvidenceItems.length} 条可用证据` : "尚无已批准证据"}</h3>
			<p>{analystEvidenceItems.length ? analystEvidenceItems.map((item) => `${item.evidence_type || "unknown"} · ${item.id || "—"}`).join("；") : "请先登记带来源、时点、数值和审批人的证据，再引用到研究输入。"}</p>
			<small>证据按资产和 available_at 隔离；ID 非空不代表证据有效。</small>
		</article>
		<article className="conclusion-card">
			<span>无新闻研究准备包</span>
			<h3>{preparation?.status === "analyst_review_required" ? "财务事实已齐，等待分析师输入" : "事实输入尚未就绪"}</h3>
			<p>{preparation?.reason || "请读取标的以检查准备状态"}</p>
			<small>不会自动生成假设、估值或评级；{preparation?.controls?.analyst_approval_required ? "必须人工批准" : "尚未确认批准门禁"}</small>
			{!!preparation?.missing_fields?.length && <p>缺失字段：{preparation.missing_fields.join("、")}</p>}
			{!!preparation?.analyst_inputs_required?.length && <details><summary>仍需人工填写</summary><p>{preparation.analyst_inputs_required.join("、")}</p></details>}
			{!!preparation?.field_lineage && Object.keys(preparation.field_lineage).length > 0 && <details><summary>事实字段来源</summary>{Object.entries(preparation.field_lineage).map(([field, source]) => <p key={field}>{field}：{source.snapshot_id || "—"} · {(source.metrics || []).join("+")} · {source.transform || "identity"}</p>)}</details>}
		</article>
		<article className="conclusion-card">
			<span>分析师一致预期</span>
			<h3>{consensusItems.length > 0 ? `${consensusItems.length} 条最近观测` : "尚无可用观测"}</h3>
			<p>{consensusItems[0]?.available_at ? `最近观测：${new Date(consensusItems[0].available_at).toLocaleString("zh-CN")}` : "可启动单标的 FMP 同步；数据不会倒填到首次观测之前。"}</p>
			<small>供应商发布时间不可用：{consensus?.provider_publication_time_available === false ? "是" : "未确认"} · 历史倒填：{consensus?.historical_backfill === false ? "关闭" : "未确认"} · 自动评级：{consensus?.automatic_rating === false ? "关闭" : "未确认"}</small>
			{consensusItems.length > 0 && <details><summary>观测明细（最多 12 条）</summary>{consensusItems.slice(0, 12).map((item) => <p key={item.id}>{item.metric || "指标"} · {item.statistic || "统计"} · {typeof item.estimate_value === "number" ? item.estimate_value : "—"} {item.currency || ""} · {item.fiscal_period_end || "—"}{item.analyst_count ? ` · ${item.analyst_count} 位分析师` : ""}</p>)}</details>}
			{!!consensus?.revisions?.length && <details><summary>聚合预期修订</summary>{consensus.revisions.slice(0, 10).map((item) => <p key={item.current_id}>{item.metric || "指标"} · {item.statistic || "统计"} · {item.direction || "—"} {typeof item.absolute_change === "number" ? item.absolute_change : "—"} · 不推断单个分析师行为</p>)}</details>}
		</article>
		<article className="conclusion-card">
			<span>SEC 指引证据候选</span>
			<h3>{guidanceSourceItems.length > 0 ? `${guidanceSourceItems.length} 份官方披露待核验` : guidanceSources?.sync_reason === "sec_identity_not_configured" ? "SEC 身份尚未配置" : "尚无 SEC 披露候选"}</h3>
			<p>已确认 {guidanceSources?.review_status_counts?.confirmed_guidance ?? 0} · 无指引 {guidanceSources?.review_status_counts?.no_guidance ?? 0} · 待跟进 {guidanceSources?.review_status_counts?.needs_follow_up ?? 0} · 未核验 {guidanceSources?.review_status_counts?.unreviewed ?? 0}</p>
			<small>候选即指引：{guidanceSources?.candidate_is_guidance === false ? "否" : "未确认"} · 人工核验：{guidanceSources?.human_review_required ? "必需" : "未确认"} · 自动抽取：{guidanceSources?.automatic_extraction === false ? "关闭" : "未确认"}</small>
			{guidanceSourceItems.length > 0 && <details><summary>官方披露明细</summary>{guidanceSourceItems.map((item) => <p key={item.id}><a href={item.filing_index_url} target="_blank" rel="noreferrer">{item.form || "SEC"} · {item.accession_number || item.id}</a> · 接收 {item.accepted_at ? new Date(item.accepted_at).toLocaleString("zh-CN") : "—"} · {item.latest_review?.decision || "unreviewed"}</p>)}</details>}
		</article>
		<article className="conclusion-card">
			<span>管理层指引修订</span>
			<h3>{guidanceItems.length > 0 ? `${guidanceItems.length} 条来源化指引` : "尚无可靠指引快照"}</h3>
			<p>{guidance?.revisions?.length ? `${guidance.revisions.length} 次可复算区间修订` : "需要带真实发布时间、适用期间和来源的管理层披露；不会用一致预期代替。"}</p>
			<small>指引等同一致预期：{guidance?.guidance_is_consensus === false ? "否" : "未确认"} · 自动评级：{guidance?.automatic_rating === false ? "关闭" : "未确认"}</small>
			{!!guidance?.revisions?.length && <details><summary>修订明细</summary>{guidance.revisions.slice(0, 10).map((item) => <p key={item.current_id}>{item.metric || "指标"} · {item.direction || "—"} · 区间 {item.range_change || "—"} · {item.fiscal_period_end || "—"}</p>)}</details>}
		</article>
		<article className="conclusion-card">
			<span>分市场研究方法</span>
			<h3>{bundle.marketPolicy?.policy?.fundamental_method || "未配置"}</h3>
			<p>范围 {bundle.marketPolicy?.policy?.prediction_scope || "—"} · 币种 {bundle.marketPolicy?.currency || "—"}</p>
			<small>{bundle.marketPolicy?.policy?.reason || `政策 ${bundle.marketPolicy?.policy?.version || "—"}`}</small>
		</article>
		<article className="conclusion-card">
        <span>基本面预测与估值</span>
        <h3>{valuationRange && typeof valuationRange.low === "number" && typeof valuationRange.high === "number" ? `${valuationRange.low.toFixed(2)}—${valuationRange.high.toFixed(2)} ${valuation?.result?.currency || ""}` : valuation?.result?.reason || "暂无可复算估值区间"}</h3>
        <p>估值模型：{valuation?.model_version || "不可用"} · 预测模型：{forecast?.model_version || "不可用"}</p>
        <small>估值运行 {valuation?.id || "—"}；预测版本 {forecast?.id || "—"}；时点 {valuation?.as_of || forecast?.as_of || "—"}</small>
      </article>
      <article className="conclusion-card">
        <span>基本面评级修订</span>
        <h3>{rating?.revision?.previous_rating || "未评级"} → {rating?.revision?.current_rating || rating?.result?.rating || "不可用"}</h3>
        <p>{rating?.revision?.reason || rating?.result?.reason || "等待评级输入"}</p>
        <small>政策 {rating?.result?.policy_version || "—"} · 期限 {rating?.result?.horizon_days ?? "—"} 天 · 基准 {rating?.result?.benchmark_id || "—"}</small>
        {rating?.reason_codes && rating.reason_codes.length > 0 && <p>原因码：{rating.reason_codes.join("、")}</p>}
        {changedAssumptions.length > 0 && <details><summary>假设变化</summary>{changedAssumptions.map(([name, value]) => <p key={name}>{name}：{JSON.stringify(value)}</p>)}</details>}
        {rating?.evidence_ids && rating.evidence_ids.length > 0 && <details><summary>证据 ID</summary><p>{rating.evidence_ids.join("、")}</p></details>}
      </article>
      <article className="conclusion-card">
        <span>固定期限短期预测</span>
        <h3>{prediction?.status === "calibrated" && typeof prediction.probability === "number" ? `上涨概率 ${Math.round(prediction.probability * 100)}%` : "概率不可用"}</h3>
        <p>期限 {prediction?.horizon_sessions ?? "—"} 个交易日 · 模型 {prediction?.model_version || "—"}</p>
        <small>校准版本 {prediction?.calibration_version || "无"} · 信号可用时间 {prediction?.signal_available_at || "—"}{prediction?.exclusion_reason ? ` · ${prediction.exclusion_reason}` : ""}</small>
      </article>
    </div>
  </section>;
}

export function WeknoraPage({ apiBase }: { apiBase: string }) {
  const [url, setUrl] = useState("http://10.15.0.28/"); const [draft, setDraft] = useState(url); const [message, setMessage] = useState(""); const [failed, setFailed] = useState(false);
  useEffect(() => { fetch(`${apiBase}/api/v1/integrations/weknora`).then((r) => r.json()).then((payload: { url: string }) => { setUrl(payload.url); setDraft(payload.url); }).catch(() => setMessage("无法读取 WeKnora 配置，已使用默认地址。")); }, [apiBase]);
  const headers = { "Content-Type": "application/json" };
  async function save() { const response = await fetch(`${apiBase}/api/v1/admin/integrations/weknora`, { method: "PUT", headers, body: JSON.stringify({ url: draft }) }); if (response.ok) { setUrl((await response.json()).url); setFailed(false); setMessage("WeKnora 地址已保存。"); } else setMessage("保存失败，请检查服务器访问配置和 URL。"); }
  async function test() { const response = await fetch(`${apiBase}/api/v1/admin/integrations/weknora/test`, { method: "POST", headers, body: JSON.stringify({ url: draft }) }); const payload = await response.json(); setMessage(payload.ok ? `连接成功（HTTP ${payload.status_code}）。` : `连接失败：${payload.error || payload.status_code}`); }
  return <section className="app-page weknora-page"><PageHeading eyebrow="LOCAL KNOWLEDGE WORKBENCH" title="WeKnora" copy="内嵌本地知识库工作台；若服务禁止 iframe，可在新窗口中继续。" /><div className="weknora-toolbar"><a href={url} target="_blank" rel="noreferrer">新窗口打开</a><span>{failed ? "内嵌加载失败，请使用“新窗口打开”。" : "若下方为空白或提示拒绝连接，请使用“新窗口打开”。"}</span></div><div className="weknora-frame"><iframe title="WeKnora 本地知识库" src={url} onError={() => setFailed(true)} /></div><div className="integration-editor"><label>WeKnora URL<input type="url" value={draft} onChange={(e) => setDraft(e.target.value)} /></label><button type="button" onClick={test}>连接测试</button><button type="button" onClick={save}>保存</button></div>{message && <div className="page-message">{message}</div>}</section>;
}

export function RoutedPage({
  route, apiBase, analysisLogs,
}: {
  route: Exclude<AppRoute, "home">; apiBase: string; analysisLogs: AnalysisLog[];
}) {
  if (route === "model-logs") return <ModelLogsPage apiBase={apiBase} onBack={() => { window.location.hash = "/home"; }} embedded />;
  if (route === "policy") return <ResearchPolicyPage apiBase={apiBase} />;
  if (route === "source-filter") return <SourceFilterPage apiBase={apiBase} />;
  if (route === "conclusions") return <ConclusionsPage apiBase={apiBase} />;
  if (route === "targets") return <ChangedTargetsPage apiBase={apiBase} />;
  if (route === "fundamental") return <FundamentalResearchPage apiBase={apiBase} />;
	if (route === "readiness") return <PhaseTwoReadinessPage apiBase={apiBase} />;
  if (route === "sources") return <SourcesPage apiBase={apiBase} />;
  if (route === "asset-universe") return <AssetUniversePage apiBase={apiBase} />;
  if (route === "news") return <NewsPage apiBase={apiBase} />;
  if (route === "queue") return <QueuePage apiBase={apiBase} />;
  if (route === "analysis") return <AnalysisPage logs={analysisLogs} />;
  if (route === "search") return <SearchPage apiBase={apiBase} />;
  return <WeknoraPage apiBase={apiBase} />;
}
