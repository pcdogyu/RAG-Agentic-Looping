import { useCallback, useEffect, useMemo, useState } from "react";

import BuildFooter from "./BuildFooter";

export type ModelLogSummary = {
  id: string;
  logical_call_id: string;
  provider: string;
  model: string;
  operation: string;
  entity_type: string | null;
  entity_id: string | null;
  attempt: number;
  status: string;
  fidelity: string;
  started_at: string;
  completed_at: string;
  duration_ms: number | null;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  input_language: string;
  output_language: string;
};

type ModelLogDetail = ModelLogSummary & {
  messages: Array<{ role: string; content: string }>;
  schema: Record<string, unknown>;
  raw_response: string;
  parsed_response: unknown;
  error: string | null;
  metrics: Record<string, unknown>;
};

type Usage = {
  calls: number;
  successes: number;
  failures: number;
  success_rate: number;
  average_duration_ms: number | null;
  prompt_tokens: number;
  completion_tokens: number;
  models: Array<{ model: string; calls: number }>;
  operations: string[];
  providers: string[];
};

export type ModelRuntimeMetrics = {
  processed_tasks: number;
  successful_tasks: number;
  failed_tasks: number;
  queued_tasks: number;
  running_tasks: number;
  success_rate: number | null;
  failure_rate: number | null;
  average_processing_ms: number | null;
  input_tokens: number;
  output_tokens: number;
  average_input_tokens: number | null;
  average_output_tokens: number | null;
  input_token_task_count: number;
  output_token_task_count: number;
};

export type ModelRuntimeModel = ModelRuntimeMetrics & {
  provider: string;
  model: string;
  configured: boolean;
  enabled: boolean;
  activity_state: "running" | "queued" | "idle" | "disabled" | "historical";
  lanes: Array<{ id: string; purpose: string; enabled: boolean }>;
};

export type ModelRuntimeSummary = {
  generated_at: string;
  window_started_at: string;
  window_ended_at: string;
  window_hours: number;
  totals: ModelRuntimeMetrics;
  models: ModelRuntimeModel[];
};

type Filters = {
  range: string;
  model: string;
  provider: string;
  operation: string;
  status: string;
  language: string;
  fidelity: string;
};

export const defaultModelLogRange = "1d";

const initialFilters: Filters = {
  range: defaultModelLogRange,
  model: "",
  provider: "",
  operation: "",
  status: "",
  language: "",
  fidelity: "",
};

export const modelRuntimeRefreshMs = 30000;

export function modelRuntimeSummaryURL(apiBase: string) {
  return `${apiBase}/api/v1/model-runtime-summary`;
}

const knownModels = ["qwen2.5:3b", "qwen2.5:7b", "qwen2.5-coder:7b"];
const knownOperations = [
  "event_extraction",
  "asset_mapping",
  "report_drafting",
  "report_revision",
  "event_report_drafting",
  "event_report_revision",
  "cloud_verification",
  "evolution_proposal",
  "embedding",
];

const operationLabels: Record<string, string> = {
  event_extraction: "事件抽取",
  asset_mapping: "标的映射",
  report_drafting: "研究草稿",
  report_revision: "研究修订",
  event_report_drafting: "事件研报",
  event_report_revision: "事件研报修订",
  cloud_verification: "云端复核",
  evolution_proposal: "代码演进",
  embedding: "向量嵌入",
};

export function isModelLogsHash(hash: string) {
  return hash === "#/model-logs";
}

export function fidelityLabel(value: string) {
  return {
    exact: "精确记录",
    reconstructed: "历史重建",
    structured_only: "仅结构化结果",
    metadata_only: "仅使用元数据",
  }[value] || value;
}

export function modelTokenLabel(promptTokens: number | null, completionTokens: number | null) {
  return `输入 ${formatCount(promptTokens)} · 生成 ${formatCount(completionTokens)}`;
}

export function modelAuditMetricRows(metrics: Record<string, unknown>) {
  const rows: Array<{ label: string; value: string }> = [];
  if (typeof metrics.think_enabled === "boolean") {
    rows.push({ label: "思考模式", value: metrics.think_enabled ? "开启" : "关闭" });
  }
  if (typeof metrics.max_output_tokens === "number") {
    rows.push({ label: "生成上限", value: formatCount(metrics.max_output_tokens) });
  }
  if (typeof metrics.done_reason === "string" && metrics.done_reason) {
    rows.push({ label: "结束原因", value: metrics.done_reason });
  }
  if (typeof metrics.thinking_char_count === "number") {
    rows.push({ label: "思考字符", value: formatCount(metrics.thinking_char_count) });
  }
  if (metrics.output_limit_reached === true) {
    rows.push({ label: "达到上限", value: "是" });
  }
  if (typeof metrics.fallback_reason === "string" && metrics.fallback_reason) {
    rows.push({ label: "降级原因", value: metrics.fallback_reason });
  }
  return rows;
}

export function buildModelLogQuery(filters: Filters, now = Date.now()) {
  const params = new URLSearchParams();
  const range = filters.range.match(/^(\d+)(m|h|d)$/);
  const unitMilliseconds = { m: 60000, h: 3600000, d: 86400000 } as const;
  if (range) {
    const [, amount, unit] = range;
    params.set("start", new Date(now - Number(amount) * unitMilliseconds[unit as keyof typeof unitMilliseconds]).toISOString());
  }
  for (const key of ["model", "provider", "operation", "status", "language", "fidelity"] as const) {
    if (filters[key]) params.set(key, filters[key]);
  }
  return params;
}

function formatTime(value: string) {
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(new Date(value));
}

function formatDuration(value: number | null) {
  if (value === null) return "历史未记录";
  if (value >= 3600000) return `${(value / 3600000).toFixed(1)} 小时`;
  if (value >= 60000) return `${(value / 60000).toFixed(1)} 分钟`;
  return value >= 1000 ? `${(value / 1000).toFixed(1)} 秒` : `${value} 毫秒`;
}

function formatCount(value: number | null) {
  return value === null ? "—" : value.toLocaleString("zh-CN");
}

function formatRate(value: number | null) {
  if (value === null) return "—";
  return `${(value * 100).toLocaleString("zh-CN", { minimumFractionDigits: 1, maximumFractionDigits: 1 })}%`;
}

export function modelActivityLabel(value: ModelRuntimeModel["activity_state"]) {
  return {
    running: "运行中",
    queued: "排队中",
    idle: "空闲",
    disabled: "已停用",
    historical: "历史模型",
  }[value];
}

function pretty(value: unknown) {
  return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

async function copyText(value: string) {
  await navigator.clipboard?.writeText(value);
}

function ContentBlock({ title, value }: { title: string; value: string }) {
  return (
    <section className="model-log-content">
      <div><h4>{title}</h4><button type="button" onClick={() => copyText(value)}>复制</button></div>
      <pre>{value || "历史未记录"}</pre>
    </section>
  );
}

function RuntimeMetric({ label, value, note }: { label: string; value: string; note?: string }) {
  return <div><span>{label}</span><strong>{value}</strong>{note && <small>{note}</small>}</div>;
}

export function ModelRuntimePanel({ summary, loading, error }: {
  summary: ModelRuntimeSummary | null;
  loading: boolean;
  error: string | null;
}) {
  return <section className="model-runtime-panel" aria-label="各模型近24小时运行状态" data-layout="responsive-cards">
    <div className="model-runtime-heading">
      <div><p className="eyebrow">MODEL RUNTIME · 24H</p><h3>各模型运行状态</h3></div>
      <span>处理指标近24小时 · 队列与处理中为当前全量</span>
    </div>
    {error && <div className="model-runtime-error">{error}</div>}
    {loading && !summary && <div className="model-log-empty">正在读取模型运行统计…</div>}
    {summary && <>
      <div className="model-runtime-overview" aria-label="模型运行全局汇总">
        <RuntimeMetric label="已处理任务" value={formatCount(summary.totals.processed_tasks)} />
        <RuntimeMetric label="当前队列" value={formatCount(summary.totals.queued_tasks)} />
        <RuntimeMetric label="处理中" value={formatCount(summary.totals.running_tasks)} />
        <RuntimeMetric label="平均处理时间" value={summary.totals.average_processing_ms === null ? "—" : formatDuration(summary.totals.average_processing_ms)} />
        <RuntimeMetric label="成功率" value={formatRate(summary.totals.success_rate)} />
        <RuntimeMetric label="失败率" value={formatRate(summary.totals.failure_rate)} />
        <RuntimeMetric label="输入 Token" value={formatCount(summary.totals.input_tokens)} note={`平均/任务 ${formatCount(summary.totals.average_input_tokens)}`} />
        <RuntimeMetric label="输出 Token" value={formatCount(summary.totals.output_tokens)} note={`平均/任务 ${formatCount(summary.totals.average_output_tokens)}`} />
      </div>
      <div className="model-runtime-grid">
        {summary.models.map((item) => <article className="model-runtime-card" key={`${item.provider}:${item.model}`}>
          <header>
            <div><h4>{item.model}</h4><span>{item.provider} · {item.lanes.length > 0
              ? item.lanes.map((lane) => `${lane.purpose}${lane.enabled ? "" : "（停用）"}`).join(" · ")
              : "近24小时历史调用"}</span></div>
            <strong className={`model-runtime-state ${item.activity_state}`}><i />{modelActivityLabel(item.activity_state)}</strong>
          </header>
          <div className="model-runtime-card-metrics">
            <RuntimeMetric label="已处理" value={formatCount(item.processed_tasks)} />
            <RuntimeMetric label="当前队列" value={formatCount(item.queued_tasks)} />
            <RuntimeMetric label="处理中" value={formatCount(item.running_tasks)} />
            <RuntimeMetric label="平均处理时间" value={item.average_processing_ms === null ? "—" : formatDuration(item.average_processing_ms)} />
            <RuntimeMetric label="成功率" value={formatRate(item.success_rate)} note={`${formatCount(item.successful_tasks)} 个成功任务`} />
            <RuntimeMetric label="失败率" value={formatRate(item.failure_rate)} note={`${formatCount(item.failed_tasks)} 个失败任务`} />
            <RuntimeMetric label="输入 Token" value={formatCount(item.input_tokens)} note={`平均/任务 ${formatCount(item.average_input_tokens)}`} />
            <RuntimeMetric label="输出 Token" value={formatCount(item.output_tokens)} note={`平均/任务 ${formatCount(item.average_output_tokens)}`} />
          </div>
        </article>)}
      </div>
    </>}
  </section>;
}

export default function ModelLogsPage({ apiBase, onBack, embedded = false }: { apiBase: string; onBack: () => void; embedded?: boolean }) {
  const [filters, setFilters] = useState<Filters>(initialFilters);
  const [usage, setUsage] = useState<Usage | null>(null);
  const [runtimeSummary, setRuntimeSummary] = useState<ModelRuntimeSummary | null>(null);
  const [items, setItems] = useState<ModelLogSummary[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [details, setDetails] = useState<Record<string, ModelLogDetail>>({});
  const [loading, setLoading] = useState(true);
  const [runtimeLoading, setRuntimeLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);

  const query = useMemo(() => buildModelLogQuery(filters).toString(), [filters]);
  const modelOptions = useMemo(
    () => [...new Set([...knownModels, ...(runtimeSummary?.models.map((item) => item.model) || []), ...(usage?.models.map((item) => item.model) || [])])],
    [runtimeSummary, usage],
  );
  const operationOptions = useMemo(
    () => [...new Set([...knownOperations, ...(usage?.operations || [])])],
    [usage],
  );

  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    try {
      const suffix = query ? `?${query}` : "";
      const [usageResponse, logsResponse] = await Promise.all([
        fetch(`${apiBase}/api/v1/model-usage${suffix}`, { signal }),
        fetch(`${apiBase}/api/v1/model-logs${suffix ? `${suffix}&` : "?"}limit=50`, { signal }),
      ]);
      if (!usageResponse.ok || !logsResponse.ok) throw new Error("模型日志请求失败");
      const usagePayload = await usageResponse.json() as Usage;
      const logPayload = await logsResponse.json() as {
        items: ModelLogSummary[]; next_cursor: string | null;
      };
      setUsage(usagePayload);
      setItems(logPayload.items);
      setNextCursor(logPayload.next_cursor);
      setExpanded(null);
    } catch (reason) {
      if (signal?.aborted) return;
      setError(reason instanceof Error ? reason.message : "模型日志请求失败");
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [apiBase, query]);

  const loadRuntimeSummary = useCallback(async (signal?: AbortSignal) => {
    setRuntimeError(null);
    try {
      const response = await fetch(modelRuntimeSummaryURL(apiBase), { signal });
      if (!response.ok) throw new Error("模型运行统计请求失败");
      setRuntimeSummary(await response.json() as ModelRuntimeSummary);
    } catch (reason) {
      if (signal?.aborted) return;
      setRuntimeError(reason instanceof Error ? reason.message : "模型运行统计请求失败");
    } finally {
      if (!signal?.aborted) setRuntimeLoading(false);
    }
  }, [apiBase]);

  useEffect(() => {
    const controller = new AbortController();
    load(controller.signal);
    return () => controller.abort();
  }, [load]);

  useEffect(() => {
    const controller = new AbortController();
    const refreshRuntime = () => loadRuntimeSummary(controller.signal);
    refreshRuntime();
    const timer = window.setInterval(refreshRuntime, modelRuntimeRefreshMs);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [loadRuntimeSummary]);

  async function loadMore() {
    if (!nextCursor || loadingMore) return;
    setLoadingMore(true);
    try {
      const params = new URLSearchParams(query);
      params.set("limit", "50");
      params.set("cursor", nextCursor);
      const response = await fetch(`${apiBase}/api/v1/model-logs?${params}`);
      if (!response.ok) throw new Error("加载更多失败");
      const payload = await response.json() as {
        items: ModelLogSummary[]; next_cursor: string | null;
      };
      setItems((current) => [...current, ...payload.items]);
      setNextCursor(payload.next_cursor);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "加载更多失败");
    } finally {
      setLoadingMore(false);
    }
  }

  async function toggleDetail(id: string) {
    if (expanded === id) {
      setExpanded(null);
      return;
    }
    setExpanded(id);
    if (details[id]) return;
    try {
      const response = await fetch(`${apiBase}/api/v1/model-logs/${id}`);
      if (!response.ok) throw new Error("日志详情请求失败");
      const detail = await response.json() as ModelLogDetail;
      setDetails((current) => ({ ...current, [id]: detail }));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "日志详情请求失败");
    }
  }

  function updateFilter(name: keyof Filters, value: string) {
    setFilters((current) => ({ ...current, [name]: value }));
  }

  function refreshAll() {
    void load();
    void loadRuntimeSummary();
  }

  return (
    <div className="model-logs-page">
      {!embedded && <header className="model-logs-header">
        <div>
          <p className="eyebrow">MODEL I/O AUDIT</p>
          <h1>模型<span>日志</span></h1>
          <p className="subhead">查看本项目模型的历史输入、输出与运行指标</p>
        </div>
        <button type="button" className="back-dashboard" onClick={onBack}>← 返回主看板</button>
      </header>}
      {embedded && <div className="page-heading"><p className="eyebrow">MODEL I/O AUDIT</p><h2>模型日志</h2><p>查看本项目模型的历史输入、输出与运行指标。</p></div>}

      <ModelRuntimePanel summary={runtimeSummary} loading={runtimeLoading} error={runtimeError} />

      <section className="model-log-filters" aria-label="模型日志筛选">
        <label>时间<select value={filters.range} onChange={(e) => updateFilter("range", e.target.value)}>
          <option value="30m">最近30分钟</option><option value="1h">最近1小时</option>
          <option value="12h">最近12小时</option><option value="1d">最近24小时</option>
          <option value="3d">最近3天</option><option value="7d">最近7天</option>
          <option value="30d">最近30天</option><option value="90d">最近90天</option>
          <option value="all">全部</option>
        </select></label>
        <label>模型<select value={filters.model} onChange={(e) => updateFilter("model", e.target.value)}>
          <option value="">全部模型</option>{modelOptions.map((item) => <option key={item}>{item}</option>)}
        </select></label>
        <label>提供方<select value={filters.provider} onChange={(e) => updateFilter("provider", e.target.value)}>
          <option value="">全部</option><option value="ollama">Ollama</option>
          <option value="openai-compatible">云端复核</option><option value="local-cpu">本地 CPU</option>
        </select></label>
        <label>阶段<select value={filters.operation} onChange={(e) => updateFilter("operation", e.target.value)}>
          <option value="">全部阶段</option>{operationOptions.map((item) => (
            <option key={item} value={item}>{operationLabels[item] || item}</option>
          ))}
        </select></label>
        <label>状态<select value={filters.status} onChange={(e) => updateFilter("status", e.target.value)}>
          <option value="">全部</option><option value="completed">成功</option>
          <option value="failed">失败</option><option value="unmapped">未映射</option>
        </select></label>
        <label>语言<select value={filters.language} onChange={(e) => updateFilter("language", e.target.value)}>
          <option value="">全部</option><option value="zh">中文</option><option value="en">英文</option>
          <option value="mixed">中英混合</option><option value="other">其他</option>
        </select></label>
        <label>记录类型<select value={filters.fidelity} onChange={(e) => updateFilter("fidelity", e.target.value)}>
          <option value="">全部</option><option value="exact">精确记录</option>
          <option value="reconstructed">历史重建</option>
          <option value="structured_only">仅结构化结果</option>
          <option value="metadata_only">仅使用元数据</option>
        </select></label>
        <button type="button" onClick={() => setFilters(initialFilters)}>重置</button>
      </section>

      <section className="panel model-log-panel">
        <div className="model-log-panel-title">
          <div><h2>调用记录</h2><span>北京时间 · 最新优先</span></div>
          <button type="button" onClick={refreshAll}>刷新</button>
        </div>
        {error && <div className="model-log-error">{error}</div>}
        {loading && <div className="model-log-empty">正在读取模型日志…</div>}
        {!loading && items.length === 0 && <div className="model-log-empty">当前筛选范围内没有模型记录。</div>}
        <div className="model-log-list">
          {items.map((item) => {
            const open = expanded === item.id;
            const detail = details[item.id];
            return (
              <article className={`model-log-row ${open ? "open" : ""}`} key={item.id}>
                <button type="button" className="model-log-summary" onClick={() => toggleDetail(item.id)} aria-expanded={open}>
                  <div className="model-log-identity"><strong>{item.model}</strong><span>{item.provider}</span></div>
                  <div><strong>{operationLabels[item.operation] || item.operation}</strong><span>{item.entity_type || "系统调用"}</span></div>
                  <div><strong>{formatDuration(item.duration_ms)}</strong><span>{modelTokenLabel(item.prompt_tokens, item.completion_tokens)}</span></div>
                  <div><strong>{formatTime(item.started_at)}</strong><span>第 {item.attempt} 次尝试</span></div>
                  <span className={`model-log-status ${item.status}`}>{item.status === "completed" ? "成功" : item.status}</span>
                  <span className={`fidelity ${item.fidelity}`}>{fidelityLabel(item.fidelity)}</span>
                  <i>{open ? "−" : "+"}</i>
                </button>
                {open && (
                  <div className="model-log-detail">
                    {!detail && <div className="model-log-empty">正在加载输入输出…</div>}
                    {detail && (
                      <>
                        <div className="model-log-meta">
                          <span>输入语言 <strong>{detail.input_language}</strong></span>
                          <span>输出语言 <strong>{detail.output_language}</strong></span>
                          <span>关联对象 <strong>{detail.entity_id || "—"}</strong></span>
                          <span>调用 ID <strong>{detail.logical_call_id}</strong></span>
                          {modelAuditMetricRows(detail.metrics).map((metric) => (
                            <span key={metric.label}>{metric.label} <strong>{metric.value}</strong></span>
                          ))}
                        </div>
                        {detail.messages.map((message, index) => (
                          <ContentBlock key={`${message.role}-${index}`} title={`输入 · ${message.role}`} value={message.content} />
                        ))}
                        {Object.keys(detail.schema || {}).length > 0 && <ContentBlock title="JSON Schema" value={pretty(detail.schema)} />}
                        <ContentBlock title="模型原始输出" value={detail.raw_response} />
                        {detail.parsed_response !== null && <ContentBlock title="解析后 JSON" value={pretty(detail.parsed_response)} />}
                        {detail.error && <ContentBlock title="错误" value={detail.error} />}
                      </>
                    )}
                  </div>
                )}
              </article>
            );
          })}
        </div>
        {nextCursor && <button type="button" className="load-more" onClick={loadMore} disabled={loadingMore}>{loadingMore ? "正在加载…" : "加载更多"}</button>}
      </section>
      {!embedded && <BuildFooter />}
    </div>
  );
}
