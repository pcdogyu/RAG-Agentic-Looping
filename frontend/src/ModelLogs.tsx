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

type Filters = {
  range: string;
  model: string;
  provider: string;
  operation: string;
  status: string;
  language: string;
  fidelity: string;
};

const initialFilters: Filters = {
  range: "7d",
  model: "",
  provider: "",
  operation: "",
  status: "",
  language: "",
  fidelity: "",
};

const knownModels = ["qwen2.5:3b", "qwen2.5:7b", "qwen2.5:14b", "qwen2.5-coder:7b"];
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

export function buildModelLogQuery(filters: Filters, now = Date.now()) {
  const params = new URLSearchParams();
  const days = filters.range.endsWith("d") ? Number(filters.range.slice(0, -1)) : 0;
  if (days > 0) params.set("start", new Date(now - days * 86400000).toISOString());
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
  return value >= 1000 ? `${(value / 1000).toFixed(1)} 秒` : `${value} 毫秒`;
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

export default function ModelLogsPage({ apiBase, onBack, embedded = false }: { apiBase: string; onBack: () => void; embedded?: boolean }) {
  const [filters, setFilters] = useState<Filters>(initialFilters);
  const [usage, setUsage] = useState<Usage | null>(null);
  const [items, setItems] = useState<ModelLogSummary[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [details, setDetails] = useState<Record<string, ModelLogDetail>>({});
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const query = useMemo(() => buildModelLogQuery(filters).toString(), [filters]);
  const modelOptions = useMemo(
    () => [...new Set([...knownModels, ...(usage?.models.map((item) => item.model) || [])])],
    [usage],
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

  useEffect(() => {
    const controller = new AbortController();
    load(controller.signal);
    return () => controller.abort();
  }, [load]);

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

      <section className="model-usage-metrics" aria-label="模型使用汇总">
        <div><span>调用总数</span><strong>{usage?.calls ?? "—"}</strong></div>
        <div><span>成功率</span><strong>{usage ? `${Math.round(usage.success_rate * 100)}%` : "—"}</strong></div>
        <div><span>失败调用</span><strong>{usage?.failures ?? "—"}</strong></div>
        <div><span>平均耗时</span><strong>{usage ? formatDuration(usage.average_duration_ms) : "—"}</strong></div>
        <div><span>总 Token</span><strong>{usage ? usage.prompt_tokens + usage.completion_tokens : "—"}</strong></div>
      </section>

      <section className="model-log-filters" aria-label="模型日志筛选">
        <label>时间<select value={filters.range} onChange={(e) => updateFilter("range", e.target.value)}>
          <option value="1d">最近24小时</option><option value="7d">最近7天</option>
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
          <button type="button" onClick={() => load()}>刷新</button>
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
                  <div><strong>{formatDuration(item.duration_ms)}</strong><span>{(item.prompt_tokens || 0) + (item.completion_tokens || 0)} token</span></div>
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
