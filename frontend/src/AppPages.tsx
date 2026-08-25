import { FormEvent, useEffect, useMemo, useState } from "react";

import ModelLogsPage from "./ModelLogs";

export type AppRoute = "home" | "conclusions" | "sources" | "model-logs" | "search" | "weknora";

const routes: Array<{ route: AppRoute; label: string }> = [
  { route: "home", label: "首页" },
  { route: "conclusions", label: "结论" },
  { route: "sources", label: "数据源" },
  { route: "model-logs", label: "模型日志" },
  { route: "search", label: "搜索引擎" },
  { route: "weknora", label: "WeKnora" },
];

export function routeFromHash(hash: string): AppRoute {
  const candidate = hash.replace(/^#\/?/, "") as AppRoute;
  return routes.some((item) => item.route === candidate) ? candidate : "home";
}

export function TopNavigation({ current }: { current: AppRoute }) {
  return (
    <nav className="top-navigation" aria-label="主导航">
      {routes.map((item) => (
        <a
          key={item.route}
          href={`#/${item.route}`}
          aria-current={current === item.route ? "page" : undefined}
        >
          {item.label}
        </a>
      ))}
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

const ratingLabels: Record<string, string> = {
  strongly_bullish: "强烈看多", bullish: "看多", watch: "中性", bearish: "看空", strongly_bearish: "强烈看空",
};

export function ConclusionsPage({ apiBase }: { apiBase: string }) {
  const [filters, setFilters] = useState({ q: "", market: "", rating: "", evidence_status: "" });
  const [items, setItems] = useState<Recommendation[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [selected, setSelected] = useState<ConclusionDetail | null>(null);
  const [error, setError] = useState("");

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
  useEffect(() => { load(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

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
      <div className="conclusion-list">
        {items.map((item) => <button type="button" className="conclusion-card" key={item.id} onClick={() => open(item)}>
          <div><span>{item.asset.market} · {new Date(item.as_of).toLocaleString("zh-CN")}</span><strong>{item.asset.symbol} · {item.asset.name}</strong><p>{item.thesis.summary}</p></div>
          <div className="conclusion-score"><strong>{item.score > 0 ? "+" : ""}{item.score}</strong><span>{ratingLabels[item.rating] || item.rating}</span><small>置信度 {Math.round(item.confidence * 100)}% · {item.evidence_complete ? "证据完整" : "证据不足"}</small></div>
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
        <h3>新闻与证据</h3><div className="evidence-links">{[...selected.news.map((item) => ({ label: item.title, url: item.url, source: item.source })), ...selected.evidence.map((item) => ({ label: item.claim, url: item.source_url, source: item.source_name }))].map((item, index) => <a key={`${item.url}-${index}`} href={item.url} target="_blank" rel="noreferrer"><strong>{item.label}</strong><span>{item.source}</span></a>)}</div>
      </article></div>}
    </section>
  );
}

type McpSource = {
  id: string; name: string; url: string; description: string; priority: number; enabled: boolean;
  managed: boolean; auth_type: string; auth_header_name: string | null; secret_configured: boolean;
  discovered_tools: Array<{ name: string; description: string; input_schema: unknown; output_schema: unknown }>;
  tool_mappings: Record<string, unknown>; last_status: string; last_error: string | null;
};

type SourceDraft = { name: string; url: string; description: string; priority: number; enabled: boolean; auth_type: string; auth_header_name: string; secret: string; clear_secret: boolean; tool_mappings: string };
const blankSource: SourceDraft = { name: "", url: "", description: "", priority: 50, enabled: true, auth_type: "none", auth_header_name: "X-API-Key", secret: "", clear_secret: false, tool_mappings: "{}" };

export const factDataSources = [
  { id: "fmp", badge: "US", name: "FMP MCP / REST", description: "美股行情、财务报表、估值指标、公司基础数据", tone: "amber" },
  { id: "sec", badge: "OFFICIAL", name: "SEC EDGAR", description: "10-K / 10-Q / 8-K / Form 4 等官方监管文件", tone: "cyan" },
  { id: "cn-news", badge: "CN / NEWS", name: "AkShare / RSS", description: "A股市场数据、公告/新闻抓取与补充事件来源", tone: "amber" },
  { id: "crypto", badge: "CRYPTO", name: "CoinGecko / DeFiLlama / CCXT", description: "Crypto 价格、市值、链上 / DeFi 指标与交易所数据", tone: "cyan" },
] as const;

type FmpFactStatus = "checking" | "mcp" | "rest" | "unconfigured";

export function FactDataSources({ fmpStatus = "checking" }: { fmpStatus?: FmpFactStatus }) {
  const fmpLabel = fmpStatus === "mcp" ? "MCP / REST 已启用" : fmpStatus === "rest" ? "REST 已配置" : fmpStatus === "unconfigured" ? "待配置" : "检测中";
  return <section className="fact-sources" aria-labelledby="fact-sources-title">
    <h3 id="fact-sources-title"><span />事实数据源</h3>
    <div className="fact-source-list">{factDataSources.map((source) => <article className={`fact-source-card ${source.tone}`} key={source.id}>
      <span className="fact-source-badge">{source.badge}</span>
      <strong>{source.name}</strong>
      <p>{source.description}</p>
      <small className={source.id === "fmp" && fmpStatus === "unconfigured" ? "pending" : ""}>{source.id === "fmp" ? fmpLabel : "内置启用"}</small>
    </article>)}</div>
  </section>;
}

function sourceDraft(source?: McpSource): SourceDraft {
  return source ? { name: source.name, url: source.url, description: source.description, priority: source.priority, enabled: source.enabled, auth_type: source.auth_type, auth_header_name: source.auth_header_name || "X-API-Key", secret: "", clear_secret: false, tool_mappings: JSON.stringify(source.tool_mappings, null, 2) } : { ...blankSource };
}

export function SourcesPage({ apiBase }: { apiBase: string }) {
  const [token, setToken] = useState(readToken);
  const [items, setItems] = useState<McpSource[]>([]);
  const [fmpStatus, setFmpStatus] = useState<FmpFactStatus>("checking");
  const [editing, setEditing] = useState<string | "new" | null>(null);
  const [draft, setDraft] = useState<SourceDraft>(sourceDraft());
  const [message, setMessage] = useState("");
  const headers = useMemo(() => ({ "Content-Type": "application/json", "X-Admin-Token": token }), [token]);
  async function load() {
    if (!token) return;
    const response = await fetch(`${apiBase}/api/v1/admin/mcp-sources`, { headers });
    if (response.status === 401) { setMessage("管理员令牌无效。"); return; }
    if (response.ok) { setItems(await response.json() as McpSource[]); setMessage(""); }
  }
  useEffect(() => {
    fetch(`${apiBase}/health`).then((response) => response.json()).then((health: { fmp_configured?: boolean; fmp_mcp_configured?: boolean }) => {
      setFmpStatus(health.fmp_mcp_configured ? "mcp" : health.fmp_configured ? "rest" : "unconfigured");
    }).catch(() => setFmpStatus("unconfigured"));
  }, [apiBase]);
  useEffect(() => { load(); }, [token]); // eslint-disable-line react-hooks/exhaustive-deps
  async function action(id: string, kind: "test" | "discover") {
    setMessage("正在连接 MCP 来源…");
    const response = await fetch(`${apiBase}/api/v1/admin/mcp-sources/${id}/${kind}`, { method: "POST", headers });
    const body = await response.json(); setMessage(response.ok ? `${kind === "test" ? "连接测试" : "工具发现"}完成。` : body.detail || "操作失败"); await load();
  }
  async function toggle(item: McpSource) {
    await fetch(`${apiBase}/api/v1/admin/mcp-sources/${item.id}/enabled`, { method: "PATCH", headers, body: JSON.stringify({ enabled: !item.enabled }) }); await load();
  }
  async function save(event: FormEvent) {
    event.preventDefault();
    try {
      const payload = { ...draft, tool_mappings: JSON.parse(draft.tool_mappings), secret: draft.secret || null };
      const url = editing === "new" ? `${apiBase}/api/v1/admin/mcp-sources` : `${apiBase}/api/v1/admin/mcp-sources/${editing}`;
      const response = await fetch(url, { method: editing === "new" ? "POST" : "PUT", headers, body: JSON.stringify(payload) });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || "保存失败");
      setEditing(null); setMessage("来源配置已保存并热生效。"); await load();
    } catch (reason) { setMessage(reason instanceof Error ? reason.message : "保存失败"); }
  }
  async function remove(item: McpSource) {
    if (item.managed || !window.confirm(`删除数据源 ${item.name}？`)) return;
    await fetch(`${apiBase}/api/v1/admin/mcp-sources/${item.id}`, { method: "DELETE", headers }); await load();
  }
  return <section className="app-page sources-page">
    <PageHeading eyebrow="RESEARCH DATA FABRIC" title="数据源" copy="统一查看研究使用的事实来源，并管理可热更新的远程 Streamable HTTP MCP。" />
    <FactDataSources fmpStatus={fmpStatus} />
    <div className="managed-sources-heading"><span>CONTROLLED MCP REGISTRY</span><h3>可管理 MCP 来源</h3><p>管理员可启停来源、测试连接、发现工具并维护受控用途映射。</p></div>
    <AdminUnlock token={token} onToken={setToken} />
    {message && <div className="page-message">{message}</div>}
    {token && <>
      <div className="page-toolbar"><button type="button" onClick={() => { setEditing("new"); setDraft(sourceDraft()); }}>新增 MCP 来源</button><button type="button" onClick={load}>刷新</button></div>
      <div className="source-list">{items.map((item) => <article className="source-card" key={item.id}>
        <div className="source-card-main"><div><span className={`health-dot ${item.last_status}`} /> <strong>{item.name}</strong>{item.managed && <small>内置</small>}<p>{item.description || item.url}</p><code>{item.url}</code></div><div><span>优先级 {item.priority}</span><span>{item.discovered_tools.length} 个工具</span><span>{item.secret_configured ? "凭据已配置" : "无凭据"}</span></div></div>
        {item.last_error && <p className="page-error">{item.last_error}</p>}
        <div className="card-actions"><button type="button" onClick={() => toggle(item)}>{item.enabled ? "关闭" : "启用"}</button><button type="button" onClick={() => action(item.id, "test")}>连接测试</button><button type="button" onClick={() => action(item.id, "discover")}>工具发现</button><button type="button" onClick={() => { setEditing(item.id); setDraft(sourceDraft(item)); }}>编辑</button>{!item.managed && <button type="button" className="danger" onClick={() => remove(item)}>删除</button>}</div>
        {item.discovered_tools.length > 0 && <details><summary>已发现工具与 Schema</summary>{item.discovered_tools.map((tool) => <pre key={tool.name}>{tool.name}\n{tool.description}\n{JSON.stringify(tool.input_schema, null, 2)}</pre>)}</details>}
      </article>)}</div>
    </>}
    {editing && <div className="modal-backdrop" onClick={() => setEditing(null)}><form className="modal source-editor" onSubmit={save} onClick={(e) => e.stopPropagation()}><button type="button" className="close" onClick={() => setEditing(null)}>×</button><h2>{editing === "new" ? "新增数据源" : "编辑数据源"}</h2>
      <label>名称<input required value={draft.name} onChange={(e) => setDraft({ ...draft, name: e.target.value })} /></label><label>Streamable HTTP URL<input required type="url" value={draft.url} onChange={(e) => setDraft({ ...draft, url: e.target.value })} /></label><label>描述<textarea value={draft.description} onChange={(e) => setDraft({ ...draft, description: e.target.value })} /></label><label>优先级<input type="number" min="0" max="1000" value={draft.priority} onChange={(e) => setDraft({ ...draft, priority: Number(e.target.value) })} /></label><label>认证<select value={draft.auth_type} onChange={(e) => setDraft({ ...draft, auth_type: e.target.value })}><option value="none">无</option><option value="bearer">Bearer</option><option value="api_key_header">API Key Header</option></select></label>{draft.auth_type === "api_key_header" && <label>Header 名称<input value={draft.auth_header_name} onChange={(e) => setDraft({ ...draft, auth_header_name: e.target.value })} /></label>}{draft.auth_type !== "none" && <><label>新凭据<input type="password" value={draft.secret} placeholder="留空则保留现有凭据" onChange={(e) => setDraft({ ...draft, secret: e.target.value })} /></label><label className="inline-check"><input type="checkbox" checked={draft.clear_secret} onChange={(e) => setDraft({ ...draft, clear_secret: e.target.checked })} />清除现有凭据</label></>}<label>用途映射 JSON<textarea className="json-editor" value={draft.tool_mappings} onChange={(e) => setDraft({ ...draft, tool_mappings: e.target.value })} /></label><button type="submit">保存配置</button>
    </form></div>}
  </section>;
}

type SearchItem = { title: string; url: string; snippet: string; source: string; domain: string; published_at: string | null };

export function SearchPage({ apiBase }: { apiBase: string }) {
  const [token, setToken] = useState(readToken);
  const [sources, setSources] = useState<McpSource[]>([]);
  const [query, setQuery] = useState(""); const [sourceId, setSourceId] = useState("");
  const [language, setLanguage] = useState("zh-CN"); const [timeRange, setTimeRange] = useState(""); const [limit, setLimit] = useState(10);
  const [items, setItems] = useState<SearchItem[]>([]); const [errors, setErrors] = useState<Array<{ source: string; error: string }>>([]); const [loading, setLoading] = useState(false);
  const headers = { "Content-Type": "application/json", "X-Admin-Token": token };
  useEffect(() => { if (token) fetch(`${apiBase}/api/v1/admin/mcp-sources`, { headers }).then((r) => r.ok ? r.json() : []).then(setSources); }, [token]); // eslint-disable-line react-hooks/exhaustive-deps
  async function search(event: FormEvent) { event.preventDefault(); setLoading(true); setErrors([]); try { const response = await fetch(`${apiBase}/api/v1/admin/search`, { method: "POST", headers, body: JSON.stringify({ query, source_id: sourceId || null, language, time_range: timeRange, limit }) }); const payload = await response.json(); if (!response.ok) throw new Error(payload.detail || "搜索失败"); setItems(payload.items); setErrors(payload.errors); } catch (reason) { setErrors([{ source: "系统", error: reason instanceof Error ? reason.message : "搜索失败" }]); } finally { setLoading(false); } }
  return <section className="app-page search-page"><PageHeading eyebrow="NETWORK VERIFICATION" title="搜索引擎" copy="通过已启用 MCP 来源手动验证本地模型结论，结果始终保留原始链接。" /><AdminUnlock token={token} onToken={setToken} />{token && <form className="search-form" onSubmit={search}><input required aria-label="搜索查询" placeholder="输入需要验证的问题" value={query} onChange={(e) => setQuery(e.target.value)} /><select aria-label="搜索来源" value={sourceId} onChange={(e) => setSourceId(e.target.value)}><option value="">全部启用来源</option>{sources.filter((item) => item.enabled && "web_search" in item.tool_mappings).map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select><select aria-label="语言" value={language} onChange={(e) => setLanguage(e.target.value)}><option value="zh-CN">中文</option><option value="en">英文</option><option value="all">不限</option></select><select aria-label="时间范围" value={timeRange} onChange={(e) => setTimeRange(e.target.value)}><option value="">不限时间</option><option value="day">24 小时</option><option value="week">一周</option><option value="month">一月</option><option value="year">一年</option></select><label>结果数<input type="number" min="1" max="20" value={limit} onChange={(e) => setLimit(Number(e.target.value))} /></label><button disabled={loading}>{loading ? "正在搜索…" : "搜索验证"}</button></form>}{errors.map((item) => <div className="page-error" key={`${item.source}-${item.error}`}>{item.source}: {item.error}</div>)}<div className="search-results">{items.map((item) => <article key={item.url}><span>{item.source} · {item.domain}{item.published_at ? ` · ${new Date(item.published_at).toLocaleString("zh-CN")}` : ""}</span><h3><a href={item.url} target="_blank" rel="noreferrer">{item.title}</a></h3><p>{item.snippet}</p></article>)}</div></section>;
}

export function WeknoraPage({ apiBase }: { apiBase: string }) {
  const [token, setToken] = useState(readToken); const [url, setUrl] = useState("http://10.15.0.28/"); const [draft, setDraft] = useState(url); const [message, setMessage] = useState(""); const [failed, setFailed] = useState(false);
  useEffect(() => { fetch(`${apiBase}/api/v1/integrations/weknora`).then((r) => r.json()).then((payload: { url: string }) => { setUrl(payload.url); setDraft(payload.url); }).catch(() => setMessage("无法读取 WeKnora 配置，已使用默认地址。")); }, [apiBase]);
  const headers = { "Content-Type": "application/json", "X-Admin-Token": token };
  async function save() { const response = await fetch(`${apiBase}/api/v1/admin/integrations/weknora`, { method: "PUT", headers, body: JSON.stringify({ url: draft }) }); if (response.ok) { setUrl((await response.json()).url); setFailed(false); setMessage("WeKnora 地址已保存。"); } else setMessage("保存失败，请检查管理员令牌和 URL。"); }
  async function test() { const response = await fetch(`${apiBase}/api/v1/admin/integrations/weknora/test`, { method: "POST", headers, body: JSON.stringify({ url: draft }) }); const payload = await response.json(); setMessage(payload.ok ? `连接成功（HTTP ${payload.status_code}）。` : `连接失败：${payload.error || payload.status_code}`); }
  return <section className="app-page weknora-page"><PageHeading eyebrow="LOCAL KNOWLEDGE WORKBENCH" title="WeKnora" copy="内嵌本地知识库工作台；若服务禁止 iframe，可在新窗口中继续。" /><div className="weknora-toolbar"><a href={url} target="_blank" rel="noreferrer">新窗口打开</a><span>{failed ? "内嵌加载失败，请使用“新窗口打开”。" : "若下方为空白或提示拒绝连接，请使用“新窗口打开”。"}</span></div><div className="weknora-frame"><iframe title="WeKnora 本地知识库" src={url} onError={() => setFailed(true)} /></div><AdminUnlock token={token} onToken={setToken} />{token && <div className="integration-editor"><label>WeKnora URL<input type="url" value={draft} onChange={(e) => setDraft(e.target.value)} /></label><button type="button" onClick={test}>连接测试</button><button type="button" onClick={save}>保存</button></div>}{message && <div className="page-message">{message}</div>}</section>;
}

export function RoutedPage({ route, apiBase }: { route: Exclude<AppRoute, "home">; apiBase: string }) {
  if (route === "model-logs") return <ModelLogsPage apiBase={apiBase} onBack={() => { window.location.hash = "/home"; }} embedded />;
  if (route === "conclusions") return <ConclusionsPage apiBase={apiBase} />;
  if (route === "sources") return <SourcesPage apiBase={apiBase} />;
  if (route === "search") return <SearchPage apiBase={apiBase} />;
  return <WeknoraPage apiBase={apiBase} />;
}
