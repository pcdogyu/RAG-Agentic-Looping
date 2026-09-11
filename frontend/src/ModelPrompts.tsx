import { useCallback, useEffect, useMemo, useState } from "react";

export type ModelPromptItem = {
  key: string;
  name: string;
  description: string;
  model_role: string;
  default_prompt: string;
  required_suffix: string;
  custom_prompt: string | null;
  effective_prompt: string;
  has_override: boolean;
  version: number;
  updated_by?: string;
  updated_at?: string;
};

type ModelPromptResponse = { items: ModelPromptItem[]; max_prompt_bytes: number };

export default function ModelPromptsPage({ apiBase }: { apiBase: string }) {
  const [items, setItems] = useState<ModelPromptItem[]>([]);
  const [selectedKey, setSelectedKey] = useState("");
  const [draft, setDraft] = useState("");
  const [maxBytes, setMaxBytes] = useState(32768);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const selected = useMemo(() => items.find((item) => item.key === selectedKey) || items[0], [items, selectedKey]);
  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch(`${apiBase}/go/model-prompts`);
      if (!response.ok) throw new Error("模型提示词读取失败");
      const payload = await response.json() as ModelPromptResponse;
      setItems(payload.items || []);
      setMaxBytes(payload.max_prompt_bytes || 32768);
      setSelectedKey((current) => current || payload.items?.[0]?.key || "");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "模型提示词读取失败");
    } finally {
      setLoading(false);
    }
  }, [apiBase]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (selected) setDraft(selected.custom_prompt ?? selected.default_prompt);
  }, [selected?.key, selected?.version]);

  const replaceItem = (next: ModelPromptItem) => {
    setItems((current) => current.map((item) => item.key === next.key ? next : item));
  };

  async function save() {
    if (!selected || !draft.trim()) return;
    setBusy(true);
    setMessage("");
    setError("");
    try {
      const response = await fetch(`${apiBase}/go/model-prompts/${encodeURIComponent(selected.key)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: draft, expected_version: selected.version }),
      });
      if (response.status === 409) throw new Error("提示词已被其他请求修改，请重新读取后再保存。");
      if (!response.ok) throw new Error("提示词保存失败");
      const next = await response.json() as ModelPromptItem;
      replaceItem(next);
      setMessage(`${next.name}已保存，新任务将立即使用版本 ${next.version}。`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "提示词保存失败");
    } finally {
      setBusy(false);
    }
  }

  async function reset() {
    if (!selected) return;
    setBusy(true);
    setMessage("");
    setError("");
    try {
      const response = await fetch(`${apiBase}/go/model-prompts/${encodeURIComponent(selected.key)}`, {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ expected_version: selected.version }),
      });
      if (response.status === 409) throw new Error("提示词已被其他请求修改，请重新读取后再重置。");
      if (!response.ok) throw new Error("恢复默认提示词失败");
      const next = await response.json() as ModelPromptItem;
      replaceItem(next);
      setMessage(`${next.name}已恢复默认提示词，当前版本 ${next.version}。`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "恢复默认提示词失败");
    } finally {
      setBusy(false);
    }
  }

  const byteLength = new TextEncoder().encode(draft).length;
  const dirty = !!selected && draft !== (selected.custom_prompt ?? selected.default_prompt);

  return <section className="app-page model-prompts-page">
    <div className="page-heading"><p className="eyebrow">MODEL PROMPT CONTROL</p><h2>模型提示词</h2><p>修改各模型任务的新调用提示词；保存后由 Worker 从数据库即时读取，无需重启。</p></div>
    {loading && <div className="model-prompt-state">正在读取模型提示词…</div>}
    {error && <div className="model-prompt-error" role="alert">{error}</div>}
    {message && <div className="model-prompt-message" role="status">{message}</div>}
    {!loading && selected && <div className="model-prompt-workbench">
      <aside className="model-prompt-list" aria-label="模型提示词列表">
        {items.map((item) => <button type="button" key={item.key} className={item.key === selected.key ? "active" : ""} onClick={() => setSelectedKey(item.key)}>
          <span>{item.model_role}</span><strong>{item.name}</strong><small>{item.has_override ? `自定义 · v${item.version}` : item.version ? `默认 · v${item.version}` : "系统默认"}</small>
        </button>)}
      </aside>
      <div className="model-prompt-editor">
        <header><div><span>{selected.key} · {selected.model_role}</span><h3>{selected.name}</h3><p>{selected.description}</p></div><strong>{selected.has_override ? "自定义提示词生效中" : "系统默认提示词"}</strong></header>
        <label>可编辑系统提示词
          <textarea value={draft} onChange={(event) => setDraft(event.target.value)} spellCheck={false} aria-label="可编辑系统提示词" />
        </label>
        <div className="model-prompt-counter"><span>{byteLength.toLocaleString()} / {maxBytes.toLocaleString()} bytes</span><span>{dirty ? "有未保存修改" : "内容已同步"}</span></div>
        <details><summary>查看不可编辑的安全边界</summary><pre>{selected.required_suffix}</pre></details>
        <details><summary>查看系统默认提示词</summary><pre>{selected.default_prompt}</pre></details>
        <footer><button type="button" onClick={() => void load()} disabled={busy}>重新读取</button><button type="button" onClick={() => void reset()} disabled={busy || (!selected.has_override && !dirty)}>恢复默认</button><button className="primary" type="button" onClick={() => void save()} disabled={busy || !dirty || !draft.trim() || byteLength > maxBytes}>{busy ? "保存中…" : "保存并立即生效"}</button></footer>
        <small>版本 {selected.version || 0}{selected.updated_at ? ` · 最后更新 ${new Date(selected.updated_at).toLocaleString()}` : " · 尚无人工修改"}。历史模型日志保留当时实际发送的提示词，不会被改写。</small>
      </div>
    </div>}
  </section>;
}
