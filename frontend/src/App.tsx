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
type TaskStatus = {
  state: string;
  progress?: { phase: string; current: number; total: number };
};

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
};

function money(value: number) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(value);
}

function time(value: string) {
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(
    new Date(value),
  );
}

export default function App() {
  const [snapshot, setSnapshot] = useState<Snapshot>({ events: [], runs: [], recommendations: [] });
  const [portfolio, setPortfolio] = useState<Portfolio | null>(null);
  const [health, setHealth] = useState<Record<string, unknown>>({});
  const [scanBusy, setScanBusy] = useState(false);
  const [scanLabel, setScanLabel] = useState("立即扫描");
  const [selected, setSelected] = useState<Recommendation | null>(null);

  const refresh = useCallback(async () => {
    const [events, runs, recommendations, portfolioData, healthData] = await Promise.all([
      fetch(`${API}/api/v1/events?limit=30`).then((r) => r.json()),
      fetch(`${API}/api/v1/research-runs?limit=20`).then((r) => r.json()),
      fetch(`${API}/api/v1/recommendations?limit=20`).then((r) => r.json()),
      fetch(`${API}/api/v1/portfolio`).then((r) => r.json()),
      fetch(`${API}/health`).then((r) => r.json()),
    ]);
    setSnapshot({ events, runs, recommendations });
    setPortfolio(portfolioData);
    setHealth(healthData);
  }, []);

  useEffect(() => {
    refresh().catch(() => undefined);
    const stream = new EventSource(`${API}/api/v1/stream`);
    stream.addEventListener("snapshot", (event) => setSnapshot(JSON.parse((event as MessageEvent).data)));
    const timer = window.setInterval(() => {
      fetch(`${API}/api/v1/portfolio`).then((r) => r.json()).then(setPortfolio).catch(() => undefined);
    }, 15000);
    return () => {
      stream.close();
      window.clearInterval(timer);
    };
  }, [refresh]);

  async function scan() {
    setScanBusy(true);
    setScanLabel("正在发现新闻…");
    try {
      const response = await fetch(`${API}/api/v1/scan`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ background: true }),
      });
      if (!response.ok) throw new Error("scan request failed");
      const queued = await response.json() as { task_id: string };
      const deadline = Date.now() + 30 * 60 * 1000;
      while (Date.now() < deadline) {
        const taskResponse = await fetch(`${API}/api/v1/tasks/${queued.task_id}`);
        if (!taskResponse.ok) throw new Error("task status failed");
        const task = await taskResponse.json() as TaskStatus;
        if (task.state === "PROGRESS" && task.progress) {
          setScanLabel(`事件抽取 ${task.progress.current}/${task.progress.total}`);
        } else if (task.state === "SUCCESS") {
          setScanLabel("扫描完成");
          await refresh();
          return;
        } else if (task.state === "FAILURE") {
          throw new Error("scan task failed");
        } else {
          setScanLabel("扫描排队中…");
        }
        await new Promise((resolve) => window.setTimeout(resolve, 2000));
      }
      throw new Error("scan task timed out");
    } catch {
      setScanLabel("扫描失败，请重试");
    } finally {
      setScanBusy(false);
      window.setTimeout(() => setScanLabel("立即扫描"), 4000);
    }
  }

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
          <div className={`status ${health.ollama ? "online" : "offline"}`}>
            <i /> Ollama {health.ollama ? "在线" : "离线"}
          </div>
          <button onClick={scan} disabled={scanBusy}>{scanLabel}</button>
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
          <PanelTitle title="市场事件" meta="20 分钟增量循环" />
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

        <div className="panel portfolio-panel">
          <PanelTitle title="模拟持仓" meta={`加密权重 ${Math.round((portfolio?.crypto_weight || 0) * 100)}%`} />
          {(!portfolio || portfolio.positions.length === 0) && <Empty text="默认关闭自动模拟下单，可从已验证建议手动创建。" />}
          {portfolio?.positions.map((position) => (
            <div className="position" key={position.asset.symbol}>
              <div><strong>{position.asset.symbol}</strong><span>{position.asset.name}</span></div>
              <strong>{money(position.market_value_usd)}</strong>
              <span className={position.unrealized_pnl_usd >= 0 ? "positive-text" : "negative-text"}>
                {position.unrealized_pnl_usd >= 0 ? "+" : ""}{money(position.unrealized_pnl_usd)}
              </span>
              <span>{Math.round(position.weight * 1000) / 10}%</span>
            </div>
          ))}
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
