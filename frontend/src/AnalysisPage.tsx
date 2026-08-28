import { useEffect, useState } from "react";

type AnalysisStep = {
  phase: string;
  status: string;
  executor: string;
  model: string | null;
  summary: string;
  metrics: Record<string, unknown>;
  occurred_at: string;
};

export type AnalysisLog = {
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
    raw_score?: number;
    confidence: number;
    evidence_complete: boolean;
    directional_evidence_complete?: boolean;
    signal_status?: "technical_failure" | "insufficient_evidence" | "neutral" | "directional";
    scoring_version?: string;
    horizon_unit?: "calendar_days" | "trading_sessions";
    horizon_days?: number;
    fact_confidence?: number;
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

const labels: Record<string, string> = {
  strongly_bullish: "强烈看多",
  bullish: "看多",
  watch: "观望",
  bearish: "看空",
  strongly_bearish: "强烈看空",
  insufficient_evidence: "证据不足",
  technical_failure: "技术失败",
  neutral: "中性信号",
  directional: "方向信号",
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
  asset_mapping_queue: "7B 股票映射入队",
  research_queue: "研究任务入队",
  market_factor_refresh_queue: "市场反应因子重评入队",
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
      ? "7B 股票映射失败，系统未生成或猜测证券代码。"
      : "7B 股票映射模型正在识别并验证新闻中明确提及的证券标的。";
  }
  if (status === "unmapped") return "该新闻尚未映射到可研究标的。";
  return "研究任务正在排队或处理中，结果会自动更新。";
}

function time(value: string) {
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(
    new Date(value),
  );
}

export function AnalysisTraceList({ logs }: { logs: AnalysisLog[] }) {
  const [expandedLog, setExpandedLog] = useState<string | null>(() => logs[0]?.id || null);

  useEffect(() => {
    setExpandedLog((current) => (
      logs.some((item) => item.id === current) ? current : logs[0]?.id || null
    ));
  }, [logs]);

  if (!logs.length) {
    return <div className="empty"><i>◇</i><p>完成新闻扫描后，这里会显示可审计的分析过程。</p></div>;
  }

  return <div className="trace-list">
    {logs.map((log) => {
      const open = expandedLog === log.id;
      return <article className={`trace-card ${open ? "open" : ""}`} key={log.id}>
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

        {open && <div className="trace-detail">
          <div className="trace-context">
            <div>
              <h3>新闻来源</h3>
              {log.news.length === 0 && <p className="muted">此研究没有关联新闻事件。</p>}
              {log.news.map((item) => <a href={item.url} target="_blank" rel="noreferrer" key={item.id}>
                <strong>{item.title}</strong>
                <span>{item.source} · {time(item.published_at)}</span>
              </a>)}
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
            {log.steps.map((step, index) => <div className={`trace-step ${step.status}`} key={`${step.phase}-${step.occurred_at}-${index}`}>
              <i />
              <div>
                <div className="trace-step-title">
                  <strong>{phaseLabels[step.phase] || step.phase}</strong>
                  <span>{step.model || step.executor}</span>
                  <time>{time(step.occurred_at)}</time>
                </div>
                <p>{step.summary}</p>
              </div>
            </div>)}
          </div>

          {log.result?.kind === "asset_recommendation" ? <div className="trace-result">
            <div><span>信号状态</span><strong>{labels[log.result.signal_status || ""] || "旧版结论"}</strong></div>
            <div><span>最终结果</span><strong>{labels[log.result.rating] || log.result.rating}</strong></div>
            <div><span>{log.result.scoring_version === "short-term-impact-v1" ? "影响分" : "发布分"}</span><strong>{log.result.score > 0 ? "+" : ""}{log.result.score}</strong></div>
            {log.result.scoring_version === "short-term-impact-v1" ? <>
              <div><span>新闻事实置信度</span><strong>{Math.round((log.result.fact_confidence ?? log.result.confidence) * 100)}%</strong></div>
              <div><span>评级置信度</span><strong>{Math.round(log.result.confidence * 100)}%</strong></div>
              <div><span>研究期限</span><strong>未来 1–{log.result.horizon_days ?? 3} 个交易日</strong></div>
            </> : <>
              <div><span>程序原始分</span><strong>{(log.result.raw_score ?? log.result.score) > 0 ? "+" : ""}{log.result.raw_score ?? log.result.score}</strong></div>
              <div><span>置信度</span><strong>{Math.round(log.result.confidence * 100)}%</strong></div>
              <div><span>资料覆盖</span><strong>{log.result.evidence_complete ? "完整" : "不足"}</strong></div>
              <div><span>方向证据</span><strong>{log.result.directional_evidence_complete ? "通过" : "未通过"}</strong></div>
            </>}
            <p>{log.result.summary}</p>
          </div> : log.result?.kind === "event_report" ? <div className="trace-result event-report-result">
            <div><span>最终结果</span><strong>中性事件研报</strong></div>
            <div><span>置信度</span><strong>{Math.round(log.result.confidence * 100)}%</strong></div>
            <div><span>证据</span><strong>{log.result.evidence_complete ? "完整" : "不足"}</strong></div>
            <div><span>影响范围</span><strong>{[...log.result.affected_markets, ...log.result.affected_sectors].join(" · ") || "待确认"}</strong></div>
            <p>{log.result.summary}</p>
          </div> : <div className="trace-pending">{analysisPendingText(log.status)}</div>}
        </div>}
      </article>;
    })}
  </div>;
}

export default function AnalysisPage({ logs }: { logs: AnalysisLog[] }) {
  return <section className="app-page analysis-page">
    <div className="page-heading">
      <p className="eyebrow">ANALYSIS AUDIT TRAIL</p>
      <h2>分析链路</h2>
      <p>模型、证据与校验审计；展示最近 10 条分析记录并随系统状态自动更新。</p>
    </div>
    <section className="panel trace-panel">
      <AnalysisTraceList logs={logs} />
    </section>
  </section>;
}
