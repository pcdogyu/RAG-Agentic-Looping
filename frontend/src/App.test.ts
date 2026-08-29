import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import App, {
  createInitialHealthTracking,
  formatCountdown,
  normalizeTheme,
  scanButtonText,
  updateHealthTracking,
} from "./App";
import AnalysisPage, {
  analysisPendingText,
  AnalysisTraceList,
  type AnalysisLog,
} from "./AnalysisPage";
import BuildFooter, { buildInfo } from "./BuildFooter";
import {
  applyCancelledTaskTombstone,
  changedTargetDesktopColumns,
  changedTargetLatestRecommendationId,
  ChangedTargetGrid,
  ChangedTargetsContent,
  ChangedTargetsPage,
  ConclusionDetailModal,
  type ConclusionDetail,
  EventConclusionCard,
  EventConclusionDetailModal,
  type EventConclusionDetail,
  factSourceGroupDefinitions,
  formatQueueDuration,
  ModelInferenceQueuePanel,
  modelQueuePanelColumns,
  modelQueueInstances,
  ModelQueueTaskGrid,
  type ModelQueueOverviewItem,
  type ModelQueueTask,
  navigationGroups,
  newsBoardRefreshIntervalMs,
  newsBoardStatusLabels,
  newsSourceDesktopColumns,
  NewsSourcePanel,
  queueDesktopColumns,
  QueueGrid,
  NewsExtractionList,
  queueRefreshIntervalMs,
  researchViewsRefreshIntervalMs,
  type ResearchConclusionItem,
  routeFromHash,
  TargetChangeGrid,
  targetChangeResearchKey,
  type TargetChange,
  TopNavigation,
  UnifiedModelQueuePanel,
  removeTasksFromQueueOverview,
} from "./AppPages";
import ModelLogsPage, {
  buildModelLogQuery,
  fidelityLabel,
  isModelLogsHash,
} from "./ModelLogs";

const baseStatus = {
  state: "idle",
  task_id: null,
  phase: "completed",
  paused_from_phase: null,
  current: 2,
  total: 2,
  started_at: "2026-08-22T00:00:00Z",
  last_completed_at: "2026-08-22T00:01:00Z",
  next_scan_at: "2026-08-22T00:21:00Z",
  last_result: null,
  last_error: null,
  interval_seconds: 1200,
  server_time: "2026-08-22T00:01:00Z",
};

describe("scan status presentation", () => {
  it("formats a completion-anchored countdown", () => {
    expect(formatCountdown(1200)).toBe("20分00秒");
    expect(scanButtonText(baseStatus, Date.parse("2026-08-22T00:01:01Z"))).toBe(
      "距离下一次扫描 19分59秒",
    );
  });

  it("keeps every active state labeled as scanning", () => {
    expect(scanButtonText({ ...baseStatus, state: "queued" }, Date.now())).toBe(
      "暂停扫描 · 排队中",
    );
    expect(scanButtonText({
      ...baseStatus, state: "running", phase: "extracting", current: 4, total: 12,
    }, Date.now())).toBe("暂停 · 事件归纳 4/12");
    expect(scanButtonText({ ...baseStatus, state: "retrying" }, Date.now())).toBe(
      "暂停扫描 · 正在重试",
    );
  });

  it("offers resume with preserved progress while paused", () => {
    expect(scanButtonText({
      ...baseStatus,
      state: "paused",
      phase: "paused",
      paused_from_phase: "extracting",
      current: 4,
      total: 12,
    }, Date.now())).toBe("继续扫描 · 已暂停 4/12");
  });
});

describe("Ollama model availability", () => {
  it("starts in checking and turns red on the third unreachable poll", () => {
    let tracking = createInitialHealthTracking();
    expect(tracking.ollama.state).toBe("checking");

    tracking = updateHealthTracking(tracking, null);
    expect(tracking.ollama).toEqual({ failures: 1, state: "checking" });
    expect(tracking.models["qwen2.5:3b"].state).toBe("checking");

    tracking = updateHealthTracking(tracking, { ollama: false, models: [] });
    expect(tracking.ollama).toEqual({ failures: 2, state: "checking" });

    tracking = updateHealthTracking(tracking, null);
    expect(tracking.ollama).toEqual({ failures: 3, state: "offline" });
    expect(tracking.models["qwen2.5:3b"].state).toBe("offline");
  });

  it("recovers every available connection immediately after three failures", () => {
    let tracking = createInitialHealthTracking();
    for (let attempt = 0; attempt < 3; attempt += 1) {
      tracking = updateHealthTracking(tracking, null);
    }

    tracking = updateHealthTracking(tracking, {
      ollama: true,
      models: ["qwen2.5:3b", "qwen2.5:7b", "qwen2.5-coder:7b"],
    });

    expect(tracking.ollama).toEqual({ failures: 0, state: "available" });
    expect(tracking.models["qwen2.5:7b"]).toEqual({ failures: 0, state: "available" });
    expect(tracking.models["qwen2.5-coder:7b"]).toEqual({ failures: 0, state: "available" });
  });

  it("tracks missing models independently from Ollama and installed models", () => {
    const health = {
      ollama: true,
      models: ["qwen2.5:3b", "qwen2.5:7b"],
    };
    let tracking = createInitialHealthTracking();
    for (let attempt = 0; attempt < 3; attempt += 1) {
      tracking = updateHealthTracking(tracking, health);
    }

    expect(tracking.ollama.state).toBe("available");
    expect(tracking.models["qwen2.5:3b"].state).toBe("available");
    expect(tracking.models["qwen2.5-coder:7b"]).toEqual({ failures: 3, state: "missing" });
  });

  it("matches Ollama model names case-insensitively", () => {
    const tracking = updateHealthTracking(
      createInitialHealthTracking(),
      { ollama: true, models: ["QWEN2.5:7B"] },
    );
    expect(tracking.models["qwen2.5:7b"].state).toBe("available");
  });
});

describe("theme selection", () => {
  it("defaults missing or invalid stored values to dark", () => {
    expect(normalizeTheme(null)).toBe("dark");
    expect(normalizeTheme("system")).toBe("dark");
  });

  it("restores either persisted theme", () => {
    expect(normalizeTheme("dark")).toBe("dark");
    expect(normalizeTheme("light")).toBe("light");
  });
});

describe("build footer", () => {
  it("renders the author and Git build coordinates", () => {
    const markup = renderToStaticMarkup(createElement(BuildFooter));
    expect(markup).toContain("Code by");
    expect(markup).toContain("mailto:Yuhao@jiansutech.com");
    expect(markup).toContain(buildInfo.commitTime);
    expect(markup).toContain(buildInfo.branch);
    expect(markup).toContain(buildInfo.commitId.slice(0, 12));
  });
});

describe("analysis mapping states", () => {
  it("explains active and failed mapping without implying deep research started", () => {
    expect(analysisPendingText("mapping_queued")).toContain("正在识别并验证");
    expect(analysisPendingText("mapping_failed")).toContain("未生成或猜测证券代码");
  });

  it("keeps genuinely unmapped events explicit", () => {
    expect(analysisPendingText("unmapped")).toBe("该新闻尚未映射到可研究标的。");
  });

  it("renders mapping progress and failure explanations inside the analysis page", () => {
    const createMappingLog = (status: "mapping_running" | "mapping_failed"): AnalysisLog => ({
      id: status,
      event_id: "event-mapping",
      run_id: null,
      event_research_run_id: null,
      status,
      updated_at: "2026-08-26T01:00:00Z",
      news: [],
      event: { headline: "待映射新闻", event_type: "earnings", direct_impact: "待确认", priority: 0.5 },
      asset: null,
      models: ["qwen2.5:7b"],
      steps: [],
      result: null,
    });

    const runningMarkup = renderToStaticMarkup(createElement(AnalysisTraceList, {
      logs: [createMappingLog("mapping_running")],
    }));
    const failedMarkup = renderToStaticMarkup(createElement(AnalysisTraceList, {
      logs: [createMappingLog("mapping_failed")],
    }));

    expect(runningMarkup).toContain("正在识别并验证");
    expect(failedMarkup).toContain("未生成或猜测证券代码");
  });

  it("renders an empty standalone analysis page without restoring the home panel", () => {
    const pageMarkup = renderToStaticMarkup(createElement(AnalysisPage, { logs: [] }));
    const homeMarkup = renderToStaticMarkup(createElement(App));

    expect(pageMarkup).toContain("ANALYSIS AUDIT TRAIL");
    expect(pageMarkup).toContain("完成新闻扫描后");
    expect(homeMarkup).not.toContain('class="panel trace-panel"');
  });

  it("renders the latest asset analysis expanded with sources, steps, and result", () => {
    const log = {
      id: "asset-log",
      event_id: "event-1",
      run_id: "run-1",
      event_research_run_id: null,
      status: "completed",
      updated_at: "2026-08-26T01:00:00Z",
      news: [{
        id: "news-1",
        title: "测试公司发布业绩公告",
        source: "官方公告",
        url: "https://example.com/news-1",
        published_at: "2026-08-26T00:30:00Z",
      }],
      event: { headline: "测试事件", event_type: "earnings", direct_impact: "利润增长", priority: 0.8 },
      asset: { symbol: "600000", name: "测试公司", market: "CN" },
      models: ["qwen2.5:7b"],
      steps: [{
        phase: "report_drafting",
        status: "completed",
        executor: "ollama",
        model: "qwen2.5:7b",
        summary: "研究报告已生成。",
        metrics: {},
        occurred_at: "2026-08-26T00:45:00Z",
      }],
      result: {
        kind: "asset_recommendation",
        rating: "bullish",
        score: 42,
        confidence: 0.8,
        evidence_complete: true,
        summary: "证据支持看多结论。",
      },
    } satisfies AnalysisLog;
    const markup = renderToStaticMarkup(createElement(AnalysisTraceList, { logs: [log] }));

    expect(markup).toContain('aria-expanded="true"');
    expect(markup).toContain("测试公司发布业绩公告");
    expect(markup).toContain("研究报告生成");
    expect(markup).toContain("+42");
    expect(markup).toContain("证据支持看多结论");
  });

  it("renders new short-term analysis without legacy evidence-gate fields", () => {
    const log = {
      id: "short-term-log",
      event_id: "event-short-term",
      run_id: "run-short-term",
      event_research_run_id: null,
      status: "completed",
      updated_at: "2026-08-28T01:00:00Z",
      news: [],
      event: { headline: "黄金 ETF 持仓下降", event_type: "fund_flow", direct_impact: "短线偏空", priority: 0.7 },
      asset: { symbol: "GOLD", name: "黄金", market: "COMMODITY" },
      models: ["qwen2.5:7b"],
      steps: [],
      result: {
        kind: "asset_recommendation",
        rating: "watch",
        score: -8,
        raw_score: -8,
        confidence: 0.82,
        fact_confidence: 0.92,
        evidence_complete: false,
        directional_evidence_complete: false,
        signal_status: "neutral",
        scoring_version: "short-term-impact-v1",
        horizon_unit: "trading_sessions",
        horizon_days: 3,
        summary: "影响很小，维持观望。",
      },
    } satisfies AnalysisLog;

    const markup = renderToStaticMarkup(createElement(AnalysisTraceList, { logs: [log] }));

    expect(markup).toContain("影响分");
    expect(markup).toContain("中性");
    expect(markup).toContain("新闻事实置信度");
    expect(markup).toContain("92%");
    expect(markup).toContain("评级置信度");
    expect(markup).toContain("未来 1–3 个交易日");
    expect(markup).not.toContain("方向证据");
    expect(markup).not.toContain("程序原始分");
  });

  it("renders v3 analysis with three core values and a dynamic natural-day horizon", () => {
    const log = {
      id: "v3-log",
      event_id: "event-v3",
      run_id: "run-v3",
      event_research_run_id: null,
      status: "completed",
      updated_at: "2026-08-29T01:00:00Z",
      news: [],
      event: { headline: "并购正式宣布", event_type: "m_and_a", direct_impact: "整合预期", priority: 0.8 },
      asset: { symbol: "ACME", name: "Acme", market: "US" },
      models: ["qwen2.5:7b"],
      steps: [],
      result: {
        kind: "asset_recommendation",
        rating: "strongly_bullish",
        score: 75,
        direction_score: 75,
        confidence: 0.78,
        rating_confidence: 0.78,
        news_confidence: 0.93,
        score_source: "llm",
        evidence_complete: true,
        directional_evidence_complete: true,
        signal_status: "directional",
        scoring_version: "llm-direction-v3",
        horizon_unit: "calendar_days",
        horizon_days: 180,
        summary: "并购传导路径明确。",
      },
    } satisfies AnalysisLog;

    const markup = renderToStaticMarkup(createElement(AnalysisTraceList, { logs: [log] }));

    expect(markup).toContain("方向分");
    expect(markup).toContain("+75");
    expect(markup).toContain("新闻可信度");
    expect(markup).toContain("93%");
    expect(markup).toContain("评级置信度");
    expect(markup).toContain("78%");
    expect(markup).toContain("未来 180 个自然日");
    expect(markup).not.toContain("未来 1–3 个交易日");
  });

  it("renders a completed target-impact report with neutral untradeable details", () => {
    const log = {
      id: "event-log",
      event_id: "event-2",
      run_id: null,
      event_research_run_id: "event-run-2",
      status: "completed",
      updated_at: "2026-08-26T01:00:00Z",
      news: [],
      event: { headline: "行业政策更新", event_type: "policy", direct_impact: "影响行业预期", priority: 0.7 },
      asset: null,
      models: ["qwen2.5:7b"],
      steps: [],
      result: {
        kind: "event_report",
        confidence: 0.72,
        evidence_complete: true,
        summary: "政策影响仍待观察。",
        affected_markets: ["CN"],
        affected_sectors: ["智能制造"],
        scenarios: [],
        catalysts: [],
        risks: [],
        unresolved_questions: [],
        scoring_version: "target-transmission-v2",
        fact_confidence: 0.9,
        trade_status: "untradeable",
        missing_information: ["sanction_scope"],
        impacts: [{
          target_type: "sector",
          target_name: "智能制造",
          asset: null,
          direction: 0,
          score: 0,
          rating: "watch",
          confidence: 0.4,
          transmission_path: ["政策更新", "行业预期"],
          rationale: "范围未确认",
          missing_information: ["sanction_scope"],
          trade_status: "untradeable",
          execution_supported: false,
          technical_failure: false,
        }],
      },
    } satisfies AnalysisLog;
    const markup = renderToStaticMarkup(createElement(AnalysisTraceList, { logs: [log] }));

    expect(markup).toContain("逐目标事件研报");
    expect(markup).toContain("逐目标宏观传导");
    expect(markup).toContain('class="target-impact-scroll"');
    expect(markup).toContain("智能制造");
    expect(markup).toContain("政策更新 → 行业预期");
    expect(markup).toContain("暂不可交易");
    expect(markup).toContain("sanction_scope");
    expect(markup).toContain("政策影响仍待观察");
  });
});

describe("model log navigation and filters", () => {
  it("recognizes only the dedicated model log hash", () => {
    expect(isModelLogsHash("#/model-logs")).toBe(true);
    expect(isModelLogsHash("#model-logs")).toBe(false);
    expect(isModelLogsHash("")).toBe(false);
  });

  it("builds stable API filters with an ISO time boundary", () => {
    const query = buildModelLogQuery({
      range: "7d",
      model: "qwen2.5:7b",
      provider: "ollama",
      operation: "report_drafting",
      status: "completed",
      language: "zh",
      fidelity: "exact",
    }, Date.parse("2026-08-23T00:00:00Z"));
    expect(query.get("start")).toBe("2026-08-16T00:00:00.000Z");
    expect(query.get("model")).toBe("qwen2.5:7b");
    expect(query.get("fidelity")).toBe("exact");
  });

  it.each([
    ["30m", "2026-08-22T23:30:00.000Z"],
    ["1h", "2026-08-22T23:00:00.000Z"],
    ["12h", "2026-08-22T12:00:00.000Z"],
    ["3d", "2026-08-20T00:00:00.000Z"],
  ])("supports the %s model log time range", (range, expectedStart) => {
    const query = buildModelLogQuery({
      range,
      model: "",
      provider: "",
      operation: "",
      status: "",
      language: "",
      fidelity: "",
    }, Date.parse("2026-08-23T00:00:00Z"));

    expect(query.get("start")).toBe(expectedStart);
  });

  it("labels reconstructed history and renders the full-screen shell", () => {
    expect(fidelityLabel("reconstructed")).toBe("历史重建");
    const markup = renderToStaticMarkup(createElement(ModelLogsPage, {
      apiBase: "http://localhost:8000",
      onBack: () => undefined,
    }));
    expect(markup).toContain("模型日志");
    expect(markup).toContain("返回主看板");
    expect(markup).toContain("模型日志筛选");
    expect(markup).toContain("最近30分钟");
    expect(markup).toContain("最近1小时");
    expect(markup).toContain("最近12小时");
    expect(markup).toContain("最近3天");
    expect(markup).toContain("正在读取模型日志");
  });
});

describe("shared hash navigation", () => {
  it("recognizes all eleven routes and falls back to home", () => {
    expect(routeFromHash("#/home")).toBe("home");
    expect(routeFromHash("#/source-filter")).toBe("source-filter");
    expect(routeFromHash("#/conclusions")).toBe("conclusions");
    expect(routeFromHash("#/targets")).toBe("targets");
    expect(routeFromHash("#/sources")).toBe("sources");
    expect(routeFromHash("#/news")).toBe("news");
    expect(routeFromHash("#/queue")).toBe("queue");
    expect(routeFromHash("#/analysis")).toBe("analysis");
    expect(routeFromHash("#/model-logs")).toBe("model-logs");
    expect(routeFromHash("#/search")).toBe("search");
    expect(routeFromHash("#/weknora")).toBe("weknora");
    expect(routeFromHash("#/unknown")).toBe("home");
    expect(routeFromHash("")).toBe("home");
  });

  it("renders grouped menu links in order and exposes the current page accessibly", () => {
    expect(navigationGroups.left.map((item) => item.route)).toEqual([
      "home", "source-filter", "sources", "news", "queue", "analysis", "conclusions", "targets",
    ]);
    expect(navigationGroups.right.map((item) => item.route)).toEqual([
      "model-logs", "search", "weknora",
    ]);
    const markup = renderToStaticMarkup(createElement(TopNavigation, { current: "source-filter" }));
    const newsMarkup = renderToStaticMarkup(createElement(TopNavigation, { current: "news" }));
    const queueMarkup = renderToStaticMarkup(createElement(TopNavigation, { current: "queue" }));
    const analysisMarkup = renderToStaticMarkup(createElement(TopNavigation, { current: "analysis" }));
    const targetsMarkup = renderToStaticMarkup(createElement(TopNavigation, { current: "targets" }));
    expect((markup.match(/<a /g) || []).length).toBe(11);
    expect(markup).toContain('href="#/source-filter" aria-current="page"');
    expect(newsMarkup).toContain('href="#/news" aria-current="page"');
    expect(queueMarkup).toContain('href="#/queue" aria-current="page"');
    expect(analysisMarkup).toContain('href="#/analysis" aria-current="page"');
    expect(targetsMarkup).toContain('href="#/targets" aria-current="page"');
    expect(markup.indexOf("数据源过滤")).toBeLessThan(markup.indexOf(">数据源<"));
    expect(markup.indexOf(">数据源<")).toBeLessThan(markup.indexOf(">新闻<"));
    expect(markup.indexOf(">新闻<")).toBeLessThan(markup.indexOf(">队列<"));
    expect(markup.indexOf(">队列<")).toBeLessThan(markup.indexOf("分析链路"));
    expect(markup.indexOf("分析链路")).toBeLessThan(markup.indexOf("结论"));
    expect(markup.indexOf("结论")).toBeLessThan(markup.indexOf("标的"));
    expect(markup.indexOf("标的")).toBeLessThan(markup.indexOf("模型日志"));
    expect(markup.indexOf("结论")).toBeLessThan(markup.indexOf("模型日志"));
    expect(markup).toContain("搜索引擎");
    expect(markup).toContain("WeKnora");
  });
});

describe("changed targets page", () => {
  it("labels the page as rating changes", () => {
    const markup = renderToStaticMarkup(createElement(ChangedTargetsPage, { apiBase: "" }));

    expect(markup).toContain("标的评级变化");
    expect(markup).toContain("宏观经济与行业变化");
    expect(markup).toContain("具体标的变化");
    expect(markup).toContain("正在加载宏观经济与行业变化");
    expect(markup).toContain("正在加载具体标的变化");
  });

  it("keeps legacy target cards compatible with the new four-column grid", () => {
    const items = Array.from({ length: 5 }, (_, index) => ({
      asset: {
        asset_id: `equity:XNAS:TARGET${index}`,
        symbol: `TARGET${index}`,
        name: `Target ${index} Corp`,
        market: "US",
      },
      recommendation_id: `recommendation-${index}`,
      latest_recommendation_id: `latest-recommendation-${index}`,
      latest_researched_at: `2026-08-28T0${index}:00:00Z`,
      changed_at: `2026-08-27T0${index}:00:00Z`,
      previous: {
        signal_status: index === 0 ? "insufficient_evidence" : "directional",
        rating: index % 2 === 0 ? "watch" : "bearish",
      },
      current: {
        signal_status: "directional",
        rating: index % 2 === 0 ? "bullish" : "strongly_bullish",
      },
      status_changed: index === 0,
      rating_changed: true,
    }));

    const markup = renderToStaticMarkup(createElement(ChangedTargetGrid, {
      items,
      researchStates: {
        "equity:XNAS:TARGET0": { status: "queued" },
      },
    }));

    expect(changedTargetDesktopColumns).toBe(4);
    expect(markup).toContain('class="target-change-grid" data-columns="4"');
    expect((markup.match(/class="target-change-card"/g) || []).length).toBe(5);
    expect(markup).not.toContain("结论状态");
    expect(markup).not.toContain("方向证据不足");
    expect(markup).not.toContain("方向信号");
    expect(markup).not.toContain("未变");
    expect(markup).toContain("中性");
    expect(markup).toContain("看空");
    expect(markup).toContain("看多");
    expect(markup).toContain("强烈看多");
    expect((markup.match(/class="research-again"/g) || []).length).toBe(5);
    expect(markup).toContain("已进入队列");
    expect(markup.indexOf("TARGET0")).toBeLessThan(markup.indexOf("已进入队列"));
    expect(markup).toContain('class="target-change-identity"');
    expect(markup).toContain('aria-label="查看 TARGET0 最近一次调研"');
    expect(markup).toMatch(/target-change-identity[\s\S]*TARGET0[\s\S]*Target 0 Corp[\s\S]*<\/button>[\s\S]*conclusion-research-action/);
  });

  it("renders four macro and four asset changes across the two target columns", () => {
    const buildItems = (kind: "macro" | "asset") => Array.from({ length: 4 }, (_, index) => ({
      kind,
      key: `${kind}-${index}`,
      label: kind === "macro" ? `能源行业 ${index}` : `Target ${index} Corp`,
      symbol: kind === "macro" ? null : `TARGET${index}`,
      market: kind === "macro" ? null : "US",
      target_type: kind === "macro" ? "sector" : "tradable_asset",
      changed_at: `2026-08-28T0${index}:00:00Z`,
      previous: { rating: "watch", direction_score: 0, rating_confidence: 0.5 },
      current: { rating: "bullish", direction_score: 45, rating_confidence: 0.7 },
      latest: { rating: "bullish", direction_score: 58, rating_confidence: 0.81 },
      latest_detail: { kind: kind === "macro" ? "event" : "asset", id: `${kind}-detail-${index}`, researched_at: `2026-08-29T0${index}:00:00Z` },
      change_detail_id: `${kind}-change-${index}`,
    })) as TargetChange[];
    const macroItems = buildItems("macro");
    const assetItems = buildItems("asset");
    const markup = renderToStaticMarkup(createElement("div", { className: "target-change-split" },
      createElement(TargetChangeGrid, {
        items: macroItems,
        onOpen: () => undefined,
        onResearch: () => undefined,
        researchStates: { "event:macro-detail-0": { status: "queued" } },
      }),
      createElement(TargetChangeGrid, {
        items: assetItems,
        onOpen: () => undefined,
        onResearch: () => undefined,
      }),
    ));

    expect(changedTargetDesktopColumns).toBe(4);
    expect((markup.match(/data-columns="4"/g) || []).length).toBe(2);
    expect((markup.match(/target-change-card (?:macro|asset)/g) || []).length).toBe(8);
    expect((markup.match(/class="research-again"/g) || []).length).toBe(8);
    expect(markup).toContain("已进入队列");
    expect(targetChangeResearchKey(macroItems[0])).toBe("event:macro-detail-0");
    expect(targetChangeResearchKey(assetItems[0])).toBe("asset:asset-0");
    expect(markup).toContain("评级变化");
    expect(markup).toContain("最新方向分");
    expect(markup).toContain("评级置信度");
    expect(markup).toContain('title="Target 0 Corp"');
    expect(markup).toContain('aria-label="查看 能源行业 0 最新研究"');
    expect(markup).toContain('aria-label="查看 TARGET0 最新研究"');
    expect(markup.indexOf("评级置信度")).toBeLessThan(markup.indexOf('class="conclusion-research-action"'));
  });

  it("renders event conclusion cards and their macro impact detail", () => {
    const item = {
      kind: "event",
      id: "event-run-1",
      occurred_at: "2026-08-29T08:00:00Z",
      status: "insufficient_evidence",
      evidence_complete: false,
      title: "能源政策调整",
      summary: "能源行业利润和原油价格可能发生变化。",
      asset: null,
      event: { id: "event-1", headline: "能源政策调整", event_type: "regulation" },
      recommendation: null,
      report: { confidence: 0.6, news_confidence: 0.82, direction_score: -45, rating: "bearish", impact_count: 1, affected_markets: ["COMMODITY"], affected_sectors: ["能源"], scoring_version: "event-report-v1" },
    } as ResearchConclusionItem;
    const card = renderToStaticMarkup(createElement(EventConclusionCard, { item, onOpen: () => undefined }));
    expect(card).toContain("-45 · 看空");
    expect(card).toContain("新闻可信度 82%");
    expect(card).toContain("研报置信度 60%");
    expect(card).toContain("影响目标 1 个");
    expect(card).not.toContain("证据不足");
    expect(card).not.toContain("资料覆盖不足");

    const emptyCard = renderToStaticMarkup(createElement(EventConclusionCard, {
      item: { ...item, report: { ...item.report!, direction_score: null, rating: null, impact_count: 0 } },
      onOpen: () => undefined,
    }));
    expect(emptyCard).toContain("— · 暂无评级");
    expect(emptyCard).toContain("影响目标 0 个");

    const detail = {
      run: { id: "event-run-1", status: "insufficient_evidence", updated_at: "2026-08-29T08:00:00Z" },
      event: { id: "event-1", headline: "能源政策调整", event_type: "regulation" },
      report: {
        summary: "事件结论摘要",
        affected_markets: ["COMMODITY"], affected_sectors: ["能源"], scenarios: ["基准情景"], catalysts: ["政策生效"], risks: ["需求下降"], unresolved_questions: ["执行日期"],
        confidence: 0.6, evidence_complete: false, news_confidence: 0.82,
        impacts: [{ target_type: "commodity_price", target_name: "WTI 原油", asset: null, direction_score: 45, rating: "bullish", rating_confidence: 0.71, horizon_days: 90, horizon_unit: "calendar_days", transmission_path: ["供给减少", "价格上升"], rationale: "供应风险溢价上升", missing_information: ["实际减产量"] }],
        macro_factors: [{ id: "factor-1", name: "原油供给", description: "供给收缩", strength: 0.7 }], missing_information: ["政策细则"],
      },
      news: [{ id: "news-1", title: "政策新闻", url: "https://example.com/news", source: "Example" }],
      evidence: [{ id: "evidence-1", claim: "政策已宣布", source_name: "Official", source_url: "https://example.com/evidence", excerpt: "摘要" }],
    } as EventConclusionDetail;
    const modal = renderToStaticMarkup(createElement(EventConclusionDetailModal, { detail, onClose: () => undefined }));
    expect(modal).toContain("事件结论摘要");
    expect(modal).toContain("WTI 原油");
    expect(modal).toContain("供给减少");
    expect(modal).toContain("资料覆盖不足");
    expect(modal).toContain("政策新闻");
  });

  it("refreshes conclusion and target views every five seconds", () => {
    expect(researchViewsRefreshIntervalMs).toBe(5000);
  });

  it("uses the latest research id and falls back for old API responses", () => {
    const base = {
      asset: { asset_id: "equity:XNAS:MRNA", symbol: "MRNA", name: "Moderna", market: "US" },
      recommendation_id: "rating-change-id",
      changed_at: "2026-08-27T00:00:00Z",
      previous: { signal_status: "neutral", rating: "watch" },
      current: { signal_status: "directional", rating: "bullish" },
      status_changed: true,
      rating_changed: true,
    };

    expect(changedTargetLatestRecommendationId(base)).toBe("rating-change-id");
    expect(changedTargetLatestRecommendationId({ ...base, latest_recommendation_id: "latest-id" })).toBe("latest-id");

    const loadingMarkup = renderToStaticMarkup(createElement(ChangedTargetGrid, {
      items: [base],
      detailLoadingId: base.asset.asset_id,
    }));
    expect(loadingMarkup).toContain("正在加载最近调研");
    expect(loadingMarkup).toContain('aria-busy="true"');
  });

  it("renders the shared latest research modal with thesis, risk, and evidence", () => {
    const detail = {
      recommendation: {
        id: "latest-id",
        run_id: "run-1",
        asset: { asset_id: "equity:XNAS:MRNA", symbol: "MRNA", name: "Moderna, Inc.", market: "US" },
        rating: "bullish",
        score: 36,
        confidence: 0.72,
        evidence_complete: true,
        scoring_version: "short-term-impact-v1",
        horizon_unit: "trading_sessions",
        horizon_days: 3,
        as_of: "2026-08-28T20:57:28Z",
        bull_probability: 0.6,
        base_probability: 0.3,
        bear_probability: 0.1,
        thesis: {
          summary: "最近调研核心观点",
          historical_context: "历史背景",
          financials_and_growth: "财务",
          products_or_protocol: "产品",
          competition: "竞争",
          valuation_or_tokenomics: "估值",
          catalysts: ["近期催化剂"],
          risks: ["关键下行风险"],
          invalidation_conditions: ["观点失效条件"],
        },
      },
      event: { headline: "关联事件标题" },
      news: [{ id: "news-1", title: "支持新闻", url: "https://example.com/news", source: "Example" }],
      evidence: [{ id: "evidence-1", claim: "支持证据", source_name: "Evidence", source_url: "https://example.com/evidence", excerpt: "摘要" }],
    } as ConclusionDetail;

    const markup = renderToStaticMarkup(createElement(ConclusionDetailModal, { detail, onClose: () => undefined }));
    expect(markup).toContain("最近调研核心观点");
    expect(markup).toContain("关键下行风险");
    expect(markup).toContain("支持新闻");
    expect(markup).toContain("关联事件标题");
    expect(markup).toContain('aria-label="关闭调研详情"');
  });

  it("distinguishes loading, empty, and failed states", () => {
    const render = (loading: boolean, error = "") => renderToStaticMarkup(createElement(
      ChangedTargetsContent,
      { items: [], loading, error, onRetry: () => undefined },
    ));

    const loading = render(true);
    const empty = render(false);
    const failed = render(false, "标的评级变化请求失败");

    expect(loading).toContain("正在加载标的评级变化");
    expect(loading).not.toContain("当前没有评级发生变化的标的");
    expect(empty).toContain("当前没有评级发生变化的标的");
    expect(failed).toContain("标的评级变化请求失败");
    expect(failed).toContain("重试");
    expect(failed).not.toContain("当前没有评级发生变化的标的");
  });
});

describe("news board page", () => {
  it("renders one source panel with linked news, status, quality, and assets", () => {
    const markup = renderToStaticMarkup(createElement(NewsSourcePanel, {
      group: {
        source: "金十",
        latest_published_at: "2026-08-26T04:03:00Z",
        item_count: 1,
        error: null,
        items: [{
          id: "news-1",
          title: "上市公司发布最新业绩",
          summary: "业绩摘要",
          url: "https://example.com/news-1",
          source_quality: "professional",
          published_at: "2026-08-26T04:03:00Z",
          observed_at: "2026-08-26T04:03:01Z",
          status: "mapping",
          status_updated_at: "2026-08-26T04:03:02Z",
          events: [{ id: "event-1", headline: "业绩", event_type: "earnings", priority: 0.8 }],
          assets: [{ asset_id: "cn:sse:600000", symbol: "600000", name: "浦发银行", market: "CN" }],
        }],
      },
    }));

    expect(markup).toContain('class="news-source-panel"');
    expect(markup).toContain("金十");
    expect(markup).toContain("最新 1/50 条");
    expect(markup).toContain('href="https://example.com/news-1"');
    expect(markup).toContain("股票映射中");
    expect(markup).toContain("专业财经");
    expect(markup).toContain("600000");
    expect(newsBoardStatusLabels.revising).toBe("修订中");
    expect(newsBoardRefreshIntervalMs).toBe(5000);
    expect(newsSourceDesktopColumns).toBe(3);
  });

  it("keeps one source failure local to that source panel", () => {
    const markup = renderToStaticMarkup(createElement(NewsSourcePanel, {
      group: {
        source: "FMP Stock News",
        latest_published_at: "2026-08-26T04:03:00Z",
        item_count: 0,
        items: [],
        error: "source query failed",
      },
    }));

    expect(markup).toContain("FMP Stock News");
    expect(markup).toContain("source query failed");
  });
});

describe("research queue page", () => {
  const overviewQueue = (
    overrides: Partial<ModelQueueOverviewItem> = {},
  ): ModelQueueOverviewItem => ({
    id: "assist",
    model: "qwen2.5:7b",
    purpose: "股票映射",
    binding: "新闻事件二次股票映射",
    enabled: true,
    state: "running",
    threads: 4,
    capacity: 1,
    available: 0,
    instance_count: 1,
    per_instance_concurrency: 1,
    observable: true,
    instances: [],
    counts: {
      queued: 2,
      running: 1,
      retrying: 1,
      verifying: 0,
      waiting_for_model: 1,
      completed: 8,
      failed: 2,
    },
    metrics: {
      average_queue_duration_ms: 90_000,
      average_execution_duration_ms: 180_000,
      longest_wait_ms: 240_000,
      estimated_clear_ms: 720_000,
      queue_duration_sample_count: 4,
      execution_duration_sample_count: 3,
      execution_p50_ms: 150_000,
      execution_p90_ms: 210_000,
      throughput_per_hour: 2.5,
    },
    total_tasks: 14,
    truncated: false,
    tasks: [],
    error: null,
    ...overrides,
  });

  it("renders square asset cards with status and merged task count", () => {
    const markup = renderToStaticMarkup(createElement(QueueGrid, { items: [{
      asset_id: "cn:600519",
      symbol: "600519",
      name: "贵州茅台",
      market: "CN",
      asset_class: "equity",
      status: "verifying",
      task_count: 3,
      queued_at: "2026-08-25T01:00:00Z",
      representative_queued_at: "2026-08-25T01:00:00Z",
      started_at: "2026-08-25T01:02:00Z",
      completed_at: null,
      queue_duration_ms: 120000,
      execution_duration_ms: 180000,
      updated_at: "2026-08-25T01:05:00Z",
    }] }));
    expect(markup).toContain('class="queue-grid"');
    expect(markup).toContain('class="queue-card verifying"');
    expect(markup).toContain("600519");
    expect(markup).toContain("贵州茅台");
    expect(markup).toContain("验证中");
    expect(markup).toContain("3 个任务");
    expect(markup).toContain("排队 2分0秒");
    expect(markup).toContain("执行 3分0秒");
  });

  it("uses a five-second refresh and a five-column half-width grid", () => {
    expect(queueRefreshIntervalMs).toBe(5000);
    expect(queueDesktopColumns).toBe(5);
    const markup = renderToStaticMarkup(createElement(QueueGrid, { items: [{
      asset_id: "us:test",
      symbol: "TEST",
      name: "Test Asset",
      market: "US",
      asset_class: "equity",
      status: "queued",
      task_count: 1,
      queued_at: "2026-08-25T01:00:00Z",
      representative_queued_at: "2026-08-25T01:00:00Z",
      started_at: null,
      completed_at: null,
      queue_duration_ms: 30000,
      execution_duration_ms: null,
      updated_at: "2026-08-25T01:00:00Z",
    }] }));
    expect(markup).toContain('data-columns="5"');
  });

  it("renders an explicit empty state", () => {
    const markup = renderToStaticMarkup(createElement(QueueGrid, { items: [] }));
    expect(markup).toContain("当前没有排队或处理中的标的");
  });

  it("renders news extraction titles, status, source and retry attempt", () => {
    const markup = renderToStaticMarkup(createElement(NewsExtractionList, { items: [{
      task_id: "extract-1",
      news_id: "news-1",
      title: "上市公司发布半年度业绩公告",
      source: "金十",
      published_at: "2026-08-25T01:00:00Z",
      status: "retrying",
      attempt: 2,
      queued_at: "2026-08-25T01:01:00Z",
      started_at: "2026-08-25T01:01:30Z",
      completed_at: null,
      queue_duration_ms: 30000,
      execution_duration_ms: 90000,
      updated_at: "2026-08-25T01:02:00Z",
      error: "RuntimeError: temporary failure",
    }] }));
    expect(markup).toContain("上市公司发布半年度业绩公告");
    expect(markup).toContain("金十");
    expect(markup).toContain("重试中");
    expect(markup).toContain("第 2 次尝试");
    expect(markup).toContain('class="extraction-list" data-columns="5"');
    expect(markup).toContain("排队 30秒");
    expect(markup).toContain("执行 1分30秒");
    expect(markup).toContain('title="上市公司发布半年度业绩公告"');
  });

  it("formats queue durations compactly", () => {
    expect(formatQueueDuration(null)).toBe("—");
    expect(formatQueueDuration(48000)).toBe("48秒");
    expect(formatQueueDuration(135000)).toBe("2分15秒");
    expect(formatQueueDuration(3720000)).toBe("1时2分");
  });

  it("renders 7B and coder 7B inference queue capacity", () => {
    const assist = renderToStaticMarkup(createElement(ModelInferenceQueuePanel, { item: {
      lane: "assist",
      model: "qwen2.5:7b",
      purpose: "股票映射",
      binding: "新闻事件二次股票映射",
      task_enabled: true,
      threads: 8,
      capacity: 1,
      queued: 0,
      running: 0,
      available: 1,
      observable: true,
      state: "idle",
    } }));
    const code = renderToStaticMarkup(createElement(ModelInferenceQueuePanel, { item: {
      lane: "code",
      model: "qwen2.5-coder:7b",
      purpose: "代码演进",
      binding: "代码演进任务",
      task_enabled: true,
      threads: 8,
      capacity: 1,
      queued: 1,
      running: 1,
      available: 0,
      observable: true,
      state: "queued",
    } }));

    expect(assist).toContain("qwen2.5:7b 股票映射队列");
    expect(assist).toContain("CPU 线程<strong>8</strong>");
    expect(assist).toContain("当前没有等待或运行中的模型请求");
    expect(code).toContain("qwen2.5-coder:7b 代码演进队列");
    expect(code).toContain("有请求排队");
    expect(code).toContain("等待进入模型 1 个请求");
  });

  it("renders concrete 7B mapping cards and business result counts", () => {
    const markup = renderToStaticMarkup(createElement(ModelQueueTaskGrid, {
      queue: overviewQueue({
        tasks: [{
          task_id: "mapping-1",
          kind: "asset_mapping",
          entity_id: "event-1",
          title: "沪电股份发布半年报",
          subtitle: "earnings",
          source: "automatic",
          status: "retrying",
          attempt: 2,
          task_count: 1,
          queued_at: "2026-08-26T01:00:00Z",
          started_at: "2026-08-26T01:01:00Z",
          completed_at: null,
          updated_at: "2026-08-26T01:03:00Z",
          queue_duration_ms: 60_000,
          execution_duration_ms: 120_000,
          error: "模型响应暂时不可解析",
          metrics: { proposed_count: 3, verified_count: 1, rejected_count: 2 },
        }],
      }),
    }));

    expect(markup).toContain("沪电股份发布半年报");
    expect(markup).toContain("重试中");
    expect(markup).toContain("提出<strong>3</strong>");
    expect(markup).toContain("通过<strong>1</strong>");
    expect(markup).toContain("拒绝<strong>2</strong>");
    expect(markup).toContain("最近错误");
    expect(markup).toContain("手动重试");
    expect(markup).toContain('title="以最高优先级插队重试"');
    expect(markup).toContain("排队 1分0秒");
  });

  it("renders coder 7B evolution stage, target branch and disabled state", () => {
    const codeQueue = overviewQueue({
      id: "code",
      model: "qwen2.5-coder:7b",
      purpose: "代码演进",
      binding: "失败案例驱动的代码演进",
      tasks: [{
        task_id: "evolve-1",
        kind: "code_evolution",
        entity_id: "candidate-1",
        title: "减少无效重试",
        subtitle: "failure_rate",
        source: "manual",
        status: "testing",
        attempt: 1,
        task_count: 1,
        queued_at: "2026-08-26T01:00:00Z",
        started_at: "2026-08-26T01:00:30Z",
        completed_at: null,
        updated_at: "2026-08-26T01:02:00Z",
        queue_duration_ms: 30_000,
        execution_duration_ms: 90_000,
        error: null,
        metrics: { target_metric: "failure_rate", branch: "evolve/retry-policy" },
      }],
    });
    const markup = renderToStaticMarkup(createElement(ModelQueueTaskGrid, { queue: codeQueue }));
    const disabled = renderToStaticMarkup(createElement(ModelQueueTaskGrid, {
      queue: { ...codeQueue, enabled: false, tasks: [] },
    }));

    expect(markup).toContain("减少无效重试");
    expect(markup).toContain("测试中");
    expect(markup).toContain("目标：failure_rate");
    expect(markup).toContain("分支：evolve/retry-policy");
    expect(disabled).toContain("代码演进未启用（EVOLUTION_ENABLED=false）");
  });

  it("renders unified queue metrics and truncation without hiding local errors", () => {
    const markup = renderToStaticMarkup(createElement(UnifiedModelQueuePanel, {
      queue: overviewQueue({ truncated: true, error: "映射记录暂时不可用" }),
    }));

    expect(markup).toContain("qwen2.5:7b 股票映射队列");
    expect(markup).toContain("完成/失败<strong>8/2</strong>");
    expect(markup).toContain("近4h吞吐<strong>2.5/时</strong>");
    expect(markup).not.toContain("近4h平均执行");
    expect(markup).toContain("最长等待<strong>4分0秒</strong>");
    expect(markup).toContain("预计清空<strong>12分0秒</strong>");
    expect(markup).not.toContain("实例个数");
    expect(markup).toContain("实例并发<strong>1 路</strong>");
    expect(markup).toContain("映射记录暂时不可用");
    expect(markup).toContain("当前显示前 500 张任务卡");
  });

  it("renders research instance health and rolling latency metrics", () => {
    const researchQueue = overviewQueue({
        id: "research",
        model: "qwen2.5:7b",
        purpose: "标的研究",
        instance_count: 2,
        per_instance_concurrency: 1,
        instances: [
          { id: "research-0", healthy: true, model_available: true },
          { id: "research-1", healthy: false, model_available: false },
        ],
      });
    const instances = modelQueueInstances(researchQueue);
    expect(instances).toHaveLength(2);
    const markup = renderToStaticMarkup(createElement(UnifiedModelQueuePanel, {
      queue: researchQueue,
      instance: instances[0],
    }));
    const offlineMarkup = renderToStaticMarkup(createElement(UnifiedModelQueuePanel, {
      queue: researchQueue,
      instance: instances[1],
    }));

    expect(markup).toContain("P50<strong>2分30秒</strong>");
    expect(markup).toContain("P90<strong>3分30秒</strong>");
    expect(markup).toContain("近24h吞吐<strong>2.5/时</strong>");
    expect(markup).toContain("近4h平均执行<strong>3分0秒</strong>");
    expect(markup).not.toContain("近24h平均执行");
    expect(markup).not.toContain("实例个数");
    expect(markup).toContain("实例并发<strong>1 路</strong>");
    expect(markup).toContain("research-0</h3>");
    expect(markup).toContain("实例可用");
    expect(offlineMarkup).toContain("research-1</h3>");
    expect(offlineMarkup).toContain("实例离线");

    const assistMarkup = renderToStaticMarkup(createElement(UnifiedModelQueuePanel, {
      queue: overviewQueue({
        id: "assist",
        model: "qwen2.5:7b",
        purpose: "股票映射",
        instances: [{ id: "assist-0", healthy: true, model_available: true }],
      }),
    }));
    expect(assistMarkup).toContain("assist-0</h3>");
    expect(assistMarkup).toContain("实例可用");
  });

  it("keeps extract and mapping instances left, then research and code instances right", () => {
    const queues = [
      overviewQueue({
        id: "code",
        purpose: "代码演进",
        instances: [{ id: "code-0", healthy: true, model_available: true }],
      }),
      overviewQueue({
        id: "research",
        purpose: "标的研究",
        instances: [
          { id: "research-0", healthy: true, model_available: true },
          { id: "research-1", healthy: true, model_available: true },
          { id: "research-2", healthy: true, model_available: true },
        ],
      }),
      overviewQueue({
        id: "assist",
        purpose: "股票映射",
        instances: [
          { id: "assist-0", healthy: true, model_available: true },
          { id: "assist-1", healthy: true, model_available: true },
        ],
      }),
      overviewQueue({
        id: "extract",
        purpose: "新闻抽取",
        instances: [
          { id: "extract-0", healthy: true, model_available: true },
          { id: "extract-1", healthy: true, model_available: true },
        ],
      }),
    ];

    const [leftColumn, rightColumn] = modelQueuePanelColumns(queues);
    expect(leftColumn.map(({ instance }) => instance.id)).toEqual([
      "extract-0", "extract-1", "assist-0", "assist-1",
    ]);
    expect(rightColumn.map(({ instance }) => instance.id)).toEqual([
      "research-0", "research-1", "research-2", "code-0",
    ]);
  });

  it("renders cancellation, retry and clear controls for supported model queues", () => {
    const research = overviewQueue({
      id: "research", model: "qwen2.5:7b", purpose: "标的研究",
      tasks: [{
        task_id: "asset:cn:600519:queued-at", kind: "asset_research", entity_id: "cn:600519",
        title: "600519 · 贵州茅台", subtitle: "CN · equity", source: "business", status: "running",
        attempt: 1, task_count: 2, queued_at: "2026-08-26T01:00:00Z",
        started_at: "2026-08-26T01:01:00Z", completed_at: null,
        updated_at: "2026-08-26T01:02:00Z", queue_duration_ms: 60_000,
        execution_duration_ms: 60_000, error: null, metrics: {},
      }],
    });
    const grid = renderToStaticMarkup(createElement(ModelQueueTaskGrid, {
      queue: research, onCancel: () => undefined,
    }));
    const panel = renderToStaticMarkup(createElement(UnifiedModelQueuePanel, {
      queue: research, onClear: () => undefined,
    }));
    const mappingTask = {
      ...research.tasks[0],
      task_id: "mapping-task",
      kind: "asset_mapping",
      title: "石四药集团：上半年归母净利润增长",
      source: "automatic",
    } satisfies ModelQueueTask;
    const mapping = renderToStaticMarkup(createElement(ModelQueueTaskGrid, {
      queue: overviewQueue({ id: "assist", purpose: "股票映射", tasks: [mappingTask] }),
      onCancel: () => undefined,
    }));

    expect(grid).toContain('aria-label="取消 600519 · 贵州茅台 的研究"');
    expect(grid).toContain('title="取消该标的研究"');
    expect(panel).toContain('class="model-queue-state running"');
    expect(panel).toContain('class="model-queue-clear"');
    expect(panel).toContain(">清空</button>");
    expect(mapping).toContain("自动任务");
    expect(mapping).toContain('aria-label="取消 石四药集团：上半年归母净利润增长 的股票映射任务"');
    expect(mapping).toContain('title="取消当前股票映射任务"');
    for (const id of ["extract", "assist", "code"] as const) {
      const modelPanel = renderToStaticMarkup(createElement(UnifiedModelQueuePanel, {
        queue: overviewQueue({ id }), onClear: () => undefined,
      }));
      expect(modelPanel).toContain('class="model-queue-clear"');
      expect(modelPanel).toContain(">清空</button>");
      expect(modelPanel).toContain('class="model-queue-retry"');
      expect(modelPanel).toContain(">重试</button>");
      expect(modelPanel).not.toContain("实例个数");
      expect(modelPanel).toContain("实例并发<strong>1 路</strong>");
    }
  });

  it("removes a cancelled card and decrements its pending count immediately", () => {
    const queuedTask = {
      task_id: "event-run", kind: "event_research", entity_id: "event-1",
      title: "富时中国A50指数期货盘初上涨", subtitle: "中性事件研报",
      source: "business", status: "queued", attempt: 1, task_count: 1,
      queued_at: "2026-08-27T01:00:00Z", started_at: null, completed_at: null,
      updated_at: "2026-08-27T01:00:00Z", queue_duration_ms: 60_000,
      execution_duration_ms: null, error: null, metrics: {},
    } satisfies ModelQueueTask;
    const research = overviewQueue({
      id: "research", model: "qwen2.5:7b", purpose: "标的研究",
      counts: {
        queued: 2, running: 0, retrying: 0, verifying: 0,
        waiting_for_model: 0, completed: 8, failed: 2,
      },
      total_tasks: 12,
      tasks: [queuedTask],
    });

    const updated = removeTasksFromQueueOverview(
      { generated_at: "2026-08-27T01:00:00Z", queues: [research] },
      "research",
      (task) => task.task_id === queuedTask.task_id,
    );

    expect(updated.queues[0].counts.queued).toBe(1);
    expect(updated.queues[0].total_tasks).toBe(11);
    expect(updated.queues[0].tasks).toEqual([]);
  });

  it("clears a failed card and decrements the failed count immediately", () => {
    const failedTask = {
      task_id: "failed-run", kind: "asset_research", entity_id: "cn:002129",
      title: "002129 · TCL中环", subtitle: "CN · equity", source: "business",
      status: "failed", attempt: 1, task_count: 1,
      queued_at: "2026-08-27T01:00:00Z", started_at: "2026-08-27T01:01:00Z",
      completed_at: "2026-08-27T01:02:00Z", updated_at: "2026-08-27T01:02:00Z",
      queue_duration_ms: 60_000, execution_duration_ms: 60_000,
      error: "模型失败", metrics: {},
    } satisfies ModelQueueTask;
    const research = overviewQueue({
      id: "research", purpose: "标的研究",
      counts: {
        queued: 0, running: 0, retrying: 0, verifying: 0,
        waiting_for_model: 0, completed: 188, failed: 1,
      },
      tasks: [failedTask], total_tasks: 189,
    });
    const panel = renderToStaticMarkup(createElement(UnifiedModelQueuePanel, {
      queue: research, onClear: () => undefined,
    }));
    const updated = removeTasksFromQueueOverview(
      { generated_at: "2026-08-27T01:03:00Z", queues: [research] },
      "research",
      (task) => task.status === "failed",
    );

    expect(panel).not.toContain('class="model-queue-clear" disabled=""');
    expect(updated.queues[0].counts.failed).toBe(0);
    expect(updated.queues[0].total_tasks).toBe(188);
    expect(updated.queues[0].tasks).toEqual([]);
  });

  it("keeps a cancelled mapping card hidden while a broker count snapshot is stale", () => {
    const mapping = overviewQueue({
      id: "assist", model: "qwen2.5:7b", purpose: "股票映射",
      counts: {
        queued: 238, running: 2, retrying: 84, verifying: 0,
        waiting_for_model: 0, completed: 546, failed: 0,
      },
      total_tasks: 870,
      tasks: [],
    });

    const applied = applyCancelledTaskTombstone(
      { generated_at: "2026-08-27T06:00:05Z", queues: [mapping] },
      "cancelled-mapping",
      { queueId: "assist", countField: "queued", maxCount: 237, cancelledAt: 0 },
    );

    expect(applied.settled).toBe(false);
    expect(applied.overview.queues[0].counts.queued).toBe(237);
    expect(applied.overview.queues[0].total_tasks).toBe(869);
  });
});

describe("fact data sources", () => {
  it("defines the five fixed fact-source groups in display order", () => {
    expect(factSourceGroupDefinitions.map((group) => group.id)).toEqual([
      "fmp", "sec", "cn_news", "crypto", "search",
    ]);
    expect(factSourceGroupDefinitions.map((group) => group.name)).toEqual([
      "FMP 美股数据", "SEC 官方文件", "A股与新闻", "数字资产", "网络搜索与交叉验证",
    ]);
  });
});
