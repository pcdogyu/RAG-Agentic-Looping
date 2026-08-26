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
  factSourceGroupDefinitions,
  formatQueueDuration,
  navigationGroups,
  queueDesktopColumns,
  QueueGrid,
  NewsExtractionList,
  queueRefreshIntervalMs,
  routeFromHash,
  TopNavigation,
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
  next_scan_at: "2026-08-22T00:11:00Z",
  last_result: null,
  last_error: null,
  interval_seconds: 600,
  server_time: "2026-08-22T00:01:00Z",
};

describe("scan status presentation", () => {
  it("formats a completion-anchored countdown", () => {
    expect(formatCountdown(600)).toBe("10分00秒");
    expect(scanButtonText(baseStatus, Date.parse("2026-08-22T00:01:01Z"))).toBe(
      "距离下一次扫描 09分59秒",
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
      models: ["qwen2.5:3b", "qwen2.5:7b", "qwen2.5:14b", "qwen2.5-coder:7b"],
    });

    expect(tracking.ollama).toEqual({ failures: 0, state: "available" });
    expect(tracking.models["qwen2.5:7b"]).toEqual({ failures: 0, state: "available" });
    expect(tracking.models["qwen2.5:14b"]).toEqual({ failures: 0, state: "available" });
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
    expect(tracking.models["qwen2.5:14b"]).toEqual({ failures: 3, state: "missing" });
    expect(tracking.models["qwen2.5-coder:7b"]).toEqual({ failures: 3, state: "missing" });
  });

  it("matches Ollama model names case-insensitively", () => {
    const tracking = updateHealthTracking(
      createInitialHealthTracking(),
      { ollama: true, models: ["QWEN2.5:14B"] },
    );
    expect(tracking.models["qwen2.5:14b"].state).toBe("available");
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
      models: ["qwen2.5:14b"],
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
      models: ["qwen2.5:14b"],
      steps: [{
        phase: "report_drafting",
        status: "completed",
        executor: "ollama",
        model: "qwen2.5:14b",
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

  it("renders a completed neutral event report and its affected scope", () => {
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
      models: ["qwen2.5:14b"],
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
      },
    } satisfies AnalysisLog;
    const markup = renderToStaticMarkup(createElement(AnalysisTraceList, { logs: [log] }));

    expect(markup).toContain("中性事件研报");
    expect(markup).toContain("CN · 智能制造");
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
  it("recognizes all eight routes and falls back to home", () => {
    expect(routeFromHash("#/home")).toBe("home");
    expect(routeFromHash("#/source-filter")).toBe("source-filter");
    expect(routeFromHash("#/conclusions")).toBe("conclusions");
    expect(routeFromHash("#/sources")).toBe("sources");
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
      "home", "source-filter", "sources", "queue", "analysis", "conclusions",
    ]);
    expect(navigationGroups.right.map((item) => item.route)).toEqual([
      "model-logs", "search", "weknora",
    ]);
    const markup = renderToStaticMarkup(createElement(TopNavigation, { current: "source-filter" }));
    const queueMarkup = renderToStaticMarkup(createElement(TopNavigation, { current: "queue" }));
    const analysisMarkup = renderToStaticMarkup(createElement(TopNavigation, { current: "analysis" }));
    expect((markup.match(/<a /g) || []).length).toBe(9);
    expect(markup).toContain('href="#/source-filter" aria-current="page"');
    expect(queueMarkup).toContain('href="#/queue" aria-current="page"');
    expect(analysisMarkup).toContain('href="#/analysis" aria-current="page"');
    expect(markup.indexOf("数据源过滤")).toBeLessThan(markup.indexOf(">数据源<"));
    expect(markup.indexOf(">数据源<")).toBeLessThan(markup.indexOf(">队列<"));
    expect(markup.indexOf(">队列<")).toBeLessThan(markup.indexOf("分析链路"));
    expect(markup.indexOf("分析链路")).toBeLessThan(markup.indexOf("结论"));
    expect(markup.indexOf("结论")).toBeLessThan(markup.indexOf("模型日志"));
    expect(markup).toContain("搜索引擎");
    expect(markup).toContain("WeKnora");
  });
});

describe("research queue page", () => {
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
