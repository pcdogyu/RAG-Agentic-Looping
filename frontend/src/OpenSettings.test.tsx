import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  availableResearchInstances,
  ConclusionCard,
  ConclusionScore,
  ConclusionsPage,
  conclusionResearchPath,
  conclusionReferences,
  factSourceGroupOptions,
  failedResearchAfterBulkRetry,
  failedResearchBulkRetryMessage,
  failedResearchBulkRetryPath,
  failedResearchRetryPath,
  firstUnhealthyGroup,
  GateReasons,
  explainGateReason,
  isSearchSource,
  ModelOpinion,
  NativeConfigEditor,
  parseFilterKeywords,
  retryAllFailedResearch,
  researchConclusion,
  ResearchAgainButton,
  ShortTermScoreDetails,
  searchSourceLabel,
  SearchPage,
  SourceFilterPage,
  SourceFilterAuditRow,
  sourceFilterRescanPath,
  SourcesPage,
  type Recommendation,
} from "./AppPages";

const shortTermRecommendation: Recommendation = {
  id: "recommendation-gold",
  run_id: "run-gold",
  asset: { asset_id: "commodity:GOLD", symbol: "GOLD", name: "黄金", market: "COMMODITY" },
  rating: "bearish",
  score: -27,
  confidence: 0.82,
  fact_confidence: 0.92,
  evidence_complete: true,
  directional_evidence_complete: true,
  signal_status: "directional",
  raw_score: -27,
  score_available: true,
  horizon_days: 3,
  horizon_unit: "trading_sessions",
  scoring_version: "short-term-impact-v1",
  calibration_version: "component-confidence-v1",
  impact_factors: {
    direction: -1,
    magnitude: { value: 0.217, reason: "单日持仓下降 0.1087%", evidence_ids: ["spdr-holdings"] },
    persistence: { value: 0.2, reason: "只有单日数据", evidence_ids: ["spdr-holdings"] },
    representativeness: { value: 0.8, reason: "SPDR 是大型黄金 ETF", evidence_ids: ["spdr-official"] },
    market_confirmation: { value: 0, reason: "没有其他市场确认", evidence_ids: [] },
  },
  confidence_factors: {
    direction_clarity: { value: 1, reason: "减持方向明确", evidence_ids: ["spdr-holdings"] },
    source_reliability: { value: 0.92, reason: "官方来源可靠", evidence_ids: ["spdr-official"] },
    magnitude_certainty: { value: 0.8, reason: "变动幅度可计算", evidence_ids: ["spdr-holdings"] },
    market_context_completeness: { value: 0.2, reason: "趋势确认信息不足", evidence_ids: [] },
  },
  evidence_warnings: ["缺少美元和美债收益率同步确认"],
  as_of: "2026-08-28T00:00:00Z",
  bull_probability: 0.25,
  base_probability: 0.5,
  bear_probability: 0.25,
  thesis: {
    summary: "黄金 ETF 小幅流出，短线弱看空。",
    historical_context: "",
    financials_and_growth: "",
    products_or_protocol: "",
    competition: "",
    valuation_or_tokenomics: "",
    catalysts: [],
    risks: [],
    invalidation_conditions: [],
  },
};

describe("open source and search settings", () => {
  it("labels the conclusion direction score and rating explicitly", () => {
    const markup = renderToStaticMarkup(createElement(ConclusionScore, {
      score: 0,
      rating: "watch",
      confidence: 0.95,
      evidenceComplete: true,
    }));

    expect(markup).toContain("发布分：0");
    expect(markup).toContain("评级：观望");
    expect(markup).toContain("发布置信度 95% · 资料覆盖完整");
  });

  it("shows the short-term impact score and both confidence values", () => {
    const markup = renderToStaticMarkup(createElement(ConclusionScore, {
      score: -27,
      rating: "bearish",
      confidence: 0.82,
      factConfidence: 0.92,
      evidenceComplete: false,
      signalStatus: "directional",
      horizonDays: 3,
      horizonUnit: "trading_sessions",
      scoringVersion: "short-term-impact-v1",
    }));

    expect(markup).toContain("影响分：-27");
    expect(markup).toContain("五档评级：看空");
    expect(markup).toContain("新闻事实置信度 92%");
    expect(markup).toContain("评级置信度 82%");
    expect(markup).toContain("未来 1–3 个交易日");
    expect(markup).not.toContain("暂不评分");
  });

  it("does not score a conclusion whose evidence gate is incomplete", () => {
    const markup = renderToStaticMarkup(createElement(ConclusionScore, {
      score: null,
      rating: "watch",
      confidence: 0,
      evidenceComplete: false,
      directionalEvidenceComplete: false,
      signalStatus: "insufficient_evidence",
    }));

    expect(markup).toContain("暂不评分");
    expect(markup).toContain("方向证据不足 · 评级：观望");
    expect(markup).toContain("门禁后参考置信度 0%");
    expect(markup).not.toContain("发布分：0");
  });

  it("shows the 7B opinion independently from the publication gate", () => {
    const markup = renderToStaticMarkup(createElement(ModelOpinion, {
      direction: "bearish",
      rating: "strongly_bearish",
      confidence: 0.68,
    }));

    expect(markup).toContain("7B 模型原始意见");
    expect(markup).toContain("看空 / Bearish");
    expect(markup).toContain("强烈看空 / Strongly bearish");
    expect(markup).toContain("68%");
    expect(markup).toContain("Evidence quality checks may reduce final confidence");
  });

  it("renders the auditable -27 impact and 82% confidence calculation", () => {
    const markup = renderToStaticMarkup(createElement(ShortTermScoreDetails, {
      recommendation: shortTermRecommendation,
    }));

    expect(markup).toContain("S = D × (45M + 25T + 15I + 15C)");
    expect(markup).toContain("D = -1");
    expect(markup).toContain("因子合计 26.765");
    expect(markup).toContain("四舍五入后为 <strong>-27</strong>");
    expect(markup).toContain("0.217 × 45 × -1 = -9.765");
    expect(markup).toContain("0.8 × 15 × -1 = -12");
    expect(markup).toContain("置信度 = 40%A + 25%R + 20%Q + 15%K");
    for (const contribution of ["40 / 40", "23 / 25", "16 / 20", "3 / 15"]) {
      expect(markup).toContain(contribution);
    }
    expect(markup).toContain("spdr-official");
    expect(markup).toContain("缺少美元和美债收益率同步确认");
  });

  it("normalizes the model typo 官网 to 观望", () => {
    const markup = renderToStaticMarkup(createElement(ModelOpinion, {
      direction: "neutral",
      rating: "官网",
      confidence: 0.6,
    }));

    expect(markup).toContain("观望 / Watch");
    expect(markup).not.toContain("<strong>官网</strong>");
  });

  it("posts a bodyless force-research request to the selected conclusion", async () => {
    const calls: Array<{ input: string; init?: RequestInit }> = [];
    const request = (async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ input: String(input), init });
      return new Response(JSON.stringify({
        task_id: "task-1",
        run_id: "run-1",
        source_recommendation_id: "recommendation/1",
        status: "queued",
      }), { status: 202, headers: { "Content-Type": "application/json" } });
    }) as typeof fetch;

    const payload = await researchConclusion("http://api.example", "recommendation/1", request);

    expect(conclusionResearchPath("recommendation/1")).toBe(
      "/api/v1/conclusions/recommendation%2F1/research",
    );
    expect(calls).toEqual([{
      input: "http://api.example/api/v1/conclusions/recommendation%2F1/research",
      init: { method: "POST" },
    }]);
    expect(payload.status).toBe("queued");
  });

  it("surfaces an active research conflict with its run id", async () => {
    const request = (async () => new Response(JSON.stringify({
      detail: {
        code: "research_already_active",
        message: "该标的已有活动研究",
        active_run_id: "active-run-1",
      },
    }), { status: 409, headers: { "Content-Type": "application/json" } })) as typeof fetch;

    await expect(researchConclusion("", "recommendation-1", request)).rejects.toThrow(
      "该标的已有活动研究（活动任务 active-run-1）",
    );
  });

  it("renders force-research progress, queued, and recoverable error states", () => {
    const pending = renderToStaticMarkup(createElement(ResearchAgainButton, {
      state: { status: "pending" }, onResearch: () => undefined,
    }));
    const queued = renderToStaticMarkup(createElement(ResearchAgainButton, {
      state: { status: "queued" }, onResearch: () => undefined,
    }));
    const failed = renderToStaticMarkup(createElement(ResearchAgainButton, {
      state: { status: "error", error: "已有活动研究" }, onResearch: () => undefined,
    }));

    expect(pending).toContain("重新调研中…");
    expect(pending).toContain("disabled");
    expect(queued).toContain("已进入队列");
    expect(queued).toContain("disabled");
    expect(failed).toContain("重新调研");
    expect(failed).toContain('role="alert"');
    expect(failed).toContain("已有活动研究");
    expect(failed).not.toContain("disabled");
  });

  it("keeps conclusion details and force research as sibling buttons", () => {
    const markup = renderToStaticMarkup(createElement(ConclusionCard, {
      item: shortTermRecommendation,
      researchState: { status: "queued" },
      onOpen: () => undefined,
      onResearch: () => undefined,
    }));
    const firstButton = markup.indexOf("<button");
    const firstButtonClose = markup.indexOf("</button>", firstButton);
    const secondButton = markup.indexOf("<button", firstButton + 1);

    expect(markup).toContain('class="conclusion-card"');
    expect((markup.match(/<button/g) || [])).toHaveLength(2);
    expect(firstButtonClose).toBeLessThan(secondButton);
    expect(markup.indexOf("影响分：-27")).toBeLessThan(markup.indexOf("已进入队列"));
  });

  it("shows one blocking gate reason before the complete reason list", () => {
    const markup = renderToStaticMarkup(createElement(GateReasons, {
      primaryReason: "claim-level evidence strength is below the publication threshold",
      allReasons: [
        "products_or_protocol",
        "claim-level evidence strength is below the publication threshold",
        "direction weakened below the publication threshold after evidence gating",
      ],
    }));

    expect((markup.match(/门禁原因 \/ Primary gate reason/g) || [])).toHaveLength(1);
    expect((markup.match(/所有门禁原因 \/ All gate reasons/g) || [])).toHaveLength(1);
    expect(markup.indexOf("门禁原因")).toBeLessThan(markup.indexOf("所有门禁原因"));
    expect(markup.match(/claim-level evidence strength/g)).toHaveLength(2);
    expect(markup.match(/products_or_protocol/g)).toHaveLength(1);
    expect(markup).toContain("产品、主营业务或协议影响缺失");
    expect(markup).toContain("For equities, this means products or core business");
    expect(explainGateReason("products_or_protocol").explanation).toContain(
      "不是要求存在区块链协议",
    );
  });

  it("deduplicates a news item repeated as evidence", () => {
    const references = conclusionReferences({
      news: [{
        id: "news-1",
        title: "洪通燃气：上半年归母净利润9895.63万元 同比增长35.77%",
        url: "https://www.example.com/story?id=1&utm_source=feed",
        source: "东方财富/AkShare",
      }],
      evidence: [
        {
          id: "evidence-1",
          claim: "洪通燃气：上半年归母净利润9895.63万元 同比增长35.77%",
          source_url: "https://example.com/story?utm_medium=search&id=1",
          source_name: "东方财富/AkShare",
          excerpt: "same item",
        },
        {
          id: "evidence-2",
          claim: "独立公告",
          source_url: "https://example.com/filing/2",
          source_name: "巨潮资讯/CNInfo",
          excerpt: "filing",
        },
      ],
    });

    expect(references).toHaveLength(2);
    expect(references.map((item) => item.label)).toEqual([
      "洪通燃气：上半年归母净利润9895.63万元 同比增长35.77%",
      "独立公告",
    ]);
  });

  it("shows failed research recovery and builds kind-specific retry paths", () => {
    const markup = renderToStaticMarkup(createElement(ConclusionsPage, { apiBase: "" }));

    expect(markup).toContain("历史失败研究");
    expect(markup).toContain("重新执行会创建新任务");
    expect(markup).toContain("正在加载历史失败研究…");
    expect(markup).toContain("正在加载研究结论…");
    expect(markup).not.toContain("当前筛选范围内没有最终标的建议。");
    expect(markup).toContain("全部重试");
    expect(markup.indexOf(">全部重试<")).toBeLessThan(markup.indexOf(">刷新<"));
    expect(failedResearchBulkRetryPath).toBe(
      "/api/v1/failed-research-runs/retry",
    );
    expect(failedResearchRetryPath({ kind: "asset", id: "asset-run" })).toBe(
      "/api/v1/research-runs/asset-run/retry",
    );
    expect(failedResearchRetryPath({ kind: "event", id: "event-run" })).toBe(
      "/api/v1/event-research-runs/event-run/retry",
    );
    expect(failedResearchRetryPath(
      { kind: "asset", id: "asset-run" },
      "research-2",
    )).toBe("/api/v1/research-runs/asset-run/retry?instance_id=research-2");
  });

  it("posts one direct bulk retry request and formats the partial result", async () => {
    const calls: Array<{ input: string; method: string | undefined }> = [];
    const request = (async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ input: String(input), method: init?.method });
      return new Response(JSON.stringify({
        requested: 5,
        retried: 3,
        skipped: 1,
        failed: 1,
        results: [],
      }), { status: 202, headers: { "Content-Type": "application/json" } });
    }) as typeof fetch;

    const payload = await retryAllFailedResearch("http://api.example", request);

    expect(calls).toEqual([{
      input: "http://api.example/api/v1/failed-research-runs/retry",
      method: "POST",
    }]);
    expect(failedResearchBulkRetryMessage(payload)).toBe(
      "批量重试完成：已排队 3 条，跳过 1 条，失败 1 条。",
    );
    const remaining = failedResearchAfterBulkRetry([
      { kind: "asset", id: "queued", status: "failed", asset: null, event: null, error: null, updated_at: "2026-08-28T00:00:00Z", retry_count: 0, latest_retry: null },
      { kind: "event", id: "failed", status: "failed", asset: null, event: null, error: null, updated_at: "2026-08-28T00:00:01Z", retry_count: 0, latest_retry: null },
    ], {
      ...payload,
      results: [
        { kind: "asset", source_run_id: "queued", run_id: "retry", task_id: "task", status: "queued", detail: null },
        { kind: "event", source_run_id: "failed", run_id: null, task_id: null, status: "failed", detail: "broker error" },
      ],
    });
    expect(remaining.map((item) => item.id)).toEqual(["failed"]);
  });

  it("lists only healthy research instances as manual retry targets", () => {
    const counts = { queued: 0, running: 0, retrying: 0, verifying: 0, waiting_for_model: 0, completed: 0, failed: 0 };
    const metrics = { average_queue_duration_ms: null, average_execution_duration_ms: null, longest_wait_ms: null, estimated_clear_ms: null, queue_duration_sample_count: 0, execution_duration_sample_count: 0, execution_p50_ms: null, execution_p90_ms: null, throughput_per_hour: null };
    const instances = availableResearchInstances([{
      id: "research", model: "qwen2.5:7b", purpose: "标的研究", binding: "研究", enabled: true,
      state: "running", threads: 30, capacity: 3, available: 3, instance_count: 3,
      per_instance_concurrency: 1, observable: true, counts, metrics, total_tasks: 0,
      truncated: false, tasks: [], error: null,
      instances: [
        { id: "research-0", healthy: true, model_available: true },
        { id: "research-1", healthy: false, model_available: true },
        { id: "research-2", healthy: true, model_available: true },
      ],
    }]);

    expect(instances.map((item) => item.id)).toEqual(["research-0", "research-2"]);
  });

  it("shows MCP management controls without an administrator unlock", () => {
    const markup = renderToStaticMarkup(createElement(SourcesPage, { apiBase: "" }));

    expect(markup).toContain("新增 MCP 来源");
    expect(markup).toContain("刷新");
    expect(markup).not.toContain("管理员令牌");
  });

  it("renders five fixed collapsed groups and keeps Other conditional", () => {
    const markup = renderToStaticMarkup(createElement(SourcesPage, { apiBase: "" }));

    for (const name of ["FMP 美股数据", "SEC 官方文件", "A股与新闻", "数字资产", "网络搜索与交叉验证"]) {
      expect(markup).toContain(name);
    }
    expect((markup.match(/aria-expanded="false"/g) || []).length).toBe(5);
    expect(markup).not.toContain(">其他数据源<");
  });

  it("auto-expands only the first unhealthy group after loading", () => {
    expect(firstUnhealthyGroup([
      { id: "fmp", status: "healthy" },
      { id: "sec", status: "pending" },
      { id: "crypto", status: "failed" },
    ])).toBe("sec");
    expect(firstUnhealthyGroup([{ id: "fmp", status: "healthy" }])).toBeNull();
  });

  it("never renders an FMP token returned accidentally by an API", () => {
    const plaintext = "rest-plaintext-must-not-render";
    const markup = renderToStaticMarkup(createElement(NativeConfigEditor, {
      group: {
        id: "fmp", badge: "US", name: "FMP 美股数据", description: "", tone: "amber",
        status: "healthy", configured_count: 1, mcp_count: 1, config_source: "database",
        config: { access_token_configured: true, access_token_source: "database", access_token: plaintext },
        mcp_sources: [],
      },
      draft: {
        base_url: "https://financialmodelingprep.com/stable", access_token: "",
        clear_access_token: false, rate_limit_per_minute: 240, news_lookback_hours: 12,
      },
      onDraft: () => undefined,
    }));

    expect(markup).not.toContain(plaintext);
    expect(markup).toContain("已配置（database）");
  });

  it("offers every fact group when assigning a new MCP source", () => {
    expect(factSourceGroupOptions.map((group) => group.id)).toEqual([
      "fmp", "sec", "cn_news", "crypto", "search", "other",
    ]);
  });

  it("shows the search form without an administrator unlock", () => {
    const markup = renderToStaticMarkup(createElement(SearchPage, { apiBase: "" }));

    expect(markup).toContain("输入需要验证的问题");
    expect(markup).toContain("搜索验证");
    expect(markup).not.toContain("管理员令牌");
  });

  it("shows every search source that found a merged result", () => {
    expect(searchSourceLabel({
      source: "SearXNG",
      sources: ["SearXNG", "DuckDuckGo", "SearXNG"],
    })).toBe("SearXNG + DuckDuckGo");
    expect(searchSourceLabel({ source: "DuckDuckGo" })).toBe("DuckDuckGo");
  });

  it("offers enabled web and financial-news MCP sources in search", () => {
    expect(isSearchSource({ enabled: true, tool_mappings: { web_search: {} } })).toBe(true);
    expect(isSearchSource({ enabled: true, tool_mappings: { news_search: {} } })).toBe(true);
    expect(isSearchSource({ enabled: false, tool_mappings: { news_search: {} } })).toBe(false);
    expect(isSearchSource({ enabled: true, tool_mappings: { news_feed: {} } })).toBe(false);
  });

  it("shows public source-filter defaults and editing guidance", () => {
    const markup = renderToStaticMarkup(createElement(SourceFilterPage, { apiBase: "" }));

    expect(markup).toContain("数据源过滤");
    expect(markup).toContain("启用新闻标题过滤");
    expect(markup).toContain("白名单关键字");
    expect(markup).toContain("黑名单关键字");
    expect(markup).toContain("天气");
    expect(markup).toContain("命中才允许进入 3B");
    expect(markup).toContain("命中即禁止进入 3B");
    expect(markup).toContain("黑名单拥有否决权");
    expect(markup).not.toContain("管理员令牌");
  });

  it("splits, trims, and deduplicates filter keywords", () => {
    expect(parseFilterKeywords(" 天气, WEATHER，weather\n公告 ")).toEqual([
      "天气", "WEATHER", "公告",
    ]);
  });

  it("offers rescan only for whitelist misses", () => {
    const item = {
      id: "filter-log-1",
      source: "FMP Stock News",
      title: "Tesla catalyst",
      url: "https://example.com/tesla",
      matched_keyword: "未命中白名单",
      published_at: "2026-08-29T00:00:00Z",
      first_filtered_at: "2026-08-29T00:01:00Z",
      last_filtered_at: "2026-08-29T00:02:00Z",
      hit_count: 3,
      rescan_allowed: true,
    };
    const allowed = renderToStaticMarkup(createElement(SourceFilterAuditRow, {
      item, busy: false, onRescan: () => undefined,
    }));
    const blocked = renderToStaticMarkup(createElement(SourceFilterAuditRow, {
      item: { ...item, matched_keyword: "天气", rescan_allowed: false },
      busy: false,
      onRescan: () => undefined,
    }));

    expect(sourceFilterRescanPath(item.id)).toBe(
      "/api/v1/source-filter/logs/filter-log-1/rescan",
    );
    expect(allowed).toContain(">重新扫描</button>");
    expect(blocked).not.toContain(">重新扫描</button>");
  });
});
