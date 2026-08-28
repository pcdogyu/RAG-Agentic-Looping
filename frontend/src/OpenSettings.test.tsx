import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  availableResearchInstances,
  ConclusionScore,
  ConclusionsPage,
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
  searchSourceLabel,
  SearchPage,
  SourceFilterPage,
  SourcesPage,
} from "./AppPages";

describe("open source and search settings", () => {
  it("labels the conclusion direction score and rating explicitly", () => {
    const markup = renderToStaticMarkup(createElement(ConclusionScore, {
      score: 0,
      rating: "watch",
      confidence: 0.95,
      evidenceComplete: true,
    }));

    expect(markup).toContain("发布分：0");
    expect(markup).toContain("评级：观察");
    expect(markup).toContain("发布置信度 95% · 资料覆盖完整");
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
    expect(markup).toContain("方向证据不足 · 评级：观察");
    expect(markup).toContain("门禁后参考置信度 0%");
    expect(markup).not.toContain("发布分：0");
  });

  it("shows the 7B opinion independently from the publication gate", () => {
    const markup = renderToStaticMarkup(createElement(ModelOpinion, {
      direction: "bearish",
      rating: "strongly_bearish",
      confidence: 0.68,
    }));

    expect(markup).toContain("7B 模型意见（门禁前）");
    expect(markup).toContain("看空 / Bearish");
    expect(markup).toContain("强烈看空 / Strongly bearish");
    expect(markup).toContain("68%");
    expect(markup).toContain("not a published score");
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
});
