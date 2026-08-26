import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  ConclusionScore,
  ConclusionsPage,
  conclusionReferences,
  factSourceGroupOptions,
  failedResearchRetryPath,
  firstUnhealthyGroup,
  isSearchSource,
  NativeConfigEditor,
  parseFilterKeywords,
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

    expect(markup).toContain("方向评分：0");
    expect(markup).toContain("评级：观察");
    expect(markup).toContain("置信度 95% · 证据完整");
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
    expect(failedResearchRetryPath({ kind: "asset", id: "asset-run" })).toBe(
      "/api/v1/research-runs/asset-run/retry",
    );
    expect(failedResearchRetryPath({ kind: "event", id: "event-run" })).toBe(
      "/api/v1/event-research-runs/event-run/retry",
    );
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
