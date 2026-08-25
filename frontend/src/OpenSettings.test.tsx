import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  factSourceGroupOptions,
  firstUnhealthyGroup,
  NativeConfigEditor,
  parseFilterKeywords,
  searchSourceLabel,
  SearchPage,
  SourceFilterPage,
  SourcesPage,
} from "./AppPages";

describe("open source and search settings", () => {
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

  it("shows public source-filter defaults and editing guidance", () => {
    const markup = renderToStaticMarkup(createElement(SourceFilterPage, { apiBase: "" }));

    expect(markup).toContain("数据源过滤");
    expect(markup).toContain("启用新闻标题过滤");
    expect(markup).toContain("白名单关键字");
    expect(markup).toContain("黑名单关键字");
    expect(markup).toContain("天气");
    expect(markup).toContain("白名单优先");
    expect(markup).not.toContain("管理员令牌");
  });

  it("splits, trims, and deduplicates filter keywords", () => {
    expect(parseFilterKeywords(" 天气, WEATHER，weather\n公告 ")).toEqual([
      "天气", "WEATHER", "公告",
    ]);
  });
});
