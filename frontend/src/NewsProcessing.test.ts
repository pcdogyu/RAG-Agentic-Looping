import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  NewsSourcePanel,
  newsBoardStatusLabels,
  type NewsBoardSource,
} from "./AppPages";

describe("durable news processing", () => {
  it("shows an interrupted item with its recovery action", () => {
    const group: NewsBoardSource = {
      source: "金十",
      latest_published_at: "2026-08-29T06:15:00Z",
      item_count: 1,
      error: null,
      items: [{
        id: "news-changxin",
        title: "长鑫存储起诉美国国防部",
        summary: "新闻摘要",
        url: "https://example.com/changxin",
        source_quality: "professional",
        published_at: "2026-08-29T06:15:00Z",
        observed_at: "2026-08-29T07:16:00Z",
        status: "orphaned",
        status_updated_at: "2026-08-29T07:16:00Z",
        status_detail: "新闻已入库，但没有抽取任务或关联事件。",
        retryable: true,
        events: [],
        assets: [],
      }],
    };

    const markup = renderToStaticMarkup(createElement(NewsSourcePanel, {
      group,
      onRetry: () => undefined,
    }));

    expect(newsBoardStatusLabels.orphaned).toBe("入队中断");
    expect(markup).toContain("入队中断");
    expect(markup).toContain("新闻已入库，但没有抽取任务或关联事件。");
    expect(markup).toContain("重新处理");
  });

  it("disables the action after the retry is accepted", () => {
    const group: NewsBoardSource = {
      source: "金十",
      latest_published_at: "2026-08-29T06:15:00Z",
      item_count: 1,
      error: null,
      items: [{
        id: "news-changxin",
        title: "长鑫存储起诉美国国防部",
        summary: "新闻摘要",
        url: "https://example.com/changxin",
        source_quality: "professional",
        published_at: "2026-08-29T06:15:00Z",
        observed_at: "2026-08-29T07:16:00Z",
        status: "failed",
        status_updated_at: "2026-08-29T07:16:00Z",
        retryable: true,
        events: [],
        assets: [],
      }],
    };

    const markup = renderToStaticMarkup(createElement(NewsSourcePanel, {
      group,
      retryStates: { "news-changxin": { status: "queued" } },
      onRetry: () => undefined,
    }));

    expect(markup).toContain("已重新入队");
    expect(markup).toContain("disabled");
  });
});
