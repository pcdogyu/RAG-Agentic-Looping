import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { TargetChangeGrid, type TargetChange } from "./AppPages";

function macroTarget(withTrend = true): TargetChange {
  return {
    kind: "macro",
    key: "sector:digital_assets",
    label: "数字资产",
    symbol: null,
    market: null,
    target_type: "sector",
    changed_at: "2026-08-30T08:00:00Z",
    previous: { rating: "bullish", direction_score: 69, rating_confidence: 0.8 },
    current: { rating: "bearish", direction_score: -50, rating_confidence: 0.3 },
    latest: { rating: "bearish", direction_score: -50, rating_confidence: 0.3, news_confidence: 0.85 },
    trend: withTrend ? {
      long_term: { direction_score: 57.25, rating: "bullish", rating_confidence: 0.825, provisional: false },
      short_term: { direction_score: -50, rating: "bearish", rating_confidence: 0.575, provisional: true },
      composite: { direction_score: 35.8, rating: "bullish", rating_confidence: 0.775, provisional: true },
      event_count_90d: 6,
      eligible_event_count_90d: 5,
      ignored_event_count_90d: 1,
      regime_break: false,
    } : undefined,
    latest_detail: { kind: "event", id: "11111111-1111-1111-1111-111111111111", researched_at: "2026-08-30T08:00:00Z" },
    change_detail_id: "11111111-1111-1111-1111-111111111111",
  };
}

describe("target trend integration", () => {
  it("renders long, short and composite views on one canonical target card", () => {
    const markup = renderToStaticMarkup(createElement(TargetChangeGrid, {
      items: [macroTarget()],
      onOpen: () => undefined,
    }));

    expect(markup.match(/target-change-card macro/g)?.length).toBe(1);
    expect(markup).toContain("数字资产");
    expect(markup).toContain("最近事件评级变化");
    expect(markup).toContain("长期趋势");
    expect(markup).toContain("短期冲击");
    expect(markup).toContain("综合参考");
    expect(markup).toContain("低置信事件未改变长期评级");
  });

  it("keeps rendering legacy target payloads without trend data", () => {
    const markup = renderToStaticMarkup(createElement(TargetChangeGrid, {
      items: [macroTarget(false)],
      onOpen: () => undefined,
    }));

    expect(markup).toContain("数字资产");
    expect(markup).not.toContain("目标趋势摘要");
  });
});
