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
    rating_state: {
      previous: "bullish",
      current: "watch",
      changed_at: "2026-08-30T08:00:00Z",
      algorithm_version: "step-limited-rating-v1",
      eligible_event_count: 6,
      transition_limited: true,
    },
    latest_event_signal: {
      event_id: "event-1",
      rating: "strongly_bearish",
      direction_score: -90,
      rating_confidence: 0.8,
      news_confidence: 0.85,
      occurred_at: "2026-08-30T07:00:00Z",
      detail: { kind: "event", id: "11111111-1111-1111-1111-111111111111", researched_at: "2026-08-30T08:00:00Z" },
    },
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
    expect(markup).toContain("总体评级变化");
    expect(markup).toContain("看多 → 中性");
    expect(markup).toContain("最新新闻信号");
    expect(markup).toContain("强烈看空");
    expect(markup).toContain("-90");
    expect(markup).toContain("单步限制");
    expect(markup).toContain("长期证据趋势");
    expect(markup).toContain("短期证据趋势");
    expect(markup).toContain("综合证据参考");
    expect(markup).toContain("低置信事件未纳入证据趋势");
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
