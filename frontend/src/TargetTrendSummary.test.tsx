import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { TargetTrendSummary, type TargetTrend } from "./TargetTrendSummary";

function trend(overrides: Partial<TargetTrend> = {}): TargetTrend {
  return {
    long_term: { direction_score: 52, rating: "bullish", rating_confidence: 0.78, provisional: false },
    short_term: { direction_score: -46, rating: "bearish", rating_confidence: 0.72, provisional: false },
    composite: { direction_score: 32, rating: "bullish", rating_confidence: 0.75, provisional: false },
    event_count_90d: 12,
    eligible_event_count_90d: 10,
    ignored_event_count_90d: 0,
    regime_break: false,
    ...overrides,
  };
}

describe("TargetTrendSummary", () => {
  it("shows a bullish long-term trend alongside a bearish short-term shock", () => {
    const markup = renderToStaticMarkup(createElement(TargetTrendSummary, { trend: trend() }));

    expect(markup).toContain("长期趋势");
    expect(markup).toContain("短期冲击");
    expect(markup).toContain("综合参考");
    expect(markup).toContain("看多");
    expect(markup).toContain("看空");
    expect(markup).toContain("+52");
    expect(markup).toContain("-46");
    expect(markup).toContain("+32");
  });

  it("explains that ignored low-confidence events did not change the long-term rating", () => {
    const markup = renderToStaticMarkup(createElement(TargetTrendSummary, {
      trend: trend({
        ignored_event_count_90d: 2,
        long_term: { direction_score: 52, rating: "bullish", rating_confidence: 0.42, provisional: true },
      }),
    }));

    expect(markup).toContain("低置信事件未改变长期评级");
    expect(markup).toContain("忽略 2 条");
    expect(markup).toContain("暂定");
  });

  it("marks a regime break", () => {
    const markup = renderToStaticMarkup(createElement(TargetTrendSummary, { trend: trend({ regime_break: true }) }));

    expect(markup).toContain("制度性转折");
    expect(markup).toContain('role="status"');
  });
});
