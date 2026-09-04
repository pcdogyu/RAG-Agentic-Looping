export interface TargetTrendPeriod {
  direction_score: number | null;
  rating: string | null;
  rating_confidence: number | null;
  provisional: boolean;
}

export interface TargetTrend {
  short_term: TargetTrendPeriod;
  long_term: TargetTrendPeriod;
  composite: TargetTrendPeriod;
  event_count_90d: number;
  eligible_event_count_90d: number;
  ignored_event_count_90d: number;
  regime_break: boolean;
}

interface TargetTrendSummaryProps {
  trend: TargetTrend;
}

const ratingLabels: Record<string, string> = {
  strongly_bullish: "强烈看多",
  bullish: "看多",
  watch: "中性",
  bearish: "看空",
  strongly_bearish: "强烈看空",
};

function ratingLabel(value: string) {
  const normalized = value.trim() === "官网" ? "watch" : value.trim();
  return ratingLabels[normalized] || normalized;
}

function scoreLabel(score: number | null) {
  if (score === null) return "—";
  return `${score > 0 ? "+" : ""}${score}`;
}

function confidenceLabel(confidence: number | null) {
  if (confidence === null) return "—";
  return `${Math.round(confidence * 100)}%`;
}

function TrendPeriod({ label, period }: { label: string; period: TargetTrendPeriod }) {
  const scoreClass = period.direction_score === null || period.direction_score === 0
    ? "neutral"
    : period.direction_score > 0 ? "positive" : "negative";
  return <section className="target-trend-period" aria-label={label}>
    <h4>{label}</h4>
    <p className="target-trend-period__signal">
      <strong>{period.rating ? ratingLabel(period.rating) : "未评级"}</strong>
      <span className={`target-trend-period__score ${scoreClass}`}>{scoreLabel(period.direction_score)}</span>
    </p>
    <p className="target-trend-period__confidence">
      趋势置信度 {confidenceLabel(period.rating_confidence)}
      {period.provisional ? <span className="target-trend-period__provisional"> · 暂定</span> : null}
    </p>
  </section>;
}

export function TargetTrendSummary({ trend }: TargetTrendSummaryProps) {
  const lowConfidenceFiltered = trend.ignored_event_count_90d > 0
    || trend.short_term.provisional
    || trend.long_term.provisional
    || trend.composite.provisional;

  return <section className="target-trend-summary" aria-label="目标趋势摘要">
    <div className="target-trend-summary__periods">
      <TrendPeriod label="长期证据趋势" period={trend.long_term} />
      <TrendPeriod label="短期证据趋势" period={trend.short_term} />
      <TrendPeriod label="综合证据参考" period={trend.composite} />
    </div>
    <p className="target-trend-summary__evidence" aria-label="90 天事件统计">
      90 天事件 {trend.event_count_90d} 条 · 有效 {trend.eligible_event_count_90d} 条 · 忽略 {trend.ignored_event_count_90d} 条
    </p>
    {lowConfidenceFiltered ? <p className="target-trend-summary__notice" role="status">低置信事件未纳入证据趋势</p> : null}
    {trend.regime_break ? <p className="target-trend-summary__regime" role="status">制度性转折（仍按单步更新总体评级）</p> : null}
  </section>;
}
