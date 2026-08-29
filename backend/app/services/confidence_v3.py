from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

from backend.app.domain import (
    ActionStage,
    CandidateAsset,
    EventType,
    Evidence,
    NewsConfidenceFactors,
    NewsEvent,
    NewsItem,
    Rating,
    RatingConfidenceFactorsV3,
    SourceQuality,
    SystemConfidenceFactor,
)
from backend.app.services.source_lineage import canonicalize_url, independent_evidence_groups

SCORING_VERSION = "llm-direction-v3"
NEWS_CONFIDENCE_VERSION = "news-confidence-v1"
RATING_CONFIDENCE_VERSION = "system-rating-confidence-v3"

_SOURCE_RELIABILITY = {
    SourceQuality.OFFICIAL: 1.00,
    SourceQuality.PRIMARY: 0.90,
    SourceQuality.PROFESSIONAL: 0.82,
    SourceQuality.AGGREGATOR: 0.65,
    SourceQuality.SOCIAL: 0.40,
}

_NEWS_CLARITY = {
    ActionStage.REALIZED: 1.00,
    ActionStage.EFFECTIVE: 0.95,
    ActionStage.ANNOUNCED: 0.85,
    ActionStage.THREAT: 0.55,
    ActionStage.STATEMENT: 0.35,
    ActionStage.UNKNOWN: 0.20,
}

_TIMING_CERTAINTY = {
    ActionStage.REALIZED: 1.00,
    ActionStage.EFFECTIVE: 0.90,
    ActionStage.ANNOUNCED: 0.75,
    ActionStage.THREAT: 0.45,
    ActionStage.STATEMENT: 0.25,
    ActionStage.UNKNOWN: 0.00,
}

_DISTANCE_VALUE = {0: 1.00, 1: 0.95, 2: 0.80, 3: 0.60, 4: 0.40, 5: 0.20}

_EVENT_HORIZONS = {
    EventType.EARNINGS: 30,
    EventType.SECURITY: 30,
    EventType.M_AND_A: 180,
    EventType.PRODUCT: 90,
    EventType.REGULATION: 90,
    EventType.MANAGEMENT: 90,
    EventType.MACRO: 90,
    EventType.SUPPLY_CHAIN: 90,
    EventType.TOKENOMICS: 90,
    EventType.OTHER: 90,
}


@dataclass(frozen=True)
class NewsConfidenceScoreV3:
    confidence: float
    factors: NewsConfidenceFactors


@dataclass(frozen=True)
class RatingConfidenceScoreV3:
    confidence: float
    factors: RatingConfidenceFactorsV3
    mapping_distance: int


def _unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def event_horizon_days(event_type: EventType) -> int:
    return _EVENT_HORIZONS[event_type]


def rating_for_direction_score(score: int) -> Rating:
    if score >= 70:
        return Rating.STRONGLY_BULLISH
    if score >= 30:
        return Rating.BULLISH
    if score <= -70:
        return Rating.STRONGLY_BEARISH
    if score <= -30:
        return Rating.BEARISH
    return Rating.WATCH


def _factor(value: float, reason: str, evidence: Sequence[Evidence] = ()) -> SystemConfidenceFactor:
    return SystemConfidenceFactor(
        value=round(_unit(value), 4),
        reason=reason,
        evidence_ids=[item.id for item in evidence],
    )


def _originality(item: NewsItem) -> float:
    if item.source_quality in {SourceQuality.OFFICIAL, SourceQuality.PRIMARY}:
        return 1.0
    metadata = item.raw_metadata
    explicit_origin = (
        metadata.get("original_source")
        or metadata.get("originalSource")
        or metadata.get("wire_service")
    )
    lineage = metadata.get("source_lineage")
    if isinstance(lineage, Mapping):
        explicit_origin = explicit_origin or lineage.get("original_source")
        publisher = str(lineage.get("publisher_domain") or "").casefold()
    else:
        publisher = ""
    if explicit_origin:
        origin = str(explicit_origin).casefold()
        if publisher and (publisher in origin or origin in publisher):
            return 0.9
        return 0.6
    return {
        SourceQuality.PROFESSIONAL: 0.7,
        SourceQuality.AGGREGATOR: 0.35,
        SourceQuality.SOCIAL: 0.2,
    }.get(item.source_quality, 0.35)


def _freshness(evidence: Sequence[Evidence]) -> float:
    if not evidence:
        return 0.0
    delay = min(
        (max(timedelta(0), item.observed_at - item.published_at) for item in evidence),
        default=timedelta(days=365),
    )
    if delay <= timedelta(hours=1):
        return 1.0
    if delay <= timedelta(hours=6):
        return 0.9
    if delay <= timedelta(hours=24):
        return 0.75
    if delay <= timedelta(hours=72):
        return 0.5
    return 0.25


def news_confidence_score(
    event: NewsEvent,
    evidence: Sequence[Evidence],
    news_items: Sequence[NewsItem] = (),
) -> NewsConfidenceScoreV3:
    relevant = list(evidence)
    source = max((_SOURCE_RELIABILITY[item.source_quality] for item in relevant), default=0.0)
    if news_items:
        originality = max((_originality(item) for item in news_items), default=0.0)
    else:
        originality = max(
            (
                1.0
                if item.source_quality in {SourceQuality.OFFICIAL, SourceQuality.PRIMARY}
                else 0.7
                if item.source_quality is SourceQuality.PROFESSIONAL
                else 0.35
                if item.source_quality is SourceQuality.AGGREGATOR
                else 0.2
                for item in relevant
            ),
            default=0.0,
        )
    groups = len(independent_evidence_groups(relevant))
    verification = {0: 0.0, 1: 0.5, 2: 0.8}.get(groups, 1.0)
    if groups == 1 and any(item.source_quality is SourceQuality.OFFICIAL for item in relevant):
        verification = 0.7
    clarity = max((_NEWS_CLARITY[item.action_stage] for item in event.actions), default=0.2)
    action = event.actions[0] if event.actions else None
    fields = [
        event.headline,
        next((item.source_url for item in relevant if item.source_url), ""),
        event.published_at,
        event.direct_impact,
        action.actor if action else "",
        action.action if action else "",
        action.object if action else "",
        action.scope if action else "",
    ]
    completeness = sum(value not in (None, "") for value in fields) / len(fields)
    timely_complete = 0.6 * completeness + 0.4 * _freshness(relevant)
    factors = NewsConfidenceFactors(
        source_reliability=_factor(source, "按事件新闻中的最高来源等级计算。", relevant),
        originality=_factor(originality, "根据一手来源标记和转载血缘计算。", relevant),
        cross_verification=_factor(
            verification, f"去重后共有 {groups} 个独立来源组。", relevant
        ),
        clarity=_factor(clarity, "根据事件动作所处阶段计算。", relevant),
        timeliness_completeness=_factor(
            timely_complete,
            f"必填信息覆盖率 {completeness:.0%}，并计入发布时间到采集时间的延迟。",
            relevant,
        ),
    )
    confidence = (
        0.30 * source
        + 0.20 * originality
        + 0.20 * verification
        + 0.15 * clarity
        + 0.15 * timely_complete
    )
    return NewsConfidenceScoreV3(round(confidence, 4), factors)


def select_news_confidence_evidence(
    evidence: Sequence[Evidence],
    news_items: Sequence[NewsItem],
) -> list[Evidence]:
    """Keep event stories and web verification, excluding research/market evidence."""
    event_news_urls = {canonicalize_url(item.url) for item in news_items}
    return [
        item
        for item in evidence
        if canonicalize_url(item.source_url) in event_news_urls
        or item.independent_group.casefold().startswith("web:")
    ]


def mapping_distance_for(
    candidate: CandidateAsset | None,
    transmission_path: Sequence[str],
) -> int:
    if candidate is not None:
        relationship = candidate.relationship.casefold()
        if relationship in {"direct", "issuer"}:
            return 0
        if relationship in {"product_owner", "cross_listing_issuer", "entity"}:
            return 1
        return 2
    return min(5, max(1, len(transmission_path) - 2))


def _direction_class(score: int) -> int:
    if score >= 30:
        return 1
    if score <= -30:
        return -1
    return 0


def _signal_agreement(score: int, signal: float) -> float:
    normalized = max(-1.0, min(1.0, float(signal)))
    return _unit(1.0 - abs(score / 100 - normalized) / 2)


def _category_confirmation(
    score: int,
    factor_summary: Mapping[str, object],
    categories: set[str],
) -> float:
    signals = factor_summary.get("categories")
    reliabilities = factor_summary.get("category_reliability")
    if not isinstance(signals, Mapping) or not isinstance(reliabilities, Mapping):
        return 0.0
    values: list[float] = []
    for category in categories:
        signal = signals.get(category)
        reliability = reliabilities.get(category)
        if isinstance(signal, int | float) and isinstance(reliability, int | float):
            values.append(_unit(float(reliability)) * _signal_agreement(score, float(signal)))
    return sum(values) / len(values) if values else 0.0


def _horizon_market_confirmation(
    score: int,
    factor_summary: Mapping[str, object],
) -> float:
    factors = factor_summary.get("factors")
    if not isinstance(factors, Sequence) or isinstance(factors, str | bytes):
        return 0.0
    values: list[float] = []
    for factor in factors:
        if not isinstance(factor, Mapping):
            continue
        key = str(factor.get("key") or "")
        if "_horizon_" not in key:
            continue
        signal = factor.get("normalized_signal", factor.get("direction"))
        reliability = factor.get("reliability")
        if isinstance(signal, int | float) and isinstance(reliability, int | float):
            values.append(_unit(float(reliability)) * _signal_agreement(score, float(signal)))
    return sum(values) / len(values) if values else 0.0


def rating_confidence_score(
    *,
    direction_score: int,
    event: NewsEvent,
    candidate: CandidateAsset | None,
    transmission_path: Sequence[str],
    cited_evidence_ids: Sequence[UUID],
    evidence: Sequence[Evidence],
    missing_information: Sequence[str] = (),
    factor_summary: Mapping[str, object] | None = None,
    historical_reactions: Sequence[int] = (),
) -> RatingConfidenceScoreV3:
    summary = factor_summary or {}
    distance = mapping_distance_for(candidate, transmission_path)
    mapping = (
        min(
            candidate.relevance,
            candidate.mapping_confidence,
            _DISTANCE_VALUE[distance],
        )
        if candidate is not None
        else _DISTANCE_VALUE[distance]
    )
    valid_ids = {item.id for item in evidence}
    proposed_ids = list(dict.fromkeys(cited_evidence_ids))
    citation_coverage = (
        len([item for item in proposed_ids if item in valid_ids]) / len(proposed_ids)
        if proposed_ids
        else 0.0
    )
    path_structure = (
        1.0
        if len(transmission_path) >= 3
        else 0.6
        if len(transmission_path) == 2
        else 0.3
        if transmission_path
        else 0.0
    )
    path_text = " ".join(transmission_path).casefold()
    financial_connection = float(
        any(
            term in path_text
            for term in (
                "营收",
                "收入",
                "成本",
                "利润",
                "现金流",
                "估值",
                "revenue",
                "cost",
                "profit",
                "earnings",
                "cash flow",
                "valuation",
            )
        )
    )
    causality = (
        0.45 * citation_coverage
        + 0.30 * path_structure
        + 0.25 * financial_connection
    )
    if any(item in {"impact_evidence", "transmission_evidence"} for item in missing_information):
        causality *= 0.5

    direction_class = _direction_class(direction_score)
    if historical_reactions:
        matches = sum(item == direction_class for item in historical_reactions)
        historical = matches / len(historical_reactions)
        if len(historical_reactions) < 3:
            historical *= len(historical_reactions) / 3
    else:
        historical = 0.0

    market = _horizon_market_confirmation(direction_score, summary)
    cited_set = set(cited_evidence_ids)
    numeric_count = sum(
        item.id in cited_set and item.numeric_value is not None for item in evidence
    )
    numeric_coverage = min(1.0, numeric_count / 3)
    business_confirmation = _category_confirmation(
        direction_score,
        summary,
        {"expectation_gap", "earnings_quality", "capital_action"},
    )
    business_exposure = (
        min(candidate.relevance, candidate.mapping_confidence)
        if candidate is not None
        else 0.0
    )
    if direction_class:
        impact = (
            0.45 * abs(direction_score) / 100
            + 0.25 * numeric_coverage
            + 0.15 * business_exposure
            + 0.15 * business_confirmation
        )
    elif causality >= 0.7 and (mapping <= 0.4 or max(historical, market) >= 0.6):
        impact = max(causality, historical, market)
    else:
        impact = 0.4 * numeric_coverage
    timing = max((_TIMING_CERTAINTY[item.action_stage] for item in event.actions), default=0.0)

    factors = RatingConfidenceFactorsV3(
        mapping_strength=_factor(
            mapping,
            f"映射距离 L{distance}；使用标的相关性和身份映射可信度。",
        ),
        causality_certainty=_factor(
            causality,
            f"有效引用覆盖率 {citation_coverage:.0%}；路径结构 {path_structure:.0%}；"
            f"财务结果连接 {'已确认' if financial_connection else '缺失'}。",
        ),
        historical_pattern=_factor(
            historical,
            f"使用 {len(historical_reactions)} 个相同事件类型、相同评级周期的历史结果。",
        ),
        impact_scale=_factor(
            impact,
            f"方向绝对值 {abs(direction_score)}，引用的可量化证据 {numeric_count} 条，"
            f"业务暴露 {business_exposure:.0%}。",
        ),
        timing_certainty=_factor(timing, "根据事件动作阶段及生效确定性计算。"),
        market_consistency=_factor(
            market,
            "根据事件窗口行情、相对基准和行业表现与方向分的一致性计算。",
        ),
    )
    confidence = (
        0.25 * mapping
        + 0.20 * causality
        + 0.15 * historical
        + 0.15 * impact
        + 0.10 * timing
        + 0.15 * market
    )
    return RatingConfidenceScoreV3(round(confidence, 4), factors, distance)
