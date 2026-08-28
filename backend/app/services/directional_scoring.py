from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

_ADVERSE_DIRECTION_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bnegative\b",
        r"\bsecurities?\s+fraud\b",
        r"\bclass[ -]action(?:\s+lawsuit)?\b",
        r"\blawsuit\s+against\b",
        r"\binvestigation\s+alert\b",
        r"\binvestigat(?:e|es|ed|ing|ion).{0,40}\bstockholders?\b",
        r"\bpotential\s+securities\s+laws?\s+violations?\b",
        r"\bnet\s+(?:income|profit).{0,24}\b(?:negative|loss)\b",
        r"\bnet\s+loss(?:es)?\b",
        r"\bsuffer(?:ed|ing)?\s+loss(?:es)?\b",
        r"\bfinancial\s+(?:condition|position).{0,24}\b(?:deteriorat|weaken)",
        r"证券欺诈|集体诉讼|股东调查|调查警报|证券违法|法律调查",
        r"净利润.{0,16}(?:仍|持续)?(?:为)?负|净亏损|持续亏损|财务状况.{0,12}(?:恶化|转差)",
        r"(?:市场|投资者)(?:信心|情绪).{0,12}(?:负面|下降|恶化)",
        r"利空|负面影响",
    )
)

_GENERIC_ADVERSE_DIRECTION_PATTERNS = (re.compile(r"下降", re.IGNORECASE),)

_BENEFICIAL_DIRECTION_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bpositive\b",
        r"\b(?:beat|growth)\b",
        r"\b(?:directly|clearly|materially|expected to|poised to)\s+benefit\b",
        r"\b(?:creates?|drives?|adds?)\s+(?:new\s+)?(?:orders?|revenue|profit|earnings)\b",
        r"\b(?:boosts?|improves?)\s+(?:revenue|profit|earnings|profitability|margins?)\b",
        r"\brecord\s+(?:revenue|profit|earnings)\b",
        r"\breturn(?:ed|s|ing)?\s+to\s+profitability\b",
        r"\b(?:regulatory\s+)?approval\b",
        r"(?:明确|直接|显著|有望|预计|可能从中)受益",
        r"带来新增订单|推动(?:收入|营收|利润|盈利).{0,12}增长|改善盈利能力",
        r"扭亏为盈|增长|创新高|创纪录(?:营收|利润|业绩)|获批|利好|正面影响",
    )
)

_UNCERTAIN_OR_NEGATED_BENEFICIAL_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\b(?:may|might|could)\s+not\s+benefit\b",
        r"\b(?:unlikely|unable)\s+to\s+benefit\b",
        r"\b(?:limited|uncertain)\s+benefit\b",
        r"\bbenefits?\s+(?:are|remain|is)\s+(?:limited|uncertain|unclear)\b",
        r"\bbenefits?.{0,32}(?:offset|outweighed)\s+by\s+(?:costs?|spending|investment)\b",
        r"(?:未必|不一定|不会|无法|难以|并未)(?:从中)?受益",
        r"受益(?:程度)?有限|能否受益.{0,12}(?:不确定|尚不明确|未知)",
        r"是否受益.{0,12}(?:不确定|尚不明确|未知)|受益.{0,12}(?:仍不确定|尚不明确)",
        r"(?:收益|受益).{0,24}(?:被|由).{0,12}(?:成本|投入|支出).{0,12}抵消",
    )
)

_TERM_NEGATION_PATTERN = re.compile(
    r"(?:未|没有|并未|不再|无|尚未|并非|不是|未能|无法|难以|避免|免于)"
    r"(?:出现|发生|实现|达到|取得|获得|形成|造成|带来|导致)?\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DirectionScore:
    raw_score: int
    probability_score: int
    event_score: int | None
    factor_score: int | None


@dataclass(frozen=True)
class ShortTermImpactScore:
    score: int
    magnitude_contribution: float
    persistence_contribution: float
    representativeness_contribution: float
    market_confirmation_contribution: float


@dataclass(frozen=True)
class RatingConfidenceScore:
    confidence: float
    direction_clarity_contribution: float
    source_reliability_contribution: float
    magnitude_certainty_contribution: float
    market_context_completeness_contribution: float


def _unit_interval(value: float | None) -> float:
    return max(0.0, min(1.0, float(value or 0)))


def round_half_up(value: float) -> int:
    """Use conventional half-up rounding instead of Python's bankers rounding."""

    return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def short_term_impact_score(
    *,
    direction: int,
    magnitude: float | None,
    persistence: float | None,
    representativeness: float | None,
    market_confirmation: float | None,
) -> ShortTermImpactScore:
    """Score expected impact over the next 1-3 trading sessions.

    Missing factors are treated as zero and values outside the documented
    interval are clamped. Direction is deliberately strict because silently
    converting an invalid sign would invert a published investment opinion.
    """

    if direction not in {-1, 0, 1}:
        raise ValueError("direction must be -1, 0, or 1")
    magnitude_contribution = direction * 45 * _unit_interval(magnitude)
    persistence_contribution = direction * 25 * _unit_interval(persistence)
    representativeness_contribution = direction * 15 * _unit_interval(
        representativeness
    )
    market_confirmation_contribution = direction * 15 * _unit_interval(
        market_confirmation
    )
    total = (
        magnitude_contribution
        + persistence_contribution
        + representativeness_contribution
        + market_confirmation_contribution
    )
    return ShortTermImpactScore(
        score=max(-100, min(100, round_half_up(total))),
        magnitude_contribution=round(magnitude_contribution, 4),
        persistence_contribution=round(persistence_contribution, 4),
        representativeness_contribution=round(
            representativeness_contribution, 4
        ),
        market_confirmation_contribution=round(
            market_confirmation_contribution, 4
        ),
    )


def rating_confidence_score(
    *,
    direction_clarity: float | None,
    source_reliability: float | None,
    magnitude_certainty: float | None,
    market_context_completeness: float | None,
) -> RatingConfidenceScore:
    """Return the independently displayed confidence for a five-level rating."""

    direction_contribution = 0.40 * _unit_interval(direction_clarity)
    source_contribution = 0.25 * _unit_interval(source_reliability)
    magnitude_contribution = 0.20 * _unit_interval(magnitude_certainty)
    context_contribution = 0.15 * _unit_interval(market_context_completeness)
    confidence = (
        direction_contribution
        + source_contribution
        + magnitude_contribution
        + context_contribution
    )
    return RatingConfidenceScore(
        confidence=round(confidence, 4),
        direction_clarity_contribution=round(direction_contribution, 4),
        source_reliability_contribution=round(source_contribution, 4),
        magnitude_certainty_contribution=round(magnitude_contribution, 4),
        market_context_completeness_contribution=round(context_contribution, 4),
    )


def _configured_term_strength(text: str, terms: Sequence[str]) -> tuple[int, int, int]:
    matches: list[int] = []
    folded = text.casefold()
    for configured in terms:
        term = configured.strip().casefold()
        if not term:
            continue
        escaped = re.escape(term)
        pattern = re.compile(
            rf"(?<!\w){escaped}(?!\w)" if term.isascii() else escaped,
            re.IGNORECASE,
        )
        for match in pattern.finditer(folded):
            prefix = folded[max(0, match.start() - 12) : match.start()]
            if _TERM_NEGATION_PATTERN.search(prefix):
                continue
            matches.append(len(term))
    return (max(matches, default=0), sum(matches), len(matches))


def _configured_text_hint(
    text: str,
    *,
    positive_terms: Sequence[str],
    neutral_terms: Sequence[str],
    negative_terms: Sequence[str],
    allow_beneficial: bool,
) -> int | None:
    positive = (
        _configured_term_strength(text, positive_terms)
        if allow_beneficial
        else (0, 0, 0)
    )
    negative = _configured_term_strength(text, negative_terms)
    if positive[0] or negative[0]:
        # Prefer the more specific (longer) phrase, then total matched text and
        # match count. Exact ties stay conservative and resolve as adverse.
        return 1 if positive > negative else -1
    if _configured_term_strength(text, neutral_terms)[0]:
        return 0
    return None


def directional_text_hint(
    *texts: str | None,
    fallback: int | None = None,
    allow_beneficial: bool = True,
    positive_terms: Sequence[str] = (),
    neutral_terms: Sequence[str] = (),
    negative_terms: Sequence[str] = (),
) -> int | None:
    """Resolve explicit directional language before using a model-provided fallback.

    The patterns intentionally cover high-precision statements such as securities
    fraud, shareholder investigations, and negative net income.  An explicit
    adverse fact wins over generic positive language like revenue growth.  A
    negated or explicitly uncertain benefit statement also blocks a positive
    model fallback instead of being misread as a benefit merely because it
    contains the word "benefit" or "受益".
    """

    combined = "\n".join(value.strip() for value in texts if value and value.strip())
    if any(pattern.search(combined) for pattern in _ADVERSE_DIRECTION_PATTERNS):
        return -1
    if any(
        pattern.search(combined)
        for pattern in _UNCERTAIN_OR_NEGATED_BENEFICIAL_PATTERNS
    ):
        return fallback if fallback == -1 else None
    configured_hint = _configured_text_hint(
        combined,
        positive_terms=positive_terms,
        neutral_terms=neutral_terms,
        negative_terms=negative_terms,
        allow_beneficial=allow_beneficial,
    )
    if configured_hint is not None:
        return configured_hint
    if any(pattern.search(combined) for pattern in _GENERIC_ADVERSE_DIRECTION_PATTERNS):
        return -1
    if allow_beneficial and any(
        pattern.search(combined) for pattern in _BENEFICIAL_DIRECTION_PATTERNS
    ):
        return 1
    return fallback if fallback in {-1, 1} else None


def normalize_probabilities(
    bull: float, base: float, bear: float
) -> tuple[float, float, float]:
    values = [max(0.0, bull), max(0.0, base), max(0.0, bear)]
    total = sum(values)
    if total <= 0:
        return (0.25, 0.5, 0.25)
    normalized = tuple(round(value / total, 8) for value in values)
    # Keep the Pydantic sum invariant exact enough after rounding.
    return (
        normalized[0],
        round(1.0 - normalized[0] - normalized[2], 8),
        normalized[2],
    )


def calibrate_probabilities(
    bull: float,
    base: float,
    bear: float,
    *,
    reliability: float,
) -> tuple[float, float, float]:
    """Contract model scenarios toward a neutral prior when support is weak.

    This is an explicit pre-empirical calibration policy.  Outcome-based Brier
    and ECE evaluation can later replace its versioned parameters without
    changing the scoring contract.
    """

    probabilities = normalize_probabilities(bull, base, bear)
    prior = (0.25, 0.5, 0.25)
    weight = max(0.0, min(1.0, reliability))
    calibrated = tuple(
        prior_value + weight * (value - prior_value)
        for value, prior_value in zip(probabilities, prior, strict=True)
    )
    return normalize_probabilities(*calibrated)


def probabilities_for_score(
    score: int,
    *,
    base_probability: float = 0.5,
) -> tuple[float, float, float]:
    """Return scenarios whose bull-bear edge exactly matches a published score.

    The deterministic score may combine scenario, event and measured-factor
    components.  Reconstructing the published scenarios prevents the UI from
    showing a bullish probability edge next to a bearish final score.
    """

    edge = max(-1.0, min(1.0, score / 100))
    base = max(0.0, min(float(base_probability), 1.0 - abs(edge)))
    directional_mass = 1.0 - base
    bull = (directional_mass + edge) / 2
    bear = (directional_mass - edge) / 2
    return normalize_probabilities(bull, base, bear)


def deterministic_direction_score(
    *,
    bull_probability: float,
    base_probability: float,
    bear_probability: float,
    event_direction: int | None = None,
    event_relevance: float = 0,
    factor_signal: float | None = None,
    factor_reliability: float = 0,
) -> DirectionScore:
    """Calculate direction from declared inputs using a fixed, auditable formula.

    The research model's self-reported score is deliberately absent.  Its
    scenario probabilities are one signal; explicit event mapping and measured
    market/fundamental factors are independent program inputs.  Missing inputs
    are omitted and the remaining weights are renormalized.
    """

    bull, _, bear = normalize_probabilities(
        bull_probability, base_probability, bear_probability
    )
    probability_score = round(100 * (bull - bear))
    components: list[tuple[float, float]] = [(float(probability_score), 0.45)]

    event_score: int | None = None
    if event_direction in {-1, 1} and event_relevance > 0:
        event_score = round(100 * event_direction * min(1.0, event_relevance))
        components.append((float(event_score), 0.25))

    measured_factor_score: int | None = None
    if factor_signal is not None and factor_reliability > 0:
        measured_factor_score = round(100 * max(-1.0, min(1.0, factor_signal)))
        components.append(
            (float(measured_factor_score), 0.30 * min(1.0, factor_reliability))
        )

    denominator = sum(weight for _, weight in components)
    raw = round(sum(value * weight for value, weight in components) / denominator)
    return DirectionScore(
        raw_score=max(-100, min(100, raw)),
        probability_score=probability_score,
        event_score=event_score,
        factor_score=measured_factor_score,
    )


def gated_score(raw_score: int, evidence_strength: float, mapping_confidence: float) -> int:
    strength = max(0.0, min(1.0, evidence_strength))
    mapping = max(0.0, min(1.0, mapping_confidence))
    return max(-100, min(100, round(raw_score * strength * mapping)))


def blocked_probabilities(*, technical_failure: bool = False) -> tuple[float, float, float]:
    """Return probabilities that do not visually imply a tradable direction."""

    return (0.2, 0.6, 0.2) if technical_failure else (0.25, 0.5, 0.25)
