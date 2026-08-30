from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from backend.app.domain import Rating, TargetType
from backend.app.services.industry_taxonomy import all_industries

SHORT_WINDOW_DAYS = 7
SHORT_HALF_LIFE_DAYS = 3
LONG_WINDOW_DAYS = 90
LONG_HALF_LIFE_DAYS = 30
LONG_TERM_CONFIDENCE_FLOOR = 0.45
ORDINARY_EVENT_STEP_LIMIT = 20.0
REGIME_BREAK_STEP_LIMIT = 45.0

_SECURITY_ASSET_CLASSES = frozenset({"equity", "crypto"})
_TAXONOMY_TARGET_TYPES = frozenset(
    {
        TargetType.SECTOR.value,
        TargetType.ECONOMY.value,
        TargetType.RISK_ASSET.value,
        TargetType.OTHER.value,
    }
)
_ENGLISH_WRAPPERS = frozenset(
    {"global", "market", "markets", "sector", "industry", "sentiment"}
)
_CHINESE_PREFIX_WRAPPERS = ("全球",)
_CHINESE_SUFFIX_WRAPPERS = ("市场", "行业", "板块", "领域", "情绪")


@dataclass(frozen=True, slots=True)
class CanonicalTarget:
    key: str
    label: str
    target_type: str
    matched_taxonomy: bool


@dataclass(frozen=True, slots=True)
class TargetObservation:
    """One canonical event observation, timestamped by news publication time."""

    occurred_at: datetime
    score: float
    rating_confidence: float
    news_confidence: float
    persistence: float
    realization_probability: float
    insufficient_evidence: bool = False
    provisional: bool = False

    def __post_init__(self) -> None:
        if not math.isfinite(float(self.score)) or not -100 <= self.score <= 100:
            raise ValueError("score must be a finite number between -100 and 100")
        for field_name in (
            "rating_confidence",
            "news_confidence",
            "persistence",
            "realization_probability",
        ):
            value = float(getattr(self, field_name))
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"{field_name} must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class TrendScore:
    score: float
    rating: Rating
    confidence: float
    provisional: bool
    event_count: int
    eligible_event_count: int
    ignored_event_count: int
    regime_break: bool


@dataclass(frozen=True, slots=True)
class TargetTrend:
    short_term: TrendScore
    long_term: TrendScore
    combined: TrendScore


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "other").strip().casefold()


def _normalized_words(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", " ", normalized).strip()


def _compact(value: str) -> str:
    return _normalized_words(value).replace(" ", "")


def _strip_target_wrappers(value: str) -> str:
    tokens = _normalized_words(value).split()
    while tokens and tokens[0] in _ENGLISH_WRAPPERS:
        tokens.pop(0)
    while tokens and tokens[-1] in _ENGLISH_WRAPPERS:
        tokens.pop()
    result = " ".join(tokens)
    changed = True
    while result and changed:
        changed = False
        for wrapper in _CHINESE_PREFIX_WRAPPERS:
            if result.startswith(wrapper):
                result = result[len(wrapper) :].strip()
                changed = True
        for wrapper in _CHINESE_SUFFIX_WRAPPERS:
            if result.endswith(wrapper):
                result = result[: -len(wrapper)].strip()
                changed = True
    return result


def _taxonomy_aliases() -> tuple[dict[str, tuple[str, str]], frozenset[str]]:
    taxonomy: dict[str, tuple[str, str]] = {}
    digital_aliases = {
        _compact(value)
        for value in (
            "Digital Assets",
            "Cryptocurrency",
            "数字资产",
            "加密货币",
            "Cryptocurrency Market",
            "Digital Assets Cryptocurrency",
            "数字资产 加密货币",
        )
    }
    for item in sorted(all_industries(), key=lambda value: value.level, reverse=True):
        normalized_terms = {
            _compact(term) for term in (item.name_zh, item.name_en, *item.aliases) if term
        }
        if item.industry_id in {"sector:digital_assets", "industry:cryptocurrency"}:
            digital_aliases.update(normalized_terms)
            continue
        for term in normalized_terms:
            taxonomy.setdefault(term, (item.industry_id, item.name_zh))
    return taxonomy, frozenset(digital_aliases)


_TAXONOMY_ALIASES, _DIGITAL_ASSET_ALIASES = _taxonomy_aliases()


def canonicalize_target(
    target_name: str,
    target_type: TargetType | str,
    *,
    asset_id: str | None = None,
    asset_class: Any | None = None,
) -> CanonicalTarget:
    """Resolve strict bilingual aliases without absorbing narrower sub-industries."""

    type_value = _enum_value(target_type)
    class_value = _enum_value(asset_class) if asset_class is not None else None
    stable_asset_id = (asset_id or "").strip()
    non_security_asset = (
        class_value not in _SECURITY_ASSET_CLASSES
        if class_value
        else type_value != TargetType.TRADABLE_ASSET.value
    )
    if stable_asset_id and non_security_asset:
        return CanonicalTarget(
            key=stable_asset_id,
            label=target_name.strip() or stable_asset_id,
            target_type=type_value,
            matched_taxonomy=False,
        )

    normalized_name = _compact(target_name) or "unknown"
    if type_value not in _TAXONOMY_TARGET_TYPES:
        return CanonicalTarget(
            key=f"{type_value}:{normalized_name}",
            label=target_name.strip() or normalized_name,
            target_type=type_value,
            matched_taxonomy=False,
        )

    unwrapped = _compact(_strip_target_wrappers(target_name))
    if unwrapped in _DIGITAL_ASSET_ALIASES:
        return CanonicalTarget(
            key="sector:digital_assets",
            label="数字资产",
            target_type=TargetType.SECTOR.value,
            matched_taxonomy=True,
        )
    if matched := _TAXONOMY_ALIASES.get(unwrapped):
        key, label = matched
        return CanonicalTarget(
            key=key,
            label=label,
            target_type=TargetType.SECTOR.value,
            matched_taxonomy=True,
        )
    return CanonicalTarget(
        key=f"{type_value}:{normalized_name}",
        label=target_name.strip() or normalized_name,
        target_type=type_value,
        matched_taxonomy=False,
    )


def rating_for_score(score: float) -> Rating:
    if score >= 70:
        return Rating.STRONGLY_BULLISH
    if score >= 30:
        return Rating.BULLISH
    if score <= -70:
        return Rating.STRONGLY_BEARISH
    if score <= -30:
        return Rating.BEARISH
    return Rating.WATCH


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _age_days(observation: TargetObservation, as_of: datetime) -> float:
    return (_as_utc(as_of) - _as_utc(observation.occurred_at)).total_seconds() / 86_400


def _decay(age_days: float, half_life_days: int) -> float:
    return 0.5 ** (age_days / half_life_days)


def _quality(observation: TargetObservation) -> float:
    return (
        observation.rating_confidence
        + observation.news_confidence
        + observation.persistence
        + observation.realization_probability
    ) / 4


def _is_regime_break(observation: TargetObservation) -> bool:
    return (
        not observation.insufficient_evidence
        and not observation.provisional
        and observation.rating_confidence >= 0.65
        and observation.news_confidence >= 0.75
        and observation.persistence >= 0.7
        and observation.realization_probability >= 0.7
        and abs(observation.score) >= 70
    )


def _in_window(
    observations: Sequence[TargetObservation], *, as_of: datetime, window_days: int
) -> list[tuple[TargetObservation, float]]:
    included: list[tuple[TargetObservation, float]] = []
    for observation in observations:
        age = _age_days(observation, as_of)
        if 0 <= age <= window_days:
            included.append((observation, age))
    return included


def _confidence(
    observations: Sequence[tuple[TargetObservation, float]], half_life_days: int
) -> float:
    if not observations:
        return 0.0
    weights = [_decay(age, half_life_days) for _, age in observations]
    total = sum(weights)
    value = sum(
        ((item.rating_confidence + item.news_confidence) / 2) * weight
        for (item, _), weight in zip(observations, weights, strict=True)
    ) / total
    return round(value, 4)


def _short_term_score(
    observations: Sequence[TargetObservation], *, as_of: datetime
) -> TrendScore:
    included = _in_window(observations, as_of=as_of, window_days=SHORT_WINDOW_DAYS)
    if not included:
        return TrendScore(0.0, Rating.WATCH, 0.0, False, 0, 0, 0, False)
    weights = [
        _decay(age, SHORT_HALF_LIFE_DAYS) * max(0.05, _quality(item))
        for item, age in included
    ]
    score = sum(
        item.score * weight
        for (item, _), weight in zip(included, weights, strict=True)
    ) / sum(weights)
    provisional = any(
        item.provisional
        or item.insufficient_evidence
        or item.rating_confidence < LONG_TERM_CONFIDENCE_FLOOR
        for item, _ in included
    )
    score = round(score, 2)
    return TrendScore(
        score,
        rating_for_score(score),
        _confidence(included, SHORT_HALF_LIFE_DAYS),
        provisional,
        len(included),
        len(included),
        0,
        any(_is_regime_break(item) for item, _ in included),
    )


def _long_term_score(
    observations: Sequence[TargetObservation], *, as_of: datetime
) -> TrendScore:
    included = _in_window(observations, as_of=as_of, window_days=LONG_WINDOW_DAYS)
    eligible = [
        (item, age)
        for item, age in included
        if not item.insufficient_evidence
        and not item.provisional
        and item.rating_confidence >= LONG_TERM_CONFIDENCE_FLOOR
    ]
    score = 0.0
    for item, age in sorted(eligible, key=lambda value: _as_utc(value[0].occurred_at)):
        event_limit = (
            REGIME_BREAK_STEP_LIMIT if _is_regime_break(item) else ORDINARY_EVENT_STEP_LIMIT
        )
        maximum_step = event_limit * _decay(age, LONG_HALF_LIFE_DAYS) * _quality(item)
        score += max(-maximum_step, min(maximum_step, item.score - score))
        score = max(-100.0, min(100.0, score))
    score = round(score, 2)
    ignored_count = len(included) - len(eligible)
    return TrendScore(
        score,
        rating_for_score(score),
        _confidence(eligible, LONG_HALF_LIFE_DAYS),
        not eligible,
        len(included),
        len(eligible),
        ignored_count,
        any(_is_regime_break(item) for item, _ in eligible),
    )


def aggregate_target_trend(
    observations: Sequence[TargetObservation], *, as_of: datetime
) -> TargetTrend:
    """Build a 7-day shock, 90-day regime, and optional 80/20 combined score."""

    short_term = _short_term_score(observations, as_of=as_of)
    long_term = _long_term_score(observations, as_of=as_of)
    if short_term.event_count:
        score = round(0.8 * long_term.score + 0.2 * short_term.score, 2)
        confidence = round(
            0.8 * long_term.confidence + 0.2 * short_term.confidence, 4
        )
    else:
        score = long_term.score
        confidence = long_term.confidence
    combined = TrendScore(
        score,
        rating_for_score(score),
        confidence,
        long_term.provisional or short_term.provisional,
        long_term.event_count,
        long_term.eligible_event_count,
        long_term.ignored_event_count,
        long_term.regime_break,
    )
    return TargetTrend(short_term, long_term, combined)
