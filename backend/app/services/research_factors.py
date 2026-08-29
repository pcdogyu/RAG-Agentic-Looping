from __future__ import annotations

import json
import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from enum import StrEnum
from typing import Any
from uuid import UUID

from dateutil.tz import gettz
from pydantic import BaseModel, ConfigDict, Field

from backend.app.domain import AssetRef, Evidence, SourceQuality, as_utc


class FactorCategory(StrEnum):
    MARKET_REACTION = "market_reaction"
    RELATIVE_PERFORMANCE = "relative_performance"
    EXPECTATION_GAP = "expectation_gap"
    EARNINGS_QUALITY = "earnings_quality"
    CAPITAL_ACTION = "capital_action"


class FactorSource(BaseModel):
    """Source metadata for deterministic factors derived from upstream records."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    url: str = Field(min_length=1, max_length=2000)
    quality: SourceQuality = SourceQuality.AGGREGATOR
    independent_group: str = Field(min_length=1, max_length=200)
    published_at: datetime | None = None


class ResearchFactor(BaseModel):
    """One auditable, deterministic input to a later scoring policy."""

    model_config = ConfigDict(extra="forbid")

    key: str
    category: FactorCategory
    label: str
    value: float
    unit: str
    direction: int = Field(ge=-1, le=1)
    confidence: float = Field(ge=0, le=1)
    normalized_signal: float = Field(ge=-1, le=1)
    reliability: float = Field(ge=0, le=1)
    description: str
    source_keys: list[str]
    window_start: datetime | None = None
    window_end: datetime | None = None
    inputs: dict[str, Any] = Field(default_factory=dict)


class ResearchFactorResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_families: list[str] = Field(default_factory=list)
    factors: list[ResearchFactor] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    aggregate_signal: float = Field(default=0, ge=-1, le=1)
    reliability: float = Field(default=0, ge=0, le=1)
    category_signals: dict[str, float] = Field(default_factory=dict)
    category_reliability: dict[str, float] = Field(default_factory=dict)
    missing_requirements: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class _PricePoint:
    timestamp: datetime
    close: float
    volume: float | None = None


_DATE_KEYS = ("date", "datetime", "timestamp", "time", "日期", "交易日期", "数据日期")
_CLOSE_KEYS = (
    "adjClose",
    "adjustedClose",
    "close",
    "price",
    "收盘",
    "收盘价",
    "当日收盘价",
)
_VOLUME_KEYS = ("volume", "成交量", "VOL")
_FINANCIAL_DATE_KEYS = (
    "date",
    "fillingDate",
    "reportedDate",
    "REPORT_DATE",
    "REPORT_DATE_NAME",
    "报告日期",
)
_SOURCE_QUALITY_RANK = {
    SourceQuality.SOCIAL: 0,
    SourceQuality.AGGREGATOR: 1,
    SourceQuality.PROFESSIONAL: 2,
    SourceQuality.PRIMARY: 3,
    SourceQuality.OFFICIAL: 4,
}
_SOURCE_RELIABILITY = {
    SourceQuality.SOCIAL: 0.30,
    SourceQuality.AGGREGATOR: 0.60,
    SourceQuality.PROFESSIONAL: 0.78,
    SourceQuality.PRIMARY: 0.90,
    SourceQuality.OFFICIAL: 0.98,
}


def _normal(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).strip().lower()


def _normal_key(value: Any) -> str:
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]", "", _normal(value))


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        number = float(value)
        return number if math.isfinite(number) else None
    text = _normal(value).replace(",", "").replace("，", "")
    text = text.replace("$", "").replace("￥", "").replace("¥", "")
    match = re.fullmatch(r"\s*([+-]?\d+(?:\.\d+)?)\s*(万|亿|千|k|m|b)?\s*(?:元|股|%)?\s*", text)
    if not match:
        return None
    number = float(match.group(1))
    multiplier = {
        "千": 1_000,
        "万": 10_000,
        "亿": 100_000_000,
        "k": 1_000,
        "m": 1_000_000,
        "b": 1_000_000_000,
    }.get(match.group(2) or "", 1)
    output = number * multiplier
    return output if math.isfinite(output) else None


def _datetime(value: Any, fallback: datetime | None = None) -> datetime | None:
    if isinstance(value, datetime):
        return as_utc(value)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, int | float) and not isinstance(value, bool):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000
        try:
            return datetime.fromtimestamp(raw, tz=UTC)
        except (OSError, OverflowError, ValueError):
            return fallback
    text = _normal(value)
    if not text:
        return fallback
    try:
        return as_utc(datetime.fromisoformat(text.replace("z", "+00:00")))
    except ValueError:
        match = re.search(r"(20\d{2})[-年/.](\d{1,2})[-月/.](\d{1,2})", text)
        if not match:
            return fallback
        try:
            return datetime(*(int(part) for part in match.groups()), tzinfo=UTC)
        except ValueError:
            return fallback


def _mapping_value(payload: Mapping[str, Any], keys: Sequence[str]) -> Any:
    wanted = {_normal_key(key) for key in keys}
    for key, value in payload.items():
        if _normal_key(key) in wanted:
            return value
    return None


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = _normal_key(value)
    if normalized in {
        "1",
        "true",
        "yes",
        "y",
        "是",
        "盘后",
        "afterclose",
        "aftermarketclose",
        "postmarket",
    }:
        return True
    if normalized in {
        "0",
        "false",
        "no",
        "n",
        "否",
        "盘前",
        "盘中",
        "premarket",
        "duringsession",
        "regularsession",
        "intraday",
    }:
        return False
    return None


def _asset_market_session(asset: AssetRef) -> tuple[str, time | None]:
    exchange = asset.exchange_or_provider.casefold()
    primary = (asset.primary_listing_asset_id or "").casefold()
    if exchange == "asx" or asset.symbol.casefold().endswith(".ax") or ":asx:" in primary:
        return "Australia/Sydney", time(16)
    if asset.market.value == "CN":
        return "Asia/Shanghai", time(15)
    if asset.market.value == "HK":
        return "Asia/Hong_Kong", time(16)
    if asset.market.value == "US":
        return "America/New_York", time(16)
    if asset.market.value == "CRYPTO":
        return "UTC", time(23, 59, 59)
    return "UTC", None


def _timezone(name: str) -> tzinfo:
    # python-dateutil ships an IANA database on Windows, where stdlib zoneinfo
    # may otherwise have no system timezone database to consult.
    return gettz(name) or UTC


def _numeric_value(payload: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    value = _number(_mapping_value(payload, keys))
    if value is not None:
        return value
    for child in payload.values():
        if isinstance(child, Mapping):
            value = _numeric_value(child, keys)
            if value is not None:
                return value
    return None


def _sequence_value(payload: Mapping[str, Any], keys: Sequence[str]) -> list[Mapping[str, Any]]:
    candidate = _mapping_value(payload, keys)
    if isinstance(candidate, Mapping):
        candidate = candidate.get("data") or candidate.get("results") or []
    if not isinstance(candidate, Sequence) or isinstance(candidate, str | bytes):
        return []
    return [item for item in candidate if isinstance(item, Mapping)]


def _price_points(
    records: Sequence[Mapping[str, Any]] | None, as_of: datetime
) -> list[_PricePoint]:
    by_day: dict[date, _PricePoint] = {}
    for record in records or ():
        if not isinstance(record, Mapping):
            continue
        timestamp = _datetime(_mapping_value(record, _DATE_KEYS))
        close = _number(_mapping_value(record, _CLOSE_KEYS))
        if timestamp is None or close is None or close <= 0 or timestamp > as_of:
            continue
        volume = _number(_mapping_value(record, _VOLUME_KEYS))
        point = _PricePoint(timestamp=timestamp, close=close, volume=volume)
        current = by_day.get(timestamp.date())
        if current is None or current.timestamp <= timestamp:
            by_day[timestamp.date()] = point
    return sorted(by_day.values(), key=lambda item: item.timestamp)


def _direction(value: float, deadband: float = 0.25) -> int:
    if value > deadband:
        return 1
    if value < -deadband:
        return -1
    return 0


def _percent_change(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start == 0:
        return None
    value = (end / start - 1) * 100
    return value if math.isfinite(value) else None


def _return_between(points: Sequence[_PricePoint], start: date, end: date) -> float | None:
    before = [item for item in points if item.timestamp.date() <= start]
    through_end = [item for item in points if item.timestamp.date() <= end]
    if not before or not through_end:
        return None
    start_point = before[-1]
    end_point = through_end[-1]
    if end_point.timestamp <= start_point.timestamp:
        return None
    return _percent_change(start_point.close, end_point.close)


def _append_factor(
    factors: list[ResearchFactor],
    *,
    key: str,
    category: FactorCategory,
    label: str,
    value: float | None,
    unit: str,
    confidence: float,
    description: str,
    source_keys: list[str],
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    inputs: Mapping[str, Any] | None = None,
    direction: int | None = None,
) -> None:
    if value is None or not math.isfinite(value):
        return
    factor = ResearchFactor(
        key=key,
        category=category,
        label=label,
        value=round(value, 6),
        unit=unit,
        direction=_direction(value) if direction is None else direction,
        confidence=confidence,
        normalized_signal=0,
        reliability=confidence,
        description=description,
        source_keys=source_keys,
        window_start=window_start,
        window_end=window_end,
        inputs=dict(inputs or {}),
    )
    factor.normalized_signal = _normalized_factor_signal(factor)
    factors.append(factor)


def _normalized_factor_signal(factor: ResearchFactor) -> float:
    """Map heterogeneous factor units onto a bounded, comparable signal scale."""

    value = factor.value
    key = factor.key
    if factor.unit == "boolean":
        magnitude = 0.45 if key == "forced_share_reduction" else 0.25
        signal = factor.direction * magnitude
    elif "average_volume_days" in key:
        signal = factor.direction * min(abs(value) / 10, 1)
    elif key in {
        "share_reduction_total_shares_pct",
        "share_reduction_free_float_pct",
        "buyback_total_shares_pct",
        "buyback_market_cap_pct",
    }:
        signal = factor.direction * min(abs(value) / 3, 1)
    elif key == "net_buyback_minus_reduction_pct":
        signal = math.tanh(value / 2)
    elif "price_cap_premium" in key:
        signal = factor.direction * min(abs(math.tanh(value / 20)), 1)
    elif factor.category is FactorCategory.MARKET_REACTION:
        scale = 5 if "_1d_" in key else 10 if "_5d_" in key else 15
        signal = math.tanh(value / scale)
    elif factor.category is FactorCategory.RELATIVE_PERFORMANCE:
        scale = 4 if "_1d_" in key else 8 if "_5d_" in key else 12
        signal = math.tanh(value / scale)
    elif factor.category is FactorCategory.EXPECTATION_GAP:
        signal = math.tanh(value / 20)
    elif factor.category is FactorCategory.EARNINGS_QUALITY:
        scale = 5 if "margin" in key else 30
        signal = math.tanh(value / scale)
    else:
        signal = factor.direction * min(abs(value), 1)
    return round(max(-1.0, min(1.0, signal)), 6)


_CATEGORY_WEIGHTS = {
    FactorCategory.MARKET_REACTION: 0.20,
    FactorCategory.RELATIVE_PERFORMANCE: 0.25,
    FactorCategory.EXPECTATION_GAP: 0.25,
    FactorCategory.EARNINGS_QUALITY: 0.15,
    FactorCategory.CAPITAL_ACTION: 0.25,
}


def _preferred_horizon_factors(factors: Sequence[ResearchFactor]) -> list[ResearchFactor]:
    """Avoid counting the same price move at 1, 5 and 20 days three times."""

    if not factors or factors[0].category not in {
        FactorCategory.MARKET_REACTION,
        FactorCategory.RELATIVE_PERFORMANCE,
    }:
        return list(factors)
    for suffix in ("_5d_pct", "_1d_pct", "_20d_pct"):
        selected = [item for item in factors if item.key.endswith(suffix)]
        if selected:
            return selected
    return list(factors)


def _aggregate_factors(
    factors: Sequence[ResearchFactor],
    *,
    event_at: datetime | None,
    families: Sequence[str],
) -> tuple[float, float, dict[str, float], dict[str, float]]:
    grouped: dict[FactorCategory, list[ResearchFactor]] = {}
    for factor in factors:
        grouped.setdefault(factor.category, []).append(factor)
    category_signals: dict[str, float] = {}
    category_reliability: dict[str, float] = {}
    for category, raw_group in grouped.items():
        group = _preferred_horizon_factors(raw_group)
        denominator = sum(item.reliability for item in group)
        if denominator <= 0:
            continue
        signal = sum(item.normalized_signal * item.reliability for item in group) / denominator
        reliability = sum(item.reliability for item in group) / len(group)
        category_signals[category.value] = round(max(-1.0, min(1.0, signal)), 6)
        category_reliability[category.value] = round(max(0.0, min(1.0, reliability)), 6)

    expected_categories: set[FactorCategory] = set()
    if event_at is not None:
        expected_categories.update(
            {FactorCategory.MARKET_REACTION, FactorCategory.RELATIVE_PERFORMANCE}
        )
    if "earnings" in families:
        expected_categories.update(
            {FactorCategory.EXPECTATION_GAP, FactorCategory.EARNINGS_QUALITY}
        )
    if {"share_reduction", "buyback"}.intersection(families):
        expected_categories.add(FactorCategory.CAPITAL_ACTION)
    if not expected_categories:
        expected_categories = set(grouped)
    expected_weight = sum(_CATEGORY_WEIGHTS[item] for item in expected_categories)
    available = [item for item in expected_categories if item.value in category_signals]
    weighted_reliability = sum(
        _CATEGORY_WEIGHTS[item] * category_reliability[item.value] for item in available
    )
    signal_denominator = weighted_reliability
    if signal_denominator <= 0 or expected_weight <= 0:
        return 0, 0, category_signals, category_reliability
    signal = (
        sum(
            _CATEGORY_WEIGHTS[item]
            * category_reliability[item.value]
            * category_signals[item.value]
            for item in available
        )
        / signal_denominator
    )
    # Reliability includes category coverage, so missing consensus/benchmark data cannot
    # masquerade as a high-quality neutral aggregate.
    reliability = weighted_reliability / expected_weight
    return (
        round(max(-1.0, min(1.0, signal)), 6),
        round(max(0.0, min(1.0, reliability)), 6),
        category_signals,
        category_reliability,
    )


def _market_factors(
    *,
    factors: list[ResearchFactor],
    missing: list[str],
    as_of: datetime,
    event_at: datetime | None,
    horizon_days: int | None,
    event_details: Mapping[str, Any],
    asset_prices: Sequence[Mapping[str, Any]] | None,
    benchmark_prices: Sequence[Mapping[str, Any]] | None,
    industry_prices: Sequence[Mapping[str, Any]] | None,
    market_timezone: str,
    market_close_time: time | None,
) -> tuple[list[_PricePoint], _PricePoint | None]:
    points = _price_points(asset_prices, as_of)
    if event_at is None:
        missing.append("market_reaction:event_timestamp_missing")
        return points, None
    if event_at > as_of:
        missing.append("market_reaction:event_is_after_as_of")
        return points, None
    if not points:
        missing.append("market_reaction:asset_prices_missing")
        return points, None

    local_timezone = _timezone(market_timezone)
    event_local = event_at.astimezone(local_timezone)
    as_of_local = as_of.astimezone(local_timezone)
    event_day = event_local.date()
    before = [item for item in points if item.timestamp.date() < event_day]
    after_close = _optional_bool(
        _mapping_value(event_details, ("after_market_close", "after_close", "盘后公告"))
    )
    session = _normal(
        _mapping_value(event_details, ("market_session", "event_session", "交易时段"))
    )
    if after_close is None and session:
        after_close = _optional_bool(session)

    explicitly_before_close = after_close is False
    inferred_after_close = bool(
        market_close_time is not None and event_local.time() >= market_close_time
    )
    close_observable = as_of_local.date() > event_day or (
        market_close_time is not None
        and as_of_local.date() == event_day
        and as_of_local.time() >= market_close_time
    )
    same_day_close_used = (
        explicitly_before_close and not inferred_after_close and close_observable
    )
    if after_close is None and inferred_after_close:
        missing.append("market_reaction:event_timestamp_is_after_market_close")
    elif after_close is None:
        missing.append("market_reaction:event_session_unknown_same_day_excluded")
    elif explicitly_before_close and inferred_after_close:
        missing.append("market_reaction:event_timestamp_is_after_market_close")
    elif explicitly_before_close and not close_observable:
        missing.append("market_reaction:same_day_close_not_observable")
    post = [
        item
        for item in points
        if item.timestamp.date() > event_day
        or (same_day_close_used and item.timestamp.date() == event_day)
    ]
    if not before or not post:
        missing.append("market_reaction:event_window_incomplete")
        return points, before[-1] if before else None

    baseline = before[-1]
    benchmark = _price_points(benchmark_prices, as_of)
    industry = _price_points(industry_prices, as_of)
    if not benchmark:
        missing.append("relative_performance:benchmark_prices_missing")
    if not industry:
        missing.append("relative_performance:industry_prices_missing")

    for sessions in (1, 5, 20):
        if len(post) < sessions:
            missing.append(f"market_reaction:{sessions}d_window_incomplete")
            continue
        endpoint = post[sessions - 1]
        if horizon_days is not None and endpoint.timestamp.date() > event_day + timedelta(
            days=horizon_days
        ):
            missing.append(f"market_reaction:{sessions}d_exceeds_event_horizon")
            continue
        asset_return = _percent_change(baseline.close, endpoint.close)
        session_inputs = {
            "market_timezone": market_timezone,
            "event_session_date": event_day.isoformat(),
            "same_day_close_used": same_day_close_used,
        }
        _append_factor(
            factors,
            key=f"asset_return_{sessions}d_pct",
            category=FactorCategory.MARKET_REACTION,
            label=f"事件后{sessions}个交易日收益",
            value=asset_return,
            unit="percent",
            confidence=0.9,
            description=f"事件前收盘至事件后第{sessions}个交易日的标的收益率",
            source_keys=["market"],
            window_start=baseline.timestamp,
            window_end=endpoint.timestamp,
            inputs={
                **session_inputs,
                "start_close": baseline.close,
                "end_close": endpoint.close,
            },
        )
        benchmark_return = _return_between(
            benchmark, baseline.timestamp.date(), endpoint.timestamp.date()
        )
        if asset_return is not None and benchmark_return is not None:
            excess = asset_return - benchmark_return
            _append_factor(
                factors,
                key=f"excess_vs_benchmark_{sessions}d_pct",
                category=FactorCategory.RELATIVE_PERFORMANCE,
                label=f"事件后{sessions}日相对基准收益",
                value=excess,
                unit="percentage_points",
                confidence=0.9,
                description="标的事件窗口收益减去同窗口市场基准收益",
                source_keys=["market", "benchmark"],
                window_start=baseline.timestamp,
                window_end=endpoint.timestamp,
                inputs={
                    **session_inputs,
                    "asset_return_pct": round(asset_return, 6),
                    "benchmark_return_pct": round(benchmark_return, 6),
                },
            )
        industry_return = _return_between(
            industry, baseline.timestamp.date(), endpoint.timestamp.date()
        )
        if asset_return is not None and industry_return is not None:
            excess = asset_return - industry_return
            _append_factor(
                factors,
                key=f"excess_vs_industry_{sessions}d_pct",
                category=FactorCategory.RELATIVE_PERFORMANCE,
                label=f"事件后{sessions}日相对行业收益",
                value=excess,
                unit="percentage_points",
                confidence=0.85,
                description="标的事件窗口收益减去同窗口行业组合收益",
                source_keys=["market", "industry"],
                window_start=baseline.timestamp,
                window_end=endpoint.timestamp,
                inputs={
                    **session_inputs,
                    "asset_return_pct": round(asset_return, 6),
                    "industry_return_pct": round(industry_return, 6),
                },
            )

    if horizon_days is not None and post:
        target_day = event_day + timedelta(days=horizon_days)
        horizon_points = [item for item in post if item.timestamp.date() <= target_day]
        if not horizon_points:
            missing.append(f"market_reaction:{horizon_days}cd_window_incomplete")
            return points, baseline
        endpoint = horizon_points[-1]
        asset_return = _percent_change(baseline.close, endpoint.close)
        horizon_inputs = {
            "market_timezone": market_timezone,
            "event_session_date": event_day.isoformat(),
            "horizon_days": horizon_days,
            "horizon_target_date": target_day.isoformat(),
            "window_complete": as_of_local.date() >= target_day,
            "start_close": baseline.close,
            "end_close": endpoint.close,
        }
        _append_factor(
            factors,
            key=f"asset_return_horizon_{horizon_days}cd_pct",
            category=FactorCategory.MARKET_REACTION,
            label=f"事件后{horizon_days}个自然日周期内收益",
            value=asset_return,
            unit="percent",
            confidence=0.9,
            description="事件前收盘至对应评级周期内当前可观察终点的标的收益率",
            source_keys=["market"],
            window_start=baseline.timestamp,
            window_end=endpoint.timestamp,
            inputs=horizon_inputs,
        )
        benchmark_return = _return_between(
            benchmark, baseline.timestamp.date(), endpoint.timestamp.date()
        )
        if asset_return is not None and benchmark_return is not None:
            _append_factor(
                factors,
                key=f"excess_vs_benchmark_horizon_{horizon_days}cd_pct",
                category=FactorCategory.RELATIVE_PERFORMANCE,
                label=f"{horizon_days}自然日周期内相对基准收益",
                value=asset_return - benchmark_return,
                unit="percentage_points",
                confidence=0.9,
                description="标的在对应评级周期内的收益减去同窗口市场基准收益",
                source_keys=["market", "benchmark"],
                window_start=baseline.timestamp,
                window_end=endpoint.timestamp,
                inputs={
                    **horizon_inputs,
                    "asset_return_pct": round(asset_return, 6),
                    "benchmark_return_pct": round(benchmark_return, 6),
                },
            )
        industry_return = _return_between(
            industry, baseline.timestamp.date(), endpoint.timestamp.date()
        )
        if asset_return is not None and industry_return is not None:
            _append_factor(
                factors,
                key=f"excess_vs_industry_horizon_{horizon_days}cd_pct",
                category=FactorCategory.RELATIVE_PERFORMANCE,
                label=f"{horizon_days}自然日周期内相对行业收益",
                value=asset_return - industry_return,
                unit="percentage_points",
                confidence=0.85,
                description="标的在对应评级周期内的收益减去同窗口行业组合收益",
                source_keys=["market", "industry"],
                window_start=baseline.timestamp,
                window_end=endpoint.timestamp,
                inputs={
                    **horizon_inputs,
                    "asset_return_pct": round(asset_return, 6),
                    "industry_return_pct": round(industry_return, 6),
                },
            )
    return points, baseline


def _event_families(event_type: str | None, event_texts: Sequence[str]) -> list[str]:
    text = " ".join(_normal(item) for item in event_texts)
    output: list[str] = []
    normalized_type = _normal(event_type)
    if normalized_type == "earnings" or re.search(
        r"财报|业绩|季报|中报|半年报|年报|盈利|营收|利润|earnings|results|revenue|profit",
        text,
    ):
        output.append("earnings")
    if re.search(r"减持|被动减持|出售股份|sell[- ]?down|share sale|dispose .*shares", text):
        output.append("share_reduction")
    if re.search(r"回购|repurchase|buyback", text):
        output.append("buyback")
    return output


def _action_percent(text: str, action: str) -> float | None:
    pattern = {
        "share_reduction": r"(?:减持|被动减持|出售股份|sell[- ]?down|share sale)",
        "buyback": r"(?:回购|repurchase|buyback)",
    }[action]
    match = re.search(
        pattern + r"[^。；;]{0,100}?(?:不超过|不超|最多|约|合计|up to)?\s*(\d+(?:\.\d+)?)\s*%",
        text,
        flags=re.IGNORECASE,
    )
    return _number(match.group(1)) if match else None


def _action_quantity(text: str, action: str, noun: str) -> float | None:
    action_pattern = {
        "share_reduction": r"(?:减持|被动减持|出售股份|sell[- ]?down|share sale)",
        "buyback": r"(?:回购|repurchase|buyback)",
    }[action]
    noun_pattern = r"股" if noun == "shares" else r"元"
    match = re.search(
        action_pattern + r"[^。；;]{0,100}?(\d+(?:[,.]\d+)*)\s*(亿|万|千|k|m|b)?\s*" + noun_pattern,
        text,
        flags=re.IGNORECASE,
    )
    return _number("".join(part or "" for part in match.groups())) if match else None


def _capital_action_factors(
    *,
    factors: list[ResearchFactor],
    missing: list[str],
    families: Sequence[str],
    event_texts: Sequence[str],
    event_details: Mapping[str, Any],
    fundamentals: Mapping[str, Any],
    price_points: Sequence[_PricePoint],
    baseline: _PricePoint | None,
    event_at: datetime | None,
) -> None:
    text = " ".join(_normal(item) for item in event_texts)
    total_shares = _numeric_value(
        event_details, ("total_shares", "shares_outstanding", "总股本")
    ) or _numeric_value(fundamentals, ("total_shares", "sharesOutstanding", "总股本"))
    float_shares = _numeric_value(
        event_details, ("free_float_shares", "float_shares", "流通股")
    ) or _numeric_value(fundamentals, ("free_float_shares", "floatShares", "流通股"))
    market_cap = _numeric_value(event_details, ("market_cap", "总市值")) or _numeric_value(
        fundamentals, ("marketCap", "mktCap", "总市值")
    )

    if "share_reduction" in families:
        _append_factor(
            factors,
            key="share_reduction_announced",
            category=FactorCategory.CAPITAL_ACTION,
            label="股东减持计划",
            value=1,
            unit="boolean",
            direction=-1,
            confidence=0.75,
            description="公告或事件文本明确包含股东减持",
            source_keys=["event"],
        )
        reduction_pct = _numeric_value(
            event_details,
            ("reduction_pct", "sell_down_pct", "share_reduction_pct", "减持比例"),
        ) or _action_percent(text, "share_reduction")
        reduction_shares = _numeric_value(
            event_details,
            ("reduction_shares", "sell_down_shares", "share_reduction_shares", "减持股数"),
        ) or _action_quantity(text, "share_reduction", "shares")
        if reduction_pct is None and reduction_shares and total_shares:
            reduction_pct = reduction_shares / total_shares * 100
        _append_factor(
            factors,
            key="share_reduction_total_shares_pct",
            category=FactorCategory.CAPITAL_ACTION,
            label="减持占总股本",
            value=reduction_pct,
            unit="percent",
            direction=-1 if reduction_pct and reduction_pct > 0 else 0,
            confidence=0.9,
            description="计划或已执行减持股数占总股本比例",
            source_keys=["event"],
            inputs={"shares": reduction_shares, "total_shares": total_shares},
        )
        if reduction_shares and float_shares:
            free_float_pct = reduction_shares / float_shares * 100
            _append_factor(
                factors,
                key="share_reduction_free_float_pct",
                category=FactorCategory.CAPITAL_ACTION,
                label="减持占自由流通盘",
                value=free_float_pct,
                unit="percent",
                direction=-1,
                confidence=0.9,
                description="计划或已执行减持股数占自由流通股比例",
                source_keys=["event", "fundamentals"],
                inputs={"shares": reduction_shares, "free_float_shares": float_shares},
            )
        average_volume = _numeric_value(
            event_details, ("average_daily_volume", "avg_volume_20d", "日均成交量")
        )
        if average_volume is None and event_at:
            prior_volumes = [
                point.volume
                for point in price_points
                if point.timestamp < event_at and point.volume and point.volume > 0
            ][-20:]
            if prior_volumes:
                average_volume = sum(prior_volumes) / len(prior_volumes)
        if reduction_shares and average_volume:
            turnover_days = reduction_shares / average_volume
            _append_factor(
                factors,
                key="share_reduction_average_volume_days",
                category=FactorCategory.CAPITAL_ACTION,
                label="减持相当于日均成交量天数",
                value=turnover_days,
                unit="trading_days",
                direction=-1,
                confidence=0.85,
                description="减持股数除以事件前最多20个交易日平均成交量",
                source_keys=["event", "market"],
                inputs={"shares": reduction_shares, "average_daily_volume": average_volume},
            )
        if re.search(r"司法|强制执行|被动减持|forced sale|court[- ]ordered", text):
            _append_factor(
                factors,
                key="forced_share_reduction",
                category=FactorCategory.CAPITAL_ACTION,
                label="司法或被动减持",
                value=1,
                unit="boolean",
                direction=-1,
                confidence=0.95,
                description="事件文本表明减持源于司法执行或被动处置",
                source_keys=["event"],
            )
        if reduction_pct is None:
            missing.append("share_reduction:total_shares_percent_missing")

    if "buyback" in families:
        _append_factor(
            factors,
            key="buyback_announced",
            category=FactorCategory.CAPITAL_ACTION,
            label="股份回购计划",
            value=1,
            unit="boolean",
            direction=1,
            confidence=0.75,
            description="公告或事件文本明确包含股份回购",
            source_keys=["event"],
        )
        buyback_pct = _numeric_value(
            event_details, ("buyback_pct", "repurchase_pct", "回购比例")
        ) or _action_percent(text, "buyback")
        buyback_shares = _numeric_value(
            event_details, ("buyback_shares", "repurchase_shares", "回购股数")
        ) or _action_quantity(text, "buyback", "shares")
        if buyback_pct is None and buyback_shares and total_shares:
            buyback_pct = buyback_shares / total_shares * 100
        _append_factor(
            factors,
            key="buyback_total_shares_pct",
            category=FactorCategory.CAPITAL_ACTION,
            label="回购占总股本",
            value=buyback_pct,
            unit="percent",
            direction=1 if buyback_pct and buyback_pct > 0 else 0,
            confidence=0.9,
            description="计划或已执行回购股数占总股本比例",
            source_keys=["event"],
            inputs={"shares": buyback_shares, "total_shares": total_shares},
        )
        buyback_amount = _numeric_value(
            event_details,
            ("buyback_amount", "repurchase_amount", "buyback_amount_max", "回购金额"),
        ) or _action_quantity(text, "buyback", "amount")
        if buyback_amount and market_cap:
            amount_pct = buyback_amount / market_cap * 100
            _append_factor(
                factors,
                key="buyback_market_cap_pct",
                category=FactorCategory.CAPITAL_ACTION,
                label="回购金额占总市值",
                value=amount_pct,
                unit="percent",
                direction=1,
                confidence=0.85,
                description="回购计划金额除以事件时点附近总市值",
                source_keys=["event", "fundamentals"],
                inputs={"buyback_amount": buyback_amount, "market_cap": market_cap},
            )
        buyback_price = _numeric_value(
            event_details,
            ("buyback_price_cap", "repurchase_price_cap", "回购价格上限", "回购价"),
        )
        if buyback_price and baseline:
            premium = _percent_change(baseline.close, buyback_price)
            _append_factor(
                factors,
                key="buyback_price_cap_premium_pct",
                category=FactorCategory.CAPITAL_ACTION,
                label="回购价格上限溢价",
                value=premium,
                unit="percent",
                confidence=0.8,
                description="回购价格上限相对事件前收盘价的溢价",
                source_keys=["event", "market"],
                inputs={"buyback_price_cap": buyback_price, "pre_event_close": baseline.close},
            )
        if buyback_pct is None and buyback_amount is None:
            missing.append("buyback:size_missing")

    if "share_reduction" in families and "buyback" in families:
        reduction = next(
            (item.value for item in factors if item.key == "share_reduction_total_shares_pct"),
            None,
        )
        buyback = next(
            (item.value for item in factors if item.key == "buyback_total_shares_pct"), None
        )
        if reduction is not None and buyback is not None:
            net_pct = buyback - reduction
            _append_factor(
                factors,
                key="net_buyback_minus_reduction_pct",
                category=FactorCategory.CAPITAL_ACTION,
                label="回购减去减持的净股本比例",
                value=net_pct,
                unit="percentage_points",
                confidence=0.8,
                description="可比口径下，回购占比减去减持占比；正值表示回购规模更大",
                source_keys=["event"],
            )


def _financial_records(fundamentals: Mapping[str, Any], as_of: datetime) -> list[Mapping[str, Any]]:
    records = _sequence_value(
        fundamentals,
        ("income", "earnings", "financial_indicators", "income_statement", "财务指标"),
    )
    dated: list[tuple[datetime | None, Mapping[str, Any]]] = []
    for item in records:
        item_date = _datetime(_mapping_value(item, _FINANCIAL_DATE_KEYS))
        if item_date is None or item_date > as_of:
            continue
        dated.append((item_date, item))
    dated.sort(key=lambda pair: pair[0] or datetime.min.replace(tzinfo=UTC), reverse=True)
    return [item for _, item in dated]


def _comparable_previous(
    current: Mapping[str, Any], records: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any] | None:
    current_date = _datetime(_mapping_value(current, _FINANCIAL_DATE_KEYS))
    if current_date:
        for candidate in records[1:]:
            candidate_date = _datetime(_mapping_value(candidate, _FINANCIAL_DATE_KEYS))
            if not candidate_date:
                continue
            if (
                current_date.year - candidate_date.year == 1
                and current_date.month == candidate_date.month
            ):
                return candidate
        if len(records) > 1:
            candidate_date = _datetime(_mapping_value(records[1], _FINANCIAL_DATE_KEYS))
            if candidate_date and (current_date - candidate_date).days >= 300:
                return records[1]
    return None


def _expectation_factor(
    factors: list[ResearchFactor],
    *,
    key: str,
    label: str,
    actual: float | None,
    estimate: float | None,
) -> bool:
    if actual is None or estimate is None or estimate == 0:
        return False
    surprise = (actual - estimate) / abs(estimate) * 100
    _append_factor(
        factors,
        key=key,
        category=FactorCategory.EXPECTATION_GAP,
        label=label,
        value=surprise,
        unit="percent",
        confidence=0.95,
        description="实际披露值相对披露前一致预期的偏离",
        source_keys=["fundamentals", "expectations"],
        inputs={"actual": actual, "consensus": estimate},
    )
    return True


def _earnings_factors(
    *,
    factors: list[ResearchFactor],
    missing: list[str],
    families: Sequence[str],
    fundamentals: Mapping[str, Any],
    expectations: Mapping[str, Any],
    as_of: datetime,
) -> None:
    if "earnings" not in families:
        return
    records = _financial_records(fundamentals, as_of)
    if not records:
        missing.append("earnings:actual_financials_missing")
        return
    current = records[0]
    revenue = _numeric_value(current, ("revenue", "actualRevenue", "TOTALOPERATEREVE", "营业收入"))
    net_income = _numeric_value(
        current, ("netIncome", "actualNetIncome", "PARENTNETPROFIT", "归母净利润")
    )
    eps = _numeric_value(current, ("eps", "epsActual", "actualEps", "epsDiluted", "基本每股收益"))
    revenue_estimate = _numeric_value(
        expectations,
        ("revenue_estimate", "estimatedRevenue", "revenueEstimated", "consensusRevenue"),
    )
    net_income_estimate = _numeric_value(
        expectations,
        ("net_income_estimate", "estimatedNetIncome", "consensusNetIncome"),
    )
    eps_estimate = _numeric_value(
        expectations, ("eps_estimate", "estimatedEps", "epsEstimated", "consensusEps")
    )
    surprise_count = sum(
        (
            _expectation_factor(
                factors,
                key="revenue_surprise_pct",
                label="营业收入预期差",
                actual=revenue,
                estimate=revenue_estimate,
            ),
            _expectation_factor(
                factors,
                key="net_income_surprise_pct",
                label="归母净利润预期差",
                actual=net_income,
                estimate=net_income_estimate,
            ),
            _expectation_factor(
                factors,
                key="eps_surprise_pct",
                label="每股收益预期差",
                actual=eps,
                estimate=eps_estimate,
            ),
        )
    )
    if surprise_count == 0:
        missing.append("expectation_gap:consensus_estimates_missing")

    revenue_yoy = _numeric_value(
        current, ("revenue_yoy_pct", "revenueGrowth", "TOTALOPERATEREVETZ", "营业收入同比")
    )
    income_yoy = _numeric_value(
        current, ("net_income_yoy_pct", "netIncomeGrowth", "PARENTNETPROFITTZ", "净利润同比")
    )
    previous = _comparable_previous(current, records)
    previous_revenue = (
        _numeric_value(previous, ("revenue", "TOTALOPERATEREVE", "营业收入")) if previous else None
    )
    previous_income = (
        _numeric_value(previous, ("netIncome", "PARENTNETPROFIT", "归母净利润"))
        if previous
        else None
    )
    if revenue_yoy is None:
        revenue_yoy = _percent_change(previous_revenue, revenue)
    if income_yoy is None:
        income_yoy = _percent_change(previous_income, net_income)
    _append_factor(
        factors,
        key="revenue_yoy_pct",
        category=FactorCategory.EARNINGS_QUALITY,
        label="营业收入同比",
        value=revenue_yoy,
        unit="percent",
        confidence=0.9,
        description="本报告期营业收入相对可比上年同期的变化",
        source_keys=["fundamentals"],
        inputs={"current": revenue, "previous": previous_revenue},
    )
    _append_factor(
        factors,
        key="net_income_yoy_pct",
        category=FactorCategory.EARNINGS_QUALITY,
        label="归母净利润同比",
        value=income_yoy,
        unit="percent",
        confidence=0.9,
        description="本报告期归母净利润相对可比上年同期的变化",
        source_keys=["fundamentals"],
        inputs={"current": net_income, "previous": previous_income},
    )
    if revenue and net_income is not None and previous_revenue and previous_income is not None:
        margin = net_income / revenue * 100
        previous_margin = previous_income / previous_revenue * 100
        _append_factor(
            factors,
            key="net_margin_yoy_delta_pp",
            category=FactorCategory.EARNINGS_QUALITY,
            label="净利率同比变化",
            value=margin - previous_margin,
            unit="percentage_points",
            confidence=0.9,
            description="本报告期净利率减去可比上年同期净利率",
            source_keys=["fundamentals"],
            inputs={"current_margin_pct": margin, "previous_margin_pct": previous_margin},
        )


def compute_research_factors(
    *,
    as_of: datetime,
    event_at: datetime | None = None,
    horizon_days: int | None = None,
    event_type: str | None = None,
    event_texts: Sequence[str] = (),
    event_details: Mapping[str, Any] | None = None,
    fundamentals: Mapping[str, Any] | None = None,
    expectations: Mapping[str, Any] | None = None,
    expectations_at: datetime | None = None,
    asset_prices: Sequence[Mapping[str, Any]] | None = None,
    benchmark_prices: Sequence[Mapping[str, Any]] | None = None,
    industry_prices: Sequence[Mapping[str, Any]] | None = None,
    market_timezone: str = "UTC",
    market_close_time: time | None = None,
) -> ResearchFactorResult:
    """Compute point-in-time factors without performing network or database I/O.

    Missing or malformed inputs are represented in ``missing_requirements``. They are
    never coerced to a neutral numeric factor, which keeps unavailable data distinct
    from an actual zero surprise or zero excess return.
    """

    as_of = as_utc(as_of)
    event_at = as_utc(event_at) if event_at else None
    expectations_at = as_utc(expectations_at) if expectations_at else None
    if horizon_days is not None and horizon_days < 1:
        raise ValueError("horizon_days must be positive")
    details = event_details if isinstance(event_details, Mapping) else {}
    financials = fundamentals if isinstance(fundamentals, Mapping) else {}
    consensus = expectations if isinstance(expectations, Mapping) else {}
    texts = [str(item) for item in event_texts if item]
    families = _event_families(event_type, texts)
    factors: list[ResearchFactor] = []
    missing: list[str] = []

    if event_at is not None and event_at > as_of:
        return ResearchFactorResult(
            event_families=families,
            missing_requirements=["event:event_is_after_as_of"],
        )
    event_inputs_available = event_at is not None
    if families and not event_inputs_available:
        missing.append("event:event_timestamp_missing")
    if consensus:
        consensus_boundary = event_at or as_of
        if expectations_at is None:
            missing.append("expectation_gap:estimate_timestamp_missing")
            consensus = {}
        elif expectations_at > consensus_boundary:
            missing.append("expectation_gap:estimate_is_after_event")
            consensus = {}

    price_points, baseline = _market_factors(
        factors=factors,
        missing=missing,
        as_of=as_of,
        event_at=event_at,
        horizon_days=horizon_days,
        event_details=details,
        asset_prices=asset_prices,
        benchmark_prices=benchmark_prices,
        industry_prices=industry_prices,
        market_timezone=market_timezone,
        market_close_time=market_close_time,
    )
    if event_inputs_available:
        _capital_action_factors(
            factors=factors,
            missing=missing,
            families=families,
            event_texts=texts,
            event_details=details,
            fundamentals=financials,
            price_points=price_points,
            baseline=baseline,
            event_at=event_at,
        )
        _earnings_factors(
            factors=factors,
            missing=missing,
            families=families,
            fundamentals=financials,
            expectations=consensus,
            as_of=as_of,
        )
    aggregate_signal, reliability, category_signals, category_reliability = _aggregate_factors(
        factors,
        event_at=event_at,
        families=families,
    )
    return ResearchFactorResult(
        event_families=families,
        factors=factors,
        aggregate_signal=aggregate_signal,
        reliability=reliability,
        category_signals=category_signals,
        category_reliability=category_reliability,
        missing_requirements=list(dict.fromkeys(missing)),
    )


def _factor_claim(
    asset: AssetRef, category: FactorCategory, factors: Sequence[ResearchFactor]
) -> str:
    labels = "；".join(
        f"{item.label}={item.value:g}{'%' if item.unit == 'percent' else ''}"
        for item in factors[:5]
    )
    category_name = {
        FactorCategory.MARKET_REACTION: "事件窗口市场反应",
        FactorCategory.RELATIVE_PERFORMANCE: "相对基准及行业表现",
        FactorCategory.EXPECTATION_GAP: "财报实际值与一致预期差",
        FactorCategory.EARNINGS_QUALITY: "财报增长与盈利质量",
        FactorCategory.CAPITAL_ACTION: "减持及回购量化因子",
    }[category]
    return f"{asset.name}{category_name}：{labels}"[:500]


def _evidence_for_factors(
    *,
    run_id: UUID,
    asset: AssetRef,
    as_of: datetime,
    event_at: datetime | None,
    factors: Sequence[ResearchFactor],
    sources: Mapping[str, FactorSource],
) -> tuple[list[Evidence], list[str], dict[str, float]]:
    output: list[Evidence] = []
    missing: list[str] = []
    source_reliability: dict[str, float] = {}
    for factor in factors:
        source_keys = list(dict.fromkeys(factor.source_keys))
        available_sources = [sources[key] for key in source_keys if key in sources]
        absent_sources = [key for key in source_keys if key not in sources]
        if absent_sources:
            missing.append(
                f"factor_evidence:{factor.key}:source_metadata_missing:{','.join(absent_sources)}"
            )
            continue
        primary = available_sources[0]
        source_times = [
            as_utc(item.published_at) for item in available_sources if item.published_at
        ]
        if any(item > as_of for item in source_times):
            missing.append(f"factor_evidence:{factor.key}:source_after_as_of")
            continue
        source_reliability[factor.key] = min(
            _SOURCE_RELIABILITY[item.quality] for item in available_sources
        )
        audited_factor = factor.model_copy(
            update={"reliability": round(factor.reliability * source_reliability[factor.key], 6)}
        )
        available_at = (
            factor.window_end
            if factor.window_end is not None and factor.window_end <= as_of
            else event_at or as_of
        )
        published_at = max([as_utc(available_at), *source_times])
        excerpt_payload = {
            "method": "deterministic-research-factors:v1",
            "asset_id": asset.asset_id,
            "factor": {
                "key": audited_factor.key,
                "value": audited_factor.value,
                "unit": audited_factor.unit,
                "normalized_signal": audited_factor.normalized_signal,
                "reliability": audited_factor.reliability,
                "inputs": audited_factor.inputs,
            },
            "sources": [item.name for item in available_sources],
        }
        excerpt = json.dumps(excerpt_payload, ensure_ascii=False, default=str)[:1000]
        output.append(
            Evidence(
                run_id=run_id,
                claim=_factor_claim(asset, audited_factor.category, [audited_factor]),
                source_name="结构化研究因子 · "
                + " + ".join(item.name for item in available_sources),
                source_url=primary.url,
                source_quality=min(
                    (item.quality for item in available_sources),
                    key=_SOURCE_QUALITY_RANK.__getitem__,
                ),
                published_at=published_at,
                observed_at=as_of,
                as_of=as_of,
                excerpt=excerpt,
                independent_group=primary.independent_group,
                numeric_value=audited_factor.value,
                numeric_unit=audited_factor.unit,
            )
        )
    return output, missing, source_reliability


def build_research_factor_evidence(
    *,
    run_id: UUID,
    asset: AssetRef,
    as_of: datetime,
    event_at: datetime | None = None,
    horizon_days: int | None = None,
    event_type: str | None = None,
    event_texts: Sequence[str] = (),
    event_details: Mapping[str, Any] | None = None,
    fundamentals: Mapping[str, Any] | None = None,
    expectations: Mapping[str, Any] | None = None,
    expectations_at: datetime | None = None,
    asset_prices: Sequence[Mapping[str, Any]] | None = None,
    benchmark_prices: Sequence[Mapping[str, Any]] | None = None,
    industry_prices: Sequence[Mapping[str, Any]] | None = None,
    sources: Mapping[str, FactorSource | Mapping[str, Any]] | None = None,
) -> ResearchFactorResult:
    """Compute factors and convert sourced factor groups into ``Evidence`` records.

    Expected source keys are ``market``, ``benchmark``, ``industry``, ``event``,
    ``fundamentals`` and ``expectations``. A factor without source metadata remains
    available for diagnostics but is deliberately not promoted to evidence.
    """

    as_of = as_utc(as_of)
    validated_sources: dict[str, FactorSource] = {}
    source_errors: list[str] = []
    for key, value in (sources or {}).items():
        try:
            validated_sources[str(key)] = (
                value if isinstance(value, FactorSource) else FactorSource.model_validate(value)
            )
        except Exception:
            source_errors.append(f"factor_evidence:{key}:invalid_source_metadata")
    if expectations_at is None and "expectations" in validated_sources:
        expectations_at = validated_sources["expectations"].published_at
    market_timezone, market_close_time = _asset_market_session(asset)
    result = compute_research_factors(
        as_of=as_of,
        event_at=event_at,
        horizon_days=horizon_days,
        event_type=event_type,
        event_texts=event_texts,
        event_details=event_details,
        fundamentals=fundamentals,
        expectations=expectations,
        expectations_at=expectations_at,
        asset_prices=asset_prices,
        benchmark_prices=benchmark_prices,
        industry_prices=industry_prices,
        market_timezone=market_timezone,
        market_close_time=market_close_time,
    )
    evidence, evidence_missing, source_reliability = _evidence_for_factors(
        run_id=run_id,
        asset=asset,
        as_of=as_of,
        event_at=as_utc(event_at) if event_at else None,
        factors=result.factors,
        sources=validated_sources,
    )
    audited_factors = [
        item.model_copy(
            update={"reliability": round(item.reliability * source_reliability[item.key], 6)}
        )
        if item.key in source_reliability
        else item.model_copy(update={"reliability": 0.0})
        for item in result.factors
    ]
    sourced_factors = [item for item in audited_factors if item.key in source_reliability]
    aggregate_signal, reliability, category_signals, category_reliability = _aggregate_factors(
        sourced_factors,
        event_at=as_utc(event_at) if event_at else None,
        families=result.event_families,
    )
    return result.model_copy(
        update={
            "factors": audited_factors,
            "evidence": evidence,
            "aggregate_signal": aggregate_signal,
            "reliability": reliability,
            "category_signals": category_signals,
            "category_reliability": category_reliability,
            "missing_requirements": list(
                dict.fromkeys([*result.missing_requirements, *source_errors, *evidence_missing])
            ),
        }
    )
