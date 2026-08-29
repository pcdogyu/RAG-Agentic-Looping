from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from backend.app.domain import AssetClass, AssetRef, Market, SourceQuality
from backend.app.services.research_factors import (
    FactorSource,
    build_research_factor_evidence,
    compute_research_factors,
)

ASSET = AssetRef(
    asset_id="equity:XSHG:688251",
    asset_class=AssetClass.EQUITY,
    market=Market.CN,
    symbol="688251",
    name="井松智能",
    exchange_or_provider="XSHG",
    currency="CNY",
)
US_ASSET = AssetRef(
    asset_id="equity:XNAS:AAPL",
    asset_class=AssetClass.EQUITY,
    market=Market.US,
    symbol="AAPL",
    name="Apple Inc.",
    exchange_or_provider="XNAS",
)


@pytest.fixture(autouse=True)
def clean_database():
    """These pure factor tests deliberately do not share the suite's SQLite fixture."""

    yield


def _prices(values, *, start=datetime(2025, 1, 1, tzinfo=UTC), volume=200_000):
    return [
        {
            "日期": (start + timedelta(days=index)).date().isoformat(),
            "收盘": value,
            "成交量": volume,
        }
        for index, value in enumerate(values)
    ]


def _factor_values(result):
    return {item.key: item.value for item in result.factors}


def test_market_reaction_and_relative_returns_are_point_in_time():
    event_at = datetime(2025, 1, 2, 8, tzinfo=UTC)
    result = compute_research_factors(
        as_of=datetime(2025, 1, 6, 23, tzinfo=UTC),
        event_at=event_at,
        horizon_days=30,
        event_texts=["公司发布重大公告"],
        event_details={"after_market_close": False},
        asset_prices=[
            *_prices([100, 110, 112, 114, 116, 120]),
            *_prices([999], start=datetime(2025, 2, 1, tzinfo=UTC)),
        ],
        benchmark_prices=_prices([100, 101, 102, 103, 104, 105]),
        industry_prices=_prices([100, 102, 104, 106, 108, 110]),
    )

    values = _factor_values(result)
    assert values["asset_return_1d_pct"] == pytest.approx(10)
    assert values["asset_return_5d_pct"] == pytest.approx(20)
    assert values["excess_vs_benchmark_5d_pct"] == pytest.approx(15)
    assert values["excess_vs_industry_5d_pct"] == pytest.approx(10)
    assert 0 < result.aggregate_signal <= 1
    assert 0 < result.reliability <= 1
    assert all(-1 <= item.normalized_signal <= 1 for item in result.factors)
    assert all(
        item.window_end is None or item.window_end <= result.factors[-1].window_end
        for item in result.factors
    )


def test_market_window_never_exceeds_event_horizon_or_as_of():
    result = compute_research_factors(
        as_of=datetime(2025, 1, 10, tzinfo=UTC),
        event_at=datetime(2025, 1, 2, tzinfo=UTC),
        horizon_days=2,
        event_details={"after_market_close": False},
        asset_prices=_prices([100, 101, 102, 103, 104, 105, 106]),
        benchmark_prices=_prices([100, 100, 100, 100, 100, 100, 100]),
        industry_prices=_prices([100, 100, 100, 100, 100, 100, 100]),
    )

    keys = {item.key for item in result.factors}
    assert "asset_return_1d_pct" in keys
    assert "asset_return_5d_pct" not in keys
    assert "market_reaction:5d_exceeds_event_horizon" in result.missing_requirements


def test_market_factors_include_the_event_calendar_horizon_to_date_window():
    result = compute_research_factors(
        as_of=datetime(2025, 1, 6, 23, tzinfo=UTC),
        event_at=datetime(2025, 1, 2, 8, tzinfo=UTC),
        horizon_days=90,
        event_details={"after_market_close": False},
        asset_prices=_prices([100, 110, 112, 114, 116, 120]),
        benchmark_prices=_prices([100, 101, 102, 103, 104, 105]),
        industry_prices=_prices([100, 102, 104, 106, 108, 110]),
    )

    factors = {item.key: item for item in result.factors}
    horizon = factors["asset_return_horizon_90cd_pct"]
    assert horizon.value == pytest.approx(20)
    assert horizon.inputs["horizon_days"] == 90
    assert horizon.inputs["window_complete"] is False
    assert "excess_vs_benchmark_horizon_90cd_pct" in factors
    assert "excess_vs_industry_horizon_90cd_pct" in factors


def test_earnings_factors_measure_consensus_surprise_and_comparable_growth():
    result = compute_research_factors(
        as_of=datetime(2025, 7, 1, tzinfo=UTC),
        event_at=datetime(2025, 6, 30, tzinfo=UTC),
        event_type="earnings",
        event_texts=["公司发布2025年中报业绩"],
        fundamentals={
            "income": [
                {
                    "date": "2025-06-30",
                    "revenue": 120,
                    "netIncome": 18,
                    "eps": 1.2,
                },
                {
                    "date": "2024-06-30",
                    "revenue": 100,
                    "netIncome": 10,
                    "eps": 0.8,
                },
            ]
        },
        expectations={
            "revenue_estimate": 100,
            "net_income_estimate": 15,
            "eps_estimate": 1.0,
        },
        expectations_at=datetime(2025, 6, 29, tzinfo=UTC),
    )

    values = _factor_values(result)
    assert values["revenue_surprise_pct"] == pytest.approx(20)
    assert values["net_income_surprise_pct"] == pytest.approx(20)
    assert values["eps_surprise_pct"] == pytest.approx(20)
    assert values["revenue_yoy_pct"] == pytest.approx(20)
    assert values["net_income_yoy_pct"] == pytest.approx(80)
    assert values["net_margin_yoy_delta_pp"] == pytest.approx(5)
    assert result.category_signals["expectation_gap"] > 0
    # Missing price/benchmark inputs reduce reliability instead of becoming neutral factors.
    assert 0 < result.reliability < 1


def test_reduction_and_buyback_factors_keep_opposing_actions_separate():
    event_at = datetime(2025, 1, 25, tzinfo=UTC)
    result = compute_research_factors(
        as_of=datetime(2025, 1, 30, tzinfo=UTC),
        event_at=event_at,
        horizon_days=30,
        event_texts=[
            "持股5%以上股东因司法强制执行，计划减持不超过1,000,000股，"
            "占公司总股本不超1%；公司同时发布股份回购计划。"
        ],
        event_details={
            "total_shares": 100_000_000,
            "buyback_shares": 2_000_000,
            "buyback_amount": 50_000_000,
            "buyback_price_cap": 25,
            "market_cap": 2_000_000_000,
        },
        asset_prices=_prices([20] * 30),
    )

    values = _factor_values(result)
    assert result.event_families == ["share_reduction", "buyback"]
    assert values["share_reduction_total_shares_pct"] == pytest.approx(1)
    assert values["share_reduction_average_volume_days"] == pytest.approx(5)
    assert values["forced_share_reduction"] == 1
    assert values["buyback_total_shares_pct"] == pytest.approx(2)
    assert values["buyback_market_cap_pct"] == pytest.approx(2.5)
    assert values["buyback_price_cap_premium_pct"] == pytest.approx(25)
    assert values["net_buyback_minus_reduction_pct"] == pytest.approx(1)
    assert (
        next(
            item for item in result.factors if item.key == "share_reduction_total_shares_pct"
        ).normalized_signal
        < 0
    )
    assert (
        next(
            item for item in result.factors if item.key == "buyback_total_shares_pct"
        ).normalized_signal
        > 0
    )


def test_each_sourced_factor_becomes_separate_evidence_without_network_io():
    event_at = datetime(2025, 1, 25, tzinfo=UTC)
    sources = {
        "market": FactorSource(
            name="行情数据",
            url="https://data.example/688251/prices",
            independent_group="market-data",
        ),
        "event": FactorSource(
            name="交易所公告",
            url="https://official.example/announcement",
            quality=SourceQuality.OFFICIAL,
            independent_group="official-filing",
            published_at=event_at,
        ),
        "fundamentals": FactorSource(
            name="公司主数据",
            url="https://data.example/688251/fundamentals",
            independent_group="fundamentals",
        ),
    }
    result = build_research_factor_evidence(
        run_id=uuid4(),
        asset=ASSET,
        as_of=datetime(2025, 1, 30, tzinfo=UTC),
        event_at=event_at,
        horizon_days=30,
        event_texts=["股东拟减持不超1%股份"],
        event_details={"reduction_pct": 1, "reduction_shares": 1_000_000},
        fundamentals={"free_float_shares": 10_000_000},
        asset_prices=_prices([20] * 30),
        sources=sources,
    )

    assert len(result.evidence) == len(result.factors)
    assert len({item.claim for item in result.evidence}) == len(result.evidence)
    assert all(item.as_of == datetime(2025, 1, 30, tzinfo=UTC) for item in result.evidence)
    assert all("normalized_signal" in item.excerpt for item in result.evidence)
    assert (
        next(item for item in result.evidence if "自由流通盘" in item.claim).source_quality
        is SourceQuality.AGGREGATOR
    )


def test_missing_and_malformed_inputs_do_not_create_fake_neutral_evidence():
    result = build_research_factor_evidence(
        run_id=uuid4(),
        asset=ASSET,
        as_of=datetime(2025, 1, 1, tzinfo=UTC),
        event_texts=["普通新闻"],
        asset_prices=[{"date": "bad", "close": "unknown"}],
        sources={"market": {"name": "", "url": ""}},
    )

    assert result.factors == []
    assert result.evidence == []
    assert result.aggregate_signal == 0
    assert result.reliability == 0
    assert "market_reaction:event_timestamp_missing" in result.missing_requirements
    assert "factor_evidence:market:invalid_source_metadata" in result.missing_requirements


def test_future_dated_source_cannot_be_promoted_to_factor_evidence():
    result = build_research_factor_evidence(
        run_id=uuid4(),
        asset=ASSET,
        as_of=datetime(2025, 1, 2, tzinfo=UTC),
        event_at=datetime(2025, 1, 1, tzinfo=UTC),
        event_texts=["股东拟减持不超1%股份"],
        event_details={"reduction_pct": 1},
        sources={
            "event": FactorSource(
                name="未来公告",
                url="https://official.example/future",
                quality=SourceQuality.OFFICIAL,
                independent_group="official-filing",
                published_at=datetime(2025, 1, 3, tzinfo=UTC),
            )
        },
    )

    assert result.factors
    assert result.evidence == []
    assert result.aggregate_signal == 0
    assert result.reliability == 0
    assert all(item.reliability == 0 for item in result.factors)
    assert any(item.endswith(":source_after_as_of") for item in result.missing_requirements)


def test_factor_without_source_metadata_is_diagnostic_with_zero_reliability():
    result = build_research_factor_evidence(
        run_id=uuid4(),
        asset=ASSET,
        as_of=datetime(2025, 1, 2, tzinfo=UTC),
        event_at=datetime(2025, 1, 1, tzinfo=UTC),
        event_texts=["股东拟减持不超1%股份"],
        event_details={"reduction_pct": 1},
    )

    assert result.factors
    assert result.evidence == []
    assert all(item.reliability == 0 for item in result.factors)
    assert result.aggregate_signal == 0
    assert result.reliability == 0
    assert any("source_metadata_missing:event" in item for item in result.missing_requirements)


def test_future_event_cannot_produce_market_or_event_specific_factors():
    result = compute_research_factors(
        as_of=datetime(2025, 1, 1, tzinfo=UTC),
        event_at=datetime(2025, 1, 2, tzinfo=UTC),
        event_texts=["股东拟减持不超1%股份"],
        event_details={"reduction_pct": 1},
        asset_prices=_prices([100, 50]),
    )

    assert result.factors == []
    assert result.aggregate_signal == 0
    assert result.reliability == 0
    assert result.missing_requirements == ["event:event_is_after_as_of"]


def test_post_event_consensus_is_not_used_as_an_earnings_surprise():
    event_at = datetime(2025, 6, 30, tzinfo=UTC)
    result = compute_research_factors(
        as_of=datetime(2025, 7, 1, tzinfo=UTC),
        event_at=event_at,
        event_type="earnings",
        fundamentals={
            "income": [
                {"date": "2025-06-30", "revenue": 120},
                {"date": "2024-06-30", "revenue": 100},
            ]
        },
        expectations={"revenue_estimate": 100},
        expectations_at=datetime(2025, 7, 1, tzinfo=UTC),
    )

    assert "revenue_surprise_pct" not in _factor_values(result)
    assert "expectation_gap:estimate_is_after_event" in result.missing_requirements


def test_unknown_event_session_excludes_announcement_day_close():
    result = compute_research_factors(
        as_of=datetime(2025, 1, 3, 23, tzinfo=UTC),
        event_at=datetime(2025, 1, 2, 8, tzinfo=UTC),
        asset_prices=_prices([100, 150, 110]),
    )

    factor = next(item for item in result.factors if item.key == "asset_return_1d_pct")
    assert factor.value == pytest.approx(10)
    assert factor.inputs["same_day_close_used"] is False
    assert (
        "market_reaction:event_session_unknown_same_day_excluded"
        in result.missing_requirements
    )


def test_exchange_timezone_overrides_false_after_close_hint_conservatively():
    event_at = datetime(2025, 1, 3, 21, 30, tzinfo=UTC)  # 16:30 New York
    result = build_research_factor_evidence(
        run_id=uuid4(),
        asset=US_ASSET,
        as_of=datetime(2025, 1, 7, 23, tzinfo=UTC),
        event_at=event_at,
        event_details={"after_market_close": "false"},
        asset_prices=[
            {"date": "2025-01-02", "close": 100},
            {"date": "2025-01-03", "close": 150},
            {"date": "2025-01-06", "close": 90},
        ],
        sources={
            "market": FactorSource(
                name="市场行情",
                url="https://data.example/aapl",
                independent_group="market-data",
            )
        },
    )

    factor = next(item for item in result.factors if item.key == "asset_return_1d_pct")
    assert factor.value == pytest.approx(-10)
    assert factor.inputs["market_timezone"] == "America/New_York"
    assert factor.inputs["same_day_close_used"] is False
    assert (
        "market_reaction:event_timestamp_is_after_market_close"
        in result.missing_requirements
    )


def test_same_day_partial_bar_is_not_used_before_exchange_close():
    result = build_research_factor_evidence(
        run_id=uuid4(),
        asset=US_ASSET,
        as_of=datetime(2025, 1, 3, 17, tzinfo=UTC),  # 12:00 New York
        event_at=datetime(2025, 1, 3, 15, tzinfo=UTC),  # 10:00 New York
        event_details={"after_market_close": False},
        asset_prices=[
            {"date": "2025-01-02", "close": 100},
            {"date": "2025-01-03", "close": 150},
        ],
        sources={
            "market": FactorSource(
                name="市场行情",
                url="https://data.example/aapl",
                independent_group="market-data",
            )
        },
    )

    assert "asset_return_1d_pct" not in _factor_values(result)
    assert "market_reaction:same_day_close_not_observable" in result.missing_requirements
