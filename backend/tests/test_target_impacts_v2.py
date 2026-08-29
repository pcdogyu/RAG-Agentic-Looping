from datetime import UTC, datetime
from uuid import uuid4

import pytest

from backend.app.config import Settings
from backend.app.domain import (
    ActionStage,
    AssetClass,
    AssetRef,
    CandidateAsset,
    EventAction,
    EventReport,
    EventType,
    Evidence,
    Market,
    NewsEvent,
    Rating,
    SourceQuality,
    TradeStatus,
)
from backend.app.providers.fmp import FmpProvider
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.directional_scoring import (
    target_confidence_score,
    target_rating_for,
    target_transmission_score,
)
from backend.app.services.macro_impacts import (
    TARGET_SCORING_VERSION,
    EventImpactDraft,
    TargetImpactDraft,
    finalize_impacts,
    rule_based_event_draft,
)


@pytest.mark.parametrize(
    ("stage", "proposed", "expected"),
    [
        (ActionStage.STATEMENT, 0.9, 0.2),
        (ActionStage.THREAT, 0.1, 0.25),
        (ActionStage.ANNOUNCED, 0.9, 0.7),
        (ActionStage.EFFECTIVE, 0.2, 0.7),
        (ActionStage.REALIZED, 0.2, 0.85),
        (ActionStage.UNKNOWN, 1.0, 0.1),
    ],
)
def test_action_strength_is_clamped_to_stage(stage, proposed, expected):
    action = EventAction(
        actor="actor",
        action_type="test",
        action_stage=stage,
        action="action",
        strength=proposed,
    )

    assert action.strength == expected


@pytest.mark.parametrize(
    ("score", "rating"),
    [
        (0.65, Rating.STRONGLY_BULLISH),
        (0.25, Rating.BULLISH),
        (0.249, Rating.WATCH),
        (-0.249, Rating.WATCH),
        (-0.25, Rating.BEARISH),
        (-0.65, Rating.STRONGLY_BEARISH),
    ],
)
def test_target_rating_boundaries_are_directional_at_quarter(score, rating):
    assert target_rating_for(score) is rating


def test_target_score_is_multiplicative_and_rounds_to_two_places():
    result = target_transmission_score(
        direction=1,
        event_strength=0.5,
        target_relevance=0.4,
        transmission_directness=0.5,
        realization_probability=0.7,
        novelty=0.3,
        persistence=1,
    )

    assert result.score == 0.02
    assert result.score_points == 2
    assert result.rating is Rating.WATCH


def test_target_confidence_is_capped_by_fact_confidence():
    result = target_confidence_score(
        fact_confidence=0.55,
        direction_clarity=1,
        source_reliability=1,
        transmission_certainty=1,
        market_context_completeness=1,
    )

    assert result.uncapped_confidence == 1
    assert result.confidence == 0.55


def test_legacy_global_direction_json_remains_readable_but_is_not_republished():
    candidate = CandidateAsset.model_validate(
        {
            "asset": SEED_ASSETS[0].model_dump(mode="json"),
            "relationship": "direct",
            "relevance": 0.9,
            "rationale": "legacy payload",
            "impact_direction": -1,
        }
    )
    report = EventReport.model_validate(
        {
            "summary": "legacy report",
            "direction": -1,
            "score": -0.6,
            "rating": "bearish",
        }
    )

    assert "impact_direction" not in candidate.model_dump()
    assert report.scoring_version == "event-report-v1"
    assert "direction" not in report.model_dump()
    assert "score" not in report.model_dump()
    assert "rating" not in report.model_dump()


def _evidence(headline: str) -> Evidence:
    observed = datetime(2026, 8, 24, tzinfo=UTC)
    return Evidence(
        run_id=uuid4(),
        claim=headline,
        source_name="Official statement",
        source_url="https://official.example/statement",
        source_quality=SourceQuality.OFFICIAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        excerpt=headline,
        independent_group="official:foreign-ministry",
    )


def _event(headline: str, action: EventAction) -> NewsEvent:
    observed = datetime(2026, 8, 24, tzinfo=UTC)
    return NewsEvent(
        news_item_ids=[uuid4()],
        headline=headline,
        event_type=EventType.REGULATION,
        entities=["伊朗", "美国"],
        actions=[action],
        direct_impact=headline,
        source_quality=SourceQuality.OFFICIAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )


def _asset_map():
    return {item.asset_id: item for item in SEED_ASSETS}


def test_iran_sanctions_are_bearish_for_iran_but_neutral_for_markets():
    headline = "伊朗外交部：美国启动新一轮对伊制裁等同于实施国家恐怖主义"
    event = _event(
        headline,
        EventAction(
            actor="美国",
            action_type="sanctions",
            action_stage=ActionStage.ANNOUNCED,
            action="启动新一轮制裁",
            object="伊朗",
            scope="制裁范围未说明",
            strength=0.6,
        ),
    )
    evidence = [_evidence(headline)]
    draft = rule_based_event_draft(event, evidence, _asset_map())
    _, impacts, _, missing = finalize_impacts(
        draft,
        event=event,
        evidence=evidence,
        assets=_asset_map(),
    )
    by_name = {item.target_name: item for item in impacts}

    assert by_name["伊朗经济"].rating is Rating.BEARISH
    assert by_name["伊朗经济"].trade_status is TradeStatus.UNTRADEABLE
    for name in ("伊朗原油出口量", "Brent/WTI 原油价格", "黄金", "全球股票"):
        assert by_name[name].rating is Rating.WATCH
        assert by_name[name].trade_status is TradeStatus.UNTRADEABLE
    assert "whether_oil_exports_are_targeted" in missing
    assert by_name["Brent/WTI 原油价格"].score > 0
    assert by_name["Brent/WTI 原油价格"].score < 0.25


def test_explicit_oil_export_sanctions_are_bullish_for_crude():
    headline = "美国宣布制裁伊朗原油出口、港口和运输"
    event = _event(
        headline,
        EventAction(
            actor="美国",
            action_type="sanctions",
            action_stage=ActionStage.ANNOUNCED,
            action="宣布石油出口制裁",
            object="伊朗原油出口",
            scope="原油出口、港口和运输",
            strength=0.6,
        ),
    )
    evidence = [_evidence(headline)]
    draft = rule_based_event_draft(event, evidence, _asset_map())
    _, impacts, _, _ = finalize_impacts(
        draft, event=event, evidence=evidence, assets=_asset_map()
    )
    oil = next(item for item in impacts if item.target_name == "Brent/WTI 原油价格")

    assert oil.rating is Rating.BULLISH
    assert oil.score >= 0.25
    assert oil.execution_supported is False


def test_bank_sanctions_keep_crude_weak_and_neutral():
    headline = "美国宣布制裁伊朗银行和金融机构"
    event = _event(
        headline,
        EventAction(
            actor="美国",
            action_type="sanctions",
            action_stage=ActionStage.ANNOUNCED,
            action="宣布银行制裁",
            object="伊朗银行",
            scope="银行和金融机构",
            strength=0.6,
        ),
    )
    evidence = [_evidence(headline)]
    draft = rule_based_event_draft(event, evidence, _asset_map())
    _, impacts, _, _ = finalize_impacts(
        draft, event=event, evidence=evidence, assets=_asset_map()
    )
    by_name = {item.target_name: item for item in impacts}

    assert by_name["伊朗经济"].rating is Rating.BEARISH
    assert by_name["Brent/WTI 原油价格"].rating is Rating.WATCH
    assert 0 < by_name["Brent/WTI 原油价格"].score < 0.25


def test_diplomatic_condemnation_alone_is_neutral_for_risk_assets():
    headline = "伊朗外交部谴责美国施压并重申既有立场"
    event = _event(
        headline,
        EventAction(
            actor="伊朗外交部",
            action_type="condemnation",
            action_stage=ActionStage.STATEMENT,
            action="公开谴责并重申立场",
            object="美国",
            strength=0.15,
        ),
    )
    evidence = [_evidence(headline)]
    draft = rule_based_event_draft(event, evidence, _asset_map())
    factors, impacts, _, _ = finalize_impacts(
        draft, event=event, evidence=evidence, assets=_asset_map()
    )
    risk = next(item for item in impacts if item.target_name == "全球风险资产")

    assert any(item.id == "diplomatic_tension" for item in factors)
    assert risk.rating is Rating.WATCH
    assert risk.trade_status is TradeStatus.UNTRADEABLE


@pytest.mark.parametrize(
    ("headline", "stage", "expected_direction", "expected_rating"),
    [
        ("伊朗威胁关闭霍尔木兹海峡", ActionStage.THREAT, 1, Rating.WATCH),
        (
            "霍尔木兹海峡已经关闭并导致航道中断",
            ActionStage.REALIZED,
            1,
            Rating.STRONGLY_BULLISH,
        ),
        ("霍尔木兹海峡恢复通航并重新开放", ActionStage.REALIZED, -1, Rating.BEARISH),
    ],
)
def test_hormuz_scenarios_have_target_specific_oil_transmission(
    headline, stage, expected_direction, expected_rating
):
    action_type = "deescalation" if expected_direction < 0 else "strait_closure"
    event = _event(
        headline,
        EventAction(
            actor="伊朗",
            action_type=action_type,
            action_stage=stage,
            action=headline,
            object="霍尔木兹海峡",
            strength=0.9 if stage is ActionStage.REALIZED else 0.35,
        ),
    )
    evidence = [_evidence(headline)]
    draft = rule_based_event_draft(event, evidence, _asset_map())
    _, impacts, _, _ = finalize_impacts(
        draft,
        event=event,
        evidence=evidence,
        assets=_asset_map(),
    )
    oil = next(item for item in impacts if item.target_name == "Brent/WTI 原油价格")

    assert oil.direction == expected_direction
    assert oil.rating is expected_rating
    assert oil.asset is not None
    assert oil.asset.market is Market.COMMODITY
    assert oil.execution_supported is False


def test_mapping_technical_failure_keeps_all_targets_untradeable():
    headline = "霍尔木兹海峡已经关闭并导致航道中断"
    event = _event(
        headline,
        EventAction(
            actor="伊朗",
            action_type="strait_closure",
            action_stage=ActionStage.REALIZED,
            action="海峡关闭",
            object="霍尔木兹海峡",
            strength=0.9,
        ),
    )
    evidence = [_evidence(headline)]
    draft = rule_based_event_draft(event, evidence, _asset_map())
    _, impacts, _, _ = finalize_impacts(
        draft,
        event=event,
        evidence=evidence,
        assets=_asset_map(),
        technical_failure=True,
    )

    assert all(item.trade_status is TradeStatus.UNTRADEABLE for item in impacts)
    assert all(item.technical_failure for item in impacts)


def test_industry_peer_association_cannot_become_an_execution_signal():
    stock = AssetRef(
        asset_id="equity:XNAS:NVDA",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="NVDA",
        name="NVIDIA",
        exchange_or_provider="XNAS",
        sector_id="sector:information_technology",
        industry_id="industry:semiconductors",
        instrument_type="common_stock",
    )
    headline = "半导体行业先进制程需求升温"
    event = _event(
        headline,
        EventAction(
            actor="行业机构",
            action_type="industry_update",
            action_stage=ActionStage.REALIZED,
            action="确认行业需求上升",
            object="半导体行业",
            strength=0.85,
        ),
    )
    event.candidates = [
        CandidateAsset(
            asset=stock,
            relationship="industry_peer",
            relevance=0.40,
            mapping_confidence=0.55,
            rationale="新闻只提到行业，公司未被点名。",
        )
    ]
    evidence = [_evidence(headline)]
    draft = EventImpactDraft(
        summary=headline,
        evidence_ids=[str(evidence[0].id)],
        impacts=[
            TargetImpactDraft(
                target_type="tradable_asset",
                target_name=stock.name,
                asset_id=stock.asset_id,
                direction_score=90,
                transmission_path=["行业需求", "芯片订单", "公司收入"],
                rationale="行业需求可能向公司订单传导。",
                evidence_ids=[str(evidence[0].id)],
            )
        ],
    )

    _, impacts, _, _ = finalize_impacts(
        draft,
        event=event,
        evidence=evidence,
        assets={stock.asset_id: stock},
    )

    assert impacts[0].trade_status is TradeStatus.UNTRADEABLE
    assert impacts[0].execution_supported is False
    assert "industry_only_mapping" in impacts[0].missing_information


def test_fmp_parses_commodity_and_fx_master_lists(monkeypatch):
    provider = FmpProvider(Settings(_env_file=None, fmp_access_token="token", fmp_mcp_url=""))

    def fake_rest(endpoint, _params, ttl=0):
        assert ttl == 86400
        if endpoint == "commodities-list":
            return [{"symbol": "CLUSD", "name": "Crude Oil", "currency": "USD"}]
        return {"data": [{"symbol": "EURUSD", "name": "EUR/USD"}]}

    monkeypatch.setattr(provider, "_rest", fake_rest)
    assets = {item.symbol: item for item in provider.list_macro_assets()}

    assert assets["CLUSD"].asset_class is AssetClass.COMMODITY
    assert assets["CLUSD"].market is Market.COMMODITY
    assert assets["EURUSD"].asset_class is AssetClass.FX
    assert assets["EURUSD"].market is Market.FX
    assert TARGET_SCORING_VERSION == "llm-direction-v3"


def test_fmp_parses_macro_quote_list_payload(monkeypatch):
    provider = FmpProvider(Settings(_env_file=None, fmp_access_token="token", fmp_mcp_url=""))
    asset = _asset_map()["commodity:fmp:CLUSD"]
    monkeypatch.setattr(
        provider,
        "_mcp_or_rest",
        lambda *args, **kwargs: [{"symbol": "CLUSD", "price": 74.25}],
    )

    quote = provider.get_quote(asset)

    assert quote == {"symbol": "CLUSD", "price": 74.25}
