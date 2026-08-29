from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from backend.app.domain import (
    ActionStage,
    AssetClass,
    AssetRef,
    EventAction,
    Evidence,
    MacroFactor,
    MacroFactorType,
    NewsEvent,
    ScoreSource,
    SourceQuality,
    TargetConfidenceFactors,
    TargetImpact,
    TargetType,
    TradeStatus,
    TransmissionFactors,
)
from backend.app.services.confidence_v3 import (
    RATING_CONFIDENCE_VERSION,
    SCORING_VERSION,
    event_horizon_days,
    rating_confidence_score,
    rating_for_direction_score,
)
from backend.app.services.directional_scoring import (
    target_transmission_score,
)

TARGET_SCORING_VERSION = SCORING_VERSION
TARGET_CONFIDENCE_VERSION = RATING_CONFIDENCE_VERSION
MAX_TARGET_IMPACTS = 6
MAX_DIRECT_TARGETS = 3
TRADE_SCORE_THRESHOLD = 30
TRADE_CONFIDENCE_THRESHOLD = 0.55


@dataclass(frozen=True)
class MacroRule:
    code: str
    description: str


MACRO_RULES: tuple[MacroRule, ...] = (
    MacroRule("sanctions_oil_exports", "制裁石油出口会通过全球供应减少推高原油价格。"),
    MacroRule("sanctions_banking", "制裁银行会压制贸易结算，并可能弱化能源出口。"),
    MacroRule("diplomatic_condemnation", "外交谴责只轻微提高紧张度，大部分资产保持中性。"),
    MacroRule("strait_closure_threat", "威胁关闭海峡提高能源运输风险，但尚未形成中断。"),
    MacroRule("strait_closed", "海峡实际关闭意味着能源运输和供应已经中断。"),
    MacroRule("talks_or_reopening", "恢复谈判或通航会降低供应风险溢价。"),
)


class MacroFactorDraft(BaseModel):
    id: str
    factor_type: MacroFactorType
    name: str
    description: str
    strength: float = Field(default=0, ge=0, le=1)
    evidence_ids: list[str] = Field(default_factory=list)


class TargetImpactDraft(BaseModel):
    target_type: TargetType
    target_name: str
    asset_id: str | None = None
    action_id: str | None = None
    direction_score: int | None = Field(default=None, ge=-100, le=100)
    direction: int = Field(default=0, ge=-1, le=1)
    factors: TransmissionFactors = Field(default_factory=TransmissionFactors)
    confidence_factors: TargetConfidenceFactors = Field(
        default_factory=TargetConfidenceFactors
    )
    macro_factor_ids: list[str] = Field(default_factory=list)
    transmission_path: list[str] = Field(default_factory=list)
    rationale: str = ""
    evidence_ids: list[str] = Field(default_factory=list)
    missing_information: list[str] = Field(default_factory=list)


class EventImpactDraft(BaseModel):
    summary: str
    affected_markets: list[str] = Field(default_factory=list)
    affected_sectors: list[str] = Field(default_factory=list)
    scenarios: list[str] = Field(default_factory=list)
    catalysts: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    macro_factors: list[MacroFactorDraft] = Field(default_factory=list, max_length=12)
    impacts: list[TargetImpactDraft] = Field(default_factory=list, max_length=MAX_TARGET_IMPACTS)
    missing_information: list[str] = Field(default_factory=list)


class ModelTargetImpactDraft(BaseModel):
    """The v3 LLM contract: direction score is its only numeric conclusion."""

    model_config = ConfigDict(extra="forbid")

    target_type: TargetType
    target_name: str
    asset_id: str | None = None
    action_id: str | None = None
    direction_score: int = Field(ge=-100, le=100)
    macro_factor_ids: list[str] = Field(default_factory=list)
    transmission_path: list[str] = Field(default_factory=list)
    rationale: str = ""
    evidence_ids: list[str] = Field(default_factory=list)
    missing_information: list[str] = Field(default_factory=list)


class ModelMacroFactorDraft(BaseModel):
    """Text-only macro context; the model does not assign numeric strength."""

    model_config = ConfigDict(extra="forbid")

    id: str
    factor_type: MacroFactorType
    name: str
    description: str
    evidence_ids: list[str] = Field(default_factory=list)


class ModelEventImpactDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str
    affected_markets: list[str] = Field(default_factory=list)
    affected_sectors: list[str] = Field(default_factory=list)
    scenarios: list[str] = Field(default_factory=list)
    catalysts: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    macro_factors: list[ModelMacroFactorDraft] = Field(default_factory=list, max_length=12)
    impacts: list[ModelTargetImpactDraft] = Field(
        default_factory=list, max_length=MAX_TARGET_IMPACTS
    )
    missing_information: list[str] = Field(default_factory=list)


def internal_event_draft(value: ModelEventImpactDraft) -> EventImpactDraft:
    payload = value.model_dump(mode="json")
    payload["macro_factors"] = [
        MacroFactorDraft(**item, strength=0).model_dump(mode="json")
        for item in payload["macro_factors"]
    ]
    payload["impacts"] = [
        TargetImpactDraft(**item).model_dump(mode="json") for item in payload["impacts"]
    ]
    return EventImpactDraft.model_validate(payload)


_SOURCE_WEIGHTS = {
    SourceQuality.OFFICIAL: 1.0,
    SourceQuality.PRIMARY: 0.9,
    SourceQuality.PROFESSIONAL: 0.82,
    SourceQuality.AGGREGATOR: 0.65,
    SourceQuality.SOCIAL: 0.4,
}


def fact_confidence_for(evidence: Sequence[Evidence]) -> float:
    if not evidence:
        return 0.0
    base = max(_SOURCE_WEIGHTS[item.source_quality] for item in evidence)
    groups = len({item.independent_group for item in evidence if item.independent_group})
    corroboration = 0.05 * min(2, max(0, groups - 1))
    return round(min(0.95, base + corroboration), 4)


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = value.strip()
        key = normalized.casefold()
        if normalized and key not in seen:
            result.append(normalized)
            seen.add(key)
    return result


def _action_strength(
    action_id: str | None,
    actions: Mapping[str, EventAction],
    proposed: float,
) -> tuple[float, list[str]]:
    if not action_id or action_id not in actions:
        return 0.10, ["action_stage"]
    action = actions[action_id]
    if action.action_stage is ActionStage.UNKNOWN:
        return 0.10, ["action_stage"]
    return min(action.strength, proposed), []


def finalize_impacts(
    draft: EventImpactDraft,
    *,
    event: NewsEvent,
    evidence: Sequence[Evidence],
    assets: Mapping[str, AssetRef],
    technical_failure: bool = False,
) -> tuple[list[MacroFactor], list[TargetImpact], float, list[str]]:
    """Validate model inputs and calculate every published v2 value in code."""

    valid_ids = {str(item.id): item.id for item in evidence}
    actions = {item.id: item for item in event.actions}
    fact_confidence = fact_confidence_for(evidence)
    macro_factors = [
        MacroFactor(
            id=item.id,
            factor_type=item.factor_type,
            name=item.name,
            description=item.description,
            strength=item.strength,
            evidence_ids=[valid_ids[value] for value in item.evidence_ids if value in valid_ids],
        )
        for item in draft.macro_factors
    ]
    factor_ids = {item.id for item in macro_factors}

    direct_ids = {
        candidate.asset.asset_id
        for candidate in event.candidates[:MAX_DIRECT_TARGETS]
    }
    proposed_drafts = list(draft.impacts)
    proposed_asset_ids = {item.asset_id for item in proposed_drafts if item.asset_id}
    default_action = event.actions[0] if event.actions else None
    for candidate in event.candidates[:MAX_DIRECT_TARGETS]:
        if candidate.asset.asset_id in proposed_asset_ids:
            continue
        proposed_drafts.append(
            TargetImpactDraft(
                target_type=TargetType.TRADABLE_ASSET,
                target_name=candidate.asset.name,
                asset_id=candidate.asset.asset_id,
                action_id=default_action.id if default_action else None,
                direction=0,
                factors=TransmissionFactors(
                    event_strength=default_action.strength if default_action else 0.10,
                    target_relevance=candidate.relevance,
                    transmission_directness=candidate.relevance,
                    novelty=event.novelty,
                ),
                confidence_factors=TargetConfidenceFactors(
                    source_reliability=fact_confidence,
                ),
                rationale="主数据只确认了工具身份，尚无目标专属方向与传导证据。",
                evidence_ids=list(draft.evidence_ids),
                missing_information=["target_direction", "transmission_evidence"],
            )
        )
    direct_drafts = [item for item in proposed_drafts if item.asset_id in direct_ids][
        :MAX_DIRECT_TARGETS
    ]
    indirect_drafts = [item for item in proposed_drafts if item not in direct_drafts]
    ordered_drafts = [*direct_drafts, *indirect_drafts][:MAX_TARGET_IMPACTS]

    impacts: list[TargetImpact] = []
    seen_targets: set[tuple[str, str]] = set()
    for item in ordered_drafts:
        asset = assets.get(item.asset_id or "")
        target_key = (
            item.target_type.value,
            asset.asset_id if asset else item.target_name.strip().casefold(),
        )
        if not target_key[1] or target_key in seen_targets:
            continue
        seen_targets.add(target_key)

        event_strength, action_missing = _action_strength(
            item.action_id, actions, item.factors.event_strength
        )
        factors = item.factors.model_copy(update={"event_strength": event_strength})
        missing = _dedupe_strings([*item.missing_information, *action_missing])
        legacy_score = target_transmission_score(
            direction=item.direction,
            **factors.model_dump(),
        )
        score_points = item.direction_score
        if score_points is None:
            score_points = legacy_score.score_points
        if technical_failure:
            score_points = max(-69, min(69, score_points))
        referenced_evidence = [
            valid_ids[value] for value in item.evidence_ids if value in valid_ids
        ]
        if not referenced_evidence:
            missing = _dedupe_strings([*missing, "impact_evidence"])
        candidate = next(
            (
                value
                for value in event.candidates
                if asset is not None and value.asset.asset_id == asset.asset_id
            ),
            None,
        )
        confidence_score = rating_confidence_score(
            direction_score=score_points,
            event=event,
            candidate=candidate,
            transmission_path=item.transmission_path,
            cited_evidence_ids=referenced_evidence,
            evidence=evidence,
            missing_information=missing,
        )
        confidence = confidence_score.confidence
        tradeable = bool(
            asset
            and score_points >= TRADE_SCORE_THRESHOLD
            and confidence >= TRADE_CONFIDENCE_THRESHOLD
            and not missing
            and not technical_failure
        )
        impacts.append(
            TargetImpact(
                target_type=item.target_type,
                target_name=item.target_name,
                asset=asset,
                direction=1 if score_points > 0 else -1 if score_points < 0 else 0,
                score=score_points / 100,
                direction_score=score_points,
                rating=rating_for_direction_score(score_points),
                confidence=confidence,
                rating_confidence=confidence,
                factors=factors,
                confidence_factors=item.confidence_factors,
                rating_confidence_factors=confidence_score.factors,
                mapping_distance=confidence_score.mapping_distance,
                score_source=(
                    ScoreSource.RULE_FALLBACK if technical_failure else ScoreSource.LLM
                ),
                horizon_days=event_horizon_days(event.event_type),
                macro_factor_ids=[
                    value for value in item.macro_factor_ids if value in factor_ids
                ],
                transmission_path=_dedupe_strings(item.transmission_path),
                rationale=item.rationale.strip(),
                evidence_ids=referenced_evidence,
                missing_information=missing,
                trade_status=(
                    TradeStatus.TRADEABLE if tradeable else TradeStatus.UNTRADEABLE
                ),
                execution_supported=bool(
                    asset
                    and asset.asset_class in {AssetClass.EQUITY, AssetClass.CRYPTO}
                ),
                technical_failure=technical_failure,
            )
        )
    missing_information = _dedupe_strings(
        [
            *draft.missing_information,
            *(value for item in impacts for value in item.missing_information),
        ]
    )
    return macro_factors, impacts, fact_confidence, missing_information


def _first_asset(assets: Mapping[str, AssetRef], *symbols: str) -> AssetRef | None:
    requested = {value.casefold() for value in symbols}
    return next(
        (item for item in assets.values() if item.symbol.casefold() in requested),
        None,
    )


def _impact(
    *,
    target_type: TargetType,
    target_name: str,
    direction: int,
    action: EventAction,
    evidence_ids: list[str],
    factors: tuple[float, float, float, float, float, float],
    confidence: tuple[float, float, float, float],
    rationale: str,
    path: list[str],
    missing: list[str] | None = None,
    asset: AssetRef | None = None,
    macro_factor_ids: list[str] | None = None,
) -> TargetImpactDraft:
    transmission = TransmissionFactors(
        event_strength=factors[0],
        target_relevance=factors[1],
        transmission_directness=factors[2],
        realization_probability=factors[3],
        novelty=factors[4],
        persistence=factors[5],
    )
    score = target_transmission_score(direction=direction, **transmission.model_dump())
    return TargetImpactDraft(
        target_type=target_type,
        target_name=target_name,
        asset_id=asset.asset_id if asset else None,
        action_id=action.id,
        direction_score=score.score_points,
        direction=direction,
        factors=transmission,
        confidence_factors=TargetConfidenceFactors(
            direction_clarity=confidence[0],
            source_reliability=confidence[1],
            transmission_certainty=confidence[2],
            market_context_completeness=confidence[3],
        ),
        macro_factor_ids=macro_factor_ids or [],
        transmission_path=path,
        rationale=rationale,
        evidence_ids=evidence_ids,
        missing_information=missing or [],
    )


def rule_based_event_draft(
    event: NewsEvent,
    evidence: Sequence[Evidence],
    assets: Mapping[str, AssetRef],
) -> EventImpactDraft:
    """Conservative v2 fallback and deterministic regression oracle."""

    text = " ".join(
        [event.headline, event.direct_impact, *event.entities]
        + [item.claim + " " + item.excerpt for item in evidence]
    ).casefold()
    evidence_ids = [str(item.id) for item in evidence]
    action = next(
        (
            item
            for item in event.actions
            if item.action_stage is not ActionStage.STATEMENT
        ),
        event.actions[0] if event.actions else EventAction(
            actor="unknown",
            action_type="unknown",
            action_stage=ActionStage.UNKNOWN,
            action="unknown",
        ),
    )
    impacts: list[TargetImpactDraft] = []
    factors: list[MacroFactorDraft] = []
    missing: list[str] = []

    sanction = any(value in text for value in ("sanction", "制裁"))
    iran = any(value in text for value in ("iran", "伊朗"))
    oil_scope = any(
        value in text
        for value in ("oil export", "petroleum", "crude export", "石油出口", "原油出口")
    )
    banking_scope = any(
        value in text for value in ("bank", "financial institution", "银行", "金融机构")
    )
    strait = any(value in text for value in ("hormuz", "霍尔木兹", "海峡"))
    closure = any(value in text for value in ("closed", "closure", "关闭", "封锁"))
    threat = any(value in text for value in ("threat", "warn", "威胁", "警告"))
    reopen = any(value in text for value in ("reopen", "restore", "talks", "开放", "恢复", "谈判"))
    condemnation = any(
        value in text for value in ("condemn", "protest", "谴责", "抗议", "重申立场")
    )

    crude = _first_asset(assets, "BZUSD", "CLUSD")
    gold = _first_asset(assets, "ZGUSD", "GCUSD", "XAUUSD")

    if sanction:
        factors.append(
            MacroFactorDraft(
                id="sanctions",
                factor_type=MacroFactorType.SANCTIONS,
                name="制裁执行压力",
                description="制裁可能限制贸易、金融结算或出口。",
                strength=action.strength,
                evidence_ids=evidence_ids,
            )
        )
        if iran:
            impacts.append(
                _impact(
                    target_type=TargetType.ECONOMY,
                    target_name="伊朗经济",
                    direction=-1,
                    action=action,
                    evidence_ids=evidence_ids,
                    factors=(0.70, 0.95, 0.90, 0.90, 0.75, 0.80),
                    confidence=(0.85, 0.90, 0.75, 0.60),
                    rationale="制裁会提高贸易、金融和出口压力。",
                    path=["美国制裁", "贸易与结算受限", "伊朗经济承压"],
                    macro_factor_ids=["sanctions"],
                )
            )
        scope_missing = [] if oil_scope else ["sanction_scope", "whether_oil_exports_are_targeted"]
        impacts.extend(
            [
                _impact(
                    target_type=TargetType.SUPPLY_VOLUME,
                    target_name="伊朗原油出口量",
                    direction=-1,
                    action=action,
                    evidence_ids=evidence_ids,
                    factors=(
                        0.85 if oil_scope else 0.60,
                        0.45 if not oil_scope else 0.95,
                        0.50 if not oil_scope else 0.90,
                        0.70 if not oil_scope else 0.85,
                        0.40 if not oil_scope else 0.80,
                        0.60 if not oil_scope else 0.80,
                    ),
                    confidence=(0.50, 0.85, 0.40, 0.35),
                    rationale="只有制裁范围覆盖石油、港口、航运或支付时才会直接压低出口。",
                    path=["制裁", "出口能力可能受限"],
                    missing=scope_missing,
                    macro_factor_ids=["sanctions"],
                ),
                _impact(
                    target_type=TargetType.COMMODITY_PRICE,
                    target_name="Brent/WTI 原油价格",
                    direction=1,
                    action=action,
                    evidence_ids=evidence_ids,
                    factors=(
                        0.85 if oil_scope else 0.60,
                        0.40 if not oil_scope else 0.95,
                        0.50 if not oil_scope else 0.90,
                        0.70 if not oil_scope else 0.85,
                        0.30 if not oil_scope else 0.80,
                        0.50 if not oil_scope else 0.80,
                    ),
                    confidence=(0.45, 0.85, 0.35, 0.30),
                    rationale="潜在供应收缩理论上利多油价，但制裁范围和实际出口影响尚不明确。",
                    path=["制裁", "潜在供应下降", "油价风险溢价"],
                    missing=scope_missing,
                    asset=crude,
                    macro_factor_ids=["sanctions"],
                ),
                _impact(
                    target_type=TargetType.COMMODITY_PRICE,
                    target_name="黄金",
                    direction=1,
                    action=action,
                    evidence_ids=evidence_ids,
                    factors=(0.20, 0.35, 0.30, 0.50, 0.30, 0.35),
                    confidence=(0.35, 0.85, 0.30, 0.25),
                    rationale="外交紧张只有轻微避险含义，尚无军事升级。",
                    path=["外交紧张", "轻微避险需求"],
                    asset=gold,
                    macro_factor_ids=["sanctions"],
                ),
                _impact(
                    target_type=TargetType.RISK_ASSET,
                    target_name="全球股票",
                    direction=-1,
                    action=action,
                    evidence_ids=evidence_ids,
                    factors=(0.20, 0.30, 0.25, 0.45, 0.30, 0.30),
                    confidence=(0.35, 0.85, 0.25, 0.25),
                    rationale="地缘情绪略偏负面，但缺少实际升级。",
                    path=["外交紧张", "风险偏好轻微下降"],
                    macro_factor_ids=["sanctions"],
                ),
            ]
        )
        if not oil_scope:
            missing.extend(scope_missing)
        if not banking_scope:
            missing.append("secondary_sanctions")
        missing.append("effective_date")

    if condemnation and not sanction:
        factors.append(
            MacroFactorDraft(
                id="diplomatic_tension",
                factor_type=MacroFactorType.OTHER,
                name="外交紧张",
                description="外交谴责只轻微提高紧张程度，尚未形成政策或实际中断。",
                strength=action.strength,
                evidence_ids=evidence_ids,
            )
        )
        impacts.append(
            _impact(
                target_type=TargetType.RISK_ASSET,
                target_name="全球风险资产",
                direction=-1,
                action=action,
                evidence_ids=evidence_ids,
                factors=(0.20, 0.30, 0.25, 0.40, 0.30, 0.25),
                confidence=(0.40, 0.85, 0.25, 0.25),
                rationale="谴责性表态只带来轻微地缘情绪，缺少政策执行或军事升级。",
                path=["外交谴责", "紧张程度轻微上升", "风险偏好影响有限"],
                macro_factor_ids=["diplomatic_tension"],
            )
        )

    if strait and (closure or threat or reopen):
        direction = -1 if reopen else 1
        realized = closure and not threat and not reopen
        severity = 0.90 if realized else 0.35 if threat else 0.85
        factors.append(
            MacroFactorDraft(
                id="shipping",
                factor_type=MacroFactorType.SHIPPING,
                name="海峡运输风险",
                description="霍尔木兹海峡状态影响全球能源运输。",
                strength=severity,
                evidence_ids=evidence_ids,
            )
        )
        impacts.insert(
            0,
            _impact(
                target_type=TargetType.COMMODITY_PRICE,
                target_name="Brent/WTI 原油价格",
                direction=direction,
                action=action,
                evidence_ids=evidence_ids,
                factors=(
                    severity,
                    1.00 if realized else 0.95,
                    1.00 if realized else 0.85 if reopen else 0.65,
                    1.00 if realized else 0.90 if reopen else 0.60,
                    0.90 if realized else 0.85,
                    0.90 if realized else 0.85 if not reopen else 0.75,
                ),
                confidence=(
                    0.95 if realized else 0.85 if reopen else 0.65,
                    0.90,
                    0.90 if realized else 0.80 if reopen else 0.55,
                    0.75,
                ),
                rationale=("实际运输中断强烈推高供应风险。" if realized else "海峡风险改变原油运输溢价。"),
                path=["海峡状态变化", "能源运输风险变化", "油价风险溢价变化"],
                asset=crude,
                macro_factor_ids=["shipping"],
            ),
        )

    for candidate in event.candidates[:MAX_DIRECT_TARGETS]:
        if len(impacts) >= MAX_TARGET_IMPACTS:
            break
        if any(item.asset_id == candidate.asset.asset_id for item in impacts):
            continue
        impacts.append(
            TargetImpactDraft(
                target_type=TargetType.TRADABLE_ASSET,
                target_name=candidate.asset.name,
                asset_id=candidate.asset.asset_id,
                action_id=action.id,
                direction=0,
                factors=TransmissionFactors(
                    event_strength=action.strength,
                    target_relevance=candidate.relevance,
                    transmission_directness=candidate.relevance,
                    realization_probability=0,
                    novelty=event.novelty,
                    persistence=0,
                ),
                confidence_factors=TargetConfidenceFactors(
                    source_reliability=fact_confidence_for(evidence)
                ),
                rationale="规则回退只确认了证券身份，未得到目标专属方向证据。",
                evidence_ids=evidence_ids,
                missing_information=["target_direction", "transmission_evidence"],
            )
        )

    if not impacts:
        impacts.append(
            TargetImpactDraft(
                target_type=TargetType.OTHER,
                target_name="待确认目标",
                action_id=action.id,
                direction=0,
                factors=TransmissionFactors(event_strength=action.strength),
                confidence_factors=TargetConfidenceFactors(
                    source_reliability=fact_confidence_for(evidence)
                ),
                rationale="当前证据不足以建立目标专属传导路径。",
                evidence_ids=evidence_ids,
                missing_information=["affected_target", "transmission_path"],
            )
        )
    return EventImpactDraft(
        summary=event.headline,
        affected_markets=[item.target_name for item in impacts[:MAX_TARGET_IMPACTS]],
        scenarios=["补充政策范围与执行证据后重新计算目标影响"],
        risks=["缺失信息可能改变传导方向或强度"],
        unresolved_questions=_dedupe_strings(missing),
        evidence_ids=evidence_ids,
        macro_factors=factors,
        impacts=impacts[:MAX_TARGET_IMPACTS],
        missing_information=_dedupe_strings(missing),
    )


def public_rule_catalog() -> list[dict[str, Any]]:
    return [rule.__dict__.copy() for rule in MACRO_RULES]
