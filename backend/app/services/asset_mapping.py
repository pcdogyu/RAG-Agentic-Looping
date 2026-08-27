from __future__ import annotations

import json
import re
import unicodedata
from typing import Literal

from pydantic import BaseModel, Field

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AssetClass,
    AssetRef,
    CandidateAsset,
    Market,
    NewsEvent,
    NewsItem,
)
from backend.app.llm import LlmGateway, gateway
from backend.app.providers.registry import (
    ProviderRegistry,
    explicit_symbol_present,
    query_mentions_issuer,
    text_contains_term,
)


class AssetMappingHint(BaseModel):
    source_mention: str = Field(min_length=1)
    name: str = Field(min_length=1)
    symbol: str = ""
    market: Market | None = None
    asset_class: AssetClass | None = None
    relationship: Literal["direct", "entity"]
    confidence: float = Field(ge=0, le=1)
    rationale: str = Field(min_length=1)
    search_queries: list[str] = Field(default_factory=list)


class AssetMappingOutput(BaseModel):
    candidates: list[AssetMappingHint] = Field(default_factory=list, max_length=10)
    no_asset_reason: str = ""


class AssetMappingResult(BaseModel):
    candidates: list[CandidateAsset] = Field(default_factory=list)
    proposed_count: int = 0
    rejected_count: int = 0
    no_asset_reason: str = ""


def normalize_security_text(value: str) -> str:
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", value.lower())


def normalize_listing_symbol(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip().casefold()


class AssetMappingService:
    minimum_confidence = 0.60

    def __init__(
        self,
        registry: ProviderRegistry,
        settings: Settings | None = None,
        llm: LlmGateway | None = None,
    ) -> None:
        self.registry = registry
        self.settings = settings or get_settings()
        self.llm = llm or gateway

    def map_event(self, event: NewsEvent, news_items: list[NewsItem]) -> AssetMappingResult:
        source_text = "\n".join(
            [event.headline, *event.entities]
            + [f"{item.title}\n{item.summary}" for item in news_items]
        )
        source_symbols = [symbol for item in news_items for symbol in item.symbols]
        prompt = (
            "从给定新闻中找出被明确提及、可交易且直接相关的股票或高流动性加密资产。"
            "只做名称到证券代码的消歧，不得推荐行业受益股、ETF、指数或新闻未提及的代理标的。"
            "source_mention 必须逐字来自新闻；证券代码必须是完整独立 token，不能从其他单词中截取。"
            "提供 symbol 时必须与主数据中的具体上市代码完全一致，不得改选同发行人的其他上市代码。"
            "机器人等同时是行业通用词的简称，原文没有明确代码或完整发行人身份时不得映射。"
            "不知道代码时保留空字符串。\n"
            f"事件：{event.model_dump_json(exclude={'analysis_steps', 'candidates'})}\n"
            f"新闻：{json.dumps([item.model_dump(mode='json') for item in news_items], ensure_ascii=False)[:18000]}"
        )
        payload = self.llm.generate_json(
            model=self.settings.ollama_assist_model,
            lane="assist",
            system="你是谨慎的跨市场证券主数据映射器。宁可不映射，也不能创造证券或关系。",
            prompt=prompt,
            schema=AssetMappingOutput,
            temperature=0,
            operation="asset_mapping",
            entity_type="news_event",
            entity_id=event.id,
        )
        output = AssetMappingOutput.model_validate(payload)
        candidates: dict[str, CandidateAsset] = {}
        rejected = 0
        for hint in output.candidates:
            candidate = self._validate_hint(hint, source_text, source_symbols)
            if not candidate:
                rejected += 1
                continue
            previous = candidates.get(candidate.asset.asset_id)
            if not previous or candidate.relevance > previous.relevance:
                candidates[candidate.asset.asset_id] = candidate
        ranked = sorted(candidates.values(), key=lambda item: item.relevance, reverse=True)
        return AssetMappingResult(
            candidates=ranked,
            proposed_count=len(output.candidates),
            rejected_count=rejected,
            no_asset_reason=output.no_asset_reason,
        )

    def _validate_hint(
        self, hint: AssetMappingHint, source_text: str, source_symbols: list[str]
    ) -> CandidateAsset | None:
        mention = normalize_security_text(hint.source_mention)
        if (
            not mention
            or not text_contains_term(source_text, hint.source_mention)
            or hint.confidence < self.minimum_confidence
            or "etf" in normalize_security_text(hint.name)
            or "基金" in hint.name
        ):
            return None

        queries = [hint.symbol, hint.name, hint.source_mention, *hint.search_queries]
        resolved: dict[str, AssetRef] = {}
        for query in queries:
            if query.strip():
                for asset in self.registry.resolve_assets(query):
                    resolved[asset.asset_id] = asset

        hinted_symbol = normalize_listing_symbol(hint.symbol)
        if hinted_symbol:
            resolved = {
                asset_id: asset
                for asset_id, asset in resolved.items()
                if normalize_listing_symbol(asset.symbol) == hinted_symbol
            }
            if not resolved:
                return None

        for asset in resolved.values():
            if hint.market and asset.market is not hint.market:
                continue
            if hint.asset_class and asset.asset_class is not hint.asset_class:
                continue
            if "etf" in normalize_security_text(asset.name) or "基金" in asset.name:
                continue
            if not self._identity_matches(hint, asset, source_text, source_symbols):
                continue
            source_symbol = explicit_symbol_present(source_text, asset.symbol) or any(
                explicit_symbol_present(value, asset.symbol, allow_bare=True)
                for value in source_symbols
            )
            cross_listing = bool(asset.primary_listing_asset_id and not source_symbol)
            return CandidateAsset(
                asset=asset,
                relationship="cross_listing_issuer" if cross_listing else hint.relationship,
                impact_direction=0,
                relevance=min(hint.confidence, 0.55) if cross_listing else hint.confidence,
                rationale=hint.rationale,
                mapping_confidence=(
                    min(hint.confidence, 0.75) if cross_listing else hint.confidence
                ),
                identity_basis=[
                    "llm_source_mention",
                    "provider_master",
                    *(["explicit_primary_listing"] if cross_listing else []),
                ],
            )
        return None

    @staticmethod
    def _identity_matches(
        hint: AssetMappingHint,
        asset: AssetRef,
        source_text: str,
        source_symbols: list[str],
    ) -> bool:
        symbol_matches = bool(hint.symbol.strip()) and (
            normalize_listing_symbol(hint.symbol) == normalize_listing_symbol(asset.symbol)
        )
        source_symbol = explicit_symbol_present(source_text, asset.symbol) or any(
            explicit_symbol_present(value, asset.symbol, allow_bare=True)
            for value in source_symbols
        )
        if symbol_matches and source_symbol:
            return True
        return query_mentions_issuer(hint.source_mention, asset)
