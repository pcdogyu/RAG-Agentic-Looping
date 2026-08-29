from __future__ import annotations

import json
import re
import unicodedata
from typing import Literal

from pydantic import BaseModel, Field, ValidationError, model_validator

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AssetClass,
    AssetRef,
    CandidateAsset,
    Market,
    NewsEvent,
    NewsItem,
)
from backend.app.llm import LlmError, LlmGateway, gateway
from backend.app.providers.registry import (
    ProviderRegistry,
    explicit_symbol_present,
    listing_symbols_equal,
    query_mentions_issuer,
    text_contains_term,
)


class AssetMappingHint(BaseModel):
    asset_id: str = ""
    source_mention: str = Field(min_length=1)
    name: str = Field(min_length=1)
    symbol: str = ""
    market: Market | None = None
    asset_class: AssetClass | None = None
    relationship: Literal["direct", "entity", "product_owner"]
    confidence: float = Field(ge=0, le=1)
    rationale: str = Field(min_length=1)
    search_queries: list[str] = Field(default_factory=list)


class AssetMappingOutput(BaseModel):
    candidates: list[AssetMappingHint] = Field(default_factory=list, max_length=10)
    industry_ids: list[str] = Field(default_factory=list, max_length=5)
    no_asset_reason: str = ""

    @model_validator(mode="after")
    def require_reason_for_empty_candidates(self) -> AssetMappingOutput:
        if not self.candidates and not self.no_asset_reason.strip():
            raise ValueError("no_asset_reason is required when candidates is empty")
        return self


class AssetMappingResult(BaseModel):
    candidates: list[CandidateAsset] = Field(default_factory=list)
    proposed_count: int = 0
    master_derived_count: int = 0
    rejected_count: int = 0
    no_asset_reason: str = ""
    technical_warning: str = ""
    industry_ids: list[str] = Field(default_factory=list)


def normalize_security_text(value: str) -> str:
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", value.lower())


def normalize_listing_symbol(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip().casefold()


def compact_mapping_news(news_items: list[NewsItem], *, max_chars: int = 12_000) -> str:
    """Keep mapping evidence focused while preserving valid JSON."""

    payloads: list[dict[str, object]] = []
    for item in news_items:
        candidate: dict[str, object] = {
            "title": item.title,
            "symbols": item.symbols,
            "summary": item.summary[:2000],
        }
        while True:
            encoded = json.dumps([*payloads, candidate], ensure_ascii=False, separators=(",", ":"))
            overflow = len(encoded) - max_chars
            if overflow <= 0:
                payloads.append(candidate)
                break
            summary = str(candidate["summary"])
            if not summary:
                return json.dumps(payloads, ensure_ascii=False, separators=(",", ":"))
            candidate["summary"] = summary[: max(0, len(summary) - overflow)]
    return json.dumps(payloads, ensure_ascii=False, separators=(",", ":"))


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
        event_payload = {
            "headline": event.headline,
            "event_type": event.event_type.value,
            "entities": event.entities,
        }
        event_json = json.dumps(event_payload, ensure_ascii=False, separators=(",", ":"))
        evidence_chars = 12_000
        news_payload = compact_mapping_news(
            news_items,
            max_chars=max(2, evidence_chars - len(event_json) - len("事件：\n新闻：")),
        )
        master_candidates = self._verified_product_candidates(source_text)
        shortlist_resolver = getattr(self.registry, "shortlist_assets", None)
        shortlist = shortlist_resolver(source_text, limit=30) if callable(shortlist_resolver) else []
        allowed_asset_ids = {item.asset_id for item in shortlist}
        industry_catalog = (
            self.registry.all_industries()
            if callable(getattr(self.registry, "all_industries", None))
            else []
        )
        allowed_industry_ids = {
            item.industry_id for item in industry_catalog if item.level == 2
        }
        mentioned_industries = (
            self.registry.industries_for_text(source_text)
            if callable(getattr(self.registry, "industries_for_text", None))
            else []
        )
        verified_products = [
            {
                "source_mention": candidate.identity_basis[-1],
                "owner": candidate.asset.name,
                "symbol": candidate.asset.symbol,
                "market": candidate.asset.market.value,
            }
            for candidate in master_candidates.values()
        ]
        prompt = (
            "从给定新闻中找出被明确提及、可交易且直接相关的股票或高流动性加密资产。"
            "只做名称到证券代码的消歧，不得推荐行业受益股、ETF、指数或新闻未提及的代理标的。"
            "新闻明确提及品牌产品，且下方主数据已验证产品归属时，可以使用 product_owner 关系；"
            "此时 symbol 留空，由系统展开同一发行人的上市代码。"
            "source_mention 必须逐字来自新闻；证券代码必须是完整独立 token，不能从其他单词中截取。"
            "提供 symbol 时必须与主数据中的具体上市代码完全一致，不得改选同发行人的其他上市代码。"
            "机器人等同时是行业通用词的简称，原文没有明确代码或完整发行人身份时不得映射。"
            "不知道代码时保留空字符串。没有候选时必须填写非空 no_asset_reason。\n"
            "优先从候选主数据中选择并原样返回 asset_id；有候选主数据时不得填写列表外代码。"
            "新闻只描述行业时填写 industry_ids，并由系统关联代表股，不得自行创造行业成分股。\n"
            f"事件：{event_json}\n"
            "已验证产品归属："
            f"{json.dumps(verified_products, ensure_ascii=False, separators=(',', ':'))}\n"
            "候选证券主数据："
            f"{json.dumps([{'asset_id': item.asset_id, 'symbol': item.symbol, 'name': item.name, 'aliases': item.aliases[:5], 'market': item.market.value, 'industry_id': item.industry_id} for item in shortlist], ensure_ascii=False, separators=(',', ':'))}\n"
            "允许行业："
            f"{json.dumps([{'industry_id': item.industry_id, 'name_zh': item.name_zh, 'name_en': item.name_en} for item in industry_catalog if item.level == 2], ensure_ascii=False, separators=(',', ':'))}\n"
            f"新闻：{news_payload}"
        )
        try:
            payload = self.llm.generate_json(
                model=self.settings.ollama_assist_model,
                lane="assist",
                system=(
                    "你是谨慎的跨市场证券主数据映射器。"
                    "宁可说明没有标的，也不能创造证券或关系。"
                ),
                prompt=prompt,
                schema=AssetMappingOutput,
                temperature=0,
                operation="asset_mapping",
                entity_type="news_event",
                entity_id=event.id,
                context_length=self.settings.ollama_asset_mapping_context_length,
                max_output_tokens=self.settings.ollama_asset_mapping_max_output_tokens,
            )
            output = AssetMappingOutput.model_validate(payload)
        except (LlmError, ValidationError) as exc:
            # Product ownership is deterministic master data. A malformed or
            # unavailable model response must not discard an already verified
            # owner/listing relationship.
            output = AssetMappingOutput(
                candidates=[],
                no_asset_reason="模型未返回合规结果，已使用主数据中的产品归属。",
            )
            technical_warning = f"{type(exc).__name__}: asset mapping validation failed"
        else:
            technical_warning = ""
        candidates: dict[str, CandidateAsset] = dict(master_candidates)
        rejected = 0
        for hint in output.candidates:
            validated = self._validate_hint(
                hint,
                source_text,
                source_symbols,
                allowed_asset_ids=allowed_asset_ids,
            )
            if not validated:
                rejected += 1
                continue
            for candidate in validated:
                previous = candidates.get(candidate.asset.asset_id)
                if not previous or candidate.relevance > previous.relevance:
                    candidates[candidate.asset.asset_id] = candidate
        industry_ids = list(
            dict.fromkeys(
                [
                    *mentioned_industries,
                    *[
                        item
                        for item in output.industry_ids
                        if item in allowed_industry_ids
                    ],
                ]
            )
        )
        representative_resolver = getattr(self.registry, "industry_representatives", None)
        if callable(representative_resolver) and industry_ids:
            for asset in representative_resolver(
                industry_ids,
                limit=max(0, 8 - len(candidates)),
            ):
                candidates.setdefault(
                    asset.asset_id,
                    CandidateAsset(
                        asset=asset,
                        relationship="industry_peer",
                        relevance=0.40,
                        rationale="新闻涉及该标的所属行业；公司未被新闻直接点名。",
                        mapping_confidence=0.55,
                        identity_basis=["industry_taxonomy", asset.industry_id],
                    ),
                )
        ranked = sorted(candidates.values(), key=lambda item: item.relevance, reverse=True)
        return AssetMappingResult(
            candidates=ranked,
            proposed_count=len(output.candidates),
            master_derived_count=len(master_candidates),
            rejected_count=rejected,
            no_asset_reason="" if ranked else output.no_asset_reason,
            technical_warning=technical_warning,
            industry_ids=industry_ids,
        )

    def _verified_product_candidates(
        self, source_text: str
    ) -> dict[str, CandidateAsset]:
        resolver = getattr(self.registry, "resolve_product_owners", None)
        if not callable(resolver):
            return {}
        candidates: dict[str, CandidateAsset] = {}
        for asset, product in resolver(source_text):
            candidates[asset.asset_id] = CandidateAsset(
                asset=asset,
                relationship="product_owner",
                relevance=0.85,
                rationale=f"主数据确认新闻中的 {product} 归属于 {asset.name}",
                mapping_confidence=0.99,
                identity_basis=["source_product", "product_owner_master", product],
            )
        return candidates

    def _validate_hint(
        self,
        hint: AssetMappingHint,
        source_text: str,
        source_symbols: list[str],
        *,
        allowed_asset_ids: set[str] | None = None,
    ) -> list[CandidateAsset]:
        mention = normalize_security_text(hint.source_mention)
        if (
            not mention
            or not text_contains_term(source_text, hint.source_mention)
            or hint.confidence < self.minimum_confidence
            or "etf" in normalize_security_text(hint.name)
            or "基金" in hint.name
        ):
            return []

        if hint.relationship == "product_owner":
            resolver = getattr(self.registry, "resolve_product_owners", None)
            if not callable(resolver):
                return []
            return [
                CandidateAsset(
                    asset=asset,
                    relationship="product_owner",
                    relevance=hint.confidence,
                    rationale=hint.rationale,
                    mapping_confidence=hint.confidence,
                    identity_basis=[
                        "llm_source_mention",
                        "product_owner_master",
                        product,
                    ],
                )
                for asset, product in resolver(hint.source_mention)
                if (not hint.asset_class or asset.asset_class is hint.asset_class)
            ]

        allowed_asset_ids = allowed_asset_ids or set()
        queries = [hint.symbol, hint.name, hint.source_mention, *hint.search_queries]
        resolved: dict[str, AssetRef] = {}
        if hint.asset_id:
            if allowed_asset_ids and hint.asset_id not in allowed_asset_ids:
                return []
            getter = getattr(self.registry, "get_asset", None)
            asset = getter(hint.asset_id) if callable(getter) else None
            if asset is not None:
                resolved[asset.asset_id] = asset
        for query in queries:
            if query.strip():
                for asset in self.registry.resolve_assets(query):
                    if allowed_asset_ids and asset.asset_id not in allowed_asset_ids:
                        continue
                    resolved[asset.asset_id] = asset

        hinted_symbol = normalize_listing_symbol(hint.symbol)
        if hinted_symbol:
            resolved = {
                asset_id: asset
                for asset_id, asset in resolved.items()
                if listing_symbols_equal(asset.symbol, hinted_symbol)
            }
            if not resolved:
                return []

        candidates: list[CandidateAsset] = []
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
            candidates.append(
                CandidateAsset(
                    asset=asset,
                    relationship=(
                        "cross_listing_issuer" if cross_listing else hint.relationship
                    ),
                    relevance=(
                        min(hint.confidence, 0.55)
                        if cross_listing
                        else hint.confidence
                    ),
                    rationale=hint.rationale,
                    mapping_confidence=(
                        min(hint.confidence, 0.75)
                        if cross_listing
                        else hint.confidence
                    ),
                    identity_basis=[
                        "llm_source_mention",
                        "provider_master",
                        *(["explicit_primary_listing"] if cross_listing else []),
                    ],
                )
            )
        return candidates

    @staticmethod
    def _identity_matches(
        hint: AssetMappingHint,
        asset: AssetRef,
        source_text: str,
        source_symbols: list[str],
    ) -> bool:
        symbol_matches = bool(hint.symbol.strip()) and (
            listing_symbols_equal(hint.symbol, asset.symbol)
        )
        source_symbol = explicit_symbol_present(source_text, asset.symbol) or any(
            explicit_symbol_present(value, asset.symbol, allow_bare=True)
            for value in source_symbols
        )
        if symbol_matches and source_symbol:
            return True
        return query_mentions_issuer(hint.source_mention, asset)
