from __future__ import annotations

import re
from difflib import SequenceMatcher

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    CandidateAsset,
    EventType,
    NewsEvent,
    NewsItem,
    SourceQuality,
)
from backend.app.llm import LlmGateway, gateway
from backend.app.providers.registry import ProviderRegistry
from backend.app.storage import save_event, save_news, upsert_asset


class ExtractedEvent(BaseModel):
    event_type: EventType = EventType.OTHER
    entities: list[str] = Field(default_factory=list)
    direct_impact: str
    horizon_days: int = Field(default=90, ge=1, le=730)
    impact_direction: int = Field(default=0, ge=-1, le=1)
    novelty: float = Field(default=0.5, ge=0, le=1)
    priority: float = Field(default=0.5, ge=0, le=1)
    search_queries: list[str] = Field(default_factory=list)


KEYWORD_TYPES = {
    EventType.EARNINGS: ["earnings", "revenue", "profit", "业绩", "营收", "利润", "财报"],
    EventType.REGULATION: ["regulation", "regulator", "ban", "监管", "处罚", "禁令"],
    EventType.PRODUCT: ["launch", "product", "release", "upgrade", "发布", "产品", "获批", "升级"],
    EventType.M_AND_A: ["acquisition", "merger", "takeover", "收购", "合并", "并购"],
    EventType.SECURITY: ["hack", "breach", "exploit", "攻击", "漏洞", "被盗"],
    EventType.TOKENOMICS: ["unlock", "airdrop", "token", "解锁", "空投", "代币"],
    EventType.SUPPLY_CHAIN: ["supplier", "shortage", "supply chain", "供应商", "短缺", "供应链"],
}


class EventService:
    def __init__(
        self,
        registry: ProviderRegistry,
        settings: Settings | None = None,
        llm: LlmGateway | None = None,
    ) -> None:
        self.registry = registry
        self.settings = settings or get_settings()
        self.llm = llm or gateway

    def ingest(self, db: Session, items: list[NewsItem]) -> list[NewsEvent]:
        events: list[NewsEvent] = []
        for item in items:
            if not save_news(db, item):
                continue
            event = self.extract(item)
            for candidate in event.candidates:
                upsert_asset(db, candidate.asset)
            existing = next(
                (candidate for candidate in events if self._same_story(candidate, event)), None
            )
            if existing:
                existing.news_item_ids.extend(event.news_item_ids)
                merged = {item.asset.asset_id: item for item in existing.candidates}
                for candidate in event.candidates:
                    previous = merged.get(candidate.asset.asset_id)
                    if not previous or candidate.relevance > previous.relevance:
                        merged[candidate.asset.asset_id] = candidate
                existing.candidates = sorted(
                    merged.values(), key=lambda value: value.relevance, reverse=True
                )
                existing.priority = max(existing.priority, event.priority)
                existing.novelty = min(existing.novelty, event.novelty)
                if self._quality_rank(event.source_quality) > self._quality_rank(
                    existing.source_quality
                ):
                    existing.source_quality = event.source_quality
            else:
                events.append(event)
        for event in events:
            save_event(db, event)
        return events

    @staticmethod
    def _same_story(left: NewsEvent, right: NewsEvent) -> bool:
        if left.event_type is not right.event_type:
            return False
        def normalize(value: str) -> str:
            return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", value.lower())

        similarity = SequenceMatcher(None, normalize(left.headline), normalize(right.headline)).ratio()
        left_assets = {candidate.asset.asset_id for candidate in left.candidates}
        right_assets = {candidate.asset.asset_id for candidate in right.candidates}
        return similarity >= 0.78 or (bool(left_assets & right_assets) and similarity >= 0.58)

    @staticmethod
    def _quality_rank(quality: SourceQuality) -> int:
        return {
            SourceQuality.SOCIAL: 0,
            SourceQuality.AGGREGATOR: 1,
            SourceQuality.PROFESSIONAL: 2,
            SourceQuality.PRIMARY: 3,
            SourceQuality.OFFICIAL: 4,
        }[quality]

    def extract(self, item: NewsItem) -> NewsEvent:
        prompt = (
            "从新闻元数据中提取一个可投资研究事件。不要补充新闻中没有的事实。"
            "影响方向只表示初步假设，不是投资建议。\n"
            f"标题：{item.title}\n摘要：{item.summary[:3000]}\n"
            f"来源：{item.source}\n已标注代码：{item.symbols}"
        )
        try:
            payload = self.llm.generate_json(
                model=self.settings.ollama_extract_model,
                system="你是谨慎的跨市场新闻结构化引擎。拒绝猜测，输出结构化事实。",
                prompt=prompt,
                schema=ExtractedEvent,
                temperature=0,
            )
            extracted = ExtractedEvent.model_validate(payload)
        except Exception:
            extracted = self._fallback_extract(item)

        queries = [item.title, *item.symbols, *extracted.entities, *extracted.search_queries]
        candidates: dict[str, CandidateAsset] = {}
        for query in queries:
            if not query.strip():
                continue
            for asset in self.registry.resolve_assets(query):
                relationship = "direct" if asset.symbol in item.symbols else "entity_or_product"
                relevance = 0.95 if relationship == "direct" else 0.70
                candidates[asset.asset_id] = CandidateAsset(
                    asset=asset,
                    relationship=relationship,
                    impact_direction=extracted.impact_direction,
                    relevance=relevance,
                    rationale=f"新闻中的 {query} 与 {asset.name} 匹配",
                )

        quality_factor = {
            SourceQuality.OFFICIAL: 1.0,
            SourceQuality.PRIMARY: 0.9,
            SourceQuality.PROFESSIONAL: 0.8,
            SourceQuality.AGGREGATOR: 0.6,
            SourceQuality.SOCIAL: 0.3,
        }[item.source_quality]
        priority = min(1.0, extracted.priority * quality_factor)
        return NewsEvent(
            news_item_ids=[item.id],
            headline=item.title,
            event_type=extracted.event_type,
            entities=extracted.entities,
            direct_impact=extracted.direct_impact,
            horizon_days=extracted.horizon_days,
            source_quality=item.source_quality,
            published_at=item.published_at,
            observed_at=item.observed_at,
            as_of=item.as_of,
            candidates=sorted(candidates.values(), key=lambda value: value.relevance, reverse=True),
            novelty=extracted.novelty,
            priority=priority,
        )

    @staticmethod
    def _fallback_extract(item: NewsItem) -> ExtractedEvent:
        text = f"{item.title} {item.summary}".lower()
        event_type = EventType.OTHER
        for candidate_type, keywords in KEYWORD_TYPES.items():
            if any(keyword in text for keyword in keywords):
                event_type = candidate_type
                break
        direction = -1 if re.search(r"loss|miss|ban|hack|跌|亏损|处罚|被盗", text) else 0
        if re.search(r"beat|growth|approval|record|增长|获批|创新高", text):
            direction = 1
        entities = list(item.symbols)
        return ExtractedEvent(
            event_type=event_type,
            entities=entities,
            direct_impact=item.summary[:400] or item.title,
            impact_direction=direction,
            novelty=0.4,
            priority=0.45,
            search_queries=entities,
        )
