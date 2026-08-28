from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from difflib import SequenceMatcher

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    ActionStage,
    AnalysisStep,
    CandidateAsset,
    EventAction,
    EventType,
    NewsEvent,
    NewsItem,
    SourceQuality,
    as_utc,
)
from backend.app.llm import LlmGateway, gateway
from backend.app.providers.registry import (
    ProviderRegistry,
    explicit_symbol_present,
    query_mentions_issuer,
)
from backend.app.services.source_lineage import enrich_news_lineage, normalize_text, source_group
from backend.app.storage import (
    event_news_item_ids,
    get_news_by_content_hash,
    list_events,
    save_event,
    save_news,
    upsert_asset,
)


class ExtractedEvent(BaseModel):
    event_type: EventType = EventType.OTHER
    entities: list[str] = Field(default_factory=list)
    direct_impact: str
    horizon_days: int = Field(default=90, ge=1, le=730)
    actions: list[EventAction] = Field(default_factory=list, max_length=3)
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
    _relationship_priority = {
        "direct": 4,
        "product_owner": 3,
        "issuer": 3,
        "cross_listing_issuer": 2,
        "entity": 1,
    }

    def __init__(
        self,
        registry: ProviderRegistry,
        settings: Settings | None = None,
        llm: LlmGateway | None = None,
    ) -> None:
        self.registry = registry
        self.settings = settings or get_settings()
        self.llm = llm or gateway

    def ingest(
        self,
        db: Session,
        items: list[NewsItem],
        progress: Callable[[int, int], None] | None = None,
    ) -> list[NewsEvent]:
        cluster_pool = list_events(db, limit=500)
        touched: dict[str, NewsEvent] = {}
        processed_ids = event_news_item_ids(db)
        total = len(items)
        for index, discovered_item in enumerate(items, start=1):
            item = enrich_news_lineage(discovered_item)
            stored_item = get_news_by_content_hash(db, item.content_hash)
            if stored_item is not None:
                if stored_item.id in processed_ids:
                    if progress:
                        progress(index, total)
                    continue
                item = stored_item
            elif not save_news(db, item):
                stored_item = get_news_by_content_hash(db, item.content_hash)
                if not stored_item or stored_item.id in processed_ids:
                    if progress:
                        progress(index, total)
                    continue
                item = stored_item
            if item.id in processed_ids:
                if progress:
                    progress(index, total)
                continue
            event = self.extract(item)
            for candidate in event.candidates:
                upsert_asset(db, candidate.asset)
            existing = next(
                (candidate for candidate in cluster_pool if self._same_story(candidate, event)),
                None,
            )
            if existing:
                existing.news_item_ids = list(
                    dict.fromkeys([*existing.news_item_ids, *event.news_item_ids])
                )
                existing.entities = list(dict.fromkeys([*existing.entities, *event.entities]))
                known_actions = {action.id for action in existing.actions}
                existing.actions.extend(
                    action for action in event.actions if action.id not in known_actions
                )
                existing.actions = existing.actions[:3]
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
                existing.published_at = min(
                    as_utc(existing.published_at), as_utc(event.published_at)
                )
                existing.observed_at = min(
                    as_utc(existing.observed_at), as_utc(event.observed_at)
                )
                existing.as_of = max(as_utc(existing.as_of), as_utc(event.as_of))
                if self._quality_rank(event.source_quality) > self._quality_rank(
                    existing.source_quality
                ):
                    existing.source_quality = event.source_quality
                self._record_cluster_step(existing, item)
                save_event(db, existing)
                touched[str(existing.id)] = existing
            else:
                self._record_cluster_step(event, item)
                cluster_pool.append(event)
                touched[str(event.id)] = event
                save_event(db, event)
            processed_ids.add(item.id)
            if progress:
                progress(index, total)
        return list(touched.values())

    def _same_story(self, left: NewsEvent, right: NewsEvent) -> bool:
        if left.event_type is not right.event_type:
            return False
        if abs(as_utc(left.published_at) - as_utc(right.published_at)) > timedelta(
            hours=self.settings.event_cluster_window_hours
        ):
            return False

        similarity = SequenceMatcher(
            None, normalize_text(left.headline), normalize_text(right.headline)
        ).ratio()
        left_asset = self._primary_asset(left)
        right_asset = self._primary_asset(right)
        left_entities = {normalize_text(item) for item in left.entities if normalize_text(item)}
        right_entities = {normalize_text(item) for item in right.entities if normalize_text(item)}
        if left_asset and right_asset:
            return self.registry.same_issuer(left_asset, right_asset) and similarity >= 0.58
        if left_asset or right_asset:
            return False
        return similarity >= 0.92 or (
            bool(left_entities & right_entities) and similarity >= 0.78
        )

    def _primary_asset(self, event: NewsEvent):
        if not event.candidates:
            return None
        primary = max(
            event.candidates,
            key=lambda candidate: (
                self._relationship_priority.get(candidate.relationship, 0),
                candidate.relevance,
                candidate.mapping_confidence,
                candidate.asset.asset_id,
            ),
        )
        return primary.asset

    @staticmethod
    def _record_cluster_step(event: NewsEvent, item: NewsItem) -> None:
        step = AnalysisStep(
            phase="story_clustering",
            executor="persistent-event-cluster:v1",
            summary=(
                f"事件簇 {event.id} 已持久化，当前包含 {len(event.news_item_ids)} 篇新闻，"
                f"最新来源血缘为 {source_group(item)}。"
            ),
            metrics={
                "cluster_id": str(event.id),
                "member_count": len(event.news_item_ids),
                "latest_source_group": source_group(item),
            },
        )
        for index in range(len(event.analysis_steps) - 1, -1, -1):
            if event.analysis_steps[index].phase == step.phase:
                event.analysis_steps[index] = step
                return
        event.analysis_steps.append(step)

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
            "只提取事实框架，不得输出全局影响方向、分数或评级。"
            "actions 逐项记录主体、动作、对象、范围、action_type 与 action_stage；"
            "谴责/表态属于 statement，威胁属于 threat，政策宣布属于 announced，"
            "正式生效属于 effective，已经发生的供应中断属于 realized。"
            "entities 必须保留新闻明确出现的公司、品牌和品牌产品名称，"
            "即使新闻没有给出证券代码。\n"
            f"标题：{item.title}\n摘要：{item.summary[:3000]}\n"
            f"来源：{item.source}\n已标注代码：{item.symbols}"
        )
        extraction_steps = [
            AnalysisStep(
                phase="news_collection",
                executor="provider",
                summary=f"已采集并归档来自 {item.source} 的新闻。",
                metrics={"source": item.source, "source_quality": item.source_quality.value},
                occurred_at=item.observed_at,
            )
        ]
        try:
            payload = self.llm.generate_json(
                model=self.settings.ollama_extract_model,
                lane="extract",
                system="你是谨慎的跨市场新闻结构化引擎。拒绝猜测，输出结构化事实。",
                prompt=prompt,
                schema=ExtractedEvent,
                temperature=0,
                operation="event_extraction",
                entity_type="news_item",
                entity_id=item.id,
            )
            extracted = ExtractedEvent.model_validate(payload)
            extraction_steps.append(
                AnalysisStep(
                    phase="event_extraction",
                    executor="ollama",
                    model=self.settings.ollama_extract_model,
                    summary=f"已整理为 {extracted.event_type.value} 事件并生成候选映射查询。",
                    metrics={"entities": len(extracted.entities)},
                )
            )
        except Exception as exc:
            extraction_steps.append(
                AnalysisStep(
                    phase="event_extraction",
                    status="failed",
                    executor="ollama",
                    model=self.settings.ollama_extract_model,
                    summary=f"事件提取模型不可用（{type(exc).__name__}），已切换规则回退。",
                )
            )
            extracted = self._fallback_extract(item)
            extraction_steps.append(
                AnalysisStep(
                    phase="event_extraction_fallback",
                    status="fallback",
                    executor="rules",
                    model="keyword-rules:v2",
                    summary=f"规则引擎已整理为 {extracted.event_type.value} 事件。",
                    metrics={"entities": len(extracted.entities)},
                )
            )

        queries = [item.title, *item.symbols, *extracted.entities, *extracted.search_queries]
        source_text = f"{item.title}\n{item.summary}"
        candidates: dict[str, CandidateAsset] = {}
        for query in queries:
            if not query.strip():
                continue
            for asset in self.registry.resolve_assets(query):
                direct_symbol = explicit_symbol_present(
                    source_text, asset.symbol
                ) or any(
                    explicit_symbol_present(value, asset.symbol, allow_bare=True)
                    for value in item.symbols
                )
                if not direct_symbol and not query_mentions_issuer(source_text, asset):
                    continue
                if direct_symbol:
                    relationship = "direct"
                    relevance = 0.95
                    mapping_confidence = 0.99
                    identity_basis = ["source_symbol", "provider_master"]
                elif asset.primary_listing_asset_id:
                    relationship = "cross_listing_issuer"
                    relevance = 0.55
                    mapping_confidence = 0.75
                    identity_basis = [
                        "issuer_name",
                        "provider_master",
                        "explicit_primary_listing",
                    ]
                else:
                    relationship = "issuer"
                    relevance = 0.70
                    mapping_confidence = 0.90
                    identity_basis = ["issuer_name", "provider_master"]
                candidates[asset.asset_id] = CandidateAsset(
                    asset=asset,
                    relationship=relationship,
                    relevance=relevance,
                    rationale=f"新闻中的 {query} 与 {asset.name} 匹配",
                    mapping_confidence=mapping_confidence,
                    identity_basis=identity_basis,
                )

        product_owner_count = 0
        for asset, product in self.registry.resolve_product_owners(source_text):
            candidate = CandidateAsset(
                asset=asset,
                relationship="product_owner",
                relevance=0.85,
                rationale=f"主数据确认新闻中的 {product} 归属于 {asset.name}",
                mapping_confidence=0.99,
                identity_basis=["source_product", "product_owner_master", product],
            )
            previous = candidates.get(asset.asset_id)
            if previous is None or candidate.relevance > previous.relevance:
                candidates[asset.asset_id] = candidate
                product_owner_count += 1

        quality_factor = {
            SourceQuality.OFFICIAL: 1.0,
            SourceQuality.PRIMARY: 0.9,
            SourceQuality.PROFESSIONAL: 0.8,
            SourceQuality.AGGREGATOR: 0.6,
            SourceQuality.SOCIAL: 0.3,
        }[item.source_quality]
        priority = min(1.0, extracted.priority * quality_factor)
        extraction_steps.append(
            AnalysisStep(
                phase="asset_mapping",
                status="completed" if candidates else "unmapped",
                executor="provider-registry",
                summary=(
                    f"确定性证券映射找到 {len(candidates)} 个候选。"
                    if candidates
                    else (
                        "确定性证券映射未找到可验证候选，将按配置进入 "
                        f"{self.settings.ollama_assist_model} 二次发现。"
                    )
                ),
                metrics={
                    "candidate_count": len(candidates),
                    "product_owner_count": product_owner_count,
                    "provider_errors": self.registry.mapping_errors,
                },
            )
        )
        return NewsEvent(
            news_item_ids=[item.id],
            headline=item.title,
            event_type=extracted.event_type,
            entities=extracted.entities,
            actions=extracted.actions,
            direct_impact=extracted.direct_impact,
            horizon_days=extracted.horizon_days,
            source_quality=item.source_quality,
            published_at=item.published_at,
            observed_at=item.observed_at,
            as_of=item.as_of,
            candidates=sorted(candidates.values(), key=lambda value: value.relevance, reverse=True),
            novelty=extracted.novelty,
            priority=priority,
            analysis_steps=extraction_steps,
        )

    @staticmethod
    def _fallback_extract(item: NewsItem) -> ExtractedEvent:
        text = f"{item.title} {item.summary}".lower()
        event_type = EventType.OTHER
        for candidate_type, keywords in KEYWORD_TYPES.items():
            if any(keyword in text for keyword in keywords):
                event_type = candidate_type
                break
        entities = list(item.symbols)
        return ExtractedEvent(
            event_type=event_type,
            entities=entities,
            direct_impact=item.summary[:400] or item.title,
            actions=EventService._fallback_actions(item),
            novelty=0.4,
            priority=0.45,
            search_queries=entities,
        )

    @staticmethod
    def _fallback_actions(item: NewsItem) -> list[EventAction]:
        source = f"{item.title} {item.summary}"
        text = source.casefold()
        actions: list[EventAction] = []

        if any(term in text for term in ("谴责", "抗议", "condemn", "等同于", "重申立场")):
            actions.append(
                EventAction(
                    actor="新闻所述表态方",
                    action_type="condemnation",
                    action_stage=ActionStage.STATEMENT,
                    action="公开谴责或表态",
                    object="新闻所述对象",
                    scope=source[:240],
                    strength=0.15,
                )
            )
        if any(term in text for term in ("制裁", "sanction")):
            effective = any(
                term in text for term in ("正式生效", "开始执行", "已实施", "takes effect")
            )
            actions.append(
                EventAction(
                    actor="制裁实施方",
                    action_type="sanctions",
                    action_stage=ActionStage.EFFECTIVE if effective else ActionStage.ANNOUNCED,
                    action="实施或宣布制裁",
                    object="受制裁方",
                    scope=source[:240],
                    strength=0.75 if effective else 0.55,
                )
            )
        if any(term in text for term in ("威胁关闭", "警告关闭", "threaten to close")):
            actions.append(
                EventAction(
                    actor="新闻所述威胁方",
                    action_type="strait_closure",
                    action_stage=ActionStage.THREAT,
                    action="威胁关闭航道",
                    object="霍尔木兹海峡",
                    scope=source[:240],
                    strength=0.35,
                )
            )
        elif any(term in text for term in ("海峡关闭", "航道中断", "strait closed")):
            actions.append(
                EventAction(
                    actor="新闻所述行为方",
                    action_type="strait_closure",
                    action_stage=ActionStage.REALIZED,
                    action="航道已经关闭或中断",
                    object="霍尔木兹海峡",
                    scope=source[:240],
                    strength=0.90,
                )
            )
        if any(term in text for term in ("恢复谈判", "恢复通航", "重新开放", "resume talks")):
            actions.append(
                EventAction(
                    actor="新闻所述参与方",
                    action_type="deescalation",
                    action_stage=ActionStage.REALIZED,
                    action="恢复谈判或通航",
                    object="相关谈判或航道",
                    scope=source[:240],
                    strength=0.85,
                )
            )
        if not actions:
            actions.append(
                EventAction(
                    actor="新闻所述主体",
                    action_type="unknown",
                    action_stage=ActionStage.UNKNOWN,
                    action=item.title[:160],
                    scope=item.summary[:240],
                    strength=0.10,
                )
            )
        return actions[:3]
