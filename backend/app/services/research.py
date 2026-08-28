from __future__ import annotations

import json
import re
import threading
from collections.abc import Sequence
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Literal, TypedDict
from uuid import UUID, uuid4

from celery.exceptions import SoftTimeLimitExceeded
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AnalysisStep,
    AssetClass,
    AssetRef,
    ClaimEvidenceAssessment,
    ClaimVerdict,
    ConfidenceFactors,
    Evidence,
    HorizonUnit,
    ImpactFactors,
    ModelDirection,
    NewsEvent,
    NewsItem,
    Rating,
    Recommendation,
    ResearchRun,
    RunStatus,
    ScoringFactor,
    SignalStatus,
    SourceQuality,
    TargetConfidenceFactors,
    TargetImpact,
    TargetType,
    Thesis,
    TradeStatus,
    TransmissionFactors,
    as_utc,
    model_direction_for,
    model_rating_for,
    rating_for,
    utc_now,
)
from backend.app.llm import LlmGateway, gateway
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.directional_scoring import (
    blocked_probabilities,
    probabilities_for_score,
    rating_confidence_score,
    round_half_up,
    short_term_impact_score,
)
from backend.app.services.macro_impacts import (
    TARGET_SCORING_VERSION,
    EventImpactDraft,
    TargetImpactDraft,
    fact_confidence_for,
    finalize_impacts,
)
from backend.app.services.mcp_registry import SearchRequest, search_enabled_sources_sync
from backend.app.services.prompt_budget import compact_evidence, compact_json_records
from backend.app.services.research_factors import (
    FactorSource,
    build_research_factor_evidence,
)
from backend.app.services.research_lifecycle import mark_asset_research_timed_out
from backend.app.services.retrieval import RetrievalService
from backend.app.services.source_lineage import (
    canonicalize_url,
    enrich_news_lineage,
    independent_evidence_groups,
    normalize_text,
    source_group,
)
from backend.app.storage import (
    get_event_research_for_event,
    get_evidence,
    get_news,
    get_news_by_content_hash,
    get_run,
    list_news,
    save_event,
    save_news,
    save_recommendation,
    save_run,
)

_checkpoint_setup_lock = threading.Lock()
_checkpoint_setup_done = False
SOURCE_GATE = "one official source or two independent sources"
INSUFFICIENT_EVIDENCE_MARKER = "现有证据不足"
DIRECTION_SCORE_INSTRUCTION = (
    "方向只能针对当前研究工具，写入 target_impact；禁止输出全局方向、全局分数或全局评级。"
    "target_impact 必须包含 direction、六个传导因子、四个方向置信因子、传导路径、理由、"
    "证据和关键缺失信息。最终 score/rating/confidence/trade_status 全部由程序计算。"
)


class DraftOutput(BaseModel):
    summary: str
    historical_context: str = ""
    financials_and_growth: str = ""
    products_or_protocol: str = ""
    competition: str = ""
    valuation_or_tokenomics: str = ""
    catalysts: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    invalidation_conditions: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    score: int = Field(default=0, ge=-100, le=100)
    direction: ModelDirection | None = None
    rating: Rating | None = None
    confidence: float = Field(default=0.3, ge=0, le=1)
    bull_probability: float = Field(default=0.33, ge=0, le=1)
    base_probability: float = Field(default=0.34, ge=0, le=1)
    bear_probability: float = Field(default=0.33, ge=0, le=1)
    impact_factors: ImpactFactors = Field(default_factory=ImpactFactors)
    confidence_factors: ConfidenceFactors = Field(default_factory=ConfidenceFactors)
    target_impact: TargetImpactDraft | None = None
    valuation_low: float | None = None
    valuation_high: float | None = None

    @model_validator(mode="after")
    def normalize_model_opinion(self) -> DraftOutput:
        # Score is the single source of truth. Normalizing these redundant
        # labels prevents a small model from returning contradictory fields.
        if self.target_impact is not None:
            self.direction = {
                1: ModelDirection.BULLISH,
                0: ModelDirection.NEUTRAL,
                -1: ModelDirection.BEARISH,
            }[self.target_impact.direction]
            self.rating = None
            return self
        self.direction = model_direction_for(self.score)
        self.rating = model_rating_for(self.score)
        self.impact_factors.direction = {
            ModelDirection.BULLISH: 1,
            ModelDirection.NEUTRAL: 0,
            ModelDirection.BEARISH: -1,
        }[self.direction]
        return self


class DraftRepairOutput(BaseModel):
    """Sparse second-pass patch; omitted fields retain the first draft."""

    summary: str | None = None
    historical_context: str | None = None
    financials_and_growth: str | None = None
    products_or_protocol: str | None = None
    competition: str | None = None
    valuation_or_tokenomics: str | None = None
    catalysts: list[str] | None = None
    risks: list[str] | None = None
    invalidation_conditions: list[str] | None = None
    evidence_ids: list[str] | None = None
    target_impact: TargetImpactDraft | None = None
    score: int | None = Field(default=None, ge=-100, le=100)
    confidence: float | None = Field(default=None, ge=0, le=1)
    bull_probability: float | None = Field(default=None, ge=0, le=1)
    base_probability: float | None = Field(default=None, ge=0, le=1)
    bear_probability: float | None = Field(default=None, ge=0, le=1)
    valuation_low: float | None = None
    valuation_high: float | None = None


class VerificationOutput(BaseModel):
    # Historical name retained for API compatibility: this is the structural
    # source/citation gate, not proof of a directional conclusion.
    evidence_complete: bool
    semantic_evidence_complete: bool = False
    direction_supported: bool = False
    semantic_status: Literal["not_run", "completed", "failed"] = "not_run"
    evidence_strength: float = Field(default=0, ge=0, le=1)
    claim_assessments: list[ClaimEvidenceAssessment] = Field(default_factory=list)
    missing_requirements: list[str] = Field(default_factory=list)
    contradictions: list[str] = Field(default_factory=list)
    unsupported_claims: list[str] = Field(default_factory=list)


class SemanticClaimOutput(BaseModel):
    claim: str
    claim_kind: Literal[
        "summary",
        "historical_context",
        "financials_and_growth",
        "products_or_protocol",
        "competition",
        "valuation_or_tokenomics",
        "catalyst",
        "risk",
        "invalidation_condition",
    ]
    stance: int = Field(default=0, ge=-1, le=1)
    verdict: ClaimVerdict
    evidence_ids: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0, ge=0, le=1)
    reason: str = ""


class SemanticVerificationOutput(BaseModel):
    claims: list[SemanticClaimOutput] = Field(default_factory=list)
    direction_supported: bool = False
    contradictions: list[str] = Field(default_factory=list)


class CloudVerification(BaseModel):
    approved: bool = False
    confidence: float = Field(default=0, ge=0, le=1)
    contradictions: list[str] = Field(default_factory=list)


class ResearchState(TypedDict, total=False):
    run: dict[str, Any]
    event: dict[str, Any] | None
    events: list[dict[str, Any]]
    research_data: dict[str, Any]
    factor_summary: dict[str, Any]
    evidence: list[dict[str, Any]]
    retrieved_context: list[dict[str, Any]]
    draft: dict[str, Any]
    verification: dict[str, Any]
    verification_round: int
    acquisition_attempts: int
    acquired_evidence_count: int
    historical_replay: bool


class ResearchService:
    def __init__(
        self,
        registry: ProviderRegistry,
        db: Session,
        settings: Settings | None = None,
        llm: LlmGateway | None = None,
    ) -> None:
        self.registry = registry
        self.db = db
        self.settings = settings or get_settings()
        self.llm = llm or gateway
        self._checkpointer_context = None
        self.checkpointer = self._make_checkpointer()
        self.graph = self._build_graph()

    def _make_checkpointer(self):
        global _checkpoint_setup_done
        if not self.settings.database_url.startswith("postgresql"):
            return InMemorySaver()
        try:
            from langgraph.checkpoint.postgres import PostgresSaver

            uri = self.settings.database_url.replace("postgresql+psycopg://", "postgresql://")
            self._checkpointer_context = PostgresSaver.from_conn_string(uri)
            saver = self._checkpointer_context.__enter__()
            with _checkpoint_setup_lock:
                if not _checkpoint_setup_done:
                    saver.setup()
                    _checkpoint_setup_done = True
            return saver
        except SoftTimeLimitExceeded:
            raise
        except Exception:
            self._close_checkpointer()
            return InMemorySaver()

    def _close_checkpointer(self) -> None:
        if self._checkpointer_context:
            self._checkpointer_context.__exit__(None, None, None)
            self._checkpointer_context = None

    def _build_graph(self):
        graph = StateGraph(ResearchState)
        graph.add_node("gather", self._gather)
        graph.add_node("draft", self._draft)
        graph.add_node("verify", self._verify)
        graph.add_node("finalize", self._finalize)
        graph.add_edge(START, "gather")
        graph.add_edge("gather", "draft")
        graph.add_edge("draft", "verify")
        graph.add_edge("verify", "finalize")
        graph.add_edge("finalize", END)
        return graph.compile(checkpointer=self.checkpointer)

    def run(
        self,
        asset: AssetRef,
        event: NewsEvent | None = None,
        as_of: datetime | None = None,
        historical_replay: bool | None = None,
        queued_run: ResearchRun | None = None,
        events: list[NewsEvent] | None = None,
    ) -> ResearchRun:
        trigger_events = events or ([event] if event else [])
        primary_event = event or (trigger_events[0] if trigger_events else None)
        run = queued_run or ResearchRun(
            event_id=primary_event.id if primary_event else None,
            trigger_event_ids=[item.id for item in trigger_events],
            asset=asset,
            as_of=as_of or utc_now(),
            historical_replay=bool(historical_replay),
            analysis_steps=[
                *(primary_event.analysis_steps if primary_event else []),
                AnalysisStep(
                    phase="research_queue",
                    executor="celery",
                    summary=f"已为 {asset.symbol} 创建研究任务。",
                ),
            ],
        )
        if historical_replay is not None:
            run.historical_replay = historical_replay
        if trigger_events:
            run.trigger_event_ids = list(dict.fromkeys([*run.trigger_event_ids, *(item.id for item in trigger_events)]))
        if run.started_at is None:
            run.started_at = utc_now()
        run.status = RunStatus.RUNNING
        save_run(self.db, run)
        state: ResearchState = {
            "run": run.model_dump(mode="json"),
            "event": primary_event.model_dump(mode="json") if primary_event else None,
            "events": [item.model_dump(mode="json") for item in trigger_events],
            "verification_round": 0,
            "acquisition_attempts": 0,
            "acquired_evidence_count": 0,
            "historical_replay": run.historical_replay,
        }
        try:
            final_state = self.graph.invoke(
                state,
                config={"configurable": {"thread_id": str(run.id)}},
                durability="sync",
            )
            return ResearchRun.model_validate(final_state["run"])
        except SoftTimeLimitExceeded:
            self.db.rollback()
            failed = get_run(self.db, run.id) or ResearchRun.model_validate(state["run"])
            mark_asset_research_timed_out(
                self.db,
                failed,
                self.settings,
                limit_kind="soft",
            )
            raise
        except Exception as exc:
            self.db.rollback()
            failed = get_run(self.db, run.id) or ResearchRun.model_validate(state["run"])
            failed.status = RunStatus.FAILED
            failed.error = f"{type(exc).__name__}: {exc}"
            failed.completed_at = utc_now()
            failed.analysis_steps.append(
                AnalysisStep(
                    phase="research_failed",
                    status="failed",
                    executor="research-graph",
                    summary=f"研究任务在 {type(exc).__name__} 后停止，请查看服务日志。",
                )
            )
            save_run(self.db, failed)
            return failed
        finally:
            self._close_checkpointer()

    def _gather(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        run.status = RunStatus.RUNNING
        run.updated_at = utc_now()
        # Live providers expose today's view and may contain later restatements. A historical
        # replay may only use evidence already persisted before its boundary.
        research_data = (
            {} if state.get("historical_replay") else self.registry.get_research_data(run.asset)
        )
        evidence = self._build_evidence(
            run,
            state.get("event"),
            research_data,
            state.get("events"),
        )
        events = [NewsEvent.model_validate(item) for item in state.get("events", [])]
        if not events and state.get("event"):
            events = [NewsEvent.model_validate(state["event"])]
        primary_event = next(
            (item for item in events if item.id == run.event_id),
            events[0] if events else None,
        )
        factor_sources: dict[str, Any] = dict(research_data.get("factor_sources", {}))
        event_evidence = [
            item
            for item in evidence
            if primary_event and item.claim == primary_event.headline
        ]
        if event_evidence and "event" not in factor_sources:
            source = max(event_evidence, key=lambda item: self._source_weight(item.source_quality))
            factor_sources["event"] = FactorSource(
                name=source.source_name,
                url=source.source_url,
                quality=source.source_quality,
                independent_group=source.independent_group,
                published_at=source.published_at,
            )
        factor_result = build_research_factor_evidence(
            run_id=run.id,
            asset=run.asset,
            as_of=run.as_of,
            event_at=primary_event.published_at if primary_event else None,
            horizon_days=primary_event.horizon_days if primary_event else None,
            event_type=primary_event.event_type.value if primary_event else None,
            event_texts=(
                [f"{primary_event.headline} {primary_event.direct_impact}"]
                if primary_event
                else []
            ),
            event_details=research_data.get("event_details", {}),
            fundamentals=research_data.get("fundamentals", {}),
            expectations=research_data.get("expectations", {}),
            expectations_at=research_data.get("expectations_at"),
            asset_prices=research_data.get("prices", []),
            benchmark_prices=research_data.get("benchmark_prices", []),
            industry_prices=research_data.get("industry_prices", []),
            sources=factor_sources,
        )
        evidence.extend(factor_result.evidence)
        factor_summary = {
            "signal": factor_result.aggregate_signal,
            "reliability": factor_result.reliability,
            "event_families": factor_result.event_families,
            "categories": factor_result.category_signals,
            "category_reliability": factor_result.category_reliability,
            "missing": factor_result.missing_requirements,
            "factors": [item.model_dump(mode="json") for item in factor_result.factors],
        }
        run.evidence = evidence
        save_run(self.db, run)
        event = NewsEvent.model_validate(state["event"]) if state.get("event") else None
        headlines = " ".join(
            NewsEvent.model_validate(item).headline for item in state.get("events", [])
        )
        query = f"{run.asset.name} {run.asset.symbol} {headlines or (event.headline if event else '')}"
        retrieved_context: list[dict[str, Any]] = []
        retrieval_error: str | None = None
        try:
            retrieval = RetrievalService(self.db, self.settings)
            retrieval.index(run.asset.asset_id, evidence)
            retrieved_context = retrieval.search(
                query, asset_id=run.asset.asset_id, as_of=run.as_of
            )
            if state.get("historical_replay"):
                current_ids = {item.id for item in evidence}
                seen_facts = {(item.source_url, item.claim) for item in evidence}
                for result in retrieved_context:
                    evidence_id = UUID(result["evidence_id"])
                    if evidence_id in current_ids:
                        continue
                    stored = get_evidence(self.db, evidence_id)
                    if not stored or (stored.source_url, stored.claim) in seen_facts:
                        continue
                    cloned = stored.model_copy(update={"id": uuid4(), "run_id": run.id})
                    evidence.append(cloned)
                    seen_facts.add((cloned.source_url, cloned.claim))
                if len(evidence) != len(current_ids):
                    run.evidence = evidence
                    save_run(self.db, run)
                    retrieval.index(run.asset.asset_id, evidence)
        except SoftTimeLimitExceeded:
            raise
        except Exception as exc:
            # Retrieval failure must not discard already collected structured evidence.
            retrieval_error = type(exc).__name__
        independent_sources = {
            item.independent_group for item in evidence if item.independent_group
        }
        run.analysis_steps.append(
            AnalysisStep(
                phase="evidence_gathering",
                status="fallback" if retrieval_error else "completed",
                executor="providers+retrieval",
                summary=(
                    f"已收集 {len(evidence)} 条证据，来自 {len(independent_sources)} 个独立来源。"
                    + (
                        f" 混合检索不可用（{retrieval_error}），保留结构化证据。"
                        if retrieval_error
                        else ""
                    )
                ),
                metrics={
                    "evidence_count": len(evidence),
                    "independent_sources": len(independent_sources),
                    "retrieved_context_count": len(retrieved_context),
                    "provider_groups": sorted(research_data.keys()),
                    "research_factor_count": len(factor_result.factors),
                    "research_factor_reliability": factor_result.reliability,
                    "research_factor_missing": factor_result.missing_requirements,
                },
            )
        )
        save_run(self.db, run)
        return {
            "run": run.model_dump(mode="json"),
            "research_data": research_data,
            "factor_summary": factor_summary,
            "evidence": [item.model_dump(mode="json") for item in evidence],
            "retrieved_context": retrieved_context,
        }

    def _build_evidence(
        self,
        run: ResearchRun,
        event_payload: dict[str, Any] | None,
        data: dict[str, Any],
        event_payloads: list[dict[str, Any]] | None = None,
    ) -> list[Evidence]:
        evidence: list[Evidence] = []
        events = [NewsEvent.model_validate(item) for item in (event_payloads or [])]
        if not events and event_payload:
            events = [NewsEvent.model_validate(event_payload)]
        seen_news_ids: set[UUID] = set()
        for event in events:
            for item_id in event.news_item_ids:
                if item_id in seen_news_ids:
                    continue
                seen_news_ids.add(item_id)
                item = get_news(self.db, item_id)
                if not item or any(
                    value > run.as_of for value in (item.published_at, item.observed_at, item.as_of)
                ):
                    continue
                evidence.append(self._news_evidence(run, item))

        if run.asset.asset_class is AssetClass.EQUITY:
            fundamentals = data.get("fundamentals", {})
            statements = fundamentals.get("income", [])
            if isinstance(statements, dict):
                statements = statements.get("data", [])
            for statement in (statements or [])[:5]:
                date_value = statement.get("date") or statement.get("fillingDate")
                try:
                    published = datetime.fromisoformat(str(date_value)).replace(
                        tzinfo=run.as_of.tzinfo
                    )
                except SoftTimeLimitExceeded:
                    raise
                except Exception:
                    published = run.as_of
                if published > run.as_of:
                    continue
                revenue = statement.get("revenue")
                net_income = statement.get("netIncome")
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=f"{statement.get('calendarYear', date_value)} revenue={revenue}, netIncome={net_income}",
                        source_name="FMP standardized financials",
                        source_url=f"https://financialmodelingprep.com/stable/income-statement?symbol={run.asset.symbol}",
                        source_quality=SourceQuality.AGGREGATOR,
                        published_at=published,
                        observed_at=run.as_of,
                        as_of=run.as_of,
                        excerpt=json.dumps(
                            {"revenue": revenue, "netIncome": net_income}, ensure_ascii=False
                        ),
                        independent_group="fmp",
                        numeric_value=float(revenue) if isinstance(revenue, int | float) else None,
                        numeric_unit=run.asset.currency,
                    )
                )
            if run.asset.market.value == "CN":
                evidence.extend(self._a_share_fundamental_evidence(run, fundamentals))
            filings = data.get("filings", [])
            for filing in filings[:10]:
                url = filing.get("finalLink") or filing.get("link")
                date_value = (
                    filing.get("acceptedDate") or filing.get("fillingDate") or filing.get("date")
                )
                if not url or not date_value:
                    continue
                try:
                    published = datetime.fromisoformat(str(date_value).replace("Z", "+00:00"))
                except SoftTimeLimitExceeded:
                    raise
                except Exception:
                    published = run.as_of
                published = as_utc(published)
                if published > run.as_of:
                    continue
                official_domain = any(
                    domain in url.lower() for domain in ("sec.gov", "cninfo.com.cn", "hkexnews.hk")
                )
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=f"SEC filing {filing.get('formType') or filing.get('type')}",
                        source_name=str(filing.get("source") or "Regulatory filing"),
                        source_url=url,
                        source_quality=(
                            SourceQuality.OFFICIAL if official_domain else SourceQuality.AGGREGATOR
                        ),
                        published_at=published,
                        observed_at=run.as_of,
                        as_of=run.as_of,
                        excerpt=str(filing.get("formType") or filing.get("type") or "filing"),
                        independent_group=(
                            "official-filing" if official_domain else "filing-aggregator"
                        ),
                    )
                )
        else:
            metrics = data.get("crypto_metrics", {})
            if metrics:
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=f"Current public crypto metrics for {run.asset.name}",
                        source_name="CoinGecko/DefiLlama",
                        source_url=f"https://www.coingecko.com/en/coins/{run.asset.asset_id.rsplit(':', 1)[-1]}",
                        source_quality=SourceQuality.AGGREGATOR,
                        published_at=run.as_of,
                        observed_at=run.as_of,
                        as_of=run.as_of,
                        excerpt=json.dumps(metrics, ensure_ascii=False, default=str)[:1000],
                        independent_group="coingecko+defillama",
                    )
                )
        return evidence

    @staticmethod
    def _a_share_source_code(asset: AssetRef) -> str:
        prefix = {
            "XSHG": "SH",
            "XSHE": "SZ",
            "XBEI": "BJ",
        }.get(asset.exchange_or_provider, "SH" if asset.symbol.startswith("6") else "SZ")
        return f"{prefix}{asset.symbol}"

    @staticmethod
    def _structured_date(value: Any, fallback: datetime) -> datetime:
        if not value:
            return fallback
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return as_utc(parsed)
        except (TypeError, ValueError):
            return fallback

    def _a_share_fundamental_evidence(
        self, run: ResearchRun, fundamentals: dict[str, Any]
    ) -> list[Evidence]:
        """Project current AkShare business, financial and valuation snapshots into evidence."""

        evidence: list[Evidence] = []
        business = fundamentals.get("business_profile")
        if isinstance(business, dict) and business:
            fields = {
                key: business.get(key)
                for key in ("主营业务", "产品类型", "产品名称", "经营范围")
                if business.get(key) not in (None, "")
            }
            if fields:
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=f"{run.asset.name}主营业务与产品概况",
                        source_name="同花顺主营介绍/AkShare",
                        source_url=(
                            f"https://basic.10jqka.com.cn/new/{run.asset.symbol}/operate.html"
                        ),
                        source_quality=SourceQuality.AGGREGATOR,
                        published_at=run.as_of,
                        observed_at=run.as_of,
                        as_of=run.as_of,
                        excerpt=json.dumps(fields, ensure_ascii=False, default=str)[:1000],
                        independent_group="ths-akshare",
                    )
                )

        composition = fundamentals.get("business_composition")
        if isinstance(composition, list) and composition:
            latest_date = max(
                (str(item.get("报告日期") or "") for item in composition if isinstance(item, dict)),
                default="",
            )
            latest_rows = [
                item
                for item in composition
                if isinstance(item, dict)
                and (not latest_date or str(item.get("报告日期") or "") == latest_date)
            ][:12]
            if latest_rows:
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=f"{run.asset.name} {latest_date or '最新'}主营构成",
                        source_name="东方财富主营构成/AkShare",
                        source_url=(
                            "https://emweb.securities.eastmoney.com/PC_HSF10/"
                            f"BusinessAnalysis/Index?type=web&code={self._a_share_source_code(run.asset)}"
                        ),
                        source_quality=SourceQuality.AGGREGATOR,
                        published_at=self._structured_date(latest_date, run.as_of),
                        observed_at=run.as_of,
                        as_of=run.as_of,
                        excerpt=json.dumps(latest_rows, ensure_ascii=False, default=str)[:1000],
                        independent_group="eastmoney-akshare",
                    )
                )

        financials = fundamentals.get("financial_indicators")
        if isinstance(financials, list):
            metric_keys = (
                "REPORT_DATE_NAME",
                "TOTALOPERATEREVE",
                "PARENTNETPROFIT",
                "KCFJCXSYJLR",
                "TOTALOPERATEREVETZ",
                "PARENTNETPROFITTZ",
                "ROEJQ",
                "XSMLL",
                "JYXJLYYSR",
                "ZCFZL",
            )
            for statement in (item for item in financials[:4] if isinstance(item, dict)):
                snapshot = {
                    key: statement.get(key)
                    for key in metric_keys
                    if statement.get(key) is not None
                }
                if not snapshot:
                    continue
                date_value = statement.get("REPORT_DATE") or statement.get("REPORT_DATE_NAME")
                report_name = statement.get("REPORT_DATE_NAME") or date_value or "最新报告期"
                revenue = statement.get("TOTALOPERATEREVE")
                net_income = statement.get("PARENTNETPROFIT")
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=(
                            f"{run.asset.name} {report_name}财务指标："
                            f"营业收入={revenue}，归母净利润={net_income}"
                        ),
                        source_name="东方财富财务指标/AkShare",
                        source_url=(
                            "https://emweb.securities.eastmoney.com/pc_hsf10/pages/"
                            f"index.html?type=web&code={self._a_share_source_code(run.asset)}#/cwfx"
                        ),
                        source_quality=SourceQuality.AGGREGATOR,
                        published_at=self._structured_date(date_value, run.as_of),
                        observed_at=run.as_of,
                        as_of=run.as_of,
                        excerpt=json.dumps(snapshot, ensure_ascii=False, default=str)[:1000],
                        independent_group="eastmoney-akshare",
                        numeric_value=float(revenue)
                        if isinstance(revenue, int | float)
                        else None,
                        numeric_unit=run.asset.currency,
                    )
                )

        valuation = fundamentals.get("valuation")
        company_info = fundamentals.get("company_info")
        if isinstance(valuation, list) and valuation:
            latest = next(
                (item for item in reversed(valuation) if isinstance(item, dict)),
                None,
            )
            if latest:
                snapshot = {
                    key: latest.get(key)
                    for key in (
                        "数据日期",
                        "当日收盘价",
                        "总市值",
                        "流通市值",
                        "PE(TTM)",
                        "PE(静)",
                        "市净率",
                        "PEG值",
                        "市现率",
                        "市销率",
                    )
                    if latest.get(key) is not None
                }
                if isinstance(company_info, dict):
                    snapshot.update(
                        {
                            key: company_info.get(key)
                            for key in ("行业", "总市值", "流通市值")
                            if company_info.get(key) is not None and key not in snapshot
                        }
                    )
                date_value = latest.get("数据日期")
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=(
                            f"{run.asset.name} {date_value or '最新'}估值："
                            f"PE(TTM)={latest.get('PE(TTM)')}，"
                            f"PB={latest.get('市净率')}，PS={latest.get('市销率')}"
                        ),
                        source_name="东方财富个股估值/AkShare",
                        source_url=f"https://data.eastmoney.com/gzfx/detail/{run.asset.symbol}.html",
                        source_quality=SourceQuality.AGGREGATOR,
                        published_at=self._structured_date(date_value, run.as_of),
                        observed_at=run.as_of,
                        as_of=run.as_of,
                        excerpt=json.dumps(snapshot, ensure_ascii=False, default=str)[:1000],
                        independent_group="eastmoney-akshare",
                        numeric_value=(
                            float(latest["PE(TTM)"])
                            if isinstance(latest.get("PE(TTM)"), int | float)
                            else None
                        ),
                        numeric_unit="multiple",
                    )
                )
        return evidence

    @staticmethod
    def _news_evidence(run: ResearchRun, item: NewsItem) -> Evidence:
        return Evidence(
            run_id=run.id,
            claim=item.title,
            source_name=item.source,
            source_url=item.url,
            source_quality=item.source_quality,
            published_at=item.published_at,
            observed_at=item.observed_at,
            as_of=item.as_of,
            excerpt=(item.summary or item.title)[:1000],
            independent_group=source_group(item),
        )

    @staticmethod
    def _event_prompt_context(
        event_payload: dict[str, Any] | None,
        asset_id: str | None = None,
    ) -> dict[str, Any]:
        if not event_payload:
            return {}
        event = NewsEvent.model_validate(event_payload)
        return {
            "headline": event.headline,
            "event_type": event.event_type.value,
            "direct_impact": event.direct_impact,
            "actions": [item.model_dump(mode="json") for item in event.actions],
            "horizon_days": event.horizon_days,
            "published_at": event.published_at.isoformat(),
            "candidates": [
                {
                    "asset_id": item.asset.asset_id,
                    "relationship": item.relationship,
                    "relevance": item.relevance,
                    "mapping_confidence": item.mapping_confidence,
                    "rationale": item.rationale,
                }
                for item in event.candidates
                if asset_id is None or item.asset.asset_id == asset_id
            ],
        }

    def _draft(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        evidence = state.get("evidence", [])
        asset_template = {
            AssetClass.EQUITY: "股票：历史同类事件、营收增长、利润现金流、产品、竞争、估值、催化和风险。",
            AssetClass.CRYPTO: "加密资产：供给解锁、链上活跃、协议费用、TVL、开发治理、流动性、安全监管。",
            AssetClass.COMMODITY: "商品连续基准：供需、库存、运输、期限结构、政策与风险溢价。",
            AssetClass.FX: "现货外汇：增长、利差、政策、资本流动、避险与支付传导。",
        }[run.asset.asset_class]
        asset_context = {
            "asset_id": run.asset.asset_id,
            "asset_class": run.asset.asset_class.value,
            "market": run.asset.market,
            "symbol": run.asset.symbol,
            "name": run.asset.name,
            "currency": run.asset.currency,
            "products": run.asset.products[:5],
            "competitors": run.asset.competitors[:5],
        }
        prompt = (
            f"研究对象：{json.dumps(asset_context, ensure_ascii=False)}\n"
            f"研究截止：{run.as_of.isoformat()}\n"
            "触发事件："
            f"{json.dumps(self._event_prompt_context(state.get('event'), run.asset.asset_id), ensure_ascii=False)}\n"
            f"程序研究因子：{json.dumps(state.get('factor_summary', {}), ensure_ascii=False, default=str)}\n"
            f"报告模板：{asset_template}\n"
            "证据："
            f"{compact_evidence(evidence, self.settings.research_prompt_evidence_chars, max_per_group=2)}\n"
            "混合检索上下文："
            f"{compact_json_records(state.get('retrieved_context', []), self.settings.research_prompt_context_chars)}\n"
            "只能引用 evidence_ids 中存在的证据。无法判断的因子填 0 并降低置信度，不得编造；"
            "当前工具就是唯一评级目标；target_impact.asset_id 必须等于研究对象 asset_id。"
            "区分事件事实、市场反应和收益方向；不要把公告完整误写成方向证据充分。"
        )
        draft_error: str | None = None
        try:
            draft = self.llm.generate_json(
                model=self.settings.ollama_research_model,
                lane="research",
                system=(
                    "你是证据优先的投资研究员。区分事实、推断和未知，不给实盘指令。"
                    f"{DIRECTION_SCORE_INSTRUCTION}"
                ),
                prompt=prompt,
                schema=DraftOutput,
                operation="report_drafting",
                entity_type="research_run",
                entity_id=run.id,
            )
        except SoftTimeLimitExceeded:
            raise
        except Exception as exc:
            draft_error = type(exc).__name__
            run.retryable_reason = f"model_{type(exc).__name__}"
            draft = self._fallback_draft(run, evidence).model_dump(mode="json")
        draft_output = DraftOutput.model_validate(draft)
        run.analysis_steps.append(
            AnalysisStep(
                phase="report_drafting",
                status="fallback" if draft_error else "completed",
                executor="ollama" if not draft_error else "rules",
                model=self.settings.ollama_research_model
                if not draft_error
                else "research-fallback:v1",
                summary=(
                    f"已生成研究草稿，初始置信度 {draft_output.confidence:.0%}，引用 "
                    f"{len(draft_output.evidence_ids)} 条证据。"
                    + (f" 模型不可用（{draft_error}），当前为保守回退结果。" if draft_error else "")
                ),
                metrics={
                    "confidence": draft_output.confidence,
                    "score": draft_output.score,
                    "direction": draft_output.direction.value,
                    "rating": draft_output.rating.value,
                    "citation_count": len(draft_output.evidence_ids),
                },
            )
        )
        save_run(self.db, run)
        return {"run": run.model_dump(mode="json"), "draft": draft_output.model_dump(mode="json")}

    @staticmethod
    def _fallback_draft(run: ResearchRun, evidence: list[dict[str, Any]]) -> DraftOutput:
        ids = [item["id"] for item in evidence]
        unavailable = ScoringFactor(
            value=0,
            reason="研究模型不可用，无法评估该因子。",
            evidence_ids=[],
        )
        return DraftOutput(
            summary=f"已收集 {len(evidence)} 条与 {run.asset.name} 相关的证据，但本地研究模型不可用或证据不足。",
            catalysts=[],
            risks=["模型综合分析尚未完成", "当前证据可能不完整"],
            invalidation_conditions=["补充官方披露后重新评估"],
            evidence_ids=ids,
            score=0,
            confidence=min(0.45, len(evidence) * 0.1),
            impact_factors=ImpactFactors(
                direction=0,
                magnitude=unavailable,
                persistence=unavailable,
                representativeness=unavailable,
                market_confirmation=unavailable,
            ),
            confidence_factors=ConfidenceFactors(
                direction_clarity=unavailable,
                source_reliability=unavailable,
                magnitude_certainty=unavailable,
                market_context_completeness=unavailable,
            ),
        )

    @staticmethod
    def _claim_specs(draft: DraftOutput) -> list[tuple[str, str]]:
        claims: list[tuple[str, str]] = [
            ("summary", draft.summary),
            ("historical_context", draft.historical_context),
            ("financials_and_growth", draft.financials_and_growth),
            ("products_or_protocol", draft.products_or_protocol),
            ("competition", draft.competition),
            ("valuation_or_tokenomics", draft.valuation_or_tokenomics),
        ]
        claims.extend(("catalyst", item) for item in draft.catalysts)
        claims.extend(("risk", item) for item in draft.risks)
        claims.extend(
            ("invalidation_condition", item) for item in draft.invalidation_conditions
        )
        return [(kind, claim.strip()) for kind, claim in claims if claim.strip()]

    @staticmethod
    def _claim_key(kind: str, claim: str) -> tuple[str, str]:
        return (kind, normalize_text(claim))

    @staticmethod
    def _mapping_inputs(
        run: ResearchRun, event_payload: dict[str, Any] | None
    ) -> tuple[int | None, float, float]:
        if not event_payload:
            return None, 0.0, 1.0
        event = NewsEvent.model_validate(event_payload)
        candidates = [
            item for item in event.candidates if item.asset.asset_id == run.asset.asset_id
        ]
        if not candidates:
            return None, 0.0, 0.0
        candidate = max(candidates, key=lambda item: item.relevance)
        confidence = min(candidate.relevance, candidate.mapping_confidence)
        return None, candidate.relevance, confidence

    @staticmethod
    def _source_weight(quality: SourceQuality) -> float:
        return {
            SourceQuality.OFFICIAL: 1.0,
            SourceQuality.PRIMARY: 0.9,
            SourceQuality.PROFESSIONAL: 0.82,
            SourceQuality.AGGREGATOR: 0.65,
            SourceQuality.SOCIAL: 0.4,
        }[quality]

    @staticmethod
    def _sanitize_factor(
        factor: ScoringFactor,
        valid_evidence_ids: set[UUID],
    ) -> tuple[ScoringFactor, list[str]]:
        invalid_ids = [value for value in factor.evidence_ids if value not in valid_evidence_ids]
        return (
            factor.model_copy(
                update={
                    "evidence_ids": [
                        value for value in factor.evidence_ids if value in valid_evidence_ids
                    ]
                }
            ),
            [f"unknown factor evidence id: {value}" for value in invalid_ids],
        )

    @classmethod
    def _resolved_impact_factors(
        cls,
        run: ResearchRun,
        event_payload: dict[str, Any] | None,
        draft: DraftOutput,
        evidence: Sequence[Evidence],
    ) -> tuple[ImpactFactors, float, list[str]]:
        _, _, mapping_confidence = cls._mapping_inputs(run, event_payload)
        valid_ids = {item.id for item in evidence}
        warnings: list[str] = []
        sanitized: dict[str, ScoringFactor] = {}
        for name in (
            "magnitude",
            "persistence",
            "representativeness",
            "market_confirmation",
        ):
            factor, factor_warnings = cls._sanitize_factor(
                getattr(draft.impact_factors, name), valid_ids
            )
            sanitized[name] = factor
            warnings.extend(factor_warnings)

        representativeness = sanitized["representativeness"]
        capped_value = min(representativeness.value, mapping_confidence)
        if capped_value < representativeness.value:
            warnings.append(
                "representativeness was capped by asset mapping confidence "
                f"({mapping_confidence:.0%})"
            )
            representativeness = representativeness.model_copy(
                update={"value": capped_value}
            )
        return (
            ImpactFactors(
                direction=draft.impact_factors.direction,
                magnitude=sanitized["magnitude"],
                persistence=sanitized["persistence"],
                representativeness=representativeness,
                market_confirmation=sanitized["market_confirmation"],
            ),
            mapping_confidence,
            warnings,
        )

    @classmethod
    def _adjusted_confidence_factors(
        cls,
        draft: DraftOutput,
        evidence: Sequence[Evidence],
        verification: VerificationOutput,
    ) -> tuple[ConfidenceFactors, list[str]]:
        valid_ids = {item.id for item in evidence}
        evidence_by_id = {item.id: item for item in evidence}
        warnings: list[str] = []
        sanitized: dict[str, ScoringFactor] = {}
        for name in (
            "direction_clarity",
            "source_reliability",
            "magnitude_certainty",
            "market_context_completeness",
        ):
            factor, factor_warnings = cls._sanitize_factor(
                getattr(draft.confidence_factors, name), valid_ids
            )
            sanitized[name] = factor
            warnings.extend(factor_warnings)

        referenced_values = {str(value) for value in draft.evidence_ids}
        for factor_set in (draft.impact_factors, draft.confidence_factors):
            for name in type(factor_set).model_fields:
                value = getattr(factor_set, name)
                if isinstance(value, ScoringFactor):
                    referenced_values.update(str(item) for item in value.evidence_ids)
        valid_values = {str(value) for value in valid_ids}
        valid_referenced_ids = {
            UUID(value) for value in referenced_values & valid_values
        }
        citation_coverage = (
            len(valid_referenced_ids) / len(referenced_values)
            if referenced_values
            else 0.0
        )
        source_quality = max(
            (
                cls._source_weight(evidence_by_id[value].source_quality)
                for value in valid_referenced_ids
            ),
            default=0.0,
        )
        supported_assessments = [
            item
            for item in verification.claim_assessments
            if item.verdict is ClaimVerdict.SUPPORTED and item.evidence_ids
        ]
        claim_coverage = (
            len(supported_assessments) / len(verification.claim_assessments)
            if verification.claim_assessments
            else 0.0
        )

        source = sanitized["source_reliability"]
        source = source.model_copy(
            update={
                "value": min(source.value, source_quality, citation_coverage),
            }
        )
        magnitude = sanitized["magnitude_certainty"]
        magnitude = magnitude.model_copy(
            update={
                "value": min(
                    magnitude.value,
                    citation_coverage,
                    verification.evidence_strength,
                )
            }
        )
        context = sanitized["market_context_completeness"]
        context = context.model_copy(
            update={"value": min(context.value, claim_coverage)}
        )
        if citation_coverage < 1:
            warnings.append(
                f"valid evidence citation coverage is {citation_coverage:.0%}"
            )
        return (
            ConfidenceFactors(
                direction_clarity=sanitized["direction_clarity"],
                source_reliability=source,
                magnitude_certainty=magnitude,
                market_context_completeness=context,
            ),
            warnings,
        )

    def _verify(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        draft = DraftOutput.model_validate(state["draft"])
        evidence = [Evidence.model_validate(item) for item in state.get("evidence", [])]
        evidence_by_id = {str(item.id): item for item in evidence}
        available_ids = set(evidence_by_id)
        cited_ids = {str(item) for item in draft.evidence_ids}
        missing: list[str] = []
        unsupported: list[str] = []
        contradictions: list[str] = []

        required_text = {
            "summary": draft.summary,
            "products_or_protocol": draft.products_or_protocol,
            "valuation_or_tokenomics": draft.valuation_or_tokenomics,
        }
        for name, value in required_text.items():
            if not value.strip() or INSUFFICIENT_EVIDENCE_MARKER in value:
                missing.append(name)
        if not draft.risks or any(
            INSUFFICIENT_EVIDENCE_MARKER in item for item in draft.risks
        ):
            missing.append("risks")
        if not draft.invalidation_conditions or any(
            INSUFFICIENT_EVIDENCE_MARKER in item
            for item in draft.invalidation_conditions
        ):
            missing.append("invalidation_conditions")
        if not cited_ids:
            missing.append("evidence citations")
        unknown = cited_ids - available_ids
        if unknown:
            unsupported.append(f"unknown evidence ids: {sorted(unknown)}")
        if any(
            item.published_at > run.as_of or item.observed_at > run.as_of or item.as_of > run.as_of
            for item in evidence
        ):
            contradictions.append("point-in-time boundary violation")

        cited_evidence = [item for item in evidence if str(item.id) in cited_ids]
        official = any(item.source_quality is SourceQuality.OFFICIAL for item in cited_evidence)
        independent_groups = independent_evidence_groups(cited_evidence)
        corroborated = official or len(independent_groups) >= 2
        if not corroborated:
            missing.append(SOURCE_GATE)

        structural_complete = not missing and not unsupported and not contradictions
        structural_missing = list(missing)
        structural_unsupported = list(unsupported)
        structural_contradictions = list(contradictions)
        semantic_complete = False
        direction_supported = False
        semantic_status: Literal["not_run", "completed", "failed"] = "not_run"
        evidence_strength = 0.0
        assessments: list[ClaimEvidenceAssessment] = []
        semantic_missing: list[str] = []

        impact_factors, _, _ = self._resolved_impact_factors(
            run, state.get("event"), draft, evidence
        )
        preview = short_term_impact_score(
            direction=impact_factors.direction,
            magnitude=impact_factors.magnitude.value,
            persistence=impact_factors.persistence.value,
            representativeness=impact_factors.representativeness.value,
            market_confirmation=impact_factors.market_confirmation.value,
        )

        if structural_complete and not run.retryable_reason:
            claim_specs = self._claim_specs(draft)
            requested_claims = [
                {"claim_kind": kind, "claim": claim} for kind, claim in claim_specs
            ]
            try:
                semantic_payload = self.llm.generate_json(
                    model=self.settings.ollama_assist_model,
                    lane="assist",
                    system=(
                        "你是独立的逐观点证据复核员，不负责写报告或打投资分。"
                        "逐条判断观点是否被给定证据语义支持；claim 必须原样返回，"
                        "只能引用实际存在的 evidence id。事实存在不等于收益方向成立。"
                        "supported 表示证据直接支持，contradicted 表示冲突，"
                        "unrelated 表示证据与观点无关，insufficient 表示无法证明。"
                    ),
                    prompt=(
                        f"程序待验证方向分：{preview.score}。\n"
                        f"待验证观点：{json.dumps(requested_claims, ensure_ascii=False)}\n"
                        "证据："
                        f"{compact_evidence(state.get('evidence', []), self.settings.research_prompt_evidence_chars, max_per_group=2)}\n"
                        "只有每个观点都逐条绑定证据，且证据支持程序方向时，"
                        "direction_supported 才能为 true。不要补充外部事实。"
                    ),
                    schema=SemanticVerificationOutput,
                    temperature=0,
                    operation="direction_verification",
                    entity_type="research_run",
                    entity_id=run.id,
                )
                semantic = SemanticVerificationOutput.model_validate(semantic_payload)
                semantic_status = "completed"
                contradictions.extend(semantic.contradictions)
                expected = {
                    self._claim_key(kind, claim): (kind, claim) for kind, claim in claim_specs
                }
                returned: dict[tuple[str, str], SemanticClaimOutput] = {}
                for item in semantic.claims:
                    key = self._claim_key(item.claim_kind, item.claim)
                    if key not in expected:
                        unsupported.append(
                            f"semantic verifier returned unexpected claim: {item.claim[:120]}"
                        )
                        continue
                    returned[key] = item

                supported_scores: list[float] = []
                stance_numerator = 0.0
                stance_denominator = 0.0
                for key, (kind, claim) in expected.items():
                    item = returned.get(key)
                    if item is None:
                        semantic_missing.append(f"claim evidence: {kind}: {claim[:120]}")
                        continue
                    unknown_ids = sorted(set(item.evidence_ids) - available_ids)
                    valid_ids = [value for value in item.evidence_ids if value in available_ids]
                    if unknown_ids:
                        unsupported.append(
                            f"unknown claim evidence ids for {kind}: {unknown_ids}"
                        )
                    assessment = ClaimEvidenceAssessment(
                        claim=claim,
                        claim_kind=kind,
                        stance=item.stance,
                        verdict=item.verdict,
                        evidence_ids=[evidence_by_id[value].id for value in valid_ids],
                        confidence=item.confidence,
                        reason=item.reason,
                    )
                    assessments.append(assessment)
                    if item.verdict is ClaimVerdict.CONTRADICTED:
                        contradictions.append(f"contradicted claim: {claim[:160]}")
                    if item.verdict is not ClaimVerdict.SUPPORTED or not valid_ids:
                        semantic_missing.append(f"unsupported claim: {kind}: {claim[:120]}")
                        continue
                    quality = max(
                        self._source_weight(evidence_by_id[value].source_quality)
                        for value in valid_ids
                    )
                    support_score = item.confidence * quality
                    supported_scores.append(support_score)
                    if item.stance:
                        stance_numerator += item.stance * support_score
                        stance_denominator += support_score

                coverage = len(supported_scores) / max(1, len(expected))
                evidence_strength = round(
                    coverage
                    * (sum(supported_scores) / max(1, len(supported_scores))),
                    4,
                )
                stance_signal = (
                    stance_numerator / stance_denominator if stance_denominator else 0.0
                )
                preview_direction = impact_factors.direction
                stance_aligned = bool(
                    preview_direction == 0
                    or preview_direction * stance_signal >= 0.2
                )
                direction_supported = bool(
                    preview_direction == 0
                    or (semantic.direction_supported and stance_aligned)
                )
                if abs(preview.score) >= 15 and not direction_supported:
                    semantic_missing.append(
                        "claim stances do not support the deterministic direction"
                    )
                if evidence_strength < self.settings.minimum_directional_confidence:
                    semantic_missing.append("claim-level evidence strength is low")
                semantic_complete = bool(
                    expected
                    and not semantic_missing
                    and not unsupported
                    and not contradictions
                    and direction_supported
                )
                run.retryable_reason = None
            except SoftTimeLimitExceeded:
                raise
            except Exception as exc:
                semantic_status = "failed"
                semantic_missing.append(
                    f"semantic evidence verifier unavailable: {type(exc).__name__}"
                )

        missing.extend(semantic_missing)
        verification = VerificationOutput(
            evidence_complete=structural_complete,
            semantic_evidence_complete=semantic_complete,
            direction_supported=direction_supported,
            semantic_status=semantic_status,
            evidence_strength=evidence_strength,
            claim_assessments=assessments,
            missing_requirements=missing,
            contradictions=contradictions,
            unsupported_claims=unsupported,
        )
        round_number = state.get("verification_round", 0) + 1
        run.status = RunStatus.VERIFYING
        run.verification_round = round_number
        run.missing_requirements = missing + unsupported
        run.contradictions = contradictions
        run.analysis_steps.append(
            AnalysisStep(
                phase="verification",
                status="completed" if structural_complete else "incomplete",
                executor="evidence-quality",
                model="evidence-quality:v1",
                summary=(
                    f"资料质量核验{'完整' if structural_complete else '存在提示'}："
                    f"缺失 {len(structural_missing)} 项、矛盾 {len(structural_contradictions)} 项、"
                    f"无效引用 {len(structural_unsupported)} 项。"
                ),
                metrics={
                    "round": round_number,
                    "evidence_complete": structural_complete,
                    "missing_requirements": structural_missing,
                    "contradictions": structural_contradictions,
                    "unsupported_claims": structural_unsupported,
                },
            )
        )
        if structural_complete:
            run.analysis_steps.append(
                AnalysisStep(
                    phase="claim_evidence_verification",
                    status=(
                        "failed"
                        if semantic_status == "failed"
                        else ("completed" if semantic_complete else "incomplete")
                    ),
                    executor="ollama-independent-verifier",
                    model=self.settings.ollama_assist_model,
                    summary=(
                        f"逐观点证据核验{'完整' if semantic_complete else '存在提示'}："
                        f"验证 {len(assessments)} 个观点，证据强度 {evidence_strength:.0%}。"
                    ),
                    metrics={
                        "semantic_status": semantic_status,
                        "semantic_evidence_complete": semantic_complete,
                        "direction_supported": direction_supported,
                        "claim_count": len(assessments),
                        "evidence_strength": evidence_strength,
                    },
                )
            )
        save_run(self.db, run)
        return {
            "run": run.model_dump(mode="json"),
            "verification": verification.model_dump(mode="json"),
            "verification_round": round_number,
        }

    def _route_after_verification(
        self, state: ResearchState
    ) -> Literal["acquire_evidence", "revise", "finalize"]:
        # Kept as a compatibility hook for callers that exercised the old
        # graph directly. Evidence quality is advisory and never causes a
        # second research pass in the short-term scoring pipeline.
        return "finalize"

    @staticmethod
    def _revision_is_useful(verification: VerificationOutput) -> bool:
        """Only spend a second Research 7B call on model-repairable structure gaps."""

        if verification.contradictions or verification.unsupported_claims:
            return False
        repairable_requirements = {
            "summary",
            "products_or_protocol",
            "valuation_or_tokenomics",
            "risks",
            "invalidation_conditions",
            "evidence citations",
        }
        return any(
            requirement in repairable_requirements
            for requirement in verification.missing_requirements
        )

    @staticmethod
    def _source_gate_can_be_repaired(evidence_payload: list[dict[str, Any]]) -> bool:
        evidence = [Evidence.model_validate(item) for item in evidence_payload]
        return any(item.source_quality is SourceQuality.OFFICIAL for item in evidence) or len(
            independent_evidence_groups(evidence)
        ) >= 2

    def _route_after_acquisition(
        self, state: ResearchState
    ) -> Literal["revise", "finalize"]:
        return "finalize"

    def _acquire_evidence(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        event = NewsEvent.model_validate(state["event"]) if state.get("event") else None
        verification = VerificationOutput.model_validate(state["verification"])
        evidence = [Evidence.model_validate(item) for item in state.get("evidence", [])]
        attempts = state.get("acquisition_attempts", 0) + 1
        if run.historical_replay:
            run.analysis_steps.append(
                AnalysisStep(
                    phase="historical_point_in_time_guard",
                    executor="target-transmission-v2",
                    summary="历史回放仅使用研究截止时已观察证据，已禁用实时搜索和当前基本面。",
                )
            )
            save_run(self.db, run)
            return {
                "run": run.model_dump(mode="json"),
                "evidence": [item.model_dump(mode="json") for item in evidence],
                "verification": verification.model_dump(mode="json"),
                "acquisition_attempts": attempts,
            }
        queries = self._targeted_queries(run.asset, event)
        seen_facts = {(canonicalize_url(item.source_url), item.claim) for item in evidence}
        structured_error: str | None = None
        structured_added = 0
        try:
            targeted_data = self.registry.get_research_data(run.asset)
            for item in self._build_evidence(run, None, targeted_data):
                fact = (canonicalize_url(item.source_url), item.claim)
                if fact in seen_facts:
                    continue
                evidence.append(item)
                seen_facts.add(fact)
                structured_added += 1
        except SoftTimeLimitExceeded:
            raise
        except Exception as exc:
            structured_error = type(exc).__name__

        candidate_by_url: dict[str, NewsItem] = {}
        for item in list_news(self.db, limit=1000, as_of=run.as_of):
            enriched = enrich_news_lineage(item)
            if self._is_targeted_candidate(enriched, run.asset, event):
                candidate_by_url[canonicalize_url(enriched.url)] = enriched

        provider_error: str | None = None
        remote_count = 0
        try:
            since = min(as_utc(run.as_of), utc_now()) - timedelta(
                hours=self.settings.event_cluster_window_hours
            )
            remote_items = self.registry.discover_news(
                since=since,
                limit=self.settings.targeted_evidence_limit,
            )
            remote_count = len(remote_items)
            for item in remote_items:
                enriched = enrich_news_lineage(item)
                if self._is_targeted_candidate(enriched, run.asset, event):
                    candidate_by_url[canonicalize_url(enriched.url)] = enriched
        except SoftTimeLimitExceeded:
            raise
        except Exception as exc:
            provider_error = type(exc).__name__

        web_results = []
        web_errors: list[dict[str, str]] = []
        for query in queries[:3]:
            try:
                found, errors = search_enabled_sources_sync(
                    SearchRequest(query=query, language="zh-CN", limit=5)
                )
                web_results.extend(found)
                web_errors.extend(errors)
            except SoftTimeLimitExceeded:
                raise
            except Exception as exc:
                web_errors.append(
                    {"source": "registry", "error": f"{type(exc).__name__}: {exc}"[:500]}
                )

        web_added = 0
        seen_web_urls = {canonicalize_url(item.source_url) for item in evidence}
        for result in web_results:
            url = canonicalize_url(result.url)
            if url in seen_web_urls or web_added >= 8:
                continue
            observed = utc_now()
            if result.published_at and as_utc(result.published_at) > as_utc(observed):
                continue
            run.as_of = max(as_utc(run.as_of), as_utc(observed))
            evidence.append(
                Evidence(
                    run_id=run.id,
                    claim=result.title,
                    source_name=result.source,
                    source_url=result.url,
                    source_quality=SourceQuality.AGGREGATOR,
                    published_at=result.published_at or observed,
                    observed_at=observed,
                    as_of=observed,
                    excerpt=result.snippet[:1000],
                    independent_group=f"web:{result.domain}",
                )
            )
            seen_web_urls.add(url)
            web_added += 1

        run.analysis_steps.append(
            AnalysisStep(
                phase="web_search_verification",
                status="completed" if web_added else ("failed" if web_errors else "no_results"),
                executor="mcp-search-registry",
                summary=f"联网补证完成，接受 {web_added} 条带原始链接的搜索结果。",
                metrics={
                    "queries": queries[:3],
                    "accepted_results": web_added,
                    "errors": web_errors,
                },
            )
        )

        seen_urls = {canonicalize_url(item.source_url) for item in evidence}
        accepted: list[NewsItem] = []
        now = utc_now()
        for candidate in sorted(
            candidate_by_url.values(), key=lambda item: item.published_at, reverse=True
        ):
            if candidate.published_at > now:
                continue
            canonical_url = canonicalize_url(candidate.url)
            if canonical_url in seen_urls:
                continue
            save_news(self.db, candidate)
            stored = get_news_by_content_hash(self.db, candidate.content_hash) or candidate
            boundary = max(
                as_utc(run.as_of),
                as_utc(stored.published_at),
                as_utc(stored.observed_at),
                as_utc(stored.as_of),
            )
            run.as_of = boundary
            evidence.append(self._news_evidence(run, stored))
            seen_urls.add(canonical_url)
            accepted.append(stored)
            if event and stored.id not in event.news_item_ids:
                event.news_item_ids.append(stored.id)
            if len(accepted) >= 8:
                break

        if event and accepted:
            event.as_of = run.as_of
            event.priority = max(event.priority, 0.5)
            quality_rank = {
                SourceQuality.SOCIAL: 0,
                SourceQuality.AGGREGATOR: 1,
                SourceQuality.PROFESSIONAL: 2,
                SourceQuality.PRIMARY: 3,
                SourceQuality.OFFICIAL: 4,
            }
            best_quality = max(
                (item.source_quality for item in accepted), key=quality_rank.__getitem__
            )
            if quality_rank[best_quality] > quality_rank[event.source_quality]:
                event.source_quality = best_quality
            source_groups = independent_evidence_groups(evidence)
            step = AnalysisStep(
                phase="story_clustering",
                executor="targeted-evidence:v1",
                summary=(
                    f"定向补证向持久事件簇 {event.id} 新增 {len(accepted)} 篇新闻，"
                    f"当前证据覆盖 {len(source_groups)} 个独立来源血缘。"
                ),
                metrics={
                    "cluster_id": str(event.id),
                    "member_count": len(event.news_item_ids),
                    "accepted_news": len(accepted),
                    "independent_sources": len(source_groups),
                },
            )
            for index in range(len(event.analysis_steps) - 1, -1, -1):
                if event.analysis_steps[index].phase == step.phase:
                    event.analysis_steps[index] = step
                    break
            else:
                event.analysis_steps.append(step)
            save_event(self.db, event)

        retrieval_error: str | None = None
        retrieved_context = state.get("retrieved_context", [])
        try:
            retrieval = RetrievalService(self.db, self.settings)
            retrieval.index(run.asset.asset_id, evidence)
            retrieved_context = retrieval.search(
                self._retrieval_query(run.asset, event),
                asset_id=run.asset.asset_id,
                as_of=run.as_of,
            )
        except SoftTimeLimitExceeded:
            raise
        except Exception as exc:
            retrieval_error = type(exc).__name__

        run.evidence = evidence
        independent_sources = independent_evidence_groups(evidence)
        added_count = structured_added + len(accepted) + web_added
        run.analysis_steps.append(
            AnalysisStep(
                phase="targeted_evidence_acquisition",
                status="completed" if added_count else "no_results",
                executor="local-index+news-providers",
                summary=(
                    f"根据验证缺口执行第 {attempts} 轮定向补证，新增 {added_count} 条证据，"
                    f"当前覆盖 {len(independent_sources)} 个独立来源血缘。"
                ),
                metrics={
                    "attempt": attempts,
                    "missing_requirements": verification.missing_requirements,
                    "queries": queries,
                    "remote_candidates": remote_count,
                    "accepted_evidence": added_count,
                    "accepted_structured_evidence": structured_added,
                    "accepted_news_evidence": len(accepted),
                    "accepted_web_evidence": web_added,
                    "independent_sources": len(independent_sources),
                    "structured_provider_error": structured_error,
                    "provider_error": provider_error,
                    "provider_errors": getattr(self.registry, "last_errors", []),
                    "retrieval_error": retrieval_error,
                },
            )
        )
        save_run(self.db, run)
        return {
            "run": run.model_dump(mode="json"),
            "event": event.model_dump(mode="json") if event else None,
            "evidence": [item.model_dump(mode="json") for item in evidence],
            "retrieved_context": retrieved_context,
            "acquisition_attempts": attempts,
            "acquired_evidence_count": added_count,
        }

    @staticmethod
    def _targeted_queries(asset: AssetRef, event: NewsEvent | None) -> list[str]:
        context = [asset.symbol, asset.name, *asset.aliases]
        if event:
            context.extend([event.headline, *event.entities])
        queries = [" ".join(item for item in context if item)]
        if asset.asset_class is AssetClass.EQUITY:
            queries.append(f"{asset.symbol} official filing investor relations")
        else:
            queries.append(f"{asset.symbol} official protocol security disclosure")
        return queries

    @staticmethod
    def _retrieval_query(asset: AssetRef, event: NewsEvent | None) -> str:
        return f"{asset.name} {asset.symbol} {event.headline if event else ''}".strip()

    @staticmethod
    def _is_targeted_candidate(item: NewsItem, asset: AssetRef, event: NewsEvent | None) -> bool:
        text = normalize_text(f"{item.title} {item.summary}")
        explicit_symbol = asset.symbol.casefold() in {value.casefold() for value in item.symbols}
        asset_terms = [asset.symbol, asset.name, *asset.aliases]
        asset_match = explicit_symbol or any(
            normalized and normalized in text
            for normalized in (normalize_text(value) for value in asset_terms)
        )
        if not asset_match:
            return False
        if not event:
            return True
        title_similarity = SequenceMatcher(
            None, normalize_text(item.title), normalize_text(event.headline)
        ).ratio()
        event_tokens = {
            token.casefold()
            for token in re.findall(r"[a-zA-Z0-9]{3,}|[\u3400-\u9fff]{2,}", event.headline)
        }
        item_tokens = {
            token.casefold()
            for token in re.findall(r"[a-zA-Z0-9]{3,}|[\u3400-\u9fff]{2,}", item.title)
        }
        overlap = len(event_tokens & item_tokens) / max(1, len(event_tokens))
        return title_similarity >= 0.35 or overlap >= 0.3

    def _revise(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        current = DraftOutput.model_validate(state["draft"])
        verification = VerificationOutput.model_validate(state["verification"])
        evidence = [Evidence.model_validate(item) for item in state.get("evidence", [])]
        valid_evidence_ids = {str(item.id) for item in evidence}
        source_lineages = sorted(independent_evidence_groups(evidence))
        repair_requirements = {
            "missing_requirements": verification.missing_requirements,
            "unsupported_claims": verification.unsupported_claims,
            "contradictions": verification.contradictions,
            "valid_evidence_ids": sorted(valid_evidence_ids),
            "independent_source_lineages": source_lineages,
        }
        current_payload = current.model_dump(
            mode="json",
            exclude={"direction", "rating"},
        )
        prompt = (
            f"当前报告：{json.dumps(current_payload, ensure_ascii=False, separators=(',', ':'))}\n"
            f"本轮修复清单：{json.dumps(repair_requirements, ensure_ascii=False)}\n"
            "触发事件："
            f"{json.dumps(self._event_prompt_context(state.get('event'), run.asset.asset_id), ensure_ascii=False)}\n"
            f"程序研究因子：{json.dumps(state.get('factor_summary', {}), ensure_ascii=False, default=str)}\n"
            "可用证据："
            f"{compact_evidence(state.get('evidence', []), min(self.settings.research_prompt_evidence_chars, 4000), max_per_group=2)}\n"
            "逐项修复清单中的缺失章节。能够由证据支持时必须填写；无法支持时不得留空，"
            f"应明确写明“{INSUFFICIENT_EVIDENCE_MARKER}”并保持低置信度和中性分数。"
            "每个重要事实和方向观点都必须绑定 valid_evidence_ids 中实际支持它的证据；"
            "不得生成、猜测或沿用无效 evidence id。删除无法被证据支持的断言，不得编造。"
            "同一原始报道的转载、改写或聚合副本只算一个独立来源，不能用来满足两份独立来源要求。"
            "官方文件必须与所支持的具体观点直接相关；例如 Form 4 或持股变动文件不能支持"
            "AI 基础设施受益观点，除非该观点本身就是该持股交易。"
            "只返回需要更新的字段；不需要修改的字段返回 null。"
        )
        revision_error: str | None = None
        try:
            repair_payload = self.llm.generate_json(
                model=self.settings.ollama_research_model,
                lane="research",
                system=(
                    "你是投资研究报告修订器，只能使用给定证据。"
                    "必须逐项执行本轮修复清单；缺失内容只能补充为证据支持的结论，"
                    f"否则明确标注“{INSUFFICIENT_EVIDENCE_MARKER}”，不能留空。"
                    "同一故事的转载不构成独立佐证，无关官方文件不构成观点支持。"
                    "修订时必须重新评估方向分数，不能机械沿用当前报告的 score。"
                    f"{DIRECTION_SCORE_INSTRUCTION}"
                ),
                prompt=prompt,
                schema=DraftRepairOutput,
                operation="report_revision",
                entity_type="research_run",
                entity_id=run.id,
                max_input_tokens=self.settings.ollama_research_revision_max_input_tokens,
                max_output_tokens=min(
                    self.settings.ollama_research_max_output_tokens,
                    1024,
                ),
            )
            repair = DraftRepairOutput.model_validate(repair_payload)
            updates = repair.model_dump(exclude_none=True)
            repaired_evidence_ids = updates.pop("evidence_ids", None)
            merged = current.model_dump(mode="json")
            merged.update(updates)
            if repaired_evidence_ids is not None:
                merged["evidence_ids"] = list(
                    dict.fromkeys([*current.evidence_ids, *repaired_evidence_ids])
                )
            revised_output = DraftOutput.model_validate(merged)
            run.retryable_reason = None
        except SoftTimeLimitExceeded:
            raise
        except Exception as exc:
            revision_error = type(exc).__name__
            run.retryable_reason = f"model_{type(exc).__name__}"
            current.confidence = min(current.confidence, 0.5)
            current.score = 0
            revised_output = current
        invalid_evidence_ids = [
            value
            for value in revised_output.evidence_ids
            if value not in valid_evidence_ids
        ]
        revised_output.evidence_ids = list(
            dict.fromkeys(
                value
                for value in revised_output.evidence_ids
                if value in valid_evidence_ids
            )
        )
        self._mark_unresolved_revision_sections(revised_output, verification)
        run.analysis_steps.append(
            AnalysisStep(
                phase="report_revision",
                status="fallback" if revision_error else "completed",
                executor="ollama" if not revision_error else "rules",
                model=self.settings.ollama_research_model
                if not revision_error
                else "revision-fallback:v1",
                summary=(
                    f"已根据校验结果修订报告，置信度调整为 {revised_output.confidence:.0%}。"
                    + (
                        f" 模型不可用（{revision_error}），已强制采用中性保守结论。"
                        if revision_error
                        else ""
                    )
                ),
                metrics={
                    "confidence": revised_output.confidence,
                    "score": revised_output.score,
                    "direction": revised_output.direction.value,
                    "rating": revised_output.rating.value,
                    "requested_repairs": verification.missing_requirements,
                    "invalid_evidence_ids_removed": invalid_evidence_ids,
                    "independent_source_lineages": len(source_lineages),
                },
            )
        )
        save_run(self.db, run)
        return {
            "run": run.model_dump(mode="json"),
            "draft": revised_output.model_dump(mode="json"),
        }

    @staticmethod
    def _mark_unresolved_revision_sections(
        draft: DraftOutput, verification: VerificationOutput
    ) -> None:
        """Make unresolved second-pass gaps explicit without inventing evidence."""

        requested = set(verification.missing_requirements)
        text_markers = {
            "summary": "现有证据不足，无法形成可验证的核心观点。",
            "products_or_protocol": "现有证据不足，无法形成产品或业务影响结论。",
            "valuation_or_tokenomics": "现有证据不足，无法形成估值影响结论。",
        }
        for field_name, marker in text_markers.items():
            if field_name in requested and not getattr(draft, field_name).strip():
                setattr(draft, field_name, marker)
        if "risks" in requested and not draft.risks:
            draft.risks = ["现有证据不足，无法识别可验证风险。"]
        if "invalidation_conditions" in requested and not draft.invalidation_conditions:
            draft.invalidation_conditions = [
                "现有证据不足，无法设定可验证失效条件。"
            ]

    def _v2_target_impact(
        self,
        run: ResearchRun,
        event_payload: dict[str, Any] | None,
        draft: DraftOutput,
        evidence: Sequence[Evidence],
        *,
        technical_failure: bool,
    ) -> TargetImpact | None:
        if not event_payload:
            return None
        event = NewsEvent.model_validate(event_payload)
        if not event.actions:
            # Payloads created before v2 remain readable and retain their
            # historical scoring. Every newly extracted event contains actions.
            return None

        event_run = get_event_research_for_event(self.db, event.id)
        if (
            event_run
            and event_run.report
            and event_run.report.scoring_version == TARGET_SCORING_VERSION
        ):
            existing = next(
                (
                    item
                    for item in event_run.report.impacts
                    if item.asset and item.asset.asset_id == run.asset.asset_id
                ),
                None,
            )
            if existing is not None:
                return existing.model_copy(
                    update={
                        "technical_failure": technical_failure,
                        "trade_status": (
                            TradeStatus.UNTRADEABLE
                            if technical_failure
                            else existing.trade_status
                        ),
                    }
                )

        mapping = next(
            (
                item
                for item in event.candidates
                if item.asset.asset_id == run.asset.asset_id
            ),
            None,
        )
        action = event.actions[0]
        if draft.target_impact is not None:
            impact_draft = draft.target_impact.model_copy(
                update={
                    "target_type": TargetType.TRADABLE_ASSET,
                    "target_name": run.asset.name,
                    "asset_id": run.asset.asset_id,
                    "action_id": draft.target_impact.action_id or action.id,
                }
            )
        else:
            relevance = (
                min(mapping.relevance, mapping.mapping_confidence) if mapping else 0.0
            )
            valid_ids = {str(item.id) for item in evidence}
            cited = [value for value in draft.evidence_ids if value in valid_ids]
            impact_draft = TargetImpactDraft(
                target_type=TargetType.TRADABLE_ASSET,
                target_name=run.asset.name,
                asset_id=run.asset.asset_id,
                action_id=action.id,
                direction=draft.impact_factors.direction,
                factors=TransmissionFactors(
                    event_strength=action.strength,
                    target_relevance=min(
                        relevance,
                        draft.impact_factors.magnitude.value,
                    ),
                    transmission_directness=(
                        draft.impact_factors.representativeness.value
                    ),
                    realization_probability=(
                        draft.impact_factors.market_confirmation.value
                    ),
                    novelty=event.novelty,
                    persistence=draft.impact_factors.persistence.value,
                ),
                confidence_factors=TargetConfidenceFactors(
                    direction_clarity=(
                        draft.confidence_factors.direction_clarity.value
                    ),
                    source_reliability=(
                        draft.confidence_factors.source_reliability.value
                    ),
                    transmission_certainty=(
                        draft.confidence_factors.magnitude_certainty.value
                    ),
                    market_context_completeness=(
                        draft.confidence_factors.market_context_completeness.value
                    ),
                ),
                transmission_path=[event.headline, run.asset.name],
                rationale=draft.summary,
                evidence_ids=cited,
                missing_information=([] if cited else ["impact_evidence"]),
            )
        _, impacts, _, _ = finalize_impacts(
            EventImpactDraft(summary=draft.summary, impacts=[impact_draft]),
            event=event,
            evidence=evidence,
            assets={run.asset.asset_id: run.asset},
            technical_failure=technical_failure,
        )
        return impacts[0] if impacts else None

    def _finalize(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        draft = DraftOutput.model_validate(state["draft"])
        verification = VerificationOutput.model_validate(state["verification"])
        structural_complete = verification.evidence_complete
        semantic_complete = verification.semantic_evidence_complete
        evidence = [Evidence.model_validate(item) for item in state.get("evidence", [])]
        impact_factors, mapping_confidence, factor_warnings = (
            self._resolved_impact_factors(
                run,
                state.get("event"),
                draft,
                evidence,
            )
        )
        impact_score = short_term_impact_score(
            direction=impact_factors.direction,
            magnitude=impact_factors.magnitude.value,
            persistence=impact_factors.persistence.value,
            representativeness=impact_factors.representativeness.value,
            market_confirmation=impact_factors.market_confirmation.value,
        )
        confidence_factors, confidence_warnings = self._adjusted_confidence_factors(
            draft,
            evidence,
            verification,
        )
        confidence_score = rating_confidence_score(
            direction_clarity=confidence_factors.direction_clarity.value,
            source_reliability=confidence_factors.source_reliability.value,
            magnitude_certainty=confidence_factors.magnitude_certainty.value,
            market_context_completeness=(
                confidence_factors.market_context_completeness.value
            ),
        )
        technical_failure = bool(
            run.retryable_reason and run.retryable_reason.startswith("model_")
        )
        target_impact = self._v2_target_impact(
            run,
            state.get("event"),
            draft,
            evidence,
            technical_failure=technical_failure,
        )
        score = (
            round_half_up(target_impact.score * 100)
            if target_impact is not None
            else (0 if technical_failure else impact_score.score)
        )
        if technical_failure:
            signal_status = SignalStatus.TECHNICAL_FAILURE
            probabilities = blocked_probabilities(technical_failure=True)
        elif abs(score) < (25 if target_impact is not None else 15):
            signal_status = SignalStatus.NEUTRAL
            probabilities = probabilities_for_score(
                score,
                base_probability=draft.base_probability,
            )
        else:
            signal_status = SignalStatus.DIRECTIONAL
            probabilities = probabilities_for_score(
                score,
                base_probability=draft.base_probability,
            )
        confidence = (
            target_impact.confidence
            if target_impact is not None
            else (0.0 if technical_failure else confidence_score.confidence)
        )
        evidence_warnings = list(
            dict.fromkeys(
                [
                    *verification.missing_requirements,
                    *verification.unsupported_claims,
                    *verification.contradictions,
                    *factor_warnings,
                    *confidence_warnings,
                ]
            )
        )
        valid_evidence_ids = {str(item.id): item.id for item in run.evidence}
        thesis_evidence_ids = [
            valid_evidence_ids[item]
            for item in draft.evidence_ids
            if item in valid_evidence_ids
        ]
        for assessment in verification.claim_assessments:
            thesis_evidence_ids.extend(assessment.evidence_ids)
        thesis = Thesis(
            summary=draft.summary,
            historical_context=draft.historical_context,
            financials_and_growth=draft.financials_and_growth,
            products_or_protocol=draft.products_or_protocol,
            competition=draft.competition,
            valuation_or_tokenomics=draft.valuation_or_tokenomics,
            catalysts=draft.catalysts,
            risks=draft.risks,
            invalidation_conditions=draft.invalidation_conditions,
            evidence_ids=list(dict.fromkeys(thesis_evidence_ids)),
        )
        model_opinion_available = target_impact is None and not (
            run.retryable_reason and run.retryable_reason.startswith("model_")
        )
        recommendation = Recommendation(
            run_id=run.id,
            asset=run.asset,
            score=score,
            model_score=draft.score if model_opinion_available else None,
            model_direction=draft.direction if model_opinion_available else None,
            model_rating=draft.rating if model_opinion_available else None,
            model_confidence=draft.confidence if model_opinion_available else None,
            raw_score=score,
            rating=target_impact.rating if target_impact is not None else rating_for(score),
            confidence=confidence,
            bull_probability=probabilities[0],
            base_probability=probabilities[1],
            bear_probability=probabilities[2],
            horizon_days=3,
            horizon_unit=HorizonUnit.TRADING_SESSIONS,
            impact_factors=impact_factors,
            confidence_factors=confidence_factors,
            fact_confidence=(
                fact_confidence_for(evidence)
                if target_impact is not None
                else (
                    0.0
                    if technical_failure
                    else confidence_factors.source_reliability.value
                )
            ),
            evidence_warnings=evidence_warnings,
            valuation_low=draft.valuation_low,
            valuation_high=draft.valuation_high,
            thesis=thesis,
            as_of=run.as_of,
            evidence_complete=structural_complete,
            directional_evidence_complete=structural_complete and semantic_complete,
            direction_verified=not technical_failure,
            signal_status=signal_status,
            evidence_strength=verification.evidence_strength,
            mapping_confidence=mapping_confidence,
            claim_assessments=verification.claim_assessments,
            primary_gate_reason=None,
            gate_reasons=[],
            scoring_version=(
                TARGET_SCORING_VERSION
                if target_impact is not None
                else "short-term-impact-v1"
            ),
            calibration_version=(
                "target-component-confidence-v2"
                if target_impact is not None
                else "component-confidence-v1"
            ),
            impact=target_impact,
        )
        run.recommendation = recommendation
        if signal_status is SignalStatus.TECHNICAL_FAILURE:
            run.status = RunStatus.FAILED
            run.error = f"研究依赖不可用：{run.retryable_reason}"
        else:
            run.status = RunStatus.COMPLETED
        run.completed_at = utc_now()
        run.analysis_steps.append(
            AnalysisStep(
                phase="finalization",
                status="failed" if technical_failure else "completed",
                executor="rating-engine",
                model=(
                    TARGET_SCORING_VERSION
                    if target_impact is not None
                    else "short-term-impact-v1"
                ),
                summary=(
                    f"最终状态 {signal_status.value}，目标影响分 {score}，"
                    f"评级置信度 {confidence:.0%}；证据提示 {len(evidence_warnings)} 项。"
                ),
                metrics={
                    "rating": recommendation.rating.value,
                    "signal_status": signal_status.value,
                    "model_score": draft.score,
                    "model_direction": draft.direction.value if draft.direction else None,
                    "model_rating": draft.rating.value if draft.rating else None,
                    "model_confidence": draft.confidence,
                    "raw_score": score,
                    "score": score,
                    "confidence": confidence,
                    "evidence_complete": structural_complete,
                    "semantic_evidence_complete": semantic_complete,
                    "evidence_strength": verification.evidence_strength,
                    "mapping_confidence": mapping_confidence,
                    "fact_confidence": recommendation.fact_confidence,
                    "evidence_warnings": evidence_warnings,
                    "target_impact": (
                        target_impact.model_dump(mode="json")
                        if target_impact is not None
                        else None
                    ),
                    "score_components": {
                        "magnitude": impact_score.magnitude_contribution,
                        "persistence": impact_score.persistence_contribution,
                        "representativeness": (
                            impact_score.representativeness_contribution
                        ),
                        "market_confirmation": (
                            impact_score.market_confirmation_contribution
                        ),
                    },
                    "confidence_components": {
                        "direction_clarity": (
                            confidence_score.direction_clarity_contribution
                        ),
                        "source_reliability": (
                            confidence_score.source_reliability_contribution
                        ),
                        "magnitude_certainty": (
                            confidence_score.magnitude_certainty_contribution
                        ),
                        "market_context_completeness": (
                            confidence_score.market_context_completeness_contribution
                        ),
                    },
                },
            )
        )
        save_recommendation(self.db, recommendation)
        save_run(self.db, run)
        self._write_report(run)
        return {"run": run.model_dump(mode="json")}

    def _write_report(self, run: ResearchRun) -> Path:
        self.settings.reports_dir.mkdir(parents=True, exist_ok=True)
        path = self.settings.reports_dir / f"{run.asset.symbol}_{run.id}.md"
        recommendation = run.recommendation
        assert recommendation is not None
        model_direction = (
            recommendation.model_direction.value
            if recommendation.model_direction
            else "unavailable"
        )
        model_rating = (
            recommendation.model_rating.value
            if recommendation.model_rating
            else "unavailable"
        )
        model_confidence = (
            f"{recommendation.model_confidence:.0%}"
            if recommendation.model_confidence is not None
            else "unavailable"
        )
        citations = "\n".join(
            f"- [{item.source_name}]({item.source_url}) — {item.claim}" for item in run.evidence
        )
        claim_checks = "\n".join(
            f"- [{item.verdict.value}] {item.claim_kind}: {item.claim} "
            f"(置信度 {item.confidence:.0%})"
            for item in recommendation.claim_assessments
        )
        warning_lines = "\n".join(
            f"- {item}" for item in recommendation.evidence_warnings
        )
        rating_label = {
            Rating.STRONGLY_BULLISH: "强烈看多",
            Rating.BULLISH: "看多",
            Rating.WATCH: "观望",
            Rating.BEARISH: "看空",
            Rating.STRONGLY_BEARISH: "强烈看空",
        }[recommendation.rating]
        content = (
            f"# {run.asset.name} ({run.asset.symbol})\n\n"
            f"- 评级：{rating_label}\n"
            f"- 信号状态：{recommendation.signal_status.value}\n"
            f"- 1–3 交易日影响分：{recommendation.score}\n"
            f"- 模型意见分（仅审计）：{recommendation.model_score}\n"
            f"- 7B 模型方向 / 五档评级：{model_direction} / {model_rating}\n"
            f"- 7B 模型原始置信度：{model_confidence}\n"
            f"- 评级置信度：{recommendation.confidence:.0%}\n"
            f"- 新闻事实置信度：{(recommendation.fact_confidence or 0):.0%}\n"
            f"- 截止时间：{run.as_of.isoformat()}\n"
            f"- 资料覆盖：{'完整' if recommendation.evidence_complete else '不足'}\n"
            f"- 逐观点证据核验：{'完整' if recommendation.directional_evidence_complete else '存在提示'}\n\n"
            f"## 核心观点\n\n{recommendation.thesis.summary}\n\n"
            f"## 财务/协议\n\n{recommendation.thesis.financials_and_growth}\n\n"
            f"## 产品与竞争\n\n{recommendation.thesis.products_or_protocol}\n\n"
            f"{recommendation.thesis.competition}\n\n"
            f"## 估值/代币经济\n\n{recommendation.thesis.valuation_or_tokenomics}\n\n"
            f"## 催化剂\n\n"
            + "\n".join(f"- {item}" for item in recommendation.thesis.catalysts)
            + "\n\n## 风险\n\n"
            + "\n".join(f"- {item}" for item in recommendation.thesis.risks)
            + "\n\n## 失效条件\n\n"
            + "\n".join(f"- {item}" for item in recommendation.thesis.invalidation_conditions)
            + f"\n\n## 证据质量提示\n\n{warning_lines or '- 无'}"
            + f"\n\n## 逐观点证据核验\n\n{claim_checks or '- 未完成'}"
            + f"\n\n## 证据\n\n{citations}\n\n> 仅用于研究与模拟，不构成投资建议。\n"
        )
        path.write_text(content, encoding="utf-8")
        return path
