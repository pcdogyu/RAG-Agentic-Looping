from __future__ import annotations

import json
import re
import threading
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Literal, TypedDict
from uuid import UUID, uuid4

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AnalysisStep,
    AssetClass,
    AssetRef,
    Evidence,
    NewsEvent,
    NewsItem,
    Recommendation,
    ResearchRun,
    RunStatus,
    SourceQuality,
    Thesis,
    as_utc,
    rating_for,
    utc_now,
)
from backend.app.llm import LlmGateway, gateway
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.mcp_registry import SearchRequest, search_enabled_sources_sync
from backend.app.services.retrieval import RetrievalService
from backend.app.services.source_lineage import (
    canonicalize_url,
    enrich_news_lineage,
    independent_evidence_groups,
    normalize_text,
    source_group,
)
from backend.app.storage import (
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
    confidence: float = Field(default=0.3, ge=0, le=1)
    bull_probability: float = Field(default=0.33, ge=0, le=1)
    base_probability: float = Field(default=0.34, ge=0, le=1)
    bear_probability: float = Field(default=0.33, ge=0, le=1)
    valuation_low: float | None = None
    valuation_high: float | None = None


class VerificationOutput(BaseModel):
    evidence_complete: bool
    missing_requirements: list[str] = Field(default_factory=list)
    contradictions: list[str] = Field(default_factory=list)
    unsupported_claims: list[str] = Field(default_factory=list)


class CloudVerification(BaseModel):
    approved: bool = False
    confidence: float = Field(default=0, ge=0, le=1)
    contradictions: list[str] = Field(default_factory=list)


class ResearchState(TypedDict, total=False):
    run: dict[str, Any]
    event: dict[str, Any] | None
    research_data: dict[str, Any]
    evidence: list[dict[str, Any]]
    retrieved_context: list[dict[str, Any]]
    draft: dict[str, Any]
    verification: dict[str, Any]
    verification_round: int
    acquisition_attempts: int
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
        graph.add_node("acquire_evidence", self._acquire_evidence)
        graph.add_node("revise", self._revise)
        graph.add_node("finalize", self._finalize)
        graph.add_edge(START, "gather")
        graph.add_edge("gather", "draft")
        graph.add_edge("draft", "verify")
        graph.add_conditional_edges(
            "verify",
            self._route_after_verification,
            {
                "acquire_evidence": "acquire_evidence",
                "revise": "revise",
                "finalize": "finalize",
            },
        )
        graph.add_edge("acquire_evidence", "revise")
        graph.add_edge("revise", "verify")
        graph.add_edge("finalize", END)
        return graph.compile(checkpointer=self.checkpointer)

    def run(
        self,
        asset: AssetRef,
        event: NewsEvent | None = None,
        as_of: datetime | None = None,
        historical_replay: bool | None = None,
        queued_run: ResearchRun | None = None,
    ) -> ResearchRun:
        run = queued_run or ResearchRun(
            event_id=event.id if event else None,
            asset=asset,
            as_of=as_of or utc_now(),
            historical_replay=bool(historical_replay),
            analysis_steps=[
                *(event.analysis_steps if event else []),
                AnalysisStep(
                    phase="research_queue",
                    executor="celery",
                    summary=f"已为 {asset.symbol} 创建研究任务。",
                ),
            ],
        )
        if historical_replay is not None:
            run.historical_replay = historical_replay
        save_run(self.db, run)
        state: ResearchState = {
            "run": run.model_dump(mode="json"),
            "event": event.model_dump(mode="json") if event else None,
            "verification_round": 0,
            "acquisition_attempts": 0,
            "historical_replay": run.historical_replay,
        }
        try:
            final_state = self.graph.invoke(
                state,
                config={"configurable": {"thread_id": str(run.id)}},
                durability="sync",
            )
            return ResearchRun.model_validate(final_state["run"])
        except Exception as exc:
            self.db.rollback()
            failed = get_run(self.db, run.id) or ResearchRun.model_validate(state["run"])
            failed.status = RunStatus.FAILED
            failed.error = f"{type(exc).__name__}: {exc}"
            failed.updated_at = utc_now()
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
        evidence = self._build_evidence(run, state.get("event"), research_data)
        run.evidence = evidence
        save_run(self.db, run)
        event = NewsEvent.model_validate(state["event"]) if state.get("event") else None
        query = f"{run.asset.name} {run.asset.symbol} {event.headline if event else ''}"
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
                },
            )
        )
        save_run(self.db, run)
        return {
            "run": run.model_dump(mode="json"),
            "research_data": research_data,
            "evidence": [item.model_dump(mode="json") for item in evidence],
            "retrieved_context": retrieved_context,
        }

    def _build_evidence(
        self,
        run: ResearchRun,
        event_payload: dict[str, Any] | None,
        data: dict[str, Any],
    ) -> list[Evidence]:
        evidence: list[Evidence] = []
        event = NewsEvent.model_validate(event_payload) if event_payload else None
        if event:
            for item_id in event.news_item_ids:
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

    def _draft(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        evidence = state.get("evidence", [])
        asset_template = (
            "股票：历史同类事件、营收增长、利润现金流、产品、竞争、估值、催化和风险。"
            if run.asset.asset_class is AssetClass.EQUITY
            else "加密资产：供给解锁、链上活跃、协议费用、TVL、开发治理、流动性、安全监管。"
        )
        prompt = (
            f"研究对象：{run.asset.model_dump_json()}\n研究截止：{run.as_of.isoformat()}\n"
            f"报告模板：{asset_template}\n"
            "证据："
            f"{json.dumps(evidence, ensure_ascii=False)[: self.settings.research_prompt_evidence_chars]}\n"
            "混合检索上下文："
            f"{json.dumps(state.get('retrieved_context', []), ensure_ascii=False)[: self.settings.research_prompt_context_chars]}\n"
            "只能引用 evidence_ids 中存在的证据。证据不足时降低 confidence 和 score，不得编造。"
        )
        draft_error: str | None = None
        try:
            draft = self.llm.generate_json(
                model=self.settings.ollama_research_model,
                system="你是证据优先的投资研究员。区分事实、推断和未知，不给实盘指令。",
                prompt=prompt,
                schema=DraftOutput,
                operation="report_drafting",
                entity_type="research_run",
                entity_id=run.id,
            )
        except Exception as exc:
            draft_error = type(exc).__name__
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
                    "citation_count": len(draft_output.evidence_ids),
                },
            )
        )
        save_run(self.db, run)
        return {"run": run.model_dump(mode="json"), "draft": draft_output.model_dump(mode="json")}

    @staticmethod
    def _fallback_draft(run: ResearchRun, evidence: list[dict[str, Any]]) -> DraftOutput:
        ids = [item["id"] for item in evidence]
        return DraftOutput(
            summary=f"已收集 {len(evidence)} 条与 {run.asset.name} 相关的证据，但本地研究模型不可用或证据不足。",
            catalysts=[],
            risks=["模型综合分析尚未完成", "当前证据可能不完整"],
            invalidation_conditions=["补充官方披露后重新评估"],
            evidence_ids=ids,
            score=0,
            confidence=min(0.45, len(evidence) * 0.1),
        )

    def _verify(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        draft = DraftOutput.model_validate(state["draft"])
        evidence = [Evidence.model_validate(item) for item in state.get("evidence", [])]
        available_ids = {str(item.id) for item in evidence}
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
            if not value.strip():
                missing.append(name)
        if not draft.risks:
            missing.append("risks")
        if not draft.invalidation_conditions:
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

        complete = not missing and not unsupported and not contradictions
        verification = VerificationOutput(
            evidence_complete=complete,
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
                status="completed" if complete else "incomplete",
                executor="evidence-gate",
                model="evidence-gate:v1",
                summary=(
                    f"第 {round_number} 轮校验{'通过' if complete else '未通过'}："
                    f"缺失 {len(missing)} 项、矛盾 {len(contradictions)} 项、无效引用 {len(unsupported)} 项。"
                ),
                metrics={
                    "round": round_number,
                    "evidence_complete": complete,
                    "missing_requirements": missing,
                    "contradictions": contradictions,
                    "unsupported_claims": unsupported,
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
        verification = VerificationOutput.model_validate(state["verification"])
        draft = DraftOutput.model_validate(state["draft"])
        can_retry = state["verification_round"] < self.settings.max_verification_rounds
        if (
            not state.get("historical_replay")
            and SOURCE_GATE in verification.missing_requirements
            and state.get("acquisition_attempts", 0) < 1
            and can_retry
        ):
            return "acquire_evidence"
        if (not verification.evidence_complete or abs(draft.score) >= 60) and can_retry:
            return "revise"
        return "finalize"

    def _acquire_evidence(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        event = NewsEvent.model_validate(state["event"]) if state.get("event") else None
        verification = VerificationOutput.model_validate(state["verification"])
        evidence = [Evidence.model_validate(item) for item in state.get("evidence", [])]
        attempts = state.get("acquisition_attempts", 0) + 1
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
        prompt = (
            f"当前报告：{current.model_dump_json()}\n验证结果：{verification.model_dump_json()}\n"
            "可用证据："
            f"{json.dumps(state.get('evidence', []), ensure_ascii=False)[: self.settings.research_prompt_evidence_chars]}\n"
            "修订报告。不能通过编造来补足缺失来源；无法补足时保持低置信度和中性分数。"
        )
        revision_error: str | None = None
        try:
            revised = self.llm.generate_json(
                model=self.settings.ollama_research_model,
                system="你是投资研究报告修订器，只能使用给定证据。",
                prompt=prompt,
                schema=DraftOutput,
                operation="report_revision",
                entity_type="research_run",
                entity_id=run.id,
            )
            revised_output = DraftOutput.model_validate(revised)
        except Exception as exc:
            revision_error = type(exc).__name__
            current.confidence = min(current.confidence, 0.5)
            current.score = 0
            revised_output = current
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
                metrics={"confidence": revised_output.confidence, "score": revised_output.score},
            )
        )
        save_run(self.db, run)
        return {
            "run": run.model_dump(mode="json"),
            "draft": revised_output.model_dump(mode="json"),
        }

    def _finalize(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        draft = DraftOutput.model_validate(state["draft"])
        verification = VerificationOutput.model_validate(state["verification"])
        complete = verification.evidence_complete
        confidence = draft.confidence if complete else min(draft.confidence, 0.54)
        score = draft.score if complete else 0
        high_impact = abs(score) >= 60
        cloud_approved = False
        if (
            high_impact
            and state.get("verification_round", 0) >= 2
            and self.settings.cloud_verifier_enabled
        ):
            try:
                cloud = self.llm.cloud_verify(
                    (
                        f"复核高影响研究结论：{draft.model_dump_json()}\n"
                        f"验证结果：{verification.model_dump_json()}\n"
                        "证据："
                        f"{json.dumps(state.get('evidence', []), ensure_ascii=False)[: self.settings.research_prompt_evidence_chars]}\n"
                        "只判断给定证据是否支持该方向与置信度，不补充外部事实。"
                    ),
                    CloudVerification,
                )
                cloud_result = CloudVerification.model_validate(cloud) if cloud else None
                cloud_approved = bool(
                    cloud_result
                    and cloud_result.approved
                    and cloud_result.confidence >= self.settings.minimum_directional_confidence
                    and not cloud_result.contradictions
                )
                run.analysis_steps.append(
                    AnalysisStep(
                        phase="cloud_verification",
                        status="completed" if cloud_approved else "incomplete",
                        executor="cloud-llm",
                        model=self.settings.cloud_llm_model,
                        summary=f"高影响结论云复核{'通过' if cloud_approved else '未通过'}。",
                        metrics={
                            "approved": cloud_approved,
                            "confidence": cloud_result.confidence if cloud_result else 0,
                            "contradictions": cloud_result.contradictions if cloud_result else [],
                        },
                    )
                )
            except Exception as exc:
                cloud_approved = False
                run.analysis_steps.append(
                    AnalysisStep(
                        phase="cloud_verification",
                        status="failed",
                        executor="cloud-llm",
                        model=self.settings.cloud_llm_model,
                        summary=f"高影响结论云复核不可用（{type(exc).__name__}），方向评级已被门控。",
                    )
                )
        directional_gate = complete and (not high_impact or cloud_approved)
        probabilities = [draft.bull_probability, draft.base_probability, draft.bear_probability]
        total = sum(probabilities)
        if total <= 0:
            probabilities = [0.33, 0.34, 0.33]
        else:
            probabilities = [value / total for value in probabilities]
        valid_evidence_ids = {str(item.id): item.id for item in run.evidence}
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
            evidence_ids=[
                valid_evidence_ids[item]
                for item in draft.evidence_ids
                if item in valid_evidence_ids
            ],
        )
        recommendation = Recommendation(
            run_id=run.id,
            asset=run.asset,
            score=score,
            rating=rating_for(score, confidence, directional_gate),
            confidence=confidence,
            bull_probability=probabilities[0],
            base_probability=probabilities[1],
            bear_probability=probabilities[2],
            horizon_days=90,
            valuation_low=draft.valuation_low,
            valuation_high=draft.valuation_high,
            thesis=thesis,
            as_of=run.as_of,
            evidence_complete=complete,
        )
        run.recommendation = recommendation
        run.status = RunStatus.COMPLETED if complete else RunStatus.INSUFFICIENT_EVIDENCE
        run.updated_at = utc_now()
        run.analysis_steps.append(
            AnalysisStep(
                phase="finalization",
                status="completed" if complete else "incomplete",
                executor="rating-engine",
                model="rating-gate:v1",
                summary=(
                    f"最终评级 {recommendation.rating.value}，方向分数 {score}，"
                    f"置信度 {confidence:.0%}，证据{'完整' if complete else '不足'}。"
                ),
                metrics={
                    "rating": recommendation.rating.value,
                    "score": score,
                    "confidence": confidence,
                    "evidence_complete": complete,
                    "cloud_approved": cloud_approved,
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
        citations = "\n".join(
            f"- [{item.source_name}]({item.source_url}) — {item.claim}" for item in run.evidence
        )
        content = (
            f"# {run.asset.name} ({run.asset.symbol})\n\n"
            f"- 评级：{recommendation.rating.value}\n"
            f"- 分数：{recommendation.score}\n"
            f"- 置信度：{recommendation.confidence:.0%}\n"
            f"- 截止时间：{run.as_of.isoformat()}\n"
            f"- 证据完整：{'是' if recommendation.evidence_complete else '否'}\n\n"
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
            + f"\n\n## 证据\n\n{citations}\n\n> 仅用于研究与模拟，不构成投资建议。\n"
        )
        path.write_text(content, encoding="utf-8")
        return path
