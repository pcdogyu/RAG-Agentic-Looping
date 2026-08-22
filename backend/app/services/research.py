from __future__ import annotations

import json
import threading
from datetime import datetime
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
from backend.app.services.retrieval import RetrievalService
from backend.app.storage import get_evidence, get_run, list_news, save_recommendation, save_run

_checkpoint_setup_lock = threading.Lock()
_checkpoint_setup_done = False


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
        graph.add_node("revise", self._revise)
        graph.add_node("finalize", self._finalize)
        graph.add_edge(START, "gather")
        graph.add_edge("gather", "draft")
        graph.add_edge("draft", "verify")
        graph.add_conditional_edges(
            "verify",
            self._route_after_verification,
            {"revise": "revise", "finalize": "finalize"},
        )
        graph.add_edge("revise", "verify")
        graph.add_edge("finalize", END)
        return graph.compile(checkpointer=self.checkpointer)

    def run(
        self,
        asset: AssetRef,
        event: NewsEvent | None = None,
        as_of: datetime | None = None,
        queued_run: ResearchRun | None = None,
    ) -> ResearchRun:
        run = queued_run or ResearchRun(
            event_id=event.id if event else None,
            asset=asset,
            as_of=as_of or utc_now(),
            analysis_steps=[
                *(event.analysis_steps if event else []),
                AnalysisStep(
                    phase="research_queue",
                    executor="celery",
                    summary=f"已为 {asset.symbol} 创建研究任务。",
                ),
            ],
        )
        save_run(self.db, run)
        state: ResearchState = {
            "run": run.model_dump(mode="json"),
            "event": event.model_dump(mode="json") if event else None,
            "verification_round": 0,
            "historical_replay": as_of is not None,
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
        independent_sources = {item.independent_group for item in evidence if item.independent_group}
        run.analysis_steps.append(
            AnalysisStep(
                phase="evidence_gathering",
                status="fallback" if retrieval_error else "completed",
                executor="providers+retrieval",
                summary=(
                    f"已收集 {len(evidence)} 条证据，来自 {len(independent_sources)} 个独立来源。"
                    + (f" 混合检索不可用（{retrieval_error}），保留结构化证据。" if retrieval_error else "")
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
            news_by_id = {item.id: item for item in list_news(self.db, limit=500, as_of=run.as_of)}
            for item_id in event.news_item_ids:
                item = news_by_id.get(item_id)
                if not item:
                    continue
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=item.title,
                        source_name=item.source,
                        source_url=item.url,
                        source_quality=item.source_quality,
                        published_at=item.published_at,
                        observed_at=item.observed_at,
                        as_of=item.as_of,
                        excerpt=item.summary[:1000],
                        independent_group=item.raw_metadata.get("site") or item.source,
                    )
                )

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
                    domain in url.lower()
                    for domain in ("sec.gov", "cninfo.com.cn", "hkexnews.hk")
                )
                evidence.append(
                    Evidence(
                        run_id=run.id,
                        claim=f"SEC filing {filing.get('formType') or filing.get('type')}",
                        source_name=str(filing.get("source") or "Regulatory filing"),
                        source_url=url,
                        source_quality=(
                            SourceQuality.OFFICIAL
                            if official_domain
                            else SourceQuality.AGGREGATOR
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
            f"证据：{json.dumps(evidence, ensure_ascii=False)[:24000]}\n"
            f"混合检索上下文：{json.dumps(state.get('retrieved_context', []), ensure_ascii=False)[:12000]}\n"
            "只能引用 evidence_ids 中存在的证据。证据不足时降低 confidence 和 score，不得编造。"
        )
        draft_error: str | None = None
        try:
            draft = self.llm.generate_json(
                model=self.settings.ollama_research_model,
                system="你是证据优先的投资研究员。区分事实、推断和未知，不给实盘指令。",
                prompt=prompt,
                schema=DraftOutput,
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
                model=self.settings.ollama_research_model if not draft_error else "research-fallback:v1",
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
        if any(item.published_at > run.as_of or item.observed_at > run.as_of for item in evidence):
            contradictions.append("point-in-time boundary violation")

        official = any(item.source_quality is SourceQuality.OFFICIAL for item in evidence)
        independent_groups = {item.independent_group for item in evidence if item.independent_group}
        corroborated = official or len(independent_groups) >= 2
        if not corroborated:
            missing.append("one official source or two independent sources")

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

    def _route_after_verification(self, state: ResearchState) -> Literal["revise", "finalize"]:
        verification = VerificationOutput.model_validate(state["verification"])
        draft = DraftOutput.model_validate(state["draft"])
        if (
            (not verification.evidence_complete or abs(draft.score) >= 60)
            and state["verification_round"] < self.settings.max_verification_rounds
        ):
            return "revise"
        return "finalize"

    def _revise(self, state: ResearchState) -> dict[str, Any]:
        run = ResearchRun.model_validate(state["run"])
        current = DraftOutput.model_validate(state["draft"])
        verification = VerificationOutput.model_validate(state["verification"])
        prompt = (
            f"当前报告：{current.model_dump_json()}\n验证结果：{verification.model_dump_json()}\n"
            f"可用证据：{json.dumps(state.get('evidence', []), ensure_ascii=False)[:24000]}\n"
            "修订报告。不能通过编造来补足缺失来源；无法补足时保持低置信度和中性分数。"
        )
        revision_error: str | None = None
        try:
            revised = self.llm.generate_json(
                model=self.settings.ollama_research_model,
                system="你是投资研究报告修订器，只能使用给定证据。",
                prompt=prompt,
                schema=DraftOutput,
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
                model=self.settings.ollama_research_model if not revision_error else "revision-fallback:v1",
                summary=(
                    f"已根据校验结果修订报告，置信度调整为 {revised_output.confidence:.0%}。"
                    + (f" 模型不可用（{revision_error}），已强制采用中性保守结论。" if revision_error else "")
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
                        f"证据：{json.dumps(state.get('evidence', []), ensure_ascii=False)[:24000]}\n"
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
