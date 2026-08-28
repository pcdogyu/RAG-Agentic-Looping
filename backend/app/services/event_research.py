from __future__ import annotations

from pathlib import Path

from pydantic import Field
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AnalysisStep,
    AssetClass,
    AssetRef,
    EventReport,
    EventResearchRun,
    Evidence,
    NewsEvent,
    RunStatus,
    SourceQuality,
    TradeStatus,
    utc_now,
)
from backend.app.llm import LlmGateway, gateway
from backend.app.services.macro_impacts import (
    TARGET_SCORING_VERSION,
    EventImpactDraft,
    finalize_impacts,
    public_rule_catalog,
    rule_based_event_draft,
)
from backend.app.services.mcp_registry import SearchRequest, search_enabled_sources_sync
from backend.app.services.prompt_budget import compact_evidence
from backend.app.services.source_lineage import independent_evidence_groups, source_group
from backend.app.storage import get_news, list_assets, save_event_research_run


class EventReportDraft(EventImpactDraft):
    confidence: float = Field(default=0.3, ge=0, le=1)


class EventResearchService:
    def __init__(
        self,
        db: Session,
        settings: Settings | None = None,
        llm: LlmGateway | None = None,
    ) -> None:
        self.db = db
        self.settings = settings or get_settings()
        self.llm = llm or gateway

    def run(self, event: NewsEvent, queued_run: EventResearchRun) -> EventResearchRun:
        run = queued_run
        run.status = RunStatus.RUNNING
        run.error = None
        run.evidence = self._build_evidence(run, event)
        run.analysis_steps.append(
            AnalysisStep(
                phase="event_evidence_gathering",
                executor="event-news",
                summary=(
                    f"已从事件关联新闻收集 {len(run.evidence)} 条证据，"
                    f"覆盖 {len({item.independent_group for item in run.evidence})} 个独立来源。"
                ),
                metrics={
                    "evidence_count": len(run.evidence),
                    "independent_sources": len({item.independent_group for item in run.evidence}),
                },
            )
        )
        save_event_research_run(self.db, run)

        draft_error: str | None = None
        try:
            draft = self._generate_draft(event, run)
        except Exception as exc:
            draft_error = type(exc).__name__
            run.retryable_reason = f"model_{draft_error}"
            draft = self._fallback_draft(event, run)
        run.analysis_steps.append(
            AnalysisStep(
                phase="event_report_drafting",
                status="fallback" if draft_error else "completed",
                executor="rules" if draft_error else "ollama",
                model=(
                    "event-research-fallback:v1"
                    if draft_error
                    else self.settings.ollama_research_model
                ),
                summary=(
                    f"已生成逐目标事件研报草稿，包含 {len(draft.impacts)} 个目标，"
                    f"引用 {len(draft.evidence_ids)} 条证据。"
                    + (f" 模型不可用（{draft_error}），当前为保守结果。" if draft_error else "")
                ),
                metrics={
                    "confidence": draft.confidence,
                    "citation_count": len(draft.evidence_ids),
                },
            )
        )
        save_event_research_run(self.db, run)

        run.status = RunStatus.VERIFYING
        save_event_research_run(self.db, run)
        complete, missing, contradictions = self._verify(run, draft)
        run.verification_round = 1
        run.missing_requirements = missing
        run.contradictions = contradictions
        run.analysis_steps.append(self._verification_step(1, complete, missing, contradictions))
        source_gate = "one official source or two independent sources"
        needs_macro_context = any(
            action.action_type in {"sanctions", "strait_closure", "deescalation"}
            for action in event.actions
        )
        if (
            not run.historical_replay
            and (source_gate in missing or needs_macro_context)
        ):
            added, errors = self._supplement_web_evidence(event, run)
            run.analysis_steps.append(
                AnalysisStep(
                    phase="web_search_verification",
                    status="completed" if added else ("failed" if errors else "no_results"),
                    executor="mcp-search-registry",
                    summary=f"联网补证完成，接受 {added} 条带原始链接的搜索结果。",
                    metrics={"accepted_results": added, "errors": errors},
                )
            )
            save_event_research_run(self.db, run)
            if (
                added
                and self.settings.max_verification_rounds > 1
                and run.retryable_reason is None
            ):
                try:
                    draft = self._generate_draft(event, run)
                    run.retryable_reason = None
                except Exception as exc:
                    run.retryable_reason = f"model_{type(exc).__name__}"
                    draft = self._fallback_draft(event, run)
                complete, missing, contradictions = self._verify(run, draft)
                run.verification_round = 2
                run.missing_requirements = missing
                run.contradictions = contradictions
                run.analysis_steps.append(
                    self._verification_step(2, complete, missing, contradictions)
                )
        if (
            not complete
            and run.retryable_reason is None
            and run.verification_round < self.settings.max_verification_rounds
            and self._draft_can_be_repaired(missing, contradictions)
        ):
            try:
                draft = self._revise(event, run, draft, missing, contradictions)
                revision_error = None
                run.retryable_reason = None
            except Exception as exc:
                revision_error = type(exc).__name__
                run.retryable_reason = f"model_{revision_error}"
                draft.confidence = min(draft.confidence, 0.5)
            run.analysis_steps.append(
                AnalysisStep(
                    phase="event_report_revision",
                    status="fallback" if revision_error else "completed",
                    executor="rules" if revision_error else "ollama",
                    model=(
                        "event-revision-fallback:v1"
                        if revision_error
                        else self.settings.ollama_research_model
                    ),
                    summary=(
                        "已根据结构和引用校验结果修订逐目标事件研报。"
                        + (
                            f" 模型不可用（{revision_error}），已保留保守草稿。"
                            if revision_error
                            else ""
                        )
                    ),
                )
            )
            complete, missing, contradictions = self._verify(run, draft)
            run.verification_round = 2
            run.missing_requirements = missing
            run.contradictions = contradictions
            run.analysis_steps.append(self._verification_step(2, complete, missing, contradictions))

        valid_ids = {str(item.id): item.id for item in run.evidence}
        technical_failure = bool(draft_error) or any(
            step.phase in {"asset_mapping", "asset_mapping_7b"}
            and step.status in {"failed", "fallback"}
            for step in event.analysis_steps
        )
        macro_factors, impacts, fact_confidence, missing_information = finalize_impacts(
            draft,
            event=event,
            evidence=run.evidence,
            assets=self._asset_map(event),
            technical_failure=technical_failure,
        )
        if not complete:
            impacts = [
                item.model_copy(
                    update={
                        "missing_information": list(
                            dict.fromkeys([*item.missing_information, "evidence_gate"])
                        ),
                        "trade_status": TradeStatus.UNTRADEABLE,
                    }
                )
                for item in impacts
            ]
            missing_information = list(
                dict.fromkeys([*missing_information, "evidence_gate"])
            )
        run.report = EventReport(
            summary=draft.summary,
            affected_markets=draft.affected_markets,
            affected_sectors=draft.affected_sectors,
            scenarios=draft.scenarios,
            catalysts=draft.catalysts,
            risks=draft.risks,
            unresolved_questions=draft.unresolved_questions,
            evidence_ids=[valid_ids[item] for item in draft.evidence_ids if item in valid_ids],
            confidence=draft.confidence if complete else min(draft.confidence, 0.54),
            evidence_complete=complete,
            scoring_version=TARGET_SCORING_VERSION,
            fact_confidence=fact_confidence,
            macro_factors=macro_factors,
            impacts=impacts,
            trade_status=(
                TradeStatus.TRADEABLE
                if any(item.trade_status is TradeStatus.TRADEABLE for item in impacts)
                else TradeStatus.UNTRADEABLE
            ),
            missing_information=missing_information,
        )
        run.status = (
            RunStatus.COMPLETED
            if complete and not technical_failure
            else RunStatus.INSUFFICIENT_EVIDENCE
        )
        run.analysis_steps.append(
            AnalysisStep(
                phase="event_report_finalization",
                status="completed" if complete else "incomplete",
                executor="event-evidence-gate",
                model="event-evidence-gate:v1",
                summary=(
                    f"逐目标事件研报已定稿，共 {len(impacts)} 个目标，"
                    f"事件状态为 {run.report.trade_status.value}。"
                ),
                metrics={
                    "confidence": run.report.confidence,
                    "evidence_complete": complete,
                    "target_count": len(impacts),
                    "trade_status": run.report.trade_status.value,
                },
            )
        )
        save_event_research_run(self.db, run)
        self._write_report(event, run)
        return run

    def _fallback_draft(self, event: NewsEvent, run: EventResearchRun) -> EventReportDraft:
        draft = rule_based_event_draft(event, run.evidence, self._asset_map(event))
        return EventReportDraft.model_validate(
            {
                **draft.model_dump(mode="json"),
                "confidence": min(0.45, len(run.evidence) * 0.1),
            }
        )

    def _supplement_web_evidence(
        self, event: NewsEvent, run: EventResearchRun
    ) -> tuple[int, list[dict[str, str]]]:
        queries = [event.headline, *[f"{event.headline} {entity}" for entity in event.entities]][:3]
        seen = {item.source_url.rstrip("/") for item in run.evidence}
        added = 0
        errors: list[dict[str, str]] = []
        for query in queries:
            try:
                results, source_errors = search_enabled_sources_sync(
                    SearchRequest(query=query, language="zh-CN", limit=5)
                )
                errors.extend(source_errors)
            except Exception as exc:
                errors.append({"source": "registry", "error": f"{type(exc).__name__}: {exc}"[:500]})
                continue
            for result in results:
                url = result.url.rstrip("/")
                if url in seen or added >= 8:
                    continue
                observed = utc_now()
                if result.published_at and result.published_at > observed:
                    continue
                run.as_of = max(run.as_of, observed)
                run.evidence.append(
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
                seen.add(url)
                added += 1
            if added >= 8:
                break
        return added, errors

    def _asset_map(self, event: NewsEvent) -> dict[str, AssetRef]:
        direct_ids = {item.asset.asset_id for item in event.candidates[:3]}
        assets = {
            item.asset_id: item
            for item in list_assets(self.db)
            if item.asset_id in direct_ids
            or item.asset_class in {AssetClass.COMMODITY, AssetClass.FX}
        }
        assets.update(
            {item.asset.asset_id: item.asset for item in event.candidates[:3]}
        )
        return assets

    def _build_evidence(self, run: EventResearchRun, event: NewsEvent) -> list[Evidence]:
        evidence: list[Evidence] = []
        for news_id in event.news_item_ids:
            item = get_news(self.db, news_id)
            if not item:
                continue
            if run.historical_replay and (
                item.published_at > run.as_of
                or item.observed_at > run.as_of
                or item.as_of > run.as_of
            ):
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
                    excerpt=(item.summary or item.title)[:1000],
                    independent_group=source_group(item),
                )
            )
        return evidence

    def _generate_draft(self, event: NewsEvent, run: EventResearchRun) -> EventReportDraft:
        allowed_assets = [
            {
                "asset_id": asset.asset_id,
                "symbol": asset.symbol,
                "name": asset.name,
                "asset_class": asset.asset_class.value,
            }
            for asset in self._asset_map(event).values()
            if asset.asset_id in {item.asset.asset_id for item in event.candidates[:3]}
            or asset.symbol in {"CLUSD", "BZUSD", "ZGUSD"}
        ]
        prompt = (
            f"事实框架：{event.model_dump_json(exclude={'analysis_steps'})}\n"
            f"研究截止：{run.as_of.isoformat()}\n"
            f"版本化宏观规则：{public_rule_catalog()}\n"
            f"允许绑定的真实主数据工具：{allowed_assets}\n"
            f"证据：{compact_evidence(run.evidence, self.settings.research_prompt_evidence_chars, max_per_group=2)}\n"
            "生成最多 6 个互不重复的目标影响；每个方向必须对应 target_name，禁止全局方向、"
            "全局 score 或全局 rating。直接证券候选最多 3 个并优先占用目标名额。"
            "summary 只总结证据支持的事件事实，不得包含资产方向、分数、评级或交易结论。"
            "只输出传导六因子与置信四因子，最终分数、评级和可交易状态由程序计算。"
            "asset_id 只能从允许工具中选择；宏观目标没有真实工具时保持空。"
            "只能引用给定 evidence_ids 和 actions.id。未知制裁范围、生效日、支付结算、"
            "港口航运、实际供应或市场反应必须写入 missing_information。"
        )
        payload = self.llm.generate_json(
            model=self.settings.ollama_research_model,
            lane="research",
            system="你是证据优先的目标传导研究员；先判断对谁，再判断可交易资产路径。",
            prompt=prompt,
            schema=EventReportDraft,
            operation="event_report_drafting",
            entity_type="event_research_run",
            entity_id=run.id,
        )
        return EventReportDraft.model_validate(payload)

    def _revise(
        self,
        event: NewsEvent,
        run: EventResearchRun,
        draft: EventReportDraft,
        missing: list[str],
        contradictions: list[str],
    ) -> EventReportDraft:
        prompt = (
            f"事件：{event.headline}\n当前草稿：{draft.model_dump_json()}\n"
            f"缺失：{missing}\n矛盾：{contradictions}\n"
            f"证据：{compact_evidence(run.evidence, self.settings.research_prompt_evidence_chars, max_per_group=2)}\n"
            "只修复结构或引用问题。不能通过编造补足独立来源，无法确认的内容写入待验证问题。"
        )
        payload = self.llm.generate_json(
            model=self.settings.ollama_research_model,
            lane="research",
            system="你是逐目标事件研报修订器，只能使用给定证据。",
            prompt=prompt,
            schema=EventReportDraft,
            operation="event_report_revision",
            entity_type="event_research_run",
            entity_id=run.id,
        )
        return EventReportDraft.model_validate(payload)

    @staticmethod
    def _verify(
        run: EventResearchRun, draft: EventReportDraft
    ) -> tuple[bool, list[str], list[str]]:
        missing: list[str] = []
        contradictions: list[str] = []
        if not draft.summary.strip():
            missing.append("summary")
        if not draft.affected_markets and not draft.affected_sectors:
            missing.append("affected markets or sectors")
        if not draft.scenarios:
            missing.append("scenarios")
        if not draft.risks:
            missing.append("risks")
        if not draft.impacts:
            missing.append("target impacts")
        target_keys = {
            (item.target_type.value, item.asset_id or item.target_name.strip().casefold())
            for item in draft.impacts
        }
        if len(target_keys) != len(draft.impacts):
            missing.append("unique target impacts")
        available_ids = {str(item.id) for item in run.evidence}
        cited_ids = set(draft.evidence_ids)
        if not cited_ids:
            missing.append("evidence citations")
        unknown = cited_ids - available_ids
        if unknown:
            missing.append(f"unknown evidence ids: {sorted(unknown)}")
        if any(
            item.published_at > run.as_of or item.observed_at > run.as_of or item.as_of > run.as_of
            for item in run.evidence
        ):
            contradictions.append("point-in-time boundary violation")
        cited_evidence = [item for item in run.evidence if str(item.id) in cited_ids]
        official = any(item.source_quality is SourceQuality.OFFICIAL for item in cited_evidence)
        independent = independent_evidence_groups(cited_evidence)
        if not official and len(independent) < 2:
            missing.append("one official source or two independent sources")
        return not missing and not contradictions, missing, contradictions

    @staticmethod
    def _draft_can_be_repaired(missing: list[str], contradictions: list[str]) -> bool:
        source_gate = "one official source or two independent sources"
        return not contradictions and any(item != source_gate for item in missing)

    @staticmethod
    def _verification_step(
        round_number: int,
        complete: bool,
        missing: list[str],
        contradictions: list[str],
    ) -> AnalysisStep:
        return AnalysisStep(
            phase="event_report_verification",
            status="completed" if complete else "incomplete",
            executor="event-evidence-gate",
            model="event-evidence-gate:v1",
            summary=(
                f"第 {round_number} 轮事件研报校验{'通过' if complete else '未通过'}："
                f"缺失 {len(missing)} 项、矛盾 {len(contradictions)} 项。"
            ),
            metrics={
                "round": round_number,
                "evidence_complete": complete,
                "missing_requirements": missing,
                "contradictions": contradictions,
            },
        )

    def _write_report(self, event: NewsEvent, run: EventResearchRun) -> Path:
        self.settings.reports_dir.mkdir(parents=True, exist_ok=True)
        path = self.settings.reports_dir / f"event_{event.id}_{run.id}.md"
        report = run.report
        assert report is not None
        citations = "\n".join(
            f"- [{item.source_name}]({item.source_url}) — {item.claim}"
            for item in run.evidence
            if item.id in report.evidence_ids
        )
        impact_rows = "\n".join(
            "| "
            + " | ".join(
                [
                    item.target_name,
                    item.rating.value,
                    f"{item.score:+.2f}",
                    f"{item.confidence:.0%}",
                    " → ".join(item.transmission_path) or "—",
                    item.trade_status.value,
                    "、".join(item.missing_information) or "—",
                ]
            )
            + " |"
            for item in report.impacts
        )
        content = (
            f"# {event.headline}\n\n"
            f"- 类型：{event.event_type.value}\n"
            f"- 置信度：{report.confidence:.0%}\n"
            f"- 证据完整：{'是' if report.evidence_complete else '否'}\n"
            f"- 截止时间：{run.as_of.isoformat()}\n\n"
            f"## 事件摘要\n\n{report.summary}\n\n"
            "## 逐目标影响\n\n"
            "| 目标 | 评级 | 分数 | 方向置信度 | 传导路径 | 可交易状态 | 缺失信息 |\n"
            "|---|---:|---:|---:|---|---|---|\n"
            f"{impact_rows}\n\n"
            "## 受影响市场与行业\n\n"
            + "\n".join(
                f"- {item}" for item in [*report.affected_markets, *report.affected_sectors]
            )
            + "\n\n## 情景\n\n"
            + "\n".join(f"- {item}" for item in report.scenarios)
            + "\n\n## 催化剂\n\n"
            + "\n".join(f"- {item}" for item in report.catalysts)
            + "\n\n## 风险\n\n"
            + "\n".join(f"- {item}" for item in report.risks)
            + "\n\n## 待验证问题\n\n"
            + "\n".join(f"- {item}" for item in report.unresolved_questions)
            + f"\n\n## 证据\n\n{citations}\n\n> 目标传导研究，不构成投资建议。\n"
        )
        path.write_text(content, encoding="utf-8")
        return path
