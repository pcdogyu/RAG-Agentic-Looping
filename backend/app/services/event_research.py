from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import (
    AnalysisStep,
    EventReport,
    EventResearchRun,
    Evidence,
    NewsEvent,
    RunStatus,
    SourceQuality,
)
from backend.app.llm import LlmGateway, gateway
from backend.app.storage import get_news, save_event_research_run


class EventReportDraft(BaseModel):
    summary: str
    affected_markets: list[str] = Field(default_factory=list)
    affected_sectors: list[str] = Field(default_factory=list)
    scenarios: list[str] = Field(default_factory=list)
    catalysts: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
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
                    "independent_sources": len(
                        {item.independent_group for item in run.evidence}
                    ),
                },
            )
        )
        save_event_research_run(self.db, run)

        draft = self._generate_draft(event, run)
        run.analysis_steps.append(
            AnalysisStep(
                phase="event_report_drafting",
                executor="ollama",
                model=self.settings.ollama_research_model,
                summary=(
                    f"已生成中性事件研报草稿，置信度 {draft.confidence:.0%}，"
                    f"引用 {len(draft.evidence_ids)} 条证据。"
                ),
                metrics={
                    "confidence": draft.confidence,
                    "citation_count": len(draft.evidence_ids),
                },
            )
        )
        save_event_research_run(self.db, run)

        complete, missing, contradictions = self._verify(run, draft)
        run.verification_round = 1
        run.missing_requirements = missing
        run.contradictions = contradictions
        run.analysis_steps.append(self._verification_step(1, complete, missing, contradictions))
        if (
            not complete
            and self.settings.max_verification_rounds > 1
            and self._draft_can_be_repaired(missing, contradictions)
        ):
            draft = self._revise(event, run, draft, missing, contradictions)
            run.analysis_steps.append(
                AnalysisStep(
                    phase="event_report_revision",
                    executor="ollama",
                    model=self.settings.ollama_research_model,
                    summary="已根据结构和引用校验结果修订中性事件研报。",
                )
            )
            complete, missing, contradictions = self._verify(run, draft)
            run.verification_round = 2
            run.missing_requirements = missing
            run.contradictions = contradictions
            run.analysis_steps.append(
                self._verification_step(2, complete, missing, contradictions)
            )

        valid_ids = {str(item.id): item.id for item in run.evidence}
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
        )
        run.status = RunStatus.COMPLETED if complete else RunStatus.INSUFFICIENT_EVIDENCE
        run.analysis_steps.append(
            AnalysisStep(
                phase="event_report_finalization",
                status="completed" if complete else "incomplete",
                executor="event-evidence-gate",
                model="event-evidence-gate:v1",
                summary=(
                    f"中性事件研报已定稿，置信度 {run.report.confidence:.0%}，"
                    f"证据{'完整' if complete else '不足'}；不会生成资产评级或模拟交易。"
                ),
                metrics={
                    "confidence": run.report.confidence,
                    "evidence_complete": complete,
                },
            )
        )
        save_event_research_run(self.db, run)
        self._write_report(event, run)
        return run

    def _build_evidence(
        self, run: EventResearchRun, event: NewsEvent
    ) -> list[Evidence]:
        evidence: list[Evidence] = []
        for news_id in event.news_item_ids:
            item = get_news(self.db, news_id)
            if not item:
                continue
            host = urlparse(item.url).hostname or item.source
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
                    independent_group=host.lower(),
                )
            )
        return evidence

    def _generate_draft(
        self, event: NewsEvent, run: EventResearchRun
    ) -> EventReportDraft:
        prompt = (
            f"事件：{event.model_dump_json(exclude={'analysis_steps', 'candidates'})}\n"
            f"研究截止：{run.as_of.isoformat()}\n"
            f"证据：{json.dumps([item.model_dump(mode='json') for item in run.evidence], ensure_ascii=False)[:24000]}\n"
            "生成不绑定证券的中性事件研报。区分事实、情景和未知；不得给个股评级、方向分数或交易指令。"
            "只能引用给定 evidence_ids；证据不足时降低 confidence 并列入 unresolved_questions。"
        )
        payload = self.llm.generate_json(
            model=self.settings.ollama_research_model,
            system="你是证据优先的宏观和行业事件研究员，不提供实盘指令。",
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
            f"证据：{json.dumps([item.model_dump(mode='json') for item in run.evidence], ensure_ascii=False)[:24000]}\n"
            "只修复结构或引用问题。不能通过编造补足独立来源，无法确认的内容写入待验证问题。"
        )
        payload = self.llm.generate_json(
            model=self.settings.ollama_research_model,
            system="你是中性事件研报修订器，只能使用给定证据。",
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
        available_ids = {str(item.id) for item in run.evidence}
        cited_ids = set(draft.evidence_ids)
        if not cited_ids:
            missing.append("evidence citations")
        unknown = cited_ids - available_ids
        if unknown:
            missing.append(f"unknown evidence ids: {sorted(unknown)}")
        if any(
            item.published_at > run.as_of
            or item.observed_at > run.as_of
            or item.as_of > run.as_of
            for item in run.evidence
        ):
            contradictions.append("point-in-time boundary violation")
        official = any(item.source_quality is SourceQuality.OFFICIAL for item in run.evidence)
        independent = {item.independent_group for item in run.evidence if item.independent_group}
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
        content = (
            f"# {event.headline}\n\n"
            f"- 类型：{event.event_type.value}\n"
            f"- 置信度：{report.confidence:.0%}\n"
            f"- 证据完整：{'是' if report.evidence_complete else '否'}\n"
            f"- 截止时间：{run.as_of.isoformat()}\n\n"
            f"## 事件摘要\n\n{report.summary}\n\n"
            "## 受影响市场与行业\n\n"
            + "\n".join(f"- {item}" for item in [*report.affected_markets, *report.affected_sectors])
            + "\n\n## 情景\n\n"
            + "\n".join(f"- {item}" for item in report.scenarios)
            + "\n\n## 催化剂\n\n"
            + "\n".join(f"- {item}" for item in report.catalysts)
            + "\n\n## 风险\n\n"
            + "\n".join(f"- {item}" for item in report.risks)
            + "\n\n## 待验证问题\n\n"
            + "\n".join(f"- {item}" for item in report.unresolved_questions)
            + f"\n\n## 证据\n\n{citations}\n\n> 中性事件研究，不构成投资建议。\n"
        )
        path.write_text(content, encoding="utf-8")
        return path
