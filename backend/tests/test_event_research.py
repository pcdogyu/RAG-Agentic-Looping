import re
from datetime import UTC, datetime
from hashlib import sha256

from backend.app.config import Settings
from backend.app.domain import (
    EventResearchRun,
    NewsEvent,
    NewsItem,
    RunStatus,
    SourceQuality,
    TargetType,
)
from backend.app.main import _analysis_logs
from backend.app.services.event_research import EventReportDraft, EventResearchService
from backend.app.services.macro_impacts import TargetImpactDraft
from backend.app.services.mcp_registry import SearchResult
from backend.app.storage import (
    list_event_research_runs,
    list_recommendations,
    save_event,
    save_news,
)


class EventResearchLlm:
    def generate_json(self, *, prompt, **kwargs):
        evidence_id = re.search(r'"id":\s*"([0-9a-f-]{36})"', prompt).group(1)
        return EventReportDraft(
            summary="该事件可能影响区域风险偏好，但当前只有一个聚合来源。",
            affected_markets=["亚洲市场"],
            affected_sectors=["供应链"],
            scenarios=["事态缓和", "维持现状", "进一步升级"],
            catalysts=["新增官方通报"],
            risks=["单一来源可能不完整"],
            unresolved_questions=["是否有独立来源确认"],
            evidence_ids=[evidence_id],
            impacts=[
                TargetImpactDraft(
                    target_type=TargetType.OTHER,
                    target_name="区域风险偏好",
                    rationale="证据尚不足以绑定交易工具。",
                    evidence_ids=[evidence_id],
                    missing_information=["tradable_asset_path"],
                )
            ],
            confidence=0.72,
        ).model_dump(mode="json")


class AllEvidenceEventResearchLlm(EventResearchLlm):
    def generate_json(self, *, prompt, **kwargs):
        payload = super().generate_json(prompt=prompt, **kwargs)
        payload["evidence_ids"] = re.findall(r'"id":\s*"([0-9a-f-]{36})"', prompt)
        return payload


class EvidenceOnlyEventResearchLlm(EventResearchLlm):
    def generate_json(self, *, prompt, **kwargs):
        payload = super().generate_json(prompt=prompt, **kwargs)
        evidence_payload = prompt.split("证据：", 1)[-1]
        payload["evidence_ids"] = re.findall(
            r'"id":\s*"([0-9a-f-]{36})"', evidence_payload
        )
        return payload


class FailingEventResearchLlm:
    def __init__(self):
        self.calls = 0

    def generate_json(self, **kwargs):
        self.calls += 1
        raise TimeoutError("research model timeout")


def test_event_report_is_neutral_and_marks_single_source_evidence_incomplete(db, tmp_path):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    news = NewsItem(
        source="Aggregator",
        source_quality=SourceQuality.AGGREGATOR,
        title="全球供应链面临新的不确定性",
        summary="一项地区事件可能影响航运。",
        url="https://example.com/supply-chain",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"event-report").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="supply_chain",
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    save_news(db, news)
    save_event(db, event)
    run = EventResearchRun(event_id=event.id, as_of=observed)
    settings = Settings(
        fmp_access_token="",
        fmp_mcp_url="",
        reports_dir=tmp_path,
    )

    result = EventResearchService(db, settings, EventResearchLlm()).run(event, run)

    assert result.status.value == "insufficient_evidence"
    assert result.report is not None
    assert result.report.confidence == 0.54
    assert result.report.evidence_complete is False
    assert result.missing_requirements == ["one official source or two independent sources"]
    assert len(list_event_research_runs(db)) == 1
    assert list_recommendations(db) == []
    assert list(tmp_path.glob("event_*.md"))
    log = _analysis_logs(db, 1)[0]
    assert log["result"]["kind"] == "event_report"
    assert "rating" not in log["result"]
    assert log["asset"] is None


def test_event_model_timeout_returns_retryable_conservative_report(db, tmp_path):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    news = NewsItem(
        source="Official Bulletin",
        source_quality=SourceQuality.OFFICIAL,
        title="官方发布供应链更新",
        summary="官方披露了最新进展。",
        url="https://official.example/update",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"event-timeout").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="supply_chain",
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    save_news(db, news)
    save_event(db, event)

    llm = FailingEventResearchLlm()
    result = EventResearchService(
        db,
        Settings(_env_file=None, reports_dir=tmp_path),
        llm,
    ).run(event, EventResearchRun(event_id=event.id, as_of=observed))

    assert result.status.value == "insufficient_evidence"
    assert result.retryable_reason == "model_TimeoutError"
    assert result.report is not None
    assert result.report.confidence <= 0.45
    assert any(step.status == "fallback" for step in result.analysis_steps)
    assert llm.calls == 1


def test_syndicated_reprints_count_as_one_independent_source(db, tmp_path):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    news_items = [
        NewsItem(
            source=source,
            source_quality=SourceQuality.PROFESSIONAL,
            title=title,
            summary="Reuters reported the same supply-chain event.",
            url=url,
            published_at=observed,
            observed_at=observed,
            as_of=observed,
            content_hash=sha256(url.encode()).hexdigest(),
            raw_metadata={"original_source": "Reuters"},
        )
        for source, title, url in (
            ("Publisher A", "Supply-chain disruption reported", "https://a.example/story"),
            ("Publisher B", "New supply-chain disruption report", "https://b.example/story"),
        )
    ]
    for item in news_items:
        save_news(db, item)
    event = NewsEvent(
        news_item_ids=[item.id for item in news_items],
        headline="Supply-chain disruption reported",
        event_type="supply_chain",
        direct_impact="Shipping may be affected.",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    save_event(db, event)
    run = EventResearchRun(event_id=event.id, as_of=observed)
    settings = Settings(fmp_access_token="", fmp_mcp_url="", reports_dir=tmp_path)

    result = EventResearchService(db, settings, AllEvidenceEventResearchLlm()).run(event, run)

    assert len({item.independent_group for item in result.evidence}) == 1
    assert result.status.value == "insufficient_evidence"
    assert result.missing_requirements == ["one official source or two independent sources"]


def test_event_research_uses_one_bounded_web_supplement(monkeypatch, db, tmp_path):
    observed = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    news = NewsItem(
        source="Aggregator",
        source_quality=SourceQuality.AGGREGATOR,
        title="供应链事件需要独立验证",
        summary="当前只有一个聚合来源。",
        url="https://a.example/event",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"event-web-supplement").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="supply_chain",
        entities=["航运"],
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    save_news(db, news)
    save_event(db, event)
    calls = []

    def fake_search(request):
        calls.append(request)
        return (
            [
                SearchResult(
                    title="Independent shipping report",
                    url="https://b.example/independent-report",
                    snippet="An independent publisher confirms the disruption.",
                    source="Search MCP",
                    domain="b.example",
                    published_at=observed,
                )
            ],
            [],
        )

    monkeypatch.setattr(
        "backend.app.services.event_research.search_enabled_sources_sync", fake_search
    )
    result = EventResearchService(
        db,
        Settings(fmp_access_token="", fmp_mcp_url="", reports_dir=tmp_path),
        EvidenceOnlyEventResearchLlm(),
    ).run(event, EventResearchRun(event_id=event.id, as_of=observed))

    assert 1 <= len(calls) <= 3
    assert result.status.value == "completed", (
        result.missing_requirements,
        result.contradictions,
        [step.model_dump(mode="json") for step in result.analysis_steps],
    )
    assert len(result.evidence) == 2
    assert [step.phase for step in result.analysis_steps].count("web_search_verification") == 1


def test_historical_event_replay_never_calls_live_search(monkeypatch, db, tmp_path):
    observed = datetime(2025, 8, 22, 12, 0, tzinfo=UTC)
    news = NewsItem(
        source="Aggregator",
        source_quality=SourceQuality.AGGREGATOR,
        title="历史供应链事件",
        summary="当时只有一个已观察来源。",
        url="https://archive.example/event",
        published_at=observed,
        observed_at=observed,
        as_of=observed,
        content_hash=sha256(b"historical-event-replay").hexdigest(),
    )
    event = NewsEvent(
        news_item_ids=[news.id],
        headline=news.title,
        event_type="supply_chain",
        direct_impact=news.summary,
        source_quality=news.source_quality,
        published_at=observed,
        observed_at=observed,
        as_of=observed,
    )
    save_news(db, news)
    save_event(db, event)
    monkeypatch.setattr(
        "backend.app.services.event_research.search_enabled_sources_sync",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("historical replay attempted live search")
        ),
    )

    result = EventResearchService(
        db,
        Settings(fmp_access_token="", fmp_mcp_url="", reports_dir=tmp_path),
        EventResearchLlm(),
    ).run(
        event,
        EventResearchRun(
            event_id=event.id,
            as_of=observed,
            historical_replay=True,
        ),
    )

    assert result.status is not RunStatus.COMPLETED
    assert len(result.evidence) == 1
