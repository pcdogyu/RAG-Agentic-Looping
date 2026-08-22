import re
from datetime import UTC, datetime
from hashlib import sha256

from backend.app.config import Settings
from backend.app.domain import EventResearchRun, NewsEvent, NewsItem, SourceQuality
from backend.app.main import _analysis_logs
from backend.app.services.event_research import EventReportDraft, EventResearchService
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
            confidence=0.72,
        ).model_dump(mode="json")


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
