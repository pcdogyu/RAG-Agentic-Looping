from datetime import UTC, datetime, timedelta
from hashlib import sha256

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from backend.app import worker
from backend.app.db import NewsProcessingOutboxRow, NewsProcessingRow
from backend.app.domain import EventType, NewsEvent, NewsItem, SourceQuality
from backend.app.main import app
from backend.app.services.news_processing import (
    DISPATCH_FAILED,
    DISPATCH_PENDING,
    QUEUED,
    claim_news_dispatches,
    mark_news_dispatch_failed,
    mark_news_dispatched,
    mark_news_processing,
    recover_orphaned_news,
    request_news_retry,
    stage_news_for_extraction,
)
from backend.app.storage import save_event, save_news

BASE_TIME = datetime(2026, 8, 29, 7, 0, tzinfo=UTC)


def news(title: str, *, minutes: int = 0) -> NewsItem:
    published_at = BASE_TIME + timedelta(minutes=minutes)
    return NewsItem(
        source="金十",
        source_quality=SourceQuality.PROFESSIONAL,
        title=title,
        summary=f"{title} 摘要",
        url=f"https://example.com/news/{minutes}/{title}",
        language="zh-CN",
        published_at=published_at,
        observed_at=published_at,
        as_of=published_at,
        content_hash=sha256(f"{minutes}:{title}".encode()).hexdigest(),
    )


def test_stage_news_atomically_creates_processing_state_and_outbox(db):
    item = news("长鑫存储起诉美国国防部")

    stored = stage_news_for_extraction(
        db,
        item,
        scan_task_id="scan-1",
        dispatch_delay_seconds=0,
    )

    processing = db.get(NewsProcessingRow, stored.id)
    outbox = db.scalar(
        select(NewsProcessingOutboxRow).where(
            NewsProcessingOutboxRow.news_id == stored.id
        )
    )
    assert processing is not None
    assert processing.status == DISPATCH_PENDING
    assert processing.scan_task_id == "scan-1"
    assert outbox is not None
    assert outbox.status == "pending"

    stage_news_for_extraction(db, item, scan_task_id="scan-2", dispatch_delay_seconds=0)
    assert db.scalar(select(func.count()).select_from(NewsProcessingOutboxRow)) == 1


def test_outbox_claim_tracks_dispatch_success_and_failure(db):
    first = stage_news_for_extraction(db, news("待成功派发"), dispatch_delay_seconds=0)
    second = stage_news_for_extraction(
        db,
        news("待失败派发", minutes=1),
        dispatch_delay_seconds=0,
    )

    claims = claim_news_dispatches(db, limit=10, now=BASE_TIME + timedelta(days=1))
    claims_by_news = {claim.news_id: claim for claim in claims}
    mark_news_dispatched(db, claims_by_news[first.id], "task-success")
    mark_news_dispatch_failed(db, claims_by_news[second.id], "broker unavailable")

    succeeded = db.get(NewsProcessingRow, first.id)
    failed = db.get(NewsProcessingRow, second.id)
    assert succeeded is not None and succeeded.status == QUEUED
    assert succeeded.celery_task_id == "task-success"
    assert failed is not None and failed.status == DISPATCH_FAILED
    assert failed.last_error == "broker unavailable"


def test_recovery_requeues_legacy_or_stale_news_but_not_processed_news(db):
    legacy = news("历史扫描遗漏", minutes=-30)
    stale = news("工作进程中断", minutes=-29)
    active = news("仍在正常抽取", minutes=-28)
    processed = news("已经生成事件", minutes=-27)
    for item in [legacy, stale, active, processed]:
        assert save_news(db, item)

    stale_heartbeat = BASE_TIME - timedelta(hours=1)
    mark_news_processing(db, stale.id, QUEUED)
    stale_row = db.get(NewsProcessingRow, stale.id)
    assert stale_row is not None
    stale_row.heartbeat_at = stale_heartbeat
    stale_row.updated_at = stale_heartbeat
    db.commit()
    mark_news_processing(db, active.id, QUEUED)
    save_event(
        db,
        NewsEvent(
            news_item_ids=[processed.id],
            headline=processed.title,
            event_type=EventType.OTHER,
            direct_impact=processed.summary,
            source_quality=processed.source_quality,
            published_at=processed.published_at,
            observed_at=processed.observed_at,
            as_of=processed.as_of,
        ),
    )

    result = recover_orphaned_news(
        db,
        now=BASE_TIME,
        grace_seconds=120,
        stale_seconds=600,
    )

    assert result == {"recovered": 2, "stale": 1}
    recovered_ids = {
        claim.news_id
        for claim in claim_news_dispatches(
            db,
            limit=10,
            now=BASE_TIME + timedelta(minutes=1),
        )
    }
    assert recovered_ids == {legacy.id, stale.id}


def test_manual_retry_rejects_an_already_active_news_item(db):
    item = news("正在处理")
    assert save_news(db, item)
    mark_news_processing(db, item.id, QUEUED)

    try:
        request_news_retry(db, item.id)
    except RuntimeError as exc:
        assert str(exc) == "news extraction is already active"
    else:
        raise AssertionError("active news retry should be rejected")


def test_news_retry_api_dispatches_by_stable_news_id(db, monkeypatch):
    item = news("长鑫存储人工重试")
    assert save_news(db, item)
    monkeypatch.setattr(
        worker,
        "enqueue_durable_news_retry",
        lambda news_id, force_asset_mapping: {
            "news_id": str(news_id),
            "task_id": "retry-task-1",
        },
    )

    with TestClient(app) as client:
        response = client.post(f"/api/v1/news/{item.id}/retry")

    assert response.status_code == 202
    assert response.json() == {
        "status": "queued",
        "task_id": "retry-task-1",
        "news_id": str(item.id),
        "title": item.title,
    }
