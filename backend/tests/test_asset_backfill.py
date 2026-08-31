from datetime import timedelta
from types import SimpleNamespace

import pytest

from backend.app import worker
from backend.app.domain import EventType, NewsEvent, SourceQuality, utc_now
from backend.app.storage import save_event


def test_asset_mapping_backfill_processes_events_in_ten_item_batches(
    db, monkeypatch
):
    now = utc_now()
    for index in range(12):
        save_event(
            db,
            NewsEvent(
                news_item_ids=[],
                headline=f"Unmapped event {index}",
                event_type=EventType.OTHER,
                entities=[],
                direct_impact="No prior asset mapping",
                source_quality=SourceQuality.PROFESSIONAL,
                published_at=now - timedelta(minutes=index),
                observed_at=now - timedelta(minutes=index),
                as_of=now - timedelta(minutes=index),
            ),
        )

    queued: list[str] = []
    monkeypatch.setattr(worker, "_redis_client", lambda: object())
    monkeypatch.setattr(worker, "list_model_task_records", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        worker,
        "enqueue_asset_mapping",
        lambda _db, event, **_kwargs: queued.append(str(event.id)) or str(event.id),
    )
    monkeypatch.setattr(worker.backfill_asset_mappings, "update_state", lambda **_kwargs: None)

    continuations: list[dict] = []
    monkeypatch.setattr(
        worker.backfill_asset_mappings,
        "apply_async",
        lambda **kwargs: continuations.append(kwargs),
    )

    with pytest.raises(worker.Ignore):
        worker.backfill_asset_mappings.run(days=30)

    assert len(queued) == 10
    assert continuations[0]["countdown"] == 2
    assert continuations[0]["queue"] == "io"
    assert continuations[0]["kwargs"]["stats"] == {
        "scanned": 10,
        "queued": 10,
        "skipped": 0,
        "failed": 0,
    }


def test_asset_mapping_backfill_waits_when_mapping_or_research_queue_is_full(
    monkeypatch,
):
    monkeypatch.setattr(worker, "_redis_client", lambda: object())
    monkeypatch.setattr(
        worker,
        "list_model_task_records",
        lambda lane, **_kwargs: [SimpleNamespace(status="queued")] * (
            10 if lane == "assist" else 0
        ),
    )
    states: list[dict] = []
    monkeypatch.setattr(
        worker.backfill_asset_mappings,
        "update_state",
        lambda **kwargs: states.append(kwargs),
    )
    continuations: list[dict] = []
    monkeypatch.setattr(
        worker.backfill_asset_mappings,
        "apply_async",
        lambda **kwargs: continuations.append(kwargs),
    )

    with pytest.raises(worker.Ignore):
        worker.backfill_asset_mappings.run(days=30)

    assert states[0]["state"] == "PROGRESS"
    assert states[0]["meta"]["phase"] == "waiting_for_capacity"
    assert states[0]["meta"]["mapping_depth"] == 10
    assert continuations[0]["countdown"] == 60
    assert continuations[0]["queue"] == "io"
