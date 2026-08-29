from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from backend.app.config import Settings
from backend.app.domain import (
    AnalysisStep,
    EvolutionCandidate,
    NewsEvent,
    ResearchRun,
    RunStatus,
    SourceQuality,
)
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.model_queue import (
    build_model_queue_overview,
    cancel_model_task,
    cancel_model_tasks,
    list_model_task_records,
    model_task_is_cancelled,
    record_model_task,
    stale_model_task_records,
    touch_model_task,
    update_model_task,
)
from backend.app.storage import save_event, save_evolution, save_run


class FakeRedis:
    def __init__(self, lists: dict[str, int] | None = None):
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists = lists or {}

    def ping(self):
        return True

    def hset(self, key, field, value):
        self.hashes.setdefault(str(key), {})[str(field)] = str(value)
        return 1

    def hget(self, key, field):
        return self.hashes.get(str(key), {}).get(str(field))

    def hgetall(self, key):
        return dict(self.hashes.get(str(key), {}))

    def hdel(self, key, *fields):
        bucket = self.hashes.get(str(key), {})
        for field in fields:
            bucket.pop(str(field), None)

    def expire(self, _key, _seconds):
        return True

    def llen(self, key):
        return self.lists.get(str(key), 0)


def _inference(capacity=1, queued=0, running=0):
    return {
        "capacity": capacity,
        "queued": queued,
        "running": running,
        "available": max(0, capacity - running),
        "observable": True,
    }


def _empty_extraction(now: datetime):
    return {
        "generated_at": now.isoformat(),
        "model": "qwen2.5:3b",
        "scan_task_id": None,
        "state": "idle",
        "total_items": 0,
        "counts": {
            "queued": 0,
            "running": 0,
            "retrying": 0,
            "completed": 0,
            "failed": 0,
        },
        "items": [],
        "error": None,
    }


def test_extract_overview_deduplicates_tracking_and_hides_stale_running_records(db):
    now = datetime(2026, 8, 28, 10, 0, tzinfo=UTC)
    redis = FakeRedis()
    record_model_task(
        "extract",
        task_id="current-extract",
        kind="news_extraction",
        title="新闻抽取任务",
        queued_at=now - timedelta(minutes=2),
        redis_client=redis,
    )
    update_model_task(
        "extract",
        "current-extract",
        status="running",
        occurred_at=now - timedelta(seconds=30),
        redis_client=redis,
    )
    record_model_task(
        "extract",
        task_id="stale-extract",
        kind="news_extraction",
        title="新闻抽取任务",
        queued_at=now - timedelta(minutes=10),
        redis_client=redis,
    )
    update_model_task(
        "extract",
        "stale-extract",
        status="running",
        occurred_at=now - timedelta(minutes=9),
        redis_client=redis,
    )
    extraction = _empty_extraction(now)
    extraction["state"] = "running"
    extraction["total_items"] = 1
    extraction["counts"]["running"] = 1
    extraction["items"] = [
        {
            "task_id": "current-extract",
            "instance_id": "extract-0",
            "news_id": "news-current",
            "title": "当前真实新闻标题",
            "source": "金十",
            "status": "running",
            "attempt": 1,
            "queued_at": (now - timedelta(minutes=2)).isoformat(),
            "started_at": (now - timedelta(seconds=30)).isoformat(),
            "updated_at": (now - timedelta(seconds=30)).isoformat(),
        }
    ]

    overview = build_model_queue_overview(
        db,
        extraction_queue=extraction,
        inference_statuses={
            "extract": _inference(running=1),
            "research": _inference(),
            "assist": _inference(),
            "code": _inference(),
        },
        threads={"extract": 4, "research": 8, "assist": 8, "code": 4},
        limit=500,
        settings=Settings(_env_file=None, model_task_lease_seconds=180),
        redis_client=redis,
        generated_at=now,
    )

    extract = overview.queues[0]
    assert extract.counts.running == 1
    assert [item.task_id for item in extract.tasks] == ["current-extract"]
    assert extract.tasks[0].title == "当前真实新闻标题"


def test_cancel_model_tasks_is_lane_scoped_and_terminal_safe():
    redis = FakeRedis()
    record_model_task(
        "assist",
        task_id="mapping-active",
        kind="asset_mapping",
        title="宏观新闻",
        redis_client=redis,
    )
    record_model_task(
        "assist",
        task_id="mapping-done",
        kind="asset_mapping",
        title="已完成映射",
        redis_client=redis,
    )
    update_model_task("assist", "mapping-done", status="completed", redis_client=redis)
    record_model_task(
        "code",
        task_id="code-active",
        kind="code_evolution",
        title="代码演进",
        redis_client=redis,
    )

    result = cancel_model_tasks("assist", redis_client=redis)
    update_model_task("assist", "mapping-active", status="completed", redis_client=redis)

    assert result.cancelled == 1
    assert result.celery_task_ids == ["mapping-active"]
    assert model_task_is_cancelled("assist", "mapping-active", redis_client=redis)
    assert not model_task_is_cancelled("code", "code-active", redis_client=redis)
    assert (
        json.loads(redis.hget("market-loop:model-queue:assist:tasks", "mapping-done"))["status"]
        == "completed"
    )


def test_clear_model_tasks_includes_failed_without_revoking_terminal_task():
    redis = FakeRedis()
    record_model_task(
        "assist",
        task_id="mapping-active",
        kind="asset_mapping",
        title="活动映射",
        redis_client=redis,
    )
    record_model_task(
        "assist",
        task_id="mapping-failed",
        kind="asset_mapping",
        title="失败映射",
        redis_client=redis,
    )
    update_model_task("assist", "mapping-failed", status="failed", redis_client=redis)

    result = cancel_model_tasks("assist", include_failed=True, redis_client=redis)

    assert result.cancelled == 2
    assert result.celery_task_ids == ["mapping-active"]
    assert model_task_is_cancelled("assist", "mapping-active", redis_client=redis)
    assert model_task_is_cancelled("assist", "mapping-failed", redis_client=redis)


def test_clear_model_tasks_can_target_one_instance_only():
    redis = FakeRedis()
    for task_id, instance_id in (("mapping-0", "assist-0"), ("mapping-1", "assist-1")):
        record_model_task(
            "assist",
            task_id=task_id,
            kind="asset_mapping",
            title=task_id,
            instance_id=instance_id,
            redis_client=redis,
        )

    result = cancel_model_tasks(
        "assist",
        instance_id="assist-0",
        redis_client=redis,
    )

    assert result.celery_task_ids == ["mapping-0"]
    assert model_task_is_cancelled("assist", "mapping-0", redis_client=redis)
    assert not model_task_is_cancelled("assist", "mapping-1", redis_client=redis)


def test_overview_splits_tasks_and_inference_counts_by_instance(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    redis = FakeRedis()
    record_model_task(
        "assist",
        task_id="mapping-0",
        kind="asset_mapping",
        title="实例零任务",
        instance_id="assist-0",
        queued_at=now,
        redis_client=redis,
    )
    record_model_task(
        "assist",
        task_id="mapping-1",
        kind="asset_mapping",
        title="实例一任务",
        instance_id="assist-1",
        queued_at=now,
        redis_client=redis,
    )
    update_model_task("assist", "mapping-1", status="failed", redis_client=redis)
    settings = Settings(
        _env_file=None,
        ollama_assist_base_urls="http://a0.invalid,http://a1.invalid",
        ollama_assist_max_concurrency=2,
    )
    inference = _inference(capacity=2, running=1)
    inference["instances"] = [
        {
            "id": "assist-0",
            "healthy": True,
            "model_available": True,
            "capacity": 1,
            "available": 0,
            "queued": 0,
            "running": 1,
            "observable": True,
        },
        {
            "id": "assist-1",
            "healthy": True,
            "model_available": True,
            "capacity": 1,
            "available": 1,
            "queued": 0,
            "running": 0,
            "observable": True,
        },
    ]
    inference["instance_count"] = 2
    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            "extract": _inference(),
            "research": _inference(),
            "assist": inference,
            "code": _inference(),
        },
        threads={"extract": 4, "research": 8, "assist": 8, "code": 4},
        limit=500,
        settings=settings,
        redis_client=redis,
        generated_at=now,
    )

    assist = overview.queues[2]
    assert [item.id for item in assist.instances] == ["assist-0", "assist-1"]
    assert assist.instances[0].counts.queued == 1
    assert assist.instances[0].counts.running == 1
    assert assist.instances[0].tasks[0].instance_id == "assist-0"
    assert assist.instances[1].counts.failed == 1
    assert assist.instances[1].tasks[0].instance_id == "assist-1"
    for field in ("queued", "running", "retrying", "completed", "failed"):
        assert getattr(assist.counts, field) == sum(
            getattr(instance.counts, field) for instance in assist.instances
        )


def test_overview_persists_a_deterministic_instance_for_legacy_tasks(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    redis = FakeRedis()
    record_model_task(
        "assist",
        task_id="legacy-mapping",
        kind="asset_mapping",
        title="旧共享队列任务",
        queued_at=now,
        redis_client=redis,
    )
    inference = _inference(capacity=2)
    inference["instances"] = [
        {
            "id": f"assist-{index}",
            "healthy": True,
            "model_available": True,
            "capacity": 1,
            "available": 1,
            "queued": 0,
            "running": 0,
            "observable": True,
        }
        for index in range(2)
    ]
    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            "extract": _inference(),
            "research": _inference(),
            "assist": inference,
            "code": _inference(),
        },
        threads={"extract": 4, "research": 8, "assist": 8, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=redis,
        generated_at=now,
    )

    task = overview.queues[2].tasks[0]
    assignment = json.loads(
        redis.hget(
            "market-loop:model-instance:assist:assignments",
            "legacy-mapping",
        )
    )
    assert task.instance_id in {"assist-0", "assist-1"}
    assert assignment["instance_id"] == task.instance_id


def test_cancel_one_extract_task_only_supersedes_the_selected_attempt():
    redis = FakeRedis()
    for task_id in ("extract-old", "extract-other"):
        record_model_task(
            "extract",
            task_id=task_id,
            kind="news_extraction",
            entity_id=f"news-{task_id}",
            title=task_id,
            redis_client=redis,
        )

    assert cancel_model_task("extract", "extract-old", redis_client=redis)
    assert model_task_is_cancelled("extract", "extract-old", redis_client=redis)
    assert not model_task_is_cancelled("extract", "extract-other", redis_client=redis)


def test_tracked_extract_retry_is_merged_into_extraction_overview(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    redis = FakeRedis()
    record_model_task(
        "extract",
        task_id="manual-extract",
        kind="news_extraction",
        entity_id="news-1",
        title="手动重试新闻",
        queued_at=now,
        redis_client=redis,
    )

    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            lane: _inference() for lane in ("extract", "research", "assist", "code")
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=redis,
        generated_at=now,
    )

    extract = overview.queues[0]
    assert extract.counts.queued == 1
    assert extract.tasks[0].task_id == "manual-extract"
    assert extract.tasks[0].title == "手动重试新闻"


def test_extraction_summary_keeps_completed_counts_and_recorded_metrics(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    extraction = _empty_extraction(now)
    extraction.update(
        {
            "counts": {
                "queued": 0,
                "running": 0,
                "retrying": 0,
                "completed": 10,
                "failed": 2,
            },
            "average_queue_duration_ms": 45_000,
            "average_execution_duration_ms": 18_000,
            "queue_duration_sample_count": 12,
            "execution_duration_sample_count": 10,
        }
    )

    overview = build_model_queue_overview(
        db,
        extraction_queue=extraction,
        inference_statuses={
            lane: _inference() for lane in ("extract", "research", "assist", "code")
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=FakeRedis(),
        generated_at=now,
    )

    extract = overview.queues[0]
    assert extract.counts.completed == 10
    assert extract.counts.failed == 2
    assert extract.metrics.average_queue_duration_ms == 45_000
    assert extract.metrics.average_execution_duration_ms == 18_000
    assert extract.metrics.queue_duration_sample_count == 12


def test_one_database_source_failure_stays_local_to_its_queue(db, monkeypatch):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    monkeypatch.setattr(
        "backend.app.services.model_queue.list_runs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db password secret")),
    )

    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            lane: _inference() for lane in ("extract", "research", "assist", "code")
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=FakeRedis(),
        generated_at=now,
    )

    assert overview.queues[0].error is None
    assert overview.queues[1].error == "标的研究任务状态暂时不可用。"
    assert overview.queues[2].error is None
    assert "secret" not in overview.queues[1].error


def test_assist_task_lifecycle_preserves_business_metadata_and_redacts_error():
    redis = FakeRedis()
    queued_at = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    record_model_task(
        "assist",
        task_id="mapping-1",
        kind="asset_mapping",
        entity_id="event-1",
        title="上市公司发布半年报",
        subtitle="earnings",
        queued_at=queued_at,
        redis_client=redis,
    )
    update_model_task(
        "assist",
        "mapping-1",
        status="running",
        attempt=2,
        occurred_at=queued_at + timedelta(seconds=30),
        redis_client=redis,
    )
    update_model_task(
        "assist",
        "mapping-1",
        status="failed",
        error="Authorization: Bearer cloud-secret-value",
        metrics={"proposed_count": 3, "verified_count": 1, "rejected_count": 2},
        occurred_at=queued_at + timedelta(minutes=2),
        redis_client=redis,
    )

    records = list_model_task_records(
        "assist",
        now=queued_at + timedelta(minutes=3),
        redis_client=redis,
    )

    assert len(records) == 1
    item = records[0]
    assert item.title == "上市公司发布半年报"
    assert item.status == "failed"
    assert item.attempt == 2
    assert item.queue_duration_ms == 30_000
    assert item.execution_duration_ms == 90_000
    assert item.metrics["verified_count"] == 1
    assert "cloud-secret-value" not in (item.error or "")
    assert "[REDACTED]" in (item.error or "")


def test_running_task_heartbeat_renews_lease_and_never_revives_terminal_task():
    redis = FakeRedis()
    started_at = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    record_model_task(
        "assist",
        task_id="mapping-heartbeat",
        kind="asset_mapping",
        title="租约测试",
        queued_at=started_at,
        redis_client=redis,
    )
    update_model_task(
        "assist",
        "mapping-heartbeat",
        status="running",
        instance_id="assist-0",
        occurred_at=started_at,
        redis_client=redis,
    )

    assert touch_model_task(
        "assist",
        "mapping-heartbeat",
        occurred_at=started_at + timedelta(seconds=150),
        redis_client=redis,
    )
    assert (
        stale_model_task_records(
            "assist",
            lease_seconds=180,
            now=started_at + timedelta(seconds=200),
            redis_client=redis,
        )
        == []
    )
    assert [
        item.task_id
        for item in stale_model_task_records(
            "assist",
            lease_seconds=180,
            now=started_at + timedelta(seconds=331),
            redis_client=redis,
        )
    ] == ["mapping-heartbeat"]

    update_model_task(
        "assist",
        "mapping-heartbeat",
        status="completed",
        occurred_at=started_at + timedelta(seconds=332),
        redis_client=redis,
    )
    assert not touch_model_task(
        "assist",
        "mapping-heartbeat",
        occurred_at=started_at + timedelta(seconds=400),
        redis_client=redis,
    )


def test_retrying_task_becomes_stale_after_lease_expires():
    redis = FakeRedis()
    started_at = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    record_model_task(
        "assist",
        task_id="mapping-retrying",
        kind="asset_mapping",
        title="重试租约测试",
        queued_at=started_at,
        redis_client=redis,
    )
    update_model_task(
        "assist",
        "mapping-retrying",
        status="retrying",
        attempt=2,
        occurred_at=started_at + timedelta(seconds=30),
        redis_client=redis,
    )

    assert stale_model_task_records(
        "assist",
        lease_seconds=180,
        now=started_at + timedelta(seconds=209),
        redis_client=redis,
    ) == []
    assert [
        item.task_id
        for item in stale_model_task_records(
            "assist",
            lease_seconds=180,
            now=started_at + timedelta(seconds=210),
            redis_client=redis,
        )
    ] == ["mapping-retrying"]


def test_terminal_task_records_expire_after_24_hours():
    redis = FakeRedis()
    queued_at = datetime(2026, 8, 24, 8, 0, tzinfo=UTC)
    record_model_task(
        "code",
        task_id="code-old",
        kind="code_evolution",
        title="旧演进任务",
        queued_at=queued_at,
        redis_client=redis,
    )
    update_model_task(
        "code",
        "code-old",
        status="rejected",
        occurred_at=queued_at + timedelta(minutes=5),
        redis_client=redis,
    )

    records = list_model_task_records(
        "code",
        now=queued_at + timedelta(hours=25),
        redis_client=redis,
    )

    assert records == []
    assert redis.hashes["market-loop:model-queue:code:tasks"] == {}


def test_overview_returns_four_queues_and_real_mapping_backlog(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    redis = FakeRedis({"mapping": 7, "evolution": 2})
    event = NewsEvent(
        news_item_ids=[],
        headline="沪电股份发布半年报",
        event_type="earnings",
        direct_impact="归母净利润增长",
        source_quality=SourceQuality.PROFESSIONAL,
        published_at=now - timedelta(minutes=10),
        observed_at=now - timedelta(minutes=10),
        as_of=now - timedelta(minutes=10),
        analysis_steps=[
            AnalysisStep(
                phase="asset_mapping_queue",
                status="queued",
                executor="celery",
                model="qwen2.5:7b",
                summary="已进入股票映射队列",
                occurred_at=now - timedelta(minutes=5),
            )
        ],
    )
    save_event(db, event)
    save_run(
        db,
        ResearchRun(
            asset=SEED_ASSETS[0],
            status=RunStatus.QUEUED,
            created_at=now - timedelta(minutes=20),
        ),
    )
    save_evolution(
        db,
        EvolutionCandidate(
            hypothesis="减少无效重试",
            target_metric="failure_rate",
            expected_improvement=0.1,
            branch="evolve/test",
            created_at=now - timedelta(minutes=3),
        ),
    )
    settings = Settings(
        _env_file=None,
        evolution_enabled=False,
        ollama_extract_num_threads=4,
        ollama_assist_num_threads=4,
        ollama_research_num_threads=16,
        ollama_code_num_threads=4,
    )
    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            "extract": _inference(),
            "research": _inference(capacity=2),
            "assist": _inference(running=1),
            "code": _inference(),
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=settings,
        redis_client=redis,
        generated_at=now,
    )

    assert [queue.id for queue in overview.queues] == [
        "extract",
        "research",
        "assist",
        "code",
    ]
    assist = overview.queues[2]
    assert assist.counts.queued == 7
    assert assist.counts.running == 1
    assert assist.tasks[0].title == "沪电股份发布半年报"
    assert assist.tasks[0].subtitle == "earnings"
    assert assist.metrics.longest_wait_ms == 5 * 60 * 1000
    code = overview.queues[3]
    assert code.state == "disabled"
    assert code.enabled is False
    assert code.counts.queued == 2


def test_estimated_clear_time_uses_capacity_and_average_execution(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    redis = FakeRedis({"mapping": 3})
    record_model_task(
        "assist",
        task_id="completed-1",
        kind="asset_mapping",
        title="已完成映射",
        queued_at=now - timedelta(minutes=4),
        redis_client=redis,
    )
    update_model_task(
        "assist",
        "completed-1",
        status="running",
        occurred_at=now - timedelta(minutes=3),
        redis_client=redis,
    )
    update_model_task(
        "assist",
        "completed-1",
        status="completed",
        occurred_at=now - timedelta(minutes=1),
        redis_client=redis,
    )
    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            "extract": _inference(),
            "research": _inference(capacity=2),
            "assist": _inference(capacity=1),
            "code": _inference(),
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=redis,
        generated_at=now,
    )

    assist = overview.queues[2]
    assert assist.metrics.average_execution_duration_ms == 2 * 60 * 1000
    assert assist.metrics.throughput_per_hour == 30
    assert assist.metrics.estimated_clear_ms == 3 * 2 * 60 * 1000


def test_assist_throughput_uses_first_and_last_completion_in_four_hour_window(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    redis = FakeRedis({"mapping": 3})
    for task_id, completed_at, instance_id in (
        ("completed-old", now - timedelta(hours=5), "assist-0"),
        ("completed-first", now - timedelta(hours=3), "assist-0"),
        ("completed-last", now - timedelta(hours=1), "assist-1"),
    ):
        record_model_task(
            "assist",
            task_id=task_id,
            kind="asset_mapping",
            title=task_id,
            instance_id=instance_id,
            queued_at=completed_at - timedelta(minutes=3),
            redis_client=redis,
        )
        update_model_task(
            "assist",
            task_id,
            status="running",
            occurred_at=completed_at - timedelta(minutes=2),
            redis_client=redis,
        )
        update_model_task(
            "assist",
            task_id,
            status="completed",
            occurred_at=completed_at,
            redis_client=redis,
        )

    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            "extract": _inference(),
            "research": _inference(),
            "assist": {
                **_inference(capacity=2),
                "instance_count": 2,
                "per_instance_concurrency": 1,
                "instances": [
                    {
                        "id": "assist-0",
                        "healthy": True,
                        "model_available": True,
                        "capacity": 1,
                        "available": 1,
                    },
                    {
                        "id": "assist-1",
                        "healthy": True,
                        "model_available": True,
                        "capacity": 1,
                        "available": 1,
                    },
                ],
            },
            "code": _inference(),
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(
            _env_file=None,
            ollama_assist_base_urls="http://assist-0.invalid,http://assist-1.invalid",
            ollama_assist_max_concurrency=2,
        ),
        redis_client=redis,
        generated_at=now,
    )

    assist = overview.queues[2]
    assert assist.metrics.throughput_per_hour == 0.5
    assert assist.metrics.estimated_clear_ms == 6 * 60 * 60 * 1000
    assert [item.metrics.throughput_per_hour for item in assist.instances] == [30, 30]


def test_assist_without_recent_completions_keeps_throughput_and_clear_unknown(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    redis = FakeRedis({"mapping": 3})
    record_model_task(
        "assist",
        task_id="failed-only",
        kind="asset_mapping",
        title="失败样本",
        queued_at=now - timedelta(minutes=4),
        redis_client=redis,
    )
    update_model_task(
        "assist",
        "failed-only",
        status="running",
        occurred_at=now - timedelta(minutes=3),
        redis_client=redis,
    )
    update_model_task(
        "assist",
        "failed-only",
        status="failed",
        occurred_at=now - timedelta(minutes=1),
        redis_client=redis,
    )

    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            lane: _inference() for lane in ("extract", "research", "assist", "code")
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=redis,
        generated_at=now,
    )

    assist = overview.queues[2]
    assert assist.metrics.average_execution_duration_ms == 2 * 60 * 1000
    assert assist.metrics.throughput_per_hour is None
    assert assist.metrics.estimated_clear_ms is None


def test_research_metrics_exclude_active_duration_and_expose_instances(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    save_run(
        db,
        ResearchRun(
            asset=SEED_ASSETS[0],
            status=RunStatus.RUNNING,
            created_at=now - timedelta(hours=5),
            started_at=now - timedelta(hours=4),
        ),
    )
    save_run(
        db,
        ResearchRun(
            asset=SEED_ASSETS[0],
            status=RunStatus.COMPLETED,
            created_at=now - timedelta(hours=6),
            started_at=now - timedelta(hours=5, minutes=30),
            completed_at=now - timedelta(hours=5),
        ),
    )
    save_run(
        db,
        ResearchRun(
            asset=SEED_ASSETS[1],
            status=RunStatus.COMPLETED,
            created_at=now - timedelta(minutes=5),
            started_at=now - timedelta(minutes=4),
            completed_at=now - timedelta(minutes=2),
        ),
    )
    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            "extract": _inference(),
            "research": {
                **_inference(capacity=2, running=1),
                "instances": [
                    {"id": "research-0", "healthy": True, "model_available": True},
                    {"id": "research-1", "healthy": False, "model_available": False},
                ],
            },
            "assist": _inference(),
            "code": _inference(),
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=FakeRedis(),
        generated_at=now,
    )

    research = overview.queues[1]
    assert research.metrics.average_execution_duration_ms == 2 * 60 * 1000
    assert research.metrics.execution_duration_sample_count == 1
    assert research.metrics.execution_p50_ms == 2 * 60 * 1000
    assert research.metrics.execution_p90_ms == 30 * 60 * 1000
    assert research.metrics.throughput_per_hour == 2 / 24
    assert research.instance_count == 2
    assert research.per_instance_concurrency == 1
    assert [instance.healthy for instance in research.instances] == [True, False]


def test_research_queue_shows_each_asset_only_once_with_active_task_first(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    asset = SEED_ASSETS[0]
    save_run(
        db,
        ResearchRun(
            asset=asset,
            status=RunStatus.FAILED,
            created_at=now - timedelta(minutes=20),
            completed_at=now - timedelta(minutes=10),
            error="historical failure",
        ),
    )
    save_run(
        db,
        ResearchRun(
            asset=asset,
            status=RunStatus.QUEUED,
            created_at=now - timedelta(minutes=5),
        ),
    )

    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            lane: _inference() for lane in ("extract", "research", "assist", "code")
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=FakeRedis(),
        generated_at=now,
    )

    research = overview.queues[1]
    asset_tasks = [task for task in research.tasks if task.entity_id == asset.asset_id]
    assert len(asset_tasks) == 1
    assert asset_tasks[0].status == "queued"
    assert research.counts.queued == 1
    assert research.counts.failed == 1


def test_research_queue_hides_old_failure_after_newer_success(db):
    now = datetime(2026, 8, 26, 8, 0, tzinfo=UTC)
    asset = SEED_ASSETS[0]
    failed = ResearchRun(
        asset=asset,
        status=RunStatus.FAILED,
        created_at=now - timedelta(minutes=20),
        completed_at=now - timedelta(minutes=10),
        error="historical failure",
    )
    save_run(db, failed)
    completed = ResearchRun(
        asset=asset,
        status=RunStatus.COMPLETED,
        created_at=now - timedelta(minutes=8),
        completed_at=now - timedelta(minutes=2),
    )
    completed.updated_at = failed.updated_at + timedelta(seconds=1)
    save_run(db, completed)

    overview = build_model_queue_overview(
        db,
        extraction_queue=_empty_extraction(now),
        inference_statuses={
            lane: _inference() for lane in ("extract", "research", "assist", "code")
        },
        threads={"extract": 4, "research": 16, "assist": 4, "code": 4},
        limit=500,
        settings=Settings(_env_file=None),
        redis_client=FakeRedis(),
        generated_at=now,
    )

    research = overview.queues[1]
    assert [task for task in research.tasks if task.entity_id == asset.asset_id] == []
    assert research.counts.completed == 1
    assert research.counts.failed == 1
