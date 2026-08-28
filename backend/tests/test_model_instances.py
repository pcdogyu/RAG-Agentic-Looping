import json

import pytest
from pydantic import ValidationError

from backend.app.config import Settings
from backend.app.services import model_instances
from backend.app.services.model_instances import (
    broker_queue_name,
    configured_model_instances,
    record_instance_assignment,
    select_model_instance,
    worker_queue_names,
)


class FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}
        self.counters: dict[str, int] = {}

    def hset(self, key, field, value):
        self.hashes.setdefault(str(key), {})[str(field)] = str(value)

    def hvals(self, key):
        return list(self.hashes.get(str(key), {}).values())

    def expire(self, _key, _seconds):
        return True

    def incr(self, key):
        value = self.counters.get(str(key), 0) + 1
        self.counters[str(key)] = value
        return value


def test_instance_configuration_splits_capacity_and_keeps_legacy_queue():
    settings = Settings(
        _env_file=None,
        ollama_research_base_urls="http://r0.invalid,http://r1.invalid",
        ollama_research_max_concurrency=3,
    )

    instances = configured_model_instances("research", settings)

    assert [(item.id, item.capacity) for item in instances] == [
        ("research-0", 2),
        ("research-1", 1),
    ]
    assert worker_queue_names("research", settings) == (
        "research,research.research-0,research.research-1"
    )
    assert broker_queue_name("assist", "assist-1") == "mapping.assist-1"


def test_three_research_endpoints_get_independent_capacity_and_queues():
    settings = Settings(
        _env_file=None,
        ollama_research_base_urls=(
            "http://r0.invalid,http://r1.invalid,http://r2.invalid"
        ),
        ollama_research_max_concurrency=3,
        research_pipeline_concurrency=3,
    )

    instances = configured_model_instances("research", settings)

    assert [(item.id, item.capacity) for item in instances] == [
        ("research-0", 1),
        ("research-1", 1),
        ("research-2", 1),
    ]
    assert worker_queue_names("research", settings) == (
        "research,research.research-0,research.research-1,research.research-2"
    )


def test_instance_configuration_rejects_more_endpoints_than_capacity():
    with pytest.raises(ValidationError, match="instance count"):
        Settings(
            _env_file=None,
            ollama_extract_base_urls="http://e0.invalid,http://e1.invalid",
            ollama_extract_max_concurrency=1,
        )


def test_extract_assist_and_code_plural_endpoints_use_stable_ids():
    settings = Settings(
        _env_file=None,
        ollama_extract_base_urls="http://e0.invalid,http://e1.invalid",
        ollama_extract_max_concurrency=2,
        ollama_assist_base_urls="http://a0.invalid,http://a1.invalid",
        ollama_assist_max_concurrency=2,
        ollama_code_base_urls="http://c0.invalid,http://c1.invalid",
        ollama_code_max_concurrency=2,
    )

    assert [
        item.id for item in configured_model_instances("extract", settings)
    ] == ["extract-0", "extract-1"]
    assert [
        item.id for item in configured_model_instances("assist", settings)
    ] == ["assist-0", "assist-1"]
    assert [item.id for item in configured_model_instances("code", settings)] == [
        "code-0",
        "code-1",
    ]


def test_dispatch_chooses_least_loaded_instance_then_round_robins_ties():
    settings = Settings(
        _env_file=None,
        ollama_research_base_urls="http://r0.invalid,http://r1.invalid",
        ollama_research_max_concurrency=2,
        research_pipeline_concurrency=2,
    )
    redis = FakeRedis()
    record_instance_assignment(
        "research", "busy", "research-0", redis_client=redis
    )

    selected = select_model_instance(
        "research",
        task_id="new",
        settings=settings,
        redis_client=redis,
    )
    assert selected.id == "research-1"

    assignments = redis.hashes["market-loop:model-instance:research:assignments"]
    assert json.loads(assignments["new"])["instance_id"] == "research-1"

    redis = FakeRedis()
    first = select_model_instance(
        "research", task_id="first", settings=settings, redis_client=redis
    )
    second = select_model_instance(
        "research", task_id="second", settings=settings, redis_client=redis
    )
    assert {first.id, second.id} == {"research-0", "research-1"}


def test_dispatch_migrates_an_offline_preferred_instance(monkeypatch):
    settings = Settings(
        _env_file=None,
        ollama_research_base_urls="http://r0.invalid,http://r1.invalid",
        ollama_research_max_concurrency=2,
        research_pipeline_concurrency=2,
    )
    redis = FakeRedis()
    monkeypatch.setattr(
        model_instances,
        "instance_health",
        lambda instance, _model: (instance.id == "research-1", True),
    )

    selected = select_model_instance(
        "research",
        task_id="migrate",
        preferred="research-0",
        settings=settings,
        redis_client=redis,
        probe_health=True,
    )

    assert selected.id == "research-1"


def test_dispatch_rebalances_an_overloaded_healthy_preferred_instance():
    settings = Settings(
        _env_file=None,
        ollama_research_base_urls="http://r0.invalid,http://r1.invalid,http://r2.invalid",
        ollama_research_max_concurrency=3,
    )
    redis = FakeRedis()
    for index in range(3):
        record_instance_assignment(
            "research", f"busy-{index}", "research-0", redis_client=redis
        )

    selected = select_model_instance(
        "research",
        task_id="move-me",
        preferred="research-0",
        settings=settings,
        redis_client=redis,
        rebalance_preferred=True,
    )

    assert selected.id in {"research-1", "research-2"}


def test_dispatch_keeps_a_balanced_preferred_instance_when_rebalancing():
    settings = Settings(
        _env_file=None,
        ollama_research_base_urls="http://r0.invalid,http://r1.invalid",
        ollama_research_max_concurrency=2,
        research_pipeline_concurrency=2,
    )
    redis = FakeRedis()
    record_instance_assignment(
        "research", "already-queued", "research-0", redis_client=redis
    )

    selected = select_model_instance(
        "research",
        task_id="keep-me",
        preferred="research-0",
        settings=settings,
        redis_client=redis,
        rebalance_preferred=True,
    )

    assert selected.id == "research-0"
