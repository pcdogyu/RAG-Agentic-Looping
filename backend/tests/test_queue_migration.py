import json

import pytest

from backend.app.queue_migration import plan_messages


def message(task: str, task_id: str) -> bytes:
    return json.dumps({"headers": {"task": task, "id": task_id}}).encode()


def test_legacy_llm_messages_are_split_without_changing_ids_or_order():
    newest = message("market_loop.research_event", "event-1")
    middle = message("market_loop.resolve_event_assets", "mapping-1")
    oldest = message("market_loop.research_asset", "asset-1")

    plan = plan_messages([newest, middle, oldest])

    assert plan.counts == {"source": 3, "mapping": 1, "research": 2}
    assert plan.task_ids == ["event-1", "mapping-1", "asset-1"]
    assert plan.destinations["mapping"] == [middle]
    assert plan.destinations["research"] == [newest, oldest]


def test_legacy_llm_migration_rejects_unknown_and_duplicate_tasks():
    with pytest.raises(ValueError, match="unsupported legacy task type"):
        plan_messages([message("market_loop.unknown", "unknown-1")])

    duplicated = message("market_loop.research_asset", "same-id")
    with pytest.raises(ValueError, match="duplicate task id"):
        plan_messages([duplicated, duplicated])
    with pytest.raises(ValueError, match="already exist"):
        plan_messages(
            [duplicated],
            {"mapping": [], "research": [duplicated]},
        )
