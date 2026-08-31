import base64
import json
from uuid import UUID, uuid4

import pytest

from backend.app.research_queue_cutover import (
    RESEARCH_TASKS,
    decode_legacy_message,
)


def celery_message(task: str, *, priority: int) -> bytes:
    task_id, event_id, run_id = str(uuid4()), str(uuid4()), str(uuid4())
    args = [event_id, run_id] if task.endswith("research_event") else ["US:AAPL", event_id, run_id]
    body = base64.b64encode(
        json.dumps([args, {"model_instance_id": "research-1"}, {}]).encode()
    ).decode()
    return json.dumps(
        {
            "body": body,
            "headers": {"task": task, "id": task_id},
            "properties": {"priority": priority},
        }
    ).encode()


@pytest.mark.parametrize(
    ("task", "priority"),
    [("market_loop.research_event", 1), ("market_loop.research_asset", 3)],
)
def test_decode_legacy_research_message_preserves_task_and_run(task, priority):
    value = decode_legacy_message(b"research.research-1", celery_message(task, priority=priority))

    assert value.task_type == task
    assert value.priority == priority
    assert str(UUID(value.task_id)) == value.task_id
    assert str(UUID(value.run_id)) == value.run_id
    assert value.kwargs == {"model_instance_id": "research-1"}


def test_decode_legacy_research_message_rejects_other_tasks():
    task = "market_loop.resolve_event_assets"
    assert task not in RESEARCH_TASKS
    with pytest.raises(ValueError, match="unsupported research task"):
        decode_legacy_message(b"research", celery_message(task, priority=3))
