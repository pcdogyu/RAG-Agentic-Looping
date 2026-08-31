import base64
import json
from uuid import UUID, uuid4

import pytest

from backend.app.evolution_queue_cutover import (
    EVOLUTION_TASKS,
    decode_legacy_message,
)


def celery_message(task: str, *, priority: int) -> bytes:
    task_id = str(uuid4())
    args: list[object] = []
    if task == "market_loop.evolve_failures":
        args = [[{"failure_type": "technical_failure"}]]
    elif task == "market_loop.execute_evolution":
        args = [str(uuid4())]
    body = base64.b64encode(
        json.dumps([args, {"model_instance_id": "code-0"}, {}]).encode()
    ).decode()
    return json.dumps(
        {
            "body": body,
            "headers": {"task": task, "id": task_id},
            "properties": {"priority": priority},
        }
    ).encode()


@pytest.mark.parametrize("task", sorted(EVOLUTION_TASKS))
def test_decode_legacy_evolution_message_preserves_task(task):
    value = decode_legacy_message(
        b"evolution.code-0", celery_message(task, priority=5)
    )

    assert value.task_type == task
    assert value.priority == 5
    assert str(UUID(value.task_id)) == value.task_id
    assert value.kwargs == {"model_instance_id": "code-0"}
    if task == "market_loop.execute_evolution":
        assert value.candidate_id is not None


def test_decode_legacy_evolution_message_rejects_other_tasks():
    with pytest.raises(ValueError, match="unsupported evolution task"):
        decode_legacy_message(
            b"evolution", celery_message("market_loop.research_asset", priority=3)
        )


def test_decode_legacy_evolution_message_preserves_priority_zero():
    value = decode_legacy_message(
        b"evolution.code-0",
        celery_message("market_loop.evolve_from_outcomes", priority=0),
    )

    assert value.priority == 0
