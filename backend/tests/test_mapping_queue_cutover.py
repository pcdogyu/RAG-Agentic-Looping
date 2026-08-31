import base64
import json
from uuid import UUID, uuid4

import pytest

from backend.app.mapping_queue_cutover import (
    MAPPING_TASK,
    decode_legacy_message,
)


def celery_message(*, task: str = MAPPING_TASK, priority: int = 5) -> bytes:
    task_id, event_id = str(uuid4()), str(uuid4())
    body = base64.b64encode(
        json.dumps(
            [[event_id], {"model_instance_id": "assist-0", "force_mapping": True}, {}]
        ).encode()
    ).decode()
    return json.dumps(
        {
            "body": body,
            "headers": {"task": task, "id": task_id},
            "properties": {"priority": priority},
        }
    ).encode()


def test_decode_legacy_mapping_message_preserves_identity_and_priority():
    value = decode_legacy_message(b"mapping.assist-0", celery_message(priority=1))

    assert value.queue_key == b"mapping.assist-0"
    assert str(UUID(value.task_id)) == value.task_id
    assert str(UUID(value.event_id)) == value.event_id
    assert value.kwargs == {"model_instance_id": "assist-0", "force_mapping": True}
    assert value.priority == 1


def test_decode_legacy_mapping_message_rejects_mixed_task_types():
    with pytest.raises(ValueError, match="unsupported mapping task"):
        decode_legacy_message(
            b"mapping.assist-0",
            celery_message(task="market_loop.research_event"),
        )
