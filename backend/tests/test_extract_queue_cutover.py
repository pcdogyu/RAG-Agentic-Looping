import base64
import json
from uuid import UUID, uuid4

import pytest

from backend.app.extract_queue_cutover import (
    EXTRACT_TASK,
    decode_legacy_message,
)


def celery_message(*, task: str = EXTRACT_TASK, priority: int = 5) -> bytes:
    task_id, news_id = str(uuid4()), str(uuid4())
    body = base64.b64encode(
        json.dumps(
            [[news_id], {"model_instance_id": "extract-0"}, {}]
        ).encode()
    ).decode()
    return json.dumps(
        {
            "body": body,
            "headers": {"task": task, "id": task_id},
            "properties": {"priority": priority},
        }
    ).encode()


def test_decode_legacy_extract_message_preserves_identity_and_priority():
    value = decode_legacy_message(b"extract.queue", celery_message(priority=1))

    assert value.queue_key == b"extract.queue"
    assert uuid_is_valid(value.task_id)
    assert uuid_is_valid(value.news_id)
    assert value.kwargs == {"model_instance_id": "extract-0"}
    assert value.priority == 1


def test_decode_legacy_extract_message_rejects_mixed_task_types():
    with pytest.raises(ValueError, match="unsupported extract task"):
        decode_legacy_message(
            b"extract.queue",
            celery_message(task="market_loop.reextract_event"),
        )


def uuid_is_valid(value: str) -> bool:
    return str(UUID(value)) == value
