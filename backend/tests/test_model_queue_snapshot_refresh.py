from __future__ import annotations

from backend.app import main


class _StopAfterFirstWait:
    def __init__(self) -> None:
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return False

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        return True


def test_model_queue_snapshot_publisher_refreshes_without_api_traffic(monkeypatch):
    refreshes: list[bool] = []
    stop = _StopAfterFirstWait()
    monkeypatch.setattr(main, "_model_queue_refreshing", False)
    monkeypatch.setattr(
        main,
        "_refresh_model_queue_snapshot_in_background",
        lambda: refreshes.append(True),
    )

    main._refresh_model_queue_snapshot_periodically(stop)  # type: ignore[arg-type]

    assert refreshes == [True]
    assert stop.waits == [main.MODEL_QUEUE_SNAPSHOT_TTL_SECONDS]
