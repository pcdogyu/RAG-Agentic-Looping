import json
import subprocess
from types import SimpleNamespace

import pytest

from backend.app.config import Settings
from backend.app.domain import EvolutionCandidate
from backend.app.services.evolution import EvolutionError, EvolutionService


def test_evolution_is_disabled_by_default(db):
    service = EvolutionService(settings=Settings(evolution_enabled=False))
    with pytest.raises(EvolutionError, match="EVOLUTION_ENABLED"):
        service.propose(db, [{"failure": "example"}])


def test_secret_detection():
    service = EvolutionService(settings=Settings())
    with pytest.raises(EvolutionError, match="secret"):
        service._assert_no_secret("+ FMP_API_KEY='do-not-commit-this'")  # pragma: allowlist secret


def test_evolution_cannot_rewrite_its_frozen_exam():
    service = EvolutionService(settings=Settings(evolution_enabled=True))
    patch = """diff --git a/evals/golden_events.json b/evals/golden_events.json
--- a/evals/golden_events.json
+++ b/evals/golden_events.json
@@ -1 +1 @@
-old
+easy
"""

    with pytest.raises(EvolutionError, match="protected evaluation files"):
        service._assert_candidate_scope(patch)


def test_evolution_checks_both_rename_paths_for_protected_files():
    service = EvolutionService(settings=Settings(evolution_enabled=True))
    patch = """diff --git a/backend/harmless.py b/evals/baseline.json
similarity index 100%
rename from backend/harmless.py
rename to evals/baseline.json
"""

    with pytest.raises(EvolutionError, match="protected evaluation files"):
        service._assert_candidate_scope(patch)


def test_check_forwards_timeout_and_converts_timeout_to_failure(monkeypatch):
    service = EvolutionService(settings=Settings())
    seen: list[int] = []

    def completed(_args, *, check, timeout):
        assert check is False
        seen.append(timeout)
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(service, "_run", completed)
    assert service._check(["test"], timeout=17)["passed"] is True
    assert seen == [17]

    def timed_out(_args, *, check, timeout):
        raise subprocess.TimeoutExpired("test", timeout)

    monkeypatch.setattr(service, "_run", timed_out)
    result = service._check(["test"], timeout=23)
    assert result["passed"] is False
    assert result["returncode"] == 124


def test_deploy_verification_includes_mapping_but_not_running_evolution_worker(monkeypatch):
    service = EvolutionService(settings=Settings())
    commands: list[list[str]] = []

    def successful_check(args, *, timeout):
        commands.append(args)
        assert timeout == 1800
        return {"passed": True, "returncode": 0, "output": "ok"}

    monkeypatch.setattr(service, "_check", successful_check)
    monkeypatch.setattr(
        service,
        "_deployment_health",
        lambda: {"passed": True, "returncode": 0, "output": "healthy"},
    )
    monkeypatch.setattr(
        service,
        "_reset_task_health_window",
        lambda: {"passed": True, "returncode": 0, "output": "reset"},
    )

    result = service._deploy_and_verify()

    assert result["passed"] is True
    assert result["health_window_reset"]["passed"] is True
    assert "mapping-worker" in commands[0]
    assert "evolution-worker" not in commands[0]


def test_successful_deploy_resets_task_health_window(monkeypatch):
    service = EvolutionService(settings=Settings(redis_url="redis://test.invalid/0"))
    deleted: list[tuple[str, ...]] = []

    class FakeRedis:
        def delete(self, *keys):
            deleted.append(keys)
            return len(keys)

    def fake_from_url(url, *, socket_connect_timeout):
        assert url == "redis://test.invalid/0"
        assert socket_connect_timeout == 1
        return FakeRedis()

    monkeypatch.setattr(
        "backend.app.services.evolution.Redis.from_url",
        fake_from_url,
    )

    result = service._reset_task_health_window()

    assert result["passed"] is True
    assert deleted == [
        (
            "market-loop:tasks:success",
            "market-loop:tasks:failure",
        )
    ]


def test_deployment_health_rejects_degraded_api(monkeypatch):
    service = EvolutionService(settings=Settings())

    class Response:
        is_success = True
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def response_for(url, *, timeout):
        assert timeout == 5
        return Response({"status": "degraded", "database": True}) if "health" in url else Response({})

    monkeypatch.setattr("backend.app.services.evolution.httpx.get", response_for)
    monkeypatch.setattr("backend.app.services.evolution.time.sleep", lambda _seconds: None)

    assert service._deployment_health()["passed"] is False


def test_deployment_health_requires_api_dependencies_and_web(monkeypatch):
    service = EvolutionService(settings=Settings())

    class Response:
        is_success = True
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    healthy = {
        "status": "ok",
        "database": True,
        "redis": True,
        "data_fresh": True,
        "ollama": True,
        "ollama_instances": [{"healthy": True, "model_available": True}],
    }
    monkeypatch.setattr(
        "backend.app.services.evolution.httpx.get",
        lambda url, timeout: Response(healthy if "health" in url else {}),
    )

    assert service._deployment_health()["passed"] is True


def test_auto_merge_disabled_returns_to_clean_main_and_preserves_candidate_branch(
    db, tmp_path, monkeypatch
):
    subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "evolution@example.invalid"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "Evolution Test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "core.autocrlf", "false"], cwd=tmp_path, check=True)
    sample = tmp_path / "sample.py"
    sample.write_text("old\n", encoding="utf-8")
    subprocess.run(["git", "add", "sample.py"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=tmp_path, check=True)

    sample.write_text("new\n", encoding="utf-8")
    patch = subprocess.run(
        ["git", "diff", "--binary"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    subprocess.run(["git", "restore", "sample.py"], cwd=tmp_path, check=True)
    candidate = EvolutionCandidate(
        hypothesis="keep reviewed branch",
        target_metric="composite_score",
        expected_improvement=0.1,
        branch="evolve/test-candidate",
        test_report={"patch": patch},
    )
    service = EvolutionService(
        root=tmp_path,
        settings=Settings(evolution_enabled=True, evolution_auto_merge=False),
    )
    passed = {"passed": True, "returncode": 0, "output": "ok"}
    monkeypatch.setattr(service, "_check", lambda *args, **kwargs: passed)
    monkeypatch.setattr(service, "_repository_secret_check", lambda: passed)
    monkeypatch.setattr(service, "_evaluation_scores", lambda _metric: (0.5, 0.7, True))

    service.execute(db, candidate)

    branch = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    candidate_content = subprocess.run(
        ["git", "show", "evolve/test-candidate:sample.py"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert branch == "main"
    assert sample.read_text(encoding="utf-8") == "old\n"
    assert candidate_content == "new\n"
    assert subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout == ""


def test_execute_exception_returns_to_clean_main(db, tmp_path, monkeypatch):
    subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, check=True, capture_output=True)
    for key, value in (
        ("user.email", "evolution@example.invalid"),
        ("user.name", "Evolution Test"),
        ("core.autocrlf", "false"),
    ):
        subprocess.run(["git", "config", key, value], cwd=tmp_path, check=True)
    sample = tmp_path / "sample.py"
    sample.write_text("old\n", encoding="utf-8")
    subprocess.run(["git", "add", "sample.py"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=tmp_path, check=True)
    sample.write_text("new\n", encoding="utf-8")
    patch = subprocess.run(
        ["git", "diff", "--binary"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    subprocess.run(["git", "restore", "sample.py"], cwd=tmp_path, check=True)
    candidate = EvolutionCandidate(
        hypothesis="failing candidate",
        target_metric="composite_score",
        expected_improvement=0.1,
        branch="evolve/failing-candidate",
        test_report={"patch": patch},
    )
    service = EvolutionService(root=tmp_path, settings=Settings(evolution_enabled=True))
    monkeypatch.setattr(
        service,
        "_check",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("check failed")),
    )

    with pytest.raises(RuntimeError, match="check failed"):
        service.execute(db, candidate)

    assert subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip() == "main"
    assert subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout == ""
    assert sample.read_text(encoding="utf-8") == "old\n"


def test_candidate_check_side_effects_are_rejected(tmp_path):
    subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "evolution@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Evolution Test"], cwd=tmp_path, check=True)
    tracked = tmp_path / "candidate.py"
    tracked.write_text("old\n", encoding="utf-8")
    subprocess.run(["git", "add", "candidate.py"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=tmp_path, check=True)
    tracked.write_text("candidate\n", encoding="utf-8")
    subprocess.run(["git", "add", "candidate.py"], cwd=tmp_path, check=True)
    tracked.write_text("test side effect\n", encoding="utf-8")
    service = EvolutionService(root=tmp_path, settings=Settings(evolution_enabled=True))

    with pytest.raises(EvolutionError, match="outside the staged patch"):
        service._assert_no_test_side_effects({"candidate.py"})


def test_successful_candidate_metrics_advance_the_trusted_baseline(
    tmp_path, monkeypatch
):
    evals = tmp_path / "evals"
    evals.mkdir()
    (evals / "baseline.json").write_text(
        '{"composite_score": 0.5, "passed": true}\n', encoding="utf-8"
    )
    (evals / "candidate.json").write_text(
        '{"composite_score": 0.7, "passed": true}\n', encoding="utf-8"
    )
    commands: list[list[str]] = []
    service = EvolutionService(root=tmp_path, settings=Settings())

    def successful(args, **_kwargs):
        commands.append(args)
        return SimpleNamespace(returncode=0, stdout="committed", stderr="")

    monkeypatch.setattr(service, "_run", successful)

    result = service._promote_candidate_baseline()

    assert result == {"passed": True, "composite_score": 0.7, "committed": True}
    assert json.loads((evals / "baseline.json").read_text(encoding="utf-8"))[
        "composite_score"
    ] == 0.7
    assert ["git", "add", "--", "evals/baseline.json"] in commands
