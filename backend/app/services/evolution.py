from __future__ import annotations

import json
import re
import shlex
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, Field
from redis import Redis
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import EvolutionCandidate, EvolutionStatus
from backend.app.evaluation import compare_model_metrics
from backend.app.llm import LlmGateway, gateway
from backend.app.storage import save_evolution


class EvolutionProposal(BaseModel):
    hypothesis: str
    target_metric: str
    expected_improvement: float = Field(ge=0, le=1)
    unified_diff: str
    tests_to_run: list[str] = Field(default_factory=list)


class EvolutionError(RuntimeError):
    pass


class EvolutionService:
    """Experimental self-modification. Disabled by default and not a security boundary."""

    secret_patterns = [
        re.compile(r"(?i)(api[_-]?key|token|secret)\s*[:=]\s*['\"][^'\"]+"),
        re.compile(r"\b[0-9a-f]{32}\b", re.IGNORECASE),
    ]
    protected_paths = {
        ".env.example",
        ".github/workflows/ci.yml",
        ".gitignore",
        "backend/app/evaluation.py",
        "backend/app/services/evolution.py",
        "backend/Dockerfile",
        "backend/Dockerfile.evolution",
        "backend/tests/conftest.py",
        "docker-compose.yml",
        "evals/baseline.json",
        "evals/golden_events.json",
        "evals/golden_predictions.json",
        "pyproject.toml",
    }
    deployed_services = (
        "api",
        "io-worker",
        "extract-worker",
        "mapping-worker",
        "research-worker",
        "scheduler",
        "web",
    )

    def __init__(
        self,
        root: Path = Path("."),
        settings: Settings | None = None,
        llm: LlmGateway | None = None,
    ) -> None:
        self.root = root.resolve()
        self.settings = settings or get_settings()
        self.llm = llm or gateway

    def propose(self, db: Session, failures: list[dict[str, Any]]) -> EvolutionCandidate:
        if not self.settings.evolution_enabled:
            raise EvolutionError("EVOLUTION_ENABLED is false")
        payload = self.llm.generate_json(
            model=self.settings.ollama_code_model,
            lane="code",
            system=(
                "你是代码演进代理。输出最小 unified diff，不读取或生成密钥，不添加实盘交易。"
                "修改必须对应一个可测量失败模式。"
            ),
            prompt=f"失败案例：{json.dumps(failures, ensure_ascii=False)[:20000]}",
            schema=EvolutionProposal,
            temperature=0,
            operation="evolution_proposal",
        )
        proposal = EvolutionProposal.model_validate(payload)
        slug = re.sub(r"[^a-z0-9]+", "-", proposal.target_metric.lower()).strip("-")[:40]
        branch = f"evolve/{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}-{slug or 'candidate'}"
        candidate = EvolutionCandidate(
            hypothesis=proposal.hypothesis,
            target_metric=proposal.target_metric,
            expected_improvement=proposal.expected_improvement,
            branch=branch,
            test_report={"patch": proposal.unified_diff, "requested_tests": proposal.tests_to_run},
        )
        save_evolution(db, candidate)
        return candidate

    def execute(self, db: Session, candidate: EvolutionCandidate) -> EvolutionCandidate:
        if not self.settings.evolution_enabled:
            raise EvolutionError("EVOLUTION_ENABLED is false")
        if self._run(["git", "status", "--porcelain"]).stdout.strip():
            raise EvolutionError("evolution requires a clean worktree")
        patch = candidate.test_report.get("patch", "")
        self._assert_no_secret(patch)
        candidate_paths = self._assert_candidate_scope(patch)
        self._run(["git", "switch", "-c", candidate.branch])
        rollback_tag: str | None = None
        try:
            applied = subprocess.run(
                ["git", "apply", "--index", "--whitespace=error", "-"],
                cwd=self.root,
                input=patch,
                text=True,
                capture_output=True,
                timeout=60,
                check=False,
            )
            if applied.returncode:
                raise EvolutionError(applied.stderr.strip() or "git apply failed")
            candidate.status = EvolutionStatus.TESTING
            save_evolution(db, candidate)
            checks = {
                "compile": self._check(["python", "-m", "compileall", "-q", "backend"]),
                "tests": self._check(["python", "-m", "pytest", "-q"]),
                "time_travel": self._check(
                    ["python", "-m", "pytest", "-q", "backend/tests/test_storage_time.py", "backend/tests/test_retrieval.py"]
                ),
                "lint": self._check(["python", "-m", "ruff", "check", "."]),
                "fixed_evidence": self._check(
                    ["python", "-m", "backend.app.evaluation", "fixed-evidence"]
                ),
                "walk_forward": self._check(
                    ["python", "-m", "backend.app.evaluation", "walk-forward"]
                ),
                "secret_scan": self._repository_secret_check(),
                "dependency_audit": self._check(["python", "-m", "pip_audit"]),
                "container_build": self._check(
                    [
                        "docker",
                        "compose",
                        "build",
                        "api",
                        "web",
                        "extract-worker",
                        "mapping-worker",
                        "research-worker",
                        "evolution-worker",
                    ],
                    timeout=1800,
                ),
            }
            self._assert_no_test_side_effects(candidate_paths)
            candidate.test_report["checks"] = checks
            baseline, score, metrics_pass = self._evaluation_scores(candidate.target_metric)
            candidate.baseline_score = baseline
            candidate.candidate_score = score
            if not all(check["passed"] for check in checks.values()) or not metrics_pass:
                candidate.status = EvolutionStatus.REJECTED
                save_evolution(db, candidate)
                return candidate
            self._run(["git", "commit", "-m", f"evolution: {candidate.hypothesis[:72]}"])
            if self.settings.evolution_auto_merge:
                self._run(["git", "switch", "main"])
                tag = f"last-known-good-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
                rollback_tag = tag
                self._run(["git", "tag", tag])
                self._run(["git", "tag", "-f", "last-known-good"])
                self._run(
                    ["git", "merge", "--no-ff", candidate.branch, "-m", f"merge {candidate.branch}"]
                )
                deployment = self._deploy_and_verify()
                candidate.test_report["deployment"] = deployment
                if deployment["passed"]:
                    candidate.test_report["baseline_promoted"] = (
                        self._promote_candidate_baseline()
                    )
                    candidate.status = EvolutionStatus.MERGED
                else:
                    self._run(["git", "reset", "--hard", tag])
                    rollback = self._deploy_and_verify()
                    candidate.test_report["rollback_deployment"] = rollback
                    candidate.status = EvolutionStatus.ROLLED_BACK
                    save_evolution(db, candidate)
                    if not rollback["passed"]:
                        raise EvolutionError(
                            "candidate deployment failed and rollback did not become healthy"
                        )
            save_evolution(db, candidate)
            return candidate
        except Exception as exc:
            current = self._run(
                ["git", "branch", "--show-current"], check=False
            ).stdout.strip()
            if current == "main" and rollback_tag:
                self._run(["git", "reset", "--hard", rollback_tag], check=False)
                try:
                    recovery = self._deploy_and_verify()
                except Exception as recovery_error:
                    recovery = {
                        "passed": False,
                        "returncode": 1,
                        "output": (
                            "exception rollback failed: "
                            f"{type(recovery_error).__name__}: {recovery_error}"
                        ),
                    }
                candidate.test_report["exception_rollback"] = recovery
                candidate.status = EvolutionStatus.ROLLED_BACK
            else:
                candidate.status = EvolutionStatus.REJECTED
            candidate.test_report["execution_error"] = f"{type(exc).__name__}: {exc}"
            try:
                save_evolution(db, candidate)
            except Exception:
                pass
            raise
        finally:
            current = self._run(["git", "branch", "--show-current"], check=False).stdout.strip()
            if current and current != "main":
                self._run(["git", "reset", "--hard", "HEAD"], check=False)
            if self._run(["git", "status", "--porcelain"], check=False).stdout.strip():
                self._run(
                    [
                        "git",
                        "stash",
                        "push",
                        "--include-untracked",
                        "-m",
                        f"evolution-cleanup-{candidate.id}",
                    ],
                    check=False,
                )
            if current and current != "main":
                self._run(["git", "switch", "main"], check=False)

    def rollback(self) -> None:
        if not self.settings.evolution_enabled:
            raise EvolutionError("EVOLUTION_ENABLED is false")
        if self._run(["git", "status", "--porcelain"]).stdout.strip():
            raise EvolutionError("rollback refused because the worktree is not clean")
        if self._run(["git", "rev-parse", "--verify", "last-known-good"], check=False).returncode:
            raise EvolutionError("last-known-good tag does not exist")
        self._run(["git", "reset", "--hard", "last-known-good"])
        deployment = self._deploy_and_verify()
        if not deployment["passed"]:
            raise EvolutionError("rollback deployment failed")

    def _evaluation_scores(self, target_metric: str) -> tuple[float, float, bool]:
        baseline_path = self.root / "evals" / "baseline.json"
        candidate_path = self.root / "evals" / "candidate.json"
        if not baseline_path.exists() or not candidate_path.exists():
            return 1.0, 0.0, False
        baseline_metrics = json.loads(baseline_path.read_text(encoding="utf-8"))
        candidate_metrics = json.loads(candidate_path.read_text(encoding="utf-8"))
        baseline = float(baseline_metrics["composite_score"])
        candidate = float(candidate_metrics["composite_score"])
        common = {
            key
            for key, value in baseline_metrics.items()
            if not isinstance(value, bool)
            and isinstance(value, int | float)
            and not isinstance(candidate_metrics.get(key), bool)
            and isinstance(candidate_metrics.get(key), int | float)
        }
        comparison = compare_model_metrics(baseline_metrics, candidate_metrics)
        no_regression = bool(comparison["passed"])
        normalized_target = target_metric.strip().lower().replace(" ", "_")
        metric = normalized_target if normalized_target in common else "composite_score"
        delta = float(candidate_metrics[metric]) - float(baseline_metrics[metric])
        target_improved = (
            delta <= -0.02
            if metric in {"brier_score", "expected_calibration_error"}
            else delta >= 0.02
        )
        return baseline, candidate, no_regression and target_improved

    def _promote_candidate_baseline(self) -> dict[str, Any]:
        """Advance the trusted champion only after merge and deployment health pass."""

        candidate_path = self.root / "evals" / "candidate.json"
        baseline_path = self.root / "evals" / "baseline.json"
        metrics = json.loads(candidate_path.read_text(encoding="utf-8"))
        if not isinstance(metrics, dict) or metrics.get("passed") is not True:
            raise EvolutionError("refusing to promote an invalid candidate baseline")
        baseline_path.write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        self._run(["git", "add", "--", "evals/baseline.json"])
        committed = self._run(
            ["git", "commit", "-m", "evolution: promote evaluated champion"],
            check=False,
        )
        commit_output = f"{committed.stdout}\n{committed.stderr}".casefold()
        if committed.returncode and "nothing to commit" not in commit_output:
            raise EvolutionError(committed.stderr.strip() or "baseline commit failed")
        return {
            "passed": True,
            "composite_score": metrics.get("composite_score"),
            "committed": committed.returncode == 0,
        }

    def _repository_secret_check(self) -> dict[str, Any]:
        tracked = self._run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard"], check=False
        )
        findings: list[str] = []
        for relative in tracked.stdout.splitlines():
            path = (self.root / relative).resolve()
            if not path.is_file() or self.root not in path.parents or path.stat().st_size > 1_000_000:
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            for line_number, line in enumerate(content.splitlines(), start=1):
                if "pragma: allowlist secret" in line:
                    continue
                if any(pattern.search(line) for pattern in self.secret_patterns):
                    findings.append(f"{relative}:{line_number}")
        return {
            "passed": not findings,
            "returncode": 0 if not findings else 1,
            "output": "no secrets detected" if not findings else f"possible secrets: {findings}",
        }

    def _deploy_and_verify(self) -> dict[str, Any]:
        deployment = self._check(
            ["docker", "compose", "up", "-d", "--build", *self.deployed_services],
            timeout=1800,
        )
        if not deployment["passed"]:
            return deployment
        health = self._deployment_health()
        if not health["passed"]:
            return health
        reset = self._reset_task_health_window()
        if not reset["passed"]:
            return reset
        return {**health, "health_window_reset": reset}

    def _reset_task_health_window(self) -> dict[str, Any]:
        try:
            client = Redis.from_url(self.settings.redis_url, socket_connect_timeout=1)
            deleted = client.delete(
                "market-loop:tasks:success",
                "market-loop:tasks:failure",
            )
            return {
                "passed": True,
                "returncode": 0,
                "output": f"reset task health counters ({deleted} keys removed)",
            }
        except Exception as exc:
            return {
                "passed": False,
                "returncode": 1,
                "output": f"task health counter reset failed: {type(exc).__name__}",
            }

    @staticmethod
    def _deployment_health() -> dict[str, Any]:
        last_error = "API and web health checks did not become ready"
        for _ in range(12):
            api_ready = False
            for url in ("http://api:8000/health", "http://127.0.0.1:8000/health"):
                try:
                    response = httpx.get(url, timeout=5)
                    payload = response.json()
                    if not isinstance(payload, dict):
                        last_error = f"invalid health payload from {url}"
                        continue
                    required_models_ready = all(
                        item.get("healthy") and item.get("model_available")
                        for item in payload.get("ollama_instances", [])
                    )
                    if (
                        response.is_success
                        and payload.get("status") == "ok"
                        and payload.get("database") is True
                        and payload.get("redis") is True
                        and payload.get("data_fresh") is True
                        and payload.get("ollama") is True
                        and required_models_ready
                    ):
                        api_ready = True
                        break
                    last_error = f"unhealthy response from {url}"
                except (httpx.HTTPError, TypeError, ValueError) as exc:
                    last_error = f"{url}: {type(exc).__name__}"
            web_ready = False
            for url in ("http://web/", "http://127.0.0.1/"):
                try:
                    response = httpx.get(url, timeout=5)
                    if response.is_success:
                        web_ready = True
                        break
                    last_error = f"web returned HTTP {response.status_code} from {url}"
                except httpx.HTTPError as exc:
                    last_error = f"{url}: {type(exc).__name__}"
            if api_ready and web_ready:
                return {
                    "passed": True,
                    "returncode": 0,
                    "output": "API and web healthy",
                }
            time.sleep(5)
        return {"passed": False, "returncode": 1, "output": last_error}

    def _assert_no_secret(self, value: str) -> None:
        if any(pattern.search(value) for pattern in self.secret_patterns):
            raise EvolutionError("candidate patch appears to contain a secret")

    def _assert_candidate_scope(self, patch: str) -> set[str]:
        paths: set[str] = set()
        for line in patch.splitlines():
            if not line.startswith("diff --git "):
                continue
            try:
                parts = shlex.split(line)
            except ValueError as exc:
                raise EvolutionError("candidate patch contains an invalid path header") from exc
            if len(parts) != 4 or not parts[2].startswith("a/") or not parts[3].startswith("b/"):
                raise EvolutionError("candidate patch contains an unsupported path header")
            paths.update((parts[2][2:], parts[3][2:]))
        if patch.strip() and not paths:
            raise EvolutionError("candidate patch does not declare any repository paths")
        protected = sorted(path for path in paths if path in self.protected_paths)
        if protected:
            raise EvolutionError(
                f"candidate patch modifies protected evaluation files: {protected}"
            )
        if re.search(r"^deleted file mode ", patch, re.MULTILINE):
            raise EvolutionError("candidate patch may not delete repository files")
        return paths

    def _assert_no_test_side_effects(self, candidate_paths: set[str]) -> None:
        unstaged = set(
            filter(
                None,
                self._run(["git", "diff", "--name-only"], check=False).stdout.splitlines(),
            )
        )
        untracked = set(
            filter(
                None,
                self._run(
                    ["git", "ls-files", "--others", "--exclude-standard"], check=False
                ).stdout.splitlines(),
            )
        )
        staged = set(
            filter(
                None,
                self._run(
                    ["git", "diff", "--cached", "--name-only"], check=False
                ).stdout.splitlines(),
            )
        )
        unexpected = sorted((unstaged | untracked | staged) - candidate_paths)
        protected = sorted((unstaged | untracked | staged) & self.protected_paths)
        if unstaged or unexpected or protected:
            raise EvolutionError(
                "candidate checks changed files outside the staged patch: "
                f"unstaged={sorted(unstaged)}, unexpected={unexpected}, protected={protected}"
            )

    def _check(self, args: list[str], *, timeout: int = 900) -> dict[str, Any]:
        try:
            result = self._run(args, check=False, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            return {
                "passed": False,
                "returncode": 124,
                "output": f"command timed out after {exc.timeout} seconds",
            }
        return {
            "passed": result.returncode == 0,
            "returncode": result.returncode,
            "output": (result.stdout + result.stderr)[-4000:],
        }

    def _run(
        self, args: list[str], *, check: bool = True, timeout: int = 120
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            args,
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=check,
        )
