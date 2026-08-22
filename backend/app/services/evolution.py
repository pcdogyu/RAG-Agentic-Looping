from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.domain import EvolutionCandidate, EvolutionStatus
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
            system=(
                "你是代码演进代理。输出最小 unified diff，不读取或生成密钥，不添加实盘交易。"
                "修改必须对应一个可测量失败模式。"
            ),
            prompt=f"失败案例：{json.dumps(failures, ensure_ascii=False)[:20000]}",
            schema=EvolutionProposal,
            temperature=0,
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
        self._run(["git", "switch", "-c", candidate.branch])
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
                    ["docker", "compose", "build", "api", "web", "llm-worker", "evolution-worker"],
                    timeout=1800,
                ),
            }
            candidate.test_report["checks"] = checks
            baseline, score, metrics_pass = self._evaluation_scores(candidate.target_metric)
            candidate.baseline_score = baseline
            candidate.candidate_score = score
            if not all(check["passed"] for check in checks.values()) or not metrics_pass:
                candidate.status = EvolutionStatus.REJECTED
                save_evolution(db, candidate)
                return candidate
            self._run(["git", "add", "-A"])
            self._run(["git", "commit", "-m", f"evolution: {candidate.hypothesis[:72]}"])
            if self.settings.evolution_auto_merge:
                self._run(["git", "switch", "main"])
                tag = f"last-known-good-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
                self._run(["git", "tag", tag])
                self._run(["git", "tag", "-f", "last-known-good"])
                self._run(
                    ["git", "merge", "--no-ff", candidate.branch, "-m", f"merge {candidate.branch}"]
                )
                deployment = self._check(
                    [
                        "docker",
                        "compose",
                        "up",
                        "-d",
                        "--build",
                        "api",
                        "io-worker",
                        "llm-worker",
                        "scheduler",
                        "web",
                    ],
                    timeout=1800,
                )
                if deployment["passed"]:
                    deployment = self._deployment_health()
                candidate.test_report["deployment"] = deployment
                if deployment["passed"]:
                    candidate.status = EvolutionStatus.MERGED
                else:
                    self._run(["git", "reset", "--hard", tag])
                    self._check(
                        [
                            "docker",
                            "compose",
                            "up",
                            "-d",
                            "--build",
                            "api",
                            "io-worker",
                            "llm-worker",
                            "scheduler",
                            "web",
                        ],
                        timeout=1800,
                    )
                    candidate.status = EvolutionStatus.ROLLED_BACK
            save_evolution(db, candidate)
            return candidate
        finally:
            current = self._run(["git", "branch", "--show-current"], check=False).stdout.strip()
            if current != "main" and candidate.status is EvolutionStatus.REJECTED:
                self._run(["git", "reset", "--hard", "HEAD"], check=False)
                self._run(["git", "switch", "main"], check=False)

    def rollback(self) -> None:
        if not self.settings.evolution_enabled:
            raise EvolutionError("EVOLUTION_ENABLED is false")
        if self._run(["git", "status", "--porcelain"]).stdout.strip():
            raise EvolutionError("rollback refused because the worktree is not clean")
        if self._run(["git", "rev-parse", "--verify", "last-known-good"], check=False).returncode:
            raise EvolutionError("last-known-good tag does not exist")
        self._run(["git", "reset", "--hard", "last-known-good"])
        deployment = self._check(
            [
                "docker",
                "compose",
                "up",
                "-d",
                "--build",
                "api",
                "io-worker",
                "llm-worker",
                "scheduler",
                "web",
            ],
            timeout=1800,
        )
        if not deployment["passed"] or not self._deployment_health()["passed"]:
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
            if isinstance(value, int | float) and isinstance(candidate_metrics.get(key), int | float)
        }
        no_regression = all(
            float(candidate_metrics[key]) >= float(baseline_metrics[key]) - 0.01
            for key in common
            if key not in {"version", "samples"}
        )
        normalized_target = target_metric.strip().lower().replace(" ", "_")
        metric = normalized_target if normalized_target in common else "composite_score"
        target_improved = float(candidate_metrics[metric]) - float(baseline_metrics[metric]) >= 0.02
        return baseline, candidate, no_regression and target_improved

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

    @staticmethod
    def _deployment_health() -> dict[str, Any]:
        last_error = "API health check did not become ready"
        for _ in range(12):
            for url in ("http://api:8000/health", "http://127.0.0.1:8000/health"):
                try:
                    response = httpx.get(url, timeout=5)
                    payload = response.json()
                    if response.is_success and payload.get("database"):
                        return {"passed": True, "returncode": 0, "output": "API healthy"}
                    last_error = f"unhealthy response from {url}"
                except (httpx.HTTPError, ValueError) as exc:
                    last_error = f"{url}: {type(exc).__name__}"
            time.sleep(5)
        return {"passed": False, "returncode": 1, "output": last_error}

    def _assert_no_secret(self, value: str) -> None:
        if any(pattern.search(value) for pattern in self.secret_patterns):
            raise EvolutionError("candidate patch appears to contain a secret")

    def _check(self, args: list[str]) -> dict[str, Any]:
        result = self._run(args, check=False, timeout=900)
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
