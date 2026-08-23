from __future__ import annotations

import argparse
import base64
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import and_, delete, or_, select
from sqlalchemy.orm import Session

from backend.app.config import Settings, get_settings
from backend.app.db import (
    EventResearchRunRow,
    EventRow,
    EvolutionRow,
    ModelCallAuditRow,
    NewsRow,
    ResearchRunRow,
    SessionLocal,
    init_db,
)
from backend.app.domain import as_utc, utc_now

BEARER_PATTERN = re.compile(r"(?i)(bearer\s+)[a-z0-9._~+/=-]+")
CREDENTIAL_PATTERN = re.compile(
    r"(?i)((?:api[_-]?key|access[_-]?token|secret|password)\s*[:=]\s*)"
    r"([^\s,;\"']+)"
)
CHINESE_PATTERN = re.compile(r"[\u3400-\u9fff]")
LATIN_PATTERN = re.compile(r"[A-Za-z]")

GENERATIVE_PHASES = {
    "event_extraction",
    "asset_mapping",
    "report_drafting",
    "report_revision",
    "event_report_drafting",
    "event_report_revision",
}


def detect_language(text: str) -> str:
    chinese = len(CHINESE_PATTERN.findall(text or ""))
    latin = len(LATIN_PATTERN.findall(text or ""))
    if chinese and latin:
        smaller, larger = sorted((chinese, latin))
        return "mixed" if smaller / larger >= 0.1 else ("zh" if chinese > latin else "en")
    if chinese:
        return "zh"
    if latin:
        return "en"
    return "other"


def _configured_secrets(settings: Settings) -> list[str]:
    values: list[str] = []
    for name, value in settings.model_dump().items():
        if not isinstance(value, str) or len(value) < 6:
            continue
        lowered = name.lower()
        if any(marker in lowered for marker in ("key", "token", "secret", "password")):
            values.append(value)
    return sorted(values, key=len, reverse=True)


def redact_text(value: str, settings: Settings | None = None) -> str:
    output = value or ""
    active_settings = settings or get_settings()
    for secret in _configured_secrets(active_settings):
        output = output.replace(secret, "[REDACTED]")
    output = BEARER_PATTERN.sub(r"\1[REDACTED]", output)
    return CREDENTIAL_PATTERN.sub(r"\1[REDACTED]", output)


def redact_value(value: Any, settings: Settings | None = None) -> Any:
    if isinstance(value, str):
        return redact_text(value, settings)
    if isinstance(value, list):
        return [redact_value(item, settings) for item in value]
    if isinstance(value, dict):
        return {key: redact_value(item, settings) for key, item in value.items()}
    return value


def persist_model_audit(
    *,
    logical_call_id: UUID,
    provider: str,
    model: str,
    operation: str,
    attempt: int,
    status: str,
    started_at: datetime,
    completed_at: datetime,
    messages: list[dict[str, Any]],
    schema_payload: dict[str, Any],
    raw_response: str = "",
    parsed_response: dict[str, Any] | list[Any] | None = None,
    error: str | None = None,
    prompt_tokens: int | None = None,
    completion_tokens: int | None = None,
    metrics: dict[str, Any] | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    fidelity: str = "exact",
    source_key: str | None = None,
    settings: Settings | None = None,
) -> None:
    active_settings = settings or get_settings()
    if not active_settings.model_audit_enabled:
        return
    safe_messages = redact_value(messages, active_settings)
    safe_schema = redact_value(schema_payload, active_settings)
    safe_raw = redact_text(raw_response, active_settings)
    safe_parsed = redact_value(parsed_response, active_settings)
    safe_error = redact_text(error or "", active_settings) or None
    safe_metrics = redact_value(metrics or {}, active_settings)
    input_text = "\n".join(str(item.get("content", "")) for item in safe_messages)
    try:
        with SessionLocal() as db:
            db.add(
                ModelCallAuditRow(
                    logical_call_id=logical_call_id,
                    source_key=source_key,
                    provider=provider,
                    model=model,
                    operation=operation,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    attempt=attempt,
                    status=status,
                    fidelity=fidelity,
                    started_at=started_at,
                    completed_at=completed_at,
                    duration_ms=max(
                        0, int((as_utc(completed_at) - as_utc(started_at)).total_seconds() * 1000)
                    ),
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    input_language=detect_language(input_text),
                    output_language=detect_language(safe_raw),
                    messages=safe_messages,
                    schema_payload=safe_schema,
                    raw_response=safe_raw,
                    parsed_response=safe_parsed,
                    error=safe_error,
                    metrics=safe_metrics,
                )
            )
            db.commit()
    except Exception:
        # Auditing must never break inference or research delivery.
        return


def _encode_cursor(row: ModelCallAuditRow) -> str:
    payload = f"{as_utc(row.started_at).isoformat()}|{row.id}"
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def _decode_cursor(value: str) -> tuple[datetime, UUID]:
    try:
        padded = value + "=" * (-len(value) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode()).decode()
        timestamp, row_id = decoded.rsplit("|", 1)
        return datetime.fromisoformat(timestamp), UUID(row_id)
    except Exception as exc:
        raise ValueError("invalid model audit cursor") from exc


def _filters(
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    model: str | None = None,
    provider: str | None = None,
    operation: str | None = None,
    status: str | None = None,
    language: str | None = None,
    fidelity: str | None = None,
) -> list[Any]:
    conditions: list[Any] = []
    if start:
        conditions.append(ModelCallAuditRow.started_at >= as_utc(start))
    if end:
        conditions.append(ModelCallAuditRow.started_at <= as_utc(end))
    if model:
        conditions.append(ModelCallAuditRow.model == model)
    if provider:
        conditions.append(ModelCallAuditRow.provider == provider)
    if operation:
        conditions.append(ModelCallAuditRow.operation == operation)
    if status:
        conditions.append(ModelCallAuditRow.status == status)
    if language:
        conditions.append(
            or_(
                ModelCallAuditRow.input_language == language,
                ModelCallAuditRow.output_language == language,
            )
        )
    if fidelity:
        conditions.append(ModelCallAuditRow.fidelity == fidelity)
    return conditions


def audit_summary(row: ModelCallAuditRow) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "logical_call_id": str(row.logical_call_id),
        "provider": row.provider,
        "model": row.model,
        "operation": row.operation,
        "entity_type": row.entity_type,
        "entity_id": row.entity_id,
        "attempt": row.attempt,
        "status": row.status,
        "fidelity": row.fidelity,
        "started_at": as_utc(row.started_at),
        "completed_at": as_utc(row.completed_at),
        "duration_ms": row.duration_ms,
        "prompt_tokens": row.prompt_tokens,
        "completion_tokens": row.completion_tokens,
        "input_language": row.input_language,
        "output_language": row.output_language,
    }


def audit_detail(row: ModelCallAuditRow) -> dict[str, Any]:
    return {
        **audit_summary(row),
        "messages": row.messages or [],
        "schema": row.schema_payload or {},
        "raw_response": row.raw_response or "",
        "parsed_response": row.parsed_response,
        "error": row.error,
        "metrics": row.metrics or {},
    }


def list_model_audits(db: Session, *, limit: int, cursor: str | None = None, **kwargs: Any) -> dict:
    conditions = _filters(**kwargs)
    if cursor:
        cursor_time, cursor_id = _decode_cursor(cursor)
        conditions.append(
            or_(
                ModelCallAuditRow.started_at < as_utc(cursor_time),
                and_(
                    ModelCallAuditRow.started_at == as_utc(cursor_time),
                    ModelCallAuditRow.id < cursor_id,
                ),
            )
        )
    statement = select(ModelCallAuditRow)
    if conditions:
        statement = statement.where(*conditions)
    rows = list(
        db.scalars(
            statement.order_by(
                ModelCallAuditRow.started_at.desc(), ModelCallAuditRow.id.desc()
            ).limit(limit + 1)
        )
    )
    has_more = len(rows) > limit
    visible = rows[:limit]
    return {
        "items": [audit_summary(row) for row in visible],
        "next_cursor": _encode_cursor(visible[-1]) if has_more and visible else None,
    }


def model_usage(db: Session, **kwargs: Any) -> dict[str, Any]:
    statement = select(
        ModelCallAuditRow.model,
        ModelCallAuditRow.status,
        ModelCallAuditRow.duration_ms,
        ModelCallAuditRow.prompt_tokens,
        ModelCallAuditRow.completion_tokens,
        ModelCallAuditRow.operation,
        ModelCallAuditRow.provider,
    )
    conditions = _filters(**kwargs)
    if conditions:
        statement = statement.where(*conditions)
    rows = list(db.execute(statement))
    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        grouped[row.model].append(row)

    def aggregate(items: list[Any]) -> dict[str, Any]:
        durations = [item.duration_ms for item in items if item.duration_ms is not None]
        successes = sum(item.status == "completed" for item in items)
        return {
            "calls": len(items),
            "successes": successes,
            "failures": len(items) - successes,
            "success_rate": successes / len(items) if items else 0,
            "average_duration_ms": round(sum(durations) / len(durations)) if durations else None,
            "prompt_tokens": sum(item.prompt_tokens or 0 for item in items),
            "completion_tokens": sum(item.completion_tokens or 0 for item in items),
        }

    return {
        **aggregate(rows),
        "models": [
            {"model": model, **aggregate(items)} for model, items in sorted(grouped.items())
        ],
        "operations": sorted({row.operation for row in rows}),
        "providers": sorted({row.provider for row in rows}),
    }


def cleanup_model_audits(db: Session, retention_days: int | None = None) -> int:
    days = retention_days or get_settings().model_audit_retention_days
    result = db.execute(
        delete(ModelCallAuditRow).where(
            ModelCallAuditRow.started_at < utc_now() - timedelta(days=days)
        )
    )
    db.commit()
    return int(result.rowcount or 0)


def _step_time(step: dict[str, Any], fallback: datetime) -> datetime:
    value = step.get("occurred_at")
    if isinstance(value, datetime):
        return as_utc(value)
    if isinstance(value, str):
        return as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    return as_utc(fallback)


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, indent=2)


def _legacy_messages(input_snapshot: Any) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": "历史原始 system prompt 未记录。"},
        {
            "role": "user",
            "content": "以下是根据已归档业务数据重建的输入快照，并非原始 prompt：\n"
            + _json_text(input_snapshot),
        },
    ]


def _add_legacy_row(
    db: Session,
    *,
    source_key: str,
    model: str,
    operation: str,
    status: str,
    occurred_at: datetime,
    entity_type: str,
    entity_id: str,
    input_snapshot: Any,
    output_snapshot: Any,
    fidelity: str = "reconstructed",
) -> bool:
    if db.scalar(select(ModelCallAuditRow.id).where(ModelCallAuditRow.source_key == source_key)):
        return False
    output_text = _json_text(output_snapshot)
    messages = redact_value(_legacy_messages(input_snapshot))
    parsed = redact_value(output_snapshot)
    row = ModelCallAuditRow(
        logical_call_id=uuid4(),
        source_key=source_key,
        provider="ollama",
        model=model,
        operation=operation,
        entity_type=entity_type,
        entity_id=entity_id,
        attempt=1,
        status=status,
        fidelity=fidelity,
        started_at=occurred_at,
        completed_at=occurred_at,
        duration_ms=None,
        prompt_tokens=None,
        completion_tokens=None,
        input_language=detect_language(_json_text(messages)),
        output_language=detect_language(output_text),
        messages=messages,
        schema_payload={},
        raw_response=redact_text(output_text),
        parsed_response=parsed,
        error=None,
        metrics={"legacy": True, "notice": "历史内容由持久化业务数据重建"},
    )
    db.add(row)
    return True


def backfill_legacy_model_audits(db: Session) -> int:
    created = 0
    settings = get_settings()
    configured_models = {
        settings.ollama_extract_model,
        settings.ollama_research_model,
        settings.ollama_code_model,
        settings.cloud_llm_model,
    } - {""}
    news_by_id = {str(row.id): row for row in db.scalars(select(NewsRow)).all()}
    events = list(db.scalars(select(EventRow)).all())
    event_payloads = {str(row.id): row.payload for row in events}

    for row in events:
        payload = row.payload or {}
        related_news = [
            news_by_id[item_id]
            for item_id in (str(value) for value in payload.get("news_item_ids", []))
            if item_id in news_by_id
        ]
        news_snapshot = [
            {
                "id": str(item.id),
                "source": item.source,
                "title": item.title,
                "summary": item.summary,
                "language": item.language,
                "published_at": item.published_at,
            }
            for item in related_news
        ]
        for step in payload.get("analysis_steps", []):
            phase = step.get("phase")
            model = step.get("model")
            if phase not in {"event_extraction", "asset_mapping"} or model not in configured_models:
                continue
            occurred_at = _step_time(step, row.observed_at)
            if phase == "event_extraction":
                input_snapshot: Any = news_snapshot
                output_snapshot: Any = {
                    key: value
                    for key, value in payload.items()
                    if key not in {"analysis_steps", "candidates"}
                }
            else:
                input_snapshot = {"event": row.headline, "news": news_snapshot}
                output_snapshot = {"candidates": payload.get("candidates", [])}
            business_status = step.get("status", "completed")
            audit_status = "completed" if business_status in {"completed", "unmapped"} else "failed"
            if audit_status == "failed":
                output_snapshot = {"历史输出": "模型调用失败，原始响应未保存"}
            source_key = f"news_event:{row.id}:{phase}:{occurred_at.isoformat()}:{model}"
            created += _add_legacy_row(
                db,
                source_key=source_key,
                model=model,
                operation=phase,
                status=audit_status,
                occurred_at=occurred_at,
                entity_type="news_event",
                entity_id=str(row.id),
                input_snapshot=input_snapshot,
                output_snapshot=output_snapshot,
                fidelity="reconstructed" if audit_status == "completed" else "structured_only",
            )

    for row in db.scalars(select(ResearchRunRow)).all():
        payload = row.payload or {}
        for step in payload.get("analysis_steps", []):
            phase = step.get("phase")
            model = step.get("model")
            if phase not in {"report_drafting", "report_revision", "cloud_verification"} or model not in configured_models:
                continue
            occurred_at = _step_time(step, row.updated_at)
            input_snapshot = {
                "asset": payload.get("asset"),
                "event": event_payloads.get(str(row.event_id)) if row.event_id else None,
                "evidence": payload.get("evidence", []),
                "verification_round": payload.get("verification_round"),
            }
            output_snapshot = {
                "thesis": payload.get("thesis"),
                "recommendation": payload.get("recommendation"),
                "missing_requirements": payload.get("missing_requirements", []),
                "contradictions": payload.get("contradictions", []),
            }
            business_status = step.get("status", "completed")
            audit_status = "completed" if business_status == "completed" else "failed"
            if audit_status == "failed":
                output_snapshot = {"历史输出": "模型调用失败或回退，原始响应未保存"}
            source_key = f"research_run:{row.id}:{phase}:{occurred_at.isoformat()}:{model}"
            created += _add_legacy_row(
                db,
                source_key=source_key,
                model=model,
                operation=phase,
                status=audit_status,
                occurred_at=occurred_at,
                entity_type="research_run",
                entity_id=str(row.id),
                input_snapshot=input_snapshot,
                output_snapshot=output_snapshot,
                fidelity="reconstructed" if audit_status == "completed" else "structured_only",
            )

    for row in db.scalars(select(EventResearchRunRow)).all():
        payload = row.payload or {}
        for step in payload.get("analysis_steps", []):
            phase = step.get("phase")
            model = step.get("model")
            if phase not in {"event_report_drafting", "event_report_revision"} or model not in configured_models:
                continue
            occurred_at = _step_time(step, row.updated_at)
            source_key = f"event_research_run:{row.id}:{phase}:{occurred_at.isoformat()}:{model}"
            created += _add_legacy_row(
                db,
                source_key=source_key,
                model=model,
                operation=phase,
                status=step.get("status", "completed"),
                occurred_at=occurred_at,
                entity_type="event_research_run",
                entity_id=str(row.id),
                input_snapshot={
                    "event": event_payloads.get(str(row.event_id)),
                    "evidence": payload.get("evidence", []),
                },
                output_snapshot=payload.get("report") or {"历史输出": "仅保留最终结构化研报"},
                fidelity="structured_only" if not payload.get("report") else "reconstructed",
            )

    for row in db.scalars(select(EvolutionRow)).all():
        payload = row.payload or {}
        source_key = f"evolution:{row.id}:evolution_proposal:{as_utc(row.created_at).isoformat()}"
        created += _add_legacy_row(
            db,
            source_key=source_key,
            model=get_settings().ollama_code_model,
            operation="evolution_proposal",
            status="completed",
            occurred_at=as_utc(row.created_at),
            entity_type="evolution_candidate",
            entity_id=str(row.id),
            input_snapshot={"历史输入": "失败案例正文未单独保存"},
            output_snapshot=payload,
            fidelity="structured_only",
        )
    db.commit()
    return created


def main() -> None:
    parser = argparse.ArgumentParser(description="Model audit maintenance")
    parser.add_argument("command", choices=("backfill", "cleanup"))
    args = parser.parse_args()
    init_db()
    with SessionLocal() as db:
        count = (
            backfill_legacy_model_audits(db)
            if args.command == "backfill"
            else cleanup_model_audits(db)
        )
    print(json.dumps({"command": args.command, "affected": count}, ensure_ascii=False))


if __name__ == "__main__":
    main()
