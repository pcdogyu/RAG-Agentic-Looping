from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from celery import Celery
from celery.signals import task_failure, task_success
from redis import Redis
from sqlalchemy import func, select

from backend.app.config import get_settings
from backend.app.db import NewsRow, SessionLocal, init_db
from backend.app.domain import Rating, as_utc, utc_now
from backend.app.providers.registry import ProviderRegistry
from backend.app.services.events import EventService
from backend.app.services.evolution import EvolutionService
from backend.app.services.notifications import notifier
from backend.app.services.outcomes import OutcomeService
from backend.app.services.portfolio import PortfolioService
from backend.app.services.research import ResearchService
from backend.app.storage import (
    get_asset,
    get_event,
    list_assets,
    list_evolutions,
    list_outcomes,
    upsert_asset,
)

settings = get_settings()
celery_app = Celery("market-loop", broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    beat_schedule={
        "discover-news": {
            "task": "market_loop.scan_news",
            "schedule": settings.scan_interval_minutes * 60,
            "options": {"queue": "io"},
        },
        "refresh-crypto-universe": {
            "task": "market_loop.refresh_crypto_universe",
            "schedule": 6 * 60 * 60,
            "options": {"queue": "io"},
        },
        "evaluate-outcomes": {
            "task": "market_loop.evaluate_outcomes",
            "schedule": 24 * 60 * 60,
            "options": {"queue": "io"},
        },
        "evolve-from-failures": {
            "task": "market_loop.evolve_from_outcomes",
            "schedule": 7 * 24 * 60 * 60,
            "options": {"queue": "evolution"},
        },
        "system-monitor": {
            "task": "market_loop.monitor_health",
            "schedule": 5 * 60,
            "options": {"queue": "evolution"},
        },
    },
)
if not settings.evolution_enabled:
    celery_app.conf.beat_schedule.pop("evolve-from-failures", None)
    celery_app.conf.beat_schedule.pop("system-monitor", None)


def _record_task_result(kind: str) -> None:
    try:
        client = Redis.from_url(settings.redis_url, socket_connect_timeout=0.5)
        key = f"market-loop:tasks:{kind}"
        client.incr(key)
        client.expire(key, 3600)
    except Exception:
        pass


@task_success.connect
def _task_succeeded(**kwargs) -> None:
    _record_task_result("success")


@task_failure.connect
def _task_failed(**kwargs) -> None:
    _record_task_result("failure")


@celery_app.task(
    name="market_loop.scan_news", autoretry_for=(Exception,), retry_backoff=True, max_retries=3
)
def scan_news() -> dict:
    init_db()
    registry = ProviderRegistry()
    since = utc_now() - timedelta(minutes=settings.scan_interval_minutes * 2)
    items = registry.discover_news(since=since, limit=200)
    with SessionLocal() as db:
        service = EventService(registry)
        events = service.ingest(db, items)
        for error in registry.last_errors:
            notifier.send(f"数据源故障：{error}")
        for event in events:
            if event.priority >= 0.75:
                notifier.send(
                    f"高优先级事件：{event.headline}\n类型：{event.event_type.value}\n"
                    f"候选标的：{', '.join(item.asset.symbol for item in event.candidates[:5]) or '待解析'}"
                )
        queued = 0
        if settings.auto_research:
            for event in events:
                for candidate in event.candidates[:3]:
                    if event.priority >= 0.4 and candidate.relevance >= 0.7:
                        research_asset.apply_async(
                            args=[candidate.asset.asset_id, str(event.id)], queue="llm"
                        )
                        queued += 1
    return {"discovered": len(items), "events": len(events), "research_queued": queued}


@celery_app.task(
    name="market_loop.research_asset", autoretry_for=(Exception,), retry_backoff=True, max_retries=2
)
def research_asset(asset_id: str, event_id: str | None = None) -> dict:
    init_db()
    registry = ProviderRegistry()
    with SessionLocal() as db:
        asset = get_asset(db, asset_id) or registry.get_asset(asset_id)
        if not asset:
            raise ValueError(f"unknown asset: {asset_id}")
        event = get_event(db, UUID(event_id)) if event_id else None
        run = ResearchService(registry, db).run(asset, event)
        if run.recommendation:
            notifier.recommendation(run.recommendation)
            if settings.auto_paper_trade and run.recommendation.rating in {
                Rating.BULLISH,
                Rating.STRONGLY_BULLISH,
            }:
                portfolio = PortfolioService(registry)
                price = portfolio.current_price(run.recommendation)
                if price > 0:
                    order = portfolio.create_from_recommendation(db, run.recommendation, price)
                    notifier.send(
                        f"模拟仓位变化：{order.side.value} {order.asset.symbol} "
                        f"{order.quantity:g} @ {order.price:g} {order.currency}"
                    )
        return run.model_dump(mode="json")


@celery_app.task(name="market_loop.refresh_crypto_universe")
def refresh_crypto_universe() -> dict:
    init_db()
    registry = ProviderRegistry()
    assets = registry.refresh_crypto_universe()
    with SessionLocal() as db:
        for asset in assets:
            upsert_asset(db, asset)
    return {"assets": len(assets)}


@celery_app.task(name="market_loop.evaluate_outcomes")
def evaluate_outcomes() -> dict:
    init_db()
    registry = ProviderRegistry()
    with SessionLocal() as db:
        outcomes = OutcomeService(registry).evaluate_due(db)
    return {"outcomes": len(outcomes)}


@celery_app.task(name="market_loop.seed_assets")
def seed_assets() -> dict:
    init_db()
    registry = ProviderRegistry()
    with SessionLocal() as db:
        for asset in registry.all_assets():
            upsert_asset(db, asset)
        count = len(list_assets(db))
    return {"assets": count}


@celery_app.task(name="market_loop.evolve_from_outcomes")
def evolve_from_outcomes() -> dict:
    if not settings.evolution_enabled:
        return {"status": "disabled"}
    init_db()
    with SessionLocal() as db:
        failures = [
            item.model_dump(mode="json")
            for item in list_outcomes(db)
            if not item.direction_correct or item.alpha < 0
        ][:50]
        if not failures:
            return {"status": "no-failures"}
    return evolve_failures.run(failures)


@celery_app.task(name="market_loop.evolve_failures")
def evolve_failures(failures: list[dict]) -> dict:
    if not settings.evolution_enabled:
        return {"status": "disabled"}
    init_db()
    with SessionLocal() as db:
        service = EvolutionService()
        candidate = service.propose(db, failures)
        result = service.execute(db, candidate)
        notifier.send(f"演进结果：{result.status.value}\n分支：{result.branch}\n假设：{result.hypothesis}")
        return result.model_dump(mode="json")


@celery_app.task(name="market_loop.execute_evolution")
def execute_evolution(candidate_id: str) -> dict:
    init_db()
    with SessionLocal() as db:
        candidate = next(
            (item for item in list_evolutions(db) if str(item.id) == candidate_id), None
        )
        if not candidate:
            raise ValueError(f"unknown evolution candidate: {candidate_id}")
        result = EvolutionService().execute(db, candidate)
        notifier.send(f"演进结果：{result.status.value}\n分支：{result.branch}")
        return result.model_dump(mode="json")


@celery_app.task(name="market_loop.monitor_health")
def monitor_health() -> dict:
    init_db()
    client = Redis.from_url(settings.redis_url, socket_connect_timeout=0.5)
    successes = int(client.get("market-loop:tasks:success") or 0)
    failures = int(client.get("market-loop:tasks:failure") or 0)
    total = successes + failures
    failure_rate = failures / total if total else 0.0
    with SessionLocal() as db:
        latest_news = db.scalar(select(func.max(NewsRow.observed_at)))
    stale = bool(
        latest_news
        and (utc_now() - as_utc(latest_news)).total_seconds()
        > settings.scan_interval_minutes * 180
    )
    unhealthy = (total >= 10 and failure_rate > 0.10) or stale
    rolled_back = False
    if unhealthy and settings.evolution_enabled and settings.evolution_auto_merge:
        try:
            EvolutionService().rollback()
            rolled_back = True
            notifier.send(
                f"系统已自动回滚：任务失败率 {failure_rate:.1%}，数据过期：{'是' if stale else '否'}"
            )
        except Exception:
            notifier.send("系统健康门禁触发，但自动回滚失败；请人工检查。")
    return {
        "failure_rate": failure_rate,
        "samples": total,
        "data_stale": stale,
        "rolled_back": rolled_back,
    }
