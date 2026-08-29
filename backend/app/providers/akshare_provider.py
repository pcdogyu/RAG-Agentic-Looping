from __future__ import annotations

import re
import socket
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from math import isfinite
from typing import Any
from zoneinfo import ZoneInfo

from dateutil.parser import parse as parse_datetime

from backend.app.domain import AssetClass, AssetRef, Market, NewsItem, SourceQuality
from backend.app.providers.cache import cache
from backend.app.services.industry_taxonomy import normalize_industry

SHANGHAI_TIMEZONE = ZoneInfo("Asia/Shanghai")
TIME_NORMALIZATION_MARKER = "Asia/Shanghai->UTC:v1"
_ADDRESS_FAMILY_LOCK = threading.Lock()


@contextmanager
def _request_address_family(ipv4_only: bool) -> Iterator[None]:
    if not ipv4_only:
        yield
        return
    from urllib3.util import connection

    with _ADDRESS_FAMILY_LOCK:
        original = connection.allowed_gai_family
        connection.allowed_gai_family = lambda: socket.AF_INET
        try:
            yield
        finally:
            connection.allowed_gai_family = original


def _normalize_security_text(value: str) -> str:
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", value.lower())


def _json_scalar(value: Any) -> Any:
    """Convert pandas/numpy values into cache and checkpoint-safe primitives."""

    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and not isfinite(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    if isinstance(value, str):
        return value.strip()
    return value


def _frame_records(frame: Any) -> list[dict[str, Any]]:
    return [
        {str(key): _json_scalar(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


class AkShareProvider:
    """Best-effort adapter; upstream public endpoints can change without notice."""

    name = "akshare"

    def __init__(self, settings=None) -> None:
        from backend.app.config import get_settings

        self.settings = settings or get_settings()
        self.last_errors: list[str] = []

    def discover_news(self, *, since: datetime, limit: int = 100) -> list[NewsItem]:
        try:
            import akshare as ak

            with _request_address_family(self.settings.akshare_ipv4_only):
                frame = ak.stock_info_global_em()
        except Exception:
            return []
        output: list[NewsItem] = []
        for row in frame.head(limit * 2).to_dict(orient="records"):
            title = str(row.get("标题") or row.get("title") or "")
            url = str(row.get("链接") or row.get("url") or "")
            value = row.get("发布时间") or row.get("时间") or row.get("date")
            if not title or not url:
                continue
            try:
                parsed = parse_datetime(str(value))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=SHANGHAI_TIMEZONE)
                published = parsed.astimezone(UTC)
            except Exception:
                published = datetime.now(UTC)
            if published < since:
                continue
            output.append(
                NewsItem(
                    source=str(row.get("来源") or "东方财富/AkShare"),
                    source_quality=SourceQuality.AGGREGATOR,
                    title=title,
                    summary=str(row.get("摘要") or row.get("内容") or ""),
                    url=url,
                    language="zh",
                    published_at=published,
                    as_of=published,
                    content_hash=sha256(f"{title}|{url}".encode()).hexdigest(),
                    raw_metadata={"time_normalization": TIME_NORMALIZATION_MARKER},
                )
            )
        return output[:limit]

    def resolve_assets(self, query: str) -> list[AssetRef]:
        normalized_query = _normalize_security_text(query)
        if not normalized_query:
            return []
        return [
            asset
            for asset in self._listed_assets()
            if self._matches(asset, normalized_query)
        ]

    def list_equity_universe(self) -> list[AssetRef]:
        """Return the cached full A-share and Hong Kong company master."""

        return self._listed_assets()

    @staticmethod
    def _matches(asset: AssetRef, normalized_query: str) -> bool:
        symbol = _normalize_security_text(asset.symbol)
        name = _normalize_security_text(asset.name)
        symbol_matches = normalized_query == symbol
        if symbol.isdigit() and len(symbol) == 5 and normalized_query.isdigit():
            symbol_matches = normalized_query.zfill(5) == symbol
        return symbol_matches or (len(name) >= 2 and name in normalized_query)

    def _listed_assets(self) -> list[AssetRef]:
        a_share_key = cache.key("akshare-a-share-security-master", {"version": 2})
        hk_share_key = cache.key("akshare-hk-share-security-master", {"version": 2})
        a_share_payload = cache.get(a_share_key)
        hk_share_payload = cache.get(hk_share_key)
        if a_share_payload and hk_share_payload:
            return [
                AssetRef.model_validate(item)
                for item in [*a_share_payload, *hk_share_payload]
            ]
        try:
            import akshare as ak
        except Exception as exc:
            self.last_errors.append(f"import: {type(exc).__name__}")
            return [
                AssetRef.model_validate(item)
                for item in [*(a_share_payload or []), *(hk_share_payload or [])]
            ]

        def market_master(
            key: str,
            cached_payload: list[dict[str, Any]] | None,
            error_label: str,
            loader,
            normalizer,
        ) -> list[dict[str, Any]]:
            if cached_payload:
                return cached_payload
            try:
                with _request_address_family(self.settings.akshare_ipv4_only):
                    payload = normalizer(loader())
            except Exception as exc:
                self.last_errors.append(f"{error_label}: {type(exc).__name__}")
                return []
            if payload:
                cache.set(key, payload, 24 * 60 * 60)
            return payload

        payload = [
            *market_master(
                a_share_key,
                a_share_payload,
                "a-share-master",
                ak.stock_info_a_code_name,
                self._a_share_records,
            ),
            *market_master(
                hk_share_key,
                hk_share_payload,
                "hk-share-master",
                lambda: self._hk_security_frame(ak),
                self._hk_share_records,
            ),
        ]
        return [AssetRef.model_validate(item) for item in payload]

    @staticmethod
    def _hk_security_frame(ak: Any):
        """Use Sina's complete HK directory when Eastmoney closes the connection."""

        try:
            return ak.stock_hk_spot_em()
        except Exception:
            return ak.stock_hk_spot()

    @staticmethod
    def _a_share_records(frame: Any) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
            raw_code = str(row.get("code") or row.get("代码") or "").strip()
            name = str(row.get("name") or row.get("名称") or "").strip()
            if not raw_code or not name or not raw_code.isdigit():
                continue
            code = raw_code.zfill(6)
            exchange = "XSHG" if code.startswith("6") else "XBEI" if code[0] in "489" else "XSHE"
            raw_industry = str(row.get("所属行业") or row.get("行业") or "").strip()
            sector_id, industry_id = normalize_industry(raw_industry, raw_industry)
            market_cap = row.get("总市值") or row.get("市值")
            try:
                market_cap = float(market_cap) if market_cap is not None else None
            except (TypeError, ValueError):
                market_cap = None
            output.append(
                AssetRef(
                    asset_id=f"equity:{exchange}:{code}",
                    asset_class=AssetClass.EQUITY,
                    market=Market.CN,
                    symbol=code,
                    name=name,
                    exchange_or_provider=exchange,
                    currency="CNY",
                    lot_size=100,
                    sector_id=sector_id,
                    industry_id=industry_id,
                    raw_sector=raw_industry,
                    raw_industry=raw_industry,
                    instrument_type="common_stock",
                    market_cap=market_cap,
                ).model_dump(mode="json")
            )
        return output

    @staticmethod
    def _hk_share_records(frame: Any) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
            raw_code = str(row.get("代码") or row.get("code") or "").strip()
            name = str(
                row.get("名称")
                or row.get("中文名称")
                or row.get("英文名称")
                or row.get("name")
                or ""
            ).strip()
            if not raw_code or not name or not raw_code.isdigit():
                continue
            code = raw_code.zfill(5)
            raw_industry = str(row.get("所属行业") or row.get("行业") or "").strip()
            sector_id, industry_id = normalize_industry(raw_industry, raw_industry)
            market_cap = row.get("总市值") or row.get("市值")
            try:
                market_cap = float(market_cap) if market_cap is not None else None
            except (TypeError, ValueError):
                market_cap = None
            output.append(
                AssetRef(
                    asset_id=f"equity:XHKG:{code}",
                    asset_class=AssetClass.EQUITY,
                    market=Market.HK,
                    symbol=code,
                    name=name,
                    exchange_or_provider="XHKG",
                    currency="HKD",
                    lot_size=100,
                    sector_id=sector_id,
                    industry_id=industry_id,
                    raw_sector=raw_industry,
                    raw_industry=raw_industry,
                    instrument_type="common_stock",
                    market_cap=market_cap,
                ).model_dump(mode="json")
            )
        return output

    def get_prices(self, asset: AssetRef, **kwargs: Any) -> list[dict[str, Any]]:
        try:
            import akshare as ak

            with _request_address_family(self.settings.akshare_ipv4_only):
                if asset.market.value == "CN":
                    frame = ak.stock_zh_a_hist(
                        symbol=asset.symbol, period="daily", adjust="qfq"
                    )
                else:
                    frame = ak.stock_hk_hist(
                        symbol=asset.symbol, period="daily", adjust="qfq"
                    )
            return frame.to_dict(orient="records")
        except Exception:
            return []

    def get_fundamentals(self, asset: AssetRef) -> dict[str, Any]:
        if asset.market is not Market.CN:
            return {}
        try:
            import akshare as ak
        except Exception as exc:
            self.last_errors.append(f"fundamentals-import: {type(exc).__name__}")
            return {}

        suffix = {"XSHG": "SH", "XSHE": "SZ", "XBEI": "BJ"}.get(
            asset.exchange_or_provider,
            "SH" if asset.symbol.startswith("6") else "BJ" if asset.symbol[0] in "489" else "SZ",
        )
        datasets: dict[str, Any] = {"provider": "akshare"}

        def load_records(
            name: str,
            loader,
            *,
            ttl_seconds: int,
            keep: str = "head",
            limit: int,
        ) -> list[dict[str, Any]]:
            key = cache.key(name, {"symbol": asset.symbol, "version": 1})
            cached = cache.get(key)
            if cached is not None:
                return cached
            try:
                with _request_address_family(self.settings.akshare_ipv4_only):
                    records = _frame_records(loader())
            except Exception as exc:
                self.last_errors.append(f"{name}: {type(exc).__name__}")
                return []
            selected = records[:limit] if keep == "head" else records[-limit:]
            if selected:
                cache.set(key, selected, ttl_seconds)
            return selected

        business = load_records(
            "akshare-business-profile",
            lambda: ak.stock_zyjs_ths(symbol=asset.symbol),
            ttl_seconds=24 * 60 * 60,
            limit=1,
        )
        composition = load_records(
            "akshare-business-composition",
            lambda: ak.stock_zygc_em(symbol=f"{suffix}{asset.symbol}"),
            ttl_seconds=24 * 60 * 60,
            limit=24,
        )
        financials = load_records(
            "akshare-financial-indicators",
            lambda: ak.stock_financial_analysis_indicator_em(
                symbol=f"{asset.symbol}.{suffix}", indicator="按报告期"
            ),
            ttl_seconds=6 * 60 * 60,
            limit=8,
        )
        valuation = load_records(
            "akshare-valuation",
            lambda: ak.stock_value_em(symbol=asset.symbol),
            ttl_seconds=60 * 60,
            keep="tail",
            limit=30,
        )
        company_info_rows = load_records(
            "akshare-company-info",
            lambda: ak.stock_individual_info_em(symbol=asset.symbol),
            ttl_seconds=60 * 60,
            limit=30,
        )
        company_info = {
            str(row.get("item")): row.get("value")
            for row in company_info_rows
            if row.get("item")
        }
        if business:
            datasets["business_profile"] = business[0]
        if composition:
            datasets["business_composition"] = composition
        if financials:
            datasets["financial_indicators"] = financials
        if valuation:
            datasets["valuation"] = valuation
        if company_info:
            datasets["company_info"] = company_info
        return datasets

    def get_filings(self, asset: AssetRef) -> list[dict[str, Any]]:
        if asset.market not in {Market.CN, Market.HK}:
            return []
        try:
            import akshare as ak

            end = datetime.now(UTC).date()
            start = end - timedelta(days=5 * 366)
            with _request_address_family(self.settings.akshare_ipv4_only):
                frame = ak.stock_zh_a_disclosure_report_cninfo(
                    symbol=asset.symbol,
                    market="沪深京" if asset.market is Market.CN else "港股",
                    keyword="",
                    category="",
                    start_date=start.strftime("%Y%m%d"),
                    end_date=end.strftime("%Y%m%d"),
                )
        except Exception:
            return []
        output: list[dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
            output.append(
                {
                    "formType": str(row.get("公告标题") or "公告"),
                    "fillingDate": str(row.get("公告时间") or ""),
                    "finalLink": str(row.get("公告链接") or ""),
                    "source": "巨潮资讯/CNInfo",
                }
            )
        return output

    def get_crypto_metrics(self, asset: AssetRef) -> dict[str, Any]:
        return {}
