from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from zoneinfo import ZoneInfo

from dateutil.parser import parse as parse_datetime

from backend.app.domain import AssetClass, AssetRef, Market, NewsItem, SourceQuality
from backend.app.providers.cache import cache

SHANGHAI_TIMEZONE = ZoneInfo("Asia/Shanghai")
TIME_NORMALIZATION_MARKER = "Asia/Shanghai->UTC:v1"


def _normalize_security_text(value: str) -> str:
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", value.lower())


class AkShareProvider:
    """Best-effort adapter; upstream public endpoints can change without notice."""

    name = "akshare"

    def __init__(self) -> None:
        self.last_errors: list[str] = []

    def discover_news(self, *, since: datetime, limit: int = 100) -> list[NewsItem]:
        try:
            import akshare as ak

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

    @staticmethod
    def _matches(asset: AssetRef, normalized_query: str) -> bool:
        symbol = _normalize_security_text(asset.symbol)
        name = _normalize_security_text(asset.name)
        return normalized_query == symbol or (len(name) >= 2 and name in normalized_query)

    def _listed_assets(self) -> list[AssetRef]:
        key = cache.key("akshare-security-master", {"version": 1})

        def loader() -> list[dict[str, Any]]:
            try:
                import akshare as ak
            except Exception as exc:
                self.last_errors.append(f"import: {type(exc).__name__}")
                return []

            records: list[dict[str, Any]] = []
            try:
                records.extend(self._a_share_records(ak.stock_info_a_code_name()))
            except Exception as exc:
                self.last_errors.append(f"a-share-master: {type(exc).__name__}")
            try:
                records.extend(self._hk_share_records(ak.stock_hk_spot_em()))
            except Exception as exc:
                self.last_errors.append(f"hk-share-master: {type(exc).__name__}")
            return records

        payload = cache.get(key)
        if not payload:
            payload = loader()
            if payload:
                cache.set(key, payload, 24 * 60 * 60)
        return [AssetRef.model_validate(item) for item in payload]

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
                ).model_dump(mode="json")
            )
        return output

    @staticmethod
    def _hk_share_records(frame: Any) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
            raw_code = str(row.get("代码") or row.get("code") or "").strip()
            name = str(row.get("名称") or row.get("name") or "").strip()
            if not raw_code or not name or not raw_code.isdigit():
                continue
            code = raw_code.zfill(5)
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
                ).model_dump(mode="json")
            )
        return output

    def get_prices(self, asset: AssetRef, **kwargs: Any) -> list[dict[str, Any]]:
        try:
            import akshare as ak

            if asset.market.value == "CN":
                frame = ak.stock_zh_a_hist(symbol=asset.symbol, period="daily", adjust="qfq")
            else:
                frame = ak.stock_hk_hist(symbol=asset.symbol, period="daily", adjust="qfq")
            return frame.to_dict(orient="records")
        except Exception:
            return []

    def get_fundamentals(self, asset: AssetRef) -> dict[str, Any]:
        return {}

    def get_filings(self, asset: AssetRef) -> list[dict[str, Any]]:
        if asset.market not in {Market.CN, Market.HK}:
            return []
        try:
            import akshare as ak

            end = datetime.now(UTC).date()
            start = end - timedelta(days=5 * 366)
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
