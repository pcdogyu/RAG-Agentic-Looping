from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

from dateutil.parser import parse as parse_datetime

from backend.app.domain import AssetRef, Market, NewsItem, SourceQuality


class AkShareProvider:
    """Best-effort adapter; upstream public endpoints can change without notice."""

    name = "akshare"

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
                published = parse_datetime(str(value)).replace(tzinfo=UTC)
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
                )
            )
        return output[:limit]

    def resolve_assets(self, query: str) -> list[AssetRef]:
        return []

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
