from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from datetime import UTC, date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("market-adapter")


def _records(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    frame = frame.where(frame.notna(), None)
    return [
        {str(key): _primitive(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _primitive(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def resolve_assets(payload: dict[str, Any]) -> dict[str, Any]:
    import akshare as ak

    query = str(payload.get("query") or "").strip().casefold()
    if not query:
        raise ValueError("query is required")
    limit = max(1, min(int(payload.get("limit") or 20), 100))
    matches: list[dict[str, Any]] = []
    for market, loader, code_key, name_key in (
        ("CN", ak.stock_info_a_code_name, "code", "name"),
        ("HK", ak.stock_hk_spot_em, "代码", "名称"),
    ):
        for row in _records(loader()):
            code = str(row.get(code_key) or "")
            name = str(row.get(name_key) or "")
            if query not in f"{code} {name}".casefold():
                continue
            exchange = "XHKG" if market == "HK" else ("XSHG" if code.startswith("6") else "XSHE")
            symbol = code.zfill(5 if market == "HK" else 6)
            matches.append(
                {
                    "asset_id": f"equity:{exchange}:{symbol}",
                    "asset_class": "equity",
                    "market": market,
                    "symbol": symbol,
                    "name": name,
                    "exchange_or_provider": exchange,
                    "currency": "HKD" if market == "HK" else "CNY",
                    "aliases": [],
                    "products": [],
                    "competitors": [],
                    "lot_size": 100,
                    "active": True,
                }
            )
            if len(matches) >= limit:
                return {"items": matches}
    return {"items": matches}


def prices(payload: dict[str, Any]) -> dict[str, Any]:
    import akshare as ak

    symbol = str(payload.get("symbol") or "").strip()
    market = str(payload.get("market") or "CN").upper()
    if not symbol:
        raise ValueError("symbol is required")
    start = str(payload.get("start") or date.today().replace(year=date.today().year - 1)).replace("-", "")
    end = str(payload.get("end") or date.today()).replace("-", "")
    if market == "HK":
        frame = ak.stock_hk_hist(symbol=symbol, period="daily", start_date=start, end_date=end, adjust="qfq")
    else:
        frame = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start, end_date=end, adjust="qfq")
    return {"items": _records(frame)}


def fundamentals(payload: dict[str, Any]) -> dict[str, Any]:
    import akshare as ak

    symbol = str(payload.get("symbol") or "").strip()
    market = str(payload.get("market") or "CN").upper()
    if not symbol:
        raise ValueError("symbol is required")
    if market != "CN":
        return {"items": [], "unsupported": True}
    return {"items": _records(ak.stock_financial_analysis_indicator(symbol=symbol))}


def discover_news(payload: dict[str, Any]) -> dict[str, Any]:
    import akshare as ak

    limit = max(1, min(int(payload.get("limit") or 40), 200))
    since_value = str(payload.get("since") or "")
    since = datetime.fromisoformat(since_value.replace("Z", "+00:00")) if since_value else None
    frame = ak.stock_info_global_em()
    items: list[dict[str, Any]] = []
    for row in _records(frame)[: limit * 3]:
        title = str(row.get("标题") or row.get("title") or "").strip()
        url = str(row.get("链接") or row.get("url") or "").strip()
        raw_time = row.get("发布时间") or row.get("时间") or row.get("date")
        if not title or not url:
            continue
        try:
            published = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00"))
            if published.tzinfo is None:
                published = published.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        except ValueError:
            published = datetime.now(UTC)
        if since is not None and published < since:
            continue
        items.append({
            "source": str(row.get("来源") or "东方财富/AkShare"),
            "title": title,
            "summary": str(row.get("摘要") or row.get("内容") or ""),
            "url": url,
            "published_at": published.astimezone(UTC).isoformat(),
            "language": "zh",
        })
        if len(items) >= limit:
            break
    return {"items": items}


ROUTES: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "/v1/assets/resolve": resolve_assets,
    "/v1/prices": prices,
    "/v1/fundamentals": fundamentals,
    "/v1/news": discover_news,
}


class Handler(BaseHTTPRequestHandler):
    server_version = "market-adapter/1"

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._json(200, {"status": "ok", "service": "market-adapter"})
            return
        self._json(404, {"detail": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        handler = ROUTES.get(self.path)
        if handler is None:
            self._json(404, {"detail": "not found"})
            return
        try:
            size = int(self.headers.get("Content-Length") or 0)
            if size > 1_000_000:
                raise ValueError("request is too large")
            payload = json.loads(self.rfile.read(size) or b"{}")
            self._json(200, handler(payload))
        except ValueError as exc:
            self._json(422, {"detail": str(exc)})
        except Exception as exc:  # pragma: no cover - provider failures are integration-tested
            logger.exception("adapter request failed")
            self._json(502, {"detail": f"{type(exc).__name__}: market provider failed"})

    def log_message(self, format: str, *args: Any) -> None:
        logger.info(format, *args)

    def _json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    address = os.getenv("ADAPTER_ADDRESS", "0.0.0.0")
    port = int(os.getenv("ADAPTER_PORT", "8091"))
    ThreadingHTTPServer((address, port), Handler).serve_forever()
