from __future__ import annotations

import re
import socket
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from math import isfinite
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dateutil.parser import parse as parse_datetime

from backend.app.domain import AssetClass, AssetRef, Market, NewsItem, SourceQuality
from backend.app.providers.base import ProviderError
from backend.app.providers.cache import cache
from backend.app.services.industry_taxonomy import normalize_industry

SHANGHAI_TIMEZONE = ZoneInfo("Asia/Shanghai")
TIME_NORMALIZATION_MARKER = "Asia/Shanghai->UTC:v1"
_ADDRESS_FAMILY_LOCK = threading.Lock()
_INDUSTRY_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60


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
        self._szse_directory = None
        self._sse_directory: list[dict[str, Any]] | None = None
        self._bjse_directory = None

    def discover_news(self, *, since: datetime, limit: int = 100) -> list[NewsItem]:
        try:
            import akshare as ak

            with _request_address_family(self.settings.akshare_ipv4_only):
                frame = ak.stock_info_global_em()
        except Exception as exc:
            error = f"news: {type(exc).__name__}: {exc}"[:500]
            self.last_errors.append(error)
            raise ProviderError(error) from exc
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
        return [asset for asset in self._listed_assets() if self._matches(asset, normalized_query)]

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
        a_share_key = cache.key("akshare-a-share-security-master", {"version": 4})
        hk_share_key = cache.key("akshare-hk-share-security-master", {"version": 4})
        a_share_payload = cache.get(a_share_key)
        hk_share_payload = cache.get(hk_share_key)
        if a_share_payload and hk_share_payload:
            return [AssetRef.model_validate(item) for item in [*a_share_payload, *hk_share_payload]]
        try:
            import akshare as ak
        except Exception as exc:
            self.last_errors.append(f"import: {type(exc).__name__}")
            return [
                AssetRef.model_validate(item)
                for item in [*(a_share_payload or []), *(hk_share_payload or [])]
            ]

        a_share_industries = (
            self._cached_industry_map(
                "akshare-a-share-industry-master",
                "a-share-industries",
                lambda: self._a_share_industry_map(ak),
                minimum_size=4_500,
            )
            if not a_share_payload
            else {}
        )
        hk_share_industries = (
            self._cached_industry_map(
                "akshare-hk-share-industry-master",
                "hk-share-industries",
                lambda: self._hk_share_industry_map(ak),
                minimum_size=2_000,
            )
            if not hk_share_payload
            else {}
        )

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
                lambda: self._a_share_security_frame(ak),
                lambda frame: self._a_share_records(frame, a_share_industries),
            ),
            *market_master(
                hk_share_key,
                hk_share_payload,
                "hk-share-master",
                lambda: self._hk_security_frame(ak),
                lambda frame: self._hk_share_records(frame, hk_share_industries),
            ),
        ]
        return [AssetRef.model_validate(item) for item in payload]

    def _cached_industry_map(
        self,
        namespace: str,
        error_label: str,
        loader,
        *,
        minimum_size: int = 1,
    ) -> dict[str, str]:
        key = cache.key(namespace, {"version": 1})
        cached = cache.get(key)
        if cached:
            return {str(code): str(industry) for code, industry in cached.items()}
        try:
            values = loader()
        except Exception as exc:
            self.last_errors.append(f"{error_label}: {type(exc).__name__}")
            return {}
        if len(values) >= minimum_size:
            cache.set(key, values, _INDUSTRY_CACHE_TTL_SECONDS)
        elif values:
            self.last_errors.append(f"{error_label}: incomplete ({len(values)} < {minimum_size})")
        return values

    def _a_share_industry_map(self, ak: Any) -> dict[str, str]:
        """Build a detailed board map, then fill gaps from official exchange lists."""

        output: dict[str, str] = {}
        if hasattr(ak, "stock_board_industry_name_em") and hasattr(
            ak, "stock_board_industry_cons_em"
        ):
            try:
                with _request_address_family(self.settings.akshare_ipv4_only):
                    boards = ak.stock_board_industry_name_em()
                for board in boards.to_dict(orient="records"):
                    board_name = str(board.get("板块名称") or "").strip()
                    board_code = str(board.get("板块代码") or board_name).strip()
                    if not board_name or not board_code:
                        continue
                    try:
                        with _request_address_family(self.settings.akshare_ipv4_only):
                            constituents = ak.stock_board_industry_cons_em(symbol=board_code)
                    except Exception as exc:
                        self.last_errors.append(
                            f"a-share-industry-{board_code}: {type(exc).__name__}"
                        )
                        continue
                    for row in constituents.to_dict(orient="records"):
                        code = str(row.get("代码") or row.get("code") or "").strip()
                        if code.isdigit():
                            output.setdefault(code.zfill(6), board_name)
            except Exception as exc:
                self.last_errors.append(f"a-share-industry-boards: {type(exc).__name__}")

        if hasattr(ak, "stock_info_bj_name_code"):
            try:
                self._merge_exchange_industries(
                    output,
                    self._load_bjse_directory(ak),
                    ("证券代码", "代码"),
                )
            except Exception as exc:
                self.last_errors.append(f"stock_info_bj_name_code: {type(exc).__name__}")

        if hasattr(ak, "stock_info_sz_name_code"):
            try:
                self._merge_szse_industries(output)
            except Exception as exc:
                self.last_errors.append(f"szse-industry-master: {type(exc).__name__}")
        if hasattr(ak, "stock_info_sh_name_code"):
            try:
                self._merge_sse_industries(output)
            except Exception as exc:
                self.last_errors.append(f"sse-industry-master: {type(exc).__name__}")
        return output

    @staticmethod
    def _merge_exchange_industries(
        output: dict[str, str], frame: Any, code_fields: tuple[str, ...]
    ) -> None:
        for row in frame.to_dict(orient="records"):
            code = next(
                (str(row.get(field) or "").strip() for field in code_fields if row.get(field)),
                "",
            )
            industry = str(
                row.get("所属行业") or row.get("行业") or row.get("行业门类") or ""
            ).strip()
            if code.isdigit() and industry:
                output.setdefault(code.zfill(6), industry)

    def _merge_sse_industries(self, output: dict[str, str]) -> None:
        for row in self._load_sse_directory():
            code = str(row.get("A_STOCK_CODE") or "").strip()
            industry = str(row.get("CSRC_CODE_DESC") or "").strip()
            if code.isdigit() and industry:
                output.setdefault(code.zfill(6), industry)

    def _load_sse_directory(self) -> list[dict[str, Any]]:
        if self._sse_directory is not None:
            return self._sse_directory
        url = "https://query.sse.com.cn/sseQuery/commonQuery.do"
        headers = {
            "Referer": "https://www.sse.com.cn/assortment/stock/list/share/",
            "User-Agent": "Mozilla/5.0",
        }
        output: list[dict[str, Any]] = []
        for stock_type in ("1", "8"):
            params = {
                "STOCK_TYPE": stock_type,
                "REG_PROVINCE": "",
                "CSRC_CODE": "",
                "STOCK_CODE": "",
                "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
                "COMPANY_STATUS": "2,4,5,7,8",
                "type": "inParams",
                "isPagination": "true",
                "pageHelp.cacheSize": "1",
                "pageHelp.beginPage": "1",
                "pageHelp.pageSize": "10000",
                "pageHelp.pageNo": "1",
                "pageHelp.endPage": "1",
            }
            with _request_address_family(self.settings.akshare_ipv4_only):
                response = requests.get(url, params=params, headers=headers, timeout=20)
            response.raise_for_status()
            output.extend(response.json().get("result", []))
        self._sse_directory = output
        return output

    def _merge_szse_industries(self, output: dict[str, str]) -> None:
        self._merge_exchange_industries(
            output,
            self._load_szse_directory(),
            ("A股代码", "证券代码"),
        )

    def _load_szse_directory(self):
        if self._szse_directory is not None:
            return self._szse_directory
        import pandas as pd

        url = "https://www.szse.cn/api/report/ShowReport"
        params = {
            "SHOWTYPE": "xlsx",
            "CATALOGID": "1110",
            "TABKEY": "tab1",
        }
        headers = {
            "Referer": "https://www.szse.cn/market/product/stock/list/index.html",
            "User-Agent": "Mozilla/5.0",
        }
        last_error: Exception | None = None
        for _attempt in range(3):
            try:
                with _request_address_family(self.settings.akshare_ipv4_only):
                    response = requests.get(
                        url,
                        params=params,
                        headers=headers,
                        timeout=60,
                    )
                response.raise_for_status()
                frame = pd.read_excel(BytesIO(response.content))
                self._szse_directory = frame
                return frame
            except Exception as exc:
                last_error = exc
        raise RuntimeError("Shenzhen industry directory unavailable") from last_error

    def _load_bjse_directory(self, ak: Any):
        if self._bjse_directory is not None:
            return self._bjse_directory
        loader = getattr(ak, "stock_info_bj_name_code", None)
        if not callable(loader):
            raise RuntimeError("Beijing security directory is unavailable")
        with _request_address_family(self.settings.akshare_ipv4_only):
            self._bjse_directory = loader()
        return self._bjse_directory

    def _a_share_security_frame(self, ak: Any):
        """Build the A-share identity directory from official exchanges."""

        import pandas as pd

        records: list[dict[str, Any]] = []
        official_capable = all(
            hasattr(ak, loader)
            for loader in (
                "stock_info_sz_name_code",
                "stock_info_sh_name_code",
                "stock_info_bj_name_code",
            )
        )
        if official_capable:
            try:
                for row in self._load_szse_directory().to_dict(orient="records"):
                    records.append(
                        {
                            "code": row.get("A股代码") or row.get("证券代码"),
                            "name": row.get("A股简称") or row.get("证券简称"),
                            "所属行业": row.get("所属行业") or row.get("行业"),
                        }
                    )
            except Exception as exc:
                self.last_errors.append(f"a-share-master-szse: {type(exc).__name__}")
            try:
                for row in self._load_sse_directory():
                    records.append(
                        {
                            "code": row.get("A_STOCK_CODE"),
                            "name": row.get("SEC_NAME_CN") or row.get("COMPANY_ABBR"),
                            "所属行业": row.get("CSRC_CODE_DESC"),
                        }
                    )
            except Exception as exc:
                self.last_errors.append(f"a-share-master-sse: {type(exc).__name__}")
            try:
                for row in self._load_bjse_directory(ak).to_dict(orient="records"):
                    records.append(
                        {
                            "code": row.get("证券代码") or row.get("代码"),
                            "name": row.get("证券简称") or row.get("名称"),
                            "所属行业": row.get("所属行业") or row.get("行业"),
                        }
                    )
            except Exception as exc:
                self.last_errors.append(f"a-share-master-bjse: {type(exc).__name__}")
        if len(records) >= 4_500:
            return pd.DataFrame(records)
        if records:
            self.last_errors.append(f"a-share-master-official: incomplete ({len(records)} < 4500)")

        # Eastmoney is retained as a full-directory fallback.  When the official
        # loaders exist, never accept a partial fallback because the caller treats
        # every omitted security as inactive.
        with _request_address_family(self.settings.akshare_ipv4_only):
            fallback = ak.stock_info_a_code_name()
        if official_capable:
            fallback_size = len(fallback.to_dict(orient="records"))
            if fallback_size < 4_500:
                raise RuntimeError(
                    f"incomplete A-share security directory ({fallback_size} < 4500)"
                )
        return fallback

    def _hk_share_industry_map(self, ak: Any) -> dict[str, str]:
        """Page the Eastmoney HK company profile directory in bounded batches."""

        if not hasattr(ak, "stock_hk_company_profile_em"):
            return {}
        url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
        base_params = {
            "reportName": "RPT_HKF10_INFO_ORGPROFILE",
            "columns": "SECURITY_CODE,BELONG_INDUSTRY",
            "quoteColumns": "",
            "pageSize": "500",
            "sortTypes": "",
            "sortColumns": "",
            "source": "F10",
            "client": "PC",
        }
        output: dict[str, str] = {}
        page_count = 1
        for page in range(1, 101):
            payload = None
            for _attempt in range(3):
                try:
                    with _request_address_family(self.settings.akshare_ipv4_only):
                        response = requests.get(
                            url,
                            params={**base_params, "pageNumber": str(page)},
                            timeout=20,
                        )
                    response.raise_for_status()
                    payload = response.json().get("result")
                    if payload:
                        break
                except (requests.RequestException, ValueError):
                    continue
            if not payload:
                raise RuntimeError(f"Hong Kong industry page {page} unavailable")
            page_count = max(1, int(payload.get("pages") or 1))
            for row in payload.get("data") or []:
                code = str(row.get("SECURITY_CODE") or "").strip()
                industry = str(row.get("BELONG_INDUSTRY") or "").strip()
                if code.isdigit() and industry:
                    output[code.zfill(5)] = industry
            if page >= page_count:
                break
        return output

    @staticmethod
    def _hk_security_frame(ak: Any):
        """Use Sina's complete HK directory when Eastmoney closes the connection."""

        try:
            return ak.stock_hk_spot_em()
        except Exception:
            return ak.stock_hk_spot()

    @staticmethod
    def _a_share_records(
        frame: Any, industry_map: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
            raw_code = str(row.get("code") or row.get("代码") or "").strip()
            name = str(row.get("name") or row.get("名称") or "").strip()
            if not raw_code or not name or not raw_code.isdigit():
                continue
            code = raw_code.zfill(6)
            exchange = "XSHG" if code.startswith("6") else "XBEI" if code[0] in "489" else "XSHE"
            raw_industry = str(
                (industry_map or {}).get(code) or row.get("所属行业") or row.get("行业") or ""
            ).strip()
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
    def _hk_share_records(
        frame: Any, industry_map: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
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
            raw_industry = str(
                (industry_map or {}).get(code) or row.get("所属行业") or row.get("行业") or ""
            ).strip()
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
                    frame = ak.stock_zh_a_hist(symbol=asset.symbol, period="daily", adjust="qfq")
                else:
                    frame = ak.stock_hk_hist(symbol=asset.symbol, period="daily", adjust="qfq")
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
            str(row.get("item")): row.get("value") for row in company_info_rows if row.get("item")
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
