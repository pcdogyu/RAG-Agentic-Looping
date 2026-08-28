from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from backend.app.config import Settings
from backend.app.db import McpSourceRow, SessionLocal
from backend.app.domain import AssetClass, AssetRef, Market, NewsItem, SourceQuality
from backend.app.providers.akshare_provider import AkShareProvider
from backend.app.providers.crypto import CryptoProvider
from backend.app.providers.fmp import FmpProvider
from backend.app.providers.rss import RssProvider
from backend.app.providers.sec import SecProvider
from backend.app.services.fact_sources import get_effective_settings
from backend.app.services.mcp_registry import (
    call_enabled_purpose_sync,
    fetch_enabled_news_feeds_sync,
)

SEED_ASSETS = [
    AssetRef(
        asset_id="equity:XNAS:AAPL",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="AAPL",
        name="Apple Inc.",
        exchange_or_provider="XNAS",
        aliases=["Apple", "苹果公司"],
        products=["iPhone", "Mac", "Services"],
        competitors=["MSFT", "GOOGL", "SAMSUNG"],
    ),
    AssetRef(
        asset_id="equity:XSHG:600519",
        asset_class=AssetClass.EQUITY,
        market=Market.CN,
        symbol="600519",
        name="贵州茅台",
        exchange_or_provider="XSHG",
        currency="CNY",
        aliases=["茅台", "Kweichow Moutai"],
        lot_size=100,
    ),
    AssetRef(
        asset_id="equity:XHKG:00700",
        asset_class=AssetClass.EQUITY,
        market=Market.HK,
        symbol="00700",
        name="腾讯控股",
        exchange_or_provider="XHKG",
        currency="HKD",
        aliases=["腾讯", "Tencent"],
        products=["微信", "游戏", "云服务"],
        competitors=["9988", "NTES"],
        lot_size=100,
    ),
    AssetRef(
        asset_id="crypto:coingecko:bitcoin",
        asset_class=AssetClass.CRYPTO,
        market=Market.CRYPTO,
        symbol="BTC",
        name="Bitcoin",
        exchange_or_provider="coingecko",
        aliases=["bitcoin", "比特币"],
    ),
    AssetRef(
        asset_id="crypto:coingecko:ethereum",
        asset_class=AssetClass.CRYPTO,
        market=Market.CRYPTO,
        symbol="ETH",
        name="Ethereum",
        exchange_or_provider="coingecko",
        aliases=["ethereum", "以太坊"],
    ),
    AssetRef(
        asset_id="equity:XHKG:09988",
        asset_class=AssetClass.EQUITY,
        market=Market.HK,
        symbol="09988",
        name="Alibaba Group Holding Limited",
        exchange_or_provider="XHKG",
        currency="HKD",
        aliases=["阿里巴巴", "阿里巴巴集团", "Alibaba", "Alibaba Group"],
        products=["阿里云", "Alibaba Cloud"],
        issuer_id="curated:alibaba-group",
        lot_size=100,
    ),
    AssetRef(
        asset_id="equity:NYSE:BABA",
        asset_class=AssetClass.EQUITY,
        market=Market.US,
        symbol="BABA",
        name="Alibaba Group Holding Limited",
        exchange_or_provider="NYSE",
        aliases=["阿里巴巴", "阿里巴巴集团", "Alibaba", "Alibaba Group"],
        products=["阿里云", "Alibaba Cloud"],
        issuer_id="curated:alibaba-group",
    ),
    AssetRef(
        asset_id="commodity:fmp:CLUSD",
        asset_class=AssetClass.COMMODITY,
        market=Market.COMMODITY,
        symbol="CLUSD",
        name="WTI Crude Oil Continuous Benchmark",
        exchange_or_provider="fmp",
        aliases=["WTI", "WTI crude", "West Texas Intermediate", "WTI 原油"],
    ),
    AssetRef(
        asset_id="commodity:fmp:BZUSD",
        asset_class=AssetClass.COMMODITY,
        market=Market.COMMODITY,
        symbol="BZUSD",
        name="Brent Crude Oil Continuous Benchmark",
        exchange_or_provider="fmp",
        aliases=["Brent", "Brent crude", "布伦特原油"],
    ),
    AssetRef(
        asset_id="commodity:fmp:ZGUSD",
        asset_class=AssetClass.COMMODITY,
        market=Market.COMMODITY,
        symbol="ZGUSD",
        name="Gold Continuous Benchmark",
        exchange_or_provider="fmp",
        aliases=["Gold", "黄金", "现货黄金"],
    ),
    AssetRef(
        asset_id="fx:fmp:EURUSD",
        asset_class=AssetClass.FX,
        market=Market.FX,
        symbol="EURUSD",
        name="EUR/USD Spot FX",
        exchange_or_provider="fmp",
        aliases=["EUR/USD", "欧元兑美元"],
    ),
    AssetRef(
        asset_id="fx:fmp:USDJPY",
        asset_class=AssetClass.FX,
        market=Market.FX,
        symbol="USDJPY",
        name="USD/JPY Spot FX",
        exchange_or_provider="fmp",
        aliases=["USD/JPY", "美元兑日元"],
    ),
    AssetRef(
        asset_id="fx:fmp:USDCNH",
        asset_class=AssetClass.FX,
        market=Market.FX,
        symbol="USDCNH",
        name="USD/CNH Spot FX",
        exchange_or_provider="fmp",
        aliases=["USD/CNH", "美元兑离岸人民币"],
    ),
]


_AMBIGUOUS_ISSUER_NAMES = {
    # Listed-company short names that are also common industry nouns. They are
    # unsafe without an explicit ticker because an ordinary topic mention is
    # not evidence that the issuer itself is involved.
    "机器人",
}
_AMBIGUOUS_PRODUCT_NAMES = {
    # These values exist on legacy seed rows but are product categories rather
    # than unique branded products. They must never imply issuer ownership.
    "game",
    "games",
    "mac",
    "services",
    "云服务",
    "游戏",
}
_LATIN_CORPORATE_SUFFIXES = {
    "adr",
    "ads",
    "co",
    "company",
    "corp",
    "corporation",
    "group",
    "holding",
    "holdings",
    "inc",
    "incorporated",
    "limited",
    "ltd",
    "plc",
}
_LATIN_LISTING_DESCRIPTORS = {
    "american",
    "depositary",
    "depository",
    "receipt",
    "receipts",
    "share",
    "shares",
    "sponsored",
    "unsponsored",
}
_CJK_CORPORATE_SUFFIXES = (
    "股份有限公司",
    "有限责任公司",
    "集团股份",
    "集团有限公司",
    "有限公司",
    "股份",
    "集团",
)
_PRICE_DATE_KEYS = ("date", "datetime", "timestamp", "time", "日期", "交易日期", "数据日期")
_PRICE_CLOSE_KEYS = (
    "adjClose",
    "adjustedClose",
    "close",
    "price",
    "收盘",
    "收盘价",
    "当日收盘价",
)
_PRICE_VOLUME_KEYS = ("volume", "成交量", "VOL")
_SHORT_TICKER_PATTERN = re.compile(r"[a-z]{1,3}")
_EXCHANGE_PREFIX_PATTERN = (
    r"(?:amex|asx|bse|hkex|lse|nasdaq|nyse|otc|otcqb|otcqx|sh|sse|sz|szse|xshg|xshe)"
)


def normalize_security_text(value: str) -> str:
    """Normalize presentation differences without destroying token boundaries."""

    return unicodedata.normalize("NFKC", value).casefold().strip()


def compact_security_text(value: str) -> str:
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", normalize_security_text(value))


def security_token_present(text: str, token: str) -> bool:
    """Return whether an alphanumeric ticker/code occurs as a complete token."""

    normalized_text = normalize_security_text(text)
    normalized_token = normalize_security_text(token)
    if not normalized_token:
        return False
    return bool(
        re.search(
            rf"(?<![a-z0-9]){re.escape(normalized_token)}(?![a-z0-9])",
            normalized_text,
        )
    )


def _is_short_ticker(symbol: str) -> bool:
    return bool(_SHORT_TICKER_PATTERN.fullmatch(normalize_security_text(symbol)))


def explicit_symbol_present(text: str, symbol: str, *, allow_bare: bool = False) -> bool:
    """Require security-specific syntax for ambiguous one-to-three letter tickers."""

    normalized_text = normalize_security_text(text)
    normalized_symbol = normalize_security_text(symbol)
    if not normalized_symbol:
        return False
    if normalized_symbol.isdigit() and len(normalized_symbol) == 5:
        # Hong Kong listings are stored as five digits, while news and users
        # commonly omit the leading zero (09988 -> 9988, 00700 -> 700).
        variants = {normalized_symbol, normalized_symbol.lstrip("0") or "0"}
        if any(security_token_present(normalized_text, value) for value in variants):
            return True
    if not _is_short_ticker(normalized_symbol):
        return security_token_present(normalized_text, normalized_symbol)
    if allow_bare and normalized_text == normalized_symbol:
        return True

    symbol_pattern = re.escape(normalized_symbol)
    patterns = (
        rf"(?<![a-z0-9])\$\s*{symbol_pattern}(?![a-z0-9])",
        rf"[\(\[]\s*{symbol_pattern}\s*[\)\]]",
        rf"(?<![a-z0-9]){_EXCHANGE_PREFIX_PATTERN}\s*:\s*{symbol_pattern}(?![a-z0-9])",
        rf"(?<![a-z0-9]){symbol_pattern}\.(?:ax|l|n|o|oq|pk|us)(?![a-z0-9])",
    )
    return any(re.search(pattern, normalized_text) for pattern in patterns)


def _latin_issuer_words(value: str) -> list[str]:
    words = re.findall(r"[a-z0-9]+", normalize_security_text(value))
    while words and words[-1] in (_LATIN_CORPORATE_SUFFIXES | _LATIN_LISTING_DESCRIPTORS):
        words.pop()
    return words


def canonical_issuer_name(value: str) -> str:
    """Build a stable issuer identity across legal suffixes and ADR labels."""

    normalized = normalize_security_text(value)
    if not normalized:
        return ""
    if re.search(r"[\u3400-\u9fff]", normalized):
        compact = compact_security_text(normalized)
        changed = True
        while compact and changed:
            changed = False
            for suffix in _CJK_CORPORATE_SUFFIXES:
                if compact.endswith(suffix) and len(compact) > len(suffix):
                    compact = compact[: -len(suffix)]
                    changed = True
                    break
        return compact

    return "".join(_latin_issuer_words(normalized))


def _issuer_terms(asset: AssetRef) -> set[str]:
    terms = {asset.name, *asset.aliases}
    canonical = canonical_issuer_name(asset.name)
    if canonical:
        terms.add(canonical)
    latin_core = " ".join(_latin_issuer_words(asset.name))
    if latin_core:
        terms.add(latin_core)
    normalized_symbol = normalize_security_text(asset.symbol)
    return {
        value
        for value in terms
        if compact_security_text(value)
        and not (
            _is_short_ticker(normalized_symbol)
            and normalize_security_text(value) == normalized_symbol
        )
    }


def text_contains_term(text: str, term: str) -> bool:
    normalized_term = normalize_security_text(term)
    compact_term = compact_security_text(term)
    if not compact_term:
        return False
    if re.search(r"[\u3400-\u9fff]", normalized_term):
        return compact_term in compact_security_text(text)

    words = re.findall(r"[a-z0-9]+", normalized_term)
    if not words:
        return False
    phrase = r"[^a-z0-9]+".join(re.escape(word) for word in words)
    return bool(
        re.search(
            rf"(?<![a-z0-9]){phrase}(?![a-z0-9])",
            normalize_security_text(text),
        )
    )


def _name_present(text: str, name: str) -> bool:
    compact_name = compact_security_text(name)
    return compact_name not in _AMBIGUOUS_ISSUER_NAMES and text_contains_term(text, name)


def query_mentions_issuer(query: str, asset: AssetRef) -> bool:
    return any(_name_present(query, term) for term in _issuer_terms(asset))


def listing_symbols_equal(left: str, right: str) -> bool:
    normalized_left = normalize_security_text(left)
    normalized_right = normalize_security_text(right)
    if normalized_left == normalized_right:
        return True
    if not (normalized_left.isdigit() and normalized_right.isdigit()):
        return False
    lengths = {len(normalized_left), len(normalized_right)}
    return max(lengths) == 5 and normalized_left.zfill(5) == normalized_right.zfill(5)


def query_mentions_product(query: str, asset: AssetRef) -> str | None:
    """Return the exact master-data product mentioned by a query, if any."""

    for product in asset.products:
        normalized_product = compact_security_text(product)
        if (
            normalized_product
            and normalized_product not in _AMBIGUOUS_PRODUCT_NAMES
            and text_contains_term(query, product)
        ):
            return product
    return None


def query_mentions_asset(query: str, asset: AssetRef) -> bool:
    """Require an explicit listing code or issuer identity in the query.

    Product names are intentionally excluded: a product/industry mention is not
    sufficient to establish that a listed issuer is the subject of the event.
    """

    if explicit_symbol_present(query, asset.symbol, allow_bare=True):
        return True
    return query_mentions_issuer(query, asset)


def issuer_key(asset: AssetRef) -> str:
    """Return a derived issuer key while keeping persisted AssetRef rows compatible."""

    if asset.issuer_id:
        return f"issuer-id:{normalize_security_text(asset.issuer_id)}"
    if asset.primary_listing_asset_id:
        return f"primary-listing:{asset.primary_listing_asset_id.casefold()}"
    canonical = canonical_issuer_name(asset.name)
    if (
        not canonical
        or canonical in _AMBIGUOUS_ISSUER_NAMES
        or canonical == compact_security_text(asset.symbol)
    ):
        return f"listing:{asset.asset_id.casefold()}"
    return f"issuer:{canonical}"


def _record_value(record: dict[str, Any], keys: tuple[str, ...]) -> Any:
    normalized = {str(key).casefold(): value for key, value in record.items()}
    for key in keys:
        if key.casefold() in normalized:
            return normalized[key.casefold()]
    return None


def _price_date(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, int | float) and not isinstance(value, bool):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000
        try:
            return datetime.fromtimestamp(timestamp, tz=UTC).date().isoformat()
        except (OSError, OverflowError, ValueError):
            return None
    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())[:10]
        except (TypeError, ValueError):
            pass
    match = re.search(r"(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})", str(value or ""))
    if not match:
        return None
    try:
        return datetime(*(int(part) for part in match.groups()), tzinfo=UTC).date().isoformat()
    except ValueError:
        return None


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def normalize_price_records(payload: Any, limit: int = 250) -> list[dict[str, Any]]:
    """Normalize provider-specific historical rows for deterministic factors."""

    if isinstance(payload, dict):
        payload = (
            payload.get("historical")
            or payload.get("data")
            or payload.get("results")
            or payload.get("prices")
            or []
        )
    if not isinstance(payload, list):
        return []
    by_date: dict[str, dict[str, Any]] = {}
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        date_value = _price_date(_record_value(raw, _PRICE_DATE_KEYS))
        close = _finite_number(_record_value(raw, _PRICE_CLOSE_KEYS))
        if not date_value or close is None or close <= 0:
            continue
        record: dict[str, Any] = {"date": date_value, "close": close}
        volume = _finite_number(_record_value(raw, _PRICE_VOLUME_KEYS))
        if volume is not None and volume >= 0:
            record["volume"] = volume
        by_date[date_value] = record
    return [by_date[key] for key in sorted(by_date)[-max(1, limit) :]]


class ProviderRegistry:
    def __init__(
        self,
        settings: Settings | None = None,
        assets: Iterable[AssetRef] | None = None,
    ) -> None:
        self.settings = settings or get_effective_settings()
        self.fmp = FmpProvider(self.settings)
        self.crypto = CryptoProvider(self.settings)
        self.rss = RssProvider(self.settings)
        self.akshare = AkShareProvider(self.settings)
        self.sec = SecProvider(self.settings)
        self.providers = [self.fmp, self.rss, self.akshare]
        self._assets = {asset.asset_id: asset for asset in SEED_ASSETS}
        self.add_assets(assets or [])
        self.last_errors: list[str] = []
        self.mapping_errors: list[str] = []

    def _source_enabled(self, name: str, default: bool = True) -> bool:
        try:
            with SessionLocal() as db:
                value = db.scalar(select(McpSourceRow.enabled).where(McpSourceRow.name == name))
            return default if value is None else bool(value)
        except Exception:
            return default

    def add_assets(self, assets: Iterable[AssetRef]) -> None:
        for asset in assets:
            existing = self._assets.get(asset.asset_id)
            if existing is None:
                self._assets[asset.asset_id] = asset
                continue
            # Curated seed metadata supplies stable aliases, product ownership,
            # and issuer linkage to older/provider-created rows. Preserve the
            # stored listing fields while filling and merging those identities.
            self._assets[asset.asset_id] = asset.model_copy(
                update={
                    "aliases": list(dict.fromkeys([*existing.aliases, *asset.aliases])),
                    "products": list(dict.fromkeys([*existing.products, *asset.products])),
                    "competitors": list(
                        dict.fromkeys([*existing.competitors, *asset.competitors])
                    ),
                    "issuer_id": existing.issuer_id or asset.issuer_id,
                    "primary_listing_asset_id": (
                        existing.primary_listing_asset_id
                        or asset.primary_listing_asset_id
                    ),
                }
            )

    def refresh_crypto_universe(self) -> list[AssetRef]:
        assets = self.crypto.top_assets(20)
        self._assets.update({asset.asset_id: asset for asset in assets})
        return assets

    def refresh_macro_universe(self) -> list[AssetRef]:
        assets = self.fmp.list_macro_assets()
        self._assets.update({asset.asset_id: asset for asset in assets})
        return assets

    def all_assets(self) -> list[AssetRef]:
        return list(self._assets.values())

    def get_asset(self, asset_id: str) -> AssetRef | None:
        return self._assets.get(asset_id)

    def resolve_product_owners(self, query: str) -> list[tuple[AssetRef, str]]:
        """Resolve only explicit, master-verified branded product ownership.

        A matched product expands to sibling listings with the same issuer so
        the model never needs to guess ADR/HK codes independently.
        """

        direct_matches = [
            (asset, product)
            for asset in self._assets.values()
            if (product := query_mentions_product(query, asset)) is not None
        ]
        resolved: dict[str, tuple[AssetRef, str]] = {}
        for owner, product in direct_matches:
            for asset in self._assets.values():
                explicitly_linked = (
                    owner.asset_id == asset.asset_id
                    or (
                        bool(owner.issuer_id)
                        and bool(asset.issuer_id)
                        and normalize_security_text(owner.issuer_id or "")
                        == normalize_security_text(asset.issuer_id or "")
                    )
                    or owner.primary_listing_asset_id == asset.asset_id
                    or asset.primary_listing_asset_id == owner.asset_id
                    or (
                        bool(owner.primary_listing_asset_id)
                        and owner.primary_listing_asset_id
                        == asset.primary_listing_asset_id
                    )
                )
                if explicitly_linked:
                    resolved[asset.asset_id] = (asset, product)
        return list(resolved.values())

    @staticmethod
    def query_mentions_asset(query: str, asset: AssetRef) -> bool:
        return query_mentions_asset(query, asset)

    @staticmethod
    def issuer_key(asset: AssetRef) -> str:
        return issuer_key(asset)

    @staticmethod
    def same_issuer(left: AssetRef, right: AssetRef) -> bool:
        if left.asset_id == right.asset_id:
            return True
        if left.issuer_id and right.issuer_id:
            return normalize_security_text(left.issuer_id) == normalize_security_text(
                right.issuer_id
            )
        if left.primary_listing_asset_id == right.asset_id:
            return True
        if right.primary_listing_asset_id == left.asset_id:
            return True
        if left.primary_listing_asset_id and right.primary_listing_asset_id:
            return left.primary_listing_asset_id == right.primary_listing_asset_id
        # Existing persisted assets predate issuer metadata; normalized legal
        # names keep those rows comparable without a migration.
        left_name = canonical_issuer_name(left.name)
        right_name = canonical_issuer_name(right.name)
        return bool(
            left_name
            and left_name == right_name
            and left_name not in _AMBIGUOUS_ISSUER_NAMES
            and left_name != compact_security_text(left.symbol)
            and right_name != compact_security_text(right.symbol)
        )

    def discover_news(self, *, since: datetime, limit: int = 200) -> list[NewsItem]:
        unique: dict[str, NewsItem] = {}
        seen_urls: set[str] = set()
        self.last_errors = []
        for provider in self.providers:
            if provider is self.fmp and not self._source_enabled("FMP", self.settings.fmp_enabled):
                continue
            try:
                for item in provider.discover_news(since=since, limit=limit):
                    if item.url in seen_urls:
                        continue
                    unique[item.content_hash] = item
                    seen_urls.add(item.url)
            except Exception as exc:
                self.last_errors.append(f"{provider.name}: {type(exc).__name__}")
                continue
        try:
            items, errors = fetch_enabled_news_feeds_sync(since, limit)
            for item in items:
                if item.url in seen_urls:
                    continue
                unique[item.content_hash] = item
                seen_urls.add(item.url)
            self.last_errors.extend(
                f"{item['source']}: MCP news feed ({item['error']})" for item in errors
            )
        except Exception as exc:
            self.last_errors.append(f"mcp-news: {type(exc).__name__}")
        return sorted(unique.values(), key=lambda item: item.published_at, reverse=True)[:limit]

    def resolve_assets(self, query: str) -> list[AssetRef]:
        exact: list[AssetRef] = []
        for asset in self._assets.values():
            if query_mentions_asset(query, asset):
                exact.append(asset)
        if exact:
            return exact
        discovered: dict[str, AssetRef] = {}
        providers = [self.fmp, self.crypto]
        if not self._source_enabled("FMP", self.settings.fmp_enabled):
            providers.remove(self.fmp)
        if self.settings.akshare_asset_master_enabled:
            providers.insert(0, self.akshare)
        for provider in providers:
            try:
                for asset in provider.resolve_assets(query):
                    discovered[asset.asset_id] = asset
                for detail in getattr(provider, "last_errors", []):
                    error = f"{provider.name}: {detail}"
                    if error not in self.mapping_errors:
                        self.mapping_errors.append(error)
            except Exception as exc:
                error = f"{provider.name}: {type(exc).__name__}"
                if error not in self.mapping_errors:
                    self.mapping_errors.append(error)
                continue
        # Cache the complete provider response so sibling listings can share a
        # derived issuer identity, but expose only assets explicitly mentioned
        # by this query to the event mapper.
        self._assets.update(discovered)
        return [
            asset for asset in discovered.values() if query_mentions_asset(query, asset)
        ]

    def provider_for(self, asset: AssetRef):
        if asset.asset_class is AssetClass.CRYPTO:
            return self.crypto
        if asset.market in {Market.CN, Market.HK}:
            return self.akshare
        return self.fmp

    def _issuer_research_asset(self, asset: AssetRef) -> AssetRef:
        """Use the primary listing for issuer facts while retaining listing prices."""

        primary_id = asset.primary_listing_asset_id
        if not primary_id:
            return asset
        known = self.get_asset(primary_id)
        if known is not None:
            return known
        parts = primary_id.split(":", 2)
        if len(parts) != 3 or parts[0].casefold() != "equity":
            return asset
        exchange, symbol = parts[1], parts[2]
        currency = {
            "ASX": "AUD",
            "XHKG": "HKD",
            "HKSE": "HKD",
            "XSHG": "CNY",
            "XSHE": "CNY",
        }.get(exchange.upper(), asset.currency)
        primary = AssetRef(
            asset_id=primary_id,
            asset_class=asset.asset_class,
            market=self.fmp._legacy_market_for_exchange(exchange),
            symbol=symbol,
            name=asset.name,
            exchange_or_provider=exchange,
            currency=currency,
            aliases=asset.aliases,
            issuer_id=asset.issuer_id,
        )
        self._assets[primary.asset_id] = primary
        return primary

    @staticmethod
    def _sec_eligible(asset: AssetRef) -> bool:
        return asset.exchange_or_provider.casefold() in {
            "amex",
            "nasdaq",
            "nasdaqgs",
            "nasdaqgm",
            "nasdaqcm",
            "nyse",
            "nysearca",
            "nysemkt",
            "xnas",
            "xnys",
        }

    @staticmethod
    def _broad_benchmark(asset: AssetRef) -> AssetRef | None:
        if asset.asset_class in {AssetClass.CRYPTO, AssetClass.COMMODITY, AssetClass.FX}:
            # BTC is not a broad-market index; leave the benchmark unavailable
            # until a true point-in-time crypto market index source is wired.
            return None
        if asset.exchange_or_provider.casefold() == "asx" or (
            asset.primary_listing_asset_id
            and ":asx:" in asset.primary_listing_asset_id.casefold()
        ):
            return AssetRef(
                asset_id="equity:ASX:STW.AX",
                asset_class=AssetClass.EQUITY,
                # The persisted enum predates AU support; exchange/provider is
                # authoritative for routing this FMP instrument.
                market=Market.US,
                symbol="STW.AX",
                name="SPDR S&P/ASX 200 Fund",
                exchange_or_provider="ASX",
                currency="AUD",
            )
        if asset.market is Market.CN:
            return AssetRef(
                asset_id="equity:XSHG:510300",
                asset_class=AssetClass.EQUITY,
                market=Market.CN,
                symbol="510300",
                name="沪深300ETF",
                exchange_or_provider="XSHG",
                currency="CNY",
                lot_size=100,
            )
        if asset.market is Market.HK:
            return AssetRef(
                asset_id="equity:XHKG:02800",
                asset_class=AssetClass.EQUITY,
                market=Market.HK,
                symbol="02800",
                name="盈富基金",
                exchange_or_provider="XHKG",
                currency="HKD",
                lot_size=100,
            )
        if asset.market is Market.US:
            return AssetRef(
                asset_id="equity:ARCX:SPY",
                asset_class=AssetClass.EQUITY,
                market=Market.US,
                symbol="SPY",
                name="SPDR S&P 500 ETF Trust",
                exchange_or_provider="ARCX",
            )
        return None

    @staticmethod
    def _price_source(provider: Any, asset: AssetRef) -> dict[str, Any]:
        provider_name = str(getattr(provider, "name", "market-data"))
        normalized_provider = provider_name.casefold()
        if normalized_provider == "fmp":
            url = "https://financialmodelingprep.com/"
        elif normalized_provider == "akshare":
            url = "https://github.com/akfamily/akshare"
        elif asset.asset_class is AssetClass.CRYPTO:
            coin_id = asset.asset_id.rsplit(":", 1)[-1]
            url = f"https://www.coingecko.com/en/coins/{coin_id}/historical_data"
        else:
            url = "https://www.nasdaq.com/market-activity"
        return {
            "name": f"{provider_name} {asset.symbol} 历史行情",
            "url": url,
            "quality": SourceQuality.AGGREGATOR.value,
            "independent_group": f"market:{provider_name}:{asset.asset_id}",
        }

    @staticmethod
    def _fundamentals_source(provider: Any, asset: AssetRef) -> dict[str, Any] | None:
        """Describe only native fundamentals whose upstream page is traceable.

        Generic MCP payloads are intentionally not attributed here: the registry does
        not receive a stable URL or quality declaration for those payloads.
        """

        provider_name = str(getattr(provider, "name", "")).casefold()
        if provider_name == "akshare" and asset.market is Market.CN:
            exchange = {
                "XSHG": "SH",
                "XSHE": "SZ",
                "XBEI": "BJ",
            }.get(
                asset.exchange_or_provider.upper(),
                "SH"
                if asset.symbol.startswith("6")
                else "BJ"
                if asset.symbol[:1] in "489"
                else "SZ",
            )
            return {
                "name": f"东方财富/AkShare {asset.symbol} 财务指标",
                "url": (
                    "https://emweb.securities.eastmoney.com/pc_hsf10/pages/index.html"
                    f"?type=web&code={exchange}{asset.symbol}#/cwfx"
                ),
                "quality": SourceQuality.AGGREGATOR.value,
                "independent_group": f"fundamentals:eastmoney-akshare:{asset.asset_id}",
            }
        if provider_name == "fmp":
            return {
                "name": f"Financial Modeling Prep {asset.symbol} 财务报表",
                "url": (
                    "https://financialmodelingprep.com/stable/income-statement"
                    f"?symbol={asset.symbol}"
                ),
                "quality": SourceQuality.AGGREGATOR.value,
                "independent_group": f"fundamentals:fmp:{asset.asset_id}",
            }
        return None

    @staticmethod
    def _has_material_fundamentals(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        return any(
            key not in {"provider", "mcp_sources"} and value not in (None, "", [], {})
            for key, value in payload.items()
        )

    def _load_prices(
        self,
        provider: Any,
        asset: AssetRef,
        *,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        if provider is self.fmp and not self._source_enabled(
            "FMP", self.settings.fmp_enabled
        ):
            return []
        try:
            rows = normalize_price_records(
                provider.get_prices(asset, start=start, end=end), limit=10_000
            )
            boundary = end.date().isoformat()
            return [row for row in rows if row["date"] <= boundary][-250:]
        except Exception:
            return []

    def get_research_data(self, asset: AssetRef) -> dict[str, Any]:
        provider = self.provider_for(asset)
        issuer_asset = self._issuer_research_asset(asset)
        issuer_provider = self.provider_for(issuer_asset)
        now = datetime.now(UTC)
        price_start = now - timedelta(days=500)
        prices = self._load_prices(provider, asset, start=price_start, end=now)
        factor_sources: dict[str, Any] = {}
        if prices:
            factor_sources["market"] = {
                **self._price_source(provider, asset),
                "published_at": f"{prices[-1]['date']}T00:00:00+00:00",
            }

        benchmark_prices: list[dict[str, Any]] = []
        benchmark = self._broad_benchmark(asset)
        if benchmark and not (
            benchmark.market is asset.market
            and benchmark.symbol.casefold() == asset.symbol.casefold()
        ):
            benchmark_provider = self.provider_for(benchmark)
            benchmark_prices = self._load_prices(
                benchmark_provider,
                benchmark,
                start=price_start,
                end=now,
            )
            if benchmark_prices:
                factor_sources["benchmark"] = {
                    **self._price_source(benchmark_provider, benchmark),
                    "published_at": f"{benchmark_prices[-1]['date']}T00:00:00+00:00",
                }
        price_data = {
            "prices": prices,
            "benchmark_prices": benchmark_prices,
            # An industry benchmark requires an explicit classification and is
            # deliberately not inferred from the company name or event text.
            "industry_prices": [],
            # Consensus estimates require a timestamped specialist feed. Do not
            # reinterpret provider fundamentals as market expectations.
            "expectations": {},
            "factor_sources": factor_sources,
        }
        if asset.asset_class is AssetClass.CRYPTO:
            metrics: dict[str, Any] = {}
            try:
                metrics = provider.get_crypto_metrics(asset)
            except Exception:
                pass
            if self._source_enabled("FMP", self.settings.fmp_enabled):
                try:
                    metrics["fmp_quote"] = self.fmp.get_crypto_metrics(asset)
                except Exception:
                    pass
            return {**price_data, "crypto_metrics": metrics}
        fundamentals: dict[str, Any] = {}
        unattributed_fundamentals = False
        filings: list[dict[str, Any]] = []
        today = datetime.now().date()
        canonical_args = {
            "asset_id": issuer_asset.asset_id,
            "symbol": issuer_asset.symbol,
            "market": issuer_asset.market.value,
            "from_date": (today - timedelta(days=730)).isoformat(),
            "to": today.isoformat(),
        }
        try:
            mcp_fundamentals, errors = call_enabled_purpose_sync("fundamentals", canonical_args)
            self.last_errors.extend(f"{item['source']}: MCP fundamentals" for item in errors)
            for source, payload in mcp_fundamentals:
                if isinstance(payload, dict):
                    candidate = payload.get("data", payload)
                    if isinstance(candidate, dict):
                        unattributed_fundamentals = (
                            unattributed_fundamentals
                            or self._has_material_fundamentals(candidate)
                        )
                        for key, value in candidate.items():
                            if (
                                key in fundamentals
                                and isinstance(fundamentals[key], list)
                                and isinstance(value, list)
                            ):
                                fundamentals[key].extend(value)
                            else:
                                fundamentals.setdefault(key, value)
                        fundamentals.setdefault("mcp_sources", []).append(source)
        except Exception:
            pass
        try:
            mcp_filings, errors = call_enabled_purpose_sync("filings", canonical_args)
            self.last_errors.extend(f"{item['source']}: MCP filings" for item in errors)
            for source, payload in mcp_filings:
                candidate = (
                    payload.get("results") or payload.get("data")
                    if isinstance(payload, dict)
                    else payload
                )
                if isinstance(candidate, dict):
                    candidate = [candidate]
                if isinstance(candidate, list):
                    filings.extend(
                        {**item, "source": item.get("source") or source}
                        for item in candidate
                        if isinstance(item, dict)
                    )
        except Exception:
            pass
        try:
            mcp_quotes, errors = call_enabled_purpose_sync("quote", canonical_args)
            self.last_errors.extend(f"{item['source']}: MCP quote" for item in errors)
            if mcp_quotes:
                quote = mcp_quotes[0][1]
                fundamentals["quote"] = quote
                unattributed_fundamentals = unattributed_fundamentals or quote not in (
                    None,
                    "",
                    [],
                    {},
                )
        except Exception:
            pass
        provider_fundamentals: dict[str, Any] = {}
        if issuer_provider is not self.fmp or self._source_enabled(
            "FMP", self.settings.fmp_enabled
        ):
            try:
                candidate = issuer_provider.get_fundamentals(issuer_asset)
                if isinstance(candidate, dict):
                    provider_fundamentals = candidate
                for key, value in provider_fundamentals.items():
                    if (
                        key in fundamentals
                        and isinstance(fundamentals[key], list)
                        and isinstance(value, list)
                    ):
                        fundamentals[key].extend(value)
                    else:
                        fundamentals.setdefault(key, value)
                filings.extend(issuer_provider.get_filings(issuer_asset))
            except Exception:
                pass
        if (
            self._has_material_fundamentals(provider_fundamentals)
            and not unattributed_fundamentals
        ):
            fundamentals_source = self._fundamentals_source(
                issuer_provider, issuer_asset
            )
            if fundamentals_source:
                factor_sources["fundamentals"] = fundamentals_source
        if self._sec_eligible(issuer_asset):
            official = self.sec.get_filings(issuer_asset)
            seen = {
                item.get("accessionNumber") or item.get("finalLink") or item.get("link")
                for item in filings
            }
            filings.extend(
                item
                for item in official
                if (item.get("accessionNumber") or item.get("finalLink")) not in seen
            )
        return {
            **price_data,
            "fundamentals": fundamentals,
            "filings": filings,
            "issuer_research_asset": issuer_asset.model_dump(mode="json"),
        }
