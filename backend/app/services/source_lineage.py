from __future__ import annotations

import re
from collections import Counter
from hashlib import sha256
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from backend.app.domain import Evidence, NewsItem

LINEAGE_KEY = "source_lineage"
TRACKING_PARAMETERS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "referrer",
    "source",
}
WIRE_ALIASES = {
    "associated press": "associated-press",
    "bloomberg": "bloomberg",
    "reuters": "reuters",
    "the associated press": "associated-press",
    "新华社": "xinhua",
    "彭博": "bloomberg",
    "路透": "reuters",
}


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", value.casefold())


def canonicalize_url(value: str) -> str:
    try:
        parsed = urlsplit(value.strip())
    except ValueError:
        return value.strip()
    host = (parsed.hostname or "").casefold()
    if host.startswith("www."):
        host = host[4:]
    try:
        port = parsed.port
    except ValueError:
        return value.strip()
    netloc = host if not port else f"{host}:{port}"
    query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.casefold().startswith("utm_")
        and key.casefold() not in TRACKING_PARAMETERS
    ]
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit(((parsed.scheme or "https").casefold(), netloc, path, urlencode(query), ""))


def _domain(value: str) -> str:
    candidate = value if "://" in value else f"https://{value}"
    try:
        host = urlsplit(candidate).hostname or ""
    except ValueError:
        return ""
    host = host.casefold()
    return host[4:] if host.startswith("www.") else host


def _wire_source(item: NewsItem) -> str | None:
    text = f"{item.source} {item.title} {item.summary}".casefold()
    for marker, canonical in WIRE_ALIASES.items():
        if marker.casefold() in text:
            return canonical
    return None


def enrich_news_lineage(item: NewsItem) -> NewsItem:
    metadata = dict(item.raw_metadata)
    existing = metadata.get(LINEAGE_KEY)
    lineage = dict(existing) if isinstance(existing, dict) else {}
    canonical_url = str(lineage.get("canonical_url") or canonicalize_url(item.url))
    publisher = str(
        lineage.get("publisher_domain")
        or _domain(str(metadata.get("site") or ""))
        or _domain(canonical_url)
        or normalize_text(item.source)
        or "unknown"
    )
    explicit_origin = (
        metadata.get("original_source")
        or metadata.get("originalSource")
        or metadata.get("wire_service")
    )
    original_source = str(
        lineage.get("original_source")
        or explicit_origin
        or _wire_source(item)
        or publisher
    ).casefold()
    content_fingerprint = str(
        lineage.get("content_fingerprint")
        or sha256(
            f"{normalize_text(item.title)}|{normalize_text(item.summary)}".encode()
        ).hexdigest()
    )
    syndication_group = str(
        lineage.get("syndication_group")
        or metadata.get("syndication_group")
        or f"origin:{normalize_text(original_source) or publisher}"
    ).casefold()
    metadata[LINEAGE_KEY] = {
        "canonical_url": canonical_url,
        "publisher_domain": publisher,
        "original_source": original_source,
        "syndication_group": syndication_group,
        "content_fingerprint": content_fingerprint,
    }
    return item.model_copy(update={"raw_metadata": metadata})


def source_group(item: NewsItem) -> str:
    enriched = enrich_news_lineage(item)
    lineage = enriched.raw_metadata[LINEAGE_KEY]
    return str(lineage["syndication_group"])


def independent_evidence_groups(evidence: list[Evidence]) -> set[str]:
    """Count source lineages while collapsing byte-equivalent reprints."""

    fingerprints = [
        sha256(
            f"{normalize_text(item.claim)}|{normalize_text(item.excerpt)}".encode()
        ).hexdigest()
        for item in evidence
    ]
    repeated = {value for value, count in Counter(fingerprints).items() if count > 1}
    return {
        f"content:{fingerprint}" if fingerprint in repeated else item.independent_group
        for item, fingerprint in zip(evidence, fingerprints, strict=True)
        if item.independent_group
    }
