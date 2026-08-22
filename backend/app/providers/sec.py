from __future__ import annotations

from typing import Any

from backend.app.config import Settings, get_settings
from backend.app.domain import AssetRef, Market


class SecProvider:
    """Direct SEC EDGAR adapter used to corroborate aggregator filings."""

    name = "sec-edgar"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def get_filings(self, asset: AssetRef, limit: int = 20) -> list[dict[str, Any]]:
        if asset.market is not Market.US or not self.settings.sec_identity:
            return []
        try:
            from edgar import Company, set_identity

            set_identity(self.settings.sec_identity)
            filings = Company(asset.symbol).get_filings(form=["10-K", "10-Q", "8-K"])
            filings = filings.latest(limit)
        except Exception:
            return []

        output: list[dict[str, Any]] = []
        try:
            iterator = iter(filings)
        except TypeError:
            return output
        for filing in iterator:
            filing_date = getattr(filing, "filing_date", None)
            accession = getattr(filing, "accession_no", None)
            url = getattr(filing, "homepage_url", None) or getattr(filing, "document_url", None)
            output.append(
                {
                    "formType": str(getattr(filing, "form", "")),
                    "fillingDate": str(filing_date or ""),
                    "acceptedDate": str(getattr(filing, "acceptance_datetime", filing_date) or ""),
                    "accessionNumber": str(accession or ""),
                    "finalLink": str(url or ""),
                    "source": "SEC EDGAR",
                }
            )
        return output
