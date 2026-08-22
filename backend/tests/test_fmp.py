import httpx
import pytest
import respx

from backend.app.config import Settings
from backend.app.providers.base import ProviderError
from backend.app.providers.fmp import FmpMcpClient, FmpProvider


def test_mcp_empty_notification_response_is_valid():
    response = httpx.Response(
        202, content=b"", request=httpx.Request("POST", "https://example.invalid/mcp")
    )
    assert FmpMcpClient._decode(response) == {}


def test_mcp_rejects_non_allowlisted_tool():
    with pytest.raises(ProviderError, match="not allowlisted"):
        FmpMcpClient("https://example.invalid/mcp").call("arbitraryTool", {})


def test_rest_key_is_sent_in_header_not_url():
    credential = "-".join(("unit", "test", "credential"))
    settings = Settings(
        fmp_access_token=credential,
        fmp_mcp_url="",
        fmp_base_url="https://example.invalid/stable",
    )
    provider = FmpProvider(settings)
    with respx.mock:
        route = respx.get("https://example.invalid/stable/profile").mock(
            return_value=httpx.Response(200, json=[])
        )
        provider._rest("profile", {"symbol": "AAPL"})

    request = route.calls.last.request
    assert request.headers["apikey"] == credential
    assert credential not in str(request.url)
