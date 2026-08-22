from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "Market Loop Agent"
    environment: str = "development"
    database_url: str = "sqlite:///./data/agent.db"
    redis_url: str = "redis://localhost:6379/0"

    ollama_base_url: str = "http://localhost:11434"
    ollama_extract_model: str = "qwen2.5:3b"
    ollama_research_model: str = "qwen2.5:7b"
    ollama_code_model: str = "qwen2.5-coder:7b"
    ollama_timeout_seconds: int = 240
    embedding_model: str = "intfloat/multilingual-e5-small"
    embedding_dimensions: int = Field(default=384, ge=64, le=4096)

    fmp_access_token: str = ""
    fmp_base_url: str = "https://financialmodelingprep.com/stable"
    fmp_mcp_url: str = ""
    fmp_rate_limit_per_minute: int = Field(default=240, ge=1, le=300)
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    defillama_base_url: str = "https://api.llama.fi"
    sec_identity: str = ""
    rss_feed_urls: str = ""
    official_rss_feed_urls: str = ""

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    cloud_llm_base_url: str = ""
    cloud_llm_api_key: str = ""
    cloud_llm_model: str = ""

    scan_interval_minutes: int = Field(default=20, ge=5, le=1440)
    auto_research: bool = True
    auto_paper_trade: bool = False
    evolution_enabled: bool = False
    evolution_auto_merge: bool = False
    max_verification_rounds: int = Field(default=2, ge=1, le=3)
    minimum_directional_confidence: float = Field(default=0.55, ge=0, le=1)

    base_currency: str = "USD"
    initial_cash: float = 100_000.0
    minimum_cash_weight: float = 0.10
    max_equity_weight: float = 0.08
    max_crypto_weight: float = 0.05
    max_total_crypto_weight: float = 0.15
    equity_cost_bps: int = 15
    crypto_cost_bps: int = 25

    reports_dir: Path = Path("reports")

    @property
    def cloud_verifier_enabled(self) -> bool:
        return bool(self.cloud_llm_base_url and self.cloud_llm_api_key and self.cloud_llm_model)

    @property
    def fmp_enabled(self) -> bool:
        return bool(self.fmp_access_token)

    @property
    def rss_feeds(self) -> list[str]:
        return [item.strip() for item in self.rss_feed_urls.split(",") if item.strip()]

    @property
    def official_rss_feeds(self) -> list[str]:
        return [item.strip() for item in self.official_rss_feed_urls.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
