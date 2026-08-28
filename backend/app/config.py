import re
from functools import lru_cache
from pathlib import Path
from typing import Self

from pydantic import Field, model_validator
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
    ollama_extract_base_urls: str = ""
    ollama_assist_base_url: str = ""
    ollama_assist_base_urls: str = ""
    ollama_research_base_urls: str = ""
    ollama_code_base_urls: str = ""
    ollama_extract_model: str = "qwen2.5:3b"
    ollama_assist_model: str = "qwen2.5:7b"
    ollama_research_model: str = "qwen2.5:7b"
    ollama_code_model: str = "qwen2.5-coder:7b"
    ollama_timeout_seconds: int = 240
    ollama_research_timeout_seconds: int = Field(default=900, ge=30, le=3600)
    ollama_research_validation_retry_timeout_seconds: int = Field(default=300, ge=30, le=900)
    ollama_context_length: int = Field(default=8192, ge=512, le=262144)
    ollama_assist_context_length: int = Field(default=16384, ge=512, le=262144)
    ollama_research_context_length: int = Field(default=16384, ge=512, le=262144)
    ollama_asset_mapping_context_length: int = Field(default=8192, ge=512, le=262144)
    ollama_num_parallel: int = Field(default=2, ge=1, le=16)
    ollama_max_loaded_models: int = Field(default=2, ge=1, le=16)
    ollama_max_queue: int = Field(default=256, ge=1, le=65536)
    ollama_load_timeout: str = "10m"
    ollama_num_threads: int = Field(default=0, ge=0, le=256)
    ollama_extract_num_threads: int = Field(default=0, ge=0, le=256)
    ollama_assist_num_threads: int = Field(default=0, ge=0, le=256)
    ollama_research_num_threads: int = Field(default=0, ge=0, le=256)
    ollama_code_num_threads: int = Field(default=0, ge=0, le=256)
    ollama_extract_max_concurrency: int = Field(default=1, ge=1, le=16)
    ollama_assist_max_concurrency: int = Field(default=1, ge=1, le=16)
    ollama_research_max_concurrency: int = Field(default=2, ge=1, le=16)
    research_pipeline_concurrency: int = Field(default=4, ge=1, le=32)
    ollama_code_max_concurrency: int = Field(default=1, ge=1, le=16)
    ollama_max_output_tokens: int = Field(default=1024, ge=64, le=8192)
    ollama_assist_max_output_tokens: int = Field(default=8192, ge=64, le=8192)
    ollama_research_max_output_tokens: int = Field(default=8192, ge=64, le=8192)
    ollama_asset_mapping_max_output_tokens: int = Field(default=1024, ge=64, le=8192)
    ollama_7b_max_input_tokens: int = Field(default=5000, ge=512, le=16384)
    ollama_research_max_input_tokens: int = Field(default=3500, ge=512, le=16384)
    ollama_research_revision_max_input_tokens: int = Field(
        default=2500, ge=512, le=16384
    )
    ollama_7b_tokenizer: str = "Qwen/Qwen2.5-7B-Instruct"
    ollama_7b_tokenizer_revision: str = "a09a35458c702b33eeacc393d103063234e8bc28"
    ollama_keep_alive: str = "-1"
    research_prompt_evidence_chars: int = Field(default=6000, ge=2000, le=24000)
    research_prompt_context_chars: int = Field(default=1000, ge=1000, le=12000)
    research_coalesce_window_hours: int = Field(default=24, ge=1, le=168)
    research_heartbeat_seconds: int = Field(default=30, ge=10, le=120)
    research_lease_seconds: int = Field(default=120, ge=30, le=600)
    model_task_heartbeat_seconds: int = Field(default=30, ge=10, le=120)
    model_task_lease_seconds: int = Field(default=180, ge=60, le=900)
    model_audit_enabled: bool = True
    model_audit_retention_days: int = Field(default=90, ge=1, le=3650)
    embedding_model: str = "intfloat/multilingual-e5-small"
    embedding_dimensions: int = Field(default=384, ge=64, le=4096)

    fmp_access_token: str = ""
    fmp_base_url: str = "https://financialmodelingprep.com/stable"
    fmp_mcp_url: str = ""
    fmp_rate_limit_per_minute: int = Field(default=240, ge=1, le=300)
    fmp_news_lookback_hours: int = Field(default=12, ge=1, le=168)
    admin_api_token: str = ""
    mcp_secret_key: str = ""
    searxng_mcp_url: str = "http://search-mcp:8080/mcp"
    duckduckgo_mcp_url: str = "http://duckduckgo-mcp:8080/mcp"
    weknora_default_url: str = "http://10.15.0.28/"
    web_search_timeout_seconds: int = Field(default=20, ge=2, le=120)
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    defillama_base_url: str = "https://api.llama.fi"
    sec_identity: str = ""
    rss_feed_urls: str = ""
    official_rss_feed_urls: str = ""
    akshare_asset_master_enabled: bool = True
    akshare_ipv4_only: bool = False

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    cloud_llm_base_url: str = ""
    cloud_llm_api_key: str = ""
    cloud_llm_model: str = ""

    scan_interval_minutes: int = Field(default=20, ge=5, le=1440)
    scan_batch_size: int = Field(default=40, ge=1, le=200)
    event_cluster_window_hours: int = Field(default=72, ge=1, le=720)
    targeted_evidence_limit: int = Field(default=120, ge=10, le=500)
    auto_research: bool = True
    auto_paper_trade: bool = False
    evolution_enabled: bool = False
    evolution_auto_merge: bool = False
    max_verification_rounds: int = Field(default=2, ge=1, le=3)
    minimum_directional_confidence: float = Field(default=0.55, ge=0, le=1)
    direction_positive_terms: str = ""
    direction_neutral_terms: str = ""
    direction_negative_terms: str = ""

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
    def ollama_extract_urls(self) -> list[str]:
        return self._ollama_urls(self.ollama_extract_base_urls, self.ollama_base_url)

    @property
    def ollama_assist_urls(self) -> list[str]:
        return self._ollama_urls(
            self.ollama_assist_base_urls,
            self.ollama_assist_base_url or self.ollama_base_url,
        )

    @property
    def ollama_research_urls(self) -> list[str]:
        return self._ollama_urls(self.ollama_research_base_urls, self.ollama_base_url)

    @property
    def ollama_code_urls(self) -> list[str]:
        return self._ollama_urls(self.ollama_code_base_urls, self.ollama_base_url)

    @staticmethod
    def _ollama_urls(configured: str, fallback: str) -> list[str]:
        values = [value.strip().rstrip("/") for value in configured.split(",") if value.strip()]
        return values or [fallback.rstrip("/")]

    @property
    def ollama_assist_url(self) -> str:
        return self.ollama_assist_urls[0]

    @model_validator(mode="after")
    def validate_ollama_instance_capacity(self) -> Self:
        lanes = (
            ("extract", self.ollama_extract_urls, self.ollama_extract_max_concurrency),
            ("assist", self.ollama_assist_urls, self.ollama_assist_max_concurrency),
            ("research", self.ollama_research_urls, self.ollama_research_max_concurrency),
            ("code", self.ollama_code_urls, self.ollama_code_max_concurrency),
        )
        for lane, urls, capacity in lanes:
            if capacity < len(urls):
                raise ValueError(
                    f"OLLAMA_{lane.upper()}_MAX_CONCURRENCY must be at least "
                    f"the configured instance count ({len(urls)})"
                )
        if self.research_pipeline_concurrency < len(self.ollama_research_urls):
            raise ValueError(
                "RESEARCH_PIPELINE_CONCURRENCY must be at least the configured "
                f"research instance count ({len(self.ollama_research_urls)})"
            )
        if self.model_task_lease_seconds < self.model_task_heartbeat_seconds * 2:
            raise ValueError(
                "MODEL_TASK_LEASE_SECONDS must be at least twice MODEL_TASK_HEARTBEAT_SECONDS"
            )
        return self

    @property
    def fmp_enabled(self) -> bool:
        return bool(self.fmp_access_token)

    @property
    def rss_feeds(self) -> list[str]:
        return [item.strip() for item in self.rss_feed_urls.split(",") if item.strip()]

    @property
    def official_rss_feeds(self) -> list[str]:
        return [item.strip() for item in self.official_rss_feed_urls.split(",") if item.strip()]

    @staticmethod
    def _direction_terms(configured: str) -> list[str]:
        return list(
            dict.fromkeys(
                item.strip()
                for item in re.split(r"[,，;；|\n]+", configured)
                if item.strip()
            )
        )

    @property
    def direction_positive_lexicon(self) -> list[str]:
        return self._direction_terms(self.direction_positive_terms)

    @property
    def direction_neutral_lexicon(self) -> list[str]:
        return self._direction_terms(self.direction_neutral_terms)

    @property
    def direction_negative_lexicon(self) -> list[str]:
        return self._direction_terms(self.direction_negative_terms)


@lru_cache
def get_settings() -> Settings:
    return Settings()
