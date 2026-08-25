from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


def utc_now() -> datetime:
    return datetime.now(UTC)


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class AssetClass(StrEnum):
    EQUITY = "equity"
    CRYPTO = "crypto"


class Market(StrEnum):
    US = "US"
    CN = "CN"
    HK = "HK"
    CRYPTO = "CRYPTO"


class SourceQuality(StrEnum):
    OFFICIAL = "official"
    PRIMARY = "primary"
    PROFESSIONAL = "professional"
    AGGREGATOR = "aggregator"
    SOCIAL = "social"


class EventType(StrEnum):
    EARNINGS = "earnings"
    PRODUCT = "product"
    REGULATION = "regulation"
    M_AND_A = "m_and_a"
    MANAGEMENT = "management"
    SECURITY = "security"
    MACRO = "macro"
    SUPPLY_CHAIN = "supply_chain"
    TOKENOMICS = "tokenomics"
    OTHER = "other"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    FAILED = "failed"


class Rating(StrEnum):
    STRONGLY_BULLISH = "strongly_bullish"
    BULLISH = "bullish"
    WATCH = "watch"
    BEARISH = "bearish"
    STRONGLY_BEARISH = "strongly_bearish"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class EvolutionStatus(StrEnum):
    PROPOSED = "proposed"
    TESTING = "testing"
    REJECTED = "rejected"
    MERGED = "merged"
    ROLLED_BACK = "rolled_back"


class AssetRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asset_id: str
    asset_class: AssetClass
    market: Market
    symbol: str
    name: str
    exchange_or_provider: str
    currency: str = "USD"
    aliases: list[str] = Field(default_factory=list)
    products: list[str] = Field(default_factory=list)
    competitors: list[str] = Field(default_factory=list)
    lot_size: int = Field(default=1, ge=1)
    active: bool = True


class NewsItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    source: str
    source_quality: SourceQuality = SourceQuality.AGGREGATOR
    title: str
    summary: str = ""
    url: str
    language: str = "en"
    published_at: datetime
    observed_at: datetime = Field(default_factory=utc_now)
    as_of: datetime = Field(default_factory=utc_now)
    content_hash: str
    symbols: list[str] = Field(default_factory=list)
    raw_metadata: dict[str, Any] = Field(default_factory=dict)


class CandidateAsset(BaseModel):
    asset: AssetRef
    relationship: str
    impact_direction: int = Field(default=0, ge=-1, le=1)
    relevance: float = Field(ge=0, le=1)
    rationale: str


class AnalysisStep(BaseModel):
    """User-facing audit summary for one processing stage.

    This deliberately stores structured outcomes, not prompts or hidden model
    reasoning.
    """

    phase: str
    status: str = "completed"
    executor: str
    model: str | None = None
    summary: str
    metrics: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime = Field(default_factory=utc_now)


class NewsEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    news_item_ids: list[UUID]
    headline: str
    event_type: EventType
    entities: list[str] = Field(default_factory=list)
    direct_impact: str
    horizon_days: int = Field(default=90, ge=1, le=730)
    source_quality: SourceQuality
    published_at: datetime
    observed_at: datetime = Field(default_factory=utc_now)
    as_of: datetime = Field(default_factory=utc_now)
    candidates: list[CandidateAsset] = Field(default_factory=list)
    novelty: float = Field(default=0.5, ge=0, le=1)
    priority: float = Field(default=0.5, ge=0, le=1)
    analysis_steps: list[AnalysisStep] = Field(default_factory=list)


class Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    run_id: UUID
    claim: str
    source_name: str
    source_url: str
    source_quality: SourceQuality
    published_at: datetime
    observed_at: datetime
    as_of: datetime
    excerpt: str = Field(max_length=1000)
    independent_group: str
    numeric_value: float | None = None
    numeric_unit: str | None = None


class Thesis(BaseModel):
    summary: str
    historical_context: str = ""
    financials_and_growth: str = ""
    products_or_protocol: str = ""
    competition: str = ""
    valuation_or_tokenomics: str = ""
    catalysts: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    invalidation_conditions: list[str] = Field(default_factory=list)
    evidence_ids: list[UUID] = Field(default_factory=list)


class Recommendation(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    run_id: UUID
    asset: AssetRef
    score: int = Field(ge=-100, le=100)
    rating: Rating
    confidence: float = Field(ge=0, le=1)
    bull_probability: float = Field(ge=0, le=1)
    base_probability: float = Field(ge=0, le=1)
    bear_probability: float = Field(ge=0, le=1)
    horizon_days: int = Field(default=90, ge=30, le=180)
    valuation_low: float | None = None
    valuation_high: float | None = None
    thesis: Thesis
    generated_at: datetime = Field(default_factory=utc_now)
    as_of: datetime
    evidence_complete: bool = False

    @model_validator(mode="after")
    def probabilities_sum_to_one(self) -> Recommendation:
        total = self.bull_probability + self.base_probability + self.bear_probability
        if abs(total - 1.0) > 0.02:
            raise ValueError("bull/base/bear probabilities must sum to 1")
        return self


class ResearchRun(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    event_id: UUID | None = None
    asset: AssetRef
    status: RunStatus = RunStatus.QUEUED
    as_of: datetime = Field(default_factory=utc_now)
    historical_replay: bool = False
    retry_of_run_id: UUID | None = None
    retry_attempt: int = Field(default=0, ge=0)
    verification_round: int = 0
    missing_requirements: list[str] = Field(default_factory=list)
    contradictions: list[str] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    recommendation: Recommendation | None = None
    error: str | None = None
    analysis_steps: list[AnalysisStep] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class EventReport(BaseModel):
    summary: str
    affected_markets: list[str] = Field(default_factory=list)
    affected_sectors: list[str] = Field(default_factory=list)
    scenarios: list[str] = Field(default_factory=list)
    catalysts: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    evidence_ids: list[UUID] = Field(default_factory=list)
    confidence: float = Field(default=0.3, ge=0, le=1)
    evidence_complete: bool = False


class EventResearchRun(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    event_id: UUID
    status: RunStatus = RunStatus.QUEUED
    as_of: datetime = Field(default_factory=utc_now)
    verification_round: int = 0
    retry_count: int = Field(default=0, ge=0)
    missing_requirements: list[str] = Field(default_factory=list)
    contradictions: list[str] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    report: EventReport | None = None
    error: str | None = None
    analysis_steps: list[AnalysisStep] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class PaperOrder(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    recommendation_id: UUID
    asset: AssetRef
    side: OrderSide
    quantity: float = Field(gt=0)
    price: float = Field(gt=0)
    currency: str
    fee: float = Field(ge=0)
    executed_at: datetime = Field(default_factory=utc_now)


class Position(BaseModel):
    asset: AssetRef
    quantity: float
    average_cost: float
    last_price: float
    market_value_usd: float
    unrealized_pnl_usd: float
    weight: float


class PortfolioSnapshot(BaseModel):
    cash_usd: float
    nav_usd: float
    crypto_weight: float
    positions: list[Position]
    as_of: datetime


class Outcome(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    recommendation_id: UUID
    horizon_days: int
    raw_return: float
    benchmark_return: float
    alpha: float
    direction_correct: bool
    brier_score: float
    max_drawdown: float = 0
    thesis_invalidated: bool = False
    observed_at: datetime = Field(default_factory=utc_now)


class EvolutionCandidate(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    hypothesis: str
    target_metric: str
    expected_improvement: float = Field(ge=0)
    branch: str
    status: EvolutionStatus = EvolutionStatus.PROPOSED
    baseline_score: float | None = None
    candidate_score: float | None = None
    test_report: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


def rating_for(score: int, confidence: float, evidence_complete: bool = True) -> Rating:
    if confidence < 0.55 or not evidence_complete:
        return Rating.WATCH
    if score >= 60:
        return Rating.STRONGLY_BULLISH
    if score >= 20:
        return Rating.BULLISH
    if score <= -60:
        return Rating.STRONGLY_BEARISH
    if score <= -20:
        return Rating.BEARISH
    return Rating.WATCH
