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
    COALESCED = "coalesced"
    CANCELLED = "cancelled"


class Rating(StrEnum):
    STRONGLY_BULLISH = "strongly_bullish"
    BULLISH = "bullish"
    WATCH = "watch"
    BEARISH = "bearish"
    STRONGLY_BEARISH = "strongly_bearish"


class ModelDirection(StrEnum):
    BULLISH = "bullish"
    NEUTRAL = "neutral"
    BEARISH = "bearish"


class SignalStatus(StrEnum):
    """Final state of the directional research pipeline.

    A zero score is intentionally not enough to describe why no direction was
    published.  Consumers can distinguish an infrastructure/model failure,
    an evidence-gate rejection, and a genuinely neutral conclusion.
    """

    TECHNICAL_FAILURE = "technical_failure"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    NEUTRAL = "neutral"
    DIRECTIONAL = "directional"


class ClaimVerdict(StrEnum):
    SUPPORTED = "supported"
    CONTRADICTED = "contradicted"
    UNRELATED = "unrelated"
    INSUFFICIENT = "insufficient"


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
    # Multiple listings/ADRs may point at the same issuer without being treated
    # as interchangeable instruments.  Existing asset payloads remain valid.
    issuer_id: str | None = None
    primary_listing_asset_id: str | None = None
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
    mapping_confidence: float = Field(default=1, ge=0, le=1)
    identity_basis: list[str] = Field(default_factory=list)


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


class ClaimEvidenceAssessment(BaseModel):
    """Auditable, claim-level semantic evidence decision."""

    claim: str
    claim_kind: str
    stance: int = Field(default=0, ge=-1, le=1)
    verdict: ClaimVerdict
    evidence_ids: list[UUID] = Field(default_factory=list)
    confidence: float = Field(default=0, ge=0, le=1)
    reason: str = ""


class Recommendation(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    run_id: UUID
    asset: AssetRef
    score: int = Field(ge=-100, le=100)
    # `model_score` is retained only for audit. `raw_score` and `score` are
    # calculated by deterministic program logic.
    model_score: int | None = Field(default=None, ge=-100, le=100)
    model_direction: ModelDirection | None = None
    model_rating: Rating | None = None
    model_confidence: float | None = Field(default=None, ge=0, le=1)
    raw_score: int = Field(default=0, ge=-100, le=100)
    rating: Rating
    confidence: float = Field(ge=0, le=1)
    bull_probability: float = Field(ge=0, le=1)
    base_probability: float = Field(ge=0, le=1)
    bear_probability: float = Field(ge=0, le=1)
    horizon_days: int = Field(default=90, ge=1, le=730)
    valuation_low: float | None = None
    valuation_high: float | None = None
    thesis: Thesis
    generated_at: datetime = Field(default_factory=utc_now)
    as_of: datetime
    evidence_complete: bool = False
    directional_evidence_complete: bool = False
    direction_verified: bool = False
    signal_status: SignalStatus | None = None
    evidence_strength: float = Field(default=1, ge=0, le=1)
    mapping_confidence: float = Field(default=1, ge=0, le=1)
    claim_assessments: list[ClaimEvidenceAssessment] = Field(default_factory=list)
    primary_gate_reason: str | None = None
    gate_reasons: list[str] = Field(default_factory=list)
    scoring_version: str = "deterministic-v1"
    calibration_version: str = "uncalibrated-v1"

    @model_validator(mode="after")
    def probabilities_sum_to_one(self) -> Recommendation:
        total = self.bull_probability + self.base_probability + self.bear_probability
        if abs(total - 1.0) > 0.02:
            raise ValueError("bull/base/bear probabilities must sum to 1")
        if self.signal_status is None:
            if not self.evidence_complete:
                self.signal_status = SignalStatus.INSUFFICIENT_EVIDENCE
            elif abs(self.score) < 20:
                self.signal_status = SignalStatus.NEUTRAL
            else:
                self.signal_status = SignalStatus.DIRECTIONAL
        if self.raw_score == 0 and self.score != 0:
            # Backward-compatible read of recommendations stored before raw
            # and final scores were separated.
            self.raw_score = self.score
        if self.model_score is not None:
            # Backfill display-safe model opinion labels for recommendations
            # stored before these fields were introduced.
            self.model_direction = model_direction_for(self.model_score)
            self.model_rating = model_rating_for(self.model_score)
        if (
            self.evidence_complete
            and not self.claim_assessments
            and not self.gate_reasons
            and self.model_score is None
        ):
            # Legacy/manual recommendations predate the semantic gate. Keep
            # their established portfolio behavior while new research always
            # writes explicit gate metadata.
            self.directional_evidence_complete = True
            self.direction_verified = True
        return self


class ResearchRun(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    event_id: UUID | None = None
    trigger_event_ids: list[UUID] = Field(default_factory=list)
    asset: AssetRef
    status: RunStatus = RunStatus.QUEUED
    as_of: datetime = Field(default_factory=utc_now)
    historical_replay: bool = False
    retry_of_run_id: UUID | None = None
    retry_attempt: int = Field(default=0, ge=0)
    celery_task_id: str | None = None
    model_instance_id: str | None = None
    coalesced_into_run_id: UUID | None = None
    retryable_reason: str | None = None
    verification_round: int = 0
    missing_requirements: list[str] = Field(default_factory=list)
    contradictions: list[str] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    recommendation: Recommendation | None = None
    error: str | None = None
    analysis_steps: list[AnalysisStep] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    updated_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def include_primary_trigger(self) -> ResearchRun:
        if self.event_id is not None and self.event_id not in self.trigger_event_ids:
            self.trigger_event_ids.insert(0, self.event_id)
        return self


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
    celery_task_id: str | None = None
    model_instance_id: str | None = None
    retryable_reason: str | None = None
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
    benchmark_return: float | None = None
    alpha: float | None = None
    benchmark_status: str = "available"
    entry_at: datetime | None = None
    exit_at: datetime | None = None
    entry_price: float | None = None
    exit_price: float | None = None
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


def model_direction_for(score: int) -> ModelDirection:
    if score >= 20:
        return ModelDirection.BULLISH
    if score <= -20:
        return ModelDirection.BEARISH
    return ModelDirection.NEUTRAL


def model_rating_for(score: int) -> Rating:
    """Map the 7B model's audit score to five ungated opinion levels."""

    if score >= 60:
        return Rating.STRONGLY_BULLISH
    if score >= 20:
        return Rating.BULLISH
    if score <= -60:
        return Rating.STRONGLY_BEARISH
    if score <= -20:
        return Rating.BEARISH
    return Rating.WATCH
