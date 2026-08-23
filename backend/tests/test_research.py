import re
from datetime import UTC, datetime
from hashlib import sha256

from backend.app.config import Settings
from backend.app.domain import (
    CandidateAsset,
    EventType,
    NewsEvent,
    NewsItem,
    Rating,
    SourceQuality,
)
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.research import DraftOutput, ResearchService
from backend.app.storage import save_news


class FakeRegistry:
    def __init__(self):
        self.research_calls = 0

    def get_research_data(self, asset):
        self.research_calls += 1
        return {
            "fundamentals": {
                "income": [
                    {
                        "date": "2024-09-30",
                        "calendarYear": "2024",
                        "revenue": 391_035_000_000,
                        "netIncome": 93_736_000_000,
                    }
                ]
            },
            "filings": [
                {
                    "formType": "10-K",
                    "acceptedDate": "2024-11-01T12:00:00Z",
                    "finalLink": "https://www.sec.gov/Archives/example-10k.htm",
                }
            ],
        }


class FakeResearchLlm:
    def generate_json(self, *, schema, prompt, **kwargs):
        ids = re.findall(r'"id": "([0-9a-f-]{36})"', prompt)
        return DraftOutput(
            summary="Services growth supports the medium-term thesis, subject to valuation risk.",
            historical_context="Prior earnings provide a comparison point.",
            financials_and_growth="Revenue and net income are cited from structured data.",
            products_or_protocol="Services and hardware are the main product groups.",
            competition="Competition remains strong.",
            valuation_or_tokenomics="Valuation requires a margin of safety.",
            catalysts=["Services growth"],
            risks=["Valuation compression"],
            invalidation_conditions=["Revenue growth turns negative"],
            evidence_ids=ids,
            score=35,
            confidence=0.75,
            bull_probability=0.6,
            base_probability=0.25,
            bear_probability=0.15,
        ).model_dump(mode="json")


class FakeStrongResearchLlm(FakeResearchLlm):
    def generate_json(self, **kwargs):
        payload = super().generate_json(**kwargs)
        payload["score"] = 80
        payload["confidence"] = 0.85
        return payload


def test_research_graph_produces_verified_recommendation(db, tmp_path):
    as_of = datetime(2025, 1, 31, tzinfo=UTC)
    item = NewsItem(
        source="Apple Investor Relations",
        source_quality=SourceQuality.OFFICIAL,
        title="Apple reports quarterly results",
        summary="Services revenue increased.",
        url="https://www.apple.com/newsroom/example",
        published_at=as_of,
        observed_at=as_of,
        as_of=as_of,
        content_hash=sha256(b"apple-official").hexdigest(),
        symbols=["AAPL"],
    )
    save_news(db, item)
    asset = SEED_ASSETS[0]
    event = NewsEvent(
        news_item_ids=[item.id],
        headline=item.title,
        event_type=EventType.EARNINGS,
        entities=["Apple"],
        direct_impact="Quarterly results",
        source_quality=SourceQuality.OFFICIAL,
        published_at=as_of,
        observed_at=as_of,
        as_of=as_of,
        candidates=[
            CandidateAsset(
                asset=asset,
                relationship="direct",
                relevance=1,
                rationale="AAPL is explicit",
            )
        ],
    )
    settings = Settings(
        database_url="sqlite:///./data/test_agent.db",
        reports_dir=tmp_path,
        fmp_access_token="",
        fmp_mcp_url="",
    )
    registry = FakeRegistry()
    run = ResearchService(registry, db, settings, FakeResearchLlm()).run(asset, event, as_of)
    assert registry.research_calls == 1
    assert any(item.source_name == "FMP standardized financials" for item in run.evidence)
    assert run.historical_replay is False
    assert run.recommendation is not None
    assert run.recommendation.evidence_complete is True
    assert run.recommendation.rating is Rating.BULLISH
    assert run.recommendation.thesis.evidence_ids
    phases = [step.phase for step in run.analysis_steps]
    assert "evidence_gathering" in phases
    assert "report_drafting" in phases
    assert "verification" in phases
    assert phases[-1] == "finalization"
    assert any(step.model == settings.ollama_research_model for step in run.analysis_steps)
    assert not any("prompt" in step.metrics for step in run.analysis_steps)
    assert list(tmp_path.glob("AAPL_*.md"))

    strong_run = ResearchService(
        FakeRegistry(), db, settings, FakeStrongResearchLlm()
    ).run(asset, event, as_of)
    assert strong_run.recommendation is not None
    assert strong_run.recommendation.evidence_complete is True
    assert strong_run.recommendation.rating is Rating.WATCH
    assert strong_run.verification_round == 2


def test_explicit_historical_replay_skips_live_providers(db, tmp_path):
    as_of = datetime(2025, 1, 31, tzinfo=UTC)
    registry = FakeRegistry()
    settings = Settings(
        database_url="sqlite:///./data/test_agent.db",
        reports_dir=tmp_path,
        fmp_access_token="",
        fmp_mcp_url="",
    )

    run = ResearchService(registry, db, settings, FakeResearchLlm()).run(
        SEED_ASSETS[0],
        as_of=as_of,
        historical_replay=True,
    )

    assert registry.research_calls == 0
    assert run.historical_replay is True
    assert run.evidence == []
    assert run.status.value == "insufficient_evidence"
