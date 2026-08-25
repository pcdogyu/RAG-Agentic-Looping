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
    ResearchRun,
    SourceQuality,
)
from backend.app.providers.registry import SEED_ASSETS
from backend.app.services.mcp_registry import SearchResult
from backend.app.services.research import DraftOutput, ResearchService
from backend.app.storage import get_event, save_event, save_news


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


class CapturingResearchLlm(FakeResearchLlm):
    def __init__(self):
        self.prompts = []

    def generate_json(self, *, prompt, **kwargs):
        self.prompts.append(prompt)
        return super().generate_json(prompt=prompt, **kwargs)


class TargetedRegistry:
    def __init__(self, corroborating_news):
        self.corroborating_news = corroborating_news
        self.research_calls = 0
        self.discovery_calls = 0

    def get_research_data(self, asset):
        self.research_calls += 1
        return {"fundamentals": {}, "filings": []}

    def discover_news(self, **kwargs):
        self.discovery_calls += 1
        return [self.corroborating_news]


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

    strong_run = ResearchService(FakeRegistry(), db, settings, FakeStrongResearchLlm()).run(
        asset, event, as_of
    )
    assert strong_run.recommendation is not None
    assert strong_run.recommendation.evidence_complete is True
    assert strong_run.recommendation.rating is Rating.WATCH
    assert strong_run.verification_round == 2


def test_research_draft_respects_cpu_prompt_budgets(db, tmp_path):
    settings = Settings(
        database_url="sqlite:///./data/test_agent.db",
        reports_dir=tmp_path,
        fmp_access_token="",
        fmp_mcp_url="",
        research_prompt_evidence_chars=2000,
        research_prompt_context_chars=1000,
    )
    llm = CapturingResearchLlm()
    service = ResearchService(FakeRegistry(), db, settings, llm)
    run = ResearchRun(asset=SEED_ASSETS[0])

    service._draft(
        {
            "run": run.model_dump(mode="json"),
            "evidence": [{"payload": "x" * 10000}],
            "retrieved_context": [{"text": "y" * 10000}],
        }
    )

    assert len(llm.prompts) == 1
    assert llm.prompts[0].count("x") <= settings.research_prompt_evidence_chars
    assert llm.prompts[0].count("y") <= settings.research_prompt_context_chars


def test_explicit_historical_replay_skips_live_providers(db, tmp_path, monkeypatch):
    as_of = datetime(2025, 1, 31, tzinfo=UTC)
    registry = FakeRegistry()
    settings = Settings(
        database_url="sqlite:///./data/test_agent.db",
        reports_dir=tmp_path,
        fmp_access_token="",
        fmp_mcp_url="",
    )
    monkeypatch.setattr(
        "backend.app.services.research.search_enabled_sources_sync",
        lambda _request: (_ for _ in ()).throw(AssertionError("historical replay searched web")),
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


def test_verification_gap_triggers_targeted_acquisition_and_reverification(
    db, tmp_path, monkeypatch
):
    as_of = datetime(2025, 1, 31, 12, 0, tzinfo=UTC)
    initial = NewsItem(
        source="Aggregator A",
        source_quality=SourceQuality.AGGREGATOR,
        title="Apple reports Services revenue growth",
        summary="Apple Services revenue increased.",
        url="https://a.example/apple-services",
        published_at=as_of,
        observed_at=as_of,
        as_of=as_of,
        content_hash=sha256(b"targeted-initial").hexdigest(),
        symbols=["AAPL"],
    )
    corroborating = NewsItem(
        source="Professional Wire B",
        source_quality=SourceQuality.PROFESSIONAL,
        title="Apple reports growth in Services revenue",
        summary="Apple confirmed higher Services revenue.",
        url="https://b.example/apple-services",
        published_at=as_of,
        observed_at=as_of,
        as_of=as_of,
        content_hash=sha256(b"targeted-corroborating").hexdigest(),
        symbols=["AAPL"],
    )
    save_news(db, initial)
    asset = SEED_ASSETS[0]
    event = NewsEvent(
        news_item_ids=[initial.id],
        headline=initial.title,
        event_type=EventType.EARNINGS,
        entities=["Apple"],
        direct_impact=initial.summary,
        source_quality=initial.source_quality,
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
    save_event(db, event)
    registry = TargetedRegistry(corroborating)
    settings = Settings(
        database_url="sqlite:///./data/test_agent.db",
        reports_dir=tmp_path,
        fmp_access_token="",
        fmp_mcp_url="",
    )
    search_calls = []

    def fake_search(request):
        search_calls.append(request)
        return (
            [
                SearchResult(
                    title="Apple investor update",
                    url="https://c.example/apple-update?utm_source=test",
                    snippet="A third independent source confirms Services growth.",
                    source="Search MCP",
                    domain="c.example",
                    published_at=as_of,
                )
            ],
            [],
        )

    monkeypatch.setattr("backend.app.services.research.search_enabled_sources_sync", fake_search)

    run = ResearchService(registry, db, settings, FakeResearchLlm()).run(asset, event, as_of)

    assert registry.discovery_calls == 1
    assert 1 <= len(search_calls) <= 3
    assert all(call.limit == 5 for call in search_calls)
    assert run.status.value == "completed"
    assert run.verification_round == 2
    assert len({item.independent_group for item in run.evidence}) == 3
    phases = [step.phase for step in run.analysis_steps]
    assert phases.count("verification") == 2
    assert phases.count("web_search_verification") == 1
    assert len([item for item in run.evidence if item.source_name == "Search MCP"]) == 1
    assert phases.index("targeted_evidence_acquisition") < phases.index("report_revision")
    persisted_event = get_event(db, event.id)
    assert persisted_event is not None
    assert len(persisted_event.news_item_ids) == 2
