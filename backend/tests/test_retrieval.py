from datetime import UTC, datetime, timedelta
from uuid import uuid4

from backend.app.config import Settings
from backend.app.domain import Evidence, SourceQuality
from backend.app.services.retrieval import RetrievalService


class FakeEmbeddings:
    def embed(self, texts: list[str]) -> list[list[float]]:
        output = []
        for text in texts:
            vector = [0.0] * 384
            lowered = text.lower()
            vector[0] = 1.0 if "tesla" in lowered else 0.0
            vector[1] = 1.0 if "bitcoin" in lowered else 0.0
            output.append(vector)
        return output


def test_hybrid_retrieval_enforces_point_in_time_boundary(db):
    run_id = uuid4()
    boundary = datetime(2025, 1, 15, tzinfo=UTC)
    known = Evidence(
        run_id=run_id,
        claim="Tesla vehicle deliveries increased",
        source_name="Official filing",
        source_url="https://example.com/known",
        source_quality=SourceQuality.OFFICIAL,
        published_at=boundary - timedelta(days=2),
        observed_at=boundary - timedelta(days=1),
        as_of=boundary - timedelta(days=1),
        excerpt="Tesla delivery data available before the research date.",
        independent_group="official",
    )
    future = Evidence(
        run_id=run_id,
        claim="Tesla future margin update",
        source_name="Future filing",
        source_url="https://example.com/future",
        source_quality=SourceQuality.OFFICIAL,
        published_at=boundary + timedelta(days=1),
        observed_at=boundary + timedelta(days=1),
        as_of=boundary + timedelta(days=1),
        excerpt="This evidence must not appear in a historical replay.",
        independent_group="official",
    )
    service = RetrievalService(
        db,
        Settings(embedding_dimensions=384),
        embeddings=FakeEmbeddings(),
    )
    service.index("equity:XNAS:TSLA", [known, future])

    results = service.search(
        "Tesla deliveries", asset_id="equity:XNAS:TSLA", as_of=boundary
    )

    assert [item["evidence_id"] for item in results] == [str(known.id)]
