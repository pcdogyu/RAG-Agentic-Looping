import pytest

from backend.app.config import Settings
from backend.app.services.evolution import EvolutionError, EvolutionService


def test_evolution_is_disabled_by_default(db):
    service = EvolutionService(settings=Settings(evolution_enabled=False))
    with pytest.raises(EvolutionError, match="EVOLUTION_ENABLED"):
        service.propose(db, [{"failure": "example"}])


def test_secret_detection():
    service = EvolutionService(settings=Settings())
    with pytest.raises(EvolutionError, match="secret"):
        service._assert_no_secret("+ FMP_API_KEY='do-not-commit-this'")  # pragma: allowlist secret
