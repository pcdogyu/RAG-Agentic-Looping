import os

os.environ["DATABASE_URL"] = "sqlite:///./data/test_agent.db"
os.environ["REDIS_URL"] = "redis://127.0.0.1:6399/15"
os.environ["OLLAMA_BASE_URL"] = "http://127.0.0.1:1"
os.environ["FMP_ACCESS_TOKEN"] = ""
os.environ["FMP_MCP_URL"] = ""

import pytest

from backend.app.db import Base, SessionLocal, engine


@pytest.fixture(autouse=True)
def clean_database():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield


@pytest.fixture
def db():
    with SessionLocal() as session:
        yield session
