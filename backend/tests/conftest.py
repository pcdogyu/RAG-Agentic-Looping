import os

os.environ["DATABASE_URL"] = "sqlite:///./data/test_agent.db"
os.environ["REDIS_URL"] = "redis://127.0.0.1:6399/15"
os.environ["OLLAMA_BASE_URL"] = "http://127.0.0.1:1"
os.environ["OLLAMA_RESEARCH_BASE_URLS"] = ""
os.environ["OLLAMA_ASSIST_BASE_URL"] = ""
os.environ["OLLAMA_RESEARCH_MODEL"] = "qwen2.5:7b"
os.environ["FMP_ACCESS_TOKEN"] = ""
os.environ["FMP_MCP_URL"] = ""
os.environ["AKSHARE_ASSET_MASTER_ENABLED"] = "false"
os.environ["SCAN_INTERVAL_MINUTES"] = "20"
os.environ["ADMIN_API_TOKEN"] = "test-admin-token"
os.environ["MCP_SECRET_KEY"] = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="

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
