from backend.app.config import Settings


def test_default_ollama_roles_keep_extract_and_research_models_resident(
    monkeypatch,
) -> None:
    for name in (
        "OLLAMA_EXTRACT_MODEL",
        "OLLAMA_ASSIST_MODEL",
        "OLLAMA_RESEARCH_MODEL",
        "OLLAMA_KEEP_ALIVE",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = Settings(_env_file=None)

    assert settings.ollama_extract_model == "qwen2.5:3b"
    assert settings.ollama_assist_model == "qwen2.5:7b"
    assert settings.ollama_research_model == "qwen2.5:14b"
    assert settings.ollama_keep_alive == "-1"
