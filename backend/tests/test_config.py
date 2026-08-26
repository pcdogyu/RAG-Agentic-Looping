from backend.app.config import Settings


def test_default_ollama_roles_keep_extract_and_research_models_resident(
    monkeypatch,
) -> None:
    for name in (
        "OLLAMA_EXTRACT_MODEL",
        "OLLAMA_ASSIST_MODEL",
        "OLLAMA_RESEARCH_MODEL",
        "OLLAMA_CONTEXT_LENGTH",
        "OLLAMA_NUM_PARALLEL",
        "OLLAMA_MAX_LOADED_MODELS",
        "OLLAMA_MAX_QUEUE",
        "OLLAMA_LOAD_TIMEOUT",
        "OLLAMA_KEEP_ALIVE",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = Settings(_env_file=None)

    assert settings.ollama_extract_model == "qwen2.5:3b"
    assert settings.ollama_assist_model == "qwen2.5:7b"
    assert settings.ollama_research_model == "qwen2.5:14b"
    assert settings.ollama_context_length == 8192
    assert settings.ollama_num_parallel == 2
    assert settings.ollama_max_loaded_models == 3
    assert settings.ollama_max_queue == 256
    assert settings.ollama_load_timeout == "10m"
    assert settings.ollama_keep_alive == "-1"


def test_research_urls_use_pool_or_main_fallback() -> None:
    fallback = Settings(_env_file=None, ollama_base_url="http://main:11434")
    pooled = Settings(
        _env_file=None,
        ollama_base_url="http://main:11434",
        ollama_research_base_urls=" http://research-0:11435, http://research-1:11436/ ",
    )

    assert fallback.ollama_research_urls == ["http://main:11434"]
    assert pooled.ollama_research_urls == [
        "http://research-0:11435",
        "http://research-1:11436",
    ]
