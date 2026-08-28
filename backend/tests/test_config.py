from pathlib import Path

import pytest

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
    assert settings.ollama_research_model == "qwen2.5:7b"
    assert settings.ollama_context_length == 8192
    assert settings.ollama_num_parallel == 2
    assert settings.ollama_max_loaded_models == 2
    assert settings.ollama_assist_context_length == 16384
    assert settings.ollama_research_context_length == 16384
    assert settings.ollama_asset_mapping_context_length == 8192
    assert settings.ollama_7b_max_input_tokens == 5000
    assert settings.ollama_research_max_input_tokens == 3500
    assert settings.ollama_research_revision_max_input_tokens == 2500
    assert settings.research_pipeline_concurrency == 3
    assert settings.research_asset_soft_time_limit_seconds == 2040
    assert settings.research_asset_hard_time_limit_seconds == 2100
    assert settings.research_prompt_evidence_chars == 6000
    assert settings.research_prompt_context_chars == 1000
    assert settings.ollama_assist_max_output_tokens == 8192
    assert settings.ollama_research_max_output_tokens == 8192
    assert settings.ollama_asset_mapping_max_output_tokens == 1024
    assert settings.model_task_heartbeat_seconds == 30
    assert settings.model_task_lease_seconds == 180
    assert settings.ollama_max_queue == 256
    assert settings.ollama_load_timeout == "10m"
    assert settings.ollama_keep_alive == "-1"


def test_research_urls_use_pool_or_main_fallback() -> None:
    fallback = Settings(_env_file=None, ollama_base_url="http://main:11434")
    pooled = Settings(
        _env_file=None,
        ollama_base_url="http://main:11434",
        ollama_research_base_urls=(
            " http://research-0:11435, http://research-1:11436/, "
            "http://research-2:11439/ "
        ),
        ollama_research_max_concurrency=3,
    )

    assert fallback.ollama_research_urls == ["http://main:11434"]
    assert pooled.ollama_research_urls == [
        "http://research-0:11435",
        "http://research-1:11436",
        "http://research-2:11439",
    ]


def test_direction_lexicons_parse_supported_separators_and_remove_duplicates() -> None:
    settings = Settings(
        _env_file=None,
        direction_positive_terms="上涨，积极进展|上涨",
        direction_neutral_terms="持平; 维持不变",
        direction_negative_terms="下跌；临床失败\n利空",
    )

    assert settings.direction_positive_lexicon == ["上涨", "积极进展"]
    assert settings.direction_neutral_lexicon == ["持平", "维持不变"]
    assert settings.direction_negative_lexicon == ["下跌", "临床失败", "利空"]


def test_model_task_lease_must_cover_two_heartbeats() -> None:
    with pytest.raises(ValueError, match="MODEL_TASK_LEASE_SECONDS"):
        Settings(
            _env_file=None,
            model_task_heartbeat_seconds=60,
            model_task_lease_seconds=60,
        )


def test_research_pipeline_concurrency_must_cover_every_instance() -> None:
    with pytest.raises(ValueError, match="RESEARCH_PIPELINE_CONCURRENCY"):
        Settings(
            _env_file=None,
            ollama_research_base_urls="http://research-0,http://research-1",
            ollama_research_max_concurrency=2,
            research_pipeline_concurrency=1,
        )


def test_research_pipeline_concurrency_must_not_exceed_model_capacity() -> None:
    with pytest.raises(ValueError, match="OLLAMA_RESEARCH_MAX_CONCURRENCY"):
        Settings(
            _env_file=None,
            ollama_research_base_urls="http://research-0,http://research-1",
            ollama_research_max_concurrency=2,
            research_pipeline_concurrency=3,
        )


def test_asset_research_soft_limit_must_precede_hard_limit() -> None:
    with pytest.raises(ValueError, match="RESEARCH_ASSET_SOFT_TIME_LIMIT_SECONDS"):
        Settings(
            _env_file=None,
            research_asset_soft_time_limit_seconds=2100,
            research_asset_hard_time_limit_seconds=2100,
        )


def test_research_host_script_defines_three_ten_cpu_numa_local_instances() -> None:
    script = (
        Path(__file__).resolve().parents[2]
        / "infra"
        / "ollama"
        / "run-research-from-env.sh"
    ).read_text(encoding="utf-8")

    assert 'host="172.17.0.1:11435"\n    cpus="0-9"\n    node="0"' in script
    assert 'host="172.17.0.1:11436"\n    cpus="20-29"\n    node="1"' in script
    assert 'host="172.17.0.1:11439"\n    cpus="30-39"\n    node="1"' in script
