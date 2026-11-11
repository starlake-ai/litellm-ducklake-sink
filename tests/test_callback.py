import importlib
import logging
import sys
from pathlib import Path

import pytest

from litellm_ducklake_sink.config import DuckLakeSinkMisconfiguredError
from litellm_ducklake_sink.ducklake_logger import DuckLakeLogger

REQUIRED_ENV = {
    "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
    "DUCKLAKE_SINK_USERNAME": "acme-admin",
    "DUCKLAKE_SINK_PASSWORD": "secret",
    "DUCKLAKE_SINK_TENANT": "acme",
    "DUCKLAKE_SINK_POOL": "bi",
}


def _set_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DUCKLAKE_SINK_SPOOL_DIR", str(tmp_path / "spool"))


def test_build_instance_registers_shutdown_drain(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _set_env(monkeypatch, tmp_path)
    from litellm_ducklake_sink.callback import build_instance

    registered: list = []
    logger = build_instance(register_shutdown=registered.append)
    assert isinstance(logger, DuckLakeLogger)
    assert registered == [logger.drain_sync]


def test_module_import_exposes_instance(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _set_env(monkeypatch, tmp_path)
    sys.modules.pop("litellm_ducklake_sink.callback", None)
    module = importlib.import_module("litellm_ducklake_sink.callback")
    assert isinstance(module.instance, DuckLakeLogger)
    sys.modules.pop("litellm_ducklake_sink.callback", None)


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in REQUIRED_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("DUCKLAKE_SINK_ENABLED", raising=False)


def test_module_import_without_config_fails_fast(monkeypatch: pytest.MonkeyPatch):
    _clear_env(monkeypatch)
    sys.modules.pop("litellm_ducklake_sink.callback", None)
    with pytest.raises(DuckLakeSinkMisconfiguredError, match="DUCKLAKE_SINK_ENDPOINT") as raised:
        importlib.import_module("litellm_ducklake_sink.callback")
    sys.modules.pop("litellm_ducklake_sink.callback", None)
    # Stays a ValueError so existing `except ValueError` handlers keep working.
    assert isinstance(raised.value, ValueError)
    # The message tells the operator how to boot without the sink.
    assert "DUCKLAKE_SINK_ENABLED=false" in str(raised.value)


def test_build_instance_logs_one_clear_line_when_misconfigured(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, tmp_path: Path
):
    # Import with a valid env: the module builds its own instance on import.
    _set_env(monkeypatch, tmp_path)
    from litellm_ducklake_sink.callback import build_instance

    _clear_env(monkeypatch)

    registered: list = []
    with caplog.at_level(logging.ERROR, logger="LiteLLM"), pytest.raises(DuckLakeSinkMisconfiguredError):
        build_instance(register_shutdown=registered.append)
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert len(errors) == 1
    line = errors[0].getMessage()
    assert "\n" not in line
    assert "DUCKLAKE_SINK_ENDPOINT" in line
    assert "DUCKLAKE_SINK_ENABLED=false" in line
    assert errors[0].exc_info is None
    assert registered == []


def test_module_import_with_only_enabled_false_succeeds(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DUCKLAKE_SINK_ENABLED", "false")
    sys.modules.pop("litellm_ducklake_sink.callback", None)
    module = importlib.import_module("litellm_ducklake_sink.callback")
    assert isinstance(module.instance, DuckLakeLogger)
    assert module.instance.sink_config.enabled is False
    sys.modules.pop("litellm_ducklake_sink.callback", None)


def test_build_instance_skips_shutdown_hook_when_disabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DUCKLAKE_SINK_ENABLED", "false")
    from litellm_ducklake_sink.callback import build_instance

    registered: list = []
    logger = build_instance(register_shutdown=registered.append)
    assert logger.sink_config.enabled is False
    assert registered == []
