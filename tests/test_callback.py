import importlib
import sys
from pathlib import Path

import pytest

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


def test_module_import_without_config_fails_fast(monkeypatch: pytest.MonkeyPatch):
    for key in REQUIRED_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("DUCKLAKE_SINK_ENABLED", raising=False)
    sys.modules.pop("litellm_ducklake_sink.callback", None)
    with pytest.raises(ValueError, match="DUCKLAKE_SINK_"):
        importlib.import_module("litellm_ducklake_sink.callback")
    sys.modules.pop("litellm_ducklake_sink.callback", None)


def test_module_import_with_only_enabled_false_succeeds(monkeypatch: pytest.MonkeyPatch):
    for key in REQUIRED_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DUCKLAKE_SINK_ENABLED", "false")
    sys.modules.pop("litellm_ducklake_sink.callback", None)
    module = importlib.import_module("litellm_ducklake_sink.callback")
    assert isinstance(module.instance, DuckLakeLogger)
    assert module.instance.sink_config.enabled is False
    sys.modules.pop("litellm_ducklake_sink.callback", None)
