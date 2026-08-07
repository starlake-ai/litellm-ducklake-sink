import pytest

from litellm_ducklake_sink.config import DuckLakeConfigError, DuckLakeSinkConfig, resolve_ducklake_config

REQUIRED = {
    "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
    "DUCKLAKE_SINK_USERNAME": "acme-admin",
    "DUCKLAKE_SINK_PASSWORD": "secret",
    "DUCKLAKE_SINK_TENANT": "acme",
    "DUCKLAKE_SINK_POOL": "bi",
}


def test_missing_required_settings_returns_error():
    result = resolve_ducklake_config({"DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338"})
    assert isinstance(result, DuckLakeConfigError)
    assert "DUCKLAKE_SINK_USERNAME" in result.message
    assert "DUCKLAKE_SINK_TENANT" not in result.message
    assert "DUCKLAKE_SINK_POOL" not in result.message


def test_tenant_and_pool_are_optional():
    env = dict(REQUIRED)
    del env["DUCKLAKE_SINK_TENANT"]
    del env["DUCKLAKE_SINK_POOL"]
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeSinkConfig)
    assert result.tenant is None
    assert result.pool is None


def test_minimal_env_resolves_with_defaults():
    result = resolve_ducklake_config(dict(REQUIRED))
    assert isinstance(result, DuckLakeSinkConfig)
    assert result.enabled is True
    assert result.capture_payloads is False
    assert result.batch_rows == 1000
    assert result.batch_interval == 10
    assert result.batch_max_bytes == 2 * 1024 * 1024
    assert result.payload_max_bytes == 1024 * 1024
    assert result.spool_max_bytes == 512 * 1024 * 1024
    assert result.drain_timeout == 10
    assert result.flush_max_attempts == 5
    assert result.retention_days == 30
    assert result.schema_name == "main"
    assert result.tenant_db is None
    assert result.maintenance_url is None
    assert result.spool_dir.endswith("litellm_ducklake_spool")


def test_overrides_and_flag_parsing():
    env = dict(REQUIRED)
    env["DUCKLAKE_SINK_BATCH_ROWS"] = "50"
    env["DUCKLAKE_SINK_CAPTURE_PAYLOADS"] = "true"
    env["DUCKLAKE_SINK_ENABLED"] = "false"
    env["DUCKLAKE_SINK_SCHEMA_NAME"] = "tpch1"
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeSinkConfig)
    assert result.batch_rows == 50
    assert result.capture_payloads is True
    assert result.enabled is False
    assert result.schema_name == "tpch1"


def test_process_env_is_default_source(monkeypatch: pytest.MonkeyPatch):
    for key, value in REQUIRED.items():
        monkeypatch.setenv(key, value)
    result = resolve_ducklake_config(None)
    assert isinstance(result, DuckLakeSinkConfig)
    assert result.username == "acme-admin"


def test_bad_integer_returns_error():
    env = dict(REQUIRED)
    env["DUCKLAKE_SINK_BATCH_ROWS"] = "lots"
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeConfigError)
    assert "DUCKLAKE_SINK_BATCH_ROWS" in result.message


def test_secrets_hidden_from_repr():
    env = dict(REQUIRED)
    env["DUCKLAKE_SINK_MAINTENANCE_API_KEY"] = "topsecretkey"
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeSinkConfig)
    rendered = repr(result)
    assert "secret" not in rendered
    assert "topsecretkey" not in rendered


def test_bad_boolean_returns_error():
    env = dict(REQUIRED)
    env["DUCKLAKE_SINK_ENABLED"] = "banana"
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeConfigError)
    assert "DUCKLAKE_SINK_ENABLED" in result.message


def test_zero_batch_rows_returns_error():
    env = dict(REQUIRED)
    env["DUCKLAKE_SINK_BATCH_ROWS"] = "0"
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeConfigError)
    assert "DUCKLAKE_SINK_BATCH_ROWS" in result.message


def test_zero_batch_interval_returns_error():
    env = dict(REQUIRED)
    env["DUCKLAKE_SINK_BATCH_INTERVAL"] = "0"
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeConfigError)
    assert "DUCKLAKE_SINK_BATCH_INTERVAL" in result.message


def test_zero_flush_max_attempts_returns_error():
    env = dict(REQUIRED)
    env["DUCKLAKE_SINK_FLUSH_MAX_ATTEMPTS"] = "0"
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeConfigError)
    assert "DUCKLAKE_SINK_FLUSH_MAX_ATTEMPTS" in result.message
