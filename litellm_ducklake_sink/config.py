from __future__ import annotations

import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field

_PREFIX = "DUCKLAKE_SINK_"


@dataclass(frozen=True, slots=True)
class DuckLakeConfigError:
    message: str


@dataclass(frozen=True, slots=True)
class DuckLakeSinkConfig:
    endpoint: str = ""
    username: str = ""
    password: str = field(default="", repr=False)
    tenant: str | None = None
    pool: str | None = None
    tenant_db: str | None = None
    schema_name: str = "main"
    tls_skip_verify: bool = False
    enabled: bool = True
    capture_payloads: bool = False
    batch_rows: int = 1000
    batch_interval: int = 10
    batch_max_bytes: int = 2 * 1024 * 1024
    payload_max_bytes: int = 1024 * 1024
    spool_dir: str = ""
    spool_max_bytes: int = 512 * 1024 * 1024
    drain_timeout: float = 10.0
    flush_max_attempts: int = 5
    retention_days: int = 30
    maintenance_url: str | None = None
    maintenance_api_key: str | None = field(default=None, repr=False)


class _Invalid(Exception):
    pass


def _text(env: Mapping[str, str], key: str) -> str | None:
    value = env.get(_PREFIX + key)
    return value if value else None


def _flag(env: Mapping[str, str], key: str, default: bool) -> bool:
    value = _text(env, key)
    if value is None:
        return default
    if value.lower() in ("true", "false", "1", "0", "yes", "no"):
        return value.lower() in ("true", "1", "yes")
    raise _Invalid(f"{_PREFIX + key} must be a boolean, got {value!r}")


def _number(env: Mapping[str, str], key: str, default: int) -> int:
    value = _text(env, key)
    if value is None:
        return default
    # isascii() guards against unicode digits like '²' that isdigit() accepts
    # but int() rejects, which would escape the DuckLakeConfigError contract.
    if value.isascii() and value.isdigit():
        return int(value)
    raise _Invalid(f"{_PREFIX + key} must be a non-negative integer, got {value!r}")


def _enabled(env: Mapping[str, str]) -> bool:
    value = env.get(_PREFIX + "ENABLED")
    if value is None:
        return True
    if value.lower() in ("true", "false", "1", "0", "yes", "no"):
        return value.lower() in ("true", "1", "yes")
    raise _Invalid(f"{_PREFIX}ENABLED must be a boolean, got {value!r}")


def _at_least_one(key: str, value: int) -> int:
    if value < 1:
        raise _Invalid(f"{_PREFIX + key} must be at least 1, got {value}")
    return value


def _resolve(env: Mapping[str, str]) -> DuckLakeSinkConfig:
    # A disabled sink is a full short-circuit: the off switch must work with
    # nothing else configured, including stale or unparseable settings.
    if not _enabled(env):
        return DuckLakeSinkConfig(enabled=False)
    endpoint = _text(env, "ENDPOINT")
    username = _text(env, "USERNAME")
    password = _text(env, "PASSWORD")
    if endpoint is None or username is None or password is None:
        missing = ", ".join(
            _PREFIX + name
            for name, value in (
                ("ENDPOINT", endpoint),
                ("USERNAME", username),
                ("PASSWORD", password),
            )
            if value is None
        )
        raise _Invalid(f"missing required ducklake sink settings: {missing}")
    return DuckLakeSinkConfig(
        endpoint=endpoint,
        username=username,
        password=password,
        tenant=_text(env, "TENANT"),
        pool=_text(env, "POOL"),
        tenant_db=_text(env, "TENANT_DB"),
        schema_name=_text(env, "SCHEMA_NAME") or "main",
        tls_skip_verify=_flag(env, "TLS_SKIP_VERIFY", False),
        enabled=True,
        capture_payloads=_flag(env, "CAPTURE_PAYLOADS", False),
        batch_rows=_at_least_one("BATCH_ROWS", _number(env, "BATCH_ROWS", 1000)),
        batch_interval=_at_least_one("BATCH_INTERVAL", _number(env, "BATCH_INTERVAL", 10)),
        batch_max_bytes=_number(env, "BATCH_MAX_BYTES", 2 * 1024 * 1024),
        payload_max_bytes=_number(env, "PAYLOAD_MAX_BYTES", 1024 * 1024),
        spool_dir=_text(env, "SPOOL_DIR") or os.path.join(tempfile.gettempdir(), "litellm_ducklake_spool"),
        spool_max_bytes=_number(env, "SPOOL_MAX_BYTES", 512 * 1024 * 1024),
        drain_timeout=float(_number(env, "DRAIN_TIMEOUT", 10)),
        flush_max_attempts=_at_least_one("FLUSH_MAX_ATTEMPTS", _number(env, "FLUSH_MAX_ATTEMPTS", 5)),
        retention_days=_number(env, "RETENTION_DAYS", 30),
        maintenance_url=_text(env, "MAINTENANCE_URL"),
        maintenance_api_key=_text(env, "MAINTENANCE_API_KEY"),
    )


def resolve_ducklake_config(env: Mapping[str, str] | None = None) -> DuckLakeSinkConfig | DuckLakeConfigError:
    try:
        return _resolve(os.environ if env is None else env)
    except _Invalid as invalid:
        return DuckLakeConfigError(str(invalid))
