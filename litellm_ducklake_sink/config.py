from __future__ import annotations

import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field, replace

_PREFIX = "DUCKLAKE_SINK_"


@dataclass(frozen=True, slots=True)
class DuckLakeConfigError:
    message: str


class DuckLakeSinkMisconfiguredError(ValueError):
    """An enabled sink cannot start from its configuration; the proxy must not boot."""

    def __init__(self, reason: str) -> None:
        super().__init__(
            f"litellm-ducklake-sink is misconfigured: {reason}. "
            f"Fix the setting, or set {_PREFIX}ENABLED=false to start the proxy without the sink."
        )


@dataclass(frozen=True, slots=True)
class DuckLakeSinkConfig:
    # Direct mode: the sink embeds DuckDB and attaches the DuckLake catalog itself.
    catalog: str | None = field(default=None, repr=False)
    data_path: str | None = None
    quack_token: str | None = field(default=None, repr=False)
    # S3 credentials for an s3:// data_path. Without keys, DuckDB uses the AWS credential chain.
    s3_key_id: str | None = None
    s3_secret: str | None = field(default=None, repr=False)
    s3_region: str | None = None
    s3_endpoint: str | None = None
    s3_url_style: str | None = None
    s3_use_ssl: bool = True
    # Server mode: the sink sends SQL to a server that has the lake attached,
    # over Quack (endpoint quack:host:port) or Arrow Flight SQL (grpc://, grpc+tls://).
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

    @property
    def is_direct(self) -> bool:
        return self.catalog is not None

    @property
    def is_quack_endpoint(self) -> bool:
        return self.endpoint.startswith("quack:")


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
    catalog = _text(env, "CATALOG")
    endpoint = _text(env, "ENDPOINT")
    if catalog is not None and endpoint is not None:
        raise _Invalid(
            f"set either {_PREFIX}CATALOG (write to DuckLake directly) or "
            f"{_PREFIX}ENDPOINT (write through a Quack or Flight SQL server), not both"
        )
    if catalog is not None:
        return _with_tuning(env, _direct_connection(env, catalog))
    if endpoint is not None and endpoint.startswith("quack:"):
        return _with_tuning(env, _quack_endpoint_connection(env, endpoint))
    return _with_tuning(env, _flight_sql_connection(env, endpoint))


def _direct_connection(env: Mapping[str, str], catalog: str) -> DuckLakeSinkConfig:
    catalog = catalog.removeprefix("ducklake:")
    data_path = _text(env, "DATA_PATH")
    if data_path is None:
        raise _Invalid(
            f"missing required ducklake sink settings: {_PREFIX}DATA_PATH (where DuckLake writes Parquet files)"
        )
    quack_token = _text(env, "QUACK_TOKEN")
    if catalog.startswith("quack:") and quack_token is None:
        raise _Invalid(f"missing required ducklake sink settings: {_PREFIX}QUACK_TOKEN (for catalog {catalog!r})")
    s3_key_id = _text(env, "S3_KEY_ID")
    s3_secret = _text(env, "S3_SECRET")
    if (s3_key_id is None) != (s3_secret is None):
        missing = "S3_SECRET" if s3_secret is None else "S3_KEY_ID"
        raise _Invalid(
            f"missing required ducklake sink settings: {_PREFIX}{missing} "
            f"({_PREFIX}S3_KEY_ID and {_PREFIX}S3_SECRET go together)"
        )
    s3_url_style = _text(env, "S3_URL_STYLE")
    if s3_url_style is not None and s3_url_style not in ("vhost", "path"):
        raise _Invalid(f"{_PREFIX}S3_URL_STYLE must be vhost or path, got {s3_url_style!r}")
    return DuckLakeSinkConfig(
        catalog=catalog,
        data_path=data_path,
        quack_token=quack_token,
        s3_key_id=s3_key_id,
        s3_secret=s3_secret,
        s3_region=_text(env, "S3_REGION"),
        s3_endpoint=_text(env, "S3_ENDPOINT"),
        s3_url_style=s3_url_style,
        s3_use_ssl=_flag(env, "S3_USE_SSL", True),
    )


def _quack_endpoint_connection(env: Mapping[str, str], endpoint: str) -> DuckLakeSinkConfig:
    quack_token = _text(env, "QUACK_TOKEN")
    username = _text(env, "USERNAME")
    password = _text(env, "PASSWORD")
    if quack_token is None and (username is None or password is None):
        raise _Invalid(
            f"missing required ducklake sink settings: {_PREFIX}USERNAME and {_PREFIX}PASSWORD (QoD), "
            f"or {_PREFIX}QUACK_TOKEN (a DuckDB quack_serve server), for endpoint {endpoint!r}"
        )
    return DuckLakeSinkConfig(
        endpoint=endpoint, username=username or "", password=password or "", quack_token=quack_token
    )


def _flight_sql_connection(env: Mapping[str, str], endpoint: str | None) -> DuckLakeSinkConfig:
    if endpoint is None:
        raise _Invalid(
            f"missing required ducklake sink settings: {_PREFIX}CATALOG (write to DuckLake directly) "
            f"or {_PREFIX}ENDPOINT (write through a Quack or Flight SQL server)"
        )
    username = _text(env, "USERNAME")
    password = _text(env, "PASSWORD")
    if username is None or password is None:
        missing = ", ".join(
            _PREFIX + name for name, value in (("USERNAME", username), ("PASSWORD", password)) if value is None
        )
        raise _Invalid(f"missing required ducklake sink settings: {missing}")
    return DuckLakeSinkConfig(endpoint=endpoint, username=username, password=password)


def _with_tuning(env: Mapping[str, str], connection: DuckLakeSinkConfig) -> DuckLakeSinkConfig:
    return replace(
        connection,
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
