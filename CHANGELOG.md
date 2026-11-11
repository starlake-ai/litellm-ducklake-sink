# Changelog

All notable changes to litellm-ducklake-sink are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/). While the version is below 1.0, a minor release (0.x.0) may contain breaking changes.

## [Unreleased]

## [0.2.0] - 2026-11-11

### Breaking changes

Existing Flight SQL setups (`DUCKLAKE_SINK_ENDPOINT=grpc://...` or `grpc+tls://...`) keep working without changes. Check the items below if you use any of these:

- **Retention job removed.** `python -m litellm_ducklake_sink.retention` no longer exists, and `DUCKLAKE_SINK_RETENTION_DAYS` is ignored. The sink now only appends; expiring old rows is the lake host's job. On QoD, configure it on the QoD side. With a SQLite or Postgres catalog, schedule the SQL from the README's [Expiring old rows](README.md#expiring-old-rows) section (`DELETE` by `request_day`, then `ducklake_expire_snapshots` and `ducklake_cleanup_old_files`).
- **QoD maintenance trigger removed.** `DUCKLAKE_SINK_TENANT_DB`, `DUCKLAKE_SINK_MAINTENANCE_URL` and `DUCKLAKE_SINK_MAINTENANCE_API_KEY` are ignored. Configure maintenance policies on the QoD side (`qod maintenance`).
- **New required dependency: `duckdb>=2.0.0`.** Environments that pin an older `duckdb` must upgrade it before installing 0.2.0. `httpx` is no longer a dependency.
- **New exception type and message.** A misconfigured sink now raises `DuckLakeSinkMisconfiguredError`. It subclasses `ValueError`, so existing `except ValueError` handlers still catch it. The message changed from `ducklake sink misconfigured: ...` to `litellm-ducklake-sink is misconfigured: ...`, so update any alert or log filter that matches the old text.
- **Python API.** `DuckLakeSinkConfig` no longer has `retention_days`, `tenant_db`, `maintenance_url` or `maintenance_api_key`, and `litellm_ducklake_sink.retention` can no longer be imported.
- **`.env.example` removed.** Use one of [`examples/ducklake-sqlite.env`](examples/ducklake-sqlite.env), [`examples/ducklake-postgres.env`](examples/ducklake-postgres.env) or [`examples/qod.env`](examples/qod.env).

### Added

- **Write to DuckLake directly**, with no server: `DUCKLAKE_SINK_CATALOG` (`sqlite:`, `postgres:`, `quack:`, or a `.ducklake` file) plus `DUCKLAKE_SINK_DATA_PATH`. The sink embeds DuckDB and attaches the lake in the proxy process. The folder of a local catalog file is created on first write.
- **Quack endpoints:** `DUCKLAKE_SINK_ENDPOINT=quack:host:port` for a QoD gateway (token built from the username, password, tenant and pool) or a DuckDB `quack_serve` server (`DUCKLAKE_SINK_QUACK_TOKEN`).
- **S3 data paths:** an `s3://` `DUCKLAKE_SINK_DATA_PATH`, with `DUCKLAKE_SINK_S3_KEY_ID` and `_S3_SECRET`, or the AWS credential chain. `_S3_REGION`, `_S3_ENDPOINT`, `_S3_URL_STYLE` and `_S3_USE_SSL` cover S3-compatible stores such as MinIO or R2.
- **Example env files** in `examples/`, one per setup, each spelling out every default.
- **A 5-minute quickstart** in the README: SQLite catalog, mock model, no server.

### Changed

- **Clear startup error.** A misconfigured sink logs one `ERROR` line that names the setting at fault and the fix (`DUCKLAKE_SINK_ENABLED=false`) before the proxy refuses to boot.
- **Mode conflicts are rejected.** Setting both `DUCKLAKE_SINK_CATALOG` and `DUCKLAKE_SINK_ENDPOINT` stops the proxy at startup, and each mode names exactly the settings it is missing.

## [0.1.2] - 2026-08-09

### Fixed

- **`DUCKLAKE_SINK_ENABLED=false` is a true kill switch.** It needs no other setting, ignores stale or unparseable ones, and a disabled sink creates no spool directory, no connection and no shutdown hook.
- **The retention job exits 0** with a clear message when the sink is disabled, instead of failing to connect.
- **An empty `DUCKLAKE_SINK_ENABLED=` is rejected** as malformed instead of silently meaning enabled.
- **Integer settings reject non-ASCII digits** with a configuration error instead of an unhandled `ValueError`.

### Changed

- **Container guidance.** The README recommends a persistent volume for the spool in container setups.
- **GitHub releases.** CI creates the release automatically after publishing to PyPI.

## [0.1.1] - 2026-08-07

### Added

- **First release:** a LiteLLM proxy logging callback that batches request telemetry and appends it to DuckLake tables (`llm_requests`, optional `llm_payloads`, partitioned by day) through an Arrow Flight SQL endpoint, with a disk spool and replay for failed batches, optional QoD settings, a retention job, and a demo.

[Unreleased]: https://github.com/starlake-ai/litellm-ducklake-sink/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/starlake-ai/litellm-ducklake-sink/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/starlake-ai/litellm-ducklake-sink/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/starlake-ai/litellm-ducklake-sink/releases/tag/v0.1.1
