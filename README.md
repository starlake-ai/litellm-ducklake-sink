# litellm-ducklake-sink

[![PyPI](https://img.shields.io/pypi/v/litellm-ducklake-sink)](https://pypi.org/project/litellm-ducklake-sink/)
[![Python](https://img.shields.io/pypi/pyversions/litellm-ducklake-sink)](https://pypi.org/project/litellm-ducklake-sink/)
[![License](https://img.shields.io/pypi/l/litellm-ducklake-sink)](LICENSE)
[![Release](https://github.com/starlake-ai/litellm-ducklake-sink/actions/workflows/publish.yml/badge.svg)](https://github.com/starlake-ai/litellm-ducklake-sink/actions/workflows/publish.yml)

An out-of-tree LiteLLM proxy logging callback that batches request telemetry and appends it to DuckLake tables through an Arrow Flight SQL endpoint. Metrics land in `llm_requests`, optional payloads in `llm_payloads`, both partitioned by day and queryable with SQL the moment they land.

It works with any DuckLake database exposed through an Arrow Flight SQL server (for example QoD - quack-on-demand). The SQL it emits is plain `CREATE TABLE IF NOT EXISTS`, `INSERT ... VALUES`, and `DELETE`, plus DuckLake's `ALTER TABLE ... SET PARTITIONED BY` at bootstrap, so the backend must be DuckDB with the DuckLake extension, but nothing in the write path is specific to one server. A few optional settings integrate with QoD deployments and are marked as such below.

## Install

```bash
pip install litellm-ducklake-sink
```

Install it into the same environment that runs the LiteLLM proxy.

## Configure

Reference the callback instance in the proxy YAML:

```yaml
litellm_settings:
  callbacks: litellm_ducklake_sink.callback.instance
```

All settings come from `DUCKLAKE_SINK_*` environment variables.

| Setting | Env var | Default |
|---|---|---|
| endpoint | DUCKLAKE_SINK_ENDPOINT | required, e.g. `grpc+tls://host:31338` |
| username / password | DUCKLAKE_SINK_USERNAME / _PASSWORD | required |
| schema_name | DUCKLAKE_SINK_SCHEMA_NAME | `main` |
| tls_skip_verify | DUCKLAKE_SINK_TLS_SKIP_VERIFY | false |
| enabled | DUCKLAKE_SINK_ENABLED | true |
| capture_payloads | DUCKLAKE_SINK_CAPTURE_PAYLOADS | false |
| batch_rows | DUCKLAKE_SINK_BATCH_ROWS | 1000 |
| batch_interval | DUCKLAKE_SINK_BATCH_INTERVAL | 10 seconds |
| batch_max_bytes | DUCKLAKE_SINK_BATCH_MAX_BYTES | 2 MiB, early-flush guard |
| payload_max_bytes | DUCKLAKE_SINK_PAYLOAD_MAX_BYTES | 1 MiB per text field, truncation |
| spool_dir | DUCKLAKE_SINK_SPOOL_DIR | `<tempdir>/litellm_ducklake_spool` |
| spool_max_bytes | DUCKLAKE_SINK_SPOOL_MAX_BYTES | 512 MiB |
| drain_timeout | DUCKLAKE_SINK_DRAIN_TIMEOUT | 10 seconds |
| flush_max_attempts | DUCKLAKE_SINK_FLUSH_MAX_ATTEMPTS | 5 (backoff 1s, 2s, 4s... capped 30s) |
| retention_days | DUCKLAKE_SINK_RETENTION_DAYS | 30 |

### QoD-only settings

These settings only apply when the endpoint is a QoD (quack-on-demand) gateway. Leave them all unset on any other Flight SQL server.

| Setting | Env var | Default |
|---|---|---|
| tenant / pool | DUCKLAKE_SINK_TENANT / _POOL | unset; sent as QoD routing headers on every connection |
| tenant_db | DUCKLAKE_SINK_TENANT_DB | unset; needed only for the maintenance trigger |
| maintenance_url / api key | DUCKLAKE_SINK_MAINTENANCE_URL / _MAINTENANCE_API_KEY | unset; QoD manager REST endpoint for the maintenance trigger |

### Startup and failure behavior

The callback instance is created when the proxy imports it at startup, and misconfiguration fails fast: if the sink is enabled but a required setting is missing or a value cannot be parsed, the proxy refuses to boot rather than silently dropping telemetry. Set `DUCKLAKE_SINK_ENABLED=false` to keep the callback referenced in `config.yaml` but inert; no other settings are required in that case.

An unreachable or wrong endpoint does not block startup. Connections are opened lazily at first flush, so with bad connectivity the proxy serves traffic normally while batches spool to disk and replay once the endpoint is reachable.

### Table creation

No manual DDL is needed. On the first flush of each process the sink runs `CREATE TABLE IF NOT EXISTS` for `llm_requests` and `llm_payloads` in `schema_name`, then `ALTER TABLE ... SET PARTITIONED BY (request_day)` on each, before the first insert. The schema itself must already exist (`main` always does). The connecting user therefore needs DDL and write privileges on that schema; under QoD with ACL enabled, grant `CREATE`/`ALTER` alongside `INSERT` (and `DELETE` for the retention job).

### Delivery semantics

Delivery is at-least-once, not exactly-once. Duplicates are possible when a drain times out, the process is killed mid-flush, or several proxy workers replay spooled batches concurrently after a crash. Exact spend accounting downstream should deduplicate by `request_id`.

The disk spool lives at `spool_dir` and is capped at `spool_max_bytes`; once full, the oldest batches are evicted first, and evicted batches are dropped by design rather than retried.

Replay of spooled batches runs on the periodic flush cycle, which starts on the first logged request after process start rather than immediately at boot. A proxy that restarts and then sits idle only replays its spool once it handles its first request.

Multi-worker deployments share one spool safely: workers claim a batch before replaying it, so the same batch is never replayed twice, and a claim left behind by a worker that crashes mid-replay is automatically recovered by another worker after 15 minutes.

## Retention

```bash
python -m litellm_ducklake_sink.retention
```

Run it out-of-band (cron or a Kubernetes CronJob). It deletes rows older than `DUCKLAKE_SINK_RETENTION_DAYS`.

Deletes in DuckLake are logical: old snapshots still reference the deleted rows' Parquet files until snapshot expiry and file cleanup run. On QoD, set `DUCKLAKE_SINK_MAINTENANCE_URL`, `DUCKLAKE_SINK_TENANT`, and `DUCKLAKE_SINK_TENANT_DB` and the retention job triggers QoD's managed maintenance chain after the deletes so the files are physically removed. On any other DuckLake setup, run `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` yourself on whatever schedule your deployment uses.
