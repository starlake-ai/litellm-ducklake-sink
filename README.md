# litellm-ducklake-sink

[![PyPI](https://img.shields.io/pypi/v/litellm-ducklake-sink)](https://pypi.org/project/litellm-ducklake-sink/)
[![Python](https://img.shields.io/pypi/pyversions/litellm-ducklake-sink)](https://pypi.org/project/litellm-ducklake-sink/)
[![License](https://img.shields.io/pypi/l/litellm-ducklake-sink)](LICENSE)
[![Release](https://github.com/starlake-ai/litellm-ducklake-sink/actions/workflows/publish.yml/badge.svg)](https://github.com/starlake-ai/litellm-ducklake-sink/actions/workflows/publish.yml)

A LiteLLM proxy logging callback that records every request in a DuckLake lakehouse: model, tokens, spend, latency, status, team and key, plus prompts and responses if you want them. Rows land in `llm_requests` and `llm_payloads`, partitioned by day, and you query them with plain SQL from any DuckDB.

LiteLLM can write to DuckLake directly. The sink embeds DuckDB, so a SQLite file and a folder are all you need to start, with no server to run. Production deployments point the same sink at a Postgres catalog, at a DuckDB server over the Quack protocol, or at an Arrow Flight SQL server such as QoD (quack-on-demand).

## Quickstart: LiteLLM to DuckLake in 5 minutes

You need Python 3.11+ and nothing else: no database server, no provider API key. The model below is a mock, so every request succeeds offline.

**1. Install** LiteLLM's proxy and the sink in a fresh folder:

```bash
mkdir llm-lake && cd llm-lake
python3 -m venv .venv && . .venv/bin/activate
pip install 'litellm[proxy]' litellm-ducklake-sink
```

**2. Configure.** Download the sink's settings, which come with every default filled in and point at a local lake in `./lake/`, and add a master key for the proxy:

```bash
curl -sSLo .env https://raw.githubusercontent.com/starlake-ai/litellm-ducklake-sink/main/examples/ducklake-sqlite.env
echo "LITELLM_MASTER_KEY=sk-$(openssl rand -hex 32)" >> .env
```

Then create `config.yaml`, the complete proxy configuration:

```yaml
model_list:
  - model_name: demo
    litellm_params:
      model: openai/demo
      api_key: not-used
      mock_response: "Hello from LiteLLM"   # remove this line to call a real model

general_settings:
  master_key: os.environ/LITELLM_MASTER_KEY

litellm_settings:
  callbacks: litellm_ducklake_sink.callback.instance
```

**3. Start the proxy.** LiteLLM looks for a `.env` above its own install folder, not in the folder you start it from, so load the file into the shell explicitly:

```bash
set -a; . ./.env; set +a
litellm --config config.yaml
```

**4. Send a few requests** from a second terminal in the same folder:

```bash
cd llm-lake && set -a; . ./.env; set +a
for i in 1 2 3; do
  curl -s http://localhost:4000/v1/chat/completions \
    -H "Authorization: Bearer $LITELLM_MASTER_KEY" -H 'Content-Type: application/json' \
    -d "{\"model\": \"demo\", \"messages\": [{\"role\": \"user\", \"content\": \"hello $i\"}]}"
  echo
done
```

**5. Query the lake.** The sink flushes every 10 seconds. After that, query from the same terminal with the `duckdb` package the sink installed. The proxy can keep running:

```bash
.venv/bin/python - <<'EOF'
import duckdb
duckdb.sql("ATTACH 'ducklake:sqlite:lake/catalog.sqlite' AS lake (READ_ONLY)")
duckdb.sql("""
  SELECT model_group, count(*) AS requests,
         sum(prompt_tokens + completion_tokens) AS tokens,
         round(sum(spend), 4) AS spend,
         round(quantile_cont(latency_ms, 0.95)) AS p95_ms
  FROM lake.main.llm_requests
  GROUP BY model_group ORDER BY spend DESC
""").show()
EOF
```

```
┌─────────────┬──────────┬────────┬────────┬────────┐
│ model_group │ requests │ tokens │ spend  │ p95_ms │
├─────────────┼──────────┼────────┼────────┼────────┤
│ demo        │        3 │     90 │    0.0 │    4.0 │
└─────────────┴──────────┴────────┴────────┴────────┘
```

The DuckDB CLI works the same way: `duckdb -c "ATTACH 'ducklake:sqlite:lake/catalog.sqlite' AS lake (READ_ONLY); FROM lake.main.llm_requests"`. Spend is 0 because the model is a mock; with a real model, LiteLLM's cost tracking fills it in. To store prompts and responses too, set `DUCKLAKE_SINK_CAPTURE_PAYLOADS=true` in `.env` and query `lake.main.llm_payloads`.

Small batches show up in `lake/catalog.sqlite` before any Parquet file appears in `lake/data/`. DuckLake inlines small inserts into the catalog and moves them to Parquet once they grow, and queries see both.

**Going further.** Everything is configured through `.env`; the sections below explain each setting. [`examples/`](examples/) has a complete env file per setup, each with every default spelled out:

| File | Setup |
|---|---|
| [`ducklake-sqlite.env`](examples/ducklake-sqlite.env) | DuckLake with a SQLite catalog, no server: the quickstart |
| [`ducklake-postgres.env`](examples/ducklake-postgres.env) | DuckLake with a Postgres catalog, shared by several proxy replicas |
| [`qod.env`](examples/qod.env) | A QoD gateway over Arrow Flight SQL (or Quack), with the demo's values |

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

All settings come from `DUCKLAKE_SINK_*` environment variables. The files in [`examples/`](examples/) list every one of them with its default, and the proxy needs them in its environment (for example `set -a; . ./.env; set +a` before `litellm`). LiteLLM also loads a `.env` it finds in a folder above its own install, such as the folder holding its `.venv`, and those values fill in any setting your shell leaves unset. Keep one `.env` per setup, or a stale `DUCKLAKE_SINK_CATALOG` from one file can meet the `DUCKLAKE_SINK_ENDPOINT` of another; the sink then refuses to start and names both settings.

### Where telemetry lands

Set `DUCKLAKE_SINK_CATALOG` or `DUCKLAKE_SINK_ENDPOINT`, never both.

| You have | Set | Notes |
|---|---|---|
| Nothing yet, one host | `CATALOG=sqlite:lake/catalog.sqlite`, `DATA_PATH=lake/data/` | The quickstart. Several processes on one host can write, and readers can query while the proxy runs. |
| Several proxy replicas | `CATALOG=postgres:dbname=lake host=db user=lake password=...`, `DATA_PATH=<shared folder>` | Any libpq connection string after `postgres:`. |
| A DuckDB server hosting the catalog | `CATALOG=quack:host:9494`, `QUACK_TOKEN=<token printed by quack_serve>`, `DATA_PATH=<shared folder>` | The server runs `CALL quack_serve('quack:host:9494', token = '...')` with the ducklake extension loaded. |
| A DuckDB file catalog | `CATALOG=lake/metadata.ducklake`, `DATA_PATH=lake/data/` | One process only: the proxy locks the file while it runs, so readers cannot attach meanwhile. |
| A QoD gateway | `ENDPOINT=grpc+tls://host:31338`, plus the [QoD settings](#qod-settings) | Through QoD's Arrow Flight SQL edge. QoD's native Quack front door (`quack:host:9494`) works too. |
| A DuckDB server with the lake attached | `ENDPOINT=quack:host:port`, `QUACK_TOKEN=<token>` | The sink sends its SQL to that server over Quack. |
| Any Arrow Flight SQL server | `ENDPOINT=grpc+tls://host:port`, `USERNAME`, `PASSWORD` | The server must run DuckDB with the lake attached. |

In the table, `CATALOG` stands for `DUCKLAKE_SINK_CATALOG`, and so on for the other names. Quote a catalog value that contains spaces, such as a Postgres connection string, so the shell keeps it whole. DuckLake records `DATA_PATH` in the catalog on the first write and refuses a different value afterwards, so keep it stable, and make it absolute when proxies start from different folders. With a catalog, the sink attaches DuckLake inside the proxy process and writes the Parquet files itself, and DuckDB downloads the extensions it needs (`ducklake`, `sqlite`, `postgres`, `quack`) on first use. The SQL the sink emits is plain `CREATE TABLE IF NOT EXISTS`, `INSERT ... VALUES`, and `DELETE`, plus DuckLake's `ALTER TABLE ... SET PARTITIONED BY` at bootstrap, so any server in the table works as long as it runs DuckDB with the DuckLake extension.

### All settings

| Setting | Env var | Default |
|---|---|---|
| catalog | DUCKLAKE_SINK_CATALOG | unset; a DuckLake catalog, see the table above |
| data_path | DUCKLAKE_SINK_DATA_PATH | required with a catalog; folder for the Parquet files |
| endpoint | DUCKLAKE_SINK_ENDPOINT | unset; `quack:host:port`, `grpc://host:port` or `grpc+tls://host:port` |
| username / password | DUCKLAKE_SINK_USERNAME / _PASSWORD | required with a Flight SQL endpoint or a QoD Quack endpoint |
| quack_token | DUCKLAKE_SINK_QUACK_TOKEN | required with a `quack:` catalog or a non-QoD `quack:` endpoint |
| schema_name | DUCKLAKE_SINK_SCHEMA_NAME | `main` |
| tls_skip_verify | DUCKLAKE_SINK_TLS_SKIP_VERIFY | false; Flight SQL only |
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

### QoD settings

These settings only apply when the endpoint is a QoD (quack-on-demand) gateway. Leave them all unset for anything else.

| Setting | Env var | Default | Value for `uvx qod start --demo` |
|---|---|---|---|
| endpoint | DUCKLAKE_SINK_ENDPOINT | unset | `grpc+tls://localhost:31338` (Flight SQL), or `quack:localhost:9494` (Quack) |
| username / password | DUCKLAKE_SINK_USERNAME / _PASSWORD | unset | `acme-admin` / `demo-acme-admin` |
| tenant / pool | DUCKLAKE_SINK_TENANT / _POOL | unset; route every statement to that tenant and pool | `acme` / `bi` |
| schema_name | DUCKLAKE_SINK_SCHEMA_NAME | `main` | `tpch1` |
| tls_skip_verify | DUCKLAKE_SINK_TLS_SKIP_VERIFY | false | `true` with Flight SQL (self-signed certificate); not used over Quack |
| tenant_db | DUCKLAKE_SINK_TENANT_DB | unset; needed only for the maintenance trigger | `acme_tpch` |
| maintenance_url / api key | DUCKLAKE_SINK_MAINTENANCE_URL / _MAINTENANCE_API_KEY | unset; QoD manager REST endpoint for the maintenance trigger | `http://localhost:20900` / unset |

[`examples/qod.env`](examples/qod.env) sets all of these values for the demo gateway, over Flight SQL. To use Quack instead, set the endpoint to `quack:localhost:9494`: the sink builds QoD's token from the username, password, tenant and pool (`tenant=acme&pool=bi&user=...&password=...`, URL-encoded). QoD's Quack listener speaks plain HTTP unless the gateway sets `QOD_QUACK_TLS_ENABLED=true`, and DuckDB uses TLS for any host other than `localhost`, so enable TLS on a remote gateway.

### Startup and failure behavior

The callback instance is created when the proxy imports it at startup, and misconfiguration fails fast: if the sink is enabled but a required setting is missing or a value cannot be parsed, the proxy refuses to boot rather than silently dropping telemetry. The sink logs one `ERROR` line naming the offending setting and the way out, then raises `DuckLakeSinkMisconfiguredError` (a `ValueError`); LiteLLM follows it with its own startup traceback, whose last line repeats the same message. Set `DUCKLAKE_SINK_ENABLED=false` to keep the callback referenced in `config.yaml` but fully inert: every other setting is ignored, nothing is validated, and no spool directory, connection, or shutdown hook is created. An explicitly empty value (`DUCKLAKE_SINK_ENABLED=`) is rejected as malformed rather than silently falling back to enabled.

An unreachable or wrong endpoint or catalog does not block startup. Connections are opened lazily at first flush, so with bad connectivity the proxy serves traffic normally while batches spool to disk and replay on later flush cycles once the endpoint is reachable. Batches spooled before the sink is disabled stay on disk untouched until it is enabled again.

### Table creation

No manual DDL is needed. On the first flush of each process the sink runs `CREATE TABLE IF NOT EXISTS` for `llm_requests` and `llm_payloads` in `schema_name`, then `ALTER TABLE ... SET PARTITIONED BY (request_day)` on each, before the first insert. The schema itself must already exist (`main` always does). The connecting user therefore needs DDL and write privileges on that schema; under QoD with ACL enabled, grant `CREATE`/`ALTER` alongside `INSERT` (and `DELETE` for the retention job).

### Delivery semantics

Delivery is at-least-once, not exactly-once. Duplicates are possible when a drain times out, the process is killed mid-flush, or several proxy workers replay spooled batches concurrently after a crash. Exact spend accounting downstream should deduplicate by `request_id`.

The disk spool lives at `spool_dir` and is capped at `spool_max_bytes`; once full, the oldest batches are evicted first, and evicted batches are dropped by design rather than retried.

The default spool location under the system tempdir is ephemeral in most container setups: a pod restart discards any batches spooled during an outage. Point `DUCKLAKE_SINK_SPOOL_DIR` at a persistent volume when spooled telemetry must survive restarts. Workers on the same host share one spool safely; separate proxy replicas each keep their own.

Replay of spooled batches runs on the periodic flush cycle, which starts on the first logged request after process start rather than immediately at boot. A proxy that restarts and then sits idle only replays its spool once it handles its first request.

Multi-worker deployments share one spool safely: workers claim a batch before replaying it, so the same batch is never replayed twice, and a claim left behind by a worker that crashes mid-replay is automatically recovered by another worker after 15 minutes.

## Retention

```bash
python -m litellm_ducklake_sink.retention
```

Run it out-of-band (cron or a Kubernetes CronJob). It deletes rows older than `DUCKLAKE_SINK_RETENTION_DAYS`. When `DUCKLAKE_SINK_ENABLED=false` it exits 0 without connecting, so a shared env file disables the proxy sink and the retention job together.

Deletes in DuckLake are logical: old snapshots still reference the deleted rows' Parquet files until snapshot expiry and file cleanup run. On QoD, set `DUCKLAKE_SINK_MAINTENANCE_URL`, `DUCKLAKE_SINK_TENANT`, and `DUCKLAKE_SINK_TENANT_DB` and the retention job triggers QoD's managed maintenance chain after the deletes so the files are physically removed. On any other DuckLake setup, run `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` yourself on whatever schedule your deployment uses.
