from __future__ import annotations

import math
from datetime import UTC, date, datetime
from typing import assert_never

from litellm_ducklake_sink.types import DuckLakePayloadRow, DuckLakeRequestRow

SqlValue = bool | int | float | str | date | datetime | None

REQUEST_COLUMNS: tuple[str, ...] = (
    "request_id",
    "request_day",
    "request_ts",
    "api_key_hash",
    "api_key_alias",
    "team_id",
    "end_user_id",
    "model_group",
    "deployment_model",
    "provider",
    "prompt_tokens",
    "completion_tokens",
    "cached_tokens",
    "spend",
    "latency_ms",
    "time_to_first_token_ms",
    "cache_hit",
    "status",
    "error_type",
    "client_source",
)

PAYLOAD_COLUMNS: tuple[str, ...] = ("request_id", "request_day", "messages", "response", "payload_bytes")

_REQUESTS_DDL = (
    "CREATE TABLE IF NOT EXISTS {schema}.llm_requests ("
    "request_id VARCHAR, request_day DATE, request_ts TIMESTAMP, api_key_hash VARCHAR, "
    "api_key_alias VARCHAR, team_id VARCHAR, end_user_id VARCHAR, model_group VARCHAR, "
    "deployment_model VARCHAR, provider VARCHAR, prompt_tokens BIGINT, completion_tokens BIGINT, "
    "cached_tokens BIGINT, spend DOUBLE, latency_ms DOUBLE, time_to_first_token_ms DOUBLE, "
    "cache_hit BOOLEAN, status VARCHAR, error_type VARCHAR, client_source VARCHAR)"
)

_PAYLOADS_DDL = (
    "CREATE TABLE IF NOT EXISTS {schema}.llm_payloads ("
    "request_id VARCHAR, request_day DATE, messages VARCHAR, response VARCHAR, payload_bytes BIGINT)"
)


def ddl_statements(schema_name: str) -> tuple[str, ...]:
    return (
        _REQUESTS_DDL.format(schema=schema_name),
        f"ALTER TABLE {schema_name}.llm_requests SET PARTITIONED BY (request_day)",
        _PAYLOADS_DDL.format(schema=schema_name),
        f"ALTER TABLE {schema_name}.llm_payloads SET PARTITIONED BY (request_day)",
    )


def sql_literal(value: SqlValue) -> str:
    match value:
        case None:
            return "NULL"
        case bool():
            return "TRUE" if value else "FALSE"
        case int():
            return str(value)
        case float():
            return repr(value) if math.isfinite(value) else "NULL"
        case datetime():
            utc = value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
            return f"TIMESTAMP '{utc.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        case date():
            return f"DATE '{value.isoformat()}'"
        case str():
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        case _:
            assert_never(value)


def render_insert(
    schema_name: str,
    table: str,
    columns: tuple[str, ...],
    value_rows: tuple[tuple[SqlValue, ...], ...],
) -> str:
    rendered = ", ".join("(" + ", ".join(sql_literal(v) for v in row) + ")" for row in value_rows)
    return f"INSERT INTO {schema_name}.{table} ({', '.join(columns)}) VALUES {rendered}"


def request_row_values(row: DuckLakeRequestRow) -> tuple[SqlValue, ...]:
    return (
        row.request_id,
        row.request_day,
        row.request_ts,
        row.api_key_hash,
        row.api_key_alias,
        row.team_id,
        row.end_user_id,
        row.model_group,
        row.deployment_model,
        row.provider,
        row.prompt_tokens,
        row.completion_tokens,
        row.cached_tokens,
        row.spend,
        row.latency_ms,
        row.time_to_first_token_ms,
        row.cache_hit,
        row.status,
        row.error_type,
        row.client_source,
    )


def payload_row_values(row: DuckLakePayloadRow) -> tuple[SqlValue, ...]:
    return (row.request_id, row.request_day, row.messages, row.response, row.payload_bytes)
