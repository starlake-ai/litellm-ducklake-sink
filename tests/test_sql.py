from datetime import UTC, date, datetime

from litellm_ducklake_sink.sql import (
    PAYLOAD_COLUMNS,
    REQUEST_COLUMNS,
    ddl_statements,
    render_insert,
    request_row_values,
    sql_literal,
)
from litellm_ducklake_sink.types import DuckLakeRequestRow


def test_sql_literal_escapes_quotes_and_handles_scalars():
    assert sql_literal("it's a 'test'") == "'it''s a ''test'''"
    assert sql_literal(None) == "NULL"
    assert sql_literal(True) == "TRUE"
    assert sql_literal(False) == "FALSE"
    assert sql_literal(42) == "42"
    assert sql_literal(1.5) == "1.5"
    assert sql_literal(float("nan")) == "NULL"
    assert sql_literal(float("inf")) == "NULL"
    assert sql_literal(date(2026, 8, 4)) == "DATE '2026-08-04'"
    assert sql_literal(datetime(2026, 8, 4, 12, 30, 0, 123456, tzinfo=UTC)) == (
        "TIMESTAMP '2026-08-04 12:30:00.123456'"
    )


def test_render_insert_is_one_statement_with_all_rows():
    statement = render_insert("main", "llm_requests", ("a", "b"), ((1, "x"), (2, None)))
    assert statement == "INSERT INTO main.llm_requests (a, b) VALUES (1, 'x'), (2, NULL)"


def test_request_row_values_align_with_columns():
    row = DuckLakeRequestRow(
        request_id="req-1",
        request_day=date(2026, 8, 4),
        request_ts=datetime(2026, 8, 4, 12, 0, 0, tzinfo=UTC),
        api_key_hash="hash",
        api_key_alias="alias",
        team_id="team",
        end_user_id="user",
        model_group="claude",
        deployment_model="claude-fable-5",
        provider="anthropic",
        prompt_tokens=10,
        completion_tokens=20,
        cached_tokens=5,
        spend=0.01,
        latency_ms=1200.0,
        time_to_first_token_ms=300.0,
        cache_hit=False,
        status="success",
        error_type=None,
        client_source="claude-code/2.0",
    )
    assert len(REQUEST_COLUMNS) == 20
    assert len(request_row_values(row)) == len(REQUEST_COLUMNS)
    assert request_row_values(row)[0] == "req-1"


def test_ddl_creates_both_tables_partitioned_by_day():
    statements = ddl_statements("main")
    assert any("CREATE TABLE IF NOT EXISTS main.llm_requests" in s for s in statements)
    assert any("CREATE TABLE IF NOT EXISTS main.llm_payloads" in s for s in statements)
    assert sum("SET PARTITIONED BY (request_day)" in s for s in statements) == 2
    assert not any("messages" in s for s in statements if "llm_requests" in s)
    assert len(PAYLOAD_COLUMNS) == 5
