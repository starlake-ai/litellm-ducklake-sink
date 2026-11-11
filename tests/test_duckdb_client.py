from datetime import date
from pathlib import Path

import duckdb
import pytest

from litellm_ducklake_sink.config import DuckLakeSinkConfig, resolve_ducklake_config
from litellm_ducklake_sink.duckdb_client import DuckDBExecutor, build_executor
from litellm_ducklake_sink.ducklake_logger import DuckLakeLogger
from litellm_ducklake_sink.flight_client import AdbcFlightSqlExecutor
from litellm_ducklake_sink.retention import run_retention
from tests.test_ducklake_logger import _event_kwargs


def _direct_config(tmp_path: Path, **overrides: str) -> DuckLakeSinkConfig:
    env = {
        "DUCKLAKE_SINK_CATALOG": f"sqlite:{tmp_path / 'catalog.sqlite'}",
        "DUCKLAKE_SINK_DATA_PATH": str(tmp_path / "data") + "/",
        "DUCKLAKE_SINK_SPOOL_DIR": str(tmp_path / "spool"),
        "DUCKLAKE_SINK_FLUSH_MAX_ATTEMPTS": "1",
    }
    env.update(overrides)
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeSinkConfig)
    return result


def _read(tmp_path: Path, sql: str) -> list:
    # A separate DuckDB, like an analyst's CLI, reading while the sink stays attached.
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH 'ducklake:sqlite:{tmp_path / 'catalog.sqlite'}' AS lake (READ_ONLY)")
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_build_executor_picks_mode(tmp_path: Path):
    assert isinstance(build_executor(_direct_config(tmp_path)), DuckDBExecutor)
    flight = resolve_ducklake_config(
        {
            "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
            "DUCKLAKE_SINK_USERNAME": "u",
            "DUCKLAKE_SINK_PASSWORD": "p",
        }
    )
    assert isinstance(flight, DuckLakeSinkConfig)
    assert isinstance(build_executor(flight), AdbcFlightSqlExecutor)


def test_executor_creates_the_lake_and_counts_rows(tmp_path: Path):
    executor = DuckDBExecutor(_direct_config(tmp_path))
    try:
        executor.execute_script("CREATE TABLE IF NOT EXISTS main.t (i INTEGER)")
        assert executor.execute("INSERT INTO main.t VALUES (1), (2), (3)") == 3
        assert executor.execute("DELETE FROM main.t WHERE i < 3") == 2
        assert _read(tmp_path, "SELECT i FROM lake.main.t") == [(3,)]
    finally:
        executor.close()


def test_executor_reattaches_an_existing_lake(tmp_path: Path):
    config = _direct_config(tmp_path)
    first = DuckDBExecutor(config)
    first.execute_script("CREATE TABLE IF NOT EXISTS main.t (i INTEGER)")
    first.execute("INSERT INTO main.t VALUES (1)")
    first.close()
    second = DuckDBExecutor(config)
    try:
        assert second.execute("INSERT INTO main.t VALUES (2)") == 1
    finally:
        second.close()
    assert _read(tmp_path, "SELECT count(*) FROM lake.main.t") == [(2,)]


def test_executor_quotes_paths(tmp_path: Path):
    quoted = tmp_path / "it's here"
    quoted.mkdir()
    executor = DuckDBExecutor(_direct_config(quoted))
    try:
        executor.execute_script("CREATE TABLE IF NOT EXISTS main.t (i INTEGER)")
        assert executor.execute("INSERT INTO main.t VALUES (1)") == 1
    finally:
        executor.close()


@pytest.mark.asyncio
async def test_logger_writes_requests_and_payloads_into_ducklake(tmp_path: Path):
    config = _direct_config(tmp_path, DUCKLAKE_SINK_CAPTURE_PAYLOADS="true")
    logger = DuckLakeLogger(config=config, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.async_log_success_event(_event_kwargs("r2"), None, None, None)
    await logger.flush_queue()
    assert logger.counters.rows_written == 2
    assert logger.counters.flushes_failed == 0
    assert _read(tmp_path, "SELECT request_id, provider FROM lake.main.llm_requests ORDER BY 1") == [
        ("r1", "anthropic"),
        ("r2", "anthropic"),
    ]
    assert _read(tmp_path, "SELECT count(*) FROM lake.main.llm_payloads") == [(2,)]
    logger._executor.close()


def test_retention_deletes_old_rows_from_ducklake(tmp_path: Path):
    config = _direct_config(tmp_path, DUCKLAKE_SINK_RETENTION_DAYS="30")
    executor = DuckDBExecutor(config)
    try:
        executor.execute_script("CREATE TABLE IF NOT EXISTS main.llm_requests (request_day DATE)")
        executor.execute_script("CREATE TABLE IF NOT EXISTS main.llm_payloads (request_day DATE)")
        executor.execute("INSERT INTO main.llm_requests VALUES (DATE '2026-01-01'), (DATE '2026-11-10')")
        outcome = run_retention(config, executor, date(2026, 11, 11), lambda _config: False)
    finally:
        executor.close()
    assert outcome.error is None
    assert outcome.deleted_requests == 1
    assert _read(tmp_path, "SELECT count(*) FROM lake.main.llm_requests") == [(1,)]


class RecordingConnection:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, sql: str):
        self.statements.append(sql)
        return self

    def fetchall(self) -> list:
        return [(1,)]

    def close(self) -> None:
        pass


def test_quack_catalog_creates_scoped_secret_before_attach(tmp_path: Path):
    config = _direct_config(
        tmp_path, DUCKLAKE_SINK_CATALOG="quack:lake.internal", DUCKLAKE_SINK_QUACK_TOKEN="it's-secret"
    )
    connection = RecordingConnection()
    executor = DuckDBExecutor(config, connect_fn=lambda: connection)
    executor.execute("INSERT INTO main.t VALUES (1)")
    secret, attach, use, insert = connection.statements
    assert secret == "CREATE OR REPLACE SECRET (TYPE quack, TOKEN 'it''s-secret', SCOPE 'quack:lake.internal')"
    assert attach.startswith("ATTACH 'ducklake:quack:lake.internal' AS ducklake_sink (DATA_PATH ")
    assert use == "USE ducklake_sink"
    assert insert == "INSERT INTO main.t VALUES (1)"


def test_failed_statement_drops_the_connection(tmp_path: Path):
    connects: list = []

    class Failing(RecordingConnection):
        def execute(self, sql: str):
            if sql.startswith("INSERT"):
                raise RuntimeError("catalog down")
            return super().execute(sql)

    def connect():
        connects.append(1)
        return Failing()

    executor = DuckDBExecutor(_direct_config(tmp_path), connect_fn=connect)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            executor.execute("INSERT INTO main.t VALUES (1)")
    assert len(connects) == 2


def test_executor_creates_missing_parent_folder_of_local_catalog(tmp_path: Path):
    config = _direct_config(
        tmp_path,
        DUCKLAKE_SINK_CATALOG=f"sqlite:{tmp_path / 'lake' / 'catalog.sqlite'}",
        DUCKLAKE_SINK_DATA_PATH=str(tmp_path / "lake" / "data") + "/",
    )
    executor = DuckDBExecutor(config)
    try:
        executor.execute_script("CREATE TABLE IF NOT EXISTS main.t (i INTEGER)")
        assert executor.execute("INSERT INTO main.t VALUES (1)") == 1
    finally:
        executor.close()
    assert (tmp_path / "lake" / "catalog.sqlite").is_file()


def test_local_catalog_folder_only_for_file_catalogs():
    from litellm_ducklake_sink.duckdb_client import local_catalog_file

    assert local_catalog_file("sqlite:lake/catalog.sqlite") == "lake/catalog.sqlite"
    assert local_catalog_file("lake/metadata.ducklake") == "lake/metadata.ducklake"
    assert local_catalog_file("postgres:dbname=lake host=db") is None
    assert local_catalog_file("quack:localhost") is None
    assert local_catalog_file("mysql:db=lake") is None


def _quack_endpoint_config(**overrides: str) -> DuckLakeSinkConfig:
    env = {
        "DUCKLAKE_SINK_ENDPOINT": "quack:localhost:9494",
        "DUCKLAKE_SINK_USERNAME": "acme-admin",
        "DUCKLAKE_SINK_PASSWORD": "p&ss=word",
        "DUCKLAKE_SINK_TENANT": "acme",
        "DUCKLAKE_SINK_POOL": "bi",
    }
    env.update(overrides)
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeSinkConfig)
    return result


def test_quack_endpoint_token_is_url_encoded_like_a_jdbc_query_string():
    from litellm_ducklake_sink.duckdb_client import quack_endpoint_token

    token = quack_endpoint_token(_quack_endpoint_config())
    assert token == "tenant=acme&pool=bi&user=acme-admin&password=p%26ss%3Dword"


def test_quack_endpoint_token_omits_unset_routing():
    from litellm_ducklake_sink.duckdb_client import quack_endpoint_token

    config = _quack_endpoint_config(DUCKLAKE_SINK_TENANT="", DUCKLAKE_SINK_POOL="", DUCKLAKE_SINK_PASSWORD="pw")
    assert quack_endpoint_token(config) == "user=acme-admin&password=pw"


def test_raw_quack_token_wins_over_credentials():
    from litellm_ducklake_sink.duckdb_client import quack_endpoint_token

    assert quack_endpoint_token(_quack_endpoint_config(DUCKLAKE_SINK_QUACK_TOKEN="raw")) == "raw"


def test_quack_endpoint_attaches_remote_database():
    connection = RecordingConnection()
    executor = DuckDBExecutor(_quack_endpoint_config(DUCKLAKE_SINK_PASSWORD="pw"), connect_fn=lambda: connection)
    executor.execute("INSERT INTO main.t VALUES (1)")
    attach, use, insert = connection.statements
    assert attach == (
        "ATTACH 'quack:localhost:9494' AS ducklake_sink "
        "(TYPE quack, TOKEN 'tenant=acme&pool=bi&user=acme-admin&password=pw')"
    )
    assert use == "USE ducklake_sink"


def test_build_executor_uses_duckdb_for_quack_endpoint():
    assert isinstance(build_executor(_quack_endpoint_config()), DuckDBExecutor)


def test_s3_data_path_creates_credential_chain_secret_scoped_to_the_bucket(tmp_path: Path):
    from litellm_ducklake_sink.duckdb_client import setup_statements

    config = _direct_config(tmp_path, DUCKLAKE_SINK_DATA_PATH="s3://llm-lake/data/")
    secret, attach, use = setup_statements(config)
    assert secret == (
        "CREATE OR REPLACE SECRET ducklake_sink_s3 (TYPE s3, PROVIDER credential_chain, SCOPE 's3://llm-lake/')"
    )
    assert attach.endswith("(DATA_PATH 's3://llm-lake/data/')")


def test_s3_static_keys_and_endpoint_go_into_the_secret(tmp_path: Path):
    from litellm_ducklake_sink.duckdb_client import setup_statements

    config = _direct_config(
        tmp_path,
        DUCKLAKE_SINK_DATA_PATH="s3://llm-lake/data/",
        DUCKLAKE_SINK_S3_KEY_ID="AKIA'X",
        DUCKLAKE_SINK_S3_SECRET="shh",
        DUCKLAKE_SINK_S3_REGION="eu-west-3",
        DUCKLAKE_SINK_S3_ENDPOINT="minio:9000",
        DUCKLAKE_SINK_S3_URL_STYLE="path",
        DUCKLAKE_SINK_S3_USE_SSL="false",
    )
    assert setup_statements(config)[0] == (
        "CREATE OR REPLACE SECRET ducklake_sink_s3 (TYPE s3, KEY_ID 'AKIA''X', SECRET 'shh', "
        "REGION 'eu-west-3', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, "
        "SCOPE 's3://llm-lake/')"
    )


def test_local_data_path_creates_no_s3_secret(tmp_path: Path):
    from litellm_ducklake_sink.duckdb_client import setup_statements

    assert not any("SECRET" in s for s in setup_statements(_direct_config(tmp_path)))
