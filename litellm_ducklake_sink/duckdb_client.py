from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, Protocol
from urllib.parse import urlencode

from litellm_ducklake_sink.config import DuckLakeSinkConfig
from litellm_ducklake_sink.flight_client import AdbcFlightSqlExecutor, FlightSqlExecutor
from litellm_ducklake_sink.sql import sql_literal

ATTACH_ALIAS = "ducklake_sink"
_SERVER_CATALOGS = ("postgres:", "mysql:", "quack:")


class DuckDBConnection(Protocol):
    def execute(self, sql: str) -> Any: ...

    def close(self) -> None: ...


def _duckdb_connect() -> DuckDBConnection:
    import duckdb

    return duckdb.connect()


def local_catalog_file(catalog: str) -> str | None:
    """The catalog's file on local disk, or None when a server holds the catalog."""
    if catalog.startswith(_SERVER_CATALOGS):
        return None
    return catalog.removeprefix("sqlite:")


def quack_endpoint_token(config: DuckLakeSinkConfig) -> str:
    """The Quack TOKEN: the raw token when set, else QoD's JDBC-style credential string."""
    if config.quack_token is not None:
        return config.quack_token
    params = (
        ("tenant", config.tenant),
        ("pool", config.pool),
        ("user", config.username),
        ("password", config.password),
    )
    return urlencode([(key, value) for key, value in params if value])


def setup_statements(config: DuckLakeSinkConfig) -> tuple[str, ...]:
    """SQL that attaches the lake in a fresh in-process DuckDB and makes it the default catalog."""
    if config.is_quack_endpoint:
        attach = (
            f"ATTACH {sql_literal(config.endpoint)} AS {ATTACH_ALIAS} "
            f"(TYPE quack, TOKEN {sql_literal(quack_endpoint_token(config))})"
        )
        return (attach, f"USE {ATTACH_ALIAS}")
    catalog = config.catalog or ""
    secret = (
        (
            f"CREATE OR REPLACE SECRET (TYPE quack, TOKEN {sql_literal(config.quack_token or '')}, "
            f"SCOPE {sql_literal(catalog)})",
        )
        if catalog.startswith("quack:")
        else ()
    )
    attach = (
        f"ATTACH {sql_literal('ducklake:' + catalog)} AS {ATTACH_ALIAS} "
        f"(DATA_PATH {sql_literal(config.data_path or '')})"
    )
    return (*secret, attach, f"USE {ATTACH_ALIAS}")


class DuckDBExecutor:
    """Runs the sink's SQL in a DuckDB embedded in the proxy process.

    With a catalog, it attaches DuckLake directly, so no server sits between LiteLLM and the
    lake: a SQLite file, a Postgres database, a Quack server hosting the catalog, or a DuckDB
    file (single process only). With a quack: endpoint, it attaches a remote database over
    the Quack protocol instead, such as a QoD gateway. DuckDB downloads the extensions it
    needs (ducklake, sqlite, postgres, quack) on first use.
    """

    def __init__(
        self,
        config: DuckLakeSinkConfig,
        connect_fn: Callable[[], DuckDBConnection] | None = None,
    ) -> None:
        self._config = config
        self._connect_fn = connect_fn if connect_fn is not None else _duckdb_connect
        self._connection: DuckDBConnection | None = None

    def _attached(self) -> DuckDBConnection:
        if self._connection is not None:
            return self._connection
        catalog_file = local_catalog_file(self._config.catalog or "")
        if catalog_file is not None and os.path.dirname(catalog_file):
            # DuckLake creates DATA_PATH itself but not the folder of a local catalog file.
            os.makedirs(os.path.dirname(catalog_file), exist_ok=True)
        connection = self._connect_fn()
        try:
            for statement in setup_statements(self._config):
                connection.execute(statement)
        except Exception:
            connection.close()
            raise
        self._connection = connection
        return connection

    def execute(self, sql: str) -> int:
        try:
            rows = self._attached().execute(sql).fetchall()
        except Exception:
            self.close()
            raise
        # DuckDB returns the affected-row count as a single-cell result for INSERT/DELETE.
        if len(rows) == 1 and len(rows[0]) == 1 and isinstance(rows[0][0], int):
            return rows[0][0]
        return 0

    def execute_script(self, sql: str) -> None:
        try:
            self._attached().execute(sql)
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            connection.close()
        except Exception:
            pass


def build_executor(config: DuckLakeSinkConfig) -> FlightSqlExecutor:
    if config.is_direct or config.is_quack_endpoint:
        return DuckDBExecutor(config)
    return AdbcFlightSqlExecutor(config)
