from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from litellm_ducklake_sink.config import DuckLakeSinkConfig


class DbApiCursor(Protocol):
    rowcount: int

    def execute(self, operation: str) -> None: ...

    def executescript(self, operation: str) -> None: ...

    def fetchall(self) -> list: ...

    def close(self) -> None: ...


class DbApiConnection(Protocol):
    def cursor(self) -> DbApiCursor: ...

    def close(self) -> None: ...


class FlightSqlExecutor(Protocol):
    def execute(self, sql: str) -> int: ...

    def execute_script(self, sql: str) -> None: ...

    def close(self) -> None: ...


def _connection_kwargs(config: DuckLakeSinkConfig, header_prefix: str, tls_skip_verify_key: str) -> dict[str, str]:
    base_kwargs = {
        "username": config.username,
        "password": config.password,
    }
    if config.tenant is not None:
        base_kwargs[header_prefix + "tenant"] = config.tenant
    if config.pool is not None:
        base_kwargs[header_prefix + "pool"] = config.pool
    tls_kwargs = {tls_skip_verify_key: "true"} if config.tls_skip_verify else {}
    return {**base_kwargs, **tls_kwargs}


def _adbc_connect(config: DuckLakeSinkConfig) -> DbApiConnection:
    import adbc_driver_flightsql.dbapi as flightsql_dbapi
    from adbc_driver_flightsql import DatabaseOptions

    header_prefix = DatabaseOptions.RPC_CALL_HEADER_PREFIX.value
    tls_skip_verify_key = DatabaseOptions.TLS_SKIP_VERIFY.value
    db_kwargs = _connection_kwargs(config, header_prefix, tls_skip_verify_key)
    return flightsql_dbapi.connect(uri=config.endpoint, db_kwargs=db_kwargs, autocommit=True)


class AdbcFlightSqlExecutor:
    def __init__(
        self,
        config: DuckLakeSinkConfig,
        connect_fn: Callable[[DuckLakeSinkConfig], DbApiConnection] | None = None,
    ) -> None:
        self._config = config
        self._connect_fn = connect_fn if connect_fn is not None else _adbc_connect
        self._connection: DbApiConnection | None = None

    def _cursor(self) -> DbApiCursor:
        connection = self._connection if self._connection is not None else self._connect_fn(self._config)
        self._connection = connection
        return connection.cursor()

    def execute(self, sql: str) -> int:
        try:
            cursor = self._cursor()
            try:
                cursor.execute(sql)
                # Drain the result stream: Flight SQL edges that execute lazily on
                # DoGet would otherwise never run the statement. DuckDB returns the
                # affected-row count as a single-cell result for INSERT/DELETE.
                rows = cursor.fetchall()
                if len(rows) == 1 and len(rows[0]) == 1 and isinstance(rows[0][0], int):
                    return rows[0][0]
                return cursor.rowcount
            finally:
                cursor.close()
        except Exception:
            self.close()
            raise

    def execute_script(self, sql: str) -> None:
        # DDL goes through the update path (DoPut): its result schema is
        # unpredictable across servers (e.g. a Success column where the edge
        # advertised Count), so it must not be executed via a fetched query.
        try:
            cursor = self._cursor()
            try:
                cursor.executescript(sql)
            finally:
                cursor.close()
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
