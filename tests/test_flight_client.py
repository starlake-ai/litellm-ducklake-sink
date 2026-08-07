import pytest

from litellm_ducklake_sink.config import DuckLakeSinkConfig, resolve_ducklake_config
from litellm_ducklake_sink.flight_client import AdbcFlightSqlExecutor, _connection_kwargs


def _config() -> DuckLakeSinkConfig:
    result = resolve_ducklake_config(
        {
            "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
            "DUCKLAKE_SINK_USERNAME": "acme-admin",
            "DUCKLAKE_SINK_PASSWORD": "secret",
            "DUCKLAKE_SINK_TENANT": "acme",
            "DUCKLAKE_SINK_POOL": "bi",
        }
    )
    assert isinstance(result, DuckLakeSinkConfig)
    return result


class FakeCursor:
    def __init__(self, log: list, fail: bool, rows: list | None = None):
        self.log = log
        self.fail = fail
        self.rows = rows if rows is not None else []
        self.rowcount = 3
        self.fetched = False

    def execute(self, operation: str) -> None:
        if self.fail:
            raise RuntimeError("edge unreachable")
        self.log.append(operation)

    def fetchall(self) -> list:
        self.fetched = True
        return self.rows

    def executescript(self, operation: str) -> None:
        if self.fail:
            raise RuntimeError("edge unreachable")
        self.log.append(("script", operation))

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self, log: list, fail: bool = False):
        self.log = log
        self.fail = fail
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.log, self.fail)

    def close(self) -> None:
        self.closed = True


def test_execute_reuses_connection_and_returns_rowcount():
    statements: list = []
    connections: list = []

    def connect(config: DuckLakeSinkConfig) -> FakeConnection:
        connection = FakeConnection(statements)
        connections.append(connection)
        return connection

    executor = AdbcFlightSqlExecutor(_config(), connect_fn=connect)
    assert executor.execute("SELECT 1") == 3
    assert executor.execute("SELECT 2") == 3
    assert statements == ["SELECT 1", "SELECT 2"]
    assert len(connections) == 1


def test_execute_drains_results_so_lazy_edges_run_the_statement():
    cursors: list = []

    class DrainConnection:
        def cursor(self) -> FakeCursor:
            cursor = FakeCursor([], fail=False, rows=[(7,)])
            cursor.rowcount = -1
            cursors.append(cursor)
            return cursor

        def close(self) -> None:
            pass

    executor = AdbcFlightSqlExecutor(_config(), connect_fn=lambda config: DrainConnection())
    assert executor.execute("INSERT INTO t VALUES (1)") == 7
    assert cursors[0].fetched is True


def test_execute_falls_back_to_rowcount_without_count_row():
    statements: list = []
    executor = AdbcFlightSqlExecutor(_config(), connect_fn=lambda config: FakeConnection(statements))
    assert executor.execute("CREATE TABLE t (x INTEGER)") == 3


def test_execute_script_uses_update_path_without_fetching():
    statements: list = []
    executor = AdbcFlightSqlExecutor(_config(), connect_fn=lambda config: FakeConnection(statements))
    executor.execute_script("CREATE TABLE t (x INTEGER)")
    assert statements == [("script", "CREATE TABLE t (x INTEGER)")]


def test_failed_execute_script_resets_connection_for_reconnect():
    statements: list = []
    plan = [True, False]
    connections: list = []

    def connect(config: DuckLakeSinkConfig) -> FakeConnection:
        connection = FakeConnection(statements, fail=plan[len(connections)])
        connections.append(connection)
        return connection

    executor = AdbcFlightSqlExecutor(_config(), connect_fn=connect)
    with pytest.raises(RuntimeError):
        executor.execute_script("CREATE TABLE t (x INTEGER)")
    assert connections[0].closed is True
    executor.execute_script("CREATE TABLE t (x INTEGER)")
    assert len(connections) == 2


def test_failed_execute_resets_connection_for_reconnect():
    statements: list = []
    plan = [True, False]
    connections: list = []

    def connect(config: DuckLakeSinkConfig) -> FakeConnection:
        connection = FakeConnection(statements, fail=plan[len(connections)])
        connections.append(connection)
        return connection

    executor = AdbcFlightSqlExecutor(_config(), connect_fn=connect)
    with pytest.raises(RuntimeError):
        executor.execute("INSERT 1")
    assert connections[0].closed is True
    assert executor.execute("INSERT 2") == 3
    assert len(connections) == 2


def test_connection_kwargs_contract():
    header_prefix = "adbc.flight.sql.rpc.call_header."
    tls_skip_verify_key = "adbc.flight.sql.client_option.tls_skip_verify"

    config_no_tls = resolve_ducklake_config(
        {
            "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
            "DUCKLAKE_SINK_USERNAME": "user1",
            "DUCKLAKE_SINK_PASSWORD": "pass1",
            "DUCKLAKE_SINK_TENANT": "acme",
            "DUCKLAKE_SINK_POOL": "bi",
            "DUCKLAKE_SINK_TLS_SKIP_VERIFY": "false",
        }
    )
    assert isinstance(config_no_tls, DuckLakeSinkConfig)
    kwargs_no_tls = _connection_kwargs(config_no_tls, header_prefix, tls_skip_verify_key)
    assert kwargs_no_tls == {
        "username": "user1",
        "password": "pass1",
        "adbc.flight.sql.rpc.call_header.tenant": "acme",
        "adbc.flight.sql.rpc.call_header.pool": "bi",
    }

    config_no_qod = resolve_ducklake_config(
        {
            "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31337",
            "DUCKLAKE_SINK_USERNAME": "user1",
            "DUCKLAKE_SINK_PASSWORD": "pass1",
        }
    )
    assert isinstance(config_no_qod, DuckLakeSinkConfig)
    kwargs_no_qod = _connection_kwargs(config_no_qod, header_prefix, tls_skip_verify_key)
    assert kwargs_no_qod == {"username": "user1", "password": "pass1"}

    config_with_tls = resolve_ducklake_config(
        {
            "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
            "DUCKLAKE_SINK_USERNAME": "user2",
            "DUCKLAKE_SINK_PASSWORD": "pass2",
            "DUCKLAKE_SINK_TENANT": "customer",
            "DUCKLAKE_SINK_POOL": "analytics",
            "DUCKLAKE_SINK_TLS_SKIP_VERIFY": "true",
        }
    )
    assert isinstance(config_with_tls, DuckLakeSinkConfig)
    kwargs_with_tls = _connection_kwargs(config_with_tls, header_prefix, tls_skip_verify_key)
    assert kwargs_with_tls == {
        "username": "user2",
        "password": "pass2",
        "adbc.flight.sql.rpc.call_header.tenant": "customer",
        "adbc.flight.sql.rpc.call_header.pool": "analytics",
        "adbc.flight.sql.client_option.tls_skip_verify": "true",
    }
