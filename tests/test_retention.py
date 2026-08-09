from datetime import date

from litellm_ducklake_sink.config import DuckLakeSinkConfig, resolve_ducklake_config
from litellm_ducklake_sink.retention import (
    RetentionOutcome,
    retention_statements,
    run_retention,
    trigger_maintenance,
)


def _config(**overrides: str) -> DuckLakeSinkConfig:
    env = {
        "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
        "DUCKLAKE_SINK_USERNAME": "u",
        "DUCKLAKE_SINK_PASSWORD": "p",
        "DUCKLAKE_SINK_TENANT": "acme",
        "DUCKLAKE_SINK_POOL": "bi",
        "DUCKLAKE_SINK_RETENTION_DAYS": "30",
    }
    env.update(overrides)
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeSinkConfig)
    return result


class RecordingExecutor:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.statements: list = []

    def execute(self, sql: str) -> int:
        if self.fail:
            raise RuntimeError("edge down")
        self.statements.append(sql)
        return 5

    def close(self) -> None:
        pass


def test_retention_statements_delete_before_cutoff():
    requests_stmt, payloads_stmt = retention_statements("main", date(2026, 7, 5))
    assert requests_stmt == "DELETE FROM main.llm_requests WHERE request_day < DATE '2026-07-05'"
    assert payloads_stmt == "DELETE FROM main.llm_payloads WHERE request_day < DATE '2026-07-05'"


def test_run_retention_deletes_and_triggers_maintenance():
    executor = RecordingExecutor()
    triggered: list = []

    def fake_maintenance(config: DuckLakeSinkConfig) -> bool:
        triggered.append(config.tenant)
        return True

    outcome = run_retention(_config(), executor, date(2026, 8, 4), fake_maintenance)
    assert outcome.cutoff == date(2026, 7, 5)
    assert outcome.deleted_requests == 5
    assert outcome.deleted_payloads == 5
    assert outcome.maintenance_triggered is True
    assert outcome.error is None
    assert triggered == ["acme"]
    assert len(executor.statements) == 2


def test_run_retention_reports_failure_as_value():
    outcome = run_retention(_config(), RecordingExecutor(fail=True), date(2026, 8, 4), lambda config: True)
    assert isinstance(outcome, RetentionOutcome)
    assert outcome.error is not None
    assert outcome.maintenance_triggered is False


def test_main_is_noop_when_sink_disabled(monkeypatch, capsys):
    import litellm_ducklake_sink.retention as retention_module

    def explode(config):
        raise AssertionError("executor must not be constructed for a disabled sink")

    monkeypatch.setattr(retention_module, "AdbcFlightSqlExecutor", explode)
    monkeypatch.setenv("DUCKLAKE_SINK_ENABLED", "false")
    assert retention_module.main() == 0
    assert "disabled" in capsys.readouterr().out


def test_trigger_maintenance_requires_tenant():
    env = {
        "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
        "DUCKLAKE_SINK_USERNAME": "u",
        "DUCKLAKE_SINK_PASSWORD": "p",
        "DUCKLAKE_SINK_TENANT_DB": "acme_llm",
        "DUCKLAKE_SINK_MAINTENANCE_URL": "http://localhost:20900",
    }
    config = resolve_ducklake_config(env)
    assert isinstance(config, DuckLakeSinkConfig)
    assert trigger_maintenance(config) is False


def test_run_retention_keeps_delete_counts_when_maintenance_fails():
    executor = RecordingExecutor()

    def failing_maintenance(config: DuckLakeSinkConfig) -> bool:
        raise RuntimeError("maintenance service down")

    outcome = run_retention(_config(), executor, date(2026, 8, 4), failing_maintenance)
    assert outcome.deleted_requests == 5
    assert outcome.deleted_payloads == 5
    assert outcome.maintenance_triggered is False
    assert outcome.error is not None
    assert "maintenance trigger failed" in outcome.error
