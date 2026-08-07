from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from litellm_ducklake_sink.config import DuckLakeConfigError, DuckLakeSinkConfig, resolve_ducklake_config
from litellm_ducklake_sink.flight_client import AdbcFlightSqlExecutor, FlightSqlExecutor


@dataclass(frozen=True, slots=True)
class RetentionOutcome:
    cutoff: date
    deleted_requests: int
    deleted_payloads: int
    maintenance_triggered: bool
    error: str | None


def retention_statements(schema_name: str, cutoff: date) -> tuple[str, str]:
    condition = f"WHERE request_day < DATE '{cutoff.isoformat()}'"
    return (
        f"DELETE FROM {schema_name}.llm_requests {condition}",
        f"DELETE FROM {schema_name}.llm_payloads {condition}",
    )


def trigger_maintenance(config: DuckLakeSinkConfig) -> bool:
    if config.maintenance_url is None or config.tenant_db is None or config.tenant is None:
        return False
    import httpx

    headers = {} if config.maintenance_api_key is None else {"X-API-Key": config.maintenance_api_key}
    response = httpx.post(
        config.maintenance_url.rstrip("/") + "/api/maintenance/run",
        json={"tenant": config.tenant, "tenantDb": config.tenant_db},
        headers=headers,
        timeout=60.0,
    )
    return response.is_success


def run_retention(
    config: DuckLakeSinkConfig,
    executor: FlightSqlExecutor,
    today: date,
    maintenance: Callable[[DuckLakeSinkConfig], bool],
) -> RetentionOutcome:
    cutoff = today - timedelta(days=config.retention_days)
    requests_stmt, payloads_stmt = retention_statements(config.schema_name, cutoff)
    try:
        deleted_requests = executor.execute(requests_stmt)
        deleted_payloads = executor.execute(payloads_stmt)
    except Exception as delete_error:
        return RetentionOutcome(cutoff, 0, 0, False, str(delete_error))
    try:
        triggered = maintenance(config)
        return RetentionOutcome(cutoff, deleted_requests, deleted_payloads, triggered, None)
    except Exception as maintenance_error:
        error_msg = f"maintenance trigger failed: {maintenance_error}"
        return RetentionOutcome(cutoff, deleted_requests, deleted_payloads, False, error_msg)


def main() -> int:
    config = resolve_ducklake_config()
    if isinstance(config, DuckLakeConfigError):
        print(config.message)
        return 2
    executor = AdbcFlightSqlExecutor(config)
    outcome = run_retention(config, executor, datetime.now(UTC).date(), trigger_maintenance)
    executor.close()
    print(
        f"ducklake retention: cutoff={outcome.cutoff} deleted_requests={outcome.deleted_requests} "
        f"deleted_payloads={outcome.deleted_payloads} maintenance_triggered={outcome.maintenance_triggered} "
        f"error={outcome.error}"
    )
    return 0 if outcome.error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
