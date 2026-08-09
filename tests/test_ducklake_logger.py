import asyncio
import time
from pathlib import Path

import pytest

from litellm_ducklake_sink.config import DuckLakeSinkConfig, resolve_ducklake_config
from litellm_ducklake_sink.ducklake_logger import DuckLakeLogger
from litellm_ducklake_sink.spool import DiskSpool


def _config(tmp_path: Path, **overrides: str) -> DuckLakeSinkConfig:
    env = {
        "DUCKLAKE_SINK_ENDPOINT": "grpc://localhost:31338",
        "DUCKLAKE_SINK_USERNAME": "acme-admin",
        "DUCKLAKE_SINK_PASSWORD": "secret",
        "DUCKLAKE_SINK_TENANT": "acme",
        "DUCKLAKE_SINK_POOL": "bi",
        "DUCKLAKE_SINK_SPOOL_DIR": str(tmp_path / "spool"),
        "DUCKLAKE_SINK_FLUSH_MAX_ATTEMPTS": "1",
        "DUCKLAKE_SINK_BATCH_ROWS": "3",
    }
    env.update(overrides)
    result = resolve_ducklake_config(env)
    assert isinstance(result, DuckLakeSinkConfig)
    return result


class FakeExecutor:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.statements: list = []
        self.script_statements: list = []

    def execute(self, sql: str) -> int:
        if self.fail:
            raise RuntimeError("edge down")
        self.statements.append(sql)
        return 1

    def execute_script(self, sql: str) -> None:
        if self.fail:
            raise RuntimeError("edge down")
        self.statements.append(sql)
        self.script_statements.append(sql)

    def close(self) -> None:
        pass

    def inserts(self, table: str) -> list:
        return [s for s in self.statements if s.startswith(f"INSERT INTO main.{table}")]


class SlowExecutor:
    def __init__(self, delay: float = 0.05):
        self.delay = delay
        self.statements: list = []

    def execute(self, sql: str) -> int:
        time.sleep(self.delay)
        self.statements.append(sql)
        return 1

    def execute_script(self, sql: str) -> None:
        time.sleep(self.delay)
        self.statements.append(sql)

    def close(self) -> None:
        pass

    def inserts(self, table: str) -> list:
        return [s for s in self.statements if s.startswith(f"INSERT INTO main.{table}")]


class PayloadFailingExecutor:
    def __init__(self):
        self.statements: list = []

    def execute(self, sql: str) -> int:
        if sql.startswith("INSERT INTO main.llm_payloads"):
            raise RuntimeError("payload table down")
        self.statements.append(sql)
        return 1

    def execute_script(self, sql: str) -> None:
        self.statements.append(sql)

    def close(self) -> None:
        pass

    def inserts(self, table: str) -> list:
        return [s for s in self.statements if s.startswith(f"INSERT INTO main.{table}")]


class WriteFailingSpool:
    def write(self, batch) -> int:
        raise OSError("disk full")

    def oldest(self):
        return None

    def claim_oldest(self, now=None):
        return None

    def release(self, path) -> None:
        pass

    def delete(self, path) -> None:
        pass

    def size_bytes(self) -> int:
        return 0


class OldestFailingSpool:
    def oldest(self):
        raise OSError("spool corrupted")

    def claim_oldest(self, now=None):
        raise OSError("spool corrupted")

    def release(self, path) -> None:
        pass

    def write(self, batch) -> int:
        return 0

    def delete(self, path) -> None:
        pass

    def size_bytes(self) -> int:
        return 0


def _event_kwargs(request_id: str) -> dict:
    return {
        "standard_logging_object": {
            "id": request_id,
            "trace_id": "t",
            "call_type": "acompletion",
            "response_cost": 0.01,
            "status": "success",
            "custom_llm_provider": "anthropic",
            "total_tokens": 3,
            "prompt_tokens": 1,
            "completion_tokens": 2,
            "startTime": 1754300000.0,
            "endTime": 1754300001.0,
            "completionStartTime": 1754300000.5,
            "response_time": 1.0,
            "model": "claude-fable-5",
            "model_group": "claude",
            "metadata": {"user_api_key_team_id": "team-1"},
            "cache_hit": False,
            "end_user": "dev",
            "user_agent": "curl",
            "messages": [{"role": "user", "content": "hi"}],
            "response": {"ok": True},
        }
    }


@pytest.mark.asyncio
async def test_flush_writes_single_insert_per_table_and_counts(tmp_path: Path):
    executor = FakeExecutor()
    config = _config(tmp_path, DUCKLAKE_SINK_CAPTURE_PAYLOADS="true")
    logger = DuckLakeLogger(config=config, executor=executor, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.async_log_success_event(_event_kwargs("r2"), None, None, None)
    await logger.flush_queue()
    assert len(executor.inserts("llm_requests")) == 1
    assert len(executor.inserts("llm_payloads")) == 1
    assert "('r1'" in executor.inserts("llm_requests")[0]
    assert "('r2'" in executor.inserts("llm_requests")[0]
    assert logger.counters.rows_accepted == 2
    assert logger.counters.rows_written == 2
    assert logger.log_queue == []


@pytest.mark.asyncio
async def test_bootstrap_ddl_runs_once_before_first_insert(tmp_path: Path):
    executor = FakeExecutor()
    logger = DuckLakeLogger(config=_config(tmp_path), executor=executor, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.flush_queue()
    await logger.async_log_success_event(_event_kwargs("r2"), None, None, None)
    await logger.flush_queue()
    creates = [s for s in executor.statements if s.startswith("CREATE TABLE")]
    assert len(creates) == 2
    assert executor.statements[0].startswith("CREATE TABLE")
    assert len(executor.script_statements) == 4
    assert all(s.startswith(("CREATE TABLE", "ALTER TABLE")) for s in executor.script_statements)


@pytest.mark.asyncio
async def test_disabled_logger_builds_no_spool_or_executor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import litellm_ducklake_sink.ducklake_logger as module

    def explode(*args, **kwargs):
        raise AssertionError("must not be constructed for a disabled sink")

    monkeypatch.setattr(module, "DiskSpool", explode)
    monkeypatch.setattr(module, "AdbcFlightSqlExecutor", explode)
    logger = DuckLakeLogger(config=_config(tmp_path, DUCKLAKE_SINK_ENABLED="false"), start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.flush_queue()
    await logger.drain()
    logger.drain_sync()
    assert logger.spool_size_bytes() == 0
    assert logger.counters.rows_accepted == 0


@pytest.mark.asyncio
async def test_flush_failure_reruns_ddl_on_next_flush(tmp_path: Path):
    executor = FakeExecutor()
    logger = DuckLakeLogger(config=_config(tmp_path), executor=executor, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.flush_queue()
    executor.fail = True
    await logger.async_log_success_event(_event_kwargs("r2"), None, None, None)
    await logger.flush_queue()
    executor.fail = False
    await logger.async_log_success_event(_event_kwargs("r3"), None, None, None)
    await logger.flush_queue()
    # The failed flush may mean the server lost the tables (e.g. it was
    # recreated); the next flush must re-run the idempotent DDL.
    assert len(executor.script_statements) == 8


@pytest.mark.asyncio
async def test_capture_payloads_off_writes_no_payload_rows(tmp_path: Path):
    executor = FakeExecutor()
    logger = DuckLakeLogger(config=_config(tmp_path), executor=executor, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.flush_queue()
    assert executor.inserts("llm_payloads") == []
    assert "'hi'" not in "".join(executor.inserts("llm_requests"))


@pytest.mark.asyncio
async def test_reaching_batch_rows_schedules_background_flush(tmp_path: Path):
    executor = FakeExecutor()
    logger = DuckLakeLogger(config=_config(tmp_path), executor=executor, start_flush_task=False)
    for i in range(3):
        await logger.async_log_success_event(_event_kwargs(f"r{i}"), None, None, None)
    for _ in range(5):
        await asyncio.sleep(0)
    assert len(executor.inserts("llm_requests")) == 1


@pytest.mark.asyncio
async def test_failed_flush_spools_batch_and_replays_on_recovery(tmp_path: Path):
    executor = FakeExecutor(fail=True)
    config = _config(tmp_path)
    spool = DiskSpool(config.spool_dir, config.spool_max_bytes)
    logger = DuckLakeLogger(config=config, executor=executor, spool=spool, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("lost-then-found"), None, None, None)
    await logger.flush_queue()
    assert logger.counters.flushes_failed == 1
    assert logger.log_queue == []
    assert spool.oldest() is not None
    executor.fail = False
    await logger.async_log_success_event(_event_kwargs("fresh"), None, None, None)
    await logger.flush_queue()
    async with logger.flush_lock:
        await logger._replay_spool()
    inserts = executor.inserts("llm_requests")
    assert any("fresh" in s for s in inserts)
    assert any("lost-then-found" in s for s in inserts)
    assert spool.oldest() is None
    assert logger.counters.rows_written == 2


@pytest.mark.asyncio
async def test_idle_replay_recovers_spool_without_new_traffic(tmp_path: Path):
    executor = FakeExecutor(fail=True)
    config = _config(tmp_path, DUCKLAKE_SINK_BATCH_INTERVAL="1")
    spool = DiskSpool(config.spool_dir, config.spool_max_bytes)
    logger = DuckLakeLogger(config=config, executor=executor, spool=spool, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("stranded"), None, None, None)
    await logger.flush_queue()
    executor.fail = False
    replay_task = asyncio.create_task(logger.periodic_flush())
    await asyncio.sleep(1.2)
    replay_task.cancel()
    assert any("stranded" in s for s in executor.inserts("llm_requests"))
    assert spool.oldest() is None


@pytest.mark.asyncio
async def test_drain_flushes_buffer(tmp_path: Path):
    executor = FakeExecutor()
    logger = DuckLakeLogger(config=_config(tmp_path), executor=executor, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.drain()
    assert len(executor.inserts("llm_requests")) == 1


@pytest.mark.asyncio
async def test_drain_sync_writes_remaining_rows_without_event_loop(tmp_path: Path):
    executor = FakeExecutor()
    logger = DuckLakeLogger(config=_config(tmp_path), executor=executor, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("last-breath"), None, None, None)
    logger.drain_sync()
    assert any("last-breath" in s for s in executor.inserts("llm_requests"))
    assert logger.log_queue == []


@pytest.mark.asyncio
async def test_drain_sync_spools_on_failure(tmp_path: Path):
    executor = FakeExecutor(fail=True)
    config = _config(tmp_path)
    spool = DiskSpool(config.spool_dir, config.spool_max_bytes)
    logger = DuckLakeLogger(config=config, executor=executor, spool=spool, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("saved-to-disk"), None, None, None)
    logger.drain_sync()
    spooled = spool.oldest()
    assert spooled is not None
    assert spooled.batch.requests[0].request_id == "saved-to-disk"


@pytest.mark.asyncio
async def test_drain_sync_survives_spool_failure(tmp_path: Path):
    executor = FakeExecutor(fail=True)
    logger = DuckLakeLogger(
        config=_config(tmp_path), executor=executor, spool=WriteFailingSpool(), start_flush_task=False
    )
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    logger.drain_sync()
    assert logger.counters.batches_dropped == 1


@pytest.mark.asyncio
async def test_lazy_flush_task_starts_on_first_event(tmp_path: Path):
    executor = FakeExecutor()
    logger = DuckLakeLogger(config=_config(tmp_path), executor=executor)
    assert logger._flush_task is None
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    assert logger._flush_task is not None
    logger._flush_task.cancel()


@pytest.mark.asyncio
async def test_disabled_sink_accepts_nothing(tmp_path: Path):
    executor = FakeExecutor()
    logger = DuckLakeLogger(
        config=_config(tmp_path, DUCKLAKE_SINK_ENABLED="false"), executor=executor, start_flush_task=False
    )
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.flush_queue()
    assert executor.statements == []
    assert logger.counters.rows_accepted == 0


@pytest.mark.asyncio
async def test_rows_enqueued_during_flush_are_preserved(tmp_path: Path):
    executor = SlowExecutor()
    logger = DuckLakeLogger(config=_config(tmp_path), executor=executor, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    flush_task = asyncio.create_task(logger.flush_queue())
    await asyncio.sleep(0.01)
    await logger.async_log_success_event(_event_kwargs("r2"), None, None, None)
    await flush_task
    assert len(logger.log_queue) == 1
    await logger.flush_queue()
    inserts = executor.inserts("llm_requests")
    assert len(inserts) == 2
    assert any("r1" in s for s in inserts)
    assert any("r2" in s for s in inserts)
    assert sum(s.count("'r1'") for s in inserts) == 1


@pytest.mark.asyncio
async def test_spool_write_failure_does_not_raise_and_counts_drop(tmp_path: Path):
    executor = FakeExecutor(fail=True)
    logger = DuckLakeLogger(
        config=_config(tmp_path), executor=executor, spool=WriteFailingSpool(), start_flush_task=False
    )
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.flush_queue()
    assert logger.log_queue == []
    assert logger.counters.batches_dropped == 1


@pytest.mark.asyncio
async def test_replay_partial_failure_counts_written_requests(tmp_path: Path):
    executor = PayloadFailingExecutor()
    config = _config(tmp_path, DUCKLAKE_SINK_CAPTURE_PAYLOADS="true")
    spool = DiskSpool(config.spool_dir, config.spool_max_bytes)
    logger = DuckLakeLogger(config=config, executor=executor, spool=spool, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("split-me"), None, None, None)
    batch = logger._snapshot_batch()
    logger.log_queue.clear()
    spool.write(batch)
    await logger._replay_spool()
    assert logger.counters.rows_written == 1
    remaining = spool.oldest()
    assert remaining is not None
    assert remaining.batch.requests == ()
    assert len(remaining.batch.payloads) == 1


@pytest.mark.asyncio
async def test_replay_total_failure_releases_claim_for_retry(tmp_path: Path):
    executor = FakeExecutor(fail=True)
    config = _config(tmp_path)
    spool = DiskSpool(config.spool_dir, config.spool_max_bytes)
    logger = DuckLakeLogger(config=config, executor=executor, spool=spool, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("still-stuck"), None, None, None)
    batch = logger._snapshot_batch()
    logger.log_queue.clear()
    spool.write(batch)
    await logger._replay_spool()
    reclaimed = spool.claim_oldest()
    assert reclaimed is not None
    assert reclaimed.batch.requests[0].request_id == "still-stuck"


@pytest.mark.asyncio
async def test_replay_exception_does_not_kill_periodic_task(tmp_path: Path):
    executor = FakeExecutor()
    config = _config(tmp_path, DUCKLAKE_SINK_BATCH_INTERVAL="1")
    logger = DuckLakeLogger(config=config, executor=executor, spool=OldestFailingSpool(), start_flush_task=False)
    task = asyncio.create_task(logger.periodic_flush())
    await asyncio.sleep(2.4)
    assert not task.done()
    task.cancel()


@pytest.mark.asyncio
async def test_drain_timeout_does_not_duplicate_rows(tmp_path: Path):
    executor = SlowExecutor(delay=0.2)
    config = _config(tmp_path, DUCKLAKE_SINK_DRAIN_TIMEOUT="0")
    logger = DuckLakeLogger(config=config, executor=executor, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    await logger.drain()
    assert logger.log_queue == []
    await asyncio.sleep(1.3)
    assert len(executor.inserts("llm_requests")) == 1
    await logger.flush_queue()
    assert len(executor.inserts("llm_requests")) == 1


@pytest.mark.asyncio
async def test_cancelled_flush_spools_snapshot(tmp_path: Path):
    executor = SlowExecutor(delay=0.2)
    config = _config(tmp_path)
    spool = DiskSpool(config.spool_dir, config.spool_max_bytes)
    logger = DuckLakeLogger(config=config, executor=executor, spool=spool, start_flush_task=False)
    await logger.async_log_success_event(_event_kwargs("r1"), None, None, None)
    task = asyncio.create_task(logger.flush_queue())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert spool.oldest() is not None
