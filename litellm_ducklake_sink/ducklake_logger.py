from __future__ import annotations

import asyncio

from litellm._logging import verbose_logger
from litellm.integrations.custom_batch_logger import CustomBatchLogger
from litellm.types.utils import StandardLoggingPayload

from litellm_ducklake_sink.config import DuckLakeConfigError, DuckLakeSinkConfig, resolve_ducklake_config
from litellm_ducklake_sink.flight_client import AdbcFlightSqlExecutor, FlightSqlExecutor
from litellm_ducklake_sink.records import build_payload_row, build_request_row
from litellm_ducklake_sink.spool import DiskSpool
from litellm_ducklake_sink.sql import (
    PAYLOAD_COLUMNS,
    REQUEST_COLUMNS,
    ddl_statements,
    payload_row_values,
    render_insert,
    request_row_values,
)
from litellm_ducklake_sink.types import DuckLakeBatch, DuckLakePayloadRow, DuckLakeRequestRow

REPLAY_BATCHES_PER_CYCLE = 10
REQUEST_ROW_BYTES_ESTIMATE = 256
BACKOFF_CAP_SECONDS = 30


class DuckLakeCounters:
    __slots__ = ("rows_accepted", "rows_written", "flushes_failed", "batches_dropped")

    def __init__(self) -> None:
        self.rows_accepted = 0
        self.rows_written = 0
        self.flushes_failed = 0
        self.batches_dropped = 0


class DuckLakeLogger(CustomBatchLogger):
    def __init__(
        self,
        config: DuckLakeSinkConfig | None = None,
        executor: FlightSqlExecutor | None = None,
        spool: DiskSpool | None = None,
        start_flush_task: bool = True,
        **kwargs,
    ) -> None:
        resolved = config if config is not None else resolve_ducklake_config()
        if isinstance(resolved, DuckLakeConfigError):
            raise ValueError(f"ducklake sink misconfigured: {resolved.message}")
        self.sink_config = resolved
        self.counters = DuckLakeCounters()
        # A disabled sink must stay side-effect free: no spool mkdir, no executor.
        if executor is not None:
            self._executor: FlightSqlExecutor | None = executor
        else:
            self._executor = AdbcFlightSqlExecutor(resolved) if resolved.enabled else None
        if spool is not None:
            self._spool: DiskSpool | None = spool
        else:
            self._spool = DiskSpool(resolved.spool_dir, resolved.spool_max_bytes) if resolved.enabled else None
        self._bootstrapped = False
        self._pending_bytes = 0
        self._start_flush_task = start_flush_task
        self._flush_task: asyncio.Task | None = None
        CustomBatchLogger.__init__(
            self,
            flush_lock=asyncio.Lock(),
            batch_size=resolved.batch_rows,
            flush_interval=resolved.batch_interval,
            **kwargs,
        )
        self.log_queue: list[tuple[DuckLakeRequestRow, DuckLakePayloadRow | None]] = []

    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        self._enqueue(kwargs.get("standard_logging_object"))

    async def async_log_failure_event(self, kwargs, response_obj, start_time, end_time):
        self._enqueue(kwargs.get("standard_logging_object"))

    def _ensure_flush_task(self) -> None:
        if not self._start_flush_task or self._flush_task is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._flush_task = loop.create_task(self.periodic_flush())

    def _enqueue(self, payload: StandardLoggingPayload | None) -> None:
        if payload is None or not self.sink_config.enabled:
            return
        self._ensure_flush_task()
        try:
            request_row = build_request_row(payload)
            payload_row = (
                build_payload_row(payload, self.sink_config.payload_max_bytes)
                if self.sink_config.capture_payloads
                else None
            )
        except Exception:
            verbose_logger.exception("ducklake sink: failed to extract row from standard_logging_object")
            return
        was_over = len(self.log_queue) >= self.batch_size or self._pending_bytes >= self.sink_config.batch_max_bytes
        self.log_queue.append((request_row, payload_row))
        self.counters.rows_accepted += 1
        self._pending_bytes += REQUEST_ROW_BYTES_ESTIMATE + (0 if payload_row is None else payload_row.payload_bytes)
        is_over = len(self.log_queue) >= self.batch_size or self._pending_bytes >= self.sink_config.batch_max_bytes
        if is_over and not was_over:
            asyncio.create_task(self.flush_queue())

    def _snapshot_batch(self) -> DuckLakeBatch:
        items = tuple(self.log_queue)
        del self.log_queue[: len(items)]
        self._pending_bytes = 0
        return DuckLakeBatch(
            requests=tuple(item[0] for item in items),
            payloads=tuple(item[1] for item in items if item[1] is not None),
        )

    def _insert_statements(self, batch: DuckLakeBatch) -> tuple[str | None, str | None]:
        schema = self.sink_config.schema_name
        request_stmt = (
            render_insert(schema, "llm_requests", REQUEST_COLUMNS, tuple(request_row_values(r) for r in batch.requests))
            if batch.requests
            else None
        )
        payload_stmt = (
            render_insert(schema, "llm_payloads", PAYLOAD_COLUMNS, tuple(payload_row_values(p) for p in batch.payloads))
            if batch.payloads
            else None
        )
        return request_stmt, payload_stmt

    async def flush_queue(self):
        if not self.sink_config.enabled or self.flush_lock is None:
            return
        async with self.flush_lock:
            batch = self._snapshot_batch()
            if not batch.requests and not batch.payloads:
                return
            try:
                await self._write_or_spool(batch)
            except asyncio.CancelledError:
                self._spool_quietly(batch)
                raise

    async def _write_or_spool(self, batch: DuckLakeBatch) -> None:
        unwritten = await self._write_tables(batch)
        self.counters.rows_written += len(batch.requests) - len(unwritten.requests)
        if not unwritten.requests and not unwritten.payloads:
            return
        self.counters.flushes_failed += 1
        try:
            dropped = await asyncio.to_thread(self._spool.write, unwritten)
            self.counters.batches_dropped += dropped
        except Exception:
            verbose_logger.exception("ducklake sink: spool write failed; batch lost")
            self.counters.batches_dropped += 1

    def _spool_quietly(self, batch: DuckLakeBatch) -> None:
        try:
            dropped = self._spool.write(batch)
        except Exception:
            verbose_logger.exception("ducklake sink: spool write failed; batch lost")
            self.counters.batches_dropped += 1
            return
        self.counters.batches_dropped += dropped

    async def _write_tables(self, batch: DuckLakeBatch) -> DuckLakeBatch:
        request_stmt, payload_stmt = self._insert_statements(batch)
        requests_ok = request_stmt is None or await self._execute_with_retry(request_stmt)
        payloads_ok = payload_stmt is None or await self._execute_with_retry(payload_stmt)
        return DuckLakeBatch(
            requests=() if requests_ok else batch.requests,
            payloads=() if payloads_ok else batch.payloads,
        )

    def _execute_bootstrapped(self, statement: str) -> None:
        if not self._bootstrapped:
            for ddl in ddl_statements(self.sink_config.schema_name):
                self._executor.execute_script(ddl)
            self._bootstrapped = True
        self._executor.execute(statement)

    async def _execute_with_retry(self, statement: str) -> bool:
        for attempt in range(self.sink_config.flush_max_attempts):
            try:
                await asyncio.to_thread(self._execute_bootstrapped, statement)
                return True
            except Exception as flush_error:
                # The server may have lost the tables (restart, recreated
                # database); force the idempotent DDL to run again next time.
                self._bootstrapped = False
                verbose_logger.warning(
                    "ducklake sink: flush attempt %s/%s failed: %s",
                    attempt + 1,
                    self.sink_config.flush_max_attempts,
                    flush_error,
                )
                if attempt + 1 < self.sink_config.flush_max_attempts:
                    await asyncio.sleep(min(2**attempt, BACKOFF_CAP_SECONDS))
        return False

    async def _replay_spool(self) -> None:
        for _ in range(REPLAY_BATCHES_PER_CYCLE):
            spooled = await asyncio.to_thread(self._spool.claim_oldest)
            if spooled is None:
                return
            unwritten = await self._write_tables(spooled.batch)
            if not unwritten.requests and not unwritten.payloads:
                self.counters.rows_written += len(spooled.batch.requests)
                await asyncio.to_thread(self._spool.delete, spooled.path)
                continue
            self.counters.rows_written += len(spooled.batch.requests) - len(unwritten.requests)
            if unwritten == spooled.batch:
                await asyncio.to_thread(self._spool.release, spooled.path)
                return
            await asyncio.to_thread(self._spool.delete, spooled.path)
            dropped = await asyncio.to_thread(self._spool.write, unwritten)
            self.counters.batches_dropped += dropped
            return

    async def periodic_flush(self):
        if not self.sink_config.enabled:
            return
        while True:
            await asyncio.sleep(self.flush_interval)
            await self.flush_queue()
            if self.flush_lock is not None:
                async with self.flush_lock:
                    try:
                        await self._replay_spool()
                    except Exception:
                        verbose_logger.exception("ducklake sink: spool replay failed")

    async def drain(self) -> None:
        if not self.sink_config.enabled:
            return
        flush_task = asyncio.ensure_future(self.flush_queue())
        try:
            await asyncio.wait_for(asyncio.shield(flush_task), timeout=self.sink_config.drain_timeout)
        except TimeoutError:
            verbose_logger.warning(
                "ducklake sink: drain timed out after %ss; flush continues in background",
                self.sink_config.drain_timeout,
            )

    def drain_sync(self) -> None:
        if not self.sink_config.enabled:
            return
        batch = self._snapshot_batch()
        if not batch.requests and not batch.payloads:
            return
        request_stmt, payload_stmt = self._insert_statements(batch)
        requests_ok = request_stmt is None or self._try_execute_sync(request_stmt)
        payloads_ok = payload_stmt is None or self._try_execute_sync(payload_stmt)
        unwritten = DuckLakeBatch(
            requests=() if requests_ok else batch.requests,
            payloads=() if payloads_ok else batch.payloads,
        )
        self.counters.rows_written += len(batch.requests) - len(unwritten.requests)
        if unwritten.requests or unwritten.payloads:
            self.counters.flushes_failed += 1
            self._spool_quietly(unwritten)

    def _try_execute_sync(self, statement: str) -> bool:
        try:
            self._execute_bootstrapped(statement)
            return True
        except Exception:
            verbose_logger.exception("ducklake sink: sync drain write failed")
            return False

    def spool_size_bytes(self) -> int:
        return 0 if self._spool is None else self._spool.size_bytes()
