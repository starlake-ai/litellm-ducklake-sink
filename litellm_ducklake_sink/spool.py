from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime
from itertools import accumulate
from pathlib import Path

from litellm_ducklake_sink.types import DuckLakeBatch, DuckLakePayloadRow, DuckLakeRequestRow

_STALE_CLAIM_SECONDS = 900


@dataclass(frozen=True, slots=True)
class SpooledBatch:
    path: Path
    batch: DuckLakeBatch


def _encode(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def batch_to_json(batch: DuckLakeBatch) -> str:
    return json.dumps(
        {
            "requests": [{k: _encode(v) for k, v in asdict(row).items()} for row in batch.requests],
            "payloads": [{k: _encode(v) for k, v in asdict(row).items()} for row in batch.payloads],
        }
    )


def _size_or_zero(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _request_from_dict(raw: dict[str, object]) -> DuckLakeRequestRow:
    return DuckLakeRequestRow(
        **{
            **raw,
            "request_day": date.fromisoformat(raw["request_day"]),
            "request_ts": datetime.fromisoformat(raw["request_ts"]),
        }
    )


def _payload_from_dict(raw: dict[str, object]) -> DuckLakePayloadRow:
    return DuckLakePayloadRow(**{**raw, "request_day": date.fromisoformat(raw["request_day"])})


def batch_from_json(text: str) -> DuckLakeBatch:
    raw = json.loads(text)
    return DuckLakeBatch(
        requests=tuple(_request_from_dict(r) for r in raw["requests"]),
        payloads=tuple(_payload_from_dict(p) for p in raw["payloads"]),
    )


class DiskSpool:
    def __init__(self, directory: str, max_bytes: int) -> None:
        self._dir = Path(directory)
        self._max_bytes = max_bytes
        self._dir.mkdir(parents=True, exist_ok=True)

    def _files(self) -> tuple[Path, ...]:
        return tuple(sorted(self._dir.glob("batch-*.json")))

    def _claimed_files(self) -> tuple[Path, ...]:
        return tuple(sorted(self._dir.glob("batch-*.json.claimed")))

    def size_bytes(self) -> int:
        return sum(_size_or_zero(path) for path in self._files() + self._claimed_files())

    def write(self, batch: DuckLakeBatch) -> int:
        name = f"batch-{time.time_ns():020d}-{uuid.uuid4().hex[:8]}.json"
        (self._dir / name).write_text(batch_to_json(batch), encoding="utf-8")
        return self._evict()

    def _evict(self) -> int:
        files = self._files()
        sizes = tuple(_size_or_zero(path) for path in files)
        total = sum(sizes)
        if total <= self._max_bytes:
            return 0
        prefix_sums = tuple(accumulate(sizes))
        cut = next(i + 1 for i, prefix in enumerate(prefix_sums) if total - prefix <= self._max_bytes)
        for path in files[:cut]:
            path.unlink(missing_ok=True)
        return cut

    def oldest(self) -> SpooledBatch | None:
        for path in self._files():
            try:
                return SpooledBatch(path=path, batch=batch_from_json(path.read_text(encoding="utf-8")))
            except (ValueError, KeyError, TypeError):
                path.unlink(missing_ok=True)
            except OSError:
                continue
        return None

    def _sweep_stale_claims(self, now: float) -> None:
        for path in self._claimed_files():
            try:
                stale = now - path.stat().st_mtime >= _STALE_CLAIM_SECONDS
            except OSError:
                continue
            if stale:
                self.release(path)

    def claim_oldest(self, now: float | None = None) -> SpooledBatch | None:
        self._sweep_stale_claims(time.time() if now is None else now)
        for path in self._files():
            claimed_path = path.with_name(path.name + ".claimed")
            try:
                path.rename(claimed_path)
            except FileNotFoundError:
                continue
            try:
                batch = batch_from_json(claimed_path.read_text(encoding="utf-8"))
            except (ValueError, KeyError, TypeError):
                claimed_path.unlink(missing_ok=True)
                continue
            except OSError:
                self.release(claimed_path)
                continue
            return SpooledBatch(path=claimed_path, batch=batch)
        return None

    def release(self, claimed_path: Path) -> None:
        try:
            claimed_path.rename(claimed_path.with_name(claimed_path.name.removesuffix(".claimed")))
        except OSError:
            pass

    def delete(self, path: Path) -> None:
        path.unlink(missing_ok=True)
