import time
from datetime import UTC, date, datetime
from pathlib import Path

from litellm_ducklake_sink.spool import DiskSpool
from litellm_ducklake_sink.types import DuckLakeBatch, DuckLakePayloadRow, DuckLakeRequestRow


def _batch(request_id: str, payload_text: str = "hello") -> DuckLakeBatch:
    row = DuckLakeRequestRow(
        request_id=request_id,
        request_day=date(2026, 8, 4),
        request_ts=datetime(2026, 8, 4, 12, 0, 0, tzinfo=UTC),
        api_key_hash=None,
        api_key_alias=None,
        team_id="team-1",
        end_user_id=None,
        model_group="claude",
        deployment_model="claude-fable-5",
        provider="anthropic",
        prompt_tokens=1,
        completion_tokens=2,
        cached_tokens=0,
        spend=0.001,
        latency_ms=100.0,
        time_to_first_token_ms=None,
        cache_hit=False,
        status="success",
        error_type=None,
        client_source=None,
    )
    payload = DuckLakePayloadRow(
        request_id=request_id,
        request_day=date(2026, 8, 4),
        messages=payload_text,
        response=None,
        payload_bytes=len(payload_text),
    )
    return DuckLakeBatch(requests=(row,), payloads=(payload,))


def test_write_then_oldest_roundtrips(tmp_path: Path):
    spool = DiskSpool(str(tmp_path / "spool"), max_bytes=10_000_000)
    assert spool.write(_batch("req-1")) == 0
    spooled = spool.oldest()
    assert spooled is not None
    assert spooled.batch == _batch("req-1")
    spool.delete(spooled.path)
    assert spool.oldest() is None
    assert spool.size_bytes() == 0


def test_eviction_drops_oldest_and_counts(tmp_path: Path):
    spool = DiskSpool(str(tmp_path), max_bytes=10_000_000)
    spool.write(_batch("req-old"))
    single_batch_size = spool.size_bytes()
    tight = DiskSpool(str(tmp_path), max_bytes=int(single_batch_size * 2.5))
    assert tight.write(_batch("req-mid")) == 0
    assert tight.write(_batch("req-new")) == 1
    spooled = tight.oldest()
    assert spooled is not None
    assert spooled.batch.requests[0].request_id == "req-mid"


def test_corrupt_file_is_skipped_and_removed(tmp_path: Path):
    spool = DiskSpool(str(tmp_path), max_bytes=10_000_000)
    (tmp_path / "batch-00000000000000000000-dead.json").write_text("{not json", encoding="utf-8")
    spool.write(_batch("req-1"))
    spooled = spool.oldest()
    assert spooled is not None
    assert spooled.batch.requests[0].request_id == "req-1"
    assert not (tmp_path / "batch-00000000000000000000-dead.json").exists()


def test_single_oversized_batch_is_written_then_evicted(tmp_path: Path):
    spool = DiskSpool(str(tmp_path), max_bytes=10)
    assert spool.write(_batch("req-big")) == 1
    assert spool.oldest() is None


def test_claim_hides_batch_from_other_claimants(tmp_path: Path):
    spool = DiskSpool(str(tmp_path), max_bytes=10_000_000)
    spool.write(_batch("req-1"))
    claimed = spool.claim_oldest()
    assert claimed is not None
    assert claimed.batch.requests[0].request_id == "req-1"
    assert spool.claim_oldest() is None
    assert spool.oldest() is None


def test_release_makes_batch_claimable_again(tmp_path: Path):
    spool = DiskSpool(str(tmp_path), max_bytes=10_000_000)
    spool.write(_batch("req-1"))
    claimed = spool.claim_oldest()
    assert claimed is not None
    spool.release(claimed.path)
    reclaimed = spool.claim_oldest()
    assert reclaimed is not None
    assert reclaimed.batch.requests[0].request_id == "req-1"


def test_stale_claim_swept_back(tmp_path: Path):
    spool = DiskSpool(str(tmp_path), max_bytes=10_000_000)
    spool.write(_batch("req-1"))
    claimed = spool.claim_oldest()
    assert claimed is not None
    reclaimed = spool.claim_oldest(now=time.time() + 1000)
    assert reclaimed is not None
    assert reclaimed.batch.requests[0].request_id == "req-1"
