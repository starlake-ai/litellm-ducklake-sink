from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class DuckLakeRequestRow:
    request_id: str
    request_day: date
    request_ts: datetime
    api_key_hash: str | None
    api_key_alias: str | None
    team_id: str | None
    end_user_id: str | None
    model_group: str | None
    deployment_model: str | None
    provider: str | None
    prompt_tokens: int
    completion_tokens: int
    cached_tokens: int
    spend: float
    latency_ms: float | None
    time_to_first_token_ms: float | None
    cache_hit: bool
    status: str
    error_type: str | None
    client_source: str | None


@dataclass(frozen=True, slots=True)
class DuckLakePayloadRow:
    request_id: str
    request_day: date
    messages: str | None
    response: str | None
    payload_bytes: int


@dataclass(frozen=True, slots=True)
class DuckLakeBatch:
    requests: tuple[DuckLakeRequestRow, ...]
    payloads: tuple[DuckLakePayloadRow, ...]
