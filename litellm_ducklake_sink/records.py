from __future__ import annotations

import json
from datetime import UTC, datetime

from litellm.types.utils import StandardLoggingPayload

from litellm_ducklake_sink.types import DuckLakePayloadRow, DuckLakeRequestRow


def _utc(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=UTC)


def _cached_tokens(payload: StandardLoggingPayload) -> int:
    usage = payload["metadata"].get("usage_object") or {}
    details = usage.get("prompt_tokens_details") if isinstance(usage, dict) else None
    cached = details.get("cached_tokens") if isinstance(details, dict) else None
    return cached if isinstance(cached, int) else 0


def _time_to_first_token_ms(payload: StandardLoggingPayload) -> float | None:
    start = payload["startTime"]
    completion_start = payload.get("completionStartTime") or 0.0
    if completion_start <= start:
        return None
    return (completion_start - start) * 1000.0


def _error_type(payload: StandardLoggingPayload) -> str | None:
    info = payload.get("error_information") or {}
    return info.get("error_class") or None


def _client_source(payload: StandardLoggingPayload) -> str | None:
    return payload.get("user_agent") or payload["metadata"].get("user_agent")


def build_request_row(payload: StandardLoggingPayload) -> DuckLakeRequestRow:
    metadata = payload["metadata"]
    ts = _utc(payload["startTime"])
    return DuckLakeRequestRow(
        request_id=payload["id"],
        request_day=ts.date(),
        request_ts=ts,
        api_key_hash=metadata.get("user_api_key_hash"),
        api_key_alias=metadata.get("user_api_key_alias"),
        team_id=metadata.get("user_api_key_team_id"),
        end_user_id=payload.get("end_user"),
        model_group=payload.get("model_group"),
        deployment_model=payload["model"],
        provider=payload.get("custom_llm_provider"),
        prompt_tokens=payload["prompt_tokens"],
        completion_tokens=payload["completion_tokens"],
        cached_tokens=_cached_tokens(payload),
        spend=payload["response_cost"],
        latency_ms=payload["response_time"] * 1000.0,
        time_to_first_token_ms=_time_to_first_token_ms(payload),
        cache_hit=bool(payload.get("cache_hit")),
        status=payload["status"],
        error_type=_error_type(payload),
        client_source=_client_source(payload),
    )


def _as_text(value: str | list | dict | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def _truncate_utf8(text: str, max_bytes: int) -> str:
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    return raw[:max_bytes].decode("utf-8", errors="ignore")


def build_payload_row(payload: StandardLoggingPayload, payload_max_bytes: int) -> DuckLakePayloadRow:
    messages = _as_text(payload.get("messages"))
    response = _as_text(payload.get("response"))
    original_bytes = sum(len(text.encode("utf-8")) for text in (messages, response) if text is not None)
    ts = _utc(payload["startTime"])
    return DuckLakePayloadRow(
        request_id=payload["id"],
        request_day=ts.date(),
        messages=None if messages is None else _truncate_utf8(messages, payload_max_bytes),
        response=None if response is None else _truncate_utf8(response, payload_max_bytes),
        payload_bytes=original_bytes,
    )
