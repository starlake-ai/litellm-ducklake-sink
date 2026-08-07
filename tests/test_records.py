from datetime import UTC, datetime

from litellm_ducklake_sink.records import build_payload_row, build_request_row

START_EPOCH = 1754300000.0
START_DAY = datetime.fromtimestamp(START_EPOCH, tz=UTC).date()


def _payload(**overrides) -> dict:
    base = {
        "id": "req-1",
        "trace_id": "trace-1",
        "call_type": "acompletion",
        "response_cost": 0.0123,
        "status": "success",
        "custom_llm_provider": "anthropic",
        "total_tokens": 30,
        "prompt_tokens": 10,
        "completion_tokens": 20,
        "startTime": START_EPOCH,
        "endTime": START_EPOCH + 2.5,
        "completionStartTime": START_EPOCH + 0.4,
        "response_time": 2.5,
        "model": "claude-fable-5",
        "model_group": "claude",
        "metadata": {
            "user_api_key_hash": "hash-1",
            "user_api_key_alias": "alias-1",
            "user_api_key_team_id": "team-1",
            "usage_object": {"prompt_tokens_details": {"cached_tokens": 7}},
        },
        "cache_hit": False,
        "end_user": "dev-42",
        "user_agent": "claude-code/2.0",
        "messages": [{"role": "user", "content": "hi"}],
        "response": {"choices": [{"message": {"content": "hello"}}]},
    }
    base.update(overrides)
    return base


def test_build_request_row_maps_metrics():
    row = build_request_row(_payload())
    assert row.request_id == "req-1"
    assert row.request_day == START_DAY
    assert row.team_id == "team-1"
    assert row.api_key_hash == "hash-1"
    assert row.end_user_id == "dev-42"
    assert row.model_group == "claude"
    assert row.deployment_model == "claude-fable-5"
    assert row.provider == "anthropic"
    assert row.prompt_tokens == 10
    assert row.completion_tokens == 20
    assert row.cached_tokens == 7
    assert row.spend == 0.0123
    assert row.latency_ms == 2500.0
    assert abs(row.time_to_first_token_ms - 400.0) < 0.001
    assert row.cache_hit is False
    assert row.status == "success"
    assert row.error_type is None
    assert row.client_source == "claude-code/2.0"


def test_build_request_row_failure_and_missing_optionals():
    payload = _payload(
        status="failure",
        error_information={"error_class": "RateLimitError", "error_code": "429"},
        completionStartTime=0.0,
        cache_hit=None,
        end_user=None,
        user_agent=None,
        model_group=None,
    )
    payload["metadata"] = {}
    row = build_request_row(payload)
    assert row.status == "failure"
    assert row.error_type == "RateLimitError"
    assert row.time_to_first_token_ms is None
    assert row.cache_hit is False
    assert row.cached_tokens == 0
    assert row.team_id is None
    assert row.client_source is None


def test_build_payload_row_serializes_and_records_size():
    row = build_payload_row(_payload(), payload_max_bytes=1024 * 1024)
    assert row.request_id == "req-1"
    assert row.messages is not None and '"role"' in row.messages
    assert row.response is not None and "hello" in row.response
    assert row.payload_bytes == len(row.messages.encode()) + len(row.response.encode())


def test_build_payload_row_truncates_but_keeps_original_size():
    payload = _payload(messages="x" * 100, response=None)
    row = build_payload_row(payload, payload_max_bytes=10)
    assert row.messages == "x" * 10
    assert row.response is None
    assert row.payload_bytes == 100
