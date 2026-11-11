from pathlib import Path

import pytest
from dotenv import dotenv_values

from litellm_ducklake_sink.config import DuckLakeSinkConfig, resolve_ducklake_config

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def _resolve(name: str) -> DuckLakeSinkConfig:
    values = {key: value or "" for key, value in dotenv_values(EXAMPLES / name).items()}
    result = resolve_ducklake_config(values)
    assert isinstance(result, DuckLakeSinkConfig), result
    return result


def test_sqlite_example_writes_to_a_local_sqlite_catalog():
    config = _resolve("ducklake-sqlite.env")
    assert config.catalog == "sqlite:lake/catalog.sqlite"
    assert config.is_quack_endpoint is False


def test_postgres_example_writes_to_a_postgres_catalog():
    config = _resolve("ducklake-postgres.env")
    assert config.catalog is not None and config.catalog.startswith("postgres:")


def test_qod_example_targets_the_demo_gateway_over_flight_sql():
    config = _resolve("qod.env")
    assert config.endpoint == "grpc+tls://localhost:31338"
    assert config.tls_skip_verify is True
    assert config.is_quack_endpoint is False
    assert (config.tenant, config.pool, config.schema_name) == ("acme", "bi", "tpch1")
    assert "MAINTENANCE" not in (EXAMPLES / "qod.env").read_text()
    assert "TENANT_DB" not in (EXAMPLES / "qod.env").read_text()


@pytest.mark.parametrize("name", ["ducklake-sqlite.env", "ducklake-postgres.env", "qod.env"])
def test_no_example_mentions_retention(name: str):
    assert "RETENTION" not in (EXAMPLES / name).read_text().upper()


@pytest.mark.parametrize("name", ["ducklake-sqlite.env", "ducklake-postgres.env", "qod.env"])
def test_every_example_spells_out_the_package_defaults(name: str):
    config = _resolve(name)
    defaults = DuckLakeSinkConfig()
    for field in (
        "capture_payloads",
        "batch_rows",
        "batch_interval",
        "batch_max_bytes",
        "payload_max_bytes",
        "spool_max_bytes",
        "drain_timeout",
        "flush_max_attempts",
    ):
        assert getattr(config, field) == getattr(defaults, field), field


@pytest.mark.parametrize("name", ["ducklake-sqlite.env", "ducklake-postgres.env", "qod.env"])
def test_shell_sourcing_reads_the_same_values_as_dotenv(name: str):
    # The README loads these files with `set -a; . ./.env; set +a`, so a value
    # the shell splits (spaces, quotes) would silently change the config.
    import subprocess

    script = f'set -a; . "{EXAMPLES / name}"; set +a; env -0'
    output = subprocess.run(["bash", "-c", script], capture_output=True, check=True, env={}).stdout
    sourced = dict(entry.split("=", 1) for entry in output.decode().split("\0") if "=" in entry)
    for key, value in dotenv_values(EXAMPLES / name).items():
        assert sourced.get(key) == (value or ""), key


@pytest.mark.parametrize("name", ["ducklake-sqlite.env", "ducklake-postgres.env"])
def test_uncommented_s3_block_resolves_to_an_s3_data_path(name: str, tmp_path: Path):
    text = (EXAMPLES / name).read_text()
    start = text.index("# DUCKLAKE_SINK_DATA_PATH=s3://")
    end = text.index("# DUCKLAKE_SINK_S3_USE_SSL=false") + len("# DUCKLAKE_SINK_S3_USE_SSL=false")
    block = "\n".join(line.removeprefix("# ") for line in text[start:end].splitlines())
    uncommented = tmp_path / name
    uncommented.write_text(text[:start] + block + text[end:])
    values = {key: value or "" for key, value in dotenv_values(uncommented).items()}
    config = resolve_ducklake_config(values)
    assert isinstance(config, DuckLakeSinkConfig), config
    assert config.data_path == "s3://my-bucket/llm-lake/"
    assert (config.s3_endpoint, config.s3_url_style, config.s3_use_ssl) == ("localhost:9000", "path", False)
