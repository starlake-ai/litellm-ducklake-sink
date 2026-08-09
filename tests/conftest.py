import os

import pytest


@pytest.fixture(autouse=True)
def _clean_ducklake_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("DUCKLAKE_SINK_"):
            monkeypatch.delenv(key)
