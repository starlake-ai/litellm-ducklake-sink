from __future__ import annotations

import atexit
from collections.abc import Callable

from litellm_ducklake_sink.ducklake_logger import DuckLakeLogger


def build_instance(
    register_shutdown: Callable[[Callable[[], None]], object] = atexit.register,
) -> DuckLakeLogger:
    logger = DuckLakeLogger()
    if logger.sink_config.enabled:
        register_shutdown(logger.drain_sync)
    return logger


instance = build_instance()
