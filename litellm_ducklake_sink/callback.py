from __future__ import annotations

import atexit
from collections.abc import Callable

from litellm._logging import verbose_logger

from litellm_ducklake_sink.config import DuckLakeSinkMisconfiguredError
from litellm_ducklake_sink.ducklake_logger import DuckLakeLogger


def build_instance(
    register_shutdown: Callable[[Callable[[], None]], object] = atexit.register,
) -> DuckLakeLogger:
    try:
        logger = DuckLakeLogger()
    except DuckLakeSinkMisconfiguredError as error:
        # The host prints a long startup traceback; this single line names the
        # cause and the way out before it.
        verbose_logger.error("%s", error)
        raise
    if logger.sink_config.enabled:
        register_shutdown(logger.drain_sync)
    return logger


instance = build_instance()
