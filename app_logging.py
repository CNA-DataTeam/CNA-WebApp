from __future__ import annotations

import atexit
import logging
from functools import lru_cache
from pathlib import Path

import config

_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOGGER_NAME = "cna_webapp"


class _SourceLogger(logging.LoggerAdapter):
    """Logger adapter that prepends source file and optional context."""

    def process(self, msg, kwargs):
        source_name = str(self.extra.get("source_name", "application.py"))
        context_name = str(self.extra.get("context_name", "")).strip()
        if context_name:
            return f"[{source_name}] [{context_name}] {msg}", kwargs
        return f"[{source_name}] {msg}", kwargs


@lru_cache(maxsize=1)
def _get_base_logger() -> logging.Logger:
    log_file = config.get_log_file_for_user()
    fallback_dir = Path.cwd() / "logs" / config.get_log_user()
    fallback_dir.mkdir(parents=True, exist_ok=True)
    fallback_file = fallback_dir / config.LOG_USER_FILE_NAME

    using_fallback = False
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "a", encoding="utf-8"):
            pass
    except Exception:
        log_file = fallback_file
        using_fallback = True

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    target = str(log_file.resolve())
    has_target_handler = False
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and str(Path(handler.baseFilename).resolve()) == target:
            has_target_handler = True
            break

    if not has_target_handler:
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT))
        logger.addHandler(handler)

    logger.info(
        "[Logging System] Active log file: %s%s",
        str(log_file),
        " (local fallback)" if using_fallback else "",
    )

    atexit.register(lambda: logger.info(""))
    return logger


def get_logger(source_file: str, context_name: str | None = None) -> logging.LoggerAdapter:
    """Return a shared-file logger adapter with source/context prefixes."""
    source_name = Path(source_file).name or "application.py"
    return _SourceLogger(
        _get_base_logger(),
        {
            "source_name": source_name,
            "context_name": context_name or "",
        },
    )

