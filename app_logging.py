from __future__ import annotations

import atexit
import logging
from functools import lru_cache
from pathlib import Path
import tempfile

import config

_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOGGER_NAME = "cna_webapp"
_APP_ROOT = Path(__file__).resolve().parent


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
    user_key = config.get_log_user()
    candidates = [
        (config.get_log_file_for_user(user_key), False),
        (_APP_ROOT / "logs" / user_key / config.LOG_USER_FILE_NAME, True),
        (Path(tempfile.gettempdir()) / "cna-webapp-logs" / user_key / config.LOG_USER_FILE_NAME, True),
    ]

    log_file: Path | None = None
    using_fallback = False
    for candidate_path, candidate_is_fallback in candidates:
        try:
            candidate_path.parent.mkdir(parents=True, exist_ok=True)
            with open(candidate_path, "a", encoding="utf-8"):
                pass
            log_file = candidate_path
            using_fallback = candidate_is_fallback
            break
        except Exception:
            continue

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if log_file is None:
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        return logger

    target = str(log_file.resolve())
    has_target_handler = False
    for handler in list(logger.handlers):
        if not isinstance(handler, logging.FileHandler):
            continue
        handler_path = str(Path(handler.baseFilename).resolve())
        if handler_path == target:
            has_target_handler = True
            continue
        logger.removeHandler(handler)
        handler.close()

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

