from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

import config

_LOG_FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOGGER_NAME = "cna_webapp"


class _SourceLogger(logging.LoggerAdapter):
    """Logger adapter that prepends a single readable source/context label."""

    def process(self, msg, kwargs):
        source_name = str(self.extra.get("source_name", "application.py"))
        context_name = str(self.extra.get("context_name", "")).strip()
        label = context_name or source_name
        return f"[{label}] {msg}", kwargs


@lru_cache(maxsize=1)
def _get_base_logger() -> logging.Logger:
    log_file = config.get_log_file_for_user()
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "a", encoding="utf-8"):
            pass
    except Exception:
        log_file = None

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    target = str(Path(log_file).resolve()) if log_file is not None else None
    has_target_handler = False if target else True
    for handler in list(logger.handlers):
        if target is not None and isinstance(handler, logging.FileHandler):
            handler_path = str(Path(handler.baseFilename).resolve())
            if handler_path == target:
                has_target_handler = True
                continue
        if isinstance(handler, logging.NullHandler) and target is None:
            has_target_handler = True
            continue
        logger.removeHandler(handler)
        handler.close()

    if target is not None and not has_target_handler:
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT))
        logger.addHandler(handler)
    if target is None and not has_target_handler:
        logger.addHandler(logging.NullHandler())
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

