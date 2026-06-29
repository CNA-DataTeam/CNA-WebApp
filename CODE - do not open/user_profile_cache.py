"""
user_profile_cache.py

Local, per-user fallback cache of the CURRENT user's row from the network
``users.parquet``.

Why this exists:
    Identity data (full name, department, admin/developer flags) is normally
    resolved by reading ``users.parquet`` from the network share. When the share
    is unreachable (no VPN / not in an office) those reads come back empty and the
    app falls back to the raw Windows login (e.g. "jfitouri" instead of "Jennifer
    Fitouri"), which can then get written into saved data.

    startup.py writes this small JSON cache on every launch WHEN CONNECTED. utils
    reads it as a fallback so the current user's identity still resolves while
    offline, using last-known-good values instead of the bare login.

Design notes:
    - Streamlit-free (startup.py avoids importing Streamlit); stdlib + pandas only.
    - Stored next to the app code, which is a per-user local install, so the file
      only ever holds the current user's own row. The stored login is re-checked
      on read, so a stray/copied cache for a different account is ignored rather
      than used to mislabel the current user.
    - Best-effort everywhere: a failed write leaves the previous cache intact; a
      failed read returns None and the caller falls back to today's behavior.
"""
from __future__ import annotations

import getpass
import json
import logging
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger("user_profile_cache")

# Lives beside the app code (a per-user local install), next to favorites.json /
# ta_account_favorites.json. Untracked, so it survives `git reset --hard` during
# Repair; gitignored so it is never committed.
CACHE_FILE = Path(__file__).resolve().parent / "user_profile_cache.json"


def _normalize_login(value: object) -> str:
    """Strip domain / UPN and lowercase a login to a stable comparison key."""
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.replace("/", "\\")
    if "\\" in text:
        text = text.split("\\")[-1]
    if "@" in text:
        text = text.split("@")[0]
    return text.strip()


def _current_login() -> str:
    return _normalize_login(getpass.getuser())


def _json_safe(value: object):
    """Coerce a cell value to something json.dumps can write with allow_nan=False."""
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass  # non-scalar (unexpected here) — fall through to stringify
    if value is None or isinstance(value, (str, int, bool, float)):
        return value
    return str(value)


def save_current_user_profile(users_df: "pd.DataFrame") -> bool:
    """Cache the current OS user's row from a users DataFrame. Best-effort.

    Returns True if a matching row was found and written. Any failure (no match,
    unusable frame, write error) returns False and leaves the existing cache
    untouched — which is exactly what we want when offline: keep last-known-good.
    """
    try:
        if users_df is None or getattr(users_df, "empty", True):
            return False
        cols = {str(c).strip().lower(): c for c in users_df.columns}
        user_col = cols.get("user")
        if not user_col:
            return False
        login = _current_login()
        if not login:
            return False
        normalized = users_df[user_col].map(_normalize_login)
        match = users_df[normalized == login]
        if match.empty:
            return False
        row = {str(k): _json_safe(v) for k, v in match.iloc[0].to_dict().items()}
        payload = {"login": login, "row": row}
        tmp = CACHE_FILE.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False),
            encoding="utf-8",
        )
        tmp.replace(CACHE_FILE)  # atomic swap; a reader never sees a half file
        return True
    except Exception as exc:
        LOGGER.warning("Failed to write local user profile cache: %s", exc)
        return False


def load_current_user_profile() -> dict | None:
    """Return the cached row dict for the CURRENT OS user, or None.

    Ignores a cache whose stored login doesn't match the current user, so a file
    left over from a different account is never used to mislabel this one.
    """
    try:
        if not CACHE_FILE.exists():
            return None
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        if _normalize_login(data.get("login")) != _current_login():
            return None
        row = data.get("row")
        return row if isinstance(row, dict) and row else None
    except Exception as exc:
        LOGGER.warning("Failed to read local user profile cache: %s", exc)
        return None
