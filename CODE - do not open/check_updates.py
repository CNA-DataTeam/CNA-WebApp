"""
Background update checker — called by StartApp.bat.

Checks if a git update check has already been done today.
If not, runs git fetch and writes a flag file when updates are available.
"""

import subprocess
import sys
from datetime import date
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
CHECK_FILE = APP_DIR / ".last_update_check"
UPDATE_FLAG = APP_DIR / ".update_available"


def already_checked_today() -> bool:
    """Return True if we already ran a git check today."""
    if not CHECK_FILE.exists():
        return False
    try:
        return CHECK_FILE.read_text().strip() == date.today().isoformat()
    except Exception:
        return False


def run_check():
    if already_checked_today():
        return

    git_dir = ROOT_DIR / ".git"
    if not git_dir.exists():
        return

    env = {
        "GIT_HTTP_LOW_SPEED_LIMIT": "1000",
        "GIT_HTTP_LOW_SPEED_TIME": "10",
    }

    # Merge with current environment
    import os
    full_env = {**os.environ, **env}

    try:
        result = subprocess.run(
            ["git", "fetch", "--prune"],
            cwd=ROOT_DIR,
            capture_output=True,
            timeout=30,
            env=full_env,
        )
        if result.returncode != 0:
            return
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return

    # Check if behind origin
    try:
        result = subprocess.run(
            ["git", "status", "-uno"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if "behind" in result.stdout:
            UPDATE_FLAG.write_text(date.today().isoformat())
        else:
            UPDATE_FLAG.unlink(missing_ok=True)
    except Exception:
        return

    # Record that we checked today
    try:
        CHECK_FILE.write_text(date.today().isoformat())
    except Exception:
        pass


if __name__ == "__main__":
    run_check()
