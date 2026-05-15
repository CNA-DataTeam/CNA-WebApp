"""
Background update checker — called by StartApp.bat at startup.

Once per day:
  1. git fetch
  2. If local branch is behind origin, attempt a clean fast-forward pull
     after discarding local modifications to known-regenerated build
     artifacts (legacy holdover from when the exe/spec were tracked).
  3. After pull success, if the launcher exe is missing (because the
     pull deleted a previously-tracked copy), run setup.bat /silent to
     rebuild it before the app continues.
  4. On pull success, clear .update_available so the in-app dialog is
     skipped — the new code is already on disk before Streamlit boots.
  5. On pull failure, set .update_available so the in-app dialog (with
     its surfaced error message) reaches the user on next render.
"""

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
CHECK_FILE = APP_DIR / ".last_update_check"
UPDATE_FLAG = APP_DIR / ".update_available"
LAUNCHER_EXE = ROOT_DIR / "CNA Web App.exe"
SETUP_BAT = ROOT_DIR / "setup.bat"

# Build artifacts that may still be tracked in clones from before they were
# gitignored. Discarding local changes to these before pulling is safe
# because setup.bat regenerates them on demand. Once a clone has pulled the
# commit that untracks them, these checkouts become silent no-ops.
REGENERATED_TRACKED_ARTIFACTS = (
    "CNA Web App.exe",
    "CODE - do not open/installer/CNA Web App.spec",
)


def already_checked_today() -> bool:
    if not CHECK_FILE.exists():
        return False
    try:
        return CHECK_FILE.read_text().strip() == date.today().isoformat()
    except Exception:
        return False


def _git(args: list[str], timeout: int = 30, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def _discard_regenerated_artifacts() -> None:
    for artifact in REGENERATED_TRACKED_ARTIFACTS:
        try:
            _git(["checkout", "--", artifact], timeout=15)
        except Exception:
            pass


def _rebuild_launcher_if_missing() -> None:
    """If the pull deleted a previously-tracked exe, run setup.bat to rebuild.

    Best-effort and silent: setup.bat's own SKIP_BUILD logic detects the
    missing exe and triggers PyInstaller. If setup.bat isn't found or the
    rebuild fails, the user will get the explicit "exe is missing" error
    on next launch from setup.bat's verification step.
    """
    if LAUNCHER_EXE.exists():
        return
    if not SETUP_BAT.exists():
        return
    try:
        subprocess.run(
            ["cmd.exe", "/c", str(SETUP_BAT), "/silent"],
            cwd=ROOT_DIR,
            capture_output=True,
            timeout=600,
        )
    except Exception:
        pass


def run_check() -> None:
    if already_checked_today():
        return

    git_dir = ROOT_DIR / ".git"
    if not git_dir.exists():
        return

    full_env = {
        **os.environ,
        "GIT_HTTP_LOW_SPEED_LIMIT": "1000",
        "GIT_HTTP_LOW_SPEED_TIME": "10",
    }

    try:
        fetch = _git(["fetch", "--prune"], timeout=30, env=full_env)
        if fetch.returncode != 0:
            return
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return

    try:
        status = _git(["status", "-uno"], timeout=10)
    except Exception:
        return

    if "behind" not in status.stdout:
        UPDATE_FLAG.unlink(missing_ok=True)
        try:
            CHECK_FILE.write_text(date.today().isoformat())
        except Exception:
            pass
        return

    _discard_regenerated_artifacts()

    try:
        pull = _git(["pull", "--ff-only"], timeout=60, env=full_env)
    except Exception:
        UPDATE_FLAG.write_text(date.today().isoformat())
        return

    if pull.returncode == 0:
        _rebuild_launcher_if_missing()
        UPDATE_FLAG.unlink(missing_ok=True)
    else:
        UPDATE_FLAG.write_text(date.today().isoformat())

    try:
        CHECK_FILE.write_text(date.today().isoformat())
    except Exception:
        pass


if __name__ == "__main__":
    run_check()
