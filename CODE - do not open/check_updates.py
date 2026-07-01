"""
Background update checker — called by StartApp.bat at startup (synchronously,
before the app boots).

On EVERY launch (lightweight, so a no-update launch is near-seamless):
  1. A quick `git ls-remote` of origin/main, compared to local HEAD. When they
     match — the common case — that's a single small round-trip and we're done.
  2. If origin/main has a new commit, attempt a clean fast-forward pull after
     discarding local modifications to known-regenerated build artifacts (legacy
     holdover from when the exe/spec were tracked).
  3. After pull success, run `uv pip install -r requirements.txt` so any
     dependency changes in the new commits land before the app boots.
  4. After pull success, if the launcher exe is missing (because the pull deleted
     a previously-tracked copy), run setup.bat /silent to rebuild it.
  5. On pull success, clear .update_available so the in-app dialog is skipped —
     the new code is already on disk before Streamlit boots.
  6. On pull failure, set .update_available so the blocking in-app dialog (with
     its surfaced error message) reaches the user on next render.

The running app also watches origin for commits that land mid-session (see
app.py's _update_watcher / soft banner) and reuses remote_is_ahead() below — the
shared read-only "is there a new commit?" check.
"""

import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
UPDATE_FLAG = APP_DIR / ".update_available"
LAUNCHER_EXE = ROOT_DIR / "CNA Web App.exe"
SETUP_BAT = ROOT_DIR / "setup.bat"
REQUIREMENTS_FILE = APP_DIR / "requirements.txt"
VENV_DIR = ROOT_DIR / ".venv"

# Build artifacts that may still be tracked in clones from before they were
# gitignored. Discarding local changes to these before pulling is safe
# because setup.bat regenerates them on demand. Once a clone has pulled the
# commit that untracks them, these checkouts become silent no-ops.
REGENERATED_TRACKED_ARTIFACTS = (
    "CNA Web App.exe",
    "CODE - do not open/installer/CNA Web App.spec",
)


_subprocess_run = subprocess.run


def _silent_run(*args, **kwargs):
    """subprocess.run that never flashes a console window on Windows.

    The app runs windowless, so a console child (git/uv/cmd) would briefly pop a
    terminal unless CREATE_NO_WINDOW is set. Every call routed here is silent /
    background, so the flag is applied uniformly."""
    if os.name == "nt":
        kwargs.setdefault("creationflags", subprocess.CREATE_NO_WINDOW)
    return _subprocess_run(*args, **kwargs)


def _low_speed_env() -> dict:
    """Env that makes git abort a stalled transfer quickly and never block on a
    credential/terminal prompt (the running app's watcher thread has no TTY, so a
    prompt would hang it). Reads on a public repo are anonymous anyway."""
    return {
        **os.environ,
        "GIT_HTTP_LOW_SPEED_LIMIT": "1000",
        "GIT_HTTP_LOW_SPEED_TIME": "10",
        "GIT_TERMINAL_PROMPT": "0",
    }


def remote_is_ahead() -> bool:
    """True only when origin/main has a commit we don't have AND we can cleanly
    fast-forward to it (i.e. we're strictly behind on the main branch).

    The common case — already up to date — is a single small `git ls-remote`
    round-trip (no object download), so this is cheap to run on every launch and
    from the running app's background watcher. Only when the remote SHA actually
    differs do we confirm it's a real fast-forward: a SHA difference ALONE would
    also fire for a local-ahead / diverged / detached / off-main checkout, and
    auto-pulling those would fail and wedge the user behind the blocking update
    dialog. Read-only throughout (ls-remote + an objects-only fetch when needed) —
    never touches the working tree. Returns False on any error/offline."""
    git_dir = ROOT_DIR / ".git"
    if not git_dir.exists():
        return False
    try:
        ls = _git(["ls-remote", "origin", "refs/heads/main"], timeout=15, env=_low_speed_env())
        local = _git(["rev-parse", "HEAD"], timeout=10)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False
    if ls.returncode != 0 or local.returncode != 0:
        return False
    remote_sha = ls.stdout.split()[0].strip() if ls.stdout.strip() else ""
    local_sha = local.stdout.strip()
    if not remote_sha or not local_sha or remote_sha == local_sha:
        return False  # up to date (or can't tell) — the cheap, common fast path

    # The remote SHA differs. Only report an update when we're cleanly BEHIND
    # origin/main on the main branch (a fast-forward is possible) — not when the
    # local checkout is ahead / diverged / detached / on another branch.
    try:
        branch = _git(["rev-parse", "--abbrev-ref", "HEAD"], timeout=10)
        if branch.returncode != 0 or branch.stdout.strip() != "main":
            return False
        # ls-remote didn't download the object, so fetch main (objects/refs only —
        # no working-tree change) before testing ancestry.
        fetch = _git(["fetch", "origin", "main", "--quiet"], timeout=30, env=_low_speed_env())
        if fetch.returncode != 0:
            return False
        ancestor = _git(["merge-base", "--is-ancestor", "HEAD", remote_sha], timeout=10)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False
    return ancestor.returncode == 0


def _git(args: list[str], timeout: int = 30, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return _silent_run(
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


def _find_uv() -> str | None:
    """Locate the uv executable. Mirrors setup.bat's :LOCATE_UV search order:
    PATH first, then every known on-disk install location.

    uv's standalone-installer default has drifted across versions: current
    0.11.x installs to %USERPROFILE%\\.local\\bin, older builds used
    %LOCALAPPDATA%\\uv\\bin. setup.bat also pins fresh installs to
    ROOT_DIR\\.uv\\bin via UV_INSTALL_DIR, so we check that first.
    """
    found = shutil.which("uv")
    if found:
        return found
    candidates = [
        ROOT_DIR / ".uv" / "bin" / "uv.exe",       # pinned by setup.bat (UV_INSTALL_DIR)
        Path.home() / ".local" / "bin" / "uv.exe",  # modern uv default (~/.local/bin)
    ]
    for env_var in ("LOCALAPPDATA", "APPDATA"):     # legacy uv locations
        base = os.environ.get(env_var)
        if base:
            candidates.append(Path(base) / "uv" / "bin" / "uv.exe")
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def _refresh_dependencies() -> None:
    """Run `uv pip install -r requirements.txt` after a successful pull.

    Catches commits that add or upgrade Python dependencies — the routine
    pull updates requirements.txt but doesn't install. uv is fast on no-op
    installs (~1-2s), so we run unconditionally rather than diffing the
    file. Best-effort: failures are logged to stderr (which StartApp.bat
    redirects to the per-launch log) and the user can recover via
    Settings > Repair App if a dependency lands broken.
    """
    if not REQUIREMENTS_FILE.exists() or not VENV_DIR.exists():
        return
    uv_exe = _find_uv()
    if uv_exe is None:
        print(
            "[check_updates] uv not on PATH or in standard locations; "
            "skipping post-pull dependency refresh.",
            file=sys.stderr,
        )
        return
    env = {**os.environ, "VIRTUAL_ENV": str(VENV_DIR)}
    try:
        result = _silent_run(
            [uv_exe, "pip", "install", "--link-mode", "copy",
             "-r", str(REQUIREMENTS_FILE)],
            cwd=ROOT_DIR,
            capture_output=True,
            timeout=300,
            env=env,
        )
        if result.returncode != 0:
            print(
                f"[check_updates] dependency refresh failed (rc={result.returncode}):",
                file=sys.stderr,
            )
            print(
                result.stderr.decode("utf-8", errors="replace"),
                file=sys.stderr,
            )
    except Exception as exc:
        print(f"[check_updates] dependency refresh exception: {exc}", file=sys.stderr)


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
        _silent_run(
            ["cmd.exe", "/c", str(SETUP_BAT), "/silent"],
            cwd=ROOT_DIR,
            capture_output=True,
            timeout=600,
        )
    except Exception:
        pass


def run_check() -> None:
    git_dir = ROOT_DIR / ".git"
    if not git_dir.exists():
        return

    # Lightweight, every-launch check: a quick ls-remote tells us whether
    # origin/main has a commit we don't have. When it doesn't (the common case)
    # we're done in one small round-trip, so running this on every launch stays
    # near-seamless. Only when behind do we do the heavier pull + dependency
    # refresh — and only then, before the app boots.
    if not remote_is_ahead():
        UPDATE_FLAG.unlink(missing_ok=True)
        return

    _discard_regenerated_artifacts()

    try:
        pull = _git(["pull", "--ff-only"], timeout=60, env=_low_speed_env())
    except Exception:
        UPDATE_FLAG.write_text(date.today().isoformat())
        return

    if pull.returncode == 0:
        _refresh_dependencies()
        _rebuild_launcher_if_missing()
        UPDATE_FLAG.unlink(missing_ok=True)
    else:
        UPDATE_FLAG.write_text(date.today().isoformat())


if __name__ == "__main__":
    run_check()
