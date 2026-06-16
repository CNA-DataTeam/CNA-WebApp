---
name: uv install location drift breaks setup.bat detection
description: Why "uv was installed but could not be located" — uv now installs to %USERPROFILE%\.local\bin, and how setup.bat/repair.bat now locate uv and git
metadata:
  type: project
---

**SYMPTOM (June 2026, multiple users):** `setup.bat` (run via installer or directly) fails with:

```
uv not found. Installing automatically...
downloading uv 0.11.21 (x86_64-pc-windows-msvc)
installing to C:\Users\<user>\.local\bin
...
everything's installed!
ERROR: uv was installed but could not be located. Please restart this script.
```

uv **is** installed — setup just looked in the wrong place.

**ROOT CAUSE:** uv's standalone installer changed its default install directory. Older uv landed in `%LOCALAPPDATA%\uv\bin` (or `%APPDATA%\uv\bin`); **uv 0.11.x installs to `%USERPROFILE%\.local\bin`** (the XDG user-exec dir — confirmed by the installer's own "installing to C:\Users\…\.local\bin" line). The old `setup.bat` only checked the two legacy dirs plus:
- `where uv` — fails because the running `cmd.exe` has a **stale PATH**; the installer's PATH edit only applies to *new* processes.
- `where /R "%USERPROFILE%" uv.exe` — a recursive scan that is slow/unreliable on **OneDrive/KFM-redirected corporate profiles** (crawls the synced tree with cloud placeholders/reparse points and bails before reaching `.local\bin`).

So every fallback missed the real location → the misleading "could not be located" error.

**RELATED: `repair.bat` "git not found".** `repair.bat` trusted bare `where git`. Git is installed by the installer via `winget install --scope user` into `%LOCALAPPDATA%\Programs\Git` and added only to the **user PATH in the registry** — invisible to the (pre-existing) process that launches repair. That's why the installer clones fine (its `.iss` `FindGitExe` probes disk) but Repair claimed git was missing. Same PATH-staleness class of bug.

**FIX (implemented June 2026):**
- `setup.bat`: pins `UV_INSTALL_DIR=%ROOT_DIR%\.uv\bin` (uv installer honors it; consistent with the existing `UV_PYTHON_INSTALL_DIR`/`UV_CACHE_DIR` pins) so fresh installs land in a deterministic, redirection-safe spot. New `:LOCATE_UV` subroutine checks, in order: PATH → `UV_INSTALL_DIR` → `%USERPROFILE%\.local\bin` → legacy `%LOCALAPPDATA%`/`%APPDATA%`. Recursive `where /R` scan removed.
- `repair.bat`: new `:FIND_GIT` subroutine probes git on disk (`%ProgramFiles%`, `(x86)`, `%LOCALAPPDATA%\Programs\Git`) then PATH — mirrors the installer's `FindGitExe`. All git calls go through `!GIT_EXE!`; git's folder is prepended to PATH before calling setup so setup's own self-heal finds it too.
- `app.py` and `check_updates.py`: both have their own `_find_uv()` (used for the post-pull `uv pip install`) — updated to the same order (`ROOT_DIR/.uv/bin`, `~/.local/bin`, then legacy). **If you touch uv-location logic, update all three: setup.bat `:LOCATE_UV`, app.py `_find_uv`, check_updates.py `_find_uv`.**

**IMMEDIATE WORKAROUND for an already-stuck user (still on old setup.bat):** uv is already installed and the installer already added `.local\bin` to the user PATH — so **sign out and back in (or reboot), then re-run setup.bat**. A fresh session sees the new PATH and the old script's first `where uv` check succeeds. The permanent fix only reaches them via a new installer/release (or a successful `git pull`).

Distinct from [[gotchas_installer_untrusted_mount]] (error 448 / RedirectionGuard) — that's a *traversal* block, this is a *location-detection* miss. Touches the same scripts; see [[feedback_git_bash_bat_files]] and [[feedback_cmd_errorlevel_redirect]] for batch pitfalls.
