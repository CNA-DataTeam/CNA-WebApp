---
name: Installer fresh install flow
description: How _internal/ gets built on fresh installs — the exe alone is not enough, PyInstaller onedir mode requires both the exe and _internal/
type: project
---

The app uses PyInstaller in `onedir` mode, which produces `CNA Web App.exe` + `_internal/` (runtime dependencies). Both must be present for the exe to launch.

`_internal/` is gitignored (it's ~16MB of compiled binaries). This means:
- A `git clone` gets the exe but NOT `_internal/`
- The installer clones the repo, then runs `setup.bat`, which detects `_internal/` is missing and builds it (installing PyInstaller first if needed)

**Why:** This was a bug discovered in April 2026 — fresh installs via the installer produced a non-functional exe because `_internal/` was never created. The original `setup.bat` only built the exe if PyInstaller was already installed, but PyInstaller wasn't in `requirements.txt`.

**How to apply:** If the exe doesn't launch after a fresh clone or install, check that `_internal/` exists alongside `CNA Web App.exe`. If missing, run `RebuildExe.bat` from a cmd prompt (or the equivalent pyinstaller commands from bash).
