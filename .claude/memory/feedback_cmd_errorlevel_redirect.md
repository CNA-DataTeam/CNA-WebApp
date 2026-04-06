---
name: cmd.exe errorlevel clobbered by output redirection
description: Using >nul 2>&1 with python -c inside batch blocks silently resets errorlevel to 0 — use file-existence checks instead
type: feedback
---

In cmd.exe batch files, `"%PYTHON%" -c "import something" >nul 2>&1` unreliably sets errorlevel. Inside parenthesized blocks it consistently returns 0 even when the import fails, and even at top level it can return 0 intermittently. This makes `if errorlevel 1` checks useless.

**Why:** Discovered April 2026 when fresh installs via the installer failed to build `_internal/`. The `setup.bat` check for PyInstaller used `python -c "import PyInstaller" >nul 2>&1` followed by `if errorlevel 1`, which never triggered, so PyInstaller was never installed and the exe build was never attempted.

**How to apply:**
- Never rely on `python -c "import X" >nul 2>&1` + `if errorlevel` in batch files to check for package availability.
- Instead, check for the package's executable directly: `if not exist "%VENV%\Scripts\pyinstaller.exe"`.
- More generally, avoid `>nul 2>&1` on commands where you need to check errorlevel in batch. Use file-existence checks (`if not exist`) or `goto`-based flow control instead of `if errorlevel` inside blocks.
