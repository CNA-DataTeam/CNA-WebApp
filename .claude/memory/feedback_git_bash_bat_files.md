---
name: Git Bash batch file pitfalls
description: Batch files and Windows-native executables do not work correctly when called from Git Bash — run underlying commands directly instead
type: feedback
---

Running `.bat` files via `cmd.exe /c` from Git Bash produces no output and silently fails or hangs. Do NOT attempt `cmd.exe /c SomeFile.bat` for any of the project's batch files (`RebuildExe.bat`, `RebuildInstaller.bat`, etc.).

**Why:** Claude Code uses Git Bash as its shell. The `cmd.exe /c` bridge swallows stdout/stderr and the batch file appears to do nothing — no error, no output, no result. This wasted significant time debugging in April 2026.

**How to apply:**
- For `RebuildExe.bat`: call `.venv/Scripts/pyinstaller.exe` directly with absolute paths for `--icon` (relative icon paths resolve against the spec directory, not the project root).
- For `RebuildInstaller.bat`: ISCC.exe also breaks from bash because spaces in the project path split arguments. Write a temp `.bat` file that accepts the root dir as `%~1` and call it via `cmd //c`.
- For `config_manager.py encrypt`: this works fine from bash since it's a Python script, not a batch file.
- The commit skill (`.claude/skills/commit/SKILL.md`) has the exact working commands for all three steps.
