---
name: commit
description: "Rebuild exe, encrypt config, rebuild installer, and commit+push to GitHub. Use this for ALL git commits in this project."
---

# Commit to GitHub

This skill handles the full pre-commit pipeline and push for the CNA-WebApp project. It MUST be used for every commit to ensure the exe, encrypted config, and installer are always up to date.

## Important: Batch files do not work from Git Bash

Claude Code runs in Git Bash. Running `.bat` files via `cmd.exe /c` produces no output and may silently fail. **Do NOT run batch files directly.** Instead, run the underlying commands as documented below.

## Steps

Run these steps in order. Stop and report to the user if any step fails.

### 1. Rebuild the launcher exe

Run these commands directly from bash (do NOT run `RebuildExe.bat`):

```bash
# Clean old artifacts
rm -f "CNA Web App.exe"
rm -rf "_internal"
rm -rf "CODE - do not open/installer/dist"
rm -f "CODE - do not open/installer/CNA Web App.spec"

# Build (use absolute path for --icon to avoid spec-relative resolution)
ICON="$(pwd)/cna_icon.ico"
STUB="$(pwd)/CODE - do not open/stub_launcher.py"
.venv/Scripts/pyinstaller.exe --onedir --noconsole \
  --icon="$ICON" \
  --name="CNA Web App" \
  --distpath="CODE - do not open/installer/dist" \
  --specpath="CODE - do not open/installer" \
  --workpath="CODE - do not open/installer/build" \
  "$STUB"

# Move build output to project root
mv "CODE - do not open/installer/dist/CNA Web App/CNA Web App.exe" .
cp -r "CODE - do not open/installer/dist/CNA Web App/_internal" .
rm -rf "CODE - do not open/installer/dist"
```

Verify that both `CNA Web App.exe` and `_internal/` exist in the project root. If the build fails, stop and tell the user.

### 2. Encrypt config

```bash
.venv/Scripts/python.exe "CODE - do not open/config_manager.py" encrypt
```

This updates `CODE - do not open/config.enc` with the latest `config.py`. If it fails, stop and tell the user.

### 3. Rebuild the installer

Spaces in this project's paths break ISCC argument parsing when called directly from bash. Use a temp bat file:

```bash
# Locate ISCC.exe
ISCC="$LOCALAPPDATA/Programs/Inno Setup 6/ISCC.exe"
if [ ! -f "$ISCC" ]; then
  ISCC="/c/Program Files/Inno Setup 6/ISCC.exe"
fi
if [ ! -f "$ISCC" ]; then
  ISCC="/c/Program Files (x86)/Inno Setup 6/ISCC.exe"
fi

# Build via temp bat (handles spaces in paths correctly)
mkdir -p installer-output
cat > /tmp/run_iscc.bat << 'BATEOF'
@echo off
"%LOCALAPPDATA%\Programs\Inno Setup 6\ISCC.exe" /Qp /O"%~1\installer-output" "%~1\CODE - do not open\installer\CNA-WebApp-Setup.iss"
BATEOF
cmd //c "$(cygpath -w /tmp/run_iscc.bat)" "$(cygpath -w "$(pwd)")"
```

If ISCC.exe is not found at all, tell the user to install Inno Setup or run `RebuildInstaller.bat` from a normal cmd prompt. If the compile fails, stop and tell the user.

Note: The installer output (`installer-output/`) is gitignored and is NOT committed. This step ensures the local installer artifact stays current.

### 4. Review changes

Run `git status` and `git diff` to review all staged and unstaged changes. Summarize what will be committed.

### 5. Stage files

Stage all relevant changed files, always including:
- `CNA Web App.exe`
- `CODE - do not open/config.enc`

Plus any other files that were modified as part of the current work. Use specific file names rather than `git add -A`.

**NEVER stage these files:**
- `config.py`
- `CODE - do not open/config.key`
- `.env`
- Any `.log` files
- Any files listed in `.gitignore`

### 6. Commit

Write a concise commit message summarizing the changes. Follow the repo's existing commit message style (check `git log --oneline -5`).

End the commit message with:
```
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
```

### 7. Push to GitHub

Push the commit to the remote:

```
git push
```

If the push is rejected because the remote has newer commits, resolve it:

#### 7a. Rebase onto latest remote

```bash
git pull --rebase
```

If the rebase applies cleanly (no conflicts), push again and move on to step 8.

#### 7b. If there are merge conflicts, assess each one

For each conflicted file, read the conflict markers and determine whether both sides can be preserved:

**Safe to resolve yourself (keep both sides):**
- Additive changes to different files or different sections of the same file
- Both sides added new entries to `CLAUDE.md`, `MEMORY.md`, `page_registry.py`, `.gitignore`, or similar list/index files — keep all entries
- Import additions — keep all new imports from both sides

**Always rebuild after conflict resolution (do not pick a side):**
- `config.enc` — after resolving all other conflicts, re-run `.venv/Scripts/python.exe "CODE - do not open/config_manager.py" encrypt` and stage the result
- `CNA Web App.exe` and `_internal/` — after resolving all other conflicts, re-run the exe rebuild (step 1) and stage the result
- `CNA Web App.spec` — delete the conflicted spec, the exe rebuild will regenerate it

**Stop and ask the user BEFORE resolving:**
- Two changes modified the same function, method, or logic block in contradictory ways
- One side deleted or renamed something the other side modified
- Changes to `config.py` structure where both sides changed the same keys/values
- Anything where preserving both sides would break functionality or create duplicates

#### 7c. After resolving all conflicts

```bash
# Stage resolved files
git add <resolved files>

# If config.enc or the exe were conflicted, rebuild them now (steps 1-2)

# Continue the rebase
git rebase --continue

# Push
git push
```

If the push is rejected again (another commit landed while you were resolving), repeat from 7a. If this happens more than twice, stop and tell the user.

### 8. Confirm

Tell the user the commit hash, branch, and a brief summary of what was pushed.
