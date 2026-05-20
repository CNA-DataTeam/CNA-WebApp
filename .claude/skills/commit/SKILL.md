---
name: commit
description: "Encrypt config, rebuild installer, and commit+push to GitHub. Use this for ALL git commits in this project."
---

# Commit to GitHub

This skill handles the full pre-commit pipeline and push for the CNA-WebApp project. It MUST be used for every commit to ensure the encrypted config and installer are always up to date.

## Important: Batch files do not work from Git Bash

Claude Code runs in Git Bash. Running `.bat` files via `cmd.exe /c` produces no output and may silently fail. **Do NOT run batch files directly.** Instead, run the underlying commands as documented below.

## What's NOT in this workflow anymore

`CNA Web App.exe` and `_internal/` are **gitignored**. They are built locally by `setup.bat` on every fresh install and rebuilt automatically by the in-app updater (`check_updates.py` and `app.py:_apply_update()`) if they go missing after a `git pull`. You do **not** need to run `RebuildExe.bat` before committing, and you must **never** stage `CNA Web App.exe`, `_internal/`, or `CODE - do not open/installer/CNA Web App.spec`.

## Steps

Run these steps in order. Stop and report to the user if any step fails.

### 1. Encrypt config

```bash
.venv/Scripts/python.exe "CODE - do not open/config_manager.py" encrypt
```

This updates `CODE - do not open/config.enc` with the latest `config.py`. If it fails, stop and tell the user.

### 2. Rebuild the installer

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
"%LOCALAPPDATA%\Programs\Inno Setup 6\ISCC.exe" /Qp /O"%~1\installer-output" "%~1\CODE - do not open\installer\CNA-Console-Installer.iss"
BATEOF
cmd //c "$(cygpath -w /tmp/run_iscc.bat)" "$(cygpath -w "$(pwd)")"
```

If ISCC.exe is not found at all, tell the user to install Inno Setup or run `RebuildInstaller.bat` from a normal cmd prompt. If the compile fails, stop and tell the user.

Note: The installer output (`installer-output/`) is gitignored and is NOT committed. This step ensures the local installer artifact stays current.

### 3. Review changes

Run `git status` and `git diff` to review all staged and unstaged changes. Summarize what will be committed.

### 4. Stage files

Stage all relevant changed files, always including:
- `CODE - do not open/config.enc`

Plus any other files that were modified as part of the current work. Use specific file names rather than `git add -A`.

**NEVER stage these files:**
- `config.py`
- `CODE - do not open/config.key`
- `CNA Web App.exe`
- `_internal/` (any files inside it)
- `CODE - do not open/installer/CNA Web App.spec`
- `.env`
- Any `.log` files
- Any files listed in `.gitignore`

### 5. Commit

Write a concise commit message summarizing the changes. Follow the repo's existing commit message style (check `git log --oneline -5`).

End the commit message with:
```
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
```

### 6. Push to GitHub

Push the commit to the remote. The repo uses HTTPS, so credentials must come from a credential helper — Git Bash from Claude Code has no TTY and a plain `git push` will fail with `could not read Username for 'https://github.com'`.

**Always run the push with Git Credential Manager explicitly enabled for this invocation:**

```bash
git -c credential.helper=manager push
```

This pulls credentials from the Windows Credential Store via `git-credential-manager.exe` (installed with Git for Windows at `C:\Users\<user>\AppData\Local\Programs\Git\mingw64\bin\git-credential-manager.exe`). The user does NOT need to enter credentials interactively — the manager handles it silently as long as they've authenticated before.

If `git-credential-manager.exe` isn't installed on this machine, plain `git push` will hang. In that case, fall back to telling the user to run `!git push` themselves.

If the push is rejected because the remote has newer commits, resolve it:

#### 6a. Rebase onto latest remote

```bash
git pull --rebase
```

If the rebase applies cleanly (no conflicts), push again and move on to step 7.

#### 6b. If there are merge conflicts, assess each one

For each conflicted file, read the conflict markers and determine whether both sides can be preserved:

**Safe to resolve yourself (keep both sides):**
- Additive changes to different files or different sections of the same file
- Both sides added new entries to `CLAUDE.md`, `MEMORY.md`, `page_registry.py`, `.gitignore`, or similar list/index files — keep all entries
- Import additions — keep all new imports from both sides

**Always rebuild after conflict resolution (do not pick a side):**
- `config.enc` — after resolving all other conflicts, re-run `.venv/Scripts/python.exe "CODE - do not open/config_manager.py" encrypt` and stage the result

**Stop and ask the user BEFORE resolving:**
- Two changes modified the same function, method, or logic block in contradictory ways
- One side deleted or renamed something the other side modified
- Changes to `config.py` structure where both sides changed the same keys/values
- Anything where preserving both sides would break functionality or create duplicates

#### 6c. After resolving all conflicts

```bash
# Stage resolved files
git add <resolved files>

# If config.enc was conflicted, rebuild it now (step 1)

# Continue the rebase
git rebase --continue

# Push (use the credential helper, same as step 6)
git -c credential.helper=manager push
```

If the push is rejected again (another commit landed while you were resolving), repeat from 6a. If this happens more than twice, stop and tell the user.

### 7. Publish GitHub Release with the installer attached

After the push lands, publish a GitHub Release with `installer-output/CNA-Console-Installer.exe` attached so that the stable shareable download link

```
https://github.com/CNA-DataTeam/CNA-WebApp/releases/latest/download/CNA-Console-Installer.exe
```

always serves the newest installer. This step runs on every commit — coworkers paste that one URL and always get the latest build.

#### 7a. Make sure `gh` is installed and authenticated

```bash
if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: GitHub CLI (gh) is not installed."
  echo "Install it with: winget install --id GitHub.cli -e --silent --scope user --accept-package-agreements --accept-source-agreements"
  echo "Then reopen the shell and re-run the commit flow."
  exit 1
fi
```

If `gh auth status` fails, open the browser for device-code sign-in. **Run this with an extended Bash timeout (e.g. `timeout: 600000`)** because it blocks until the user finishes signing in:

```bash
if ! gh auth status >/dev/null 2>&1; then
  echo "Not signed in to gh — opening browser for sign-in. Complete the prompt in the browser."
  gh auth login --hostname github.com --git-protocol https --web
fi
```

Do **not** ask the user to do anything else — `--web` opens the browser automatically. The user only needs to confirm the device code in the browser tab.

#### 7b. Compute the next release tag

Auto-bump the patch number from the highest existing `vMAJOR.MINOR.PATCH` tag. If no semver tag exists yet, start at `v1.0.0`.

```bash
git fetch --tags --quiet 2>/dev/null || true

LATEST_TAG=$(git tag -l 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -1)
if [[ -z "$LATEST_TAG" ]]; then
  NEXT_TAG="v1.0.0"
else
  IFS='.' read -r MAJOR MINOR PATCH <<< "${LATEST_TAG#v}"
  NEXT_TAG="v${MAJOR}.${MINOR}.$((PATCH + 1))"
fi
echo "Next release tag: $NEXT_TAG"
```

#### 7c. Create the release

```bash
gh release create "$NEXT_TAG" "installer-output/CNA-Console-Installer.exe" \
  --title "CNA Web App $NEXT_TAG" \
  --generate-notes
```

`gh release create` creates the git tag at `HEAD`, pushes it to the remote, uploads the installer as a release asset, and auto-generates release notes from commits since the previous release.

If the call fails because the tag somehow already exists (rare race), bump the patch once more and retry:

```bash
NEXT_TAG="v${MAJOR}.${MINOR}.$((PATCH + 2))"
gh release create "$NEXT_TAG" "installer-output/CNA-Console-Installer.exe" \
  --title "CNA Web App $NEXT_TAG" \
  --generate-notes
```

If it still fails, stop and report to the user — do not silently skip the release.

### 8. Confirm

Tell the user the commit hash, branch, a brief summary of what was pushed, and the release tag that was just published (so they know the `/releases/latest/download/CNA-Console-Installer.exe` URL now serves the newest installer).
