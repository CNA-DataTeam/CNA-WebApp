---
name: commit
description: "Rebuild exe, encrypt config, rebuild installer, and commit+push to GitHub. Use this for ALL git commits in this project."
---

# Commit to GitHub

This skill handles the full pre-commit pipeline and push for the CNA-WebApp project. It MUST be used for every commit to ensure the exe, encrypted config, and installer are always up to date.

## Steps

Run these steps in order. Stop and report to the user if any step fails.

### 1. Rebuild the launcher exe

Run `RebuildExe.bat` from the project root:

```
cmd.exe /c RebuildExe.bat
```

Verify that `CNA Web App.exe` exists in the project root after this completes. If it fails, stop and tell the user.

### 2. Encrypt config

Run the config encryption command:

```
.venv\Scripts\python.exe "CODE - do not open\config_manager.py" encrypt
```

This updates `CODE - do not open/config.enc` with the latest `config.py`. If it fails, stop and tell the user.

### 3. Rebuild the installer

Run `RebuildInstaller.bat` from the project root:

```
cmd.exe /c RebuildInstaller.bat
```

This uses ISCC.exe (Inno Setup CLI compiler) to build `installer-output/CNA-WebApp-Setup.exe`. If it fails, stop and tell the user.

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

If the push fails due to upstream changes, run `git pull --ff-only` first, then retry the push. If there are merge conflicts, stop and ask the user for guidance.

### 8. Confirm

Tell the user the commit hash, branch, and a brief summary of what was pushed.
