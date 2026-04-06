# Shared Project Memory

This is the shared memory index for the CNA-WebApp project. Every entry here is available to any developer using Claude Code on this repo.

## How this works

- Each memory is a separate `.md` file in this directory with YAML frontmatter (`name`, `description`, `type`).
- This index (`MEMORY.md`) has one-line pointers to each file. Keep entries under 150 characters.
- Memory types: `feedback` (how to work), `project` (what's happening), `reference` (where to look), `user` (who we're working with).
- **Add memories** when you learn something non-obvious that will help future conversations across any developer.
- **Update or remove** memories that become stale or wrong.
- **Do not duplicate** what's already in `CLAUDE.md` — memory is for things learned through experience, not documented rules.

## Index

- [Git Bash batch file pitfalls](feedback_git_bash_bat_files.md) — .bat files and Windows exes don't work right from Git Bash
- [Installer fresh install flow](project_installer_flow.md) — how _internal/ gets built on fresh installs
- [Pre-commit steps are non-negotiable](feedback_precommit_mandatory.md) — never skip or soften pre-commit steps
- [cmd.exe errorlevel + redirect bug](feedback_cmd_errorlevel_redirect.md) — >nul 2>&1 clobbers errorlevel; use file checks instead
