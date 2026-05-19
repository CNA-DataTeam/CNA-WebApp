# Shared Project Memory

This is the shared memory index for the CNA-WebApp project. Every entry here is available to any developer using Claude Code on this repo.

## How this works

- Each memory is a separate `.md` file in this directory with YAML frontmatter (`name`, `description`, `metadata.type`).
- This index (`MEMORY.md`) has one-line pointers to each file. Keep entries under 150 characters.
- Memory types: `feedback` (how to work), `project` (what's happening / how something behaves), `reference` (where to look), `user` (who we're working with).
- **Add memories** when you learn something non-obvious that will help future conversations across any developer.
- **Update or remove** memories that become stale or wrong.
- **Do not duplicate** what's already in `CLAUDE.md` — memory is for things learned through experience, not documented rules.

## Index

### How to work

- [Git Bash batch file pitfalls](feedback_git_bash_bat_files.md) — .bat files and Windows exes don't work right from Git Bash
- [Pre-commit steps are non-negotiable](feedback_precommit_mandatory.md) — never skip or soften pre-commit steps
- [cmd.exe errorlevel + redirect bug](feedback_cmd_errorlevel_redirect.md) — `>nul 2>&1` clobbers errorlevel; use file checks instead

### Project behavior

- [Installer fresh install flow](project_installer_flow.md) — how `_internal/` gets built on fresh installs
- [Accounts parquet daily regen lag](project_accounts_parquet_daily_regen.md) — new accounts columns appear only after the daily parquet rebuilds

### Per-page gotchas

- [FedEx validator gotchas](gotchas_fedex_validator.md) — "Mark as Disputed" acts on all visible rows; `__source_row_id`; email fallback chain
- [Packaging estimator config mismatch](gotchas_packaging_estimator_config.md) — page reads `shipping_calculator_api`; config.py defines `PACKAGING_CONFIG["api"]`
- [Time allocation editing rules](gotchas_time_allocation_editing.md) — strict This Week/Last Week window; channel order flips to frequency sort at 50+ saved
- [Task tracker LS vs DA differences](gotchas_task_tracker_ls_vs_da.md) — shared file, separate state keys, separate dirs, LS-only `sync_tasks_parquet_targets()` call

### Reference

- [Legacy artifacts](legacy_artifacts.md) — `config.json`, `scripts/validate_lake.py`, etc. — files that look authoritative but aren't
- [Manual test checklist](manual_test_checklist.md) — smoke tests to run after edits (no automated tests exist)
