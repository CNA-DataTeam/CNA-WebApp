# CLAUDE.md

This file is automatically loaded by Claude Code at the start of every conversation. It documents the load-bearing rules and conventions of this project so changes can be made safely.

Most descriptive "what does this page do" content has been moved out — read the page file when you need the workflow. Per-page gotchas and hard-won lessons live in `.claude/memory/` (see below).

## What this is

An internal Windows-only Streamlit suite for Logistics Support. Pages include a task timer / live activity tracker, historical analytics, admin maintenance, a packaging estimator, a time allocation tool, a FedEx address-validation dispute workflow, and a stocking agreement generator.

Designed around cached parquet datasets, shared UNC network storage, and lightweight Streamlit pages with shared styling.

The app assumes Windows. Batch files, Outlook automation, and Word-to-PDF conversion are Windows-specific. Many pages will appear broken if the configured UNC paths or synced SharePoint data are unavailable.

## Shared project memory

This project uses `.claude/memory/` for hard-won lessons that aren't documented rules — per-page gotchas, pitfalls, workarounds, user preferences. The index is `.claude/memory/MEMORY.md`.

- **Read** `MEMORY.md` at the start of any conversation that might benefit from past context.
- **Write** a memory when you learn something non-obvious through experience. Format is documented in `MEMORY.md`.
- **Don't duplicate** what's in this file — CLAUDE.md is for load-bearing rules; memory is for things learned through experience.
- Memory files are committed to git so everyone benefits.

## Ground rules

1. **`CODE - do not open/` is the real application source directory.** Launcher scripts and imports depend on the name. Do not rename or move it.

2. **`StartApp.bat` and `setup.bat` depend on the current layout.** If you move `app.py`, `startup.py`, or `requirements.txt`, update the batch files too.

3. **Shared storage conventions matter.** Pages read/write parquet/CSV on UNC paths from `config.py`. Don't change schemas, filenames, or partition layouts without updating every consumer.

4. **`tasks.parquet` is admin-managed.** `startup.py` deliberately refreshes accounts and users only; the Management page owns task definitions.

5. **Clear caches after writes.** Most loaders use `@st.cache_data`. When you write to persisted files, clear the relevant cached loaders.

6. **Reuse shared helpers.** Common patterns live in `utils.py`, `app_logging.py`, `page_registry.py`, and `config.py` — grep these before writing new helpers.

7. **Admin gating must be enforced in both places.** Admin-only pages hide from the registry AND check access inside the page. The current source of truth for admin status is `users.parquet`; `utils.is_user_admin(...)` tolerates alias variations — keep that tolerance unless you lock the upstream schema.

8. **MANDATORY pre-commit/pre-push steps.** ALWAYS run these in order before any git commit or push, even if the user doesn't mention them:
   1. Encrypt config: `.venv\Scripts\python.exe "CODE - do not open\config_manager.py" encrypt`
   2. Rebuild installer: `RebuildInstaller.bat`
   3. Stage `config.enc` alongside other changes.

   The repo is public — NEVER commit `config.py` directly. `CNA Web App.exe` and `_internal/` are gitignored (built by `setup.bat` and rebuilt by the in-app updater if missing); don't commit them and don't run `RebuildExe.bat` as part of commits. See "Config encryption" below for the file layout.

9. **Keep the commit skill in sync.** `.claude/skills/commit/SKILL.md` is the executable version of the commit workflow. Any change to pre-commit steps, commit steps, or related process here MUST be reflected there, or commits will be wrong.

10. **Running batch files from Claude Code (Git Bash).** Two recurring problems:
    - `cmd.exe /c SomeFile.bat` produces no visible output and may hang. Call the underlying commands directly from bash (e.g. `.venv/Scripts/pyinstaller.exe ...` instead of `RebuildExe.bat`).
    - Spaces in paths break argument parsing for Windows-native exes (like ISCC.exe). Wrap the call in a small temp `.bat` that handles quoting via `%~1`, then run it via `cmd //c`.

    See the commit skill for the working commands.

11. **Resolve push conflicts — don't give up.** When `git push` is rejected:
    1. `git pull --rebase`, then push if clean.
    2. If conflicts are clearly additive (different files; different sections of same file; CLAUDE.md/MEMORY.md additions), resolve and continue.
    3. For `config.enc`, always re-encrypt after resolving other conflicts and use the fresh output.
    4. If conflicting changes are logically incompatible (same function with contradictory edits, renames, deletes-vs-modifies), STOP and tell the user before resolving.

    See the commit skill for the step-by-step.

## Config encryption

`config.py` is sensitive (UNC paths, connection strings). It's encrypted in the repo as `config.enc` using Fernet (`cryptography` package).

- `config.enc` — committed
- `config.key` — gitignored; lives on the network share at `\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.key`
- `CODE - do not open/config_manager.py` — encrypt/decrypt tool

See ground rule #8 for the mandatory pre-commit flow.

## Repair and update flows

**Repair** (Settings > Repair App button, or double-click `repair.bat`):
- Force-kills the app, runs `git reset --hard origin/main` (or `HEAD` if offline), clears `__pycache__` and stale update markers, re-runs `setup.bat /silent` (which rebuilds venv / launcher / `_internal/` / config as needed), relaunches.
- Output is teed to `repair.log`. Spawned detached via `app.py:_launch_repair()` which calls `os._exit(0)` so file handles release immediately.
- Covers many failure modes deliberately as one button — don't split into surgical sub-options.

**Update** (background check once per day via `check_updates.py`, or manual via Settings):
- `git fetch` → if behind, `git pull --ff-only`. On failure, writes `.update_available` so the in-app dialog surfaces the error.
- After successful pull, runs `uv pip install --link-mode copy -r requirements.txt` unconditionally (uv no-ops fast) so dependency changes land before reboot.
- Also re-runs `setup.bat /silent` if `CNA Web App.exe` is missing post-pull (self-healing for the exe-untracking transition).
- In-app "Update Available" dialog has "Update Now" (apply + restart) and "Repair App" (escape hatch when updates keep failing).

## Navigation

`page_registry.py` is the single source of truth for pages, sections, icons, captions, quotes, and admin visibility. Add/rename/reorder pages there. The home page cards and sidebar both read from it. Sections use `st.expander()` with the active section auto-expanded.

Hidden pages (registered in `app.py` for URL routing only, not shown in sidebar):
- `pages/da-task-tracker.py` — provides `/da-task-tracker` URL so the D&A version of the task tracker persists on refresh. It's a thin wrapper that sets `_da_page_active` in session state and exec's `task-tracker.py` with `encoding="utf-8"`.

## Standard page pattern

Every page should call:
- `st.set_page_config(..., page_icon=utils.get_app_icon())`
- `utils.render_app_logo()`
- `st.markdown(utils.get_global_css(), unsafe_allow_html=True)`
- `utils.render_page_header(PAGE_TITLE)` — title comes from `utils.get_registry_page_title(...)`
- `utils.log_page_open_once(...)` — logger from `utils.get_page_logger(...)`

UI font stack is Poppins (headings) + Work Sans (body), imported via the shared CSS. Don't duplicate page-local CSS unless truly one-off. Brand assets: `config.LOGO_PATH`, `utils.get_app_icon()` (returns `icon.png` for favicon), `cna_icon.ico` (Windows shortcut icon).

## Data layout (load-bearing partition paths)

Storage roots are in `config.py`. The partition layouts other code depends on:

- Completed tasks: `COMPLETED_TASKS_DIR / user=<user_key>/year=<YYYY>/month=<MM>/day=<DD>/*.parquet`
- Live activity: `LIVE_ACTIVITY_DIR / user=<user_key>.parquet` (one small file per active user)
- Archived paused: `ARCHIVED_TASKS_DIR / user=<user_key>/archive_<timestamp>_<id>.parquet`
- Time allocation: `TIME_ALLOCATION_DIR / time_allocation_<YYYYMMDD>_<HHMMSS>_<id>.parquet` — **saving a day replaces all prior rows for the same user+date across existing export files before writing the new file**
- FedEx results: `config.ADDRESS_VALIDATION_RESULTS_FILE` (the validator page uses `.csv`; the admin results page prefers `.parquet` with `.csv` fallback)

Schemas live in code, not here. Grep `utils.PARQUET_SCHEMA` for completed-task columns; `task-tracker.py` also defines `DA_PARQUET_SCHEMA` (adds `Department`).

LS and DA task tracker use separate directory constants (`LS_COMPLETED_TASKS_DIR` / `DA_COMPLETED_TASKS_DIR`, etc.) — see [LS vs DA differences](.claude/memory/gotchas_task_tracker_ls_vs_da.md) for the practical implications.

## Logging

`app_logging.py` creates one `AppLogs.log` per user under `config.LOGS_ROOT_DIR` (user folder from `config.get_log_dir_for_user()`). Use `utils.get_page_logger(...)` or `utils.get_program_logger(...)` rather than rolling your own. The admin Logging page parses these and maps user folders to full name/department via `users.parquet`.

Message format: `timestamp | level | [context] message`.

## Performance conventions

Preserve these patterns when adding features:
- `@st.cache_data` on file reads, lookups, analytics loads, Excel parsing
- `pyarrow.dataset` for partitioned parquet reads (not per-file loops)
- Atomic parquet writes via `utils.atomic_write_parquet(...)`
- Selective column reads where possible
- Avoid repeated network file reads in the main rerun path

## Pages index

Registered in `page_registry.py`; source files in `CODE - do not open/pages/`. Read the page file when you need the workflow detail.

| Page | Purpose | Admin |
|---|---|---|
| `home.py` | Landing page; renders cards from the registry | no |
| `tasks-management.py` | Task definitions, monthly targets, task log edit/delete, user list. Writes to live production parquet/CSV — verify every consumer if you change column names. | **yes** |
| `admin-logs.py` | Per-user log viewer with filters | **yes** |
| `fedex-address-validation-management.py` | Review/clear disputed flags on validation results | **yes** |
| `task-tracker.py` | Combined LS + DA task timer; version toggle at top. UTC internally, Eastern for display. Excludes paused time. | no |
| `da-task-tracker.py` | Wrapper providing `/da-task-tracker` URL for the DA version (see Navigation) | no |
| `task-tracker-analytics.py` | Historical completed-task analytics with version toggle | no |
| `packaging-estimator.py` | Item matching + shipping calculator API estimate | no |
| `time-allocation-tool.py` | Per-day account/channel time entries; admin exports | no |
| `fedex-address-validator.py` | Generate FedEx residential-fee dispute Excel + email | no |
| `stocking-agreement-generator.py` | Renders DOCX (and optional PDF) from Word templates | no |

**Per-page gotchas live in `.claude/memory/`** — check there before modifying a page's behavior. Notable ones: FedEx validator's "Mark as Disputed" scope, packaging estimator's config-key mismatch, time allocation's editing window, task tracker LS vs DA differences.

## Stocking agreement templates

- Canonical templates: `CODE - do not open/templates/stocking_agreements/{general_resupply,consumables}_template.docx`
- `stocking_agreement_service.py` references specific table indexes (e.g. `doc.tables[3]`). **Template structure changes can break rendering even if placeholders still exist** — re-test both tabs after any edit.
- `MIN_TEMPLATE_PRICING_ROWS = 6` (pricing rows padded for rendering).
- Template font is `Sofia Pro` at `7 pt` default. (This applies to Word output only, not the Streamlit UI.)
- PDF conversion uses Word COM and is optional at runtime — DOCX still downloads on failure, with a PDF warning.
- Force a rebuild of canonical templates from legacy sources: `scripts/build_stocking_agreement_templates.py`.

## Common changes

- **New page**: create the file following the standard page pattern (above), register in `page_registry.py`, set `admin_only=True` if needed AND enforce access in the page.
- **Nav labels/icons/captions/quotes**: edit `page_registry.py`. Don't hardcode titles elsewhere.
- **Shared visual styling**: edit `utils.get_global_css()`.
- **New field on task records**: update `utils.PARQUET_SCHEMA`, `build_task_record(...)`, then task-log loaders/editors and analytics loaders if relevant. Verify older parquet files still load gracefully.

## Files worth reading first

- `config.py` (decrypt locally if needed)
- `CODE - do not open/app.py`
- `CODE - do not open/page_registry.py`
- `CODE - do not open/utils.py`
- `CODE - do not open/pages/task-tracker.py`
- `CODE - do not open/pages/tasks-management.py`
- `CODE - do not open/stocking_agreement_service.py`

## Manual test checklist

After meaningful edits, run the relevant smoke tests in `.claude/memory/manual_test_checklist.md` — there are no automated tests in this repo.
