---
name: gotchas_task_log_editing
description: Management > Task Log editor — which columns are editable, precedence rules, and partition relocation on save
metadata:
  type: project
---

The admin Management page (`pages/tasks-management.py`, `render_task_log_section`) renders the Task Log as an editable `st.data_editor`. Double-clicking a cell edits it. Editable columns: **TaskName, Duration, Notes, Start (ET), End (ET), Entry Date, FullName**. `PartiallyComplete` is intentionally read-only. The "Select" checkbox column drives row deletion (separate confirm dialog).

**Why this is non-trivial:** completed tasks are stored partitioned by `user=<sanitize_key(UserLogin)>/year=/month=/day=` (see `utils.build_out_dir`). So editing the date/time or the owner changes which file the row belongs in. `_save_task_log_changes` handles two cases:
- **In-place** (TaskName/Notes/Duration, or a time edit that stays on the same calendar day under the same user): the row is updated inside its existing source file.
- **Relocation** (FullName reassignment, or Start/Entry-Date change that crosses a day): the row is written to a new file in the destination partition and removed from the source file (source file is unlinked if it becomes empty). Relocations write the new file **before** pruning the source so a crash can't lose data.

**Precedence rules when multiple coupled fields are edited on one row** (Start/End/Duration/Entry Date all interrelate):
- Start: `Start (ET)` edit wins over `Entry Date` edit. Editing only `Entry Date` moves the calendar day but keeps the original time-of-day.
- End/Duration: `End (ET)` edit wins over `Duration` edit. Editing only Start shifts End to preserve the original duration.
- Duration is always recomputed as End − Start when End is set; End is recomputed as Start + Duration otherwise.

**FullName is a SelectboxColumn**, not free text — its options come from `users.parquet` (via `_build_fullname_login_map`) unioned with names already in the log. Reassignment only succeeds if the chosen name resolves to a login in `users.parquet` (needed to compute the new `user_key`); otherwise the save errors. This is why a row whose current FullName isn't in `users.parquet` still shows (it's unioned into options) but can't be the *target* of a reassignment unless that user exists.

Validation happens on the Save click (not per keystroke): bad Start/End ("YYYY-MM-DD HH:MM[:SS]" Eastern), bad Duration (HH:MM[:SS]), End-before-Start, and blank TaskName/FullName all block the save with a per-row error. Times are Eastern in the UI and converted to UTC for storage (DST-aware via `tz_localize("America/New_York", ambiguous=True, nonexistent="shift_forward")`).

After any save the loader caches are cleared: `_load_task_log_entries.clear()`, `utils.load_all_completed_tasks.clear()`, `utils.load_completed_tasks_for_analytics.clear()`. The page reads `config.COMPLETED_TASKS_DIR` (the LS root); the partition root for relocations is derived from the source file path, not hardcoded, so it always matches where the loader reads from. Related: [LS vs DA differences](gotchas_task_tracker_ls_vs_da.md).
