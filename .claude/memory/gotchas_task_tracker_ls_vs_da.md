---
name: gotchas_task_tracker_ls_vs_da
description: Logistics Support vs Data & Analytics task tracker differences — shared file, separate state, different upload behavior
metadata:
  type: project
---

`pages/task-tracker.py` is one file that renders two versions (LS and DA), with `pages/da-task-tracker.py` as a thin wrapper that sets `_da_page_active` in session state and exec's the main file with `encoding="utf-8"`. The version toggle at the top uses `st.switch_page()` to navigate between `/task-tracker` and `/da-task-tracker`; the active version is determined on each render from which page file is active, NOT from a persistent session-state flag.

Practical differences to remember:

1. **Session state keys**: LS keys are unprefixed; DA keys are prefixed with `da_`. Don't mix them.

2. **Storage**: LS and DA use separate directory constants (`LS_COMPLETED_TASKS_DIR` / `DA_COMPLETED_TASKS_DIR`, etc.). Completed tasks, live activity, and archives all live in version-specific roots.

3. **Schemas differ**: DA defines `DA_PARQUET_SCHEMA` which adds a `Department` column on top of the standard `utils.PARQUET_SCHEMA`. If you add a field to one schema, decide explicitly whether it goes in the other.

4. **LS upload calls `utils.sync_tasks_parquet_targets()`. DA does NOT.** This is intentional — DA uses free-text task names, so there's nothing to sync into `tasks.parquet`. Don't "fix" this by adding the call to DA.

5. **DA Today's Activity uses `st.data_editor` with a Delete checkbox column** for deleting own tasks. LS uses a plain table. DA also supports resuming partially-complete tasks: select one and click "Resume task" to delete the old entry and pre-populate fields. Only available when tracker is idle, task is own, and task is marked partially complete.

6. **Inputs differ**: LS pulls covering-for users from `users.parquet` and active tasks from `tasks.parquet`. DA uses a primary stakeholder text input, free-text task input, and a Department dropdown.

**Why this matters:** It's easy to make a change to "the task tracker" and only test one version. Both versions exercise the same file but with different code paths and storage roots.

**How to apply:** When changing `task-tracker.py`, ask: does this affect LS, DA, or both? Test both before declaring done. If only one is intended, gate the change on whether `_da_page_active` is set in session state.
