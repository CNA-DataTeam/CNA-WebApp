"""
pages/tasks-management.py

Purpose:
    Admin-only management page with content sections:
        - Task Definition: view and update tasks.parquet
        - Task Log: view submitted task records
        - Users: view users.parquet
"""

from __future__ import annotations

from pathlib import Path
import uuid

import pandas as pd
import streamlit as st

import config
import utils


LOGGER = utils.get_page_logger("Tasks Management")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Content Management")

st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
utils.render_page_header(PAGE_TITLE, config.LOGO_PATH)

utils.log_page_open_once("tasks_management_page", LOGGER)
if "_tasks_management_render_logged" not in st.session_state:
    st.session_state._tasks_management_render_logged = True
    LOGGER.info("Render UI.")

if not utils.is_current_user_admin():
    LOGGER.warning("Access denied for non-admin user '%s'.", utils.get_os_user())
    st.error("Access denied. This page is available to admin users only.")
    st.stop()


TASKS_PARQUET_PATH = Path(config.PERSONNEL_DIR) / "tasks.parquet"
USERS_PARQUET_PATH = Path(config.PERSONNEL_DIR) / "users.parquet"
CADENCE_OPTIONS = ["Daily", "Weekly", "Periodic"]
EDITABLE_COLUMNS = ["TaskName", "TaskCadence"]
TM_STATE_VERSION = 3

if "tm_confirm_open" not in st.session_state:
    st.session_state.tm_confirm_open = False
if "tm_confirm_rendered" not in st.session_state:
    st.session_state.tm_confirm_rendered = False
if "tm_updated_toast" not in st.session_state:
    st.session_state.tm_updated_toast = False
if "tm_add_mode" not in st.session_state:
    st.session_state.tm_add_mode = False
if "tm_new_task_name" not in st.session_state:
    st.session_state.tm_new_task_name = ""
if "tm_new_task_cadence" not in st.session_state:
    st.session_state.tm_new_task_cadence = CADENCE_OPTIONS[0]
if "tm_reset_add_form" not in st.session_state:
    st.session_state.tm_reset_add_form = False
if "tm_task_log_delete_confirm_open" not in st.session_state:
    st.session_state.tm_task_log_delete_confirm_open = False
if "tm_task_log_delete_confirm_rendered" not in st.session_state:
    st.session_state.tm_task_log_delete_confirm_rendered = False
if "tm_task_log_delete_payload" not in st.session_state:
    st.session_state.tm_task_log_delete_payload = None

# Reset add-form widget values only before widgets are instantiated in the run.
if st.session_state.tm_reset_add_form:
    st.session_state.tm_new_task_name = ""
    st.session_state.tm_new_task_cadence = CADENCE_OPTIONS[0]
    st.session_state.tm_reset_add_form = False


def _normalize_col_name(value: object) -> str:
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def _resolve_existing_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    if df.empty:
        return None
    alias_set = {_normalize_col_name(alias) for alias in aliases}
    for col in df.columns:
        if _normalize_col_name(col) in alias_set:
            return str(col)
    return None


def _load_tasks_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"tasks.parquet not found at '{path}'.")
    return pd.read_parquet(path)


def _load_users_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"users.parquet not found at '{path}'.")
    return pd.read_parquet(path)


def _write_tasks_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.stem}.{uuid.uuid4().hex}.tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)


def _resolve_column_map(full_df: pd.DataFrame) -> dict[str, str]:
    return {
        "TaskName": _resolve_existing_column(full_df, ["TaskName", "Task Name"]) or "TaskName",
        "TaskCadence": _resolve_existing_column(full_df, ["TaskCadence", "Task Cadence"]) or "TaskCadence",
        "IsActive": _resolve_existing_column(full_df, ["IsActive", "Is Active"]) or "IsActive",
    }


def _ensure_required_columns(full_df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    df = full_df.copy()
    if col_map["TaskName"] not in df.columns:
        df[col_map["TaskName"]] = ""
    if col_map["TaskCadence"] not in df.columns:
        df[col_map["TaskCadence"]] = CADENCE_OPTIONS[0]
    if col_map["IsActive"] not in df.columns:
        df[col_map["IsActive"]] = True
    return df


def _get_view_df(full_df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    task_col = col_map["TaskName"]
    cadence_col = col_map["TaskCadence"]
    view_df = pd.DataFrame(
        {
            "Task Name": full_df[task_col].fillna("").astype(str).str.strip(),
            "Task Cadence": full_df[cadence_col].fillna("").astype(str).str.strip(),
        }
    )
    return view_df.reset_index(drop=True)


def _format_new_is_active(raw_value: bool, active_series: pd.Series) -> object:
    clean_series = active_series.dropna()
    if clean_series.empty:
        return bool(raw_value)

    sample = clean_series.iloc[0]
    if isinstance(sample, bool):
        return bool(raw_value)
    if isinstance(sample, (int, float)):
        return 1 if raw_value else 0

    sample_text = str(sample).strip().lower()
    if sample_text in {"1", "0"}:
        return "1" if raw_value else "0"
    if sample_text in {"true", "false"}:
        return "true" if raw_value else "false"
    if sample_text in {"y", "n"}:
        return "y" if raw_value else "n"
    if sample_text in {"yes", "no"}:
        return "yes" if raw_value else "no"
    return bool(raw_value)


def _initialize_state(force_reload: bool = False) -> None:
    state_version_mismatch = st.session_state.get("tm_state_version") != TM_STATE_VERSION
    missing_state = (
        "tm_original_full_df" not in st.session_state
        or "tm_working_full_df" not in st.session_state
        or "tm_column_map" not in st.session_state
    )
    if not force_reload and not missing_state and not state_version_mismatch:
        return

    full_df = _load_tasks_parquet(TASKS_PARQUET_PATH)
    col_map = _resolve_column_map(full_df)
    full_df = _ensure_required_columns(full_df, col_map)

    st.session_state.tm_original_full_df = full_df.copy(deep=True)
    st.session_state.tm_working_full_df = full_df.copy(deep=True)
    st.session_state.tm_column_map = col_map
    st.session_state.tm_state_version = TM_STATE_VERSION

    LOGGER.info("Loaded tasks parquet | path='%s' rows=%s", TASKS_PARQUET_PATH, len(full_df))


def _has_pending_changes() -> bool:
    original = st.session_state.tm_original_full_df.reset_index(drop=True)
    working = st.session_state.tm_working_full_df.reset_index(drop=True)
    return not working.equals(original)


def _compute_change_summary() -> tuple[int, int]:
    col_map = st.session_state.tm_column_map
    original_view = _get_view_df(st.session_state.tm_original_full_df, col_map)
    working_view = _get_view_df(st.session_state.tm_working_full_df, col_map)

    common_len = min(len(original_view), len(working_view))
    changed_rows = 0
    changed_cells = 0

    if common_len > 0:
        left = original_view.iloc[:common_len].fillna("").astype(str)
        right = working_view.iloc[:common_len].fillna("").astype(str)
        diff_mask = left.ne(right)
        changed_rows += int(diff_mask.any(axis=1).sum())
        changed_cells += int(diff_mask.sum().sum())

    extra_rows = abs(len(original_view) - len(working_view))
    changed_rows += int(extra_rows)
    changed_cells += int(extra_rows * len(EDITABLE_COLUMNS))
    return changed_rows, changed_cells


def _apply_update() -> None:
    updated_df = st.session_state.tm_working_full_df.copy(deep=True)
    _write_tasks_parquet(TASKS_PARQUET_PATH, updated_df)

    st.session_state.tm_original_full_df = updated_df.copy(deep=True)
    st.session_state.tm_working_full_df = updated_df.copy(deep=True)
    st.session_state.tm_updated_toast = True
    utils.load_tasks.clear()

    LOGGER.info("tasks.parquet updated | path='%s' rows=%s", TASKS_PARQUET_PATH, len(updated_df))


def _get_users_view_df(users_df: pd.DataFrame) -> pd.DataFrame:
    desired_order = [
        "EmployeeNumber",
        "StartDate",
        "First Name",
        "Last Name",
        "Full Name",
        "Email",
        "Role",
        "IsManager",
        "Department",
        "Manager",
        "ManagerEmployeeNumber",
        "isAdmin",
        "User",
    ]
    df = users_df.copy()
    for col in desired_order:
        if col not in df.columns:
            df[col] = pd.NA
    extra_cols = [c for c in df.columns if c not in desired_order]
    ordered_cols = desired_order + extra_cols

    if "StartDate" in df.columns:
        df["StartDate"] = pd.to_datetime(df["StartDate"], errors="coerce")
    return df[ordered_cols]


@st.dialog("Confirm Update")
def confirm_update_dialog() -> None:
    changed_rows, changed_cells = _compute_change_summary()
    st.caption(f"Rows changed: **{changed_rows:,}**")
    st.caption(f"Cells changed: **{changed_cells:,}**")
    st.warning("This will overwrite tasks.parquet with your current changes.")

    confirm_col, cancel_col = st.columns(2)
    with confirm_col:
        if st.button("Confirm Update", type="primary", width="stretch"):
            try:
                _apply_update()
                st.session_state.tm_confirm_open = False
                st.session_state.tm_confirm_rendered = False
                st.rerun()
            except Exception as exc:
                LOGGER.exception("Failed to update tasks.parquet: %s", exc)
                st.error(f"Update failed: {exc}")
    with cancel_col:
        if st.button("Cancel", width="stretch"):
            st.session_state.tm_confirm_open = False
            st.session_state.tm_confirm_rendered = False
            st.rerun()


def render_tasks_section() -> None:
    try:
        _initialize_state()
    except Exception as exc:
        LOGGER.exception("Failed to initialize tasks editor: %s", exc)
        st.error(f"Failed to load tasks.parquet: {exc}")
        st.stop()

    if st.session_state.get("tm_updated_toast"):
        st.toast("Update confirmed and saved.")
        st.session_state.tm_updated_toast = False

    col_map = st.session_state.tm_column_map
    working_full_df = st.session_state.tm_working_full_df.copy(deep=True)
    view_df = _get_view_df(working_full_df, col_map)
    view_df["__row_pos"] = view_df.index.astype(int)

    filter_left, filter_right = st.columns(2)
    with filter_left:
        task_name_filter = st.text_input(
            "Filter by Task Name",
            key="tm_filter_task_name",
            placeholder="Search task name",
        )
    with filter_right:
        cadence_options = sorted(
            c for c in view_df["Task Cadence"].dropna().astype(str).str.strip().unique().tolist() if c
        )
        selected_cadences = st.multiselect(
            "Filter by Task Cadence",
            options=cadence_options,
            key="tm_filter_task_cadence",
        )

    filtered_view_df = view_df.copy()
    if task_name_filter and task_name_filter.strip():
        needle = task_name_filter.strip().lower()
        name_series = filtered_view_df["Task Name"].fillna("").astype(str).str.lower()
        filtered_view_df = filtered_view_df[name_series.str.contains(needle, na=False)]
    if selected_cadences:
        cadence_series = filtered_view_df["Task Cadence"].fillna("").astype(str)
        filtered_view_df = filtered_view_df[cadence_series.isin(selected_cadences)]

    display_df = filtered_view_df[["Task Name", "Task Cadence"]].copy()
    st.caption(f"Rows: {len(display_df):,} (of {len(view_df):,})")

    selected_view_rows: list[int] = []
    selected_row_positions: list[int] = []
    try:
        event = st.dataframe(
            display_df,
            width="stretch",
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
        )
        if event is not None and hasattr(event, "selection"):
            selected_view_rows = list(getattr(event.selection, "rows", []) or [])
    except TypeError:
        st.dataframe(display_df, width="stretch", hide_index=True)
        row_options = list(range(len(display_df)))
        selected_view_rows = st.multiselect(
            "Select rows to delete",
            options=row_options,
            format_func=lambda idx: f"{idx + 1}: {display_df.iloc[idx]['Task Name']}",
        )

    if selected_view_rows:
        safe_positions = [int(idx) for idx in selected_view_rows if 0 <= int(idx) < len(filtered_view_df)]
        if safe_positions:
            selected_row_positions = filtered_view_df.iloc[safe_positions]["__row_pos"].astype(int).tolist()

    add_col, delete_col = st.columns([1, 1])
    with add_col:
        if st.button("Add a task", width="stretch"):
            opening_add_mode = not st.session_state.tm_add_mode
            st.session_state.tm_add_mode = opening_add_mode
            if opening_add_mode:
                st.session_state.tm_new_task_name = ""
                st.session_state.tm_new_task_cadence = CADENCE_OPTIONS[0]

    with delete_col:
        if st.button("Delete", width="stretch", disabled=len(selected_row_positions) == 0):
            if selected_row_positions:
                drop_positions = sorted(
                    {int(idx) for idx in selected_row_positions if 0 <= int(idx) < len(working_full_df)}
                )
                if drop_positions:
                    drop_index = working_full_df.index[drop_positions]
                    working_full_df = working_full_df.drop(index=drop_index).reset_index(drop=True)
                    st.session_state.tm_working_full_df = working_full_df
                    LOGGER.info("Deleted task rows | count=%s", len(drop_positions))
                    st.rerun()

    if st.session_state.tm_add_mode:
        name_col, cadence_col, save_col, cancel_col = st.columns([3.6, 2.0, 1.2, 1.2])
        with name_col:
            st.text_input(
                "Task Name",
                key="tm_new_task_name",
                placeholder="Type the Task Name",
                label_visibility="collapsed",
            )
        with cadence_col:
            st.selectbox(
                "Task Cadence",
                options=CADENCE_OPTIONS,
                key="tm_new_task_cadence",
                label_visibility="collapsed",
            )

        with save_col:
            if st.button("Add", type="primary", width="stretch"):
                new_task_name = str(st.session_state.get("tm_new_task_name")).strip()
                if not new_task_name:
                    st.warning("Task name cannot be blank.")
                else:
                    new_row = {col: pd.NA for col in working_full_df.columns}
                    new_row[col_map["TaskName"]] = new_task_name
                    new_row[col_map["TaskCadence"]] = str(
                        st.session_state.get("tm_new_task_cadence", CADENCE_OPTIONS[0])
                    )
                    new_row[col_map["IsActive"]] = _format_new_is_active(
                        True,
                        working_full_df[col_map["IsActive"]],
                    )
                    working_full_df = pd.concat([working_full_df, pd.DataFrame([new_row])], ignore_index=True)
                    st.session_state.tm_working_full_df = working_full_df
                    st.session_state.tm_add_mode = False
                    st.session_state.tm_reset_add_form = True
                    LOGGER.info("Added task row | task='%s'", new_task_name)
                    st.rerun()

        with cancel_col:
            if st.button("Cancel", width="stretch"):
                st.session_state.tm_add_mode = False
                st.session_state.tm_reset_add_form = True
                st.rerun()

    st.divider()
    spacer_left, update_col, discard_col, spacer_right = st.columns(4)
    with update_col:
        if st.button("Update", type="primary", width="stretch"):
            if not _has_pending_changes():
                st.info("No pending changes.")
            else:
                st.session_state.tm_confirm_open = True
                st.session_state.tm_confirm_rendered = False

    with discard_col:
        if st.button("Discard Edits", width="stretch"):
            st.session_state.tm_working_full_df = st.session_state.tm_original_full_df.copy(deep=True)
            st.session_state.tm_add_mode = False
            st.session_state.tm_reset_add_form = True
            LOGGER.info("Discarded pending tasks management edits.")
            st.info("Edits discarded.")
            st.rerun()

    if st.session_state.tm_confirm_open and not st.session_state.get("tm_confirm_rendered"):
        st.session_state.tm_confirm_rendered = True
        confirm_update_dialog()


def render_users_section() -> None:
    try:
        users_df = _load_users_parquet(USERS_PARQUET_PATH)
    except Exception as exc:
        LOGGER.exception("Failed to load users parquet: %s", exc)
        st.error(f"Failed to load users.parquet: {exc}")
        return

    if users_df.empty:
        st.info("users.parquet is empty.")
        return

    view_df = _get_users_view_df(users_df)
    dept_col = _resolve_existing_column(view_df, ["Department", "Dept"])
    full_name_col = _resolve_existing_column(view_df, ["Full Name", "FullName", "Name"])
    user_col = _resolve_existing_column(view_df, ["User", "UserLogin", "Login", "Username"])
    email_col = _resolve_existing_column(view_df, ["Email", "Email Address"])
    admin_col = _resolve_existing_column(view_df, ["isAdmin", "IsAdmin", "Admin", "Is Admin"])

    filter_col1, filter_col2, filter_col3, filter_spacer, refresh_col = st.columns([3, 2, 2, 0.25, 1.15])
    with filter_col1:
        user_search = st.text_input(
            "Search User",
            key="tm_users_search",
            placeholder="Full name, login, or email",
        )
    with filter_col2:
        dept_options = ["All"]
        if dept_col:
            dept_values = view_df[dept_col].fillna("").astype(str).str.strip()
            dept_options += sorted(v for v in dept_values.unique().tolist() if v)
        selected_department = st.selectbox(
            "Department",
            options=dept_options,
            index=0,
            disabled=dept_col is None,
            key="tm_users_department",
        )
    with filter_col3:
        selected_admin_status = st.selectbox(
            "Admin Status",
            options=["All", "Admin", "Non-Admin"],
            key="tm_users_admin_status",
        )
    with filter_spacer:
        st.markdown("&nbsp;", unsafe_allow_html=True)
    with refresh_col:
        st.space(size="small")
        if st.button("Refresh", width="stretch", key="tm_users_refresh_inline"):
            st.rerun()

    filtered_df = view_df.copy()
    if user_search and user_search.strip():
        needle = user_search.strip().lower()
        mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
        for candidate_col in [full_name_col, user_col, email_col]:
            if candidate_col and candidate_col in filtered_df.columns:
                col_series = filtered_df[candidate_col].fillna("").astype(str).str.lower()
                mask = mask | col_series.str.contains(needle, na=False)
        filtered_df = filtered_df[mask]

    if selected_department != "All" and dept_col:
        dept_series = filtered_df[dept_col].fillna("").astype(str).str.strip()
        filtered_df = filtered_df[dept_series == selected_department]

    if selected_admin_status != "All" and admin_col:
        admin_series = filtered_df[admin_col].fillna("").astype(str).str.strip().str.lower()
        admin_mask = admin_series.isin({"1", "true", "t", "yes", "y", "on", "admin", "administrator"})
        if selected_admin_status == "Admin":
            filtered_df = filtered_df[admin_mask]
        else:
            filtered_df = filtered_df[~admin_mask]

    st.caption(f"Rows: {len(filtered_df):,} (of {len(view_df):,})")
    st.dataframe(filtered_df, hide_index=True, width="stretch")


@st.cache_data(ttl=30)
def _load_task_log_entries(completed_dir: Path) -> pd.DataFrame:
    files = list(completed_dir.glob("user=*/year=*/month=*/day=*/*.parquet"))
    if not files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for file_path in files:
        try:
            one = pd.read_parquet(file_path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable completed-task file '%s': %s", file_path, exc)
            continue
        if one.empty:
            continue
        one = one.copy()
        one["__source_file"] = str(file_path)
        one["__row_idx"] = range(len(one))
        if "TaskID" not in one.columns:
            one["TaskID"] = pd.NA
        frames.append(one)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _save_task_log_task_name_changes(changes_df: pd.DataFrame) -> tuple[int, int]:
    """Persist edited TaskName values back to the original parquet files."""
    if changes_df.empty:
        return 0, 0

    updated_rows = 0
    touched_files = 0
    for source_file, group in changes_df.groupby("__source_file"):
        file_path = Path(str(source_file))
        if not file_path.exists():
            LOGGER.warning("Task log source file no longer exists: %s", file_path)
            continue

        try:
            file_df = pd.read_parquet(file_path)
        except Exception as exc:
            LOGGER.warning("Failed to read task log source file '%s': %s", file_path, exc)
            continue
        if file_df.empty:
            continue

        file_updated = False
        for _, change in group.iterrows():
            new_name = str(change.get("TaskName_new") or "").strip()
            if not new_name:
                continue

            target_index = None
            task_id = str(change.get("TaskID") or "").strip()
            if task_id and "TaskID" in file_df.columns:
                task_id_series = file_df["TaskID"].fillna("").astype(str).str.strip()
                matches = file_df.index[task_id_series.eq(task_id)]
                if len(matches) > 0:
                    target_index = matches[0]

            if target_index is None:
                try:
                    row_idx = int(change.get("__row_idx"))
                except (TypeError, ValueError):
                    row_idx = -1
                if 0 <= row_idx < len(file_df):
                    target_index = file_df.index[row_idx]

            if target_index is None:
                continue

            if "TaskName" not in file_df.columns:
                file_df["TaskName"] = ""
            file_df.at[target_index, "TaskName"] = new_name
            file_updated = True
            updated_rows += 1

        if file_updated:
            _write_tasks_parquet(file_path, file_df)
            touched_files += 1

    return updated_rows, touched_files


def _delete_task_log_rows(delete_df: pd.DataFrame) -> tuple[int, int]:
    """Delete selected task-log rows from their original parquet files."""
    if delete_df.empty:
        return 0, 0

    deleted_rows = 0
    touched_files = 0
    for source_file, group in delete_df.groupby("__source_file"):
        file_path = Path(str(source_file))
        if not file_path.exists():
            LOGGER.warning("Task log source file no longer exists for delete: %s", file_path)
            continue

        try:
            file_df = pd.read_parquet(file_path)
        except Exception as exc:
            LOGGER.warning("Failed to read task log source file for delete '%s': %s", file_path, exc)
            continue
        if file_df.empty:
            continue

        drop_index_labels: set[int] = set()
        for _, row in group.iterrows():
            target_index = None
            try:
                row_idx = int(row.get("__row_idx"))
            except (TypeError, ValueError):
                row_idx = -1
            if 0 <= row_idx < len(file_df):
                target_index = file_df.index[row_idx]

            if target_index is None:
                task_id = str(row.get("TaskID") or "").strip()
                if task_id and "TaskID" in file_df.columns:
                    task_id_series = file_df["TaskID"].fillna("").astype(str).str.strip()
                    matches = file_df.index[task_id_series.eq(task_id)]
                    if len(matches) > 0:
                        target_index = matches[0]

            if target_index is not None:
                drop_index_labels.add(target_index)

        if not drop_index_labels:
            continue

        remaining_df = file_df.drop(index=list(drop_index_labels))
        deleted_rows += len(drop_index_labels)
        touched_files += 1

        if remaining_df.empty:
            try:
                file_path.unlink(missing_ok=True)
            except Exception as exc:
                LOGGER.warning("Failed to remove emptied task log source file '%s': %s", file_path, exc)
                _write_tasks_parquet(file_path, remaining_df.reset_index(drop=True))
        else:
            _write_tasks_parquet(file_path, remaining_df.reset_index(drop=True))

    return deleted_rows, touched_files


@st.dialog("Confirm Delete")
def confirm_task_log_delete_dialog() -> None:
    payload = st.session_state.get("tm_task_log_delete_payload")
    if not isinstance(payload, pd.DataFrame) or payload.empty:
        st.error("No rows selected for deletion.")
        st.session_state.tm_task_log_delete_confirm_open = False
        st.session_state.tm_task_log_delete_confirm_rendered = False
        st.session_state.tm_task_log_delete_payload = None
        return

    st.warning(f"You are about to delete {len(payload):,} task log row(s).")
    preview_cols = [c for c in ["Entry Date", "FullName", "TaskName", "Notes"] if c in payload.columns]
    if preview_cols:
        st.dataframe(payload[preview_cols], hide_index=True, width="stretch")

    confirm_col, cancel_col = st.columns(2)
    with confirm_col:
        if st.button("Confirm Delete", type="primary", width="stretch", key="tm_task_log_confirm_delete_btn"):
            deleted_rows, touched_files = _delete_task_log_rows(payload)
            _load_task_log_entries.clear()
            utils.load_all_completed_tasks.clear()
            st.session_state.tm_task_log_delete_confirm_open = False
            st.session_state.tm_task_log_delete_confirm_rendered = False
            st.session_state.tm_task_log_delete_payload = None
            if deleted_rows > 0:
                st.success(f"Deleted {deleted_rows} row(s) across {touched_files} file(s).")
            else:
                st.info("No rows were deleted.")
            st.rerun()

    with cancel_col:
        if st.button("Cancel", width="stretch", key="tm_task_log_cancel_delete_btn"):
            st.session_state.tm_task_log_delete_confirm_open = False
            st.session_state.tm_task_log_delete_confirm_rendered = False
            st.session_state.tm_task_log_delete_payload = None
            st.rerun()


def render_task_log_section() -> None:
    try:
        df = _load_task_log_entries(config.COMPLETED_TASKS_DIR)
    except Exception as exc:
        LOGGER.exception("Failed to load task log data: %s", exc)
        st.error(f"Failed to load task log data: {exc}")
        return

    if df.empty:
        st.info("No submitted tasks found.")
        return

    if "StartTimestampUTC" in df.columns:
        df["StartTimestampUTC"] = pd.to_datetime(df["StartTimestampUTC"], utc=True, errors="coerce")
    if "EndTimestampUTC" in df.columns:
        df["EndTimestampUTC"] = pd.to_datetime(df["EndTimestampUTC"], utc=True, errors="coerce")

    if "StartTimestampUTC" in df.columns:
        start_et = df["StartTimestampUTC"].dt.tz_convert("America/New_York")
        df["Entry Date"] = start_et.dt.date
        df["Start (ET)"] = start_et.dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        df["Entry Date"] = pd.NaT
        df["Start (ET)"] = ""

    if "EndTimestampUTC" in df.columns:
        end_et = df["EndTimestampUTC"].dt.tz_convert("America/New_York")
        df["End (ET)"] = end_et.dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        df["End (ET)"] = ""

    if "DurationSeconds" in df.columns:
        df["Duration"] = df["DurationSeconds"].fillna(0).astype(int).map(utils.format_hhmmss)
    else:
        df["Duration"] = ""

    display_cols = [
        "Entry Date",
        "FullName",
        "UserLogin",
        "TaskName",
        "Duration",
        "Start (ET)",
        "End (ET)",
        "PartiallyComplete",
        "Notes",
    ]
    for col in display_cols:
        if col not in df.columns:
            df[col] = pd.NA

    filter_date_col, filter_task_col, filter_user_col, filter_notes_col = st.columns(4)
    with filter_date_col:
        valid_entry_dates = sorted(
            d for d in df["Entry Date"].dropna().unique().tolist() if pd.notna(d)
        )
        if valid_entry_dates:
            min_entry_date = valid_entry_dates[0]
            max_entry_date = valid_entry_dates[-1]
            selected_date_range = st.date_input(
                "Date Range",
                value=(min_entry_date, max_entry_date),
                min_value=min_entry_date,
                max_value=max_entry_date,
                key="tm_task_log_date_range",
            )
        else:
            selected_date_range = None
            st.date_input(
                "Date Range",
                value=utils.to_eastern(utils.now_utc()).date(),
                disabled=True,
                key="tm_task_log_date_range_disabled",
            )
    with filter_task_col:
        task_options = ["All"] + sorted(
            t for t in df["TaskName"].fillna("").astype(str).str.strip().unique().tolist() if t
        )
        selected_task = st.selectbox(
            "Task Name",
            options=task_options,
            key="tm_task_log_task_name",
        )
    with filter_user_col:
        user_options = ["All"] + sorted(
            u for u in df["FullName"].fillna("").astype(str).str.strip().unique().tolist() if u
        )
        selected_user = st.selectbox(
            "User Full Name",
            options=user_options,
            key="tm_task_log_user_full_name",
        )
    with filter_notes_col:
        notes_search = st.text_input(
            "Notes",
            key="tm_task_log_notes",
            placeholder="Contains text",
        )

    filtered_df = df.copy()
    if selected_date_range is not None:
        if isinstance(selected_date_range, (tuple, list)) and len(selected_date_range) == 2:
            start_date, end_date = selected_date_range
            if start_date and end_date:
                if start_date > end_date:
                    start_date, end_date = end_date, start_date
                filtered_df = filtered_df[
                    filtered_df["Entry Date"].ge(start_date) & filtered_df["Entry Date"].le(end_date)
                ]
        else:
            filtered_df = filtered_df[filtered_df["Entry Date"].eq(selected_date_range)]
    if selected_task != "All":
        filtered_df = filtered_df[
            filtered_df["TaskName"].fillna("").astype(str).str.strip().eq(selected_task)
        ]
    if selected_user != "All":
        filtered_df = filtered_df[
            filtered_df["FullName"].fillna("").astype(str).str.strip().eq(selected_user)
        ]
    if notes_search and notes_search.strip():
        needle = notes_search.strip().lower()
        notes_series = filtered_df["Notes"].fillna("").astype(str).str.lower()
        filtered_df = filtered_df[notes_series.str.contains(needle, na=False)]

    if "StartTimestampUTC" in filtered_df.columns:
        sorted_df = filtered_df.sort_values("StartTimestampUTC", ascending=False, na_position="last")
    else:
        sorted_df = filtered_df
    working_df = sorted_df[display_cols + ["TaskID", "__source_file", "__row_idx"]].reset_index(drop=True)
    view_df = working_df[display_cols].copy()
    editor_input_df = view_df.copy()
    editor_input_df.insert(0, "Select", False)
    st.caption(f"Rows: {len(view_df):,} (of {len(df):,})")

    edited_view_df = st.data_editor(
        editor_input_df,
        hide_index=True,
        width="stretch",
        key="tm_task_log_editor",
        column_config={
            "TaskName": st.column_config.TextColumn("TaskName", required=True),
            "Select": st.column_config.CheckboxColumn("Select", default=False),
        },
        disabled=[c for c in editor_input_df.columns if c not in {"TaskName", "Select"}],
    )

    base_task_series = editor_input_df["TaskName"].fillna("").astype(str).str.strip()
    new_task_series = edited_view_df["TaskName"].fillna("").astype(str).str.strip()
    changed_mask = new_task_series.ne(base_task_series)
    has_changes = bool(changed_mask.any())
    delete_mask = edited_view_df["Select"].fillna(False).astype(bool)
    has_deletes = bool(delete_mask.any())

    save_col, delete_col, refresh_col = st.columns([1, 1, 1])
    with save_col:
        if st.button("Save Task Log Edits", type="primary", width="stretch", disabled=not has_changes):
            changes = working_df.loc[changed_mask, ["TaskID", "__source_file", "__row_idx"]].copy()
            changes["TaskName_new"] = new_task_series.loc[changed_mask].values

            if changes["TaskName_new"].astype(str).str.strip().eq("").any():
                st.error("Task Name cannot be blank.")
            else:
                updated_rows, touched_files = _save_task_log_task_name_changes(changes)
                _load_task_log_entries.clear()
                utils.load_all_completed_tasks.clear()
                if updated_rows > 0:
                    st.success(f"Saved {updated_rows} row update(s) across {touched_files} file(s).")
                    st.rerun()
                else:
                    st.info("No rows were updated.")

    with delete_col:
        if st.button("Delete Selected", type="secondary", width="stretch", disabled=not has_deletes):
            delete_payload = working_df.loc[
                delete_mask,
                ["TaskID", "__source_file", "__row_idx", "Entry Date", "FullName", "TaskName", "Notes"],
            ].copy()
            st.session_state.tm_task_log_delete_payload = delete_payload
            st.session_state.tm_task_log_delete_confirm_open = True
            st.session_state.tm_task_log_delete_confirm_rendered = False
            st.rerun()

    with refresh_col:
        if st.button("Refresh Task Log", width="stretch"):
            _load_task_log_entries.clear()
            utils.load_all_completed_tasks.clear()
            st.rerun()

    if st.session_state.tm_task_log_delete_confirm_open and not st.session_state.tm_task_log_delete_confirm_rendered:
        st.session_state.tm_task_log_delete_confirm_rendered = True
        confirm_task_log_delete_dialog()


macro_logistics, macro_project_services, macro_general = st.tabs(
    ["Logistics - Support", "Project Services", "General"],
    width="stretch",
)

with macro_logistics:
    logistics_definition_tab, logistics_log_tab = st.tabs(["Task Definition", "Task Log"], width="stretch")
    with logistics_definition_tab:
        render_tasks_section()
    with logistics_log_tab:
        render_task_log_section()

with macro_project_services:
    st.info("No sections configured yet.")

with macro_general:
    general_users_tab = st.tabs(["Users"], width="stretch")[0]
    with general_users_tab:
        render_users_section()
