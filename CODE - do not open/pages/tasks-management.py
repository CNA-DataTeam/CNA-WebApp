"""
pages/tasks-management.py

Purpose:
    Admin-only page to view and update tasks.parquet.
"""

from __future__ import annotations

from pathlib import Path
import uuid

import pandas as pd
import streamlit as st

import config
import utils


LOGGER = utils.get_page_logger("Tasks Management")

st.set_page_config(page_title="Tasks Management", layout="wide")
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

logo_b64 = utils.get_logo_base64(str(config.LOGO_PATH))
st.markdown(
    f"""
    <div class="header-row">
        <img class="header-logo" src="data:image/png;base64,{logo_b64}" />
        <h1 class="header-title">LS - Tasks Management</h1>
    </div>
    """,
    unsafe_allow_html=True,
)
st.divider()

utils.log_page_open_once("tasks_management_page", LOGGER)
if "_tasks_management_render_logged" not in st.session_state:
    st.session_state._tasks_management_render_logged = True
    LOGGER.info("Render UI.")

if not utils.is_current_user_admin():
    LOGGER.warning("Access denied for non-admin user '%s'.", utils.get_os_user())
    st.error("Access denied. This page is available to admin users only.")
    st.stop()


TASKS_PARQUET_PATH = Path(config.PERSONNEL_DIR) / "tasks.parquet"
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


def _coerce_bool(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    if isinstance(value, (int, float)):
        return float(value) != 0.0
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y", "on"}


def _load_tasks_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"tasks.parquet not found at '{path}'.")
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

selected_rows: list[int] = []
try:
    event = st.dataframe(
        view_df,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
    )
    if event is not None and hasattr(event, "selection"):
        selected_rows = list(getattr(event.selection, "rows", []) or [])
except TypeError:
    st.dataframe(view_df, width="stretch", hide_index=True)
    row_options = list(range(len(view_df)))
    selected_rows = st.multiselect(
        "Select rows to delete",
        options=row_options,
        format_func=lambda idx: f"{idx + 1}: {view_df.iloc[idx]['Task Name']}",
    )


add_col, delete_col = st.columns([1, 1])
with add_col:
    if st.button("Add a task", width="stretch"):
        opening_add_mode = not st.session_state.tm_add_mode
        st.session_state.tm_add_mode = opening_add_mode
        if opening_add_mode:
            st.session_state.tm_new_task_name = ""
            st.session_state.tm_new_task_cadence = CADENCE_OPTIONS[0]

with delete_col:
    if st.button("Delete", width="stretch", disabled=len(selected_rows) == 0):
        if selected_rows:
            drop_positions = sorted({int(idx) for idx in selected_rows if 0 <= int(idx) < len(working_full_df)})
            if drop_positions:
                drop_index = working_full_df.index[drop_positions]
                working_full_df = working_full_df.drop(index=drop_index).reset_index(drop=True)
                st.session_state.tm_working_full_df = working_full_df
                LOGGER.info("Deleted task rows | count=%s", len(drop_positions))
                st.rerun()


if st.session_state.tm_add_mode:
    name_col, cadence_col, save_col, cancel_col = st.columns([3.6, 2.0, 1.2, 1.2])

    with name_col:
        st.text_input("Task Name", key="tm_new_task_name", placeholder="Type the Task Name", label_visibility="collapsed")
    with cadence_col:
        st.selectbox("Task Cadence", options=CADENCE_OPTIONS, key="tm_new_task_cadence", label_visibility="collapsed")

    with save_col:
        if st.button("Add", type="primary", width="stretch"):
            new_task_name = str(st.session_state.get("tm_new_task_name")).strip()
            if not new_task_name:
                st.warning("Task name cannot be blank.")
            else:
                new_row = {col: pd.NA for col in working_full_df.columns}
                new_row[col_map["TaskName"]] = new_task_name
                new_row[col_map["TaskCadence"]] = str(st.session_state.get("tm_new_task_cadence", CADENCE_OPTIONS[0]))
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
