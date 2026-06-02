"""
pages/tasks-management.py

Purpose:
    Admin-only management page with content sections:
        - Task Definition: view and update tasks.parquet
        - Task Log: view submitted task records
        - Users: view users.parquet
"""

from __future__ import annotations

import datetime
import math
from pathlib import Path
import uuid

import pandas as pd
import streamlit as st

import config
import utils


LOGGER = utils.get_page_logger("Tasks Management")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Management")

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon=utils.get_app_icon())
utils.render_app_logo()
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
utils.render_page_header(PAGE_TITLE)

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
TASK_TARGETS_CSV_PATH = Path(config.TASK_TARGETS_CSV_PATH)
CADENCE_OPTIONS = ["Daily", "Weekly", "Periodic"]
DAILY_TARGET_BUFFER = 0.90
EDITABLE_COLUMNS = ["TaskName", "TaskCadence"]
TASK_TARGET_FILE_COLUMNS = ["Month", "Year", "TaskName", "Cadence", "UsersAssigned", "Multiplier", "Target"]
TASK_TARGET_DISPLAY_COLUMNS = ["Month", "Year", "TaskName", "Cadence", "UsersAssigned", "Target"]
TM_STATE_VERSION = 6

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
if "tm_task_targets_pending_year" not in st.session_state:
    st.session_state.tm_task_targets_pending_year = ""
if "tm_task_targets_pending_month" not in st.session_state:
    st.session_state.tm_task_targets_pending_month = ""

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


def _estimate_text_column_width(
    series: pd.Series,
    label: str,
    *,
    min_px: int = 90,
    max_px: int = 320,
    quantile: float = 0.9,
    char_px: int = 8,
    cap_chars: int = 80,
) -> int:
    """Estimate a reasonable editor column width from typical cell content."""
    lengths = series.fillna("").astype(str).str.len().clip(upper=cap_chars)
    if lengths.empty:
        target_chars = len(label)
    else:
        quantile_value = lengths.quantile(quantile)
        if pd.isna(quantile_value):
            target_chars = len(label)
        else:
            target_chars = max(len(label), int(round(float(quantile_value))))
    return max(min_px, min(max_px, 32 + target_chars * char_px))


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


def _get_file_signature(path: Path) -> tuple[int, int] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))), int(stat.st_size)


def _load_task_targets_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"taskstargets.csv not found at '{path}'.")
    try:
        df = pd.read_csv(path, sep=None, engine="python")
    except pd.errors.EmptyDataError:
        df = pd.DataFrame(columns=TASK_TARGET_FILE_COLUMNS)

    if "Cadence" not in df.columns and "TaskCadence" in df.columns:
        df["Cadence"] = df["TaskCadence"]
    elif "Cadence" in df.columns and "TaskCadence" in df.columns:
        df["Cadence"] = df["Cadence"].fillna(df["TaskCadence"])

    for col in TASK_TARGET_FILE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    ordered_columns = TASK_TARGET_FILE_COLUMNS + [c for c in df.columns if c not in TASK_TARGET_FILE_COLUMNS]
    df = df[ordered_columns].copy()
    for col in ["Month", "Year", "UsersAssigned"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["Multiplier"] = pd.to_numeric(df["Multiplier"], errors="coerce")
    df["Target"] = pd.to_numeric(df["Target"], errors="coerce").fillna(0.0).round(2)
    df["TaskName"] = df["TaskName"].fillna("").astype(str).str.strip()
    df["Cadence"] = df["Cadence"].fillna("").astype(str).str.strip()
    return df


def _write_task_targets_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.stem}.{uuid.uuid4().hex}.tmp")
    df.to_csv(tmp_path, sep="\t", index=False)
    tmp_path.replace(path)


def _format_int_filter_value(value: object) -> str:
    if pd.isna(value):
        return ""
    try:
        return str(int(value))
    except Exception:
        return str(value).strip()


def _task_target_exists(
    targets_df: pd.DataFrame,
    *,
    task_name: str,
    cadence: str,
    month: int,
    year: int,
) -> bool:
    if targets_df.empty:
        return False

    normalized_task_name = str(task_name or "").strip()
    normalized_cadence = str(cadence or "").strip().title()
    normalized_month = int(month)
    normalized_year = int(year)

    duplicate_mask = (
        targets_df["TaskName"].fillna("").astype(str).str.strip().eq(normalized_task_name)
        & targets_df["Cadence"].fillna("").astype(str).str.strip().str.title().eq(normalized_cadence)
        & pd.to_numeric(targets_df["Month"], errors="coerce").fillna(-1).astype(int).eq(normalized_month)
        & pd.to_numeric(targets_df["Year"], errors="coerce").fillna(-1).astype(int).eq(normalized_year)
    )
    return bool(duplicate_mask.any())


def _resolve_column_map(full_df: pd.DataFrame) -> dict[str, str]:
    return {
        "TaskName": _resolve_existing_column(full_df, ["TaskName", "Task Name"]) or "TaskName",
        "TaskCadence": _resolve_existing_column(full_df, ["TaskCadence", "Task Cadence"]) or "TaskCadence",
        "IsActive": _resolve_existing_column(full_df, ["IsActive", "Is Active"]) or "IsActive",
        "TargetMonthEnd": _resolve_existing_column(
            full_df,
            ["TargetMonthEnd", "Target Month End", "TargetMonth", "MonthEnd", "Month"],
        ) or "TargetMonthEnd",
        "Target": _resolve_existing_column(full_df, ["Target", "TaskTarget"]) or "Target",
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


def _get_task_definition_df(full_df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    task_col = col_map["TaskName"]
    cadence_col = col_map["TaskCadence"]
    active_col = col_map["IsActive"]

    definitions_df = full_df.copy()
    if task_col not in definitions_df.columns:
        definitions_df[task_col] = ""
    if cadence_col not in definitions_df.columns:
        definitions_df[cadence_col] = ""
    if active_col not in definitions_df.columns:
        definitions_df[active_col] = True

    definitions_df = definitions_df[[task_col, cadence_col, active_col]].copy()
    definitions_df[task_col] = definitions_df[task_col].fillna("").astype(str).str.strip()
    definitions_df[cadence_col] = definitions_df[cadence_col].fillna("").astype(str).str.strip().str.title()
    definitions_df = definitions_df[definitions_df[task_col].ne("")].copy()
    return definitions_df.drop_duplicates(subset=[task_col, cadence_col], keep="first").reset_index(drop=True)


def _format_new_is_active(raw_value: bool, active_series: pd.Series) -> object:
    if pd.api.types.is_bool_dtype(active_series):
        return bool(raw_value)
    if pd.api.types.is_numeric_dtype(active_series):
        return 1 if raw_value else 0

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


def _current_month_end() -> pd.Timestamp:
    eastern_now = pd.Timestamp(utils.to_eastern(utils.now_utc())).tz_localize(None)
    return (eastern_now + pd.offsets.MonthEnd(0)).normalize()


def _get_stored_monthly_targets(full_df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    month_col = col_map["TargetMonthEnd"]
    target_col = col_map["Target"]
    task_col = col_map["TaskName"]
    cadence_col = col_map["TaskCadence"]

    if month_col not in full_df.columns or target_col not in full_df.columns:
        return pd.DataFrame(columns=["TaskName", "TaskCadence", "TargetMonthEnd", "Target"])

    targets_df = full_df[[task_col, cadence_col, month_col, target_col]].copy()
    targets_df.columns = ["TaskName", "TaskCadence", "TargetMonthEnd", "Target"]
    targets_df["TaskName"] = targets_df["TaskName"].fillna("").astype(str).str.strip()
    targets_df["TaskCadence"] = targets_df["TaskCadence"].fillna("").astype(str).str.strip().str.title()
    targets_df["TargetMonthEnd"] = pd.to_datetime(targets_df["TargetMonthEnd"], errors="coerce").dt.normalize()
    targets_df["Target"] = pd.to_numeric(targets_df["Target"], errors="coerce").fillna(0.0).round(2)
    targets_df = targets_df.dropna(subset=["TargetMonthEnd"])
    return targets_df.sort_values(
        ["TargetMonthEnd", "TaskName", "TaskCadence"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def _get_target_month_options(targets_df: pd.DataFrame) -> list[pd.Timestamp]:
    if not targets_df.empty and "TargetMonthEnd" in targets_df.columns:
        month_options = sorted(
            {
                pd.Timestamp(value).normalize()
                for value in targets_df["TargetMonthEnd"].dropna().tolist()
            },
            reverse=True,
        )
        if month_options:
            return month_options
    return [_current_month_end()]


def _format_target_month_label(value: object) -> str:
    if pd.isna(value):
        return ""
    return pd.Timestamp(value).strftime("%B %Y")


def get_business_days_in_month(year: int, month: int) -> int:
    import calendar

    _, last_day = calendar.monthrange(year, month)
    return sum(
        1 for day in range(1, last_day + 1)
        if datetime.date(year, month, day).weekday() < 5
    )


def compute_target_multiplier(cadence: str, year: int, month: int) -> int:
    if cadence == "Daily":
        return get_business_days_in_month(year, month)
    elif cadence == "Weekly":
        bdays = get_business_days_in_month(year, month)
        return round(bdays / 5)
    else:
        return 1


def compute_default_target(cadence: str, users_assigned: int, year: int, month: int) -> tuple[int, int]:
    multiplier = compute_target_multiplier(cadence, year, month)
    actual_target = int(users_assigned) * multiplier
    if cadence == "Daily":
        return multiplier, int(math.ceil(actual_target * DAILY_TARGET_BUFFER))
    return multiplier, int(actual_target)


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

    definitions_df = _get_task_definition_df(full_df, col_map)

    st.session_state.tm_original_full_df = definitions_df.copy(deep=True)
    st.session_state.tm_working_full_df = definitions_df.copy(deep=True)
    st.session_state.tm_column_map = col_map
    st.session_state.tm_state_version = TM_STATE_VERSION
    st.session_state.tm_tasks_signature = _get_file_signature(TASKS_PARQUET_PATH)

    LOGGER.info(
        "Loaded tasks parquet | path='%s' stored_rows=%s task_definitions=%s",
        TASKS_PARQUET_PATH,
        len(full_df),
        len(definitions_df),
    )


def _tasks_state_needs_reload() -> bool:
    return (
        st.session_state.get("tm_state_version") != TM_STATE_VERSION
        or "tm_original_full_df" not in st.session_state
        or "tm_working_full_df" not in st.session_state
        or "tm_column_map" not in st.session_state
        or st.session_state.get("tm_tasks_signature") != _get_file_signature(TASKS_PARQUET_PATH)
    )


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
    utils.load_tasks.clear()
    stored_df = _load_tasks_parquet(TASKS_PARQUET_PATH)
    col_map = _resolve_column_map(stored_df)
    refreshed_definitions_df = _get_task_definition_df(stored_df, col_map)
    st.session_state.tm_original_full_df = refreshed_definitions_df.copy(deep=True)
    st.session_state.tm_working_full_df = refreshed_definitions_df.copy(deep=True)
    st.session_state.tm_column_map = col_map
    st.session_state.tm_tasks_signature = _get_file_signature(TASKS_PARQUET_PATH)
    st.session_state.tm_updated_toast = True

    LOGGER.info("tasks.parquet updated | path='%s' rows=%s", TASKS_PARQUET_PATH, len(stored_df))


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


@st.fragment
def render_tasks_section() -> None:
    try:
        if _tasks_state_needs_reload():
            with st.spinner("Loading task definitions..."):
                _initialize_state()
        else:
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

    filter_left, filter_mid = st.columns(2)
    with filter_left:
        task_name_filter = st.text_input(
            "Filter by Task Name",
            key="tm_filter_task_name",
            placeholder="Search task name",
        )
    with filter_mid:
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
        st.dataframe(
            display_df,
            width="stretch",
            hide_index=True,
        )
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


@st.fragment
def render_task_targets_section() -> None:
    try:
        if _tasks_state_needs_reload():
            _initialize_state()
        else:
            _initialize_state()
    except Exception as exc:
        LOGGER.exception("Failed to initialize task targets state: %s", exc)
        st.error(f"Failed to load task definitions: {exc}")
        return

    try:
        targets_df = _load_task_targets_csv(TASK_TARGETS_CSV_PATH)
    except Exception as exc:
        LOGGER.exception("Failed to load task targets csv: %s", exc)
        st.error(f"Failed to load task targets csv: {exc}")
        return

    saved_message = str(st.session_state.get("tm_task_target_saved_message", "") or "").strip()
    if saved_message:
        st.success(saved_message)
        st.session_state.tm_task_target_saved_message = ""

    current_eastern = pd.Timestamp(utils.to_eastern(utils.now_utc())).tz_localize(None)
    fallback_year = str(int(current_eastern.year))
    fallback_month = str(int(current_eastern.month))

    year_options = [
        _format_int_filter_value(value)
        for value in sorted(
            {value for value in targets_df["Year"].dropna().tolist()},
            reverse=True,
        )
    ]
    if not year_options:
        year_options = [fallback_year]

    year_key = "tm_task_targets_year"
    pending_year = str(st.session_state.get("tm_task_targets_pending_year", "") or "").strip()
    if pending_year in year_options:
        st.session_state[year_key] = pending_year
    st.session_state.tm_task_targets_pending_year = ""
    if st.session_state.get(year_key) not in year_options:
        st.session_state[year_key] = year_options[0]
    selected_year = st.pills(
        "Year",
        options=year_options,
        selection_mode="single",
        default=year_options[0],
        key=year_key,
        width="stretch",
    )
    if not selected_year:
        selected_year = year_options[0]

    filtered_month_source = targets_df[
        targets_df["Year"].map(_format_int_filter_value).eq(str(selected_year))
    ]
    month_options = [
        _format_int_filter_value(value)
        for value in sorted(
            {value for value in filtered_month_source["Month"].dropna().tolist()}
        )
    ]
    if not month_options:
        month_options = [fallback_month]

    month_key = "tm_task_targets_month"
    pending_month = str(st.session_state.get("tm_task_targets_pending_month", "") or "").strip()
    if pending_month in month_options:
        st.session_state[month_key] = pending_month
    st.session_state.tm_task_targets_pending_month = ""
    if st.session_state.get(month_key) not in month_options:
        st.session_state[month_key] = month_options[0]
    selected_month = st.pills(
        "Month",
        options=month_options,
        selection_mode="single",
        default=month_options[0],
        key=month_key,
        width="stretch",
    )
    if not selected_month:
        selected_month = month_options[0]

    filtered_targets_df = targets_df[
        targets_df["Year"].map(_format_int_filter_value).eq(str(selected_year))
        & targets_df["Month"].map(_format_int_filter_value).eq(str(selected_month))
    ].copy()
    display_df = filtered_targets_df[TASK_TARGET_DISPLAY_COLUMNS].copy()
    st.caption(f"Rows: {len(display_df):,} (of {len(targets_df):,})")

    edited_df = st.data_editor(
        display_df,
        hide_index=True,
        width="stretch",
        key="tm_task_targets_editor",
        column_config={
            "Target": st.column_config.NumberColumn("Target", format="%.2f"),
        },
        disabled=[col for col in display_df.columns if col != "Target"],
    )

    base_targets = pd.to_numeric(display_df.get("Target", pd.Series(dtype="float64")), errors="coerce").fillna(0.0).round(2)
    edited_targets = pd.to_numeric(edited_df.get("Target", pd.Series(dtype="float64")), errors="coerce").fillna(0.0).round(2)
    has_target_changes = bool(not edited_targets.equals(base_targets))

    save_col, refresh_col = st.columns([1, 1])
    with save_col:
        if st.button("Save Task Targets", type="primary", width="stretch", disabled=not has_target_changes):
            updated_targets_df = targets_df.copy(deep=True)
            if not filtered_targets_df.empty:
                updated_targets_df.loc[filtered_targets_df.index, "Target"] = edited_targets.values
                updated_targets_df["Target"] = pd.to_numeric(updated_targets_df["Target"], errors="coerce").fillna(0.0).round(2)
            _write_task_targets_csv(TASK_TARGETS_CSV_PATH, updated_targets_df)
            LOGGER.info(
                "taskstargets.csv updated | path='%s' filtered_rows=%s",
                TASK_TARGETS_CSV_PATH,
                len(filtered_targets_df),
            )
            st.success("Task targets updated.")
            st.rerun()

    with refresh_col:
        if st.button("Refresh Task Targets", width="stretch"):
            st.rerun()

    col_map = st.session_state.tm_column_map
    task_definitions_df = _get_task_definition_df(
        st.session_state.tm_working_full_df.copy(deep=True),
        col_map,
    )
    task_col = col_map["TaskName"]
    cadence_col = col_map["TaskCadence"]

    st.divider()
    st.subheader("Add Target", anchor=False)

    form_col1, form_col2, form_col3 = st.columns(3)

    target_task_options = [""] + sorted(
        value
        for value in task_definitions_df[task_col].fillna("").astype(str).str.strip().unique().tolist()
        if value
    )
    if st.session_state.get("tm_target_task_select") not in target_task_options:
        st.session_state.tm_target_task_select = ""
    with form_col1:
        target_task = st.selectbox("Task", target_task_options, key="tm_target_task_select")

    if target_task and not task_definitions_df.empty:
        target_cadence_options = (
            task_definitions_df.loc[
                task_definitions_df[task_col].fillna("").astype(str).str.strip().eq(target_task),
                cadence_col,
            ]
            .dropna()
            .astype(str)
            .str.strip()
            .str.title()
            .unique()
            .tolist()
        )
        target_cadence_options = [c for c in CADENCE_OPTIONS if c in target_cadence_options]
    else:
        target_cadence_options = CADENCE_OPTIONS
    if not target_cadence_options:
        target_cadence_options = CADENCE_OPTIONS
    if st.session_state.get("tm_target_cadence_select") not in target_cadence_options:
        st.session_state.tm_target_cadence_select = target_cadence_options[0]
    with form_col2:
        target_cadence = st.selectbox("Cadence", target_cadence_options, key="tm_target_cadence_select")

    if "tm_target_users_assigned" not in st.session_state:
        st.session_state.tm_target_users_assigned = 1
    with form_col3:
        target_users_assigned = st.number_input(
            "Users Assigned",
            min_value=1,
            step=1,
            key="tm_target_users_assigned",
        )

    now_et = utils.to_eastern(utils.now_utc())
    month_options = list(range(1, 13))
    year_options = list(range(now_et.year - 2, now_et.year + 3))

    if st.session_state.get("tm_target_month_select") not in month_options:
        st.session_state.tm_target_month_select = int(now_et.month)
    if st.session_state.get("tm_target_year_select") not in year_options:
        st.session_state.tm_target_year_select = int(now_et.year)

    date_col1, date_col2 = st.columns(2)
    with date_col1:
        target_month = st.selectbox(
            "Month",
            month_options,
            format_func=lambda m: datetime.date(2000, m, 1).strftime("%B"),
            key="tm_target_month_select",
        )
    with date_col2:
        target_year = st.selectbox(
            "Year",
            year_options,
            key="tm_target_year_select",
        )

    multiplier, default_target = compute_default_target(
        target_cadence,
        int(target_users_assigned),
        int(target_year),
        int(target_month),
    )
    actual_target = int(target_users_assigned) * multiplier
    target_signature = (
        str(target_task).strip(),
        str(target_cadence).strip(),
        int(target_users_assigned),
        int(target_month),
        int(target_year),
    )
    if st.session_state.get("tm_target_default_signature") != target_signature:
        st.session_state.tm_target_value_input = int(default_target)
        st.session_state.tm_target_default_signature = target_signature
    elif "tm_target_value_input" not in st.session_state:
        st.session_state.tm_target_value_input = int(default_target)

    if target_cadence == "Daily":
        st.caption(
            f"Default target: {target_users_assigned} user{'s' if target_users_assigned != 1 else ''} "
            f"x {multiplier} business days = {actual_target}, then 90% buffer applied -> **{default_target}**"
        )
    else:
        st.caption(
            f"Default target: {target_users_assigned} user{'s' if target_users_assigned != 1 else ''} "
            f"x {multiplier} "
            f"({'weeks' if target_cadence == 'Weekly' else 'periodic occurrence'}) "
            f"= **{default_target}**"
        )

    target_value = st.number_input(
        "Target",
        min_value=0,
        step=1,
        key="tm_target_value_input",
        help="Auto-computed from users assigned and cadence. You can override this.",
    )

    if st.button("Save Target", type="primary", key="tm_save_target_button"):
        if not target_task:
            st.error("Please select a task.")
        elif _task_target_exists(
            targets_df,
            task_name=target_task,
            cadence=target_cadence,
            month=int(target_month),
            year=int(target_year),
        ):
            st.error(
                "A target already exists for this task for the selected cadence and month. "
                "Please edit or delete the existing target before adding a new one."
            )
            LOGGER.warning(
                "Duplicate task target blocked | task='%s' cadence='%s' month=%s year=%s",
                target_task,
                target_cadence,
                target_month,
                target_year,
            )
        else:
            try:
                utils.save_task_target(
                    task_name=target_task,
                    cadence=target_cadence,
                    month=int(target_month),
                    year=int(target_year),
                    users_assigned=int(target_users_assigned),
                    target=int(target_value),
                    saved_by=utils.get_os_user(),
                )
            except ValueError as exc:
                LOGGER.warning("Task target rejected during save: %s", exc)
                st.error(str(exc))
            else:
                st.session_state.tm_task_targets_pending_year = str(target_year)
                st.session_state.tm_task_targets_pending_month = str(target_month)
                st.session_state.tm_task_target_saved_message = (
                    f"Target saved: {target_task} / {target_cadence} / "
                    f"{datetime.date(int(target_year), int(target_month), 1).strftime('%B %Y')} -> {int(target_value)}"
                )
                LOGGER.info(
                    "Task target saved | task='%s' cadence='%s' month=%s year=%s users=%s target=%s",
                    target_task,
                    target_cadence,
                    target_month,
                    target_year,
                    target_users_assigned,
                    target_value,
                )
                st.rerun()


@st.fragment
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


@st.cache_data(ttl=30, show_spinner="Loading task log...")
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


def _parse_partition_user_key(source_file: Path) -> str | None:
    """Extract the ``user=<key>`` partition segment from a completed-task path."""
    for part in Path(source_file).parts:
        text = str(part)
        if text.startswith("user="):
            return text[len("user="):]
    return None


def _parse_partition_date(source_file: Path) -> tuple[int, int, int] | None:
    """Extract the (year, month, day) partition segments from a path, if present."""
    year = month = day = None
    for part in Path(source_file).parts:
        text = str(part)
        try:
            if text.startswith("year="):
                year = int(text[len("year="):])
            elif text.startswith("month="):
                month = int(text[len("month="):])
            elif text.startswith("day="):
                day = int(text[len("day="):])
        except ValueError:
            continue
    if year and month and day:
        return (year, month, day)
    return None


def _completed_root_from_source(source_file: Path) -> Path:
    """Return the partition root (the path above ``user=...``) for a source file."""
    parts = Path(source_file).parts
    for i, part in enumerate(parts):
        if str(part).startswith("user=") and i > 0:
            return Path(*parts[:i])
    return Path(config.COMPLETED_TASKS_DIR)


def _et_str_to_utc(text: object):
    """Parse an Eastern 'YYYY-MM-DD HH:MM[:SS]' string to a tz-aware UTC Timestamp.

    Returns None for blank input, or the sentinel string 'INVALID' if it cannot
    be parsed.
    """
    text = str(text or "").strip()
    if not text:
        return None
    parsed = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            parsed = datetime.datetime.strptime(text, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        return "INVALID"
    try:
        localized = pd.Timestamp(parsed).tz_localize(
            "America/New_York", ambiguous=True, nonexistent="shift_forward"
        )
    except Exception:
        return "INVALID"
    return localized.tz_convert("UTC")


def _build_fullname_login_map() -> dict[str, str]:
    """Map each known Full Name to its login from users.parquet (first wins)."""
    try:
        users_df = _load_users_parquet(USERS_PARQUET_PATH)
    except Exception:
        return {}
    name_col = _resolve_existing_column(users_df, ["Full Name", "FullName", "Name"])
    login_col = _resolve_existing_column(
        users_df, ["User", "UserLogin", "Login", "Username", "User Name"]
    )
    mapping: dict[str, str] = {}
    if name_col and login_col:
        for _, row in users_df.iterrows():
            name = str(row.get(name_col) or "").strip()
            login = str(row.get(login_col) or "").strip()
            if name and login and name not in mapping:
                mapping[name] = login
    return mapping


def _new_task_log_filename(start_ts: object, task_id: object, target_dir: Path) -> Path:
    """Build a non-colliding task_<ts>_<id>.parquet path inside ``target_dir``."""
    if start_ts is not None and not pd.isna(start_ts):
        eastern = pd.Timestamp(start_ts).tz_convert("America/New_York")
        stamp = eastern.strftime("%Y%m%d_%H%M%S")
    else:
        stamp = "00000000_000000"
    tid = str(task_id or "").strip() or uuid.uuid4().hex
    base = f"task_{stamp}_{tid[:8]}"
    candidate = target_dir / f"{base}.parquet"
    if candidate.exists():
        candidate = target_dir / f"{base}_{uuid.uuid4().hex[:8]}.parquet"
    return candidate


def _save_task_log_changes(changes_df: pd.DataFrame) -> tuple[int, int, int]:
    """Persist edited task-log values back to the partitioned parquet files.

    Rows whose owning user (FullName) or calendar date changed are relocated to
    the correct ``user=/year=/month=/day=`` partition: the row is written to a
    new file in the destination partition and removed from its source file.
    Everything else is updated in place. Returns
    (updated_rows, touched_files, relocated_rows).
    """
    if changes_df.empty:
        return 0, 0, 0

    string_cols = ["TaskID", "Notes", "TaskName", "FullName", "UserLogin"]
    ts_cols = ["StartTimestampUTC", "EndTimestampUTC"]

    updated_rows = 0
    relocated_rows = 0
    touched_files: set[str] = set()

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

        for col in string_cols:
            if col not in file_df.columns:
                file_df[col] = pd.NA
        for col in ts_cols:
            if col not in file_df.columns:
                file_df[col] = pd.NaT
        if "DurationSeconds" not in file_df.columns:
            file_df["DurationSeconds"] = 0

        src_user_key = _parse_partition_user_key(file_path)
        src_date = _parse_partition_date(file_path)
        root = _completed_root_from_source(file_path)

        file_changed = False
        drop_index_labels: set = set()
        inserts: list[tuple[Path, pd.DataFrame]] = []

        for _, change in group.iterrows():
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

            new_task_name = str(change.get("new_TaskName") or "").strip()
            new_notes = str(change.get("new_Notes") or "").strip()
            new_full_name = str(change.get("new_FullName") or "").strip()
            new_login = str(change.get("new_UserLogin") or "").strip()
            new_start = change.get("new_StartUTC")
            new_end = change.get("new_EndUTC")
            try:
                new_duration = int(change.get("new_DurationSeconds"))
            except (TypeError, ValueError):
                new_duration = None
            user_changed = bool(change.get("user_changed"))
            time_changed = bool(change.get("time_changed"))

            new_start_ts = (
                pd.Timestamp(new_start) if new_start is not None and not pd.isna(new_start) else None
            )
            new_end_ts = (
                pd.Timestamp(new_end) if new_end is not None and not pd.isna(new_end) else None
            )

            # ---- resolve the destination partition ----
            if user_changed and new_login:
                target_user_key = utils.sanitize_key(new_login)
            elif src_user_key is not None:
                target_user_key = src_user_key
            else:
                target_user_key = utils.sanitize_key(new_login)

            target_date = None
            if time_changed and new_start_ts is not None:
                eastern = new_start_ts.tz_convert("America/New_York")
                target_date = (eastern.year, eastern.month, eastern.day)
            elif src_date is not None:
                target_date = src_date
            elif new_start_ts is not None:
                eastern = new_start_ts.tz_convert("America/New_York")
                target_date = (eastern.year, eastern.month, eastern.day)

            user_moved = bool(
                user_changed and src_user_key is not None and target_user_key != src_user_key
            )
            date_moved = bool(
                time_changed
                and target_date is not None
                and (src_date is None or target_date != src_date)
            )
            relocate = (user_moved or date_moved) and target_date is not None

            def _apply_values(frame: pd.DataFrame, idx) -> None:
                frame.at[idx, "TaskName"] = new_task_name or None
                frame.at[idx, "Notes"] = new_notes or None
                frame.at[idx, "FullName"] = new_full_name or None
                if new_login:
                    frame.at[idx, "UserLogin"] = new_login
                if new_start_ts is not None:
                    frame.at[idx, "StartTimestampUTC"] = new_start_ts
                if new_end_ts is not None:
                    frame.at[idx, "EndTimestampUTC"] = new_end_ts
                if new_duration is not None and new_duration >= 0:
                    frame.at[idx, "DurationSeconds"] = int(new_duration)

            if relocate:
                insert_df = file_df.loc[[target_index]].copy()
                ins_idx = insert_df.index[0]
                _apply_values(insert_df, ins_idx)
                if not str(insert_df.at[ins_idx, "TaskID"] or "").strip():
                    insert_df.at[ins_idx, "TaskID"] = str(uuid.uuid4())
                year, month, day = target_date
                target_dir = (
                    root
                    / f"user={target_user_key}"
                    / f"year={year}"
                    / f"month={month:02d}"
                    / f"day={day:02d}"
                )
                dest = _new_task_log_filename(new_start_ts, insert_df.at[ins_idx, "TaskID"], target_dir)
                inserts.append((dest, insert_df.reset_index(drop=True)))
                drop_index_labels.add(target_index)
                relocated_rows += 1
                updated_rows += 1
            else:
                _apply_values(file_df, target_index)
                file_changed = True
                updated_rows += 1

        # Write relocated rows to their new partition before pruning the source,
        # so a mid-operation failure cannot lose data.
        for dest, insert_df in inserts:
            _write_tasks_parquet(dest, insert_df)
            touched_files.add(str(dest))

        if drop_index_labels:
            file_df = file_df.drop(index=list(drop_index_labels))
            file_changed = True

        if file_changed:
            touched_files.add(str(file_path))
            if file_df.empty:
                try:
                    file_path.unlink(missing_ok=True)
                except Exception as exc:
                    LOGGER.warning("Failed to remove emptied task log source file '%s': %s", file_path, exc)
                    _write_tasks_parquet(file_path, file_df.reset_index(drop=True))
            else:
                _write_tasks_parquet(file_path, file_df.reset_index(drop=True))

    return updated_rows, len(touched_files), relocated_rows


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
            utils.load_completed_tasks_for_analytics.clear()
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


@st.fragment
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

    if "DurationSeconds" in df.columns:
        df["DurationSeconds"] = pd.to_numeric(df["DurationSeconds"], errors="coerce").fillna(0).astype(int)
    else:
        df["DurationSeconds"] = 0

    # Truth columns needed to recompute edits / relocate rows on save.
    for _ts_col in ("StartTimestampUTC", "EndTimestampUTC"):
        if _ts_col not in df.columns:
            df[_ts_col] = pd.NaT
    if "UserLogin" not in df.columns:
        df["UserLogin"] = ""

    display_cols = [
        "Entry Date",
        "FullName",
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
    working_df = sorted_df[
        display_cols
        + [
            "DurationSeconds",
            "StartTimestampUTC",
            "EndTimestampUTC",
            "UserLogin",
            "TaskID",
            "__source_file",
            "__row_idx",
        ]
    ].reset_index(drop=True)
    view_df = working_df[display_cols].copy()
    editor_input_df = view_df.copy()
    editor_input_df.insert(0, "Select", False)
    st.caption(f"Rows: {len(view_df):,} (of {len(df):,})")

    # FullName becomes a dropdown so reassignments resolve to a real login.
    fullname_login_map = _build_fullname_login_map()
    existing_full_names = sorted(
        n
        for n in editor_input_df["FullName"].fillna("").astype(str).str.strip().unique().tolist()
        if n
    )
    fullname_options = sorted(set(fullname_login_map.keys()) | set(existing_full_names))
    # SelectboxColumn requires every shown value to be in options; blanks -> None.
    editor_input_df["FullName"] = (
        editor_input_df["FullName"].fillna("").astype(str).str.strip().replace("", None)
    )

    task_log_column_config = {
        "Entry Date": st.column_config.DateColumn(
            "Entry Date",
            width=120,
            format="YYYY-MM-DD",
            help=(
                "Moves the entry to another day, keeping its original time of day. "
                "If you also edit Start (ET), Start (ET) wins."
            ),
        ),
        "FullName": st.column_config.SelectboxColumn(
            "FullName",
            width=_estimate_text_column_width(editor_input_df["FullName"], "FullName", min_px=140, max_px=260),
            options=fullname_options,
            help="Reassign this entry to another user. Only users in users.parquet can be picked.",
        ),
        "TaskName": st.column_config.TextColumn(
            "TaskName",
            width=_estimate_text_column_width(editor_input_df["TaskName"], "TaskName", min_px=160, max_px=300),
        ),
        "Duration": st.column_config.TextColumn("Duration", width=110, help="Use HH:MM or HH:MM:SS."),
        "Start (ET)": st.column_config.TextColumn(
            "Start (ET)", width=170, help="Eastern time: YYYY-MM-DD HH:MM:SS"
        ),
        "End (ET)": st.column_config.TextColumn(
            "End (ET)", width=170, help="Eastern time: YYYY-MM-DD HH:MM:SS"
        ),
        "PartiallyComplete": st.column_config.CheckboxColumn("PartiallyComplete", width=145),
        "Notes": st.column_config.TextColumn(
            "Notes",
            width=_estimate_text_column_width(
                editor_input_df["Notes"],
                "Notes",
                min_px=180,
                max_px=520,
                quantile=0.95,
                cap_chars=120,
            ),
        ),
        "Select": st.column_config.CheckboxColumn("Select", default=False, width="small"),
    }

    edited_view_df = st.data_editor(
        editor_input_df,
        hide_index=True,
        width="stretch",
        key="tm_task_log_editor",
        column_config=task_log_column_config,
        disabled=[
            c
            for c in editor_input_df.columns
            if c
            not in {
                "Entry Date",
                "FullName",
                "TaskName",
                "Duration",
                "Start (ET)",
                "End (ET)",
                "Notes",
                "Select",
            }
        ],
    )

    def _strip(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip()

    base_full = _strip(editor_input_df["FullName"])
    new_full = _strip(edited_view_df["FullName"])
    base_task = _strip(editor_input_df["TaskName"])
    new_task = _strip(edited_view_df["TaskName"])
    base_notes_series = _strip(editor_input_df["Notes"])
    new_notes_series = _strip(edited_view_df["Notes"])
    base_start = _strip(editor_input_df["Start (ET)"])
    new_start = _strip(edited_view_df["Start (ET)"])
    base_end = _strip(editor_input_df["End (ET)"])
    new_end = _strip(edited_view_df["End (ET)"])
    base_duration = _strip(editor_input_df["Duration"])
    new_duration = _strip(edited_view_df["Duration"])

    def _date_key(series: pd.Series) -> pd.Series:
        # Normalize dates (and any null representation) to YYYY-MM-DD / "" so an
        # untouched empty Entry Date never reads as a change.
        return series.apply(
            lambda v: "" if v is None or pd.isna(v) else pd.Timestamp(v).strftime("%Y-%m-%d")
        )

    base_entry = _date_key(editor_input_df["Entry Date"])
    new_entry = _date_key(edited_view_df["Entry Date"])

    task_changed = new_task.ne(base_task)
    notes_changed = new_notes_series.ne(base_notes_series)
    full_changed = new_full.ne(base_full)
    start_changed = new_start.ne(base_start)
    end_changed = new_end.ne(base_end)
    duration_changed = new_duration.ne(base_duration)
    entry_changed = new_entry.ne(base_entry)

    row_changed_mask = (
        task_changed
        | notes_changed
        | full_changed
        | start_changed
        | end_changed
        | duration_changed
        | entry_changed
    )
    has_changes = bool(row_changed_mask.any())
    delete_mask = edited_view_df["Select"].fillna(False).astype(bool)
    has_deletes = bool(delete_mask.any())

    save_col, delete_col, refresh_col = st.columns([1, 1, 1])
    with save_col:
        if st.button("Save Task Log Edits", type="primary", width="stretch", disabled=not has_changes):
            errors: list[str] = []
            payload_rows: list[dict] = []
            for i in [pos for pos in range(len(edited_view_df)) if bool(row_changed_mask.iloc[pos])]:
                row_label = i + 1
                orig_start_ts = working_df.at[i, "StartTimestampUTC"]
                orig_end_ts = working_df.at[i, "EndTimestampUTC"]
                try:
                    orig_duration_val = int(working_df.at[i, "DurationSeconds"])
                except (TypeError, ValueError):
                    orig_duration_val = 0
                orig_login = str(working_df.at[i, "UserLogin"] or "").strip()

                time_changed = bool(
                    start_changed.iloc[i]
                    or end_changed.iloc[i]
                    or duration_changed.iloc[i]
                    or entry_changed.iloc[i]
                )

                # --- FullName / login ---
                row_full_name = base_full.iloc[i]
                row_login = orig_login
                user_changed = False
                if full_changed.iloc[i]:
                    row_full_name = new_full.iloc[i]
                    if not row_full_name:
                        errors.append(f"Row {row_label}: FullName cannot be blank.")
                        continue
                    resolved = fullname_login_map.get(row_full_name)
                    if not resolved:
                        errors.append(
                            f"Row {row_label}: cannot reassign to '{row_full_name}' "
                            "— no matching user in users.parquet."
                        )
                        continue
                    row_login = resolved
                    user_changed = utils.sanitize_key(row_login) != utils.sanitize_key(orig_login)

                # --- TaskName ---
                row_task_name = base_task.iloc[i]
                if task_changed.iloc[i]:
                    row_task_name = new_task.iloc[i]
                    if not row_task_name:
                        errors.append(f"Row {row_label}: TaskName cannot be blank.")
                        continue

                # --- Notes ---
                row_notes = new_notes_series.iloc[i] if notes_changed.iloc[i] else base_notes_series.iloc[i]

                # --- Start / End / Duration ---
                row_start_ts = orig_start_ts
                row_end_ts = orig_end_ts
                row_duration = orig_duration_val
                if time_changed:
                    # Start (ET) wins over Entry Date when both were touched.
                    if start_changed.iloc[i]:
                        parsed = _et_str_to_utc(new_start.iloc[i])
                        if parsed is None or isinstance(parsed, str):
                            errors.append(
                                f"Row {row_label}: invalid Start (ET). Use YYYY-MM-DD HH:MM:SS."
                            )
                            continue
                        row_start_ts = parsed
                    elif entry_changed.iloc[i]:
                        new_date = edited_view_df.iloc[i]["Entry Date"]
                        if new_date is None or pd.isna(new_date):
                            errors.append(f"Row {row_label}: invalid Entry Date.")
                            continue
                        if orig_start_ts is not None and not pd.isna(orig_start_ts):
                            tod = pd.Timestamp(orig_start_ts).tz_convert("America/New_York").time()
                        else:
                            tod = datetime.time(0, 0, 0)
                        naive = datetime.datetime.combine(new_date, tod)
                        try:
                            row_start_ts = (
                                pd.Timestamp(naive)
                                .tz_localize("America/New_York", ambiguous=True, nonexistent="shift_forward")
                                .tz_convert("UTC")
                            )
                        except Exception:
                            errors.append(f"Row {row_label}: invalid Entry Date.")
                            continue

                    # End (ET) wins over Duration when both were touched.
                    if end_changed.iloc[i]:
                        parsed_end = _et_str_to_utc(new_end.iloc[i])
                        if parsed_end is None or isinstance(parsed_end, str):
                            errors.append(
                                f"Row {row_label}: invalid End (ET). Use YYYY-MM-DD HH:MM:SS."
                            )
                            continue
                        row_end_ts = parsed_end
                        row_duration = int((row_end_ts - row_start_ts).total_seconds())
                    elif duration_changed.iloc[i]:
                        parsed_dur = utils.parse_hhmmss(new_duration.iloc[i])
                        if parsed_dur < 0:
                            errors.append(
                                f"Row {row_label}: invalid Duration. Use HH:MM or HH:MM:SS."
                            )
                            continue
                        row_duration = parsed_dur
                        row_end_ts = (
                            row_start_ts + pd.to_timedelta(parsed_dur, unit="s")
                            if (row_start_ts is not None and not pd.isna(row_start_ts))
                            else orig_end_ts
                        )
                    else:
                        # Only the start moved: shift end with it, keep duration.
                        row_duration = orig_duration_val
                        row_end_ts = (
                            row_start_ts + pd.to_timedelta(orig_duration_val, unit="s")
                            if (row_start_ts is not None and not pd.isna(row_start_ts))
                            else orig_end_ts
                        )

                    if row_duration is not None and row_duration < 0:
                        errors.append(f"Row {row_label}: End (ET) is before Start (ET).")
                        continue

                payload_rows.append(
                    {
                        "TaskID": working_df.at[i, "TaskID"],
                        "__source_file": working_df.at[i, "__source_file"],
                        "__row_idx": working_df.at[i, "__row_idx"],
                        "new_TaskName": row_task_name,
                        "new_Notes": row_notes,
                        "new_FullName": row_full_name,
                        "new_UserLogin": row_login,
                        "new_StartUTC": row_start_ts,
                        "new_EndUTC": row_end_ts,
                        "new_DurationSeconds": int(row_duration)
                        if row_duration is not None
                        else orig_duration_val,
                        "user_changed": user_changed,
                        "time_changed": time_changed,
                    }
                )

            if errors:
                st.error("Could not save:\n\n" + "\n\n".join(f"- {e}" for e in errors[:20]))
            elif not payload_rows:
                st.info("No rows were updated.")
            else:
                changes = pd.DataFrame(payload_rows)
                updated_rows, touched_files, relocated_rows = _save_task_log_changes(changes)
                _load_task_log_entries.clear()
                utils.load_all_completed_tasks.clear()
                utils.load_completed_tasks_for_analytics.clear()
                if updated_rows > 0:
                    message = f"Saved {updated_rows} row update(s) across {touched_files} file(s)."
                    if relocated_rows:
                        message += f" {relocated_rows} row(s) moved to a different user/date."
                    st.success(message)
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
            utils.load_completed_tasks_for_analytics.clear()
            st.rerun()

    if st.session_state.tm_task_log_delete_confirm_open and not st.session_state.tm_task_log_delete_confirm_rendered:
        st.session_state.tm_task_log_delete_confirm_rendered = True
        confirm_task_log_delete_dialog()


macro_logistics, macro_project_services, macro_general = st.tabs(
    ["Logistics - Support", "Project Services", "General"],
    width="stretch",
)

with macro_logistics:
    logistics_definition_tab, logistics_targets_tab, logistics_log_tab = st.tabs(
        ["Task Definition", "Task Targets", "Task Log"],
        width="stretch",
    )
    with logistics_definition_tab:
        render_tasks_section()
    with logistics_targets_tab:
        render_task_targets_section()
    with logistics_log_tab:
        render_task_log_section()

with macro_project_services:
    st.info("No sections configured yet.")

with macro_general:
    general_users_tab = st.tabs(["Users"], width="stretch")[0]
    with general_users_tab:
        render_users_section()
