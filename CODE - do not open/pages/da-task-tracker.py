"""
pages/da-task-tracker.py

Purpose:
    Daily task logging UI with timer + live activity broadcast for Data & Analytics.

What it does:
    - Loads tasks/users/accounts metadata.
    - Lets user:
        * choose task
        * optionally select account, department, primary stakeholder + notes
        * start/pause/resume/end a timer
        * upload completed task record as parquet (partitioned by user/date)
    - Writes/updates a small per-user live activity parquet while timing,
      so other users can see who is working on what in real time.
    - Displays:
        * Live Activity (other users)
        * Today's Activity (completed tasks)
"""

import streamlit as st
import pandas as pd
import pyarrow as pa
import uuid
from pathlib import Path
import config
import utils

_AUTOREFRESH_IMPORT_ERROR: Exception | None = None
try:
    from streamlit_autorefresh import st_autorefresh
except Exception as exc:
    _AUTOREFRESH_IMPORT_ERROR = exc

    def st_autorefresh(*_args, **_kwargs):
        return None

LOGGER = utils.get_page_logger("DA Task Tracker")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Task Tracker")

# D&A parquet schema (adds Department column)
DA_PARQUET_SCHEMA = pa.schema([
    ("TaskID", pa.string()),
    ("UserLogin", pa.string()),
    ("FullName", pa.string()),
    ("TaskName", pa.string()),
    ("TaskCadence", pa.string()),
    ("CompanyGroup", pa.string()),
    ("IsCoveringFor", pa.bool_()),
    ("CoveringFor", pa.string()),
    ("Notes", pa.string()),
    ("PartiallyComplete", pa.bool_()),
    ("StartTimestampUTC", pa.timestamp("us", tz="UTC")),
    ("EndTimestampUTC", pa.timestamp("us", tz="UTC")),
    ("DurationSeconds", pa.int64()),
    ("UploadTimestampUTC", pa.timestamp("us", tz="UTC")),
    ("AppVersion", pa.string()),
    ("Department", pa.string()),
])

DEPARTMENT_OPTIONS = [
    "",
    "Data & Analytics",
    "Leadership",
    "Business Relations",
    "Platform",
    "Project Services",
    "Logistics",
    "FS&D",
    "Account Support",
    "Accounts Receivable",
    "Partnership Development",
    "Marketing",
    "Employee Optimization",
]

# Page configuration
st.set_page_config(page_title=PAGE_TITLE, layout="wide")
utils.render_app_logo()
utils.log_page_open_once("da_task_tracker_page", LOGGER)
if "da__task_tracker_render_logged" not in st.session_state:
    st.session_state.da__task_tracker_render_logged = True
    LOGGER.info("Render UI.")
if _AUTOREFRESH_IMPORT_ERROR is not None and "da__task_tracker_autorefresh_warned" not in st.session_state:
    st.session_state.da__task_tracker_autorefresh_warned = True
    LOGGER.warning(
        "Auto-refresh component unavailable. Timer will update on user interaction. error=%s",
        _AUTOREFRESH_IMPORT_ERROR,
    )

# Apply global styling
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

# Resolve paths and constants — D&A directories
COMPLETED_TASKS_DIR = config.DA_COMPLETED_TASKS_DIR
LIVE_ACTIVITY_DIR = config.DA_LIVE_ACTIVITY_DIR
ARCHIVED_TASKS_DIR = config.DA_ARCHIVED_TASKS_DIR
PERSONNEL_DIR = config.PERSONNEL_DIR

# Session state initialization (prefixed with da_ to avoid conflicts)
DEFAULT_STATE = {
    "da_state": "idle",
    "da_start_utc": None,
    "da_end_utc": None,
    "da_pause_start_utc": None,
    "da_paused_seconds": 0,
    "da_elapsed_seconds": 0,
    "da_notes": "",
    "da_last_task_name": "",
    "da_reset_counter": 0,
    "da_confirm_open": False,
    "da_confirm_rendered": False,
    "da_partially_complete": False,
    "da_primary_stakeholder": "",
    "da_department": "",
    "da_live_activity_saved": False,
    "da_live_task_name": "",
    "da_live_account": "",
    "da_state_restored": False,
    "da_restored_task_name": None,
    "da_restored_account": None,
    "da_restored_primary_stakeholder": None,
    "da_restored_department": None,
    "da_review_archive_open": False,
    "da_review_archive_rendered": False,
    "da_ended_from_paused": False,
    "da_active_task_name": "",
}
# Ensure all default keys are set
for k, v in DEFAULT_STATE.items():
    if k not in st.session_state:
        st.session_state[k] = v

# Restore state from live activity file on page load/refresh
_user_key_for_restore = utils.sanitize_key(utils.get_os_user())
if not st.session_state.da_state_restored:
    st.session_state.da_state_restored = True
    restored = utils.load_own_live_activity(LIVE_ACTIVITY_DIR, _user_key_for_restore)
    if restored:
        st.session_state.da_state = restored["state"]
        st.session_state.da_start_utc = restored["start_utc"]
        st.session_state.da_paused_seconds = restored["paused_seconds"]
        st.session_state.da_pause_start_utc = restored["pause_start_utc"]
        st.session_state.da_notes = restored["notes"]
        st.session_state.da_primary_stakeholder = restored.get("covering_for", "")
        st.session_state.da_live_activity_saved = True
        st.session_state.da_last_task_name = restored["task_name"]
        st.session_state.da_restored_task_name = restored["task_name"]
        st.session_state.da_restored_account = restored["account"]
        st.session_state.da_restored_primary_stakeholder = restored.get("covering_for", "")
        LOGGER.info(
            "Restored active session | task='%s' state='%s'",
            restored["task_name"],
            restored["state"],
        )

# Business logic functions
def compute_elapsed_seconds() -> int:
    """Compute total elapsed seconds for current task (excluding paused time)."""
    if not st.session_state.da_start_utc:
        return 0
    now = st.session_state.da_end_utc if st.session_state.da_state == "ended" else utils.now_utc()
    base = int((now - st.session_state.da_start_utc).total_seconds())
    paused = int(st.session_state.da_paused_seconds or 0)
    if st.session_state.da_pause_start_utc:
        pause_delta = int((now - st.session_state.da_pause_start_utc).total_seconds())
        if pause_delta > 0:
            paused += pause_delta
    return max(0, base - paused)

def reset_all():
    """Reset all task state and delete current live activity record."""
    if "da_current_user_key" in st.session_state:
        utils.delete_live_activity(LIVE_ACTIVITY_DIR, st.session_state.da_current_user_key)
    old_counter = st.session_state.da_reset_counter
    st.session_state.da_reset_counter += 1
    # Remove old widget state keys
    for key in [f"da_task_{old_counter}", f"da_acct_{old_counter}", f"da_dept_{old_counter}"]:
        st.session_state.pop(key, None)
    # Reset all state values (except restoration flag)
    for k, v in DEFAULT_STATE.items():
        if k != "da_state_restored":
            st.session_state[k] = v

def start_task():
    """Start a new task timing."""
    selected_task = str(st.session_state.get(f"da_task_{st.session_state.da_reset_counter}", "") or "").strip()
    LOGGER.info("Started task timer | task='%s'", selected_task)
    st.session_state.da_active_task_name = selected_task
    st.session_state.da_state = "running"
    st.session_state.da_start_utc = utils.now_utc()
    st.session_state.da_end_utc = None
    st.session_state.da_paused_seconds = 0
    st.session_state.da_pause_start_utc = None
    st.session_state.da_elapsed_seconds = 0
    st.session_state.da_ended_from_paused = False
    st.session_state.da_live_activity_saved = False

def pause_task():
    """Pause the current task."""
    if st.session_state.da_state != "running":
        return
    LOGGER.info("Paused task timer.")
    st.session_state.da_state = "paused"
    if not st.session_state.da_pause_start_utc:
        st.session_state.da_pause_start_utc = utils.now_utc()
    if "da_current_user_key" in st.session_state:
        utils.update_live_activity_state(
            LIVE_ACTIVITY_DIR,
            st.session_state.da_current_user_key,
            state="paused",
            paused_seconds=st.session_state.da_paused_seconds,
            pause_start_utc=st.session_state.da_pause_start_utc,
        )

def resume_task():
    """Resume a paused task."""
    if st.session_state.da_state != "paused":
        return
    LOGGER.info("Resumed task timer.")
    if st.session_state.da_pause_start_utc:
        pause_delta = int((utils.now_utc() - st.session_state.da_pause_start_utc).total_seconds())
        if pause_delta > 0:
            st.session_state.da_paused_seconds += pause_delta
    st.session_state.da_pause_start_utc = None
    st.session_state.da_state = "running"
    if "da_current_user_key" in st.session_state:
        utils.update_live_activity_state(
            LIVE_ACTIVITY_DIR,
            st.session_state.da_current_user_key,
            state="running",
            paused_seconds=st.session_state.da_paused_seconds,
            pause_start_utc=None,
        )

def end_task():
    """End the current task."""
    LOGGER.info("Ended task timer.")
    if st.session_state.da_pause_start_utc:
        pause_delta = int((utils.now_utc() - st.session_state.da_pause_start_utc).total_seconds())
        if pause_delta > 0:
            st.session_state.da_paused_seconds += pause_delta
    st.session_state.da_pause_start_utc = None
    st.session_state.da_ended_from_paused = False
    st.session_state.da_state = "ended"
    st.session_state.da_end_utc = utils.now_utc()
    if "da_current_user_key" in st.session_state:
        utils.delete_live_activity(LIVE_ACTIVITY_DIR, st.session_state.da_current_user_key)

def format_start_datetime(dt_utc):
    """Format UTC datetime for human-readable Eastern display."""
    if not dt_utc:
        return "None"
    dt_et = utils.to_eastern(dt_utc)
    return dt_et.strftime("%m/%d/%Y %I:%M:%S %p").lower()

def get_submit_duration_seconds(default_seconds: int) -> int:
    """Use effective elapsed time (paused time excluded)."""
    return max(0, int(default_seconds))

def archive_task(user_login: str, full_name: str, user_key: str, task_name: str, selected_account: str) -> None:
    """Archive the currently paused task and reset the page state."""
    if st.session_state.da_state != "paused" or not st.session_state.da_start_utc:
        return
    paused_seconds = int(st.session_state.da_paused_seconds)
    if st.session_state.da_pause_start_utc:
        paused_seconds += int((utils.now_utc() - st.session_state.da_pause_start_utc).total_seconds())
    utils.save_archived_task(
        ARCHIVED_TASKS_DIR,
        user_key,
        user_login,
        full_name,
        task_name,
        None,
        selected_account,
        st.session_state.da_primary_stakeholder,
        st.session_state.da_notes,
        st.session_state.da_start_utc,
        paused_seconds=paused_seconds,
        pause_start_utc=None,
    )
    utils.load_archived_tasks.clear()
    if "da_current_user_key" in st.session_state:
        utils.delete_live_activity(LIVE_ACTIVITY_DIR, st.session_state.da_current_user_key)
    reset_all()
    LOGGER.info("Archived paused task | task='%s' user='%s'", task_name, user_login)
    st.session_state.da_archived = True

def build_task_record(user_login: str, full_name: str, task_name: str,
                      account: str, primary_stakeholder: str, department: str,
                      notes: str, duration_seconds: int, partially_complete: bool) -> dict:
    """Build a record dict for a completed task entry."""
    has_stakeholder = bool(primary_stakeholder and primary_stakeholder.strip())
    return {
        "TaskID": str(uuid.uuid4()),
        "UserLogin": user_login,
        "FullName": full_name or None,
        "TaskName": task_name,
        "TaskCadence": None,
        "CompanyGroup": account or None,
        "IsCoveringFor": has_stakeholder,
        "CoveringFor": primary_stakeholder or None,
        "Notes": notes.strip() if notes and notes.strip() else None,
        "PartiallyComplete": partially_complete,
        "StartTimestampUTC": st.session_state.da_start_utc,
        "EndTimestampUTC": st.session_state.da_end_utc,
        "DurationSeconds": int(duration_seconds),
        "UploadTimestampUTC": utils.now_utc(),
        "AppVersion": config.APP_VERSION,
        "Department": department or None,
    }


def get_effective_task_name(current_task_name: str) -> str:
    """Return the best non-empty task name to use for save/log operations."""
    candidates = [
        current_task_name,
        st.session_state.get("da_active_task_name", ""),
        st.session_state.get("da_live_task_name", ""),
        st.session_state.get("da_last_task_name", ""),
        st.session_state.get("da_restored_task_name", ""),
    ]
    for candidate in candidates:
        task_value = str(candidate or "").strip()
        if task_value:
            return task_value
    return ""

# Confirmation dialog for task submission
@st.dialog("Submit?")
def confirm_submit(user_login, full_name, user_key, task_name, selected_account):
    """Modal confirmation dialog for submitting a completed task."""
    task_name = get_effective_task_name(task_name)
    st.caption(f"**User:** {full_name}")
    st.caption(f"**Task:** {task_name}")
    st.caption(f"**Started On:** {format_start_datetime(st.session_state.da_start_utc)}")
    stakeholder = st.session_state.da_primary_stakeholder
    st.caption(f"**Primary Stakeholder:** {stakeholder if stakeholder and stakeholder.strip() else 'None'}")
    st.caption(f"**Department:** {st.session_state.da_department if st.session_state.da_department else 'None'}")
    st.caption(f"**Account:** {selected_account if selected_account else 'None'}")
    st.caption(f"**Notes:** {st.session_state.da_notes if st.session_state.da_notes else 'None'}")
    st.caption(f"**Partially Complete:** {'Yes' if st.session_state.get('da_submit_partially_complete', False) else 'No'}")
    st.divider()
    effective_duration_seconds = get_submit_duration_seconds(st.session_state.da_elapsed_seconds)
    current_duration = utils.format_hhmmss(effective_duration_seconds)
    edited_duration = st.text_input("Duration", value=current_duration, key="da_edit_duration", max_chars=8)
    parsed_duration = utils.parse_hhmmss(edited_duration)
    if parsed_duration < 0:
        parsed_duration = effective_duration_seconds
        LOGGER.warning("Invalid edited duration format '%s'. Reverting to original duration.", edited_duration)
        st.warning("Invalid format - using original duration")
    st.divider()
    left, right = st.columns(2)
    with left:
        if st.button("Submit", type="primary", width="stretch"):
            if not task_name:
                LOGGER.warning("Submit blocked because task name is blank.")
                st.error("Task name is required. Please reset and select a task again.")
                return
            if edited_duration.strip() == current_duration:
                parsed_duration = effective_duration_seconds
            record = build_task_record(
                user_login,
                full_name,
                task_name,
                selected_account,
                st.session_state.da_primary_stakeholder,
                st.session_state.da_department,
                st.session_state.da_notes,
                parsed_duration,
                st.session_state.get("da_submit_partially_complete", False),
            )
            df_record = pd.DataFrame([record])
            out_dir = utils.build_out_dir(COMPLETED_TASKS_DIR, user_key, st.session_state.da_start_utc)
            eastern_start = utils.to_eastern(st.session_state.da_start_utc)
            fname = f"task_{eastern_start:%Y%m%d_%H%M%S}_{record['TaskID'][:8]}.parquet"
            utils.atomic_write_parquet(df_record, out_dir / fname, schema=DA_PARQUET_SCHEMA)
            utils.load_all_completed_tasks.clear()
            utils.load_completed_tasks_for_analytics.clear()
            utils.load_recent_tasks.clear()
            try:
                utils.sync_tasks_parquet_targets()
            except Exception as exc:
                LOGGER.warning("Completed task uploaded but target sync failed: %s", exc)
            LOGGER.info(
                "Uploaded completed task | task='%s' department='%s' duration_seconds=%s partially_complete=%s",
                task_name,
                st.session_state.da_department,
                parsed_duration,
                st.session_state.get("da_submit_partially_complete", False),
            )
            st.session_state.da_confirm_open = False
            st.session_state.da_confirm_rendered = False
            reset_all()
            st.session_state.da_uploaded = True
            st.rerun()
    with right:
        if st.button("Cancel", width="stretch"):
            st.session_state.da_confirm_open = False
            st.session_state.da_confirm_rendered = False
            st.rerun()

@st.dialog("Archived Tasks")
def review_archived_tasks_dialog(user_login, full_name, user_key):
    """Review archived tasks with options to resume or delete."""
    archived_df = utils.load_archived_tasks(ARCHIVED_TASKS_DIR, user_key)
    if archived_df.empty:
        LOGGER.info("Archived tasks dialog opened but no archived tasks were found.")
        st.info("No archived tasks found.")
        return

    st.caption("Resume or delete paused tasks saved in archive.")
    for idx, row in archived_df.iterrows():
        task_name = str(row.get("TaskName") or "")
        start_utc = pd.to_datetime(row.get("StartTimestampUTC"), utc=True).to_pydatetime()
        start_text = format_start_datetime(start_utc)
        st.markdown(f"**{task_name}**")
        st.caption(f"Start: {start_text}")
        resume_col, delete_col = st.columns(2)
        with resume_col:
            if st.button("Resume", key=f"da_resume_archive_{idx}", width="stretch"):
                archive_path_raw = str(row.get("ArchiveFilePath") or "").strip()
                if archive_path_raw:
                    archive_path = Path(archive_path_raw)
                    utils.delete_archived_task_file(archive_path)

                st.session_state.da_state = "paused"
                st.session_state.da_start_utc = start_utc
                st.session_state.da_end_utc = None
                st.session_state.da_paused_seconds = int(row.get("PausedSeconds", 0) or 0)
                st.session_state.da_pause_start_utc = utils.now_utc()
                st.session_state.da_notes = str(row.get("Notes") or "")
                st.session_state.da_primary_stakeholder = str(row.get("CoveringFor") or "")
                st.session_state.da_restored_task_name = task_name
                st.session_state.da_restored_account = str(row.get("CompanyGroup") or "")
                st.session_state.da_restored_primary_stakeholder = st.session_state.da_primary_stakeholder
                st.session_state.da_live_activity_saved = False
                st.session_state.da_last_task_name = task_name
                st.session_state.da_review_archive_open = False
                st.session_state.da_review_archive_rendered = False
                utils.load_archived_tasks.clear()
                LOGGER.info("Resumed archived task | task='%s' user='%s'", task_name, user_login)
                st.rerun()
        with delete_col:
            if st.button("Delete", key=f"da_delete_archive_{idx}", width="stretch"):
                archive_path_raw = str(row.get("ArchiveFilePath") or "").strip()
                if archive_path_raw:
                    archive_path = Path(archive_path_raw)
                    utils.delete_archived_task_file(archive_path)
                utils.load_archived_tasks.clear()
                st.session_state.da_review_archive_open = True
                st.session_state.da_review_archive_rendered = False
                LOGGER.info("Deleted archived task | task='%s' user='%s'", task_name, user_login)
                st.rerun()
        st.divider()

def load_own_tasks_with_paths(completed_dir: Path, user_key_val: str) -> pd.DataFrame:
    """Load current user's completed tasks for today, with file paths for deletion."""
    today_eastern = utils.to_eastern(utils.now_utc()).date()
    base = (
        completed_dir
        / f"user={user_key_val}"
        / f"year={today_eastern.year}"
        / f"month={today_eastern.month:02d}"
        / f"day={today_eastern.day:02d}"
    )
    if not base.exists():
        return pd.DataFrame()
    files = list(base.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        try:
            df_one = pd.read_parquet(f)
            df_one["_file_path"] = str(f)
            frames.append(df_one)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "StartTimestampUTC" in df.columns:
        df["StartTimestampUTC"] = pd.to_datetime(df["StartTimestampUTC"], utc=True)
        df = df.sort_values("StartTimestampUTC", ascending=False)
    return df.reset_index(drop=True)


def delete_completed_task(file_path: str, completed_dir: Path, user_key_val: str) -> bool:
    """Delete a completed task parquet file, only if it belongs to the user."""
    p = Path(file_path)
    expected_prefix = str(completed_dir / f"user={user_key_val}")
    if not str(p).startswith(expected_prefix):
        LOGGER.warning("Blocked delete outside allowed directory: %s", p)
        return False
    if p.exists():
        p.unlink()
        LOGGER.info("Deleted completed task file: %s", p.name)
        return True
    return False


# Live activity section (refreshes periodically to show team activity)
@st.fragment(run_every=30)
def live_activity_section():
    # Load live activities for all other users
    live_activities_df = utils.load_live_activities(LIVE_ACTIVITY_DIR, _exclude_user_key=st.session_state.get("da_current_user_key"))
    if not live_activities_df.empty:
        st.divider()
        st.markdown(
            """
            <h3 style="margin-bottom: 0;">
                <span class="live-activity-pulse"></span>
                Live Activity
            </h3>
            """,
            unsafe_allow_html=True,
        )
        st.caption("Tasks currently in progress by other team members")
        live_display_df = live_activities_df[["StartTimestampUTC", "FullName", "UserLogin", "TaskName", "Notes"]].copy()
        start_utc = pd.to_datetime(live_display_df["StartTimestampUTC"], utc=True)
        live_display_df["Start Time"] = start_utc.dt.tz_convert(utils.EASTERN_TZ).dt.strftime("%#I:%M %p").str.lower() + " - " + start_utc.apply(lambda x: utils.format_time_ago(x))
        if "Notes" not in live_display_df.columns:
            live_display_df["Notes"] = ""
        live_display_df["Notes"] = live_display_df["Notes"].fillna("")
        # Resolve display name
        live_display_df["User"] = live_display_df["FullName"].fillna("").astype(str).str.strip()
        mask_blank = live_display_df["User"].eq("")
        live_display_df.loc[mask_blank, "User"] = live_display_df.loc[mask_blank, "UserLogin"].fillna("").astype(str)
        # Prepare final display DataFrame
        display_cols = pd.DataFrame({
            "User": live_display_df["User"],
            "Task": live_display_df["TaskName"],
            "Start Time": live_display_df["Start Time"],
            "Notes": live_display_df["Notes"],
        })
        st.dataframe(display_cols, hide_index=True, width="stretch")

# Header
utils.render_page_header(PAGE_TITLE)
if st.session_state.get("da_uploaded"):
    LOGGER.info("Showing upload success toast.")
    st.toast("Upload Successful", icon="\u2705")
    st.session_state.da_uploaded = False
if st.session_state.get("da_archived"):
    LOGGER.info("Showing archived-task toast.")
    st.toast("Task archived")
    st.session_state.da_archived = False

# Main layout columns
spacer_l, left_col, _, mid_col, _, right_col, spacer_r = st.columns([0.4, 4, 0.2, 4, 0.2, 4, 0.4])
with left_col:
    user_login = utils.get_os_user()
    full_name = utils.get_full_name_for_user(None, user_login)
    user_key = utils.sanitize_key(user_login)
    st.session_state.da_current_user_key = user_key
    inputs_locked = st.session_state.da_state != "idle"
    st.text_input("User", value=full_name, disabled=True)
    # Primary Stakeholder (free-text field)
    stakeholder_key = f"da_stakeholder_{st.session_state.da_reset_counter}"
    if st.session_state.da_restored_primary_stakeholder and stakeholder_key not in st.session_state:
        st.session_state[stakeholder_key] = st.session_state.da_restored_primary_stakeholder
    primary_stakeholder = st.text_input(
        "Primary Stakeholder (optional)",
        disabled=inputs_locked,
        key=stakeholder_key,
    )
    st.session_state.da_primary_stakeholder = primary_stakeholder
    # Department dropdown
    dept_key = f"da_dept_{st.session_state.da_reset_counter}"
    if st.session_state.da_restored_department and dept_key not in st.session_state:
        if st.session_state.da_restored_department in DEPARTMENT_OPTIONS:
            st.session_state[dept_key] = st.session_state.da_restored_department
    selected_department = st.selectbox("Department (optional)", DEPARTMENT_OPTIONS, key=dept_key, disabled=inputs_locked)
    st.session_state.da_department = selected_department
    # Account dropdown
    account_options = [""] + utils.load_accounts(str(PERSONNEL_DIR))
    acct_key = f"da_acct_{st.session_state.da_reset_counter}"
    if st.session_state.da_restored_account and acct_key not in st.session_state:
        if st.session_state.da_restored_account in account_options:
            st.session_state[acct_key] = st.session_state.da_restored_account
    selected_account = st.selectbox("Account (optional)", account_options, key=acct_key, disabled=inputs_locked)
    archived_count = len(utils.load_archived_tasks(ARCHIVED_TASKS_DIR, user_key))
    data_signature = (
        len([a for a in account_options if a]),
        archived_count,
    )
    if st.session_state.get("da__task_tracker_data_signature") != data_signature:
        st.session_state.da__task_tracker_data_signature = data_signature
        LOGGER.info(
            "Data snapshot | accounts=%s archived_tasks=%s",
            data_signature[0],
            data_signature[1],
        )

with mid_col:
    task_key = f"da_task_{st.session_state.da_reset_counter}"
    if st.session_state.da_restored_task_name and task_key not in st.session_state:
        st.session_state[task_key] = st.session_state.da_restored_task_name
    task_name = st.text_input("Task", disabled=inputs_locked, key=task_key)
    effective_task_name = get_effective_task_name(task_name)
    st.text_area("Notes (optional)", key="da_notes", height=120)

with right_col:
    st.session_state.da_elapsed_seconds = compute_elapsed_seconds()
    hh, mm = utils.format_hh_mm_parts(st.session_state.da_elapsed_seconds)
    colon_class = "blink-colon" if st.session_state.da_state == "running" else ""
    st.markdown(
        f"""
        <div style="text-align:center;margin-bottom:20px;">
            <div style="font-size:36px;font-weight:600;">
                {hh}<span class="{colon_class}">:</span>{mm}
            </div>
            <div style="font-size:15px;color:#6b6b6b;">Elapsed Time</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.session_state.da_state == "running":
        st_autorefresh(interval=10000, key="da_timer")
    if st.session_state.da_state == "idle":
        c1, c2 = st.columns(2)
        can_start = bool(task_name)
        with c1:
            st.button("Start", width="stretch", disabled=not can_start,
                      help=None if can_start else "Select a task to start time",
                      on_click=start_task if can_start else None)
        with c2:
            st.button("End", width="stretch", disabled=True)
    elif st.session_state.da_state == "running":
        c1, c2 = st.columns(2)
        with c1:
            st.button("Pause", width="stretch", on_click=pause_task)
        with c2:
            st.button("End", width="stretch", on_click=end_task)
    elif st.session_state.da_state == "paused":
        c1, c2 = st.columns(2)
        with c1:
            st.button("Resume", width="stretch", on_click=resume_task)
        with c2:
            st.button("End", width="stretch", on_click=end_task)
    if st.session_state.da_state == "paused":
        st.button(
            "Archive",
            width="stretch",
            on_click=archive_task,
            args=(user_login, full_name, user_key, effective_task_name, selected_account),
        )
    if archived_count > 0:
        if st.button(
            f"You have {archived_count} archived tasks, click here to review",
            key="da_review_archived_link",
            type="tertiary",
        ):
            LOGGER.info("User opened archived tasks review with %s archived item(s).", archived_count)
            st.session_state.da_review_archive_open = True
            st.session_state.da_review_archive_rendered = False
            st.rerun()
    if st.session_state.da_state == "ended":
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Upload", type="primary", width="stretch"):
                if not effective_task_name:
                    LOGGER.warning("Upload blocked because effective task name is blank.")
                    st.error("Task name is missing. Please reset and select a task again.")
                else:
                    st.session_state.da_submit_partially_complete = st.session_state.get("da_partially_complete", False)
                    st.session_state.da_confirm_open = True
                    st.session_state.da_confirm_rendered = False
                    st.rerun()
        with c2:
            st.button("Reset", width="stretch", on_click=reset_all)
    if st.session_state.da_state != "idle":
        pc_left, pc_right = st.columns([1.4, 3])
        with pc_left:
            st.markdown("<div style='padding-top: 8px;'>Partially complete?</div>", unsafe_allow_html=True)
        with pc_right:
            st.toggle("Partially complete", key="da_partially_complete", label_visibility="collapsed")

# Save live activity after input changes (to broadcast running task to others)
if st.session_state.da_state in ("running", "paused") and not st.session_state.da_live_activity_saved and effective_task_name:
    try:
        utils.save_live_activity(
            LIVE_ACTIVITY_DIR, user_key, user_login, full_name,
            effective_task_name, None, selected_account,
            st.session_state.da_primary_stakeholder, st.session_state.da_notes,
            st.session_state.da_start_utc,
            state=st.session_state.da_state,
            paused_seconds=st.session_state.da_paused_seconds,
            pause_start_utc=st.session_state.da_pause_start_utc,
        )
        st.session_state.da_live_activity_saved = True
        st.session_state.da_live_task_name = effective_task_name
        st.session_state.da_live_account = selected_account
        LOGGER.info(
            "Live activity saved | task='%s' state='%s'",
            effective_task_name,
            st.session_state.da_state,
        )
    except Exception as exc:
        LOGGER.warning("Failed to save live activity: %s", exc)

# Open confirmation modal if triggered
if st.session_state.da_confirm_open and not st.session_state.get("da_confirm_rendered"):
    st.session_state.da_confirm_rendered = True
    confirm_submit(user_login, full_name, user_key, effective_task_name, selected_account)
if st.session_state.da_review_archive_open and not st.session_state.get("da_review_archive_rendered"):
    st.session_state.da_review_archive_rendered = True
    review_archived_tasks_dialog(user_login, full_name, user_key)

# Render live activity section (ongoing tasks of other users)
live_activity_section()

# Today's completed tasks section
st.divider()
title_col, text_col, toggle_col = st.columns([6, 5, 0.5], vertical_alignment="center")
with title_col:
    st.subheader("Today's Activity", anchor=False)
with text_col:
    st.markdown("Show all users?", text_alignment="right")
with toggle_col:
    show_all_users = st.toggle("Show all users?", value=True, key="da_show_all_users", label_visibility="collapsed")
_last_show_all = st.session_state.get("da__last_show_all_users")
if _last_show_all is None or _last_show_all != show_all_users:
    LOGGER.info("Today's Activity filter changed | show_all_users=%s", show_all_users)
    st.session_state.da__last_show_all_users = show_all_users

if st.session_state.get("da_task_deleted"):
    st.toast("Task deleted", icon="\U0001F5D1")
    st.session_state.da_task_deleted = False

# Load today's completed tasks and own-task file paths for deletion
if show_all_users:
    recent_df = utils.load_recent_tasks(COMPLETED_TASKS_DIR, user_key=None, limit=50)
else:
    recent_df = utils.load_recent_tasks(COMPLETED_TASKS_DIR, user_key=user_key, limit=50)

own_tasks_df = load_own_tasks_with_paths(COMPLETED_TASKS_DIR, user_key)
own_file_map: dict[str, str] = {}
if not own_tasks_df.empty and "StartTimestampUTC" in own_tasks_df.columns:
    for _, orow in own_tasks_df.iterrows():
        own_file_map[str(orow["StartTimestampUTC"])] = str(orow["_file_path"])

if not recent_df.empty:
    recent_df["Duration"] = recent_df["DurationSeconds"].apply(utils.format_hhmmss)
    recent_df["Uploaded"] = pd.to_datetime(recent_df["EndTimestampUTC"], utc=True).apply(lambda x: utils.format_time_ago(x))
    if "PartiallyComplete" not in recent_df.columns:
        recent_df["PartiallyComplete"] = pd.Series([pd.NA] * len(recent_df), dtype="boolean")
    else:
        recent_df["PartiallyComplete"] = recent_df["PartiallyComplete"].astype("boolean")
    recent_df["Part. Completed?"] = recent_df["PartiallyComplete"].fillna(False).astype(bool)
    if "Notes" not in recent_df.columns:
        recent_df["Notes"] = ""
    recent_df["Notes"] = recent_df["Notes"].fillna("")
    if "FullName" not in recent_df.columns:
        recent_df["FullName"] = ""
    recent_df["DisplayUser"] = recent_df["FullName"].fillna("").astype(str).str.strip()
    mask_blank = recent_df["DisplayUser"].eq("")
    recent_df.loc[mask_blank, "DisplayUser"] = recent_df.loc[mask_blank, "UserLogin"].fillna("").astype(str)

    # Build per-row file path list for deletion lookups
    row_file_paths: list[str] = []
    for _, rrow in recent_df.iterrows():
        is_own = str(rrow.get("UserLogin", "")).strip().lower() == user_login.lower()
        ts_key = str(rrow.get("StartTimestampUTC"))
        row_file_paths.append(own_file_map.get(ts_key, "") if is_own else "")

    display_df = recent_df.rename(columns={"TaskName": "Task", "DisplayUser": "User"})[["User", "Task", "Part. Completed?", "Uploaded", "Duration", "Notes"]].copy()
    display_df.insert(0, "Delete", False)

    edited_df = st.data_editor(
        display_df,
        hide_index=True,
        width="stretch",
        disabled=["User", "Task", "Part. Completed?", "Uploaded", "Duration", "Notes"],
        column_config={
            "Delete": st.column_config.CheckboxColumn("\U0001F5D1", width="small", default=False),
            "Part. Completed?": st.column_config.CheckboxColumn("Part. Completed?", disabled=True, width="small"),
            "Notes": st.column_config.TextColumn("Notes", width="large"),
            "Uploaded": st.column_config.TextColumn("Uploaded", width="small"),
        },
    )

    # Find checked rows that belong to the current user
    checked_indices = edited_df.index[edited_df["Delete"]].tolist()
    if checked_indices:
        deletable = [(i, row_file_paths[i]) for i in checked_indices if row_file_paths[i]]
        non_own = len(checked_indices) - len(deletable)
        if non_own > 0:
            st.caption("You can only delete your own tasks.")
        if deletable:
            n = len(deletable)
            if st.button(
                f"Confirm delete ({n} task{'s' if n > 1 else ''})",
                key="da_confirm_delete_btn",
                type="primary",
            ):
                for _, fpath in deletable:
                    delete_completed_task(fpath, COMPLETED_TASKS_DIR, user_key)
                utils.load_recent_tasks.clear()
                utils.load_all_completed_tasks.clear()
                utils.load_completed_tasks_for_analytics.clear()
                st.session_state.da_task_deleted = True
                st.rerun()
else:
    LOGGER.info("No tasks completed today to display.")
    st.info("No tasks completed today.")

# Footer with app version
st.caption(f"\n\n\nApp version: {config.APP_VERSION}", text_alignment="center")
