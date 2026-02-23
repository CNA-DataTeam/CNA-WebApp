"""
pages/task-tracker.py

Purpose:
    Daily task logging UI with timer + live activity broadcast.

What it does:
    - Loads tasks/users/accounts metadata.
    - Lets user:
        * choose task + cadence
        * optionally select account + covering-for + notes
        * start/pause/resume/end a timer
        * upload completed task record as parquet (partitioned by user/date)
    - Writes/updates a small per-user live activity parquet while timing,
      so other users can see who is working on what in real time.
    - Displays:
        * Live Activity (other users)
        * Today's Activity (completed tasks)

Key utils used (inputs -> outputs):
    - utils.get_global_css() -> str
    - utils.get_logo_base64(logo_path) -> str
    - utils.get_os_user() -> str
    - utils.get_full_name_for_user(None, user_login) -> str
    - utils.load_all_user_full_names() -> list[str]
    - utils.load_tasks() -> DataFrame (active tasks)
    - utils.load_accounts(personnel_dir) -> list[str] (company groups)
    - utils.sanitize_key(value) -> str (safe user key)
    - utils.now_utc() -> datetime
    - utils.to_eastern(dt) -> datetime
    - utils.format_hh_mm_parts(seconds) -> (hh, mm)
    - utils.format_hhmmss(seconds) -> str
    - utils.parse_hhmmss("HH:MM[:SS]") -> int seconds or -1
    - utils.format_time_ago(dt) -> str
    - utils.build_out_dir(completed_dir, user_key, ts) -> Path
    - utils.atomic_write_parquet(df, path, schema) -> writes parquet atomically
    - Live Activity:
        * utils.save_live_activity(...)
        * utils.update_live_activity_state(...)
        * utils.load_own_live_activity(...)
        * utils.delete_live_activity(...)
        * utils.load_live_activities(...)
    - Recent tasks:
        * utils.load_recent_tasks(completed_dir, user_key, limit) -> DataFrame

Primary inputs:
    - config.* directories (CompletedTasks, LiveActivity, Personnel, Logo)
    - startup output parquet(s) under config.PERSONNEL_DIR

Primary outputs:
    - Completed task parquet files written under config.COMPLETED_TASKS_DIR
      partitioned as: user=<key>/year=<YYYY>/month=<MM>/day=<DD>/*.parquet
    - Live activity parquet written under config.LIVE_ACTIVITY_DIR as:
      user=<key>.parquet
    - Streamlit UI rendering of timer, forms, and data tables
"""

import streamlit as st
import pandas as pd
import uuid
from pathlib import Path
import config
import utils
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(page_title="Task Tracker", layout="wide")

# Apply global styling
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

# Resolve paths and constants
COMPLETED_TASKS_DIR = config.COMPLETED_TASKS_DIR
LIVE_ACTIVITY_DIR = config.LIVE_ACTIVITY_DIR
ARCHIVED_TASKS_DIR = config.ARCHIVED_TASKS_DIR
PERSONNEL_DIR = config.PERSONNEL_DIR
LOGO_PATH = config.LOGO_PATH

# Session state initialization
DEFAULT_STATE = {
    "state": "idle",
    "start_utc": None,
    "end_utc": None,
    "pause_start_utc": None,
    "paused_seconds": 0,
    "elapsed_seconds": 0,
    "notes": "",
    "selected_cadence": None,
    "last_task_name": "",
    "reset_counter": 0,
    "confirm_open": False,
    "confirm_rendered": False,
    "partially_complete": False,
    "covering_for": "",
    "live_activity_saved": False,
    "live_task_name": "",
    "live_cadence": "",
    "live_account": "",
    "state_restored": False,
    "restored_task_name": None,
    "restored_account": None,
    "restored_covering_for": None,
    "review_archive_open": False,
    "review_archive_rendered": False,
    "ended_from_paused": False,
}
# Ensure all default keys are set
for k, v in DEFAULT_STATE.items():
    if k not in st.session_state:
        st.session_state[k] = v

# Restore state from live activity file on page load/refresh
_user_key_for_restore = utils.sanitize_key(utils.get_os_user())
if not st.session_state.state_restored:
    st.session_state.state_restored = True
    restored = utils.load_own_live_activity(LIVE_ACTIVITY_DIR, _user_key_for_restore)
    if restored:
        st.session_state.state = restored["state"]
        st.session_state.start_utc = restored["start_utc"]
        st.session_state.paused_seconds = restored["paused_seconds"]
        st.session_state.pause_start_utc = restored["pause_start_utc"]
        st.session_state.selected_cadence = restored["cadence"]
        st.session_state.notes = restored["notes"]
        st.session_state.covering_for = restored["covering_for"]
        st.session_state.live_activity_saved = True
        st.session_state.last_task_name = restored["task_name"]
        st.session_state.restored_task_name = restored["task_name"]
        st.session_state.restored_account = restored["account"]
        st.session_state.restored_covering_for = restored["covering_for"]

# Business logic functions
def compute_elapsed_seconds() -> int:
    """Compute total elapsed seconds for current task (excluding paused time)."""
    if not st.session_state.start_utc:
        return 0
    now = st.session_state.end_utc if st.session_state.state == "ended" else utils.now_utc()
    base = int((now - st.session_state.start_utc).total_seconds())
    paused = int(st.session_state.paused_seconds or 0)
    if st.session_state.pause_start_utc:
        pause_delta = int((now - st.session_state.pause_start_utc).total_seconds())
        if pause_delta > 0:
            paused += pause_delta
    return max(0, base - paused)

def reset_all():
    """Reset all task state and delete current live activity record."""
    if "current_user_key" in st.session_state:
        utils.delete_live_activity(LIVE_ACTIVITY_DIR, st.session_state.current_user_key)
    old_counter = st.session_state.reset_counter
    st.session_state.reset_counter += 1
    # Remove old widget state keys
    for key in [f"task_{old_counter}", f"acct_{old_counter}", f"covering_{old_counter}"]:
        st.session_state.pop(key, None)
    # Reset all state values (except restoration flag)
    for k, v in DEFAULT_STATE.items():
        if k != "state_restored":
            st.session_state[k] = v

def start_task():
    """Start a new task timing."""
    st.session_state.state = "running"
    st.session_state.start_utc = utils.now_utc()
    st.session_state.end_utc = None
    st.session_state.paused_seconds = 0
    st.session_state.pause_start_utc = None
    st.session_state.elapsed_seconds = 0
    st.session_state.ended_from_paused = False
    st.session_state.live_activity_saved = False

def pause_task():
    """Pause the current task."""
    if st.session_state.state != "running":
        return
    st.session_state.state = "paused"
    if not st.session_state.pause_start_utc:
        st.session_state.pause_start_utc = utils.now_utc()
    if "current_user_key" in st.session_state:
        utils.update_live_activity_state(
            LIVE_ACTIVITY_DIR,
            st.session_state.current_user_key,
            state="paused",
            paused_seconds=st.session_state.paused_seconds,
            pause_start_utc=st.session_state.pause_start_utc,
        )

def resume_task():
    """Resume a paused task."""
    if st.session_state.state != "paused":
        return
    if st.session_state.pause_start_utc:
        pause_delta = int((utils.now_utc() - st.session_state.pause_start_utc).total_seconds())
        if pause_delta > 0:
            st.session_state.paused_seconds += pause_delta
    st.session_state.pause_start_utc = None
    st.session_state.state = "running"
    if "current_user_key" in st.session_state:
        utils.update_live_activity_state(
            LIVE_ACTIVITY_DIR,
            st.session_state.current_user_key,
            state="running",
            paused_seconds=st.session_state.paused_seconds,
            pause_start_utc=None,
        )

def end_task():
    """End the current task."""
    st.session_state.ended_from_paused = st.session_state.state == "paused"
    st.session_state.state = "ended"
    st.session_state.end_utc = utils.now_utc()
    if "current_user_key" in st.session_state:
        utils.delete_live_activity(LIVE_ACTIVITY_DIR, st.session_state.current_user_key)

def format_start_datetime(dt_utc):
    """Format UTC datetime for human-readable Eastern display."""
    if not dt_utc:
        return "None"
    dt_et = utils.to_eastern(dt_utc)
    return dt_et.strftime("%m/%d/%Y %I:%M:%S %p").lower()

def get_submit_duration_seconds(default_seconds: int) -> int:
    """Duration rule: if ended while paused, count from start to submission time."""
    if st.session_state.get("ended_from_paused") and st.session_state.start_utc:
        return max(0, int((utils.now_utc() - st.session_state.start_utc).total_seconds()))
    return max(0, int(default_seconds))

def archive_task(user_login: str, full_name: str, user_key: str, task_name: str, selected_account: str) -> None:
    """Archive the currently paused task and reset the page state."""
    if st.session_state.state != "paused" or not st.session_state.start_utc:
        return
    paused_seconds = int(st.session_state.paused_seconds)
    if st.session_state.pause_start_utc:
        paused_seconds += int((utils.now_utc() - st.session_state.pause_start_utc).total_seconds())
    utils.save_archived_task(
        ARCHIVED_TASKS_DIR,
        user_key,
        user_login,
        full_name,
        task_name,
        st.session_state.selected_cadence,
        selected_account,
        st.session_state.covering_for,
        st.session_state.notes,
        st.session_state.start_utc,
        paused_seconds=paused_seconds,
        pause_start_utc=None,
    )
    utils.load_archived_tasks.clear()
    if "current_user_key" in st.session_state:
        utils.delete_live_activity(LIVE_ACTIVITY_DIR, st.session_state.current_user_key)
    reset_all()
    st.session_state.archived = True

def select_cadence(cadence: str):
    """Button callback to select a task cadence."""
    st.session_state.selected_cadence = cadence

def build_task_record(user_login: str, full_name: str, task_name: str, cadence: str,
                      account: str, covering_for: str, notes: str,
                      duration_seconds: int, partially_complete: bool) -> dict:
    """Build a record dict for a completed task entry."""
    is_covering_for = bool(covering_for and covering_for.strip())
    return {
        "TaskID": str(uuid.uuid4()),
        "UserLogin": user_login,
        "FullName": full_name or None,
        "TaskName": task_name,
        "TaskCadence": cadence,
        "CompanyGroup": account or None,
        "IsCoveringFor": is_covering_for,
        "CoveringFor": covering_for or None,
        "Notes": notes.strip() if notes and notes.strip() else None,
        "PartiallyComplete": partially_complete,
        "StartTimestampUTC": st.session_state.start_utc,
        "EndTimestampUTC": st.session_state.end_utc,
        "DurationSeconds": int(duration_seconds),
        "UploadTimestampUTC": utils.now_utc(),
        "AppVersion": config.APP_VERSION,
    }

# Confirmation dialog for task submission
@st.dialog("Submit?")
def confirm_submit(user_login, full_name, user_key, task_name, selected_account):
    """Modal confirmation dialog for submitting a completed task."""
    st.caption(f"**User:** {full_name}")
    st.caption(f"**Task:** {task_name}")
    st.caption(f"**Cadence:** {st.session_state.selected_cadence}")
    st.caption(f"**Started On:** {format_start_datetime(st.session_state.start_utc)}")
    is_covering = bool(st.session_state.covering_for and st.session_state.covering_for.strip())
    st.caption(f"**Covering For:** {'Yes - ' + st.session_state.covering_for if is_covering else 'No'}")
    st.caption(f"**Account:** {selected_account if selected_account else 'None'}")
    st.caption(f"**Notes:** {st.session_state.notes if st.session_state.notes else 'None'}")
    st.caption(f"**Partially Complete:** {'Yes' if st.session_state.get('submit_partially_complete', False) else 'No'}")
    st.divider()
    effective_duration_seconds = get_submit_duration_seconds(st.session_state.elapsed_seconds)
    current_duration = utils.format_hhmmss(effective_duration_seconds)
    edited_duration = st.text_input("Duration", value=current_duration, key="edit_duration", max_chars=8)
    parsed_duration = utils.parse_hhmmss(edited_duration)
    if parsed_duration < 0:
        parsed_duration = effective_duration_seconds
        st.warning("Invalid format - using original duration")
    st.divider()
    left, right = st.columns(2)
    with left:
        if st.button("Submit", type="primary", width="stretch"):
            if edited_duration.strip() == current_duration:
                parsed_duration = get_submit_duration_seconds(effective_duration_seconds)
            record = build_task_record(
                user_login,
                full_name,
                task_name,
                st.session_state.selected_cadence,
                selected_account,
                st.session_state.covering_for,
                st.session_state.notes,
                parsed_duration,
                st.session_state.get("submit_partially_complete", False),
            )
            df_record = pd.DataFrame([record])
            out_dir = utils.build_out_dir(COMPLETED_TASKS_DIR, user_key, st.session_state.start_utc)
            eastern_start = utils.to_eastern(st.session_state.start_utc)
            fname = f"task_{eastern_start:%Y%m%d_%H%M%S}_{record['TaskID'][:8]}.parquet"
            utils.atomic_write_parquet(df_record, out_dir / fname)
            st.session_state.confirm_open = False
            st.session_state.confirm_rendered = False
            reset_all()
            st.session_state.uploaded = True
            st.rerun()
    with right:
        if st.button("Cancel", width="stretch"):
            st.session_state.confirm_open = False
            st.session_state.confirm_rendered = False
            st.rerun()

@st.dialog("Archived Tasks")
def review_archived_tasks_dialog(user_login, full_name, user_key):
    """Review archived tasks with options to resume or delete."""
    archived_df = utils.load_archived_tasks(ARCHIVED_TASKS_DIR, user_key)
    if archived_df.empty:
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
            if st.button("Resume", key=f"resume_archive_{idx}", width="stretch"):
                archive_path_raw = str(row.get("ArchiveFilePath") or "").strip()
                if archive_path_raw:
                    archive_path = Path(archive_path_raw)
                    utils.delete_archived_task_file(archive_path)

                st.session_state.state = "paused"
                st.session_state.start_utc = start_utc
                st.session_state.end_utc = None
                st.session_state.paused_seconds = int(row.get("PausedSeconds", 0) or 0)
                st.session_state.pause_start_utc = utils.now_utc()
                st.session_state.ended_from_paused = False
                st.session_state.notes = str(row.get("Notes") or "")
                st.session_state.selected_cadence = str(row.get("TaskCadence") or "")
                st.session_state.covering_for = str(row.get("CoveringFor") or "")
                st.session_state.covering_toggle = bool(st.session_state.covering_for)
                st.session_state.restored_task_name = task_name
                st.session_state.restored_account = str(row.get("CompanyGroup") or "")
                st.session_state.restored_covering_for = st.session_state.covering_for
                st.session_state.live_activity_saved = False
                st.session_state.last_task_name = task_name
                st.session_state.review_archive_open = False
                st.session_state.review_archive_rendered = False
                utils.load_archived_tasks.clear()
                st.rerun()
        with delete_col:
            if st.button("Delete", key=f"delete_archive_{idx}", width="stretch"):
                archive_path_raw = str(row.get("ArchiveFilePath") or "").strip()
                if archive_path_raw:
                    archive_path = Path(archive_path_raw)
                    utils.delete_archived_task_file(archive_path)
                utils.load_archived_tasks.clear()
                st.session_state.review_archive_open = True
                st.session_state.review_archive_rendered = False
                st.rerun()
        st.divider()

# Live activity section (refreshes periodically to show team activity)
@st.fragment(run_every=30)
def live_activity_section():
    # Load live activities for all other users
    live_activities_df = utils.load_live_activities(LIVE_ACTIVITY_DIR, _exclude_user_key=st.session_state.get("current_user_key"))
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
logo_b64 = utils.get_logo_base64(str(LOGO_PATH))
st.markdown(
    f"""
    <div class="header-row">
        <img class="header-logo" src="data:image/png;base64,{logo_b64}" />
        <h1 class="header-title">LS - Task Tracker</h1>
    </div>
    """,
    unsafe_allow_html=True,
)
st.divider()
if st.session_state.get("uploaded"):
    st.toast("Upload Successful", icon="✅")
    st.session_state.uploaded = False
if st.session_state.get("archived"):
    st.toast("Task archived")
    st.session_state.archived = False

# Main layout columns
spacer_l, left_col, _, mid_col, _, right_col, spacer_r = st.columns([0.4, 4, 0.2, 4, 0.2, 4, 0.4])
with left_col:
    user_login = utils.get_os_user()
    full_name = utils.get_full_name_for_user(None, user_login)
    user_key = utils.sanitize_key(user_login)
    st.session_state.current_user_key = user_key
    inputs_locked = st.session_state.state != "idle"
    st.text_input("User", value=full_name, disabled=True)
    all_users = utils.load_all_user_full_names()
    # Exclude current user from covering list
    covering_options = [""] + [u for u in all_users if u != full_name]
    covering_key = f"covering_{st.session_state.reset_counter}"
    # Restore covering selection if present
    if st.session_state.restored_covering_for and covering_key not in st.session_state:
        if st.session_state.restored_covering_for in covering_options:
            st.session_state[covering_key] = st.session_state.restored_covering_for
    covering_for = st.selectbox(
        "Covering For (optional)",
        covering_options,
        disabled=inputs_locked,
        key=covering_key,
    )
    account_options = [""] + utils.load_accounts(str(PERSONNEL_DIR))
    acct_key = f"acct_{st.session_state.reset_counter}"
    if st.session_state.restored_account and acct_key not in st.session_state:
        if st.session_state.restored_account in account_options:
            st.session_state[acct_key] = st.session_state.restored_account
    selected_account = st.selectbox("Account (optional)", account_options, key=acct_key)
    st.session_state.covering_for = covering_for
    archived_count = len(utils.load_archived_tasks(ARCHIVED_TASKS_DIR, user_key))

with mid_col:
    tasks_df = utils.load_tasks()
    task_options = [""] + sorted(tasks_df["TaskName"].unique()) if not tasks_df.empty else [""]
    task_key = f"task_{st.session_state.reset_counter}"
    if st.session_state.restored_task_name and task_key not in st.session_state:
        if st.session_state.restored_task_name in task_options:
            st.session_state[task_key] = st.session_state.restored_task_name
    task_name = st.selectbox("Task", task_options, disabled=inputs_locked, key=task_key)
    CADENCE_ORDER = ["Daily", "Weekly", "Periodic"]
    available_cadences = []
    if task_name:
        available_cadences = tasks_df.loc[tasks_df["TaskName"] == task_name, "TaskCadence"].dropna().unique().tolist()
    # Auto-select cadence if needed
    if task_name and st.session_state.state == "idle":
        if task_name != st.session_state.last_task_name:
            st.session_state.selected_cadence = None
            st.session_state.last_task_name = task_name
        if st.session_state.selected_cadence not in available_cadences:
            st.session_state.selected_cadence = next((c for c in CADENCE_ORDER if c in available_cadences), None)
    st.caption("Cadence")
    cad_cols = st.columns(3)
    for col, cadence in zip(cad_cols, CADENCE_ORDER):
        is_selected = st.session_state.selected_cadence == cadence
        disabled = (not task_name) or (cadence not in available_cadences) or (inputs_locked and not is_selected)
        with col:
            st.button(cadence, disabled=disabled, type="primary" if is_selected else "secondary",
                      key=f"cad_{cadence}_{st.session_state.reset_counter}", on_click=select_cadence, args=(cadence,), width="stretch")
    st.text_area("Notes (optional)", key="notes", height=120)

with right_col:
    st.session_state.elapsed_seconds = compute_elapsed_seconds()
    hh, mm = utils.format_hh_mm_parts(st.session_state.elapsed_seconds)
    colon_class = "blink-colon" if st.session_state.state == "running" else ""
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
    if st.session_state.state == "idle":
        c1, c2 = st.columns(2)
        can_start = bool(task_name and st.session_state.selected_cadence)
        with c1:
            st.button("Start", width="stretch", disabled=not can_start,
                      help=None if can_start else "Select a task to start time",
                      on_click=start_task if can_start else None)
        with c2:
            st.button("End", width="stretch", disabled=True)
    elif st.session_state.state == "running":
        c1, c2 = st.columns(2)
        with c1:
            st.button("Pause", width="stretch", on_click=pause_task)
        with c2:
            st.button("End", width="stretch", on_click=end_task)
    elif st.session_state.state == "paused":
        c1, c2 = st.columns(2)
        with c1:
            st.button("Resume", width="stretch", on_click=resume_task)
        with c2:
            st.button("End", width="stretch", on_click=end_task)
    if st.session_state.state == "paused":
        st.button(
            "Archive",
            width="stretch",
            on_click=archive_task,
            args=(user_login, full_name, user_key, task_name, selected_account),
        )
    if archived_count > 0:
        if st.button(
            f"You have {archived_count} archived tasks, click here to review",
            key="review_archived_link",
            type="tertiary",
        ):
            st.session_state.review_archive_open = True
            st.session_state.review_archive_rendered = False
            st.rerun()
    if st.session_state.state == "ended":
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Upload", type="primary", width="stretch"):
                st.session_state.submit_partially_complete = st.session_state.get("partially_complete", False)
                st.session_state.confirm_open = True
                st.session_state.confirm_rendered = False
                st.rerun()
        with c2:
            st.button("Reset", width="stretch", on_click=reset_all)
    if st.session_state.state != "idle":
        pc_left, pc_right = st.columns([1.4, 3])
        with pc_left:
            st.markdown("<div style='padding-top: 8px;'>Partially complete?</div>", unsafe_allow_html=True)
        with pc_right:
            st.toggle("Partially complete", key="partially_complete", label_visibility="collapsed")

# Save live activity after input changes (to broadcast running task to others)
if st.session_state.state in ("running", "paused") and not st.session_state.live_activity_saved and task_name and st.session_state.selected_cadence:
    utils.save_live_activity(
        LIVE_ACTIVITY_DIR, user_key, user_login, full_name,
        task_name, st.session_state.selected_cadence, selected_account,
        st.session_state.covering_for, st.session_state.notes,
        st.session_state.start_utc,
        state=st.session_state.state,
        paused_seconds=st.session_state.paused_seconds,
        pause_start_utc=st.session_state.pause_start_utc,
    )
    st.session_state.live_activity_saved = True
    st.session_state.live_task_name = task_name
    st.session_state.live_cadence = st.session_state.selected_cadence
    st.session_state.live_account = selected_account

# Open confirmation modal if triggered
if st.session_state.confirm_open and not st.session_state.get("confirm_rendered"):
    st.session_state.confirm_rendered = True
    confirm_submit(user_login, full_name, user_key, task_name, selected_account)
if st.session_state.review_archive_open and not st.session_state.get("review_archive_rendered"):
    st.session_state.review_archive_rendered = True
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
    show_all_users = st.toggle("Show all users?", value=True, key="show_all_users", label_visibility="collapsed")

# Load and display today's completed tasks
if show_all_users:
    recent_df = utils.load_recent_tasks(COMPLETED_TASKS_DIR, user_key=None, limit=50)
else:
    recent_df = utils.load_recent_tasks(COMPLETED_TASKS_DIR, user_key=user_key, limit=50)
if not recent_df.empty:
    recent_df["Duration"] = recent_df["DurationSeconds"].apply(utils.format_hhmmss)
    recent_df["Uploaded"] = pd.to_datetime(recent_df["EndTimestampUTC"], utc=True).apply(lambda x: utils.format_time_ago(x))
    if "PartiallyComplete" not in recent_df.columns:
        recent_df["PartiallyComplete"] = pd.Series([pd.NA] * len(recent_df), dtype="boolean")
    else:
        recent_df["PartiallyComplete"] = recent_df["PartiallyComplete"].astype("boolean")
    recent_df["Partially Completed?"] = recent_df["PartiallyComplete"].fillna(False).astype(bool)
    if "Notes" not in recent_df.columns:
        recent_df["Notes"] = ""
    recent_df["Notes"] = recent_df["Notes"].fillna("")
    if "FullName" not in recent_df.columns:
        recent_df["FullName"] = ""
    recent_df["DisplayUser"] = recent_df["FullName"].fillna("").astype(str).str.strip()
    mask_blank = recent_df["DisplayUser"].eq("")
    recent_df.loc[mask_blank, "DisplayUser"] = recent_df.loc[mask_blank, "UserLogin"].fillna("").astype(str)
    display_df = recent_df.rename(columns={"TaskName": "Task", "DisplayUser": "User"})[["User", "Task", "Partially Completed?", "Uploaded", "Duration", "Notes"]]
    st.dataframe(
        display_df,
        hide_index=True,
        width="stretch",
        column_config={
            "Partially Completed?": st.column_config.CheckboxColumn("Partially Completed?", disabled=True, width= 30),
            "Notes": st.column_config.TextColumn("Notes", width="large"),
            "Uploaded": st.column_config.TextColumn("Uploaded", width=1),
        },
    )
else:
    st.info("No tasks completed today.")

# Footer with app version
st.caption(f"\n\n\nApp version: {config.APP_VERSION}", text_alignment="center")
if st.session_state.state == "running":
    st_autorefresh(interval=10000, key="timer")
