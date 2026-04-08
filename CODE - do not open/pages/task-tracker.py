"""
pages/task-tracker.py

Combined Task Tracker for Logistics Support and Data & Analytics.
Toggle between versions using the buttons at the top of the page.
Defaults to Logistics Support on first load.
"""

import streamlit as st
import pandas as pd
import pyarrow as pa
import uuid
from datetime import date, timedelta
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

LOGGER = utils.get_page_logger("Task Tracker")
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
    "Logistics Support",
    "Project Fulfillment",
    "FS&D",
    "Account Support",
    "Accounts Receivable",
    "Partnership Development",
    "Marketing",
    "Employee Optimization",
    "Procurement",
    "Misc.",
]

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon=utils.get_app_icon())
utils.render_app_logo()
utils.log_page_open_once("task_tracker_page", LOGGER)
utils.log_page_open_once("da_task_tracker_page", LOGGER)
if "_task_tracker_render_logged" not in st.session_state:
    st.session_state._task_tracker_render_logged = True
    LOGGER.info("Render UI.")
if _AUTOREFRESH_IMPORT_ERROR is not None and "_task_tracker_autorefresh_warned" not in st.session_state:
    st.session_state._task_tracker_autorefresh_warned = True
    LOGGER.warning(
        "Auto-refresh component unavailable. Timer will update on user interaction. error=%s",
        _AUTOREFRESH_IMPORT_ERROR,
    )

st.markdown(utils.get_global_css(), unsafe_allow_html=True)

# ============================================================
# DIRECTORY CONSTANTS
# ============================================================
# Logistics Support
LS_COMPLETED_TASKS_DIR = config.COMPLETED_TASKS_DIR
LS_LIVE_ACTIVITY_DIR = config.LIVE_ACTIVITY_DIR
LS_ARCHIVED_TASKS_DIR = config.ARCHIVED_TASKS_DIR
PERSONNEL_DIR = config.PERSONNEL_DIR

# Data & Analytics
DA_COMPLETED_TASKS_DIR = config.DA_COMPLETED_TASKS_DIR
DA_LIVE_ACTIVITY_DIR = config.DA_LIVE_ACTIVITY_DIR
DA_ARCHIVED_TASKS_DIR = config.DA_ARCHIVED_TASKS_DIR

# ============================================================
# SPRINT HELPERS (D&A)
# ============================================================
_SPRINT_ANCHOR_DATE = date(2026, 4, 6)
_SPRINT_ANCHOR_NUMBER = 92
_SPRINT_DAYS = 14


def _sprint_number_for_date(d: date) -> int:
    return _SPRINT_ANCHOR_NUMBER + (d - _SPRINT_ANCHOR_DATE).days // _SPRINT_DAYS


def _sprint_dates(n: int) -> tuple[date, date]:
    n = int(n)
    start = _SPRINT_ANCHOR_DATE + timedelta(days=(n - _SPRINT_ANCHOR_NUMBER) * _SPRINT_DAYS)
    return start, start + timedelta(days=_SPRINT_DAYS - 1)


def _sprint_label(n: int) -> str:
    s, e = _sprint_dates(n)
    return f"Sprint {n}  ({s.strftime('%m/%d')} – {e.strftime('%m/%d/%Y')})"


# ============================================================
# STATE INITIALIZATION — LOGISTICS SUPPORT
# ============================================================
LS_DEFAULT_STATE = {
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
    "active_task_name": "",
}
for k, v in LS_DEFAULT_STATE.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ============================================================
# STATE INITIALIZATION — DATA & ANALYTICS
# ============================================================
DA_DEFAULT_STATE = {
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
for k, v in DA_DEFAULT_STATE.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ============================================================
# STATE RESTORATION (both versions, always on page load)
# ============================================================
_user_key_for_restore = utils.sanitize_key(utils.get_os_user())

if not st.session_state.state_restored:
    st.session_state.state_restored = True
    restored = utils.load_own_live_activity(LS_LIVE_ACTIVITY_DIR, _user_key_for_restore)
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
        LOGGER.info(
            "Restored LS session | task='%s' cadence='%s' state='%s'",
            restored["task_name"],
            restored["cadence"],
            restored["state"],
        )

if not st.session_state.da_state_restored:
    st.session_state.da_state_restored = True
    da_restored = utils.load_own_live_activity(DA_LIVE_ACTIVITY_DIR, _user_key_for_restore)
    if da_restored:
        st.session_state.da_state = da_restored["state"]
        st.session_state.da_start_utc = da_restored["start_utc"]
        st.session_state.da_paused_seconds = da_restored["paused_seconds"]
        st.session_state.da_pause_start_utc = da_restored["pause_start_utc"]
        st.session_state.da_notes = da_restored["notes"]
        st.session_state.da_primary_stakeholder = da_restored.get("covering_for", "")
        st.session_state.da_live_activity_saved = True
        st.session_state.da_last_task_name = da_restored["task_name"]
        st.session_state.da_restored_task_name = da_restored["task_name"]
        st.session_state.da_restored_account = da_restored["account"]
        st.session_state.da_restored_primary_stakeholder = da_restored.get("covering_for", "")
        LOGGER.info(
            "Restored DA session | task='%s' state='%s'",
            da_restored["task_name"],
            da_restored["state"],
        )

# ============================================================
# SHARED HELPERS
# ============================================================
def format_start_datetime(dt_utc):
    if not dt_utc:
        return "None"
    dt_et = utils.to_eastern(dt_utc)
    return dt_et.strftime("%m/%d/%Y %I:%M:%S %p").lower()

def get_submit_duration_seconds(default_seconds: int) -> int:
    return max(0, int(default_seconds))

# ============================================================
# LOGISTICS SUPPORT — BUSINESS LOGIC
# ============================================================
def compute_elapsed_seconds() -> int:
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
    if "current_user_key" in st.session_state:
        utils.delete_live_activity(LS_LIVE_ACTIVITY_DIR, st.session_state.current_user_key)
    old_counter = st.session_state.reset_counter
    st.session_state.reset_counter += 1
    for key in [f"task_{old_counter}", f"acct_{old_counter}", f"covering_{old_counter}"]:
        st.session_state.pop(key, None)
    for k, v in LS_DEFAULT_STATE.items():
        if k != "state_restored":
            st.session_state[k] = v

def start_task():
    selected_task = str(st.session_state.get(f"task_{st.session_state.reset_counter}", "") or "").strip()
    LOGGER.info("Started LS task timer | task='%s' cadence='%s'", selected_task, st.session_state.selected_cadence)
    st.session_state.active_task_name = selected_task
    st.session_state.state = "running"
    st.session_state.start_utc = utils.now_utc()
    st.session_state.end_utc = None
    st.session_state.paused_seconds = 0
    st.session_state.pause_start_utc = None
    st.session_state.elapsed_seconds = 0
    st.session_state.ended_from_paused = False
    st.session_state.live_activity_saved = False

def pause_task():
    if st.session_state.state != "running":
        return
    LOGGER.info("Paused LS task timer.")
    st.session_state.state = "paused"
    if not st.session_state.pause_start_utc:
        st.session_state.pause_start_utc = utils.now_utc()
    if "current_user_key" in st.session_state:
        utils.update_live_activity_state(
            LS_LIVE_ACTIVITY_DIR, st.session_state.current_user_key,
            state="paused", paused_seconds=st.session_state.paused_seconds,
            pause_start_utc=st.session_state.pause_start_utc,
        )

def resume_task():
    if st.session_state.state != "paused":
        return
    LOGGER.info("Resumed LS task timer.")
    if st.session_state.pause_start_utc:
        pause_delta = int((utils.now_utc() - st.session_state.pause_start_utc).total_seconds())
        if pause_delta > 0:
            st.session_state.paused_seconds += pause_delta
    st.session_state.pause_start_utc = None
    st.session_state.state = "running"
    if "current_user_key" in st.session_state:
        utils.update_live_activity_state(
            LS_LIVE_ACTIVITY_DIR, st.session_state.current_user_key,
            state="running", paused_seconds=st.session_state.paused_seconds, pause_start_utc=None,
        )

def end_task():
    LOGGER.info("Ended LS task timer.")
    if st.session_state.pause_start_utc:
        pause_delta = int((utils.now_utc() - st.session_state.pause_start_utc).total_seconds())
        if pause_delta > 0:
            st.session_state.paused_seconds += pause_delta
    st.session_state.pause_start_utc = None
    st.session_state.ended_from_paused = False
    st.session_state.state = "ended"
    st.session_state.end_utc = utils.now_utc()
    if "current_user_key" in st.session_state:
        utils.delete_live_activity(LS_LIVE_ACTIVITY_DIR, st.session_state.current_user_key)

def archive_task(user_login, full_name, user_key, task_name, selected_account):
    if st.session_state.state != "paused" or not st.session_state.start_utc:
        return
    paused_seconds = int(st.session_state.paused_seconds)
    if st.session_state.pause_start_utc:
        paused_seconds += int((utils.now_utc() - st.session_state.pause_start_utc).total_seconds())
    utils.save_archived_task(
        LS_ARCHIVED_TASKS_DIR, user_key, user_login, full_name, task_name,
        st.session_state.selected_cadence, selected_account, st.session_state.covering_for,
        st.session_state.notes, st.session_state.start_utc,
        paused_seconds=paused_seconds, pause_start_utc=None,
    )
    utils.load_archived_tasks.clear()
    if "current_user_key" in st.session_state:
        utils.delete_live_activity(LS_LIVE_ACTIVITY_DIR, st.session_state.current_user_key)
    reset_all()
    LOGGER.info("Archived LS paused task | task='%s' user='%s'", task_name, user_login)
    st.session_state.archived = True

def build_task_record(user_login, full_name, task_name, cadence, account, covering_for,
                      notes, duration_seconds, partially_complete):
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

def get_effective_task_name(current_task_name):
    candidates = [
        current_task_name,
        st.session_state.get("active_task_name", ""),
        st.session_state.get("live_task_name", ""),
        st.session_state.get("last_task_name", ""),
        st.session_state.get("restored_task_name", ""),
    ]
    for candidate in candidates:
        task_value = str(candidate or "").strip()
        if task_value:
            return task_value
    return ""

# ============================================================
# DATA & ANALYTICS — BUSINESS LOGIC
# ============================================================
def da_compute_elapsed_seconds() -> int:
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

def da_reset_all():
    if "da_current_user_key" in st.session_state:
        utils.delete_live_activity(DA_LIVE_ACTIVITY_DIR, st.session_state.da_current_user_key)
    old_counter = st.session_state.da_reset_counter
    st.session_state.da_reset_counter += 1
    for key in [f"da_task_{old_counter}", f"da_acct_{old_counter}", f"da_dept_{old_counter}"]:
        st.session_state.pop(key, None)
    for k, v in DA_DEFAULT_STATE.items():
        if k != "da_state_restored":
            st.session_state[k] = v

def da_start_task():
    selected_task = str(st.session_state.get(f"da_task_{st.session_state.da_reset_counter}", "") or "").strip()
    LOGGER.info("Started DA task timer | task='%s'", selected_task)
    st.session_state.da_active_task_name = selected_task
    st.session_state.da_state = "running"
    st.session_state.da_start_utc = utils.now_utc()
    st.session_state.da_end_utc = None
    st.session_state.da_paused_seconds = 0
    st.session_state.da_pause_start_utc = None
    st.session_state.da_elapsed_seconds = 0
    st.session_state.da_ended_from_paused = False
    st.session_state.da_live_activity_saved = False

def da_pause_task():
    if st.session_state.da_state != "running":
        return
    LOGGER.info("Paused DA task timer.")
    st.session_state.da_state = "paused"
    if not st.session_state.da_pause_start_utc:
        st.session_state.da_pause_start_utc = utils.now_utc()
    if "da_current_user_key" in st.session_state:
        utils.update_live_activity_state(
            DA_LIVE_ACTIVITY_DIR, st.session_state.da_current_user_key,
            state="paused", paused_seconds=st.session_state.da_paused_seconds,
            pause_start_utc=st.session_state.da_pause_start_utc,
        )

def da_resume_task():
    if st.session_state.da_state != "paused":
        return
    LOGGER.info("Resumed DA task timer.")
    if st.session_state.da_pause_start_utc:
        pause_delta = int((utils.now_utc() - st.session_state.da_pause_start_utc).total_seconds())
        if pause_delta > 0:
            st.session_state.da_paused_seconds += pause_delta
    st.session_state.da_pause_start_utc = None
    st.session_state.da_state = "running"
    if "da_current_user_key" in st.session_state:
        utils.update_live_activity_state(
            DA_LIVE_ACTIVITY_DIR, st.session_state.da_current_user_key,
            state="running", paused_seconds=st.session_state.da_paused_seconds, pause_start_utc=None,
        )

def da_end_task():
    LOGGER.info("Ended DA task timer.")
    if st.session_state.da_pause_start_utc:
        pause_delta = int((utils.now_utc() - st.session_state.da_pause_start_utc).total_seconds())
        if pause_delta > 0:
            st.session_state.da_paused_seconds += pause_delta
    st.session_state.da_pause_start_utc = None
    st.session_state.da_ended_from_paused = False
    st.session_state.da_state = "ended"
    st.session_state.da_end_utc = utils.now_utc()
    if "da_current_user_key" in st.session_state:
        utils.delete_live_activity(DA_LIVE_ACTIVITY_DIR, st.session_state.da_current_user_key)

def da_archive_task(user_login, full_name, user_key, task_name, selected_account):
    if st.session_state.da_state != "paused" or not st.session_state.da_start_utc:
        return
    paused_seconds = int(st.session_state.da_paused_seconds)
    if st.session_state.da_pause_start_utc:
        paused_seconds += int((utils.now_utc() - st.session_state.da_pause_start_utc).total_seconds())
    utils.save_archived_task(
        DA_ARCHIVED_TASKS_DIR, user_key, user_login, full_name, task_name,
        None, selected_account, st.session_state.da_primary_stakeholder,
        st.session_state.da_notes, st.session_state.da_start_utc,
        paused_seconds=paused_seconds, pause_start_utc=None,
    )
    utils.load_archived_tasks.clear()
    if "da_current_user_key" in st.session_state:
        utils.delete_live_activity(DA_LIVE_ACTIVITY_DIR, st.session_state.da_current_user_key)
    da_reset_all()
    LOGGER.info("Archived DA paused task | task='%s' user='%s'", task_name, user_login)
    st.session_state.da_archived = True

def da_build_task_record(user_login, full_name, task_name, account, primary_stakeholder,
                         department, notes, duration_seconds, partially_complete):
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

def da_get_effective_task_name(current_task_name):
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

# ============================================================
# DA HELPERS
# ============================================================
def load_own_tasks_with_paths(completed_dir: Path, user_key_val: str) -> pd.DataFrame:
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

# ============================================================
# DIALOGS — LOGISTICS SUPPORT
# ============================================================
@st.dialog("Submit?")
def confirm_submit(user_login, full_name, user_key, task_name, selected_account):
    task_name = get_effective_task_name(task_name)
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
                user_login, full_name, task_name, st.session_state.selected_cadence,
                selected_account, st.session_state.covering_for, st.session_state.notes,
                parsed_duration, st.session_state.get("submit_partially_complete", False),
            )
            df_record = pd.DataFrame([record])
            out_dir = utils.build_out_dir(LS_COMPLETED_TASKS_DIR, user_key, st.session_state.start_utc)
            eastern_start = utils.to_eastern(st.session_state.start_utc)
            fname = f"task_{eastern_start:%Y%m%d_%H%M%S}_{record['TaskID'][:8]}.parquet"
            utils.atomic_write_parquet(df_record, out_dir / fname)
            utils.load_all_completed_tasks.clear()
            utils.load_completed_tasks_for_analytics.clear()
            try:
                utils.sync_tasks_parquet_targets()
            except Exception as exc:
                LOGGER.warning("Completed task uploaded but target sync failed: %s", exc)
            LOGGER.info(
                "Uploaded LS completed task | task='%s' cadence='%s' duration_seconds=%s partially_complete=%s",
                task_name, st.session_state.selected_cadence, parsed_duration,
                st.session_state.get("submit_partially_complete", False),
            )
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
    archived_df = utils.load_archived_tasks(LS_ARCHIVED_TASKS_DIR, user_key)
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
            if st.button("Resume", key=f"resume_archive_{idx}", width="stretch"):
                archive_path_raw = str(row.get("ArchiveFilePath") or "").strip()
                if archive_path_raw:
                    utils.delete_archived_task_file(Path(archive_path_raw))
                st.session_state.state = "paused"
                st.session_state.start_utc = start_utc
                st.session_state.end_utc = None
                st.session_state.paused_seconds = int(row.get("PausedSeconds", 0) or 0)
                st.session_state.pause_start_utc = utils.now_utc()
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
                LOGGER.info("Resumed LS archived task | task='%s' user='%s'", task_name, user_login)
                st.rerun()
        with delete_col:
            if st.button("Delete", key=f"delete_archive_{idx}", width="stretch"):
                archive_path_raw = str(row.get("ArchiveFilePath") or "").strip()
                if archive_path_raw:
                    utils.delete_archived_task_file(Path(archive_path_raw))
                utils.load_archived_tasks.clear()
                st.session_state.review_archive_open = True
                st.session_state.review_archive_rendered = False
                LOGGER.info("Deleted LS archived task | task='%s' user='%s'", task_name, user_login)
                st.rerun()
        st.divider()

# ============================================================
# DIALOGS — DATA & ANALYTICS
# ============================================================
@st.dialog("Submit?")
def da_confirm_submit(user_login, full_name, user_key, task_name, selected_account):
    task_name = da_get_effective_task_name(task_name)
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
            record = da_build_task_record(
                user_login, full_name, task_name, selected_account,
                st.session_state.da_primary_stakeholder, st.session_state.da_department,
                st.session_state.da_notes, parsed_duration,
                st.session_state.get("da_submit_partially_complete", False),
            )
            df_record = pd.DataFrame([record])
            out_dir = utils.build_out_dir(DA_COMPLETED_TASKS_DIR, user_key, st.session_state.da_start_utc)
            eastern_start = utils.to_eastern(st.session_state.da_start_utc)
            fname = f"task_{eastern_start:%Y%m%d_%H%M%S}_{record['TaskID'][:8]}.parquet"
            utils.atomic_write_parquet(df_record, out_dir / fname, schema=DA_PARQUET_SCHEMA)
            utils.load_recent_tasks.clear()
            LOGGER.info(
                "Uploaded DA completed task | task='%s' department='%s' duration_seconds=%s partially_complete=%s",
                task_name, st.session_state.da_department, parsed_duration,
                st.session_state.get("da_submit_partially_complete", False),
            )
            st.session_state.da_confirm_open = False
            st.session_state.da_confirm_rendered = False
            da_reset_all()
            st.session_state.da_uploaded = True
            st.rerun()
    with right:
        if st.button("Cancel", width="stretch"):
            st.session_state.da_confirm_open = False
            st.session_state.da_confirm_rendered = False
            st.rerun()

@st.dialog("Archived Tasks")
def da_review_archived_tasks_dialog(user_login, full_name, user_key):
    archived_df = utils.load_archived_tasks(DA_ARCHIVED_TASKS_DIR, user_key)
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
                    utils.delete_archived_task_file(Path(archive_path_raw))
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
                LOGGER.info("Resumed DA archived task | task='%s' user='%s'", task_name, user_login)
                st.rerun()
        with delete_col:
            if st.button("Delete", key=f"da_delete_archive_{idx}", width="stretch"):
                archive_path_raw = str(row.get("ArchiveFilePath") or "").strip()
                if archive_path_raw:
                    utils.delete_archived_task_file(Path(archive_path_raw))
                utils.load_archived_tasks.clear()
                st.session_state.da_review_archive_open = True
                st.session_state.da_review_archive_rendered = False
                LOGGER.info("Deleted DA archived task | task='%s' user='%s'", task_name, user_login)
                st.rerun()
        st.divider()

# ============================================================
# FRAGMENTS — LIVE ACTIVITY
# ============================================================
@st.fragment(run_every=30)
def live_activity_section():
    live_activities_df = utils.load_live_activities(LS_LIVE_ACTIVITY_DIR, _exclude_user_key=st.session_state.get("current_user_key"))
    if not live_activities_df.empty:
        st.divider()
        st.markdown(
            '<h3 style="margin-bottom: 0;"><span class="live-activity-pulse"></span> Live Activity</h3>',
            unsafe_allow_html=True,
        )
        st.caption("Tasks currently in progress by other team members")
        live_display_df = live_activities_df[["StartTimestampUTC", "FullName", "UserLogin", "TaskName", "Notes"]].copy()
        start_utc = pd.to_datetime(live_display_df["StartTimestampUTC"], utc=True)
        live_display_df["Start Time"] = (
            start_utc.dt.tz_convert(utils.EASTERN_TZ).dt.strftime("%#I:%M %p").str.lower()
            + " - "
            + start_utc.apply(lambda x: utils.format_time_ago(x))
        )
        live_display_df["Notes"] = live_display_df["Notes"].fillna("")
        live_display_df["User"] = live_display_df["FullName"].fillna("").astype(str).str.strip()
        mask_blank = live_display_df["User"].eq("")
        live_display_df.loc[mask_blank, "User"] = live_display_df.loc[mask_blank, "UserLogin"].fillna("").astype(str)
        st.dataframe(
            pd.DataFrame({"User": live_display_df["User"], "Task": live_display_df["TaskName"],
                          "Start Time": live_display_df["Start Time"], "Notes": live_display_df["Notes"]}),
            hide_index=True, width="stretch",
        )

@st.fragment(run_every=30)
def da_live_activity_section():
    live_activities_df = utils.load_live_activities(DA_LIVE_ACTIVITY_DIR, _exclude_user_key=st.session_state.get("da_current_user_key"))
    if not live_activities_df.empty:
        st.divider()
        st.markdown(
            '<h3 style="margin-bottom: 0;"><span class="live-activity-pulse"></span> Live Activity</h3>',
            unsafe_allow_html=True,
        )
        st.caption("Tasks currently in progress by other team members")
        live_display_df = live_activities_df[["StartTimestampUTC", "FullName", "UserLogin", "TaskName", "Notes"]].copy()
        start_utc = pd.to_datetime(live_display_df["StartTimestampUTC"], utc=True)
        live_display_df["Start Time"] = (
            start_utc.dt.tz_convert(utils.EASTERN_TZ).dt.strftime("%#I:%M %p").str.lower()
            + " - "
            + start_utc.apply(lambda x: utils.format_time_ago(x))
        )
        live_display_df["Notes"] = live_display_df["Notes"].fillna("")
        live_display_df["User"] = live_display_df["FullName"].fillna("").astype(str).str.strip()
        mask_blank = live_display_df["User"].eq("")
        live_display_df.loc[mask_blank, "User"] = live_display_df.loc[mask_blank, "UserLogin"].fillna("").astype(str)
        st.dataframe(
            pd.DataFrame({"User": live_display_df["User"], "Task": live_display_df["TaskName"],
                          "Start Time": live_display_df["Start Time"], "Notes": live_display_df["Notes"]}),
            hide_index=True, width="stretch",
        )

# ============================================================
# VERSION TOGGLE
# ============================================================
# Determine version from which page is active (da-task-tracker.py sets the flag)
_is_da_page = st.session_state.pop("_da_page_active", False)
st.session_state.task_tracker_version = "da" if _is_da_page else "logistics"

utils.render_page_header(PAGE_TITLE)

_v_ls, _v_da, _ = st.columns([1.3, 1.3, 6])
with _v_ls:
    if st.button(
        "Logistics Support",
        use_container_width=True,
        type="primary" if st.session_state.task_tracker_version == "logistics" else "secondary",
        key="tracker_v_ls",
    ):
        st.switch_page("pages/task-tracker.py")
with _v_da:
    if st.button(
        "Data & Analytics",
        use_container_width=True,
        type="primary" if st.session_state.task_tracker_version == "da" else "secondary",
        key="tracker_v_da",
    ):
        st.switch_page("pages/da-task-tracker.py")

# ============================================================
# LOGISTICS SUPPORT — UI
# ============================================================
if st.session_state.task_tracker_version == "logistics":
    if st.session_state.get("uploaded"):
        LOGGER.info("Showing LS upload success toast.")
        st.toast("Upload Successful", icon="✅")
        st.session_state.uploaded = False
    if st.session_state.get("archived"):
        LOGGER.info("Showing LS archived-task toast.")
        st.toast("Task archived")
        st.session_state.archived = False

    spacer_l, left_col, _, mid_col, _, right_col, spacer_r = st.columns([0.4, 4, 0.2, 4, 0.2, 4, 0.4])
    with left_col:
        user_login = utils.get_os_user()
        full_name = utils.get_full_name_for_user(None, user_login)
        user_key = utils.sanitize_key(user_login)
        st.session_state.current_user_key = user_key
        inputs_locked = st.session_state.state != "idle"
        st.text_input("User", value=full_name, disabled=True)
        all_users = utils.load_all_user_full_names(department="Logistics - Support")
        covering_options = [""] + [u for u in all_users if u != full_name]
        covering_key = f"covering_{st.session_state.reset_counter}"
        if st.session_state.restored_covering_for and covering_key not in st.session_state:
            if st.session_state.restored_covering_for in covering_options:
                st.session_state[covering_key] = st.session_state.restored_covering_for
        covering_for = st.selectbox("Covering For (optional)", covering_options, disabled=inputs_locked, key=covering_key)
        account_options = [""] + utils.load_accounts(str(PERSONNEL_DIR))
        acct_key = f"acct_{st.session_state.reset_counter}"
        if st.session_state.restored_account and acct_key not in st.session_state:
            if st.session_state.restored_account in account_options:
                st.session_state[acct_key] = st.session_state.restored_account
        selected_account = st.selectbox("Account (optional)", account_options, key=acct_key)
        st.session_state.covering_for = covering_for
        archived_count = len(utils.load_archived_tasks(LS_ARCHIVED_TASKS_DIR, user_key))
        data_signature = (len([u for u in covering_options if u]), len([a for a in account_options if a]), archived_count)
        if st.session_state.get("_task_tracker_data_signature") != data_signature:
            st.session_state._task_tracker_data_signature = data_signature
            LOGGER.info("Data snapshot | covering_users=%s accounts=%s archived_tasks=%s", *data_signature)

    with mid_col:
        tasks_df = utils.load_tasks()
        task_options = [""] + sorted(tasks_df["TaskName"].unique()) if not tasks_df.empty else [""]
        task_key = f"task_{st.session_state.reset_counter}"
        if st.session_state.restored_task_name and task_key not in st.session_state:
            if st.session_state.restored_task_name in task_options:
                st.session_state[task_key] = st.session_state.restored_task_name
        task_name = st.selectbox("Task", task_options, disabled=inputs_locked, key=task_key)
        effective_task_name = get_effective_task_name(task_name)
        CADENCE_ORDER = ["Daily", "Weekly", "Periodic"]
        available_cadences = []
        if task_name:
            available_cadences = tasks_df.loc[tasks_df["TaskName"] == task_name, "TaskCadence"].dropna().unique().tolist()
        ordered_available_cadences = [c for c in CADENCE_ORDER if c in available_cadences]
        if task_name and st.session_state.state == "idle":
            if task_name != st.session_state.last_task_name:
                st.session_state.selected_cadence = None
                st.session_state.last_task_name = task_name
            if st.session_state.selected_cadence not in ordered_available_cadences:
                st.session_state.selected_cadence = next((c for c in CADENCE_ORDER if c in ordered_available_cadences), None)
        cadence_pills_key = f"cad_pills_{st.session_state.reset_counter}_{utils.sanitize_key(task_name) if task_name else 'none'}"
        if not task_name:
            cadence_options = CADENCE_ORDER
            cadence_default = None
            cadence_disabled = True
        elif ordered_available_cadences:
            cadence_options = ordered_available_cadences
            cadence_default = (
                st.session_state.selected_cadence
                if st.session_state.selected_cadence in ordered_available_cadences
                else ordered_available_cadences[0]
            )
            cadence_disabled = inputs_locked
        else:
            cadence_options = CADENCE_ORDER
            cadence_default = None
            cadence_disabled = True
        cadence_choice = st.pills(
            "Cadence", cadence_options, selection_mode="single",
            default=cadence_default, key=cadence_pills_key, disabled=cadence_disabled, width="stretch",
        )
        if cadence_choice in ordered_available_cadences:
            st.session_state.selected_cadence = cadence_choice
        st.text_area("Notes (optional)", key="notes", height=120)

    with right_col:
        st.session_state.elapsed_seconds = compute_elapsed_seconds()
        hh, mm = utils.format_hh_mm_parts(st.session_state.elapsed_seconds)
        colon_class = "blink-colon" if st.session_state.state == "running" else ""
        st.markdown(
            f'<div style="text-align:center;margin-bottom:20px;">'
            f'<div style="font-size:36px;font-weight:600;">{hh}<span class="{colon_class}">:</span>{mm}</div>'
            f'<div style="font-size:15px;color:#6b6b6b;">Elapsed Time</div></div>',
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
            st.button("Archive", width="stretch", on_click=archive_task,
                      args=(user_login, full_name, user_key, effective_task_name, selected_account))
        if archived_count > 0:
            if st.button(f"You have {archived_count} archived tasks, click here to review",
                         key="review_archived_link", type="tertiary"):
                LOGGER.info("User opened archived tasks review with %s archived item(s).", archived_count)
                st.session_state.review_archive_open = True
                st.session_state.review_archive_rendered = False
                st.rerun()
        if st.session_state.state == "ended":
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Upload", type="primary", width="stretch"):
                    if not effective_task_name:
                        LOGGER.warning("Upload blocked because effective task name is blank.")
                        st.error("Task name is missing. Please reset and select a task again.")
                    else:
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

    if st.session_state.state in ("running", "paused") and not st.session_state.live_activity_saved and effective_task_name and st.session_state.selected_cadence:
        utils.save_live_activity(
            LS_LIVE_ACTIVITY_DIR, user_key, user_login, full_name,
            effective_task_name, st.session_state.selected_cadence, selected_account,
            st.session_state.covering_for, st.session_state.notes, st.session_state.start_utc,
            state=st.session_state.state, paused_seconds=st.session_state.paused_seconds,
            pause_start_utc=st.session_state.pause_start_utc,
        )
        st.session_state.live_activity_saved = True
        st.session_state.live_task_name = effective_task_name
        st.session_state.live_cadence = st.session_state.selected_cadence
        st.session_state.live_account = selected_account
        LOGGER.info("LS live activity saved | task='%s' cadence='%s' state='%s'",
                    effective_task_name, st.session_state.selected_cadence, st.session_state.state)

    if st.session_state.confirm_open and not st.session_state.get("confirm_rendered"):
        st.session_state.confirm_rendered = True
        confirm_submit(user_login, full_name, user_key, effective_task_name, selected_account)
    if st.session_state.review_archive_open and not st.session_state.get("review_archive_rendered"):
        st.session_state.review_archive_rendered = True
        review_archived_tasks_dialog(user_login, full_name, user_key)

    live_activity_section()

    st.divider()
    title_col, text_col, toggle_col = st.columns([6, 5, 0.5], vertical_alignment="center")
    with title_col:
        st.subheader("Today's Activity", anchor=False)
    with text_col:
        st.markdown("Show all users?", text_alignment="right")
    with toggle_col:
        show_all_users = st.toggle("Show all users?", value=True, key="show_all_users", label_visibility="collapsed")
    _last_show_all = st.session_state.get("_last_show_all_users")
    if _last_show_all is None or _last_show_all != show_all_users:
        LOGGER.info("Today's Activity filter changed | show_all_users=%s", show_all_users)
        st.session_state._last_show_all_users = show_all_users

    if show_all_users:
        recent_df = utils.load_recent_tasks(LS_COMPLETED_TASKS_DIR, user_key=None, limit=50)
    else:
        recent_df = utils.load_recent_tasks(LS_COMPLETED_TASKS_DIR, user_key=user_key, limit=50)
    if not recent_df.empty:
        recent_df["Duration"] = recent_df["DurationSeconds"].apply(utils.format_hhmmss)
        recent_df["Uploaded"] = pd.to_datetime(recent_df["EndTimestampUTC"], utc=True).apply(lambda x: utils.format_time_ago(x))
        if "PartiallyComplete" not in recent_df.columns:
            recent_df["PartiallyComplete"] = pd.Series([pd.NA] * len(recent_df), dtype="boolean")
        else:
            recent_df["PartiallyComplete"] = recent_df["PartiallyComplete"].astype("boolean")
        recent_df["Part. Completed?"] = recent_df["PartiallyComplete"].fillna(False).astype(bool)
        recent_df["Notes"] = recent_df.get("Notes", pd.Series([""] * len(recent_df))).fillna("")
        recent_df["DisplayUser"] = recent_df["FullName"].fillna("").astype(str).str.strip()
        mask_blank = recent_df["DisplayUser"].eq("")
        recent_df.loc[mask_blank, "DisplayUser"] = recent_df.loc[mask_blank, "UserLogin"].fillna("").astype(str)
        display_df = recent_df.rename(columns={"TaskName": "Task", "DisplayUser": "User"})[["User", "Task", "Part. Completed?", "Uploaded", "Duration", "Notes"]]
        st.dataframe(
            display_df, hide_index=True, width="stretch",
            column_config={
                "Part. Completed?": st.column_config.CheckboxColumn("Part. Completed?", disabled=True, width="small"),
                "Notes": st.column_config.TextColumn("Notes", width="large"),
                "Uploaded": st.column_config.TextColumn("Uploaded", width="small"),
            },
        )
    else:
        LOGGER.info("No LS tasks completed today to display.")
        st.info("No tasks completed today.")

    st.caption(f"\n\n\nApp version: {config.APP_VERSION}", text_alignment="center")
    if st.session_state.state == "running":
        st_autorefresh(interval=10000, key="timer")

# ============================================================
# DATA & ANALYTICS — UI
# ============================================================
else:
    if st.session_state.get("da_uploaded"):
        LOGGER.info("Showing DA upload success toast.")
        st.toast("Upload Successful", icon="✅")
        st.session_state.da_uploaded = False
    if st.session_state.get("da_archived"):
        LOGGER.info("Showing DA archived-task toast.")
        st.toast("Task archived")
        st.session_state.da_archived = False
    if st.session_state.get("da_task_resumed"):
        LOGGER.info("Showing DA task-resumed toast.")
        st.toast("Task resumed — fill in any details and press Start", icon="▶️")
        st.session_state.da_task_resumed = False

    spacer_l, left_col, _, mid_col, _, right_col, spacer_r = st.columns([0.4, 4, 0.2, 4, 0.2, 4, 0.4])
    with left_col:
        user_login = utils.get_os_user()
        full_name = utils.get_full_name_for_user(None, user_login)
        user_key = utils.sanitize_key(user_login)
        st.session_state.da_current_user_key = user_key
        inputs_locked = st.session_state.da_state != "idle"
        st.text_input("User", value=full_name, disabled=True, key="da_user_display")
        task_key = f"da_task_{st.session_state.da_reset_counter}"
        if st.session_state.da_restored_task_name and task_key not in st.session_state:
            st.session_state[task_key] = st.session_state.da_restored_task_name
        task_name = st.text_input("Task", disabled=inputs_locked, key=task_key)
        effective_task_name = da_get_effective_task_name(task_name)
        st.text_area("Notes (optional)", key="da_notes", height=120)

    with mid_col:
        dept_key = f"da_dept_{st.session_state.da_reset_counter}"
        if st.session_state.da_restored_department and dept_key not in st.session_state:
            if st.session_state.da_restored_department in DEPARTMENT_OPTIONS:
                st.session_state[dept_key] = st.session_state.da_restored_department
        selected_department = st.selectbox("Department", DEPARTMENT_OPTIONS, key=dept_key, disabled=inputs_locked)
        if not inputs_locked:
            st.session_state.da_department = selected_department
        stakeholder_key = f"da_stakeholder_{st.session_state.da_reset_counter}"
        if st.session_state.da_restored_primary_stakeholder and stakeholder_key not in st.session_state:
            st.session_state[stakeholder_key] = st.session_state.da_restored_primary_stakeholder
        primary_stakeholder = st.text_input("Primary Stakeholder (optional)", disabled=inputs_locked, key=stakeholder_key)
        if not inputs_locked:
            st.session_state.da_primary_stakeholder = primary_stakeholder
        account_options = [""] + utils.load_accounts(str(PERSONNEL_DIR))
        acct_key = f"da_acct_{st.session_state.da_reset_counter}"
        if st.session_state.da_restored_account and acct_key not in st.session_state:
            if st.session_state.da_restored_account in account_options:
                st.session_state[acct_key] = st.session_state.da_restored_account
        selected_account = st.selectbox("Account (optional)", account_options, key=acct_key, disabled=inputs_locked)
        archived_count = len(utils.load_archived_tasks(DA_ARCHIVED_TASKS_DIR, user_key))
        data_signature = (len([a for a in account_options if a]), archived_count)
        if st.session_state.get("da__task_tracker_data_signature") != data_signature:
            st.session_state.da__task_tracker_data_signature = data_signature
            LOGGER.info("DA data snapshot | accounts=%s archived_tasks=%s", *data_signature)

    with right_col:
        st.session_state.da_elapsed_seconds = da_compute_elapsed_seconds()
        hh, mm = utils.format_hh_mm_parts(st.session_state.da_elapsed_seconds)
        colon_class = "blink-colon" if st.session_state.da_state == "running" else ""
        st.markdown(
            f'<div style="text-align:center;margin-bottom:20px;">'
            f'<div style="font-size:36px;font-weight:600;">{hh}<span class="{colon_class}">:</span>{mm}</div>'
            f'<div style="font-size:15px;color:#6b6b6b;">Elapsed Time</div></div>',
            unsafe_allow_html=True,
        )
        if st.session_state.da_state == "idle":
            c1, c2 = st.columns(2)
            can_start = bool(task_name and selected_department)
            with c1:
                st.button("Start", width="stretch", disabled=not can_start,
                          help=None if can_start else "Enter a task and select a department to start",
                          on_click=da_start_task if can_start else None,
                          key="da_start_btn")
            with c2:
                st.button("End", width="stretch", disabled=True, key="da_end_disabled")
        elif st.session_state.da_state == "running":
            c1, c2 = st.columns(2)
            with c1:
                st.button("Pause", width="stretch", on_click=da_pause_task, key="da_pause_btn")
            with c2:
                st.button("End", width="stretch", on_click=da_end_task, key="da_end_btn")
        elif st.session_state.da_state == "paused":
            c1, c2 = st.columns(2)
            with c1:
                st.button("Resume", width="stretch", on_click=da_resume_task, key="da_resume_btn")
            with c2:
                st.button("End", width="stretch", on_click=da_end_task, key="da_end_paused_btn")
        if st.session_state.da_state == "paused":
            st.button("Archive", width="stretch", on_click=da_archive_task,
                      args=(user_login, full_name, user_key, effective_task_name, selected_account),
                      key="da_archive_btn")
        if archived_count > 0:
            if st.button(f"You have {archived_count} archived tasks, click here to review",
                         key="da_review_archived_link", type="tertiary"):
                LOGGER.info("User opened DA archived tasks review with %s archived item(s).", archived_count)
                st.session_state.da_review_archive_open = True
                st.session_state.da_review_archive_rendered = False
                st.rerun()
        if st.session_state.da_state == "ended":
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Upload", type="primary", width="stretch", key="da_upload_btn"):
                    if not effective_task_name:
                        LOGGER.warning("Upload blocked because effective task name is blank.")
                        st.error("Task name is missing. Please reset and select a task again.")
                    else:
                        st.session_state.da_submit_partially_complete = st.session_state.get("da_partially_complete", False)
                        st.session_state.da_confirm_open = True
                        st.session_state.da_confirm_rendered = False
                        st.rerun()
            with c2:
                st.button("Reset", width="stretch", on_click=da_reset_all, key="da_reset_btn")
        if st.session_state.da_state != "idle":
            pc_left, pc_right = st.columns([1.4, 3])
            with pc_left:
                st.markdown("<div style='padding-top: 8px;'>Partially complete?</div>", unsafe_allow_html=True)
            with pc_right:
                st.toggle("Partially complete", key="da_partially_complete", label_visibility="collapsed")

    if st.session_state.da_state in ("running", "paused") and not st.session_state.da_live_activity_saved and effective_task_name:
        try:
            utils.save_live_activity(
                DA_LIVE_ACTIVITY_DIR, user_key, user_login, full_name,
                effective_task_name, None, selected_account,
                st.session_state.da_primary_stakeholder, st.session_state.da_notes,
                st.session_state.da_start_utc, state=st.session_state.da_state,
                paused_seconds=st.session_state.da_paused_seconds,
                pause_start_utc=st.session_state.da_pause_start_utc,
            )
            st.session_state.da_live_activity_saved = True
            st.session_state.da_live_task_name = effective_task_name
            st.session_state.da_live_account = selected_account
            LOGGER.info("DA live activity saved | task='%s' state='%s'", effective_task_name, st.session_state.da_state)
        except Exception as exc:
            LOGGER.warning("Failed to save DA live activity: %s", exc)

    if st.session_state.da_confirm_open and not st.session_state.get("da_confirm_rendered"):
        st.session_state.da_confirm_rendered = True
        da_confirm_submit(user_login, full_name, user_key, effective_task_name, selected_account)
    if st.session_state.da_review_archive_open and not st.session_state.get("da_review_archive_rendered"):
        st.session_state.da_review_archive_rendered = True
        da_review_archived_tasks_dialog(user_login, full_name, user_key)

    da_live_activity_section()

    st.divider()

    # --- Recent Activity header row ---
    title_col, text_col, toggle_col = st.columns([6, 5, 0.5], vertical_alignment="center")
    with title_col:
        st.subheader("Recent Activity", anchor=False)
    with text_col:
        st.markdown("Show all users?", text_alignment="right")
    with toggle_col:
        show_all_users = st.toggle("Show all users?", value=True, key="da_show_all_users", label_visibility="collapsed")
    _last_show_all = st.session_state.get("da__last_show_all_users")
    if _last_show_all is None or _last_show_all != show_all_users:
        LOGGER.info("DA Recent Activity filter changed | show_all_users=%s", show_all_users)
        st.session_state.da__last_show_all_users = show_all_users

    # --- Date-range view toggle buttons ---
    today_eastern = utils.to_eastern(utils.now_utc()).date()
    current_sprint_num = _sprint_number_for_date(today_eastern)
    sprint_start, sprint_end = _sprint_dates(current_sprint_num)

    if "da_activity_view" not in st.session_state:
        st.session_state.da_activity_view = "Today"

    view_cols = st.columns(4)
    view_options = ["Today", "This Week", "This Sprint", "All Time"]
    for idx, label in enumerate(view_options):
        with view_cols[idx]:
            btn_type = "primary" if st.session_state.da_activity_view == label else "secondary"
            if st.button(label, key=f"da_view_{label}", use_container_width=True, type=btn_type):
                st.session_state.da_activity_view = label
                st.rerun()

    active_view = st.session_state.da_activity_view

    # Compute date boundaries for the active view
    if active_view == "Today":
        view_start, view_end = today_eastern, today_eastern
    elif active_view == "This Week":
        view_start = today_eastern - timedelta(days=today_eastern.weekday())  # Monday
        view_end = today_eastern
    elif active_view == "This Sprint":
        view_start, view_end = sprint_start, sprint_end
    else:  # All Time
        view_start, view_end = None, None

    # All Time date range filter
    if active_view == "All Time":
        d1, d2 = st.columns(2)
        with d1:
            alltime_start = st.date_input("From", value=today_eastern - timedelta(days=30), key="da_alltime_start")
        with d2:
            alltime_end = st.date_input("To", value=today_eastern, key="da_alltime_end")
        if alltime_start and alltime_end:
            view_start = alltime_start
            view_end = alltime_end

    if st.session_state.get("da_task_deleted"):
        st.toast("Task deleted", icon="🗑️")
        st.session_state.da_task_deleted = False

    # --- Load data based on view ---
    if active_view == "Today":
        if show_all_users:
            recent_df = utils.load_recent_tasks(DA_COMPLETED_TASKS_DIR, user_key=None, limit=50)
        else:
            recent_df = utils.load_recent_tasks(DA_COMPLETED_TASKS_DIR, user_key=user_key, limit=50)
    else:
        all_df = utils.load_all_completed_tasks(DA_COMPLETED_TASKS_DIR)
        if not all_df.empty:
            if not show_all_users:
                all_df = all_df[all_df["UserLogin"].str.strip().str.lower() == user_key.lower()]
            if view_start is not None and view_end is not None:
                all_df = all_df[(all_df["Date"] >= view_start) & (all_df["Date"] <= view_end)]
            recent_df = all_df.sort_values("StartTimestampUTC", ascending=False).reset_index(drop=True)
        else:
            recent_df = pd.DataFrame()

    # --- Build own-task data map for edits/deletes (load from all matching dates) ---
    own_task_data_map: dict[str, dict] = {}
    if not recent_df.empty:
        if active_view == "Today":
            own_tasks_df = load_own_tasks_with_paths(DA_COMPLETED_TASKS_DIR, user_key)
        else:
            own_files = list(DA_COMPLETED_TASKS_DIR.glob(f"user={user_key}/year=*/month=*/day=*/*.parquet"))
            if own_files:
                frames = []
                for f in own_files:
                    try:
                        df_one = pd.read_parquet(f)
                        df_one["_file_path"] = str(f)
                        frames.append(df_one)
                    except Exception:
                        continue
                own_tasks_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                if not own_tasks_df.empty and "StartTimestampUTC" in own_tasks_df.columns:
                    own_tasks_df["StartTimestampUTC"] = pd.to_datetime(own_tasks_df["StartTimestampUTC"], utc=True)
            else:
                own_tasks_df = pd.DataFrame()
        if not own_tasks_df.empty and "StartTimestampUTC" in own_tasks_df.columns:
            for _, orow in own_tasks_df.iterrows():
                ts_key = str(orow["StartTimestampUTC"])
                pc_val = orow.get("PartiallyComplete", False)
                own_task_data_map[ts_key] = {
                    "file_path": str(orow["_file_path"]),
                    "task_name": str(orow.get("TaskName") or ""),
                    "account": str(orow.get("CompanyGroup") or ""),
                    "stakeholder": str(orow.get("CoveringFor") or ""),
                    "department": str(orow.get("Department") or ""),
                    "notes": str(orow.get("Notes") or ""),
                    "partially_complete": pc_val is True or pc_val == True,
                }

    if st.session_state.get("da_task_edited"):
        st.toast("Changes saved", icon="✅")
        st.session_state.da_task_edited = False

    if not recent_df.empty:
        recent_df["Duration"] = recent_df["DurationSeconds"].apply(utils.format_hhmmss)
        recent_df["Uploaded"] = pd.to_datetime(recent_df["EndTimestampUTC"], utc=True).apply(lambda x: utils.format_time_ago(x))
        # Add Date column (Eastern time)
        recent_df["Date"] = pd.to_datetime(recent_df["StartTimestampUTC"], utc=True).apply(
            lambda x: utils.to_eastern(x).date()
        )
        if "PartiallyComplete" not in recent_df.columns:
            recent_df["PartiallyComplete"] = pd.Series([pd.NA] * len(recent_df), dtype="boolean")
        else:
            recent_df["PartiallyComplete"] = recent_df["PartiallyComplete"].astype("boolean")
        recent_df["Part. Completed?"] = recent_df["PartiallyComplete"].fillna(False).astype(bool)
        recent_df["Notes"] = recent_df.get("Notes", pd.Series([""] * len(recent_df))).fillna("")
        recent_df["Department"] = recent_df.get("Department", pd.Series([""] * len(recent_df))).fillna("")
        recent_df["DisplayUser"] = recent_df["FullName"].fillna("").astype(str).str.strip()
        mask_blank = recent_df["DisplayUser"].eq("")
        recent_df.loc[mask_blank, "DisplayUser"] = recent_df.loc[mask_blank, "UserLogin"].fillna("").astype(str)
        row_file_paths: list[str] = []
        row_is_own: list[bool] = []
        for _, rrow in recent_df.iterrows():
            is_own = str(rrow.get("UserLogin", "")).strip().lower() == user_login.lower()
            row_is_own.append(is_own)
            ts_key = str(rrow.get("StartTimestampUTC"))
            task_data = own_task_data_map.get(ts_key, {})
            row_file_paths.append(task_data.get("file_path", "") if is_own else "")
        display_df = recent_df.rename(columns={"TaskName": "Task", "DisplayUser": "User"})[
            ["Date", "User", "Task", "Department", "Part. Completed?", "Uploaded", "Duration", "Notes"]
        ].copy()
        display_df.insert(0, "Delete", False)
        edited_df = st.data_editor(
            display_df, hide_index=True, width="stretch",
            disabled=["Date", "User", "Uploaded"],
            column_config={
                "Delete": st.column_config.CheckboxColumn(" ", width=25, default=False),
                "Date": st.column_config.DateColumn("Date", width="small"),
                "Part. Completed?": st.column_config.CheckboxColumn("Part. Completed?", width="small"),
                "Department": st.column_config.SelectboxColumn("Department", options=DEPARTMENT_OPTIONS[1:], width="small"),
                "Notes": st.column_config.TextColumn("Notes", width="large"),
                "Uploaded": st.column_config.TextColumn("Uploaded", width="small"),
            },
        )

        # --- Detect edits on non-Delete columns ---
        editable_cols = ["Task", "Department", "Part. Completed?", "Duration", "Notes"]
        edited_own_rows: list[int] = []
        edited_other_rows: list[int] = []
        for i in display_df.index:
            changed = False
            for col in editable_cols:
                orig_val = display_df.at[i, col]
                new_val = edited_df.at[i, col]
                if col == "Part. Completed?":
                    if bool(orig_val) != bool(new_val):
                        changed = True
                        break
                else:
                    if str(orig_val) != str(new_val):
                        changed = True
                        break
            if changed:
                if row_file_paths[i]:
                    edited_own_rows.append(i)
                else:
                    edited_other_rows.append(i)

        if edited_other_rows:
            st.caption("You can only edit your own tasks. Changes to other users' rows will be ignored.")

        # --- Action buttons row ---
        checked_indices = edited_df.index[edited_df["Delete"]].tolist()
        has_edits = len(edited_own_rows) > 0
        has_deletes = len(checked_indices) > 0

        if has_edits and not has_deletes:
            if st.button(f"Save changes ({len(edited_own_rows)} row{'s' if len(edited_own_rows) > 1 else ''})", key="da_save_edits_btn", type="primary"):
                for i in edited_own_rows:
                    fpath = row_file_paths[i]
                    try:
                        src_df = pd.read_parquet(fpath)
                        src_df["TaskName"] = str(edited_df.at[i, "Task"])
                        src_df["Notes"] = str(edited_df.at[i, "Notes"]) if edited_df.at[i, "Notes"] else None
                        src_df["PartiallyComplete"] = bool(edited_df.at[i, "Part. Completed?"])
                        new_dept = str(edited_df.at[i, "Department"]) if edited_df.at[i, "Department"] else None
                        if "Department" in src_df.columns:
                            src_df["Department"] = new_dept
                        new_dur = utils.parse_hhmmss(str(edited_df.at[i, "Duration"]))
                        if new_dur >= 0:
                            src_df["DurationSeconds"] = int(new_dur)
                        utils.atomic_write_parquet(src_df, Path(fpath), schema=DA_PARQUET_SCHEMA)
                        LOGGER.info("Edited DA task | file='%s'", Path(fpath).name)
                    except Exception as exc:
                        LOGGER.warning("Failed to save edit for %s: %s", fpath, exc)
                        st.error(f"Failed to save edit: {exc}")
                utils.load_recent_tasks.clear()
                utils.load_all_completed_tasks.clear()
                utils.load_completed_tasks_for_analytics.clear()
                st.session_state.da_task_edited = True
                st.rerun()

        if has_deletes:
            deletable = [(i, row_file_paths[i]) for i in checked_indices if row_file_paths[i]]
            non_own = len(checked_indices) - len(deletable)
            if non_own > 0:
                st.caption("You can only delete your own tasks.")
            # Find resumable tasks (own + partially complete + tracker idle)
            resumable = []
            if st.session_state.da_state == "idle":
                for i, fpath in deletable:
                    ts_key = str(recent_df.iloc[i].get("StartTimestampUTC"))
                    td = own_task_data_map.get(ts_key, {})
                    if td.get("partially_complete", False):
                        resumable.append((i, fpath, td))
            show_resume = len(resumable) == 1
            if deletable:
                n = len(deletable)
                if show_resume:
                    del_col, resume_col = st.columns(2)
                else:
                    del_col = st.container()
                with del_col:
                    if st.button(f"Confirm delete ({n} task{'s' if n > 1 else ''})", key="da_confirm_delete_btn", type="primary"):
                        for _, fpath in deletable:
                            delete_completed_task(fpath, DA_COMPLETED_TASKS_DIR, user_key)
                        utils.load_recent_tasks.clear()
                        utils.load_all_completed_tasks.clear()
                        utils.load_completed_tasks_for_analytics.clear()
                        st.session_state.da_task_deleted = True
                        st.rerun()
                if show_resume:
                    _, resume_fpath, resume_data = resumable[0]
                    with resume_col:
                        if st.button("Resume task", key="da_resume_completed_btn"):
                            delete_completed_task(resume_fpath, DA_COMPLETED_TASKS_DIR, user_key)
                            utils.load_recent_tasks.clear()
                            da_reset_all()
                            st.session_state.da_restored_task_name = resume_data["task_name"]
                            st.session_state.da_restored_account = resume_data["account"]
                            st.session_state.da_restored_primary_stakeholder = resume_data["stakeholder"]
                            st.session_state.da_restored_department = resume_data["department"]
                            st.session_state.da_notes = resume_data["notes"]
                            st.session_state.da_last_task_name = resume_data["task_name"]
                            st.session_state.da_task_resumed = True
                            LOGGER.info("Resumed DA partially complete task | task='%s'", resume_data["task_name"])
                            st.rerun()
                elif len(resumable) > 1:
                    st.caption("Select only one partially complete task to resume.")
    else:
        st.info("No tasks found for this view.")

    st.caption(f"\n\n\nApp version: {config.APP_VERSION}", text_alignment="center")
    if st.session_state.da_state == "running":
        st_autorefresh(interval=10000, key="da_timer")
