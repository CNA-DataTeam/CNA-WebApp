"""

Purpose:
    Shared utility layer for the Logistics Support Streamlit app.

What it does:
    - Time helpers:
        * now_utc() -> datetime (UTC)
        * to_eastern(dt) -> datetime (America/New_York)
        * format_time_ago(dt) -> str (human relative time)
        * format_hhmm / format_hhmmss / format_hh_mm_parts -> str/tuple
        * parse_hhmmss(str) -> int seconds (or -1 if invalid)
    - Identity/helpers:
        * get_os_user() -> str (cached)
        * sanitize_key(str) -> str (safe filesystem/user key)
        * UserContext + get_user_context() -> permission-ready user metadata
    - Styling/assets:
        * get_global_css() -> str (cached CSS)
        * get_logo_base64(path) -> str base64 (cached)
    - Parquet I/O (atomic, schema-driven):
        * atomic_write_parquet(df, path, schema) -> writes parquet safely
        * build_out_dir(completed_dir, user_key, ts) -> Path partitioned by date
    - Live Activity (real-time collaboration via small parquet files):
        * save_live_activity(...) -> writes user=<key>.parquet
        * update_live_activity_state(...) -> updates State/PausedSeconds
        * load_own_live_activity(dir, user_key) -> dict|None (restore state)
        * delete_live_activity(dir, user_key) -> removes file
        * load_live_activities(dir, exclude_user_key) -> DataFrame (team view)
    - Data loading:
        * load_recent_tasks(root, user_key, limit) -> DataFrame (today’s tasks)
        * load_all_completed_tasks(base_dir) -> DataFrame (historical)
        * load_tasks(tasks_xlsx_path) -> DataFrame (active tasks)
        * load_accounts(personnel_dir) -> list[str] (company groups)
        * load_user_fullname_map(tasks_xlsx_path) -> dict[user_login->full name]
        * get_full_name_for_user(tasks_xlsx_path, user_login) -> str
        * load_all_user_full_names(tasks_xlsx_path) -> list[str]

Inputs:
    - Paths and constants from config.py
    - Parquet directories (completed tasks, live activity, cached personnel)
    - Excel file path for task/user mappings

Outputs:
    - Consistent dataframes/lists/dicts for pages to render
    - Parquet artifacts for completed tasks and live activity
"""

from __future__ import annotations
import base64
import getpass
import re
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import streamlit as st
import config

# Timezone for Eastern Time
EASTERN_TZ = ZoneInfo("America/New_York")

# Define schemas for Parquet files
PARQUET_SCHEMA = pa.schema([
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
])
LIVE_ACTIVITY_SCHEMA = pa.schema([
    ("UserKey", pa.string()),
    ("UserLogin", pa.string()),
    ("FullName", pa.string()),
    ("TaskName", pa.string()),
    ("TaskCadence", pa.string()),
    ("CompanyGroup", pa.string()),
    ("IsCoveringFor", pa.bool_()),
    ("CoveringFor", pa.string()),
    ("Notes", pa.string()),
    ("StartTimestampUTC", pa.timestamp("us", tz="UTC")),
    ("State", pa.string()),
    ("PausedSeconds", pa.int64()),
    ("PauseStartTimestampUTC", pa.timestamp("us", tz="UTC")),
])
ARCHIVED_TASK_SCHEMA = pa.schema([
    ("ArchiveID", pa.string()),
    ("UserKey", pa.string()),
    ("UserLogin", pa.string()),
    ("FullName", pa.string()),
    ("TaskName", pa.string()),
    ("TaskCadence", pa.string()),
    ("CompanyGroup", pa.string()),
    ("IsCoveringFor", pa.bool_()),
    ("CoveringFor", pa.string()),
    ("Notes", pa.string()),
    ("StartTimestampUTC", pa.timestamp("us", tz="UTC")),
    ("PausedSeconds", pa.int64()),
    ("PauseStartTimestampUTC", pa.timestamp("us", tz="UTC")),
    ("ArchivedTimestampUTC", pa.timestamp("us", tz="UTC")),
    ("AppVersion", pa.string()),
])

@lru_cache(maxsize=1)
def get_os_user() -> str:
    """Return the current OS username (cached)."""
    return getpass.getuser()

def now_utc() -> datetime:
    """Get current time in UTC."""
    return datetime.now(timezone.utc)

def to_eastern(dt: datetime) -> datetime:
    """Convert a datetime to Eastern Time."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(EASTERN_TZ)

@lru_cache(maxsize=128)
def sanitize_key(value: str) -> str:
    """Sanitize a string to be filesystem-friendly and lowercase."""
    value = value.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_\-\.]", "", value)
    return value

def format_hhmm(seconds: int) -> str:
    """Format seconds as HH:MM."""
    seconds = max(0, int(seconds))
    return f"{seconds//3600:02d}:{(seconds%3600)//60:02d}"

def format_hhmmss(seconds: int) -> str:
    """Format seconds as HH:MM:SS."""
    seconds = max(0, int(seconds))
    return f"{seconds//3600:02d}:{(seconds%3600)//60:02d}:{seconds%60:02d}"

def format_hh_mm_parts(seconds: int) -> tuple[str, str]:
    """Return hours and minutes (zero-padded) from seconds."""
    seconds = max(0, int(seconds))
    return f"{seconds//3600:02d}", f"{(seconds%3600)//60:02d}"

def parse_hhmmss(time_str: str) -> int:
    """Parse a time string HH:MM[:SS] to total seconds. Returns -1 on failure."""
    try:
        parts = time_str.strip().split(":")
        if len(parts) == 3:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
            return h * 3600 + m * 60 + s
        if len(parts) == 2:
            h, m = int(parts[0]), int(parts[1])
            return h * 3600 + m * 60
    except (ValueError, AttributeError):
        pass
    return -1

def format_time_ago(dt: datetime) -> str:
    """Format a past datetime as a relative time string (e.g., '5 min ago')."""
    if dt is None:
        return ""
    now = now_utc()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    diff = now - dt
    seconds = int(diff.total_seconds())
    if seconds < 60:
        return "less than a minute ago"
    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} min ago"
    if seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hr ago"
    days = seconds // 86400
    return f"{days} day{'s' if days > 1 else ''} ago"

@st.cache_data
def get_global_css() -> str:
    """Return global CSS styling for the app (cached)."""
    return """
    <style>
    /* Import custom fonts */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@600&family=Work+Sans:wght@400;500;600&display=swap');
    /* Base font settings */
    html, body, [class*="css"] {
        font-family: 'Work Sans', sans-serif;
    }
    h1, h2, h3 {
        font-family: 'Poppins', sans-serif;
        font-weight: 600;
    }
    /* Hide default Streamlit footer */
    footer {visibility: hidden;}
    /* Adjust main container padding */
    .block-container {
        padding-top: 1rem;
    }
    /* Hide Deploy button (last header button in toolbar) */
    [data-testid="stToolbar"] button[data-testid="stBaseButton-header"]:last-of-type {
        display: none !important;
    }
    /* Header styling */
    .header-row {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 14px;
        margin-top: 10px;
        margin-bottom: 6px;
    }
    .header-logo {
        width: 80px;
        height: auto;
    }
    .header-title {
        margin: 0 !important;
        text-align: center;
    }
    /* App navigation card styling */
    .app-card {
        border: 1px solid #E6E6E6;
        border-radius: 12px;
        padding: 18px 20px;
        background-color: #FFFFFF;
        transition: box-shadow 0.15s ease-in-out;
    }
    .app-card:hover {
        box-shadow: 0 6px 18px rgba(0,0,0,0.08);
    }
    .app-title {
        font-size: 18px;
        font-weight: 600;
        margin-bottom: 6px;
    }
    .app-desc {
        color: #6b6b6b;
        font-size: 14px;
        margin-bottom: 14px;
    }
    /* Timer blinking colon */
    @keyframes blink { 50% { opacity: 0; } }
    .blink-colon {
        animation: blink 1s steps(1, start) infinite;
    }
    /* Live activity pulse dot */
    .live-activity-pulse {
        display: inline-block;
        width: 12px;
        height: 12px;
        background-color: #C30000;
        border-radius: 100%;
        margin-right: 2px;
        animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(1.2); }
    }
    /* Reset button style */
    .reset-button div > button {
        background-color: #C30000 !important;
        color: white !important;
        border: none !important;
    }
    .reset-button div > button:hover {
        background-color: #A00000 !important;
    }
    .reset-button div > button:focus {
        box-shadow: none !important;
    }
    /* Hide autorefresh iframe (used for timer) */
    iframe[title="streamlit_autorefresh.st_autorefresh"] {
        display: none;
    }
    /* Dataframe header style */
    .stDataFrame thead th {
        font-weight: 800 !important;
    }
    /* KPI card styling (analytics page) */
    .kpi-card {
        background-color: #F7F7F7;
        padding: 18px;
        border-radius: 12px;
        text-align: center;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: 600;
    }
    .kpi-label {
        color: #6b6b6b;
        font-size: 14px;
    }
    </style>
    """

@st.cache_data
def get_logo_base64(logo_path: str) -> str:
    """Return base64-encoded string of the logo image (cached)."""
    try:
        data = Path(logo_path).read_bytes()
        return base64.b64encode(data).decode("utf-8")
    except Exception:
        return ""

@lru_cache(maxsize=1)
def find_task_tracker_root() -> Path:
    """Locate Task-Tracker folder from configured roots."""
    # Preferred direct roots (new local sync structure)
    for candidate in getattr(config, "TASK_TRACKER_ROOT_HINTS", []):
        if candidate.exists():
            return candidate

    # Legacy SharePoint/OneDrive discovery fallback
    for root in config.POTENTIAL_ROOTS:
        for lib in config.DOCUMENT_LIBRARIES:
            candidate = root / lib / config.RELATIVE_APP_PATH
            if candidate.exists():
                return candidate
    raise FileNotFoundError("Task-Tracker folder not found. Ensure SharePoint is synced locally.")

def atomic_write_parquet(df: pd.DataFrame, path: Path, schema: pa.Schema = PARQUET_SCHEMA) -> None:
    """Atomically write DataFrame to a Parquet file at the given path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".parquet.tmp")
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, tmp_path)
    tmp_path.replace(path)

def build_out_dir(completed_dir: Path, user_key: str, ts: datetime) -> Path:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    ts_eastern = to_eastern(ts)

    out_dir = (
        completed_dir
        / f"user={user_key}"
        / f"year={ts_eastern.year}"
        / f"month={ts_eastern.month:02d}"
        / f"day={ts_eastern.day:02d}"
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

def save_live_activity(
    live_activity_dir: Path,
    user_key: str,
    user_login: str,
    full_name: str,
    task_name: str,
    cadence: str,
    account: str,
    covering_for: str,
    notes: str,
    start_utc: datetime,
    state: str = "running",
    paused_seconds: int = 0,
    pause_start_utc: datetime | None = None,
) -> None:
    """Save (or update) the current live activity status for a user."""
    live_activity_dir.mkdir(parents=True, exist_ok=True)
    is_covering_for = bool(covering_for and covering_for.strip())
    record = {
        "UserKey": user_key,
        "UserLogin": user_login,
        "FullName": full_name or None,
        "TaskName": task_name,
        "TaskCadence": cadence,
        "CompanyGroup": account or None,
        "IsCoveringFor": is_covering_for,
        "CoveringFor": covering_for or None,
        "Notes": notes.strip() if notes and notes.strip() else None,
        "StartTimestampUTC": start_utc,
        "State": state,
        "PausedSeconds": paused_seconds,
        "PauseStartTimestampUTC": pause_start_utc,
    }
    df = pd.DataFrame([record])
    path = live_activity_dir / f"user={user_key}.parquet"
    atomic_write_parquet(df, path, schema=LIVE_ACTIVITY_SCHEMA)

def update_live_activity_state(
    live_activity_dir: Path,
    user_key: str,
    state: str,
    paused_seconds: int = 0,
    pause_start_utc: datetime | None = None,
) -> None:
    """Update state fields in an existing live activity file for the user."""
    path = live_activity_dir / f"user={user_key}.parquet"
    if not path.exists():
        return
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return
        df["State"] = state
        df["PausedSeconds"] = paused_seconds
        df["PauseStartTimestampUTC"] = pause_start_utc
        atomic_write_parquet(df, path, schema=LIVE_ACTIVITY_SCHEMA)
    except Exception:
        pass

def load_own_live_activity(live_activity_dir: Path, user_key: str) -> dict | None:
    """Load the current user's live activity file (if any) to restore their state."""
    path = live_activity_dir / f"user={user_key}.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return None
        row = df.iloc[0]
        return {
            "full_name": row.get("FullName") or "",
            "task_name": row.get("TaskName"),
            "cadence": row.get("TaskCadence"),
            "account": row.get("CompanyGroup") or "",
            "covering_for": row.get("CoveringFor") or "",
            "notes": row.get("Notes") or "",
            "start_utc": pd.to_datetime(row.get("StartTimestampUTC"), utc=True).to_pydatetime(),
            "state": row.get("State", "running"),
            "paused_seconds": int(row.get("PausedSeconds", 0) or 0),
            "pause_start_utc": pd.to_datetime(row.get("PauseStartTimestampUTC"), utc=True).to_pydatetime() if pd.notna(row.get("PauseStartTimestampUTC")) else None,
        }
    except Exception:
        return None

def delete_live_activity(live_activity_dir: Path, user_key: str) -> None:
    """Delete the live activity file for a user (if it exists)."""
    path = live_activity_dir / f"user={user_key}.parquet"
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass

def save_archived_task(
    archived_tasks_dir: Path,
    user_key: str,
    user_login: str,
    full_name: str,
    task_name: str,
    cadence: str,
    account: str,
    covering_for: str,
    notes: str,
    start_utc: datetime,
    paused_seconds: int,
    pause_start_utc: datetime | None = None,
) -> Path:
    """Save a paused task to archive and return the file path."""
    archived_tasks_dir.mkdir(parents=True, exist_ok=True)
    archive_id = str(uuid.uuid4())
    is_covering_for = bool(covering_for and covering_for.strip())
    record = {
        "ArchiveID": archive_id,
        "UserKey": user_key,
        "UserLogin": user_login,
        "FullName": full_name or None,
        "TaskName": task_name,
        "TaskCadence": cadence,
        "CompanyGroup": account or None,
        "IsCoveringFor": is_covering_for,
        "CoveringFor": covering_for or None,
        "Notes": notes.strip() if notes and notes.strip() else None,
        "StartTimestampUTC": start_utc,
        "PausedSeconds": int(paused_seconds),
        "PauseStartTimestampUTC": pause_start_utc,
        "ArchivedTimestampUTC": now_utc(),
        "AppVersion": config.APP_VERSION,
    }
    df = pd.DataFrame([record])
    start_eastern = to_eastern(start_utc)
    path = (
        archived_tasks_dir
        / f"user={user_key}"
        / f"archive_{start_eastern:%Y%m%d_%H%M%S}_{archive_id[:8]}.parquet"
    )
    atomic_write_parquet(df, path, schema=ARCHIVED_TASK_SCHEMA)
    return path

@st.cache_data(ttl=15)
def load_archived_tasks(archived_tasks_dir: Path, user_key: str) -> pd.DataFrame:
    """Load archived tasks for one user, newest first."""
    base = archived_tasks_dir / f"user={user_key}"
    files = list(base.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    try:
        frames = []
        for file_path in files:
            df_one = pd.read_parquet(file_path)
            if df_one.empty:
                continue
            df_one["ArchiveFilePath"] = str(file_path)
            frames.append(df_one)
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        if df.empty:
            return pd.DataFrame()
        if "ArchivedTimestampUTC" in df.columns:
            df["ArchivedTimestampUTC"] = pd.to_datetime(df["ArchivedTimestampUTC"], utc=True)
            df = df.sort_values("ArchivedTimestampUTC", ascending=False)
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

def delete_archived_task_file(path: Path) -> bool:
    """Delete one archived task file if it exists. Returns True if removed."""
    try:
        if path.exists():
            path.unlink()
            return True
        return False
    except Exception:
        return False

@st.cache_data(ttl=15)
def load_live_activities(
    live_activity_dir: Path,
    _exclude_user_key: str | None = None,
) -> pd.DataFrame:
    files = list(live_activity_dir.glob("user=*.parquet"))
    if not files:
        return pd.DataFrame()
    try:
        needed_cols = [
            "UserKey",
            "FullName",
            "UserLogin",
            "TaskName",
            "StartTimestampUTC",
            "Notes",
        ]
        dataset = ds.dataset(files, format="parquet")
        table = dataset.to_table(columns=needed_cols)
        df = table.to_pandas()
        if _exclude_user_key:
            df = df[df["UserKey"] != _exclude_user_key]
        return df
    except Exception as e:
        st.error(f"Failed to load live activities: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=30)
def load_recent_tasks(completed_dir: Path, user_key: str | None = None, limit: int = 50) -> pd.DataFrame:
    today_eastern = to_eastern(now_utc()).date()
    day_part = f"year={today_eastern.year}/month={today_eastern.month:02d}/day={today_eastern.day:02d}"
    if user_key:
        base = completed_dir / f"user={user_key}" / day_part
        if not base.exists():
            return pd.DataFrame()
        files = list(base.glob("*.parquet"))
    else:
        files = list(completed_dir.glob(f"user=*/{day_part}/*.parquet"))
    if not files:
        return pd.DataFrame()
    try:
        needed_cols = ["StartTimestampUTC", "EndTimestampUTC", "DurationSeconds", "PartiallyComplete", "Notes", "FullName", "UserLogin", "TaskName"]
        dataset = ds.dataset(files, format="parquet")
        table = dataset.to_table(columns=needed_cols)
        df = table.to_pandas()
    except Exception as e:
        st.error(f"Failed to load recent tasks: {e}")
        return pd.DataFrame()
    return df.sort_values("StartTimestampUTC", ascending=False).head(limit)

@st.cache_data(ttl=300)
def load_all_completed_tasks(base_dir: Path) -> pd.DataFrame:
    """Load all completed task records from the CompletedTasks directory."""
    files = list(base_dir.glob("user=*/year=*/month=*/day=*/*.parquet"))
    if not files:
        return pd.DataFrame()
    try:
        dataset = ds.dataset(files, format="parquet")
        df = dataset.to_table().to_pandas()
        # Ensure timestamp columns are proper datetime and add a date field
        df["StartTimestampUTC"] = pd.to_datetime(df["StartTimestampUTC"], utc=True)
        df["EndTimestampUTC"] = pd.to_datetime(df["EndTimestampUTC"], utc=True)
        df["Date"] = df["StartTimestampUTC"].dt.date
        return df
    except Exception as e:
        st.error(f"Failed to load completed tasks: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_user_fullname_map(tasks_xlsx_path: str | None = None) -> dict[str, str]:
    """
    Load mapping from user login to full name from startup output users.parquet.
    """
    users_parquet = Path(config.PERSONNEL_DIR) / "users.parquet"
    try:
        if not users_parquet.exists():
            return {}
        df = pd.read_parquet(users_parquet)
    except Exception:
        return {}
    if df.empty:
        return {}
    cols = {str(c).strip().lower(): c for c in df.columns}
    user_col = cols.get("user")
    full_col = cols.get("full name") or cols.get("fullname")
    if not user_col or not full_col:
        return {}
    df = df[[user_col, full_col]].dropna(subset=[user_col]).copy()
    df[user_col] = df[user_col].astype(str).str.strip().str.lower()
    df[full_col] = df[full_col].astype(str).str.strip()
    mapping: dict[str, str] = {}
    for u, fn in zip(df[user_col], df[full_col]):
        if u and str(fn).strip():
            mapping[u] = str(fn).strip()
    return mapping

def get_full_name_for_user(tasks_xlsx_path: str | None, user_login: str) -> str:
    """Get the full name for a given user login. If not found, return the login."""
    mapping = load_user_fullname_map(tasks_xlsx_path)
    return mapping.get(str(user_login).strip().lower(), user_login)

@st.cache_data(ttl=3600)
def load_all_user_full_names(tasks_xlsx_path: str | None = None) -> list[str]:
    """Load all full names from startup output users.parquet (sorted)."""
    try:
        users_parquet = Path(config.PERSONNEL_DIR) / "users.parquet"
        if not users_parquet.exists():
            return []
        df = pd.read_parquet(users_parquet)
    except Exception:
        return []
    if df.empty:
        return []
    if "Full Name" in df.columns:
        names = df["Full Name"]
    elif "fullname" in df.columns or "FullName" in df.columns:
        col = "fullname" if "fullname" in df.columns else "FullName"
        names = df[col]
    else:
        return []
    names = names.dropna().astype(str).str.strip()
    return sorted(n for n in names.unique() if n)

@st.cache_data(ttl=3600)
def load_tasks(tasks_xlsx_path: str | None = None) -> pd.DataFrame:
    """Load active tasks from startup output tasks.parquet."""
    try:
        tasks_parquet = Path(config.PERSONNEL_DIR) / "tasks.parquet"
        if not tasks_parquet.exists():
            st.error("tasks.parquet not found in Personnel directory. Run startup.py first.")
            return pd.DataFrame()
        df = pd.read_parquet(tasks_parquet)
    except Exception as e:
        st.error(f"Failed to read tasks.parquet: {e}")
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    # Keep startup output clean/consistent
    if "IsActive" in df.columns:
        df = df[df["IsActive"].astype(int) == 1].copy()
    if "TaskName" in df.columns:
        df["TaskName"] = df["TaskName"].astype(str).str.strip()
    if "TaskCadence" in df.columns:
        df["TaskCadence"] = df["TaskCadence"].astype(str).str.strip().str.title()
    return df

@st.cache_data(ttl=3600)
def load_accounts(accounts_dir: str) -> list[str]:
    """Load list of Company Group accounts from cached accounts parquet file."""
    parquet_files = list(Path(accounts_dir).glob("accounts_*.parquet"))
    if not parquet_files:
        return []
    parquet_files.sort(reverse=True)
    df = pd.read_parquet(parquet_files[0], columns=["Company Group USE"])
    return df["Company Group USE"].dropna().astype(str).str.strip().unique().tolist()

class UserContext:
    """Contextual information about the current user (for permission handling)."""
    def __init__(self):
        self.user_login: str = get_os_user()
        try:
            self.full_name: str = get_full_name_for_user(None, self.user_login)
        except Exception:
            self.full_name: str = self.user_login
        if config.ALLOWED_ANALYTICS_USERS and len(config.ALLOWED_ANALYTICS_USERS) > 0:
            login_lower = str(self.user_login).strip().lower()
            full_lower = str(self.full_name).strip().lower()
            allowed_set = [u.lower() for u in config.ALLOWED_ANALYTICS_USERS]
            self.can_view_analytics: bool = (login_lower in allowed_set) or (full_lower in allowed_set)
        else:
            self.can_view_analytics: bool = True

@st.cache_data
def get_user_context() -> UserContext:
    """Get a cached UserContext for the current user."""
    return UserContext()
