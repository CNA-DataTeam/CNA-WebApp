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
        * load_tasks() -> DataFrame (active tasks)
        * load_accounts(personnel_dir) -> list[str] (company groups)
        * load_user_fullname_map() -> dict[user_login->full name]
        * get_full_name_for_user(..., user_login) -> str
        * load_all_user_full_names() -> list[str]

Inputs:
    - Paths and constants from config.py
    - Parquet directories (completed tasks, live activity, cached personnel)
    - Cached parquet files for task and user mappings

Outputs:
    - Consistent dataframes/lists/dicts for pages to render
    - Parquet artifacts for completed tasks and live activity
"""

from __future__ import annotations
import base64
import getpass
import hashlib
import html
import inspect
import logging
import re
import tempfile
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
import app_logging
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
    /* Hide default Streamlit hamburger menu (3 vertical dots) */
    [data-testid="stMainMenu"] {
        display: none !important;
    }
    /* Adjust main container padding */
    .block-container {
        padding-top: 1rem;
    }
    /* Hide Deploy button (last header button in toolbar) */
    [data-testid="stToolbar"] button[data-testid="stBaseButton-header"]:last-of-type {
        display: none !important;
    }
    /* Sidebar navigation sizing */
    [data-testid="stSidebar"] [data-testid="stPageLink"] p,
    [data-testid="stSidebar"] [data-testid="stPageLink"] span {
        font-size: 0.92rem !important;
    }
    /* Tighten sidebar page links inside expanders */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stPageLink"] {
        padding-top: 0 !important;
        padding-bottom: 0 !important;
        min-height: unset !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stVerticalBlock"] {
        gap: 16px !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] {
        padding-bottom: 0 !important;
        margin-bottom: 10px !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] .stElementContainer {
        margin-bottom: 0 !important;
        padding: 0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child [data-testid="stVerticalBlock"] {
        gap: 0 !important;
        height: 20px !important;
        overflow: visible !important;
    }
    /* Thin dividers between page rows inside expanders */
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] > [data-testid="stVerticalBlock"] > div {
        position: relative !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] > [data-testid="stVerticalBlock"] > div::before {
        content: "" !important;
        position: absolute !important;
        top: -12px !important;
        left: 0 !important;
        right: 0 !important;
        height: 1px !important;
        background: #e6e6e6 !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] > [data-testid="stVerticalBlock"] > div:first-child::before {
        display: none !important;
    }
    /* Sidebar page link icons — nudge up to align with text */
    [data-testid="stSidebar"] [data-testid="stPageLink"] [data-testid="stIconMaterial"],
    [data-testid="stSidebar"] [data-testid="stPageLink"] span[class*="icon"] {
        position: relative !important;
        top: -1px !important;
    }
    /* Favorite star: hover toggle and positioning */
    .star-toggle {
        position: relative !important;
        top: -2px !important;
        width: 20px !important;
        height: 34px !important;
        pointer-events: none !important;
    }
    .star-toggle img {
        position: absolute !important;
        top: 0 !important;
        left: 0 !important;
    }
    .star-toggle .star-hover {
        visibility: hidden !important;
    }
    /* Hover on the parent COLUMN (not .star-toggle) because the invisible
       button sits on top with z-index and intercepts all pointer events.
       Hovering the button still bubbles :hover up to the column ancestor. */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child:hover .star-default {
        visibility: hidden !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child:hover .star-hover {
        visibility: visible !important;
    }
    /* Favorite star: column is the positioned ancestor for the absolute button */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child {
        position: relative !important;
    }
    /* Reset all intermediate wrapper divs so position:absolute on the button
       anchors to the column, not a nested Streamlit container. */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child div {
        position: static !important;
    }
    /* Invisible button covers the full column area for click capture. */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child [data-testid="stBaseButton-tertiary"] {
        position: absolute !important;
        inset: 0 !important;
        opacity: 0 !important;
        z-index: 10 !important;
        width: 100% !important;
        height: 100% !important;
        cursor: pointer !important;
        min-height: unset !important;
        padding: 0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
        font-size: 0.72rem !important;
    }
    /* Settings popover — clean dropdown style (popover body portals outside sidebar) */
    [data-testid="stPopoverBody"] {
        padding: 4px 0 !important;
    }
    [data-testid="stPopoverBody"] [data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    [data-testid="stPopoverBody"] .stElementContainer {
        border-bottom: 1px solid #e6e6e6 !important;
    }
    [data-testid="stPopoverBody"] .stElementContainer:last-child {
        border-bottom: none !important;
    }
    [data-testid="stPopoverBody"] [data-testid="stBaseButton-tertiary"] {
        padding: 2px 8px !important;
        min-height: unset !important;
        border-radius: 0 !important;
    }
    [data-testid="stPopoverBody"] [data-testid="stBaseButton-tertiary"] p {
        font-size: 0.85rem !important;
        text-align: left !important;
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
    .header-title {
        margin: 0 !important;
        text-align: center;
    }
    .header-subtitle {
        text-align: center;
        font-style: italic;
        margin-top: -0.35rem;
        margin-bottom: 0.5rem;
        color: #4d4d4d;
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


@lru_cache(maxsize=4)
def get_app_logo_path(logo_path: str | Path | None = None) -> str | None:
    """Return the configured app logo path when the asset exists."""
    try:
        source_path = Path(logo_path if logo_path is not None else config.LOGO_PATH)
        if not source_path.exists():
            return None
        return str(source_path)
    except Exception:
        return None


def render_app_logo(logo_path: str | Path | None = None) -> None:
    """Render the shared app logo in Streamlit chrome when supported."""
    image_path = get_app_logo_path(logo_path)
    if image_path is None or not hasattr(st, "logo"):
        return
    try:
        st.logo(image_path)
    except Exception:
        return


@lru_cache(maxsize=4)
def get_nav_logo_svg_path(logo_path: str | Path | None = None) -> str | None:
    """Return an absolute SVG path compatible with streamlit-navigation-bar."""
    source_path = Path(logo_path if logo_path is not None else config.LOGO_PATH)
    try:
        if not source_path.exists():
            return None
        if source_path.suffix.lower() == ".svg":
            return str(source_path.resolve())

        width = 240
        height = 64
        try:
            from PIL import Image

            with Image.open(source_path) as image:
                width, height = image.size
        except Exception:
            pass

        suffix = source_path.suffix.lower()
        if suffix in {".jpg", ".jpeg"}:
            mime_type = "image/jpeg"
        elif suffix == ".gif":
            mime_type = "image/gif"
        elif suffix == ".webp":
            mime_type = "image/webp"
        else:
            mime_type = "image/png"

        image_b64 = base64.b64encode(source_path.read_bytes()).decode("utf-8")
        cache_key = hashlib.sha1(
            f"{source_path}|{source_path.stat().st_mtime_ns}|{source_path.stat().st_size}".encode("utf-8")
        ).hexdigest()[:12]
        output_dir = Path(tempfile.gettempdir()) / "cna-webapp"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"navbar-logo-{cache_key}.svg"
        if not output_path.exists():
            output_path.write_text(
                (
                    f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
                    'preserveAspectRatio="xMinYMid meet">'
                    f'<image width="{width}" height="{height}" '
                    f'href="data:{mime_type};base64,{image_b64}" /></svg>'
                ),
                encoding="utf-8",
            )
        return str(output_path.resolve())
    except Exception:
        return None


@lru_cache(maxsize=1)
def _registry_title_map() -> dict[str, str]:
    """Build a normalized path->title map from page_registry."""
    try:
        import page_registry
    except Exception:
        return {}

    mapping: dict[str, str] = {}
    section_pages = getattr(page_registry, "SECTION_PAGES", {})
    if not isinstance(section_pages, dict):
        section_pages = {}

    for entries in section_pages.values():
        for entry in entries:
            rel_path = str(getattr(entry, "path", "")).replace("\\", "/").strip().lower()
            title = str(getattr(entry, "title", "")).strip()
            if rel_path and title:
                mapping[rel_path] = title

    home_entry = getattr(page_registry, "HOME_PAGE", None)
    if home_entry is not None:
        rel_path = str(getattr(home_entry, "path", "")).replace("\\", "/").strip().lower()
        title = str(getattr(home_entry, "title", "")).strip()
        if rel_path and title:
            mapping[rel_path] = title
    return mapping


def get_app_icon():
    """Return path to the app icon for st.set_page_config, or None if missing."""
    icon_path = Path(__file__).resolve().parent.parent / "icon.png"
    return str(icon_path) if icon_path.exists() else None




def get_registry_page_title(source_file: str | Path, fallback_title: str) -> str:
    """
    Resolve page title from page_registry using the source file path.
    Falls back to fallback_title when no registry match is found.
    """
    fallback = str(fallback_title).strip() or "Page"
    try:
        source_norm = Path(source_file).resolve().as_posix().lower()
        for rel_path, title in _registry_title_map().items():
            if source_norm.endswith(f"/{rel_path}") or source_norm.endswith(rel_path):
                return title
    except Exception:
        pass
    return fallback


@lru_cache(maxsize=1)
def _registry_quote_map() -> dict[str, str]:
    """Build a normalized title->quote map from page_registry."""
    try:
        import page_registry
    except Exception:
        return {}

    mapping: dict[str, str] = {}
    section_pages = getattr(page_registry, "SECTION_PAGES", {})
    if not isinstance(section_pages, dict):
        section_pages = {}

    for entries in section_pages.values():
        for entry in entries:
            title = str(getattr(entry, "title", "")).strip()
            quote = str(getattr(entry, "quote", "")).strip()
            if title and quote:
                mapping[title] = quote

    home_entry = getattr(page_registry, "HOME_PAGE", None)
    if home_entry is not None:
        title = str(getattr(home_entry, "title", "")).strip()
        quote = str(getattr(home_entry, "quote", "")).strip()
        if title and quote:
            mapping[title] = quote
    return mapping


def get_registry_page_quote(page_title: str) -> str:
    """Resolve page quote from page_registry using the page title."""
    return _registry_quote_map().get(str(page_title).strip(), "")


def render_page_header(page_title: str, show_divider: bool = True) -> None:
    """Render the standard page title header."""
    safe_title = html.escape(str(page_title).strip() or "Page")
    safe_quote = html.escape(get_registry_page_quote(page_title))
    subtitle_html = f'<div class="header-subtitle">{safe_quote}</div>' if safe_quote else ""
    st.markdown(
        f"""
        <div class="header-row">
            <h1 class="header-title">{safe_title}</h1>
        </div>
        {subtitle_html}
        """,
        unsafe_allow_html=True,
    )
    if show_divider:
        st.divider()

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
            "full_name": str(row.get("FullName") or ""),
            "task_name": str(row.get("TaskName") or ""),
            "cadence": str(row.get("TaskCadence") or ""),
            "account": str(row.get("CompanyGroup") or ""),
            "covering_for": str(row.get("CoveringFor") or ""),
            "notes": str(row.get("Notes") or ""),
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
        get_page_logger("Shared Utilities").exception("Failed to load live activities: %s", e)
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
        needed_cols = ["TaskID", "StartTimestampUTC", "EndTimestampUTC", "DurationSeconds", "PartiallyComplete", "Notes", "FullName", "UserLogin", "TaskName", "Department"]
        dataset = ds.dataset(files, format="parquet")
        available = set(dataset.schema.names)
        selected = [c for c in needed_cols if c in available]
        table = dataset.to_table(columns=selected)
        df = table.to_pandas()
    except Exception as e:
        get_page_logger("Shared Utilities").exception("Failed to load recent tasks: %s", e)
        st.error(f"Failed to load recent tasks: {e}")
        return pd.DataFrame()
    return df.sort_values("StartTimestampUTC", ascending=False).head(limit)

@st.cache_data(ttl=300, show_spinner="Loading analytics history...")
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
        get_page_logger("Shared Utilities").exception("Failed to load completed tasks: %s", e)
        st.error(f"Failed to load completed tasks: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Loading analytics history...")
def load_completed_tasks_for_analytics(base_dir: Path) -> pd.DataFrame:
    """Load only the columns required by the analytics page."""
    analytics_cols = [
        "StartTimestampUTC",
        "DurationSeconds",
        "PartiallyComplete",
        "FullName",
        "TaskName",
        "TaskCadence",
    ]
    base_path = Path(base_dir)
    if not base_path.exists():
        return pd.DataFrame(columns=analytics_cols + ["Date"])

    try:
        dataset = ds.dataset(base_path, format="parquet", partitioning="hive")
        available_cols = set(dataset.schema.names)
        selected_cols = [col for col in analytics_cols if col in available_cols]
        if not selected_cols:
            return pd.DataFrame(columns=analytics_cols + ["Date"])

        df = dataset.to_table(columns=selected_cols).to_pandas()
    except Exception as e:
        get_page_logger("Shared Utilities").exception("Failed to load analytics task history: %s", e)
        st.error(f"Failed to load analytics task history: {e}")
        return pd.DataFrame(columns=analytics_cols + ["Date"])

    if df.empty:
        return pd.DataFrame(columns=analytics_cols + ["Date"])

    for col in analytics_cols:
        if col not in df.columns:
            if col == "DurationSeconds":
                df[col] = 0
            elif col == "PartiallyComplete":
                df[col] = False
            else:
                df[col] = ""

    df["StartTimestampUTC"] = pd.to_datetime(df["StartTimestampUTC"], utc=True, errors="coerce")
    df = df[df["StartTimestampUTC"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=analytics_cols + ["Date"])

    df["DurationSeconds"] = pd.to_numeric(df["DurationSeconds"], errors="coerce").fillna(0)
    df["PartiallyComplete"] = df["PartiallyComplete"].fillna(False).astype(bool)
    df["FullName"] = df["FullName"].fillna("").astype(str).str.strip()
    df["TaskName"] = df["TaskName"].fillna("").astype(str).str.strip()
    df["TaskCadence"] = df["TaskCadence"].fillna("").astype(str).str.strip().str.title()
    df["Date"] = df["StartTimestampUTC"].dt.date
    return df[analytics_cols + ["Date"]].reset_index(drop=True)


def _current_month_end_timestamp() -> pd.Timestamp:
    eastern_now = pd.Timestamp(to_eastern(now_utc())).tz_localize(None)
    return (eastern_now + pd.offsets.MonthEnd(0)).normalize()


def _extract_task_definitions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["TaskName", "TaskCadence", "IsActive"])

    task_col = _find_column_by_alias(df, ["TaskName", "Task Name"]) or "TaskName"
    cadence_col = _find_column_by_alias(df, ["TaskCadence", "Task Cadence"]) or "TaskCadence"
    active_col = _find_column_by_alias(df, ["IsActive", "Is Active"]) or "IsActive"

    working_df = df.copy()
    if task_col not in working_df.columns:
        working_df[task_col] = ""
    if cadence_col not in working_df.columns:
        working_df[cadence_col] = ""
    if active_col not in working_df.columns:
        working_df[active_col] = True

    definitions_df = working_df[[task_col, cadence_col, active_col]].copy()
    definitions_df.columns = ["TaskName", "TaskCadence", "IsActive"]
    definitions_df["TaskName"] = definitions_df["TaskName"].fillna("").astype(str).str.strip()
    definitions_df["TaskCadence"] = definitions_df["TaskCadence"].fillna("").astype(str).str.strip().str.title()
    definitions_df = definitions_df[definitions_df["TaskName"].ne("")].copy()
    if definitions_df.empty:
        return pd.DataFrame(columns=["TaskName", "TaskCadence", "IsActive"])

    return definitions_df.drop_duplicates(subset=["TaskName", "TaskCadence"], keep="first").reset_index(drop=True)


def _extract_existing_target_months(df: pd.DataFrame) -> list[pd.Timestamp]:
    if df.empty:
        return []
    month_col = _find_column_by_alias(
        df,
        ["TargetMonthEnd", "Target Month End", "TargetMonth", "MonthEnd", "Month"],
    )
    if not month_col or month_col not in df.columns:
        return []

    month_series = pd.to_datetime(df[month_col], errors="coerce").dropna()
    if month_series.empty:
        return []
    return sorted({pd.Timestamp(value).normalize() for value in month_series.tolist()})


def compute_monthly_task_targets(completed_df: pd.DataFrame) -> pd.DataFrame:
    target_cols = [
        "TaskName",
        "TaskCadence",
        "TargetMonthEnd",
        "CompletedTasks",
        "DistinctUsers",
        "Target",
    ]
    if completed_df.empty or "StartTimestampUTC" not in completed_df.columns:
        return pd.DataFrame(columns=target_cols)

    df = completed_df.copy()
    if "PartiallyComplete" not in df.columns:
        df["PartiallyComplete"] = False
    df["PartiallyComplete"] = df["PartiallyComplete"].fillna(False).astype(bool)
    df = df[~df["PartiallyComplete"]].copy()
    if df.empty:
        return pd.DataFrame(columns=target_cols)

    if "TaskName" not in df.columns:
        return pd.DataFrame(columns=target_cols)
    if "TaskCadence" not in df.columns:
        df["TaskCadence"] = ""
    if "UserLogin" not in df.columns:
        df["UserLogin"] = ""
    if "FullName" not in df.columns:
        df["FullName"] = ""

    df["TaskName"] = df["TaskName"].fillna("").astype(str).str.strip()
    df["TaskCadence"] = df["TaskCadence"].fillna("").astype(str).str.strip().str.title()
    df = df[df["TaskName"].ne("")].copy()
    if df.empty:
        return pd.DataFrame(columns=target_cols)

    user_login_series = df["UserLogin"].fillna("").astype(str).str.strip().str.lower()
    full_name_series = df["FullName"].fillna("").astype(str).str.strip().str.lower()
    df["__completed_user_key"] = user_login_series.where(user_login_series.ne(""), full_name_series)
    df["__completed_user_key"] = df["__completed_user_key"].where(
        df["__completed_user_key"].ne(""),
        "__unknown_user__",
    )

    start_local = pd.to_datetime(df["StartTimestampUTC"], utc=True, errors="coerce")
    df = df[start_local.notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=target_cols)

    start_local = start_local.loc[df.index].dt.tz_convert(EASTERN_TZ).dt.tz_localize(None)
    df["TargetMonthEnd"] = start_local.dt.to_period("M").dt.to_timestamp(how="end").dt.normalize()

    targets_df = (
        df.groupby(["TaskName", "TaskCadence", "TargetMonthEnd"], as_index=False)
        .agg(
            CompletedTasks=("TaskName", "size"),
            DistinctUsers=("__completed_user_key", "nunique"),
        )
    )
    targets_df["Target"] = (
        targets_df["CompletedTasks"]
        .div(targets_df["DistinctUsers"].replace(0, pd.NA))
        .fillna(0.0)
        .round(2)
    )
    return targets_df[target_cols].sort_values(
        ["TargetMonthEnd", "TaskName", "TaskCadence"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def sync_tasks_parquet_targets(
    tasks_parquet_path: Path | None = None,
    completed_dir: Path | None = None,
    task_definitions_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    tasks_parquet = Path(tasks_parquet_path) if tasks_parquet_path is not None else Path(config.PERSONNEL_DIR) / "tasks.parquet"
    completed_root = Path(completed_dir) if completed_dir is not None else config.COMPLETED_TASKS_DIR

    if task_definitions_df is not None:
        raw_df = task_definitions_df.copy(deep=True)
        if tasks_parquet.exists():
            existing_months = _extract_existing_target_months(pd.read_parquet(tasks_parquet))
        else:
            existing_months = []
    else:
        if not tasks_parquet.exists():
            raise FileNotFoundError(f"tasks.parquet not found at '{tasks_parquet}'.")
        raw_df = pd.read_parquet(tasks_parquet)
        existing_months = _extract_existing_target_months(raw_df)

    definitions_df = _extract_task_definitions(raw_df)
    completed_df = load_all_completed_tasks(completed_root)
    target_history_df = compute_monthly_task_targets(completed_df)

    month_values = sorted(
        {
            *existing_months,
            *(
                pd.Timestamp(value).normalize()
                for value in target_history_df["TargetMonthEnd"].dropna().tolist()
            ),
        }
    )
    if not month_values:
        month_values = [_current_month_end_timestamp()]

    if definitions_df.empty:
        output_df = pd.DataFrame(
            columns=["TaskName", "TaskCadence", "TargetMonthEnd", "Target", "IsActive"]
        )
    else:
        months_df = pd.DataFrame({"TargetMonthEnd": month_values})
        definitions_df["__join_key"] = 1
        months_df["__join_key"] = 1
        output_df = definitions_df.merge(months_df, on="__join_key", how="inner").drop(columns="__join_key")
        output_df = output_df.merge(
            target_history_df[["TaskName", "TaskCadence", "TargetMonthEnd", "Target"]],
            on=["TaskName", "TaskCadence", "TargetMonthEnd"],
            how="left",
        )
        output_df["Target"] = output_df["Target"].fillna(0.0).astype(float).round(2)
        output_df = output_df[["TaskName", "TaskCadence", "TargetMonthEnd", "Target", "IsActive"]]

    output_df = output_df.sort_values(
        ["TaskName", "TaskCadence", "TargetMonthEnd"],
        ascending=[True, True, False],
        na_position="last",
    ).reset_index(drop=True)

    tasks_parquet.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = tasks_parquet.with_name(f"{tasks_parquet.stem}.{uuid.uuid4().hex}.tmp")
    output_df.to_parquet(tmp_path, index=False)
    tmp_path.replace(tasks_parquet)
    load_tasks.clear()
    return output_df


def save_task_target(
    task_name: str,
    cadence: str,
    month: int,
    year: int,
    users_assigned: int,
    target: int,
    saved_by: str,
) -> None:
    targets_path = Path(config.TASK_TARGETS_CSV_PATH)
    columns = [
        "TaskName",
        "TaskCadence",
        "Month",
        "Year",
        "UsersAssigned",
        "Target",
        "SavedBy",
        "UpdatedTimestampUTC",
    ]

    if targets_path.exists():
        try:
            existing_df = pd.read_csv(targets_path, sep=None, engine="python")
        except pd.errors.EmptyDataError:
            existing_df = pd.DataFrame(columns=columns)
    else:
        existing_df = pd.DataFrame(columns=columns)

    if "TaskCadence" not in existing_df.columns and "Cadence" in existing_df.columns:
        existing_df["TaskCadence"] = existing_df["Cadence"]
    for column in columns:
        if column not in existing_df.columns:
            existing_df[column] = pd.NA

    normalized_task_name = str(task_name or "").strip()
    normalized_cadence = str(cadence or "").strip().title()
    normalized_month = int(month)
    normalized_year = int(year)
    updated_timestamp = now_utc().isoformat()

    duplicate_mask = (
        existing_df["TaskName"].fillna("").astype(str).str.strip().eq(normalized_task_name)
        & existing_df["TaskCadence"].fillna("").astype(str).str.strip().str.title().eq(normalized_cadence)
        & pd.to_numeric(existing_df["Month"], errors="coerce").fillna(-1).astype(int).eq(normalized_month)
        & pd.to_numeric(existing_df["Year"], errors="coerce").fillna(-1).astype(int).eq(normalized_year)
    )
    if bool(duplicate_mask.any()):
        raise ValueError(
            "A target already exists for this task for the selected cadence and month. "
            "Please edit or delete the existing target before adding a new one."
        )

    existing_df = existing_df.loc[:, columns].copy()

    new_row = pd.DataFrame(
        [
            {
                "TaskName": normalized_task_name,
                "TaskCadence": normalized_cadence,
                "Month": normalized_month,
                "Year": normalized_year,
                "UsersAssigned": int(users_assigned),
                "Target": int(target),
                "SavedBy": str(saved_by or "").strip() or None,
                "UpdatedTimestampUTC": updated_timestamp,
            }
        ]
    )

    output_df = pd.concat([existing_df, new_row], ignore_index=True)
    output_df["Month"] = pd.to_numeric(output_df["Month"], errors="coerce").fillna(0).astype(int)
    output_df["Year"] = pd.to_numeric(output_df["Year"], errors="coerce").fillna(0).astype(int)
    output_df["UsersAssigned"] = pd.to_numeric(output_df["UsersAssigned"], errors="coerce").fillna(0).astype(int)
    output_df["Target"] = pd.to_numeric(output_df["Target"], errors="coerce").fillna(0).astype(int)
    output_df = output_df[columns].sort_values(
        ["Year", "Month", "TaskName", "TaskCadence"],
        ascending=[False, False, True, True],
        kind="stable",
    ).reset_index(drop=True)

    targets_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = targets_path.with_name(f"{targets_path.stem}.{uuid.uuid4().hex}.tmp")
    output_df.to_csv(tmp_path, sep="\t", index=False)
    tmp_path.replace(targets_path)


@st.cache_data(ttl=3600)
def load_user_fullname_map(tasks_xlsx_path: str | None = None) -> dict[str, str]:
    """
    Load mapping from user login to full name from startup output users.parquet.
    """
    users_parquet = Path(config.PERSONNEL_DIR) / "users.parquet"
    try:
        if not users_parquet.exists():
            get_page_logger("Shared Utilities").warning("users.parquet not found in Personnel directory.")
            return {}
        df = pd.read_parquet(users_parquet)
    except Exception as exc:
        get_page_logger("Shared Utilities").exception("Failed to read users.parquet: %s", exc)
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
def load_all_user_full_names(
    tasks_xlsx_path: str | None = None,
    department: str | None = None,
) -> list[str]:
    """Load full names from users.parquet, optionally filtered by department."""
    _ = tasks_xlsx_path  # Backward-compatible arg kept intentionally.
    try:
        users_parquet = Path(config.PERSONNEL_DIR) / "users.parquet"
        if not users_parquet.exists():
            get_page_logger("Shared Utilities").warning("users.parquet not found when loading full-name list.")
            return []
        df = pd.read_parquet(users_parquet)
    except Exception as exc:
        get_page_logger("Shared Utilities").exception("Failed to load all user full names: %s", exc)
        return []
    if df.empty:
        return []

    full_col = _find_column_by_alias(df, ["Full Name", "FullName", "fullname", "Name"])
    if not full_col:
        return []

    if department and str(department).strip():
        dept_col = _find_column_by_alias(df, ["Department", "Dept"])
        if dept_col:
            target_dept = str(department).strip().lower()
            dept_series = df[dept_col].fillna("").astype(str).str.strip().str.lower()
            df = df[dept_series == target_dept]
        else:
            get_page_logger("Shared Utilities").warning(
                "Department filter requested but no Department column found in users.parquet."
            )

    names = df[full_col].dropna().astype(str).str.strip()
    return sorted(n for n in names.unique() if n)


def _normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _find_column_by_alias(df: pd.DataFrame, aliases: list[str]) -> str | None:
    if df.empty:
        return None
    alias_set = {_normalize_column_name(a) for a in aliases}
    for col in df.columns:
        if _normalize_column_name(str(col)) in alias_set:
            return str(col)
    return None


def _coerce_bool_like(value: object) -> bool:
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
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off", ""}:
        return False
    return text in {"admin", "administrator"}


def _normalize_login_key(value: object) -> str:
    text = str(value).strip().lower()
    if not text:
        return ""
    text = text.replace("/", "\\")
    if "\\" in text:
        text = text.split("\\")[-1]
    if "@" in text:
        text = text.split("@")[0]
    return text.strip()


@st.cache_data(ttl=300)
def load_users_table() -> pd.DataFrame:
    """Load users.parquet from personnel storage."""
    users_parquet = Path(config.PERSONNEL_DIR) / "users.parquet"
    try:
        if not users_parquet.exists():
            get_page_logger("Shared Utilities").warning("users.parquet not found in Personnel directory.")
            return pd.DataFrame()
        return pd.read_parquet(users_parquet)
    except Exception as exc:
        get_page_logger("Shared Utilities").exception("Failed to read users.parquet: %s", exc)
        return pd.DataFrame()


def get_user_department(
    user_login: str,
    users_df: pd.DataFrame | None = None,
    full_name: str | None = None,
) -> str:
    """Return the department for a given user, or empty string if unknown."""
    lookup_user = str(user_login).strip().lower()
    lookup_key = _normalize_login_key(user_login)
    if not lookup_user:
        return ""

    def _resolve(df_local: pd.DataFrame) -> str:
        if df_local.empty:
            return ""

        user_col = _find_column_by_alias(
            df_local,
            ["User", "UserLogin", "Login", "Username", "User Name", "NetworkLogin", "SamAccountName"],
        )
        dept_col = _find_column_by_alias(df_local, ["Department", "Dept"])
        if not dept_col:
            return ""

        matched = pd.DataFrame()
        if user_col:
            user_series = df_local[user_col].astype(str).str.strip().str.lower()
            user_key_series = user_series.map(_normalize_login_key)
            matched = df_local[(user_series == lookup_user) | (user_key_series == lookup_key)]

        if matched.empty:
            email_col = _find_column_by_alias(df_local, ["Email", "EmailAddress", "E-mail"])
            if email_col:
                email_key_series = (
                    df_local[email_col].fillna("").astype(str).str.strip().str.lower().map(_normalize_login_key)
                )
                matched = df_local[email_key_series == lookup_key]

        if matched.empty and full_name and str(full_name).strip():
            full_col = _find_column_by_alias(df_local, ["Full Name", "FullName", "Name"])
            if full_col:
                lookup_name = str(full_name).strip().lower()
                name_series = df_local[full_col].fillna("").astype(str).str.strip().str.lower()
                matched = df_local[name_series == lookup_name]

        if matched.empty:
            return ""

        dept_series = matched[dept_col].dropna().astype(str).str.strip()
        dept_series = dept_series[dept_series != ""]
        if dept_series.empty:
            return ""
        return str(dept_series.iloc[0])

    df = users_df.copy() if isinstance(users_df, pd.DataFrame) else load_users_table()
    department = _resolve(df)
    if department:
        return department

    # If cache is stale or mismatched, retry with a direct parquet read.
    if users_df is None:
        try:
            users_parquet = Path(config.PERSONNEL_DIR) / "users.parquet"
            if users_parquet.exists():
                fresh_df = pd.read_parquet(users_parquet)
                return _resolve(fresh_df)
        except Exception:
            pass

    return ""


def is_user_admin(user_login: str, users_df: pd.DataFrame | None = None) -> bool:
    """Return True when the user is marked as admin in users.parquet."""
    lookup_user = str(user_login).strip().lower()
    lookup_key = _normalize_login_key(user_login)
    if not lookup_user:
        return False

    df = users_df.copy() if isinstance(users_df, pd.DataFrame) else load_users_table()
    if df.empty:
        return False

    user_col = _find_column_by_alias(
        df,
        ["User", "UserLogin", "Login", "Username", "User Name", "NetworkLogin", "SamAccountName"],
    )
    if not user_col:
        return False

    user_series = df[user_col].astype(str).str.strip().str.lower()
    user_key_series = user_series.map(_normalize_login_key)
    matched = df[(user_series == lookup_user) | (user_key_series == lookup_key)]
    if matched.empty:
        return False

    admin_col = _find_column_by_alias(
        matched,
        ["IsAdmin", "Admin", "Is Admin", "IsAdministrator", "TaskAdmin", "CanManageTasks"],
    )
    if admin_col:
        return bool(matched[admin_col].map(_coerce_bool_like).any())

    role_col = _find_column_by_alias(
        matched,
        ["Role", "UserRole", "Permission", "Permissions", "AccessLevel"],
    )
    if role_col:
        role_series = matched[role_col].fillna("").astype(str).str.strip().str.lower()
        return bool(role_series.isin({"admin", "administrator"}).any())

    for col in matched.columns:
        if "admin" in _normalize_column_name(str(col)):
            return bool(matched[col].map(_coerce_bool_like).any())

    return False


def is_current_user_admin() -> bool:
    """Return True when the current OS user is marked admin in users.parquet."""
    return is_user_admin(get_os_user())


@st.cache_data(ttl=3600)
def load_tasks(tasks_xlsx_path: str | None = None) -> pd.DataFrame:
    """Load active tasks from tasks.parquet managed by Tasks Management."""
    _ = tasks_xlsx_path  # Backward-compatible arg kept intentionally.
    try:
        tasks_parquet = Path(config.PERSONNEL_DIR) / "tasks.parquet"
        if not tasks_parquet.exists():
            get_page_logger("Shared Utilities").warning(
                "tasks.parquet not found in Personnel directory."
            )
            st.error("tasks.parquet not found in Personnel directory. Add tasks in Tasks Management.")
            return pd.DataFrame()
        df = pd.read_parquet(tasks_parquet)
    except Exception as e:
        get_page_logger("Shared Utilities").exception("Failed to read tasks.parquet: %s", e)
        st.error(f"Failed to read tasks.parquet: {e}")
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    definitions_df = _extract_task_definitions(df)
    if definitions_df.empty:
        return pd.DataFrame()

    if "IsActive" in definitions_df.columns:
        active_mask = definitions_df["IsActive"].map(_coerce_bool_like)
        definitions_df = definitions_df[active_mask].copy()

    definitions_df["TaskName"] = definitions_df["TaskName"].astype(str).str.strip()
    definitions_df["TaskCadence"] = definitions_df["TaskCadence"].astype(str).str.strip().str.title()
    return definitions_df.reset_index(drop=True)

@st.cache_data(ttl=3600)
def load_accounts(accounts_dir: str) -> list[str]:
    """Load list of Company Group accounts from cached accounts parquet file."""
    parquet_files = list(Path(accounts_dir).glob("accounts_*.parquet"))
    if not parquet_files:
        get_page_logger("Shared Utilities").info("No accounts parquet files found under '%s'.", accounts_dir)
        return []
    try:
        parquet_files.sort(reverse=True)
        df = pd.read_parquet(parquet_files[0], columns=["Company Group USE"])
        return df["Company Group USE"].dropna().astype(str).str.strip().unique().tolist()
    except Exception as exc:
        get_page_logger("Shared Utilities").exception("Failed to load accounts parquet: %s", exc)
        return []

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


def get_page_logger(page_name: str, source_file: str | None = None) -> logging.LoggerAdapter:
    """Return logger adapter for a specific page/source name."""
    clean_page_name = str(page_name).strip()
    if clean_page_name.lower().endswith(" page"):
        clean_page_name = clean_page_name[:-5].strip()
    if not clean_page_name:
        clean_page_name = "Page"
    if source_file is None:
        caller_frame = inspect.stack()[1]
        source_file = caller_frame.filename
    return app_logging.get_logger(source_file, clean_page_name)


def get_program_logger(source_file: str, context_name: str | None = None) -> logging.LoggerAdapter:
    """Return logger adapter for non-page modules/program entrypoints."""
    return app_logging.get_logger(source_file, context_name)


def log_page_open_once(page_key: str, logger: logging.LoggerAdapter) -> None:
    """Log one page-open event per Streamlit session."""
    state_key = f"_log_opened_{page_key}"
    if state_key in st.session_state:
        return
    st.session_state[state_key] = True
    logger.info("Page opened.")
