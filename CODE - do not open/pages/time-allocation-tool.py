"""
pages/time-allocation-tool.py

Purpose:
    Capture employee time-allocation entries by account/channel and export results.

What it does:
    - Input mode (all users):
        * Auto-fills User and Department
        * Writes one parquet file per user/day under year/month/user folders
    - Exports mode (admins only):
        * Loads saved parquet entries
        * Provides date/user filters and CSV export

Output schema:
    Entry Date | User | Full Name | Department | Account | Time | Channel
"""

from __future__ import annotations

import hashlib
import html
import json
import time
import uuid
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import streamlit as st
import streamlit.components.v1 as components
from streamlit_calendar import calendar as st_calendar

import config
import utils
import time_allocation_store as ta_store

LOGGER = utils.get_page_logger("Time Allocation Tool")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Time Allocation Tool")

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon=utils.get_app_icon())
utils.render_app_logo()
utils.log_page_open_once("time_allocation_tool_page", LOGGER)
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

_TA_COMPACT_CSS = """
<style>
/* --- Time Allocation: compact inputs ------------------------------------ */
[data-testid="stTextInput"] label,
[data-testid="stNumberInput"] label,
[data-testid="stDateInput"] label,
[data-testid="stSelectbox"] label,
[data-testid="stCheckbox"] label,
[data-testid="stTextArea"] label,
[data-testid="stRadio"] label {
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    margin-bottom: 0.15rem !important;
    padding-bottom: 0 !important;
}
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stDateInput"] input {
    font-size: 0.85rem !important;
    padding: 0.25rem 0.55rem !important;
    min-height: 34px !important;
}
[data-testid="stSelectbox"] [data-baseweb="select"] {
    font-size: 0.85rem !important;
}
[data-testid="stSelectbox"] [data-baseweb="select"] > div {
    min-height: 34px !important;
}
[data-testid="stTextArea"] textarea {
    font-size: 0.85rem !important;
    padding: 0.3rem 0.55rem !important;
}
/* Default bordered card (admin views, dialogs) — unchanged faint tint. Input-row cards
   are restyled below, scoped to the rows that carry .ta-row-marker. */
[data-testid="stVerticalBlockBorderWrapper"] {
    padding: 0.55rem 0.75rem !important;
    border-radius: 8px !important;
    background-color: rgba(0, 177, 158, 0.04);
}
/* --- Column header row above the entry cards: table-style head ----------- */
.ta-entry-col-header {
    font-family: var(--cna-body);
    font-size: 0.66rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: var(--cna-navy-soft);
    padding: 0 0.85rem 5px;
}
.ta-entry-pct {
    font-size: 0.95rem;
    font-weight: 600;
    color: rgba(49, 51, 63, 0.95);
    line-height: 1.0;
    padding-top: 0.55rem;
}
.ta-entry-pct-label {
    font-size: 0.72rem;
    color: rgba(49, 51, 63, 0.65);
}
/* --- Input-row cards (scoped via .ta-row-marker) -------------------------- */
/* Refined white card: hairline border, rounded, soft shadow, hover lift, plus a colored
   LEFT STATUS BAR — green = saved, amber = unsaved/draft (with a faint amber wash). */
[data-testid="stVerticalBlockBorderWrapper"]:has(.ta-row-marker) {
    position: relative;
    padding: 0.7rem 0.9rem !important;
    border: 1px solid var(--cna-rule) !important;
    border-left: 4px solid var(--cna-muted) !important;
    border-radius: 11px !important;
    background-color: var(--cna-white) !important;
    box-shadow: 0 2px 8px rgba(0, 46, 101, 0.06) !important;
    transition: box-shadow 0.15s ease, transform 0.15s ease, border-color 0.15s ease;
}
[data-testid="stVerticalBlockBorderWrapper"]:has(.ta-row-marker):hover {
    box-shadow: 0 6px 16px rgba(0, 46, 101, 0.12) !important;
    transform: translateY(-1px);
}
/* Saved row — green left bar */
[data-testid="stVerticalBlockBorderWrapper"]:has(.ta-row-saved) {
    border-left-color: var(--cna-green) !important;
}
/* Draft / unsaved row — amber left bar + faint wash to flag pending changes */
[data-testid="stVerticalBlockBorderWrapper"]:has(.ta-row-marker):not(:has(.ta-row-saved)) {
    border-left-color: #E0A100 !important;
    background-color: rgba(224, 161, 0, 0.05) !important;
}
/* Hide the row marker element itself (kept in DOM only for the :has() hooks above) */
[data-testid="stElementContainer"]:has(> [data-testid="stMarkdown"] .ta-row-marker) {
    display: none !important;
}
/* Green focus ring on the row's selects */
[data-testid="stVerticalBlockBorderWrapper"]:has(.ta-row-marker) [data-baseweb="select"] > div:focus-within {
    border-color: var(--cna-green) !important;
    box-shadow: 0 0 0 2px rgba(0, 177, 154, 0.18) !important;
}
/* Add Row — dashed "ghost" card (button restyle, scoped to its keyed container) */
.st-key-ta_add_row_wrap button {
    border: 1.5px dashed var(--cna-rule) !important;
    background: transparent !important;
    color: var(--cna-teal) !important;
    border-radius: 11px !important;
    padding: 0.55rem !important;
    font-weight: 600 !important;
    transition: background 0.15s ease, border-color 0.15s ease, color 0.15s ease;
}
.st-key-ta_add_row_wrap button:hover {
    background: rgba(0, 177, 154, 0.07) !important;
    border-color: var(--cna-green) !important;
    color: var(--cna-green) !important;
}
/* Timeframe-total callout — sits above the calendar's top-right corner (rendered into
   the header row's right column). Per-day totals still live in the calendar squares as
   ::after badges built in render_input_day_selector. */
.ta-hours-callout-wrap {
    display: flex;
    justify-content: flex-end;
    align-items: center;
    height: 100%;
}
.ta-hours-callout {
    display: inline-flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 2px;
    padding: 7px 16px;
    background: var(--cna-sky-lite);
    border: 1px solid var(--cna-rule);
    border-radius: 12px;
    box-shadow: 0 2px 8px rgba(0, 46, 101, 0.07);
}
.ta-hours-callout-label {
    font-family: var(--cna-body);
    font-size: 0.66rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: var(--cna-muted);
    line-height: 1;
}
.ta-hours-callout-value {
    font-family: var(--cna-heading);
    font-size: 1.55rem;
    font-weight: 700;
    line-height: 1.05;
    color: var(--cna-green);
}
/* Channel color legend shown under the calendar (only channels present in the view) */
.ta-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem 0.7rem;
    align-items: center;
    margin: 0.1rem 0 0.4rem;
    padding: 0 0.15rem;
}
.ta-legend-chip {
    display: inline-flex;
    align-items: center;
    font-size: 0.68rem;
    font-weight: 500;
    color: rgba(49, 51, 63, 0.75);
    white-space: nowrap;
}
.ta-legend-dot {
    width: 10px;
    height: 10px;
    border-radius: 3px;
    margin-right: 0.32rem;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.15);
}
/* --- Calendar view selector: themed segmented (pill) control ------------------
   Restyles the st.radio (inside the keyed container .st-key-ta_view_toggle) into a
   connected pill group with a green selected segment, matching the CNA theme. Scoped to
   the container class so no other radio is touched. The native radio circle is hidden;
   the <label> stays the click target (BaseWeb drives selection from the label). */
.st-key-ta_view_toggle {
    display: flex;
    justify-content: flex-start;
}
.st-key-ta_view_toggle [role="radiogroup"] {
    display: inline-flex;
    gap: 2px;
    background: var(--cna-sky-lite);
    border: 1px solid var(--cna-rule);
    border-radius: 999px;
    padding: 3px;
    box-shadow: inset 0 1px 2px rgba(0, 46, 101, 0.06);
}
.st-key-ta_view_toggle [role="radiogroup"] > label {
    margin: 0 !important;
    padding: 5px 16px !important;
    gap: 0 !important;
    border-radius: 999px;
    cursor: pointer;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    text-align: center;
    font-family: var(--cna-body);
    font-size: 0.8rem;
    font-weight: 600;
    line-height: 1.2;
    white-space: nowrap;
    transition: background 0.15s ease, color 0.15s ease, box-shadow 0.15s ease;
}
/* Hide the native radio circle (first child) */
.st-key-ta_view_toggle [role="radiogroup"] > label > div:first-child {
    display: none !important;
}
/* Keep nested label text sized to the segment */
.st-key-ta_view_toggle [role="radiogroup"] > label * {
    font-size: inherit !important;
    font-weight: inherit !important;
}
/* Unselected segment text — navy on the light track */
.st-key-ta_view_toggle [role="radiogroup"] > label,
.st-key-ta_view_toggle [role="radiogroup"] > label * {
    color: var(--cna-navy-soft) !important;
}
/* Hover (unselected) — green text + faint green tint */
.st-key-ta_view_toggle [role="radiogroup"] > label:hover {
    background: rgba(0, 177, 154, 0.10);
}
.st-key-ta_view_toggle [role="radiogroup"] > label:hover,
.st-key-ta_view_toggle [role="radiogroup"] > label:hover * {
    color: var(--cna-green) !important;
}
/* Selected segment — filled CNA green */
.st-key-ta_view_toggle [role="radiogroup"] > label:has(input:checked) {
    background: var(--cna-green);
    box-shadow: 0 2px 6px rgba(0, 177, 154, 0.30);
}
/* Selected segment text — white (wins over unselected + hover via higher specificity) */
.st-key-ta_view_toggle [role="radiogroup"] > label:has(input:checked),
.st-key-ta_view_toggle [role="radiogroup"] > label:has(input:checked) *,
.st-key-ta_view_toggle [role="radiogroup"] > label:has(input:checked):hover,
.st-key-ta_view_toggle [role="radiogroup"] > label:has(input:checked):hover * {
    color: var(--cna-white) !important;
}
</style>
"""
st.markdown(_TA_COMPACT_CSS, unsafe_allow_html=True)

# Raw CSS (no <style> tags) injected into the calendar component via st_calendar's
# `custom_css` so it lands *inside* the component iframe.
#
# Why this exists: streamlit_calendar calls Streamlit.setFrameHeight() only once on
# mount, so the iframe height is locked to the calendar's initial height. FullCalendar's
# default "+X more" popover is position:absolute aligned to the clicked cell with no
# max-height, so on a day with many entries it grows past that fixed iframe boundary, gets
# clipped, and — not being scrollable — the hidden entries are unreachable.
#
# IMPORTANT: do NOT switch this to position:fixed. A fixed popover has offsetParent === null,
# and FullCalendar's popover updateSize() does `this.offsetParent.getBoundingClientRect()`
# in componentDidMount — with a null offsetParent that throws, which corrupts the popover's
# mount and silently kills its close affordances (the X button, click-outside, and Escape all
# stop working, so the popover can't be dismissed). See the close-button bug fixed here.
#
# Instead keep position:absolute but pin the popover to the top of the calendar's
# .fc-view-harness (its offsetParent, which spans the full grid height) via top/left/right
# with !important, and size it with % (relative to that harness) so it always fits inside the
# locked-height iframe no matter which day — top row or bottom row — was clicked. Its body
# scrolls so every entry is reachable, and the native close button keeps working.
_CALENDAR_MORE_POPOVER_CSS = """
.fc .fc-popover.fc-more-popover {
    position: absolute !important;
    top: 8px !important;
    left: 8px !important;
    right: 8px !important;
    bottom: auto !important;
    width: auto !important;
    max-width: calc(100% - 16px) !important;
    max-height: calc(100% - 16px) !important;
    display: flex !important;
    flex-direction: column !important;
    overflow: hidden !important;
    z-index: 10000 !important;
    border-radius: 8px !important;
    box-shadow: 0 6px 24px rgba(0, 0, 0, 0.22) !important;
}
.fc .fc-popover.fc-more-popover .fc-popover-header {
    flex: 0 0 auto !important;
    padding: 6px 10px !important;
    font-weight: 600 !important;
}
.fc .fc-popover.fc-more-popover .fc-popover-body {
    flex: 1 1 auto !important;
    min-height: 0 !important;
    overflow-y: auto !important;
    padding: 6px 8px !important;
}
.fc .fc-popover.fc-more-popover .fc-daygrid-event {
    white-space: normal !important;
}
"""

TIME_ALLOCATION_DIR = config.TIME_ALLOCATION_DIR
PERSONNEL_DIR = config.PERSONNEL_DIR
# Machine-local, shared-by-all-users favorites for the Reporting Name dropdown.
# Lives next to app.py's page favorites (the "CODE - do not open" app dir, one
# level up from pages/). Deliberately NOT on the network share: favorites are a
# convenience, and keeping them local keeps every dropdown rerun off the network.
ACCOUNT_FAVORITES_FILE = Path(__file__).resolve().parent.parent / "ta_account_favorites.json"
# The canonical channel set, in a fixed display order shown verbatim in every
# Channel dropdown. (Previously the dropdown re-sorted by usage frequency once
# enough history existed, which required a full scan of every saved file on each
# cold load; that ordering was dropped in favor of this fixed order.)
CHANNEL_OPTIONS = [
    "Resupply",
    "Consolidated: Smallwares",
    "Consolidated: Equipment",
    "Consolidated: Rollout",
    "Consolidated: Full",
    "Express: Smallwares",
    "Express: Equipment",
    "Express: Full",
]

# Calendar event colors grouped by channel TYPE — the trailing word of the channel
# (after the colon), or the whole name for Resupply. Five on-brand buckets so the grid
# reads by work type at a glance, regardless of the Consolidated/Express prefix:
#   Resupply / Smallwares / Equipment / Rollout / Full.
# The calendar runs in an iframe that can't see the app's --cna-* CSS variables, so these
# are spelled out as literal hex. Drawn from the CNA palette (Green / Turquoise / Teal /
# steel-blue / Navy). (background, text) pairs; text is chosen for contrast.
CHANNEL_TYPE_COLORS: dict[str, tuple[str, str]] = {
    "Resupply": ("#00B19A", "#ffffff"),
    "Smallwares": ("#08B4C5", "#ffffff"),
    "Equipment": ("#06828D", "#ffffff"),
    "Rollout": ("#3F6FB0", "#ffffff"),
    "Full": ("#002E65", "#ffffff"),
}
# Fallback for blank / legacy channel values that match no known type bucket.
CHANNEL_COLOR_DEFAULT: tuple[str, str] = ("#5B6B7B", "#ffffff")


def _channel_type(channel: object) -> str:
    """Classify a channel into its color-bucket key. Returns the segment after the last
    colon ('Consolidated: Full' -> 'Full', 'Express: Smallwares' -> 'Smallwares') or the
    whole name when there's no colon ('Resupply'), provided it's a known bucket; else ''.
    """
    text = str(channel or "").strip()
    if not text:
        return ""
    tail = text.split(":")[-1].strip()
    return tail if tail in CHANNEL_TYPE_COLORS else ""


def _channel_color(channel: object) -> tuple[str, str]:
    """Return the (background, text) color pair for a channel value, bucketed by type."""
    return CHANNEL_TYPE_COLORS.get(_channel_type(channel), CHANNEL_COLOR_DEFAULT)

# Hour/minute dropdown options for the per-row Time input.
TIME_HOUR_OPTIONS = list(range(0, 13))
TIME_MINUTE_OPTIONS = [0, 15, 30, 45]

_TIME_ALLOCATION_BASE_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("Entry Date", pa.date32()),
    ("User", pa.string()),
    ("Full Name", pa.string()),
    ("Department", pa.string()),
    ("Account", pa.string()),
    ("Customer Code", pa.string()),
    ("Time", pa.string()),
    ("Channel", pa.string()),
)


def _arrow_type_for_field(field_type: str) -> pa.DataType:
    """Map an entry-field type to its pyarrow column type."""
    if field_type == "number":
        return pa.float64()
    if field_type == "date":
        return pa.date32()
    return pa.string()


def _current_time_allocation_schema() -> pa.Schema:
    """Build the parquet schema from current entry-field definitions (one cf_<id> column per field)."""
    fields = [pa.field(name, dtype) for name, dtype in _TIME_ALLOCATION_BASE_FIELDS]
    for entry_field in _load_entry_fields():
        col = f"cf_{entry_field['id']}"
        fields.append(pa.field(col, _arrow_type_for_field(entry_field["type"])))
    return pa.schema(fields)


def _empty_to_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _get_time_allocation_user_partition(user_login: str, full_name: str = "") -> str:
    """Return a stable folder-safe user key for time-allocation storage."""
    return ta_store.user_partition(user_login, full_name)


def _get_time_allocation_month_dir(base_dir: Path, entry_date: date) -> Path:
    """Return the year/month partition directory for an entry date."""
    return ta_store.month_dir(base_dir, entry_date)


def _get_time_allocation_daily_file(base_dir: Path, user_login: str, full_name: str, entry_date: date) -> Path:
    """Return the one-file-per-day parquet path for a user/date."""
    return ta_store.daily_file(base_dir, user_login, full_name, entry_date)


def _iter_time_allocation_files(base_dir: Path) -> list[Path]:
    """Return all saved time-allocation parquet files, including nested partitions."""
    if not base_dir.exists():
        return []
    return sorted((path for path in base_dir.rglob("*.parquet") if path.is_file()), reverse=True)


def _iter_user_day_candidate_files(
    base_dir: Path,
    user_login: str,
    full_name: str,
    entry_date: date,
) -> list[Path]:
    """Candidate parquet files for one specific user/date — targets the known per-user file path directly instead of a recursive month-dir scan, which is materially faster over UNC."""
    files: list[Path] = []
    seen: set[str] = set()

    user_path = _get_time_allocation_daily_file(base_dir, user_login, full_name, entry_date)
    if user_path.is_file():
        files.append(user_path)
        seen.add(str(user_path).lower())

    legacy_pattern = f"time_allocation_{entry_date:%Y%m%d}*.parquet"
    for path in sorted(base_dir.glob(legacy_pattern), reverse=True):
        path_key = str(path).lower()
        if path.is_file() and path_key not in seen:
            files.append(path)
            seen.add(path_key)

    return files


def _iter_user_window_candidate_files(
    base_dir: Path,
    user_login: str,
    full_name: str,
    window_start: date,
    window_end: date,
) -> list[Path]:
    """Candidate parquet files for one user across a date window (see time_allocation_store)."""
    return ta_store.iter_user_window_candidate_files(
        base_dir, user_login, full_name, window_start, window_end
    )


def _read_time_allocation_exports_from_files(file_paths: list[Path], base_dir: Path) -> pd.DataFrame:
    """Read and normalize saved time-allocation files into one DataFrame."""
    return ta_store.read_exports_from_files(file_paths, base_dir)


# How many per-user-day files the dataset scanner reads concurrently. The export
# load is latency-bound (one tiny parquet per user per day over a UNC share), so a
# high readahead hides per-file round-trip latency — measured ~5x faster than the
# sequential per-file reader on the real share.
_EXPORTS_FRAGMENT_READAHEAD = 48


# Initial admin tables load only this many most-recent rows so the open is cheap and
# (because dataset.head() stops early) stays roughly constant-time as history grows.
_EXPORTS_RECENT_LIMIT = 100
# Files-per-batch when reading a filtered window with a progress bar.
_EXPORTS_WINDOW_CHUNK = 40


def _export_base_schema() -> pa.Schema:
    """The 8 base export columns as an explicit pyarrow schema (drives every scan)."""
    return pa.schema([pa.field(name, dtype) for name, dtype in _TIME_ALLOCATION_BASE_FIELDS])


def _normalize_export_frame(raw_df: pd.DataFrame, base_dir: Path) -> pd.DataFrame:
    """Turn a raw dataset-scan frame (8 base cols + ``__filename``) into the canonical
    export frame: add the base-relative ``Source File`` string the per-file loader
    produced (the Edit Entries save indexes back into each file by it, so it must
    match exactly), normalize Entry Date, and order columns. NOT sorted — callers
    sort as needed."""
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    base_str = str(base_dir)

    def _source_file(filename: object) -> str:
        text = str(filename or "")
        if not text:
            return ""
        try:
            return str(Path(text).relative_to(base_dir))
        except ValueError:
            normalized = text.replace("/", "\\")
            base_normalized = base_str.replace("/", "\\")
            if normalized.lower().startswith(base_normalized.lower()):
                return normalized[len(base_normalized):].lstrip("\\/")
            return Path(text).name

    df = raw_df.copy()
    df["Source File"] = df["__filename"].map(_source_file) if "__filename" in df.columns else ""
    df = df.drop(columns=["__filename"], errors="ignore")
    expected_cols = list(_export_base_schema().names)
    df["Entry Date"] = pd.to_datetime(df["Entry Date"], errors="coerce").dt.date
    return df[expected_cols + ["Source File"]]


def _read_export_files(base_dir: Path, files: list[Path]) -> pd.DataFrame:
    """Read an explicit list of *.parquet files in ONE concurrent dataset scan and
    return the normalized (unsorted) export frame.

    An explicit base schema is passed so a file missing a base column (e.g. an old one
    written before ``Customer Code`` existed) null-fills it instead of the column being
    dropped, and so schema inference doesn't depend on the first-discovered file. The
    file set is explicit (not a directory scan) so a stray non-parquet file in the tree
    — a synced-share desktop.ini/Thumbs.db, or a leftover *.parquet.tmp from an
    interrupted atomic write — can't make the scan raise.
    """
    if not files:
        return pd.DataFrame()
    base_schema = _export_base_schema()
    dataset = ds.dataset([str(path) for path in files], format="parquet", schema=base_schema)
    table = dataset.scanner(
        columns=list(base_schema.names) + ["__filename"],
        use_threads=True,
        fragment_readahead=_EXPORTS_FRAGMENT_READAHEAD,
        batch_readahead=_EXPORTS_FRAGMENT_READAHEAD,
    ).to_table()
    return _normalize_export_frame(table.to_pandas(), base_dir)


def _read_exports_via_dataset(base_dir: Path) -> pd.DataFrame:
    """Fast path for ``load_time_allocation_exports``: read ALL files in one scan,
    ordered like the per-file loader (files reverse-sorted; within-file order
    preserved, which ``_source_pos`` relies on). Raises on failure so the caller can
    fall back to the resilient per-file reader."""
    files = _iter_time_allocation_files(base_dir)
    if not files:
        return pd.DataFrame()
    df = _read_export_files(base_dir, files)
    if df.empty:
        return df
    return df.sort_values("Source File", ascending=False, kind="stable").reset_index(drop=True)


def _date_from_ta_filename(path: Path) -> date | None:
    """Parse the entry date from a ``time_allocation_YYYYMMDD[...].parquet`` filename.

    Each saved file holds one user's entries for one day, so its filename date IS the
    Entry Date of every row in it — letting a date-window filter prune at the FILE
    level (no read) before any data is loaded."""
    stem = Path(path).stem
    prefix = "time_allocation_"
    if not stem.startswith(prefix):
        return None
    token = stem[len(prefix):len(prefix) + 8]
    if len(token) < 8 or not token.isdigit():
        return None
    try:
        return date(int(token[:4]), int(token[4:6]), int(token[6:8]))
    except ValueError:
        return None


def _iter_time_allocation_files_in_window(
    base_dir: Path, window_start: date, window_end: date
) -> list[Path]:
    """Files whose filename date falls within [window_start, window_end].

    Files whose date can't be parsed are included (conservative — they're filtered
    by Entry Date in memory afterward), so no rows are ever dropped at this stage."""
    out: list[Path] = []
    for path in _iter_time_allocation_files(base_dir):
        day = _date_from_ta_filename(path)
        if day is None or (window_start <= day <= window_end):
            out.append(path)
    return out


def _read_export_files_with_progress(
    base_dir: Path, files: list[Path], progress: object = None, chunk: int = _EXPORTS_WINDOW_CHUNK
) -> pd.DataFrame:
    """Read ``files`` in batches, advancing an optional st.progress bar per batch, so a
    filtered-window load shows real progress. Each batch is itself a concurrent scan."""
    if not files:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    total = len(files)
    for start in range(0, total, max(1, chunk)):
        batch = files[start:start + max(1, chunk)]
        batch_df = _read_export_files(base_dir, batch)
        if not batch_df.empty:
            frames.append(batch_df)
        if progress is not None:
            done = min(start + max(1, chunk), total)
            try:
                progress.progress(done / total, text=f"Loading entries… {done}/{total} files")
            except Exception:
                pass
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


@st.cache_data(ttl=30, show_spinner="Loading recent entries...")
def _load_recent_exports(base_dir: Path, limit: int = _EXPORTS_RECENT_LIMIT) -> pd.DataFrame:
    """Load only the ``limit`` most-recent entry rows for the initial table view.

    Uses ``dataset.head(limit)`` over the newest-first file list, which STOPS reading
    once it has ``limit`` rows — so it touches only a handful of files regardless of
    how much total history exists (roughly constant-time open). Result is sorted by
    Entry Date descending. Falls back to truncating the full load if head() fails."""
    files = _iter_time_allocation_files(base_dir)
    if not files:
        return pd.DataFrame()
    try:
        base_schema = _export_base_schema()
        dataset = ds.dataset([str(path) for path in files], format="parquet", schema=base_schema)
        table = dataset.head(
            int(limit), columns=list(base_schema.names) + ["__filename"], use_threads=True
        )
        df = _normalize_export_frame(table.to_pandas(), base_dir)
    except Exception as exc:
        LOGGER.warning("Recent-exports head() failed; falling back to full load: %s", exc)
        df = load_time_allocation_exports(base_dir)
    if df.empty:
        return df
    return (
        df.sort_values("Entry Date", ascending=False, kind="stable")
        .head(int(limit))
        .reset_index(drop=True)
    )


def _load_exports_window(
    base_dir: Path, window_start: date, window_end: date, progress: object = None
) -> pd.DataFrame:
    """Load only the files whose date falls in [window_start, window_end], with an
    optional progress bar. Sorted like the full loader (Source File desc, within-file
    order preserved for ``_source_pos``)."""
    files = _iter_time_allocation_files_in_window(base_dir, window_start, window_end)
    df = _read_export_files_with_progress(base_dir, files, progress=progress)
    if df.empty:
        return df
    return df.sort_values("Source File", ascending=False, kind="stable").reset_index(drop=True)


@st.cache_data(ttl=30, show_spinner="Loading saved allocations...")
def load_time_allocation_exports(base_dir: Path) -> pd.DataFrame:
    """Load all saved time-allocation parquet files from the output directory.

    Tries the fast single-scan path first (``_read_exports_via_dataset``) and falls
    back to the resilient per-file reader if it errors or finds nothing — so
    correctness never depends on the fast path, only speed does.
    """
    try:
        fast_df = _read_exports_via_dataset(base_dir)
        if not fast_df.empty:
            return fast_df
    except Exception as exc:
        LOGGER.warning(
            "Fast dataset export load failed; falling back to per-file reader: %s", exc
        )
    return _read_time_allocation_exports_from_files(_iter_time_allocation_files(base_dir), base_dir)


def _invalidate_admin_export_tables() -> None:
    """Clear the cached export loaders AND any per-session windowed data so the admin
    Exports / Edit Entries tables reflect a fresh save or delete on the next render.

    Call this everywhere the old code cleared ``load_time_allocation_exports`` — the
    new top-100 (``_load_recent_exports``) cache and the windowed frames stashed in
    session state must be invalidated together or the tables would show stale rows.
    """
    load_time_allocation_exports.clear()
    _load_recent_exports.clear()
    for prefix in ("ta_export", "ta_admin_editor"):
        st.session_state.pop(f"{prefix}_loaded_sig", None)
        st.session_state.pop(f"{prefix}_loaded_df", None)


def _normalize_login(value: object) -> str:
    return ta_store.normalize_login(value)


def _filter_user_exports(exports_df: pd.DataFrame, user_login: str, full_name: str) -> pd.DataFrame:
    """Return exports filtered to the current user by login and full-name fallback."""
    return ta_store.filter_user_exports(exports_df, user_login, full_name)


@st.cache_data(ttl=30, show_spinner=False)
def load_time_allocation_user_window(
    base_dir: Path,
    user_login: str,
    full_name: str,
    window_start_iso: str,
    window_end_iso: str,
) -> pd.DataFrame:
    """Load one user's saved allocations for a bounded date window."""
    window_start = _parse_date_value(window_start_iso)
    window_end = _parse_date_value(window_end_iso)
    if window_start is None or window_end is None:
        return pd.DataFrame()
    return ta_store.load_user_window(base_dir, user_login, full_name, window_start, window_end)


def _coerce_account(value: object, account_options: list[str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for option in account_options:
        option_text = str(option or "").strip()
        if option_text.lower() == text.lower():
            return option_text
    # Preserve unknown historical account values so loaded rows don't blank out.
    return text


def _account_options_for_row(account_options: list[str], current_value: object) -> list[str]:
    """Ensure the current row value exists in selectbox options."""
    options = [str(opt or "").strip() for opt in account_options]
    current = str(current_value or "").strip()
    if not current:
        return options
    if any(opt.lower() == current.lower() for opt in options):
        return options
    if options and options[0] == "":
        return [options[0], current, *options[1:]]
    return [current, *options]


# _account_options_for_row works for any "blank-first" option list (Reporting
# Name or Customer Code); kept as a named alias for call-site readability.
_customer_code_options_for_row = _account_options_for_row


def _customer_code_pool_for_reporting_name(account_lookup: dict, reporting_name: object) -> list[str]:
    """Customer Codes to offer in a row's dropdown, given its selected Reporting Name.

    When a Reporting Name is selected, restrict the Customer Code dropdown to just
    that name's codes (the row's autofilled first code is always among them). With
    no Reporting Name selected, fall back to every customer code so the dropdown can
    still drive the reverse autofill.
    """
    name = str(reporting_name or "").strip()
    if name:
        codes = account_lookup.get("rn_to_codes", {}).get(name)
        if codes:
            return list(codes)
    return list(account_lookup["customer_codes"])


# -----------------------------------------------------------------
# Reporting Name favorites (machine-local, shared by all users)
# -----------------------------------------------------------------
def _load_account_favorites() -> list[str]:
    """Load the machine-local list of favorited Reporting Names (empty on any error)."""
    try:
        if ACCOUNT_FAVORITES_FILE.exists():
            data = json.loads(ACCOUNT_FAVORITES_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [str(x or "").strip() for x in data if str(x or "").strip()]
    except Exception:
        LOGGER.warning("Could not read account favorites file", exc_info=True)
    return []


def _save_account_favorites(favs: list[str]) -> None:
    """Persist the favorited Reporting Names list (best-effort; never raises to UI)."""
    try:
        ACCOUNT_FAVORITES_FILE.write_text(json.dumps(favs, indent=2), encoding="utf-8")
    except Exception:
        LOGGER.warning("Could not write account favorites file", exc_info=True)


def _account_favorites_list() -> list[str]:
    """Favorited Reporting Names, read from disk once per session into session_state.

    Caching the list in session_state keeps the per-rerun dropdown reorder off the
    filesystem entirely; the star toggle keeps this copy and the file in sync.
    """
    favs = st.session_state.get("ta_account_favorites")
    if favs is None:
        favs = _load_account_favorites()
        st.session_state["ta_account_favorites"] = favs
    return favs


def _toggle_account_favorite(name: str) -> None:
    """Star/unstar a Reporting Name, persisting to disk and the session copy."""
    name = str(name or "").strip()
    if not name:
        return
    favs = list(_account_favorites_list())
    if name in favs:
        favs.remove(name)
    else:
        favs.append(name)
    _save_account_favorites(favs)
    st.session_state["ta_account_favorites"] = favs


def _favorites_first(options: list[str], favorites: set[str]) -> list[str]:
    """Pull favorited values to the top of a blank-first option list.

    The leading blank ("" = no selection) stays first; favorited options follow,
    then the rest. Order *within* each group is preserved, so the base alphabetical
    sort still holds inside favorites and inside the remainder. Favorites no longer
    present in ``options`` simply don't appear, so stale stars drop off silently.
    Pure list ops; runs once per rerun for the shared option list (not per row).
    """
    if not favorites:
        return options
    favs = [o for o in options if o != "" and o in favorites]
    if not favs:
        return options
    blank = [o for o in options if o == ""]
    rest = [o for o in options if o != "" and o not in favorites]
    return blank + favs + rest


# Reporting names consolidated into a single dropdown entry for this tool. The
# merged name offers the union of the originals' Customer Codes; existing saved
# entries keep their original names (preserved by _coerce_account on load).
_REPORTING_NAME_MERGES = {
    "Domino's Canada": "Domino's",
    "Domino's Domestic": "Domino's",
    "Domino's International": "Domino's",
}


def _build_account_lookup(lookup_df: pd.DataFrame) -> dict:
    """
    Build the Customer Code <-> Reporting Name lookup structures from the
    accounts parquet DataFrame (columns: CustomerCode, ReportingName).

    Returns a dict with:
        reporting_names    - sorted unique Reporting Name values
        customer_codes     - sorted unique Customer Code values
        rn_to_first_code   - Reporting Name -> first (alphabetical) Customer Code
        rn_to_codes        - Reporting Name -> sorted list of its Customer Codes
        code_to_rn         - Customer Code -> Reporting Name
    """
    # Built with vectorized pandas ops rather than a per-row ``iterrows()`` loop
    # (which re-ran over ~15k rows on every cold lookup build). Output is identical
    # to the old loop for any input: each cell is normalized with the same
    # ``str(x or "").strip()`` expression (kept as a per-cell map so the result is
    # stable across pandas versions, whose ``astype(str)`` differ on how they
    # render NaN), and the grouping preserves the original tiebreaks — every
    # non-blank Reporting Name is offered even if it has no codes, and a Customer
    # Code maps to the Reporting Name of its FIRST occurrence (original row order)
    # among rows where both are present.
    if lookup_df is None or lookup_df.empty:
        rn_series = pd.Series([], dtype="object")
        code_series = pd.Series([], dtype="object")
    else:
        # A missing column behaves like the old ``row.get`` returning None -> "".
        rn_series = (
            lookup_df["ReportingName"].map(lambda x: str(x or "").strip())
            if "ReportingName" in lookup_df.columns
            else pd.Series("", index=lookup_df.index, dtype="object")
        )
        code_series = (
            lookup_df["CustomerCode"].map(lambda x: str(x or "").strip())
            if "CustomerCode" in lookup_df.columns
            else pd.Series("", index=lookup_df.index, dtype="object")
        )

    # Consolidate any merged reporting names (e.g. the Domino's sub-accounts) before
    # building the lookup, so the dropdown shows the single merged name and its
    # autofill/validation pool is the union of the originals' Customer Codes.
    if _REPORTING_NAME_MERGES:
        rn_series = rn_series.map(lambda n: _REPORTING_NAME_MERGES.get(n, n))

    norm = pd.DataFrame({"rn": rn_series.to_numpy(), "code": code_series.to_numpy()})
    has_rn = norm["rn"] != ""
    has_code = norm["code"] != ""

    # Every non-blank Reporting Name is offered, even one whose rows carry no code.
    reporting_names = sorted(norm.loc[has_rn, "rn"].unique().tolist())

    # Only rows with BOTH a name and a code drive the mappings; the boolean mask
    # preserves original row order so "first occurrence wins" for code -> name.
    both = norm[has_rn & has_code]
    rn_to_codes = {
        name: sorted(pd.unique(codes).tolist())
        for name, codes in both.groupby("rn", sort=False)["code"]
    }
    rn_to_first_code = {name: codes[0] for name, codes in rn_to_codes.items()}
    code_first = both.drop_duplicates(subset="code", keep="first")
    code_to_rn = dict(zip(code_first["code"], code_first["rn"]))

    return {
        "reporting_names": reporting_names,
        "customer_codes": sorted(code_to_rn.keys()),
        "rn_to_first_code": rn_to_first_code,
        "rn_to_codes": rn_to_codes,
        "code_to_rn": code_to_rn,
    }


@st.cache_data(ttl=3600, show_spinner=False)
def _account_lookup_for_dir(accounts_dir: str) -> dict:
    """Cached Customer Code <-> Reporting Name lookup for an accounts directory.

    `utils.load_account_lookup` already caches the parquet read, but building the
    lookup dicts on top of it iterates ~15k rows and used to re-run on every
    full-script rerun because it sat at module scope. Caching it here on the cheap
    directory-string key means that build runs about once an hour instead of on
    every Add Row / calendar click / page rerun. The returned dict adds a
    ``loaded_at`` timestamp but is otherwise identical, so the Reporting Name
    <-> Customer Code autofill is unchanged.
    """
    lookup = _build_account_lookup(utils.load_account_lookup(accounts_dir))
    # Stamp when this lookup was actually (re)built. Because the value is cached,
    # this is the true data-load time: it survives full page reloads (the cache
    # is server-side) and advances only when the cache is cleared (the Refresh Account
    # Data button) or the TTL expires, so the "time since last refresh" line reflects
    # real staleness instead of resetting to "just now" on every rerun.
    lookup["loaded_at"] = utils.now_utc()
    return lookup


def _row_selection_valid(account_lookup: dict, reporting_name: object, customer_code: object) -> bool:
    """True if a row's Reporting Name / Customer Code pairing is representable in this account data.

    A blank Reporting Name is always valid. A non-blank one must exist, and any
    chosen Customer Code must belong to it (or, with no Reporting Name, must exist
    among all codes). Used by the Refresh Account Data button to decide whether a
    refresh invalidated a selection — i.e. whether the source changed under the
    user's choice — so untouched/still-valid selections are never disturbed.
    """
    rn = str(reporting_name or "").strip()
    code = str(customer_code or "").strip()
    if rn:
        if rn not in set(account_lookup.get("reporting_names", [])):
            return False
        if code and code not in set(account_lookup.get("rn_to_codes", {}).get(rn, [])):
            return False
        return True
    if code and code not in set(account_lookup.get("customer_codes", [])):
        return False
    return True


def _split_duration_to_hm(value: object) -> tuple[int, int]:
    """Parse an HH:MM(:SS) duration string into (hour, minute) for the row dropdowns."""
    seconds = max(0, utils.parse_hhmmss(str(value or "")))
    hour = min(max(seconds // 3600, 0), TIME_HOUR_OPTIONS[-1])
    remainder_minutes = (seconds % 3600) // 60
    # Snap to the nearest configured 15-minute interval.
    minute = min(TIME_MINUTE_OPTIONS, key=lambda m: abs(m - remainder_minutes))
    return int(hour), int(minute)


def _hm_to_duration(hour: object, minute: object) -> str:
    """Combine hour/minute dropdown values into the canonical HH:MM string."""
    try:
        hour_int = max(0, int(hour))
    except (TypeError, ValueError):
        hour_int = 0
    try:
        minute_int = max(0, int(minute))
    except (TypeError, ValueError):
        minute_int = 0
    return f"{hour_int:02d}:{minute_int:02d}"


def _on_reporting_name_change(idx: int, lookup: dict) -> None:
    """Autofill the row's Customer Code when the Reporting Name selection changes."""
    reporting_name = str(st.session_state.get(f"ta_detailed_account_{idx}", "")).strip()
    st.session_state[f"ta_detailed_custcode_{idx}"] = lookup["rn_to_first_code"].get(reporting_name, "")


def _on_customer_code_change(idx: int, lookup: dict) -> None:
    """Autofill the row's Reporting Name when the Customer Code selection changes."""
    customer_code = str(st.session_state.get(f"ta_detailed_custcode_{idx}", "")).strip()
    reporting_name = lookup["code_to_rn"].get(customer_code, "")
    if reporting_name:
        st.session_state[f"ta_detailed_account_{idx}"] = reporting_name


def _effective_customer_code(reporting_name: object, customer_code: object) -> str:
    """The Customer Code to persist for a row, defaulting to the Reporting Name's
    first code when the row's code is blank.

    Any code under the correct Reporting Name is acceptable, so this guarantees a
    non-blank code is saved whenever the name has one — closing the gap that produced
    blank-code entries (the autofill only fires on a *fresh* dropdown change, so
    pre-filled/copied rows, or names that were uncoded in the accounts file at entry
    time, slipped through with no code). A name absent from the current accounts data
    stays blank (there is nothing to fill from)."""
    code = str(customer_code or "").strip()
    if code:
        return code
    name = str(reporting_name or "").strip()
    if not name:
        return ""
    lookup = _account_lookup_for_dir(str(PERSONNEL_DIR))
    return lookup.get("rn_to_first_code", {}).get(name, "")


def _reporting_name_to_code_map() -> dict[str, str]:
    """Reporting Name -> Customer Code to backfill onto blank-code entries.

    This is the same source `_effective_customer_code` fills from at save time
    (each name's first/alphabetical code), so the one-off cleanup assigns exactly
    what a fresh save would. It additionally maps any pre-merge Reporting Name
    (e.g. "Domino's Canada") to the merged name's code, so legacy rows that still
    carry an original name are filled too."""
    lookup = _account_lookup_for_dir(str(PERSONNEL_DIR))
    rn_to_first_code = dict(lookup.get("rn_to_first_code", {}))
    for original, merged in _REPORTING_NAME_MERGES.items():
        if original not in rn_to_first_code and merged in rn_to_first_code:
            rn_to_first_code[original] = rn_to_first_code[merged]
    return rn_to_first_code


def _parse_date_value(value: object) -> date | None:
    """Safely parse date-like values into a date object."""
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _today_eastern() -> date:
    """Return today's date in Eastern time."""
    return utils.to_eastern(utils.now_utc()).date()


def _work_week_bounds(reference: date) -> tuple[date, date]:
    """Return (Monday, Friday) of the work week containing `reference`."""
    monday = reference - timedelta(days=reference.weekday())
    return monday, monday + timedelta(days=4)


def _editable_window() -> tuple[date, date]:
    """
    The span of days users can add to or edit on the Input tab: last week's
    Monday through this week's Friday — i.e. the days reachable from the
    This Week and Last Week calendar views.
    """
    today = _today_eastern()
    this_monday, this_friday = _work_week_bounds(today)
    return this_monday - timedelta(days=7), this_friday


def _is_editable_day(day: date) -> bool:
    """
    Editing is allowed for days inside the editable window (this week and last
    week). Admins bypass this on the Input tab; the admin Edit Entries table
    can still change any date.
    """
    if utils.is_current_user_admin():
        return True
    window_start, window_end = _editable_window()
    return window_start <= day <= window_end


@st.cache_data(ttl=30)
def _build_calendar_events(
    window_rows: tuple[tuple[str, str, int, str], ...],
    compact: bool = False,
) -> list[dict[str, object]]:
    """Build calendar event payload from normalized window rows.

    Each event is colored by its channel TYPE (see ``CHANNEL_TYPE_COLORS``) so the grid
    reads by work type at a glance. In ``compact`` mode every event becomes a small
    textless color dot — useful for the multi-week "This Period" view; otherwise block
    height scales with logged hours (0.5-hour buckets).
    """
    events: list[dict[str, object]] = []
    for row_idx, (entry_day_iso, account_name, seconds, channel) in enumerate(window_rows):
        hours_value = max(0, int(seconds)) / 3600.0
        height_units = min(24, max(1, int(round(hours_value * 2))))  # 0.5-hour buckets
        day_value = _parse_date_value(entry_day_iso)
        if day_value is None:
            continue

        bg_color, text_color = _channel_color(channel)
        class_names = ["ta-entry-event"]
        if compact:
            # Compact mode renders each entry as a small colored dot (no text) — the
            # per-day total badge carries the hours, the dot color carries the type, and a
            # per-dot ``ta-tip-<idx>`` class drives the hover tooltip (see
            # _build_dot_tooltip_css). The index matches enumerate(window_rows) so the
            # tooltip CSS built from the same rows lines up.
            class_names.extend(["ta-dot", f"ta-tip-{row_idx}"])
            title = ""
        else:
            class_names.append(f"ta-hu-{height_units}")
            title = f"{account_name} | {hours_value:.2f} hours"

        events.append(
            {
                "id": f"ta-{entry_day_iso}-{row_idx}",
                "title": title,
                "start": entry_day_iso,
                "end": (day_value + timedelta(days=1)).isoformat(),
                "allDay": True,
                "display": "block",
                "backgroundColor": bg_color,
                "borderColor": bg_color,
                "textColor": text_color,
                "classNames": class_names,
                "extendedProps": {"entry_date": entry_day_iso, "channel": str(channel or "")},
            }
        )
    return events


def _default_channel() -> str:
    """Default channel for a new/blank entry row (the first canonical option)."""
    return CHANNEL_OPTIONS[0] if CHANNEL_OPTIONS else ""


def _coerce_channel(value: object) -> str:
    """Return the saved channel verbatim if present so legacy values are preserved."""
    text = str(value or "").strip()
    return text if text else _default_channel()


def _channel_options_for_row(current_value: object) -> list[str]:
    """Ensure the row's current channel exists in the selectbox options.

    Options are always CHANNEL_OPTIONS in their fixed defined order; a
    non-canonical legacy value on the row is prepended so it stays selectable.
    """
    current = str(current_value or "").strip()
    if not current or current in CHANNEL_OPTIONS:
        return list(CHANNEL_OPTIONS)
    return [current, *CHANNEL_OPTIONS]


def _default_for_field(entry_field: dict) -> object:
    """Default widget value for an empty custom field cell."""
    field_type = entry_field["type"]
    if field_type in ("number", "date"):
        return None
    return ""


def _coerce_field_value(entry_field: dict, raw_value: object) -> object:
    """Normalize a parquet-loaded value to the widget's expected Python type."""
    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return _default_for_field(entry_field)
    field_type = entry_field["type"]
    if field_type == "number":
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return None
        if pd.isna(value):
            return None
        return value
    if field_type == "date":
        parsed = pd.to_datetime(raw_value, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date()
    return str(raw_value).strip()


def _list_field_options_for_row(entry_field: dict, current_value: object) -> list[str]:
    """Build selectbox options for a list-type custom field, preserving legacy values."""
    base_options = [str(o or "").strip() for o in entry_field.get("options") or []]
    base_options = [o for o in base_options if o]
    options = ["", *base_options]
    current = str(current_value or "").strip()
    if not current or current in options:
        return options
    return [options[0], current, *options[1:]]


def _serialize_custom_value(entry_field: dict, raw_value: object) -> object:
    """Convert a widget value to the parquet column type. Returns None for empty."""
    if raw_value is None:
        return None
    field_type = entry_field["type"]
    if field_type == "number":
        if isinstance(raw_value, bool):
            return float(int(raw_value))
        if isinstance(raw_value, (int, float)):
            return None if pd.isna(raw_value) else float(raw_value)
        return None
    if field_type == "date":
        if isinstance(raw_value, date):
            return raw_value
        parsed = pd.to_datetime(raw_value, errors="coerce")
        return parsed.date() if not pd.isna(parsed) else None
    text = str(raw_value).strip()
    return text if text else None


def _is_field_value_present(entry_field: dict, raw_value: object) -> bool:
    """True when the user actually entered a value (used for required-field validation)."""
    if raw_value is None:
        return False
    field_type = entry_field["type"]
    if field_type == "number":
        if isinstance(raw_value, (int, float)) and not pd.isna(raw_value):
            return True
        return False
    if field_type == "date":
        return isinstance(raw_value, date)
    return bool(str(raw_value).strip())


def _format_field_value_for_preview(entry_field: dict, raw_value: object) -> str:
    """Stringify a custom-field value for the submit-preview table."""
    if not _is_field_value_present(entry_field, raw_value):
        return ""
    field_type = entry_field["type"]
    if field_type == "number":
        value = float(raw_value)
        return f"{value:.0f}" if value.is_integer() else f"{value:g}"
    if field_type == "date":
        return raw_value.strftime("%m/%d/%Y") if isinstance(raw_value, date) else str(raw_value)
    return str(raw_value)


def _render_custom_field_widget(
    entry_field: dict, row_idx: int, disabled: bool, key_prefix: str = "ta_detailed"
) -> object:
    """Render a single custom-field input bound to <key_prefix>_cf_<id>_<row> session state."""
    field_id = entry_field["id"]
    field_type = entry_field["type"]
    key = f"{key_prefix}_cf_{field_id}_{row_idx}"
    label = f"{entry_field['name']}{' *' if entry_field['required'] else ''}"

    if key not in st.session_state:
        st.session_state[key] = _default_for_field(entry_field)

    if field_type == "number":
        return st.number_input(label, key=key, disabled=disabled, step=1.0, format="%g")
    if field_type == "date":
        return st.date_input(label, key=key, disabled=disabled, format="MM/DD/YYYY")
    if field_type == "list":
        return st.selectbox(
            label,
            options=_list_field_options_for_row(entry_field, st.session_state.get(key, "")),
            key=key,
            disabled=disabled,
        )
    return st.text_input(label, key=key, disabled=disabled)


def _normalize_cf_for_signature(value: object) -> object:
    """Normalize a custom-field value into a hashable, comparable token for _row_signature."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float)):
        return float(value)
    return str(value).strip()


def _row_signature(idx: int, entry_field_ids: list[str]) -> tuple:
    """Hashable fingerprint of one input row's current values, read from session state.

    Distinguishes a saved row from a draft: a row matches the saved baseline only
    when this fingerprint equals one captured at seed time (``ta_detailed_saved_sigs``).
    It reads the same session keys the row widgets bind to, so an untouched seeded
    row matches and any edit (or a brand-new row) does not. Time is compared as
    (hour, minute) — exactly what the row persists.
    """
    base = (
        str(st.session_state.get(f"ta_detailed_account_{idx}", "") or "").strip(),
        str(st.session_state.get(f"ta_detailed_custcode_{idx}", "") or "").strip(),
        int(st.session_state.get(f"ta_detailed_dur_h_{idx}", 0) or 0),
        int(st.session_state.get(f"ta_detailed_dur_m_{idx}", 0) or 0),
        str(st.session_state.get(f"ta_detailed_channel_{idx}", "") or "").strip(),
    )
    custom = tuple(
        (fid, _normalize_cf_for_signature(st.session_state.get(f"ta_detailed_cf_{fid}_{idx}")))
        for fid in entry_field_ids
    )
    return (base, custom)


def _seed_input_state_for_day(selected_day: date, selected_day_df: pd.DataFrame, account_options: list[str]) -> None:
    """
    Initialize the input widgets from selected-day data.
    If no day data exists, reset to default blank state.
    """
    day_token = selected_day.isoformat()
    if st.session_state.get("ta_loaded_day_token") == day_token:
        return

    entry_fields = _load_entry_fields()

    rows: list[dict[str, object]] = []
    if not selected_day_df.empty:
        for _, row in selected_day_df.iterrows():
            custom_values: dict[str, object] = {}
            for entry_field in entry_fields:
                col = f"cf_{entry_field['id']}"
                custom_values[entry_field["id"]] = _coerce_field_value(entry_field, row.get(col))
            hour, minute = _split_duration_to_hm(row.get("Time"))
            rows.append(
                {
                    "account": _coerce_account(row.get("Account"), account_options),
                    "customer_code": str(row.get("Customer Code") or "").strip(),
                    "channel": _coerce_channel(row.get("Channel")),
                    "hour": hour,
                    "minute": minute,
                    "custom_values": custom_values,
                }
            )

    if not rows:
        st.session_state["ta_detailed_count"] = 1
        st.session_state["ta_detailed_account_0"] = ""
        st.session_state["ta_detailed_custcode_0"] = ""
        st.session_state["ta_detailed_dur_h_0"] = 0
        st.session_state["ta_detailed_dur_m_0"] = 0
        st.session_state["ta_detailed_channel_0"] = _default_channel()
        for entry_field in entry_fields:
            st.session_state[f"ta_detailed_cf_{entry_field['id']}_0"] = _default_for_field(entry_field)
        # No saved rows for this day → the single blank row is a draft.
        st.session_state["ta_detailed_saved_sigs"] = []
        st.session_state["ta_loaded_day_token"] = day_token
        return

    st.session_state["ta_detailed_count"] = len(rows)
    for idx, row in enumerate(rows):
        st.session_state[f"ta_detailed_account_{idx}"] = row["account"]
        st.session_state[f"ta_detailed_custcode_{idx}"] = row["customer_code"]
        st.session_state[f"ta_detailed_dur_h_{idx}"] = row["hour"]
        st.session_state[f"ta_detailed_dur_m_{idx}"] = row["minute"]
        st.session_state[f"ta_detailed_channel_{idx}"] = row["channel"]
        row_custom = row.get("custom_values") or {}
        for entry_field in entry_fields:
            st.session_state[f"ta_detailed_cf_{entry_field['id']}_{idx}"] = row_custom.get(
                entry_field["id"], _default_for_field(entry_field)
            )

    # Baseline of saved-row fingerprints for this day. Captured now (after writing
    # the seeded values to session state) so the render loop can tell which rows
    # still match what's on disk vs. which the user has since edited.
    entry_field_ids = [entry_field["id"] for entry_field in entry_fields]
    st.session_state["ta_detailed_saved_sigs"] = [
        _row_signature(idx, entry_field_ids) for idx in range(len(rows))
    ]
    st.session_state["ta_loaded_day_token"] = day_token


def _delete_detailed_row(delete_idx: int) -> None:
    """Delete one row from Detailed mode widget state."""
    count = int(st.session_state.get("ta_detailed_count", 1) or 1)
    if count <= 1 or delete_idx < 0 or delete_idx >= count:
        return

    entry_fields = _load_entry_fields()

    for idx in range(delete_idx, count - 1):
        st.session_state[f"ta_detailed_account_{idx}"] = st.session_state.get(f"ta_detailed_account_{idx + 1}", "")
        st.session_state[f"ta_detailed_custcode_{idx}"] = st.session_state.get(
            f"ta_detailed_custcode_{idx + 1}", ""
        )
        st.session_state[f"ta_detailed_dur_h_{idx}"] = st.session_state.get(f"ta_detailed_dur_h_{idx + 1}", 0)
        st.session_state[f"ta_detailed_dur_m_{idx}"] = st.session_state.get(f"ta_detailed_dur_m_{idx + 1}", 0)
        st.session_state[f"ta_detailed_channel_{idx}"] = st.session_state.get(
            f"ta_detailed_channel_{idx + 1}",
            _default_channel(),
        )
        for entry_field in entry_fields:
            next_key = f"ta_detailed_cf_{entry_field['id']}_{idx + 1}"
            st.session_state[f"ta_detailed_cf_{entry_field['id']}_{idx}"] = st.session_state.get(
                next_key, _default_for_field(entry_field)
            )

    last_idx = count - 1
    last_keys = [
        f"ta_detailed_account_{last_idx}",
        f"ta_detailed_custcode_{last_idx}",
        f"ta_detailed_dur_h_{last_idx}",
        f"ta_detailed_dur_m_{last_idx}",
        f"ta_detailed_channel_{last_idx}",
    ]
    for entry_field in entry_fields:
        last_keys.append(f"ta_detailed_cf_{entry_field['id']}_{last_idx}")
    for key in last_keys:
        if key in st.session_state:
            del st.session_state[key]

    st.session_state["ta_detailed_count"] = count - 1


def _rerun_input_fragment() -> None:
    """Rerun just the Input fragment, falling back to a full-app rerun.

    Add Row, multi-row Delete, and calendar day-clicks all happen inside the
    @st.fragment render_input_view, so a fragment-scoped rerun refreshes only the
    input area and skips re-executing the (now cached, but still non-trivial)
    module body. If this is ever reached while the fragment is running as part of
    a full-app rerun, st.rerun(scope="fragment") raises StreamlitAPIException — an
    Exception raised *before* the rerun is requested — whereas a valid fragment
    rerun unwinds via the BaseException-derived RerunException (not caught here).
    So the except branch runs only in the disallowed-scope case, where we fall
    back to a normal app rerun.
    """
    try:
        st.rerun(scope="fragment")
    except Exception:
        st.rerun()


def _refresh_account_data(number_of_accounts: int) -> None:
    """Refresh the in-app account lookup by reloading the latest parquet on the share.

    The "Refresh Account Data" button keeps its name, but it no longer rebuilds from
    the SharePoint source. The accounts/users parquet is produced centrally (the daily
    startup step on synced machines, plus a twice-daily scheduled task running
    ``refresh_data.py``), so end users never need to read the source directly. This
    button drops the cached lookups and re-reads the latest parquet from
    ``config.PERSONNEL_DIR``, which works for every user whether or not they have
    SharePoint synced locally.

    Clearing the caches restamps the cached ``loaded_at`` (so the "time since last
    refresh" line resets) and reruns only this fragment (no full-page reload). Nothing
    the user entered is touched, with one exception honored by the caller: a row whose
    Reporting Name / Customer Code was valid before but is no longer present in the
    refreshed data is queued to reset to blank, because the data changed under the
    user's choice. Time, channel, custom fields, and still-valid rows are preserved.

    Field resets are *queued* (not applied here) because this runs from a button
    below the row widgets, which are already instantiated this run; Streamlit forbids
    mutating an instantiated widget's state, so the blanking is deferred to the top of
    the next fragment run.
    """
    previous_lookup = _account_lookup_for_dir(str(PERSONNEL_DIR))

    # Drop every cache layer that feeds account selections so the reload actually hits
    # disk: the parquet read (load_account_lookup), the derived dropdown lookup
    # (_account_lookup_for_dir), and the task-tracker account list (load_accounts).
    with st.spinner("Refreshing account data..."):
        utils.load_account_lookup.clear()
        _account_lookup_for_dir.clear()
        utils.load_accounts.clear()
        refreshed_lookup = _account_lookup_for_dir(str(PERSONNEL_DIR))

    # Guard against a transient empty read (a UNC hiccup, or the producer's brief
    # delete/rewrite window): if the refreshed lookup came back empty but we DID have
    # data a moment ago, treat it as a failed refresh — surface an error and keep every
    # current selection rather than blanking valid rows against an empty list.
    prev_had_data = bool(previous_lookup.get("reporting_names") or previous_lookup.get("customer_codes"))
    refreshed_empty = not (refreshed_lookup.get("reporting_names") or refreshed_lookup.get("customer_codes"))
    if refreshed_empty and prev_had_data:
        _queue_input_status(
            "error",
            "Couldn't reach the latest account data just now, so your current selections "
            "were kept. Please try again in a moment.",
        )
        _rerun_input_fragment()
        return

    # Keep the user's chosen Reporting Name / Customer Code across an account-data
    # refresh. The accounts file is a moving target (names/codes get added or renamed
    # over time), but an entry's pairing is a historical choice and must not be wiped
    # just because the source changed under it. (Previously, pairings that became
    # invalid after a refresh were reset to blank, which silently lost valid selections.)
    _queue_input_status("success", "Account data refreshed.")

    _rerun_input_fragment()


def _render_last_refresh_caption() -> None:
    """Render the auto-ticking "time since last refresh" line under the Refresh Account Data button.

    The line only surfaces once the account data is 6+ hours stale; below that the JS
    returns an empty string so nothing shows. Its 20px slot stays reserved either way,
    so the line appears in place (no layout shift) and ticks up on its own once visible.

    The elapsed-time text advances entirely client-side via a tiny components.html
    iframe (a JS setInterval) — so it updates on its own with ZERO server-side
    reruns. This deliberately avoids a ``run_every`` fragment: an auto-rerunning
    fragment nested in the input fragment/column raced with active data entry and
    could blank the whole app (the frontend received a delta it couldn't place).

    The base timestamp is the cached data-load time the parent fragment mirrored
    into session state (``ta_account_loaded_at``); the parent refreshes it on each
    of its reruns, so the text reflects true staleness and survives full page
    reloads instead of resetting to "just now".
    """
    loaded_at = st.session_state.get("ta_account_loaded_at")
    base_ms = int(loaded_at.timestamp() * 1000) if loaded_at is not None else 0
    components.html(
        f"""<!DOCTYPE html><html><head><style>
html, body {{ margin: 0; padding: 0; background: transparent; }}
#ta-refresh-since {{
    text-align: right;
    font-family: 'Work Sans', sans-serif;
    font-size: 0.72rem;
    color: rgba(49, 51, 63, 0.6);
    line-height: 1.2;
    padding-right: 2px;
    white-space: nowrap;
    overflow: hidden;
}}
</style></head><body>
<div id="ta-refresh-since"></div>
<script>
const baseMs = {base_ms};
function taRefreshSince() {{
    if (!baseMs) return "";
    const secs = Math.max(0, Math.floor((Date.now() - baseMs) / 1000));
    const hrs = Math.floor(secs / 3600);
    // Only surface staleness once the account data is 6+ hours old; below that the
    // line stays blank (its slot is reserved, so nothing shifts when it appears).
    if (hrs < 6) return "";
    if (hrs < 24) return hrs + " hour" + (hrs !== 1 ? "s" : "") + " since last refresh - refresh recommended";
    const days = Math.floor(hrs / 24);
    return days + " day" + (days !== 1 ? "s" : "") + " since last refresh - refresh recommended";
}}
const el = document.getElementById("ta-refresh-since");
function taRefreshTick() {{ if (el) el.textContent = taRefreshSince(); }}
taRefreshTick();
setInterval(taRefreshTick, 15000);
</script>
</body></html>""",
        height=20,
    )


def _compute_calendar_window(view_mode: str, today: date) -> tuple[date, date]:
    """Return (window_start, window_end) for the chosen calendar view."""
    if view_mode == "Last Week":
        # Monday through Friday of the previous work week
        return _work_week_bounds(today - timedelta(days=7))
    if view_mode == "This Period":
        period = utils.get_fiscal_period_for_date(today)
        if period is not None:
            return period["StartDate"], period["EndDate"]
        # Fallback when no fiscal period covers today: show the calendar month.
        first_of_month = today.replace(day=1)
        if today.month == 12:
            last_of_month = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            last_of_month = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
        return first_of_month, last_of_month
    # Default: This Week — Monday through Friday of the current work week
    return _work_week_bounds(today)


def _format_hours_label(seconds: object) -> str:
    """Compact decimal-hours label for the summary line (e.g. 0h, 7.5h, 6.25h).

    Matches the calendar's decimal-hours convention; trailing zeros are trimmed so
    whole hours read as "8h" rather than "8.00h".
    """
    hours = max(0, int(seconds or 0)) / 3600.0
    text = f"{hours:.2f}".rstrip("0").rstrip(".")
    return f"{text or '0'}h"


def _build_day_total_badge_css(day_total_seconds: dict[str, int]) -> str:
    """Build per-day ``::after`` badge CSS that prints each day's total hours in the
    top-left corner of its calendar square.

    FullCalendar tags every day CELL (the ``<td>.fc-daygrid-day``) with
    ``data-date="YYYY-MM-DD"``. The badge ``::after`` is anchored to that td (made
    ``position: relative`` in the base CSS) — NOT the inner day frame, which FC forces
    ``position: static`` in its fixed-height layout, causing a frame-anchored badge to
    trail the last entry instead of pinning to the corner. The td always spans the full
    cell height, so ``bottom``/``right`` lock the badge to the bottom-right corner. The
    day NUMBER sits top-right and events render full-width from near the top, so the
    bottom-right corner is the reliably-empty one. Days with no hours get no badge.
    Colors are literal (the calendar iframe doesn't see the parent app's ``--cna-green``
    variable).
    """
    parts: list[str] = []
    for entry_day_iso, seconds in day_total_seconds.items():
        if seconds <= 0:
            continue
        label = _format_hours_label(seconds)
        parts.append(
            f'.fc .fc-daygrid-day[data-date="{entry_day_iso}"]::after {{'
            f' content: "{label}";'
            " position: absolute !important; bottom: 3px; right: 4px;"
            " font-size: 0.64rem; font-weight: 700; color: #007A6C;"
            " background: #ffffff; border: 1px solid rgba(0, 177, 158, 0.6);"
            " box-shadow: 0 1px 2px rgba(0, 0, 0, 0.12);"
            " padding: 0 5px; border-radius: 8px; line-height: 1.5;"
            " pointer-events: none; z-index: 6; }"
        )
    return "\n".join(parts)


def _build_empty_day_hint_css(empty_day_isos: list[str]) -> str:
    """Build CSS that gives in-range days with NO logged hours a faint dashed outline that
    fills the WHOLE cell plus a low-contrast centered ``+`` prompt, so empty days read as
    "click to add" rather than dead space. The box brightens on hover (reinforcing the
    hover affordance). Targets specific ``data-date`` cells — padding/out-of-range days are
    excluded by the caller, so they stay plain.

    The box is one ``::before`` anchored to the day CELL (the ``<td>``, which is
    ``position: relative`` and always spans the full cell height) — NOT the inner day
    frame, which FullCalendar caps near its ~48px ``min-height`` in the fixed-height grid,
    leaving the dashed box covering only the top of the square. ``::before`` (not
    ``::after``) keeps it clear of the bottom-right total badge (``::after``), which only
    appears on days that DO have hours, so the two never collide.
    """
    if not empty_day_isos:
        return ""
    before_sels: list[str] = []
    hover_sels: list[str] = []
    for iso in empty_day_isos:
        day_sel = f'.fc .fc-daygrid-day[data-date="{iso}"]'
        before_sels.append(f"{day_sel}::before")
        hover_sels.append(f"{day_sel}:hover::before")
    box_css = (
        ", ".join(before_sels)
        + ' { content: "+"; position: absolute; top: 3px; right: 3px; bottom: 3px;'
        " left: 3px; display: flex; align-items: center; justify-content: center;"
        " border: 1px dashed rgba(0, 177, 154, 0.30); border-radius: 6px;"
        " font-size: 1.25rem; font-weight: 600; color: rgba(0, 177, 154, 0.28);"
        " pointer-events: none; z-index: 0;"
        " transition: color 0.15s ease, border-color 0.15s ease; }"
    )
    hover_css = (
        ", ".join(hover_sels)
        + " { color: rgba(0, 177, 154, 0.72); border-color: rgba(0, 177, 154, 0.5); }"
    )
    return "\n".join([box_css, hover_css])


def _css_str_escape(text: object) -> str:
    """Escape a value for safe use inside a CSS ``content: "..."`` string literal."""
    return (
        str(text)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", " ")
        .replace("\r", " ")
    )


def _build_dot_tooltip_css(window_rows: tuple[tuple[str, str, int, str], ...]) -> str:
    """Per-dot hover-tooltip content for compact (This Period) view.

    Each dot carries a ``ta-tip-<idx>`` class (idx == enumerate(window_rows) index, so it
    lines up with the events built from the same rows). The shared tooltip card/arrow
    styling lives in the static calendar CSS; here we only emit the per-dot ``content``
    (reporting name, channel, hours) revealed on hover. This per-index trick mirrors the
    day-total badges — streamlit_calendar exposes no JS render hook to attach a tooltip
    library inside the calendar iframe. ``\\A`` (with its terminating space, which CSS
    consumes) gives the multi-line layout under ``white-space: pre-line``.
    """
    parts: list[str] = []
    for idx, (entry_day_iso, account_name, seconds, channel) in enumerate(window_rows):
        if _parse_date_value(entry_day_iso) is None:
            continue
        hours_value = max(0, int(seconds)) / 3600.0
        hours_text = f"{hours_value:.2f}".rstrip("0").rstrip(".") or "0"
        channel_text = str(channel or "").strip() or "—"
        body = (
            f"{_css_str_escape(account_name)}\\A "
            f"{_css_str_escape(channel_text)}\\A "
            f"{_css_str_escape(hours_text + ' hrs')}"
        )
        parts.append(f'.fc .ta-tip-{idx}:hover::after {{ content: "{body}"; }}')
    return "\n".join(parts)


def _render_channel_legend(types_present: list[str]) -> None:
    """Render a small color legend mapping each channel TYPE present in the current view
    (Resupply / Smallwares / Equipment / Rollout / Full) to its calendar color. Only types
    actually present in the window are shown, so it stays short and contextual."""
    if not types_present:
        return
    chips = []
    for type_key in types_present:
        bg_color, _text = CHANNEL_TYPE_COLORS.get(type_key, CHANNEL_COLOR_DEFAULT)
        chips.append(
            '<span class="ta-legend-chip">'
            f'<span class="ta-legend-dot" style="background:{bg_color};"></span>'
            f"{html.escape(type_key)}</span>"
        )
    st.markdown(
        '<div class="ta-legend">' + "".join(chips) + "</div>",
        unsafe_allow_html=True,
    )


def _render_window_hours_summary(window_df: pd.DataFrame, view_mode: str) -> None:
    """Render the timeframe-total callout (shown above the calendar's top-right corner).

    Per-day totals are shown as badges inside the calendar squares (see
    ``_build_day_total_badge_css``); this callout carries only the timeframe sum
    (This Week / Last Week / This Period) so the two don't duplicate each other.
    """
    total_seconds = 0
    for _, row in window_df.iterrows():
        if isinstance(row.get("Entry Date"), date):
            total_seconds += max(0, int(row.get("seconds", 0)))

    total_label = "Total Hours"
    st.markdown(
        '<div class="ta-hours-callout-wrap">'
        '<div class="ta-hours-callout">'
        f'<span class="ta-hours-callout-label">{html.escape(total_label)}</span>'
        f'<span class="ta-hours-callout-value">{_format_hours_label(total_seconds)}</span>'
        "</div></div>",
        unsafe_allow_html=True,
    )


def render_input_day_selector(user_login: str, full_name: str) -> tuple[date, pd.DataFrame]:
    """Render clickable calendar and return selected day + selected day rows."""
    # Header row above the calendar: the view toggle (left) and the timeframe-total
    # callout (right, above the calendar's top-right corner). The total is rendered into
    # total_col further down, once window_df is loaded.
    toggle_col, total_col = st.columns([1, 1])
    with toggle_col:
        view_mode_options = ["This Week", "This Period", "Last Week"]
        stored_view = st.session_state.get("ta_calendar_view", "This Week")
        if stored_view not in view_mode_options:
            stored_view = "This Week"
            st.session_state["ta_calendar_view"] = stored_view
        # Keyed container so the segmented-control CSS can scope to .st-key-ta_view_toggle
        # (a keyed st.container reliably emits that class).
        with st.container(key="ta_view_toggle"):
            view_mode = st.radio(
                "View",
                options=view_mode_options,
                index=view_mode_options.index(stored_view),
                horizontal=True,
                key="ta_calendar_view",
                label_visibility="collapsed",
            )
    # The multi-week This Period grid always renders entries as compact color dots (with
    # hover tooltips); the week views keep the height-scaled labeled blocks. There's no
    # block view for This Period and no toggle.
    compact_view = view_mode == "This Period"

    today = utils.to_eastern(utils.now_utc()).date()
    window_start, window_end = _compute_calendar_window(view_mode, today)

    if "ta_input_selected_day" not in st.session_state:
        st.session_state["ta_input_selected_day"] = today
    selected_day = _parse_date_value(st.session_state.get("ta_input_selected_day", today)) or today
    st.session_state["ta_input_selected_day"] = selected_day
    if selected_day < window_start or selected_day > window_end:
        selected_day = today if window_start <= today <= window_end else window_start
        st.session_state["ta_input_selected_day"] = selected_day

    window_df = load_time_allocation_user_window(
        TIME_ALLOCATION_DIR,
        user_login,
        full_name,
        window_start.isoformat(),
        window_end.isoformat(),
    ).copy()
    window_df = _apply_window_overrides(window_df, window_start, window_end)
    if window_df.empty:
        window_df = pd.DataFrame(columns=["Entry Date", "Account", "Time", "Channel", "Source File"])

    window_df["seconds"] = window_df["Time"].astype(str).apply(utils.parse_hhmmss).clip(lower=0)

    # Timeframe-total callout in the header row's right column (above the calendar's
    # top-right corner).
    with total_col:
        _render_window_hours_summary(window_df, view_mode)

    window_rows: list[tuple[str, str, int, str]] = []
    for _, row in window_df.reset_index(drop=True).iterrows():
        entry_day = row["Entry Date"]
        if not isinstance(entry_day, date):
            continue
        account_name = str(row.get("Account") or "").strip() or "(No Account)"
        channel = str(row.get("Channel") or "").strip()
        seconds = max(0, int(row.get("seconds", 0)))
        window_rows.append((entry_day.isoformat(), account_name, seconds, channel))
    calendar_events = _build_calendar_events(tuple(window_rows), compact=compact_view)

    # Per-day hours total, keyed by ISO date, to paint a corner badge on each day cell.
    day_total_seconds: dict[str, int] = {}
    for entry_day_iso, _account_name, seconds, _channel in window_rows:
        day_total_seconds[entry_day_iso] = day_total_seconds.get(entry_day_iso, 0) + max(0, int(seconds))
    day_total_badge_css = _build_day_total_badge_css(day_total_seconds)

    num_days = (window_end - window_start).days + 1

    # In-range days with no logged hours get the "click to add" empty-state hint — but
    # only in the week views, where a day is selectable/editable. The multi-week This
    # Period grid is a read-only overview (dots), so it gets no "+" prompt. Padding days
    # (This Period grid spillover) sit outside [window_start, window_end] anyway.
    if compact_view:
        empty_day_hint_css = ""
    else:
        empty_day_isos = [
            (window_start + timedelta(days=i)).isoformat()
            for i in range(num_days)
            if day_total_seconds.get((window_start + timedelta(days=i)).isoformat(), 0) <= 0
        ]
        empty_day_hint_css = _build_empty_day_hint_css(empty_day_isos)

    # Per-dot hover tooltips (compact / This Period only).
    dot_tooltip_css = _build_dot_tooltip_css(tuple(window_rows)) if compact_view else ""

    # Channel TYPES present in this window, in canonical color order, for the legend.
    types_seen = {_channel_type(ch) for *_rest, ch in window_rows}
    types_seen.discard("")
    types_present = [t for t in CHANNEL_TYPE_COLORS if t in types_seen]
    calendar_initial_date = window_start.isoformat()
    valid_range: dict[str, str] | None = None
    if view_mode == "This Period":
        # Pad to whole weeks (Sun–Sat) so dayGrid wraps into rows of 7.
        # Python weekday: Mon=0..Sun=6; FullCalendar default firstDay=Sunday.
        sunday_offset = (window_start.weekday() + 1) % 7
        grid_start = window_start - timedelta(days=sunday_offset)
        saturday_offset = (5 - window_end.weekday()) % 7
        grid_end = window_end + timedelta(days=saturday_offset)
        weeks_in_view = max(1, ((grid_end - grid_start).days + 1) // 7)
        initial_view = "dayGridPeriod"
        # Size the multi-week period grid to its CONTENT (height "auto" + expandRows
        # False, set below) so each day cell hugs its own entries. A fixed pixel height
        # made FullCalendar stretch every week row to fill it, leaving a large empty gap
        # below the entries — with the bottom-pinned daily-total badge marooned at the
        # far cell bottom instead of sitting just under the entries.
        calendar_height = "auto"
        views_config = {
            "dayGridPeriod": {
                "type": "dayGrid",
                "duration": {"weeks": weeks_in_view},
                "buttonText": "This Period",
            }
        }
        calendar_initial_date = grid_start.isoformat()
        # Gray out padding days before/after the actual period.
        valid_range = {
            "start": window_start.isoformat(),
            "end": (window_end + timedelta(days=1)).isoformat(),
        }
    else:
        # This Week / Last Week — a 5-day Mon–Fri work-week grid
        initial_view = "dayGridWorkWeek"
        calendar_height = 180
        views_config = {
            "dayGridWorkWeek": {
                "type": "dayGrid",
                "duration": {"days": 5},
                "buttonText": "5 days",
            }
        }

    calendar_options = {
        "initialView": initial_view,
        "views": views_config,
        "editable": False,
        "selectable": False,
        "eventDisplay": "block",
        # Compact dots are tiny and wrap, and This Period uses auto height (cells expand),
        # so show them all (no "+X more"). Block view keeps the 4-row cap + popover.
        "dayMaxEventRows": False if compact_view else 4,
        "headerToolbar": {"left": "", "center": "title", "right": ""},
        "height": calendar_height,
        "initialDate": calendar_initial_date,
    }
    if valid_range is not None:
        calendar_options["validRange"] = valid_range
    if view_mode == "This Period":
        # With height "auto", don't let rows expand to fill — keep them content-sized so
        # the bottom-right total badge sits directly under each day's entries (no gap).
        calendar_options["expandRows"] = False
    height_rules = "\n".join(
        f".fc .ta-hu-{unit} {{ min-height: {16 + ((unit - 1) * 3)}px; }}"
        for unit in range(1, 25)
    )
    selected_day_iso = selected_day.isoformat()
    selected_day_css = f"""
    .fc .fc-daygrid-day.fc-day-today,
    .fc .fc-daygrid-day.fc-day-today .fc-daygrid-day-frame {{
        background-color: transparent !important;
        box-shadow: none !important;
    }}
    .fc .fc-daygrid-day[data-date="{selected_day_iso}"] {{
        background-color: rgba(0, 177, 158, 0.16) !important;
        box-shadow: inset 0 0 0 2px rgba(0, 177, 158, 0.55);
    }}
    .fc .fc-daygrid-day[data-date="{selected_day_iso}"] .fc-daygrid-event {{
        border-color: transparent !important;
    }}
    """
    custom_css = """
    .fc {
        font-family: 'Work Sans', sans-serif;
    }
    .fc .fc-toolbar-title {
        font-family: 'Poppins', 'Work Sans', sans-serif;
        font-size: 0.95rem;
        font-weight: 600;
        letter-spacing: 0.01em;
        color: #002E65;
    }
    .fc .fc-toolbar {
        margin-bottom: 0.4rem;
    }
    /* Weekday header row: uppercase, letter-spaced, brand-tinted (Sky) */
    .fc .fc-col-header-cell {
        background-color: rgba(208, 236, 238, 0.45);
        border-color: rgba(0, 46, 101, 0.08);
    }
    .fc .fc-col-header-cell-cushion {
        font-size: 0.62rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.09em;
        color: rgba(0, 46, 101, 0.6);
        padding: 4px 2px;
    }
    .fc .fc-daygrid-day-frame {
        min-height: 48px;
        cursor: pointer;
    }
    /* Anchor each day's corner total badge to the day CELL (the <td>), which always
       spans the full cell height. In FullCalendar's fixed-height (liquid-hack) layout
       the <td> is already position:relative and the day FRAME is forced position:static,
       so anchoring the badge to the frame makes it trail the last entry instead of
       pinning to the cell's bottom-right. Setting the td relative here also covers the
       non-liquid layout (where FC doesn't set it). */
    .fc .fc-daygrid-day {
        position: relative;
        transition: background-color 0.15s ease;
    }
    /* Hover affordance: subtle tint over the WHOLE day cell (the <td>, full height) so
       the entire box dims — not just the inner frame, which only spans the cell's top in
       the fixed-height week views (that left the "half box dims" artifact). */
    .fc .fc-daygrid-day:hover {
        background-color: rgba(0, 177, 154, 0.07);
    }
    .fc .fc-daygrid-day,
    .fc .fc-daygrid-day-top,
    .fc .fc-daygrid-day-number,
    .fc .fc-daygrid-bg-harness {
        cursor: pointer;
    }
    /* Day number: a touch larger, lighter weight, navy-tinted */
    .fc .fc-daygrid-day-number {
        font-weight: 500;
        font-size: 0.82rem;
        color: rgba(0, 46, 101, 0.78);
        padding: 4px 6px 2px;
    }
    /* Events: depth via gradient sheen + drop shadow + rounding (no flat rectangles) */
    .fc .fc-daygrid-event {
        border-radius: 7px;
        padding: 2px 6px;
        cursor: pointer;
        background-image: linear-gradient(180deg, rgba(255, 255, 255, 0.16), rgba(0, 0, 0, 0.10));
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.18);
        transition: transform 0.12s ease, box-shadow 0.12s ease, filter 0.12s ease;
    }
    /* Hover lift on events */
    .fc .fc-daygrid-event:hover {
        transform: translateY(-1px);
        box-shadow: 0 3px 6px rgba(0, 0, 0, 0.22);
        filter: brightness(1.06);
        z-index: 7;
    }
    .fc .ta-entry-event {
        align-items: flex-start;
    }
    /* Compact dot mode (This Period only): each entry becomes a small colored circle, no
       text. Dots flow left-to-right and wrap, so a day's entries read as a tidy cluster
       of colored dots instead of stacked bars. The day-events container is flipped to a
       wrapping flexbox and the harnesses sized to content (overriding FullCalendar's
       full-width vertical stacking). */
    .fc .fc-daygrid-day-events:has(.ta-dot) {
        display: flex;
        flex-wrap: wrap;
        align-content: flex-start;
        gap: 4px;
        padding: 3px 4px 0;
        min-height: 0;
    }
    .fc .fc-daygrid-day-events:has(.ta-dot) .fc-daygrid-event-harness {
        position: static !important;
        display: inline-block !important;
        width: auto !important;
        margin: 0 !important;
        top: auto !important;
        left: auto !important;
        right: auto !important;
    }
    .fc .ta-dot {
        position: relative;
        width: 12px !important;
        height: 12px !important;
        min-height: 0 !important;
        padding: 0 !important;
        border: 1.5px solid rgba(255, 255, 255, 0.65) !important;
        border-radius: 50% !important;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.25);
        background-image: none;
    }
    .fc .ta-dot .fc-event-main,
    .fc .ta-dot .fc-event-title,
    .fc .ta-dot .fc-event-time {
        display: none !important;
    }
    /* Dot hover tooltip — shared card + arrow styling; the per-dot ``content`` (reporting
       name / channel / hours) is appended separately (see _build_dot_tooltip_css). Hidden
       until the dot is hovered. Anchored to the dot (position: relative above). */
    .fc .ta-dot::after,
    .fc .ta-dot::before {
        position: absolute;
        left: 50%;
        transform: translateX(-50%);
        opacity: 0;
        visibility: hidden;
        transition: opacity 0.12s ease;
        pointer-events: none;
        z-index: 9999;
    }
    .fc .ta-dot::after {
        content: "";
        bottom: calc(100% + 7px);
        white-space: pre-line;
        text-align: left;
        width: max-content;
        max-width: 200px;
        background: #002E65;
        color: #ffffff;
        font-family: 'Work Sans', sans-serif;
        font-size: 0.66rem;
        font-weight: 500;
        line-height: 1.5;
        padding: 6px 9px;
        border-radius: 8px;
        box-shadow: 0 6px 18px rgba(0, 0, 0, 0.28);
    }
    .fc .ta-dot::before {
        content: "";
        bottom: calc(100% + 1px);
        border: 6px solid transparent;
        border-top-color: #002E65;
    }
    .fc .ta-dot:hover::after,
    .fc .ta-dot:hover::before {
        opacity: 1;
        visibility: visible;
    }
    /* Let the tooltip escape the grid instead of being clipped by FullCalendar's
       overflow:hidden containers (safe here — This Period uses auto height, no scroller). */
    .fc:has(.ta-dot) .fc-view-harness,
    .fc:has(.ta-dot) .fc-scroller,
    .fc:has(.ta-dot) .fc-daygrid-body,
    .fc:has(.ta-dot) .fc-daygrid-day-frame,
    .fc:has(.ta-dot) .fc-daygrid-day-events {
        overflow: visible !important;
    }
    .fc .fc-event-title {
        font-weight: 600;
        white-space: normal;
        line-height: 1.15;
        font-size: 0.68rem;
    }
    """ + "\n" + height_rules + "\n" + selected_day_css + "\n" + day_total_badge_css + "\n" + empty_day_hint_css + "\n" + dot_tooltip_css + "\n" + _CALENDAR_MORE_POPOVER_CSS
    calendar_widget_key = f"ta_input_calendar_{view_mode}_{num_days}_{'c' if compact_view else 'f'}"
    calendar_state = st_calendar(
        events=calendar_events,
        options=calendar_options,
        custom_css=custom_css,
        callbacks=["dateClick", "eventClick"],
        key=calendar_widget_key,
    )

    if isinstance(calendar_state, dict):
        callback_name = str(calendar_state.get("callback") or "")
        clicked_day: date | None = None
        callback_token: str | None = None

        if callback_name == "dateClick":
            date_payload = calendar_state.get("dateClick", {}) or {}
            clicked_raw = date_payload.get("dateStr") or date_payload.get("date")
            clicked_day = _parse_date_value(clicked_raw)

        elif callback_name == "eventClick":
            event_payload = calendar_state.get("eventClick", {}).get("event", {}) or {}
            extended_props = event_payload.get("extendedProps", {}) or {}
            event_id = str(event_payload.get("id") or "")

            # Trust only explicit entry_date payload to avoid stale/timezone-shifted start values.
            clicked_raw = extended_props.get("entry_date")
            if not clicked_raw and event_id.startswith("ta-selected-"):
                clicked_raw = event_id.removeprefix("ta-selected-")

            clicked_day = _parse_date_value(clicked_raw)

        if clicked_day is not None:
            callback_token = f"{callback_name}:{clicked_day.isoformat()}"

        if callback_token and callback_token != st.session_state.get("ta_input_last_callback_token"):
            st.session_state["ta_input_last_callback_token"] = callback_token
            if window_start <= clicked_day <= window_end and clicked_day != selected_day:
                selected_day = clicked_day
                st.session_state["ta_input_selected_day"] = selected_day
                _rerun_input_fragment()

    _render_channel_legend(types_present)

    selected_day_df = window_df[window_df["Entry Date"].eq(selected_day)].copy()
    return selected_day, selected_day_df


@st.fragment
def render_input_view(
    user_login: str,
    full_name: str,
    department: str,
    account_options: list[str],
    account_lookup: dict,
) -> None:
    """Render input controls and save parquet entries."""
    if "ta_confirm_open" not in st.session_state:
        st.session_state.ta_confirm_open = False
    if "ta_confirm_rendered" not in st.session_state:
        st.session_state.ta_confirm_rendered = False
    if "ta_confirm_payload" not in st.session_state:
        st.session_state.ta_confirm_payload = None
    if "ta_delete_confirm_open" not in st.session_state:
        st.session_state.ta_delete_confirm_open = False
    if "ta_delete_confirm_rendered" not in st.session_state:
        st.session_state.ta_delete_confirm_rendered = False
    if "ta_delete_payload" not in st.session_state:
        st.session_state.ta_delete_payload = None

    # Re-resolve the account lookup from cache on every (possibly fragment-scoped)
    # rerun. @st.fragment replays the *original* call arguments, so reading the
    # cached lookup here — rather than trusting the passed-in args — is what lets
    # the Refresh Account Data button surface freshly-reloaded Reporting Names /
    # Customer Codes after it clears the cache, all without a full-page reload.
    account_lookup = _account_lookup_for_dir(str(PERSONNEL_DIR))
    # Favorited Reporting Names float to the top of every row's dropdown (after the
    # blank). Computed once here for the shared option list, so row count and the
    # number of favorites don't add per-row cost. Values are unchanged — only order.
    favorite_names = set(_account_favorites_list())
    account_options = _favorites_first([""] + account_lookup["reporting_names"], favorite_names)
    # Mirror the cached data-load time into session state so the auto-ticking
    # caption fragment can read it cheaply (without re-copying the whole lookup).
    st.session_state["ta_account_loaded_at"] = account_lookup.get("loaded_at")

    _render_input_status()

    selected_day, selected_day_df = render_input_day_selector(user_login, full_name)
    _seed_input_state_for_day(selected_day, selected_day_df, account_options)

    view_mode = st.session_state.get("ta_calendar_view", "This Week")
    period_view_locked = view_mode == "This Period"
    editing_locked = period_view_locked or not _is_editable_day(selected_day)
    if period_view_locked:
        st.info(
            "This Period is a read-only overview. Switch to This Week or "
            "Last Week to add or change entries."
        )
    elif editing_locked:
        window_start, window_end = _editable_window()
        st.warning(
            f"Entries for {selected_day:%m/%d/%Y} are read-only. You can edit "
            f"days from {window_start:%m/%d/%Y} through {window_end:%m/%d/%Y} "
            f"(this week and last week)."
        )

    pending_delete_idx = st.session_state.pop("ta_detailed_delete_idx", None)
    if pending_delete_idx is not None and not editing_locked:
        _delete_detailed_row(int(pending_delete_idx))

    # Apply any field resets queued by a Refresh Account Data click. This runs
    # before the row widgets are instantiated, so blanking their session_state is
    # allowed; only rows whose selection the refresh actually invalidated are here.
    refresh_reset_idxs = st.session_state.pop("ta_account_refresh_reset_idxs", []) or []
    for reset_idx in refresh_reset_idxs:
        st.session_state[f"ta_detailed_account_{reset_idx}"] = ""
        st.session_state[f"ta_detailed_custcode_{reset_idx}"] = ""
    if refresh_reset_idxs:
        count = len(refresh_reset_idxs)
        st.toast(
            f"Cleared {count} selection{'s' if count != 1 else ''} no longer present "
            "in the latest account data.",
            icon=":material/info:",
        )

    rows = []
    number_of_accounts = int(max(1, st.session_state.get("ta_detailed_count", 1) or 1))
    st.session_state["ta_detailed_count"] = number_of_accounts
    entry_fields = _load_entry_fields()

    for idx in range(number_of_accounts):
        account_key = f"ta_detailed_account_{idx}"
        custcode_key = f"ta_detailed_custcode_{idx}"
        hour_key = f"ta_detailed_dur_h_{idx}"
        minute_key = f"ta_detailed_dur_m_{idx}"
        channel_key = f"ta_detailed_channel_{idx}"
        if account_key not in st.session_state:
            st.session_state[account_key] = ""
        if custcode_key not in st.session_state:
            st.session_state[custcode_key] = ""
        if hour_key not in st.session_state:
            st.session_state[hour_key] = 0
        if minute_key not in st.session_state:
            st.session_state[minute_key] = 0
        if channel_key not in st.session_state:
            st.session_state[channel_key] = _default_channel()
        for entry_field in entry_fields:
            cf_key = f"ta_detailed_cf_{entry_field['id']}_{idx}"
            if cf_key not in st.session_state:
                st.session_state[cf_key] = _default_for_field(entry_field)

    has_saved_rows_for_day = not selected_day_df.empty

    st.caption("Select a Reporting Name or Customer Code (they fill each other in), then pick the hours and minutes for each row.")

    header_labels = [
        ("Reporting Name", 3.2),
        ("Customer Code", 2.3),
        ("Time", 2.6),
        ("Channel", 2),
        ("", 1),
    ]
    header_cols = st.columns([w for _, w in header_labels])
    for hcol, (label, _) in zip(header_cols, header_labels):
        if label:
            hcol.markdown(f"<div class='ta-entry-col-header'>{label}</div>", unsafe_allow_html=True)

    # Flag each row as saved (still matches the seed-time baseline) or draft (new,
    # or a saved row that's been edited). Computed from session state before the
    # widgets render, so an edit this rerun already reads as a draft. Content-based,
    # so it survives row add/delete/reorder; consumed from a multiset so duplicate
    # rows don't both claim the same saved fingerprint.
    entry_field_ids = [entry_field["id"] for entry_field in entry_fields]
    saved_sig_counter = Counter(st.session_state.get("ta_detailed_saved_sigs", []))
    saved_row_flags: list[bool] = []
    for idx in range(number_of_accounts):
        row_sig = _row_signature(idx, entry_field_ids)
        if saved_sig_counter[row_sig] > 0:
            saved_sig_counter[row_sig] -= 1
            saved_row_flags.append(True)
        else:
            saved_row_flags.append(False)

    for idx in range(number_of_accounts):
        is_saved_row = saved_row_flags[idx]
        with st.container(border=True):
            # Hidden marker carrying this row's saved/draft state. Static CSS uses
            # :has() on it to darken the card fill and thicken its border for saved
            # rows — without touching the inputs. Always rendered (constant element
            # position) so toggling state never re-mounts the row's widgets.
            st.markdown(
                f"<div class='ta-row-marker{' ta-row-saved' if is_saved_row else ''}'></div>",
                unsafe_allow_html=True,
            )
            c1, c2, c3, c4, c5 = st.columns([3.2, 2.3, 2.6, 2, 1])
            with c1:
                account_key = f"ta_detailed_account_{idx}"
                name_col, star_col = st.columns([7, 1], vertical_alignment="center")
                with name_col:
                    account = st.selectbox(
                        "Reporting Name",
                        options=_account_options_for_row(account_options, st.session_state.get(account_key, "")),
                        key=account_key,
                        disabled=editing_locked,
                        label_visibility="collapsed",
                        on_change=_on_reporting_name_change,
                        args=(idx, account_lookup),
                        # Favorited names display a star; the stored value is unchanged.
                        format_func=lambda n: (f"⭐ {n}" if n in favorite_names else n),
                    )
                with star_col:
                    selected_name = str(st.session_state.get(account_key, "") or "").strip()
                    is_fav = selected_name in favorite_names
                    if st.button(
                        "★" if is_fav else "☆",
                        key=f"ta_detailed_fav_btn_{idx}",
                        help=(
                            "Remove this Reporting Name from favorites"
                            if is_fav
                            else "Pin this Reporting Name to the top of the dropdown"
                        ),
                        # Favoriting is a personal preference, not a data edit, so it
                        # stays available even on read-only days (only needs a name).
                        disabled=not selected_name,
                        type="tertiary",
                        width="content",
                    ):
                        _toggle_account_favorite(selected_name)
                        _rerun_input_fragment()
            with c2:
                custcode_key = f"ta_detailed_custcode_{idx}"
                code_pool = _customer_code_pool_for_reporting_name(
                    account_lookup, st.session_state.get(account_key, "")
                )
                customer_code = st.selectbox(
                    "Customer Code",
                    options=_customer_code_options_for_row(
                        [""] + code_pool,
                        st.session_state.get(custcode_key, ""),
                    ),
                    key=custcode_key,
                    disabled=editing_locked,
                    label_visibility="collapsed",
                    on_change=_on_customer_code_change,
                    args=(idx, account_lookup),
                )
            with c3:
                h_col, m_col = st.columns(2)
                with h_col:
                    hour_value = st.selectbox(
                        "Hours",
                        options=TIME_HOUR_OPTIONS,
                        key=f"ta_detailed_dur_h_{idx}",
                        disabled=editing_locked,
                        label_visibility="collapsed",
                        format_func=lambda h: f"{h} hr",
                    )
                with m_col:
                    minute_value = st.selectbox(
                        "Minutes",
                        options=TIME_MINUTE_OPTIONS,
                        key=f"ta_detailed_dur_m_{idx}",
                        disabled=editing_locked,
                        label_visibility="collapsed",
                        format_func=lambda m: f"{m:02d} min",
                    )
                duration = _hm_to_duration(hour_value, minute_value)
            with c4:
                channel = st.selectbox(
                    "Channel",
                    options=_channel_options_for_row(st.session_state.get(f"ta_detailed_channel_{idx}", "")),
                    key=f"ta_detailed_channel_{idx}",
                    disabled=editing_locked,
                    label_visibility="collapsed",
                )
            with c5:
                if st.button(
                    "Delete",
                    key=f"ta_detailed_delete_btn_{idx}",
                    icon=":material/delete_outline:",
                    disabled=editing_locked or (number_of_accounts <= 1 and not has_saved_rows_for_day),
                    type="tertiary",
                    width="content",
                ):
                    if number_of_accounts <= 1:
                        st.session_state.ta_delete_payload = {
                            "entry_date": selected_day.isoformat(),
                            "saved_row_count": int(len(selected_day_df.index)),
                        }
                        st.session_state.ta_delete_confirm_open = True
                        st.session_state.ta_delete_confirm_rendered = False
                        # Opens a confirmation dialog — keep this an app rerun.
                        st.rerun()
                    else:
                        st.session_state["ta_detailed_delete_idx"] = idx
                        # Deleting a row changes the element count — use a full rerun,
                        # not a fragment-scoped one, so the browser rebuilds the tree
                        # cleanly. A partial delta against the now-shorter row list (with
                        # the re-mounting st_calendar in the same subtree) is what blanks
                        # the whole app. The queued delete is applied at the fragment top,
                        # which still runs on a full rerun.
                        st.rerun()

            custom_values: dict[str, object] = {}
            for chunk_start in range(0, len(entry_fields), 4):
                chunk = entry_fields[chunk_start:chunk_start + 4]
                cf_cols = st.columns(4)
                for slot, entry_field in enumerate(chunk):
                    with cf_cols[slot]:
                        custom_values[entry_field["id"]] = _render_custom_field_widget(
                            entry_field, idx, editing_locked
                        )

        rows.append(
            {
                "account": account,
                "customer_code": _effective_customer_code(account, customer_code),
                "duration": duration,
                "channel": channel,
                "custom_values": custom_values,
            }
        )

    # Add Row — full-width dashed "ghost" card (styled via .st-key-ta_add_row_wrap),
    # echoing the calendar's empty-day "click to add" hint so adding feels like part of
    # the list rather than a detached button.
    with st.container(key="ta_add_row_wrap"):
        if st.button(
            "Add Row",
            key="ta_detailed_add_row_btn",
            icon=":material/add:",
            type="secondary",
            disabled=editing_locked,
            width="stretch",
        ):
            st.session_state["ta_detailed_count"] = number_of_accounts + 1
            # Adding a row changes the fragment's element count. A fragment-scoped
            # rerun sends the browser a PARTIAL delta for the changed subtree — which
            # also contains the st_calendar component that re-mounts on every render —
            # and the frontend intermittently fails to reconcile it, blanking the whole
            # app (sidebar and all). A full rerun rebuilds the tree from scratch (the
            # same robust path Save uses), so it can't hit that mismatch.
            st.rerun()

    save_col, _spacer_col, refresh_col = st.columns([2, 5, 2])
    with save_col:
        save_detailed_clicked = st.button(
            "Save Allocation",
            type="primary",
            key="ta_save_detailed",
            disabled=editing_locked,
            width="stretch",
        )

    with refresh_col:
        # Right-aligned (fills this rightmost column so its edge lines up with the
        # input fields above). Independent of the day-edit lock; reloading the
        # account data is always allowed; it never touches saved allocations.
        if st.button(
            "Refresh Account Data",
            key="ta_refresh_accounts_btn",
            icon=":material/refresh:",
            type="secondary",
            width="stretch",
            help="Updates the Reporting Name and Customer Code fields.",
        ):
            _refresh_account_data(number_of_accounts)
        _render_last_refresh_caption()

    if save_detailed_clicked:
        errors: list[str] = []
        parsed_seconds = []
        for idx, row in enumerate(rows, start=1):
            if not _empty_to_none(row["account"]):
                errors.append(f"Reporting Name for row {idx} is required.")
            seconds = utils.parse_hhmmss(str(row["duration"]))
            if seconds <= 0:
                errors.append(f"Time for row {idx} must be greater than 0 hr 00 min.")
            parsed_seconds.append(max(0, seconds))
            row_custom = row.get("custom_values") or {}
            for entry_field in entry_fields:
                if not entry_field["required"]:
                    continue
                if not _is_field_value_present(entry_field, row_custom.get(entry_field["id"])):
                    errors.append(f"Row {idx}: '{entry_field['name']}' is required.")
        if errors:
            for message in errors:
                st.error(message)
            return

        st.session_state.ta_confirm_payload = {
            "entry_date": selected_day.isoformat(),
            "rows": rows,
            "parsed_seconds": parsed_seconds,
        }
        st.session_state.ta_confirm_open = True
        st.session_state.ta_confirm_rendered = False
        st.rerun()

    if st.session_state.ta_confirm_open and not st.session_state.ta_confirm_rendered:
        st.session_state.ta_confirm_rendered = True
        confirm_time_allocation_submit_dialog(user_login, full_name, department)
    if st.session_state.ta_delete_confirm_open and not st.session_state.ta_delete_confirm_rendered:
        st.session_state.ta_delete_confirm_rendered = True
        confirm_time_allocation_delete_dialog(user_login, full_name)


def _ensure_time_allocation_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a dataframe to the current parquet schema (adds missing columns, drops unknown ones, coerces custom-field types)."""
    fixed = df.copy()
    schema = _current_time_allocation_schema()
    for col_name in schema.names:
        if col_name not in fixed.columns:
            fixed[col_name] = None
    fixed["Entry Date"] = pd.to_datetime(fixed["Entry Date"], errors="coerce").dt.date
    for entry_field in _load_entry_fields():
        col = f"cf_{entry_field['id']}"
        if col not in fixed.columns:
            continue
        ftype = entry_field["type"]
        if ftype == "number":
            fixed[col] = pd.to_numeric(fixed[col], errors="coerce")
        elif ftype == "date":
            fixed[col] = pd.to_datetime(fixed[col], errors="coerce").dt.date
    return fixed[schema.names].copy()


def _build_user_day_mask(df: pd.DataFrame, user_login: str, full_name: str, entry_date: date) -> pd.Series:
    normalized = _ensure_time_allocation_columns(df)
    date_match = normalized["Entry Date"].eq(entry_date)

    login_key = _normalize_login(user_login)
    full_name_key = str(full_name or "").strip().lower()

    user_match = normalized["User"].fillna("").astype(str).map(_normalize_login).eq(login_key)
    if full_name_key:
        full_name_match = normalized["Full Name"].fillna("").astype(str).str.strip().str.lower().eq(full_name_key)
        user_match = user_match | full_name_match
    return date_match & user_match


def _replace_user_day_entries(
    base_dir: Path,
    user_login: str,
    full_name: str,
    entry_date: date,
    skip_path: Path | None = None,
) -> tuple[int, int]:
    """Remove existing rows for (user, entry_date) across export files.

    ``skip_path`` (the save's own target file) is left untouched: the caller is about
    to overwrite it wholesale via atomic_write_parquet, so reading then deleting it
    first would be two wasted UNC round-trips. Pass it on save; leave it None on delete
    (which must actually remove the file)."""
    removed_rows = 0
    touched_files = 0
    skip_key = str(skip_path).lower() if skip_path is not None else None
    for file_path in _iter_user_day_candidate_files(base_dir, user_login, full_name, entry_date):
        if skip_key is not None and str(file_path).lower() == skip_key:
            continue
        try:
            file_df = pd.read_parquet(file_path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable export file during replace '%s': %s", file_path, exc)
            continue
        if file_df.empty:
            continue

        mask = _build_user_day_mask(file_df, user_login, full_name, entry_date)
        if not bool(mask.any()):
            continue

        touched_files += 1
        removed_rows += int(mask.sum())
        remaining = _ensure_time_allocation_columns(file_df.loc[~mask].copy())
        try:
            if remaining.empty:
                file_path.unlink(missing_ok=True)
            else:
                utils.atomic_write_parquet(remaining, file_path, schema=_current_time_allocation_schema())
        except Exception as exc:
            LOGGER.exception("Failed to update export file '%s' while replacing rows: %s", file_path, exc)
            raise
    return removed_rows, touched_files


def _invalidate_input_seed() -> None:
    """Force the selected-day editor to reload from persisted data on next rerun."""
    st.session_state.pop("ta_loaded_day_token", None)


_WINDOW_OVERRIDE_COLUMNS = [
    "Entry Date",
    "User",
    "Full Name",
    "Department",
    "Account",
    "Customer Code",
    "Time",
    "Channel",
    "Source File",
]

_WINDOW_OVERRIDE_TTL_SECONDS = 60.0


def _store_window_override(entry_date: date, override_df: pd.DataFrame) -> None:
    """Stash freshly-saved rows for a date so the next render can skip the UNC reload."""
    overrides = st.session_state.get("ta_input_window_overrides")
    if not isinstance(overrides, dict):
        overrides = {}
    overrides[entry_date.isoformat()] = {
        "df": override_df.copy(),
        "stored_at": time.monotonic(),
    }
    st.session_state["ta_input_window_overrides"] = overrides


def _build_window_override_from_records(
    rows: list[dict[str, object]],
    parsed_seconds: list[int],
    user_login: str,
    full_name: str,
    department: str,
    entry_date: date,
) -> pd.DataFrame:
    """Build a window-shaped DataFrame from the rows we just persisted."""
    records: list[dict[str, object]] = []
    for row, seconds in zip(rows, parsed_seconds):
        records.append({
            "Entry Date": entry_date,
            "User": _empty_to_none(user_login),
            "Full Name": _empty_to_none(full_name),
            "Department": _empty_to_none(department),
            "Account": _empty_to_none(row.get("account")),
            "Customer Code": _empty_to_none(row.get("customer_code")),
            "Time": utils.format_hhmmss(int(max(0, seconds))),
            "Channel": _empty_to_none(row.get("channel")),
            "Source File": "",
        })
    if not records:
        return pd.DataFrame(columns=_WINDOW_OVERRIDE_COLUMNS)
    df = pd.DataFrame(records)
    df["Entry Date"] = pd.to_datetime(df["Entry Date"], errors="coerce").dt.date
    return df[_WINDOW_OVERRIDE_COLUMNS].copy()


def _apply_window_overrides(
    window_df: pd.DataFrame,
    window_start: date,
    window_end: date,
) -> pd.DataFrame:
    """Splice saved/deleted-row overrides into the loaded window DataFrame."""
    overrides = st.session_state.get("ta_input_window_overrides")
    if not isinstance(overrides, dict) or not overrides:
        return window_df

    now = time.monotonic()
    relevant: dict[date, pd.DataFrame] = {}
    stale_keys: list[str] = []
    for date_iso, entry in overrides.items():
        if not isinstance(entry, dict):
            stale_keys.append(date_iso)
            continue
        stored_at = entry.get("stored_at")
        if not isinstance(stored_at, (int, float)) or (now - stored_at) > _WINDOW_OVERRIDE_TTL_SECONDS:
            stale_keys.append(date_iso)
            continue
        override_df = entry.get("df")
        if not isinstance(override_df, pd.DataFrame):
            stale_keys.append(date_iso)
            continue
        try:
            d = pd.to_datetime(date_iso).date()
        except Exception:
            stale_keys.append(date_iso)
            continue
        if window_start <= d <= window_end:
            relevant[d] = override_df

    if stale_keys:
        for key in stale_keys:
            overrides.pop(key, None)
        st.session_state["ta_input_window_overrides"] = overrides

    if not relevant:
        return window_df

    base = window_df
    if not base.empty and "Entry Date" in base.columns:
        base = base[~base["Entry Date"].isin(set(relevant.keys()))].copy()

    extra_frames = [df for df in relevant.values() if not df.empty]
    if not extra_frames:
        return base
    return pd.concat([base, *extra_frames], ignore_index=True)


def _queue_input_status(level: str, message: str) -> None:
    """Persist a one-shot status message across reruns."""
    st.session_state["ta_input_status"] = {
        "level": str(level or "").strip().lower(),
        "message": str(message or "").strip(),
    }


def _render_input_status() -> None:
    """Render and clear the most recent persisted input status message."""
    payload = st.session_state.pop("ta_input_status", None)
    if not isinstance(payload, dict):
        return

    message = str(payload.get("message") or "").strip()
    if not message:
        return

    level = str(payload.get("level") or "").strip().lower()
    if level == "success":
        st.success(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)


def save_records(
    rows: list[dict[str, object]],
    parsed_seconds: list[int],
    user_login: str,
    full_name: str,
    department: str,
    entry_date: date,
) -> bool:
    """Build and persist time-allocation records to a parquet file."""
    if not _is_editable_day(entry_date):
        window_start, window_end = _editable_window()
        st.error(
            f"Editing is limited to this week and last week "
            f"({window_start:%m/%d/%Y}–{window_end:%m/%d/%Y}). "
            f"Cannot save changes for {entry_date:%m/%d/%Y}."
        )
        return False

    entry_fields = _load_entry_fields()
    records: list[dict[str, object]] = []
    for row, seconds in zip(rows, parsed_seconds):
        record: dict[str, object] = {
            "Entry Date": entry_date,
            "User": _empty_to_none(user_login),
            "Full Name": _empty_to_none(full_name),
            "Department": _empty_to_none(department),
            "Account": _empty_to_none(row.get("account")),
            "Customer Code": _empty_to_none(row.get("customer_code")),
            "Time": utils.format_hhmmss(seconds),
            "Channel": _empty_to_none(row.get("channel")),
        }
        custom_values = row.get("custom_values") or {}
        for entry_field in entry_fields:
            col = f"cf_{entry_field['id']}"
            record[col] = _serialize_custom_value(entry_field, custom_values.get(entry_field["id"]))
        records.append(record)

    df = _ensure_time_allocation_columns(pd.DataFrame(records))
    output_path = _get_time_allocation_daily_file(TIME_ALLOCATION_DIR, user_login, full_name, entry_date)
    try:
        target_existed = output_path.is_file()
        # Skip the target file in the replace step — atomic_write_parquet below
        # overwrites it wholesale, so reading + deleting it first is wasted I/O. Only
        # other (legacy) files holding this user/day need their rows removed.
        removed_rows, touched_files = _replace_user_day_entries(
            TIME_ALLOCATION_DIR, user_login, full_name, entry_date, skip_path=output_path
        )
        utils.atomic_write_parquet(df, output_path, schema=_current_time_allocation_schema())
        _invalidate_admin_export_tables()
        _store_window_override(
            entry_date,
            _build_window_override_from_records(
                rows, parsed_seconds, user_login, full_name, department, entry_date
            ),
        )
        _invalidate_input_seed()
        LOGGER.info(
            "Saved time allocation export | rows=%s file='%s' user='%s' entry_date=%s target_existed=%s legacy_removed=%s touched_files=%s",
            len(df),
            str(output_path),
            user_login,
            entry_date,
            target_existed,
            removed_rows,
            touched_files,
        )
        if target_existed or removed_rows > 0:
            _queue_input_status(
                "success",
                f"Updated {entry_date:%m/%d/%Y}: saved {len(df)} row(s).",
            )
        else:
            _queue_input_status("success", f"Saved {len(df)} row(s) for {entry_date:%m/%d/%Y}.")
        return True
    except Exception as exc:
        LOGGER.exception("Failed to save time allocation export: %s", exc)
        st.error(f"Failed to save parquet file: {exc}")
        return False


def delete_records_for_day(user_login: str, full_name: str, entry_date: date) -> bool:
    """Delete all saved rows for one user/day and refresh the editor state."""
    if not _is_editable_day(entry_date):
        window_start, window_end = _editable_window()
        st.error(
            f"Editing is limited to this week and last week "
            f"({window_start:%m/%d/%Y}–{window_end:%m/%d/%Y}). "
            f"Cannot delete entries for {entry_date:%m/%d/%Y}."
        )
        return False

    try:
        removed_rows, touched_files = _replace_user_day_entries(TIME_ALLOCATION_DIR, user_login, full_name, entry_date)
        _invalidate_admin_export_tables()
        _store_window_override(entry_date, pd.DataFrame(columns=_WINDOW_OVERRIDE_COLUMNS))
        _invalidate_input_seed()
        LOGGER.info(
            "Deleted time allocation export rows | user='%s' entry_date=%s removed_rows=%s touched_files=%s",
            user_login,
            entry_date,
            removed_rows,
            touched_files,
        )
        if removed_rows > 0:
            _queue_input_status(
                "success",
                f"Deleted {removed_rows} saved row(s) for {entry_date:%m/%d/%Y}.",
            )
        else:
            _queue_input_status("info", f"No saved rows were found for {entry_date:%m/%d/%Y}.")
        return True
    except Exception as exc:
        LOGGER.exception("Failed to delete time allocation rows: %s", exc)
        st.error(f"Failed to delete saved rows: {exc}")
        return False


@st.dialog("Submit Allocation?")
def confirm_time_allocation_submit_dialog(user_login: str, full_name: str, department: str) -> None:
    """Confirmation modal for time-allocation submission."""
    payload = st.session_state.get("ta_confirm_payload")
    if not isinstance(payload, dict):
        st.error("No pending submission found.")
        st.session_state.ta_confirm_open = False
        st.session_state.ta_confirm_rendered = False
        return

    entry_date_dt = pd.to_datetime(payload.get("entry_date"), errors="coerce")
    if pd.isna(entry_date_dt):
        st.error("Invalid submission date.")
        st.session_state.ta_confirm_open = False
        st.session_state.ta_confirm_rendered = False
        return
    entry_date = entry_date_dt.date()

    rows: list[dict[str, object]] = list(payload.get("rows") or [])
    parsed_seconds: list[int] = [int(max(0, s)) for s in list(payload.get("parsed_seconds") or [])]
    display_user = full_name.strip() if str(full_name).strip() else user_login

    st.caption(f"**User:** {display_user}")
    st.caption(f"**Department:** {department or 'N/A'}")
    st.caption(f"**Entry Date:** {entry_date:%m/%d/%Y}")
    st.divider()

    entry_fields = _load_entry_fields()
    preview_rows: list[dict[str, object]] = []
    for row, seconds in zip(rows, parsed_seconds):
        preview: dict[str, object] = {
            "Reporting Name": _empty_to_none(row.get("account")) or "",
            "Customer Code": _empty_to_none(row.get("customer_code")) or "",
            "Time": utils.format_hhmmss(seconds),
            "Channel": _empty_to_none(row.get("channel")) or "",
        }
        custom_values = row.get("custom_values") or {}
        for entry_field in entry_fields:
            preview[entry_field["name"]] = _format_field_value_for_preview(
                entry_field, custom_values.get(entry_field["id"])
            )
        preview_rows.append(preview)
    preview_df = pd.DataFrame(preview_rows)
    st.dataframe(preview_df, hide_index=True, width="stretch")

    left, right = st.columns(2)
    with left:
        if st.button("Confirm", type="primary", width="stretch", key="ta_confirm_submit_button"):
            if save_records(rows, parsed_seconds, user_login, full_name, department, entry_date):
                st.session_state.ta_confirm_open = False
                st.session_state.ta_confirm_rendered = False
                st.session_state.ta_confirm_payload = None
                st.rerun()
    with right:
        if st.button("Cancel", width="stretch", key="ta_confirm_cancel_button"):
            st.session_state.ta_confirm_open = False
            st.session_state.ta_confirm_rendered = False
            st.session_state.ta_confirm_payload = None
            st.rerun()


@st.dialog("Delete Saved Allocation?")
def confirm_time_allocation_delete_dialog(user_login: str, full_name: str) -> None:
    """Confirmation modal for deleting one user's saved rows for a selected day."""
    payload = st.session_state.get("ta_delete_payload")
    if not isinstance(payload, dict):
        st.error("No pending deletion found.")
        st.session_state.ta_delete_confirm_open = False
        st.session_state.ta_delete_confirm_rendered = False
        st.session_state.ta_delete_payload = None
        return

    entry_date_dt = pd.to_datetime(payload.get("entry_date"), errors="coerce")
    if pd.isna(entry_date_dt):
        st.error("Invalid deletion date.")
        st.session_state.ta_delete_confirm_open = False
        st.session_state.ta_delete_confirm_rendered = False
        st.session_state.ta_delete_payload = None
        return

    entry_date = entry_date_dt.date()
    saved_row_count = int(max(0, payload.get("saved_row_count") or 0))
    display_user = full_name.strip() if str(full_name).strip() else user_login

    st.caption(f"**User:** {display_user}")
    st.caption(f"**Entry Date:** {entry_date:%m/%d/%Y}")
    st.warning(f"This will permanently delete {saved_row_count:,} saved row(s) for this day.")

    left, right = st.columns(2)
    with left:
        if st.button("Delete Saved Rows", type="primary", width="stretch", key="ta_confirm_delete_button"):
            if delete_records_for_day(user_login, full_name, entry_date):
                st.session_state.ta_delete_confirm_open = False
                st.session_state.ta_delete_confirm_rendered = False
                st.session_state.ta_delete_payload = None
                st.rerun()
    with right:
        if st.button("Cancel", width="stretch", key="ta_cancel_delete_button"):
            st.session_state.ta_delete_confirm_open = False
            st.session_state.ta_delete_confirm_rendered = False
            st.session_state.ta_delete_payload = None
            st.rerun()


def _attach_fiscal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'Fiscal Year' and 'Fiscal Period' columns derived from Entry Date."""
    out = df.copy()
    out["Fiscal Year"] = pd.NA
    out["Fiscal Period"] = pd.NA
    if out.empty:
        return out

    periods = utils.load_fiscal_periods()
    if periods.empty:
        return out

    entry_dates = pd.to_datetime(out["Entry Date"], errors="coerce").dt.date
    for _, period_row in periods.iterrows():
        mask = entry_dates.ge(period_row["StartDate"]) & entry_dates.le(period_row["EndDate"])
        if not bool(mask.any()):
            continue
        period_label = str(period_row["PeriodName"] or "").strip() or f"Period {int(period_row['PeriodNumber'])}"
        out.loc[mask, "Fiscal Year"] = int(period_row["Year"])
        out.loc[mask, "Fiscal Period"] = period_label
    return out


def _resolve_downloads_dir() -> Path:
    """
    Return the current user's Downloads folder.

    Honors Windows known-folder redirection (e.g. OneDrive) by reading the
    Downloads GUID from the Shell Folders registry key, which stores fully
    expanded absolute paths. Falls back to ~/Downloads if anything fails.
    """
    try:
        import winreg

        shell_folders_key = r"Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders"
        downloads_guid = "{374DE290-123F-4565-9164-39C4925E467B}"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, shell_folders_key) as key:
            value, _ = winreg.QueryValueEx(key, downloads_guid)
        candidate = Path(str(value).strip())
        if str(candidate).strip():
            return candidate
    except Exception:
        pass
    return Path.home() / "Downloads"


def _filter_state_token(applied: dict | None) -> str:
    """Stable token of the applied-filter state, used to remount widgets on change."""
    if applied is None:
        return "recent"
    return "_".join(
        str(applied.get(key, "")) for key in ("date_from", "date_to", "year", "period", "user", "dept")
    )


def _editor_seed_signature(df: pd.DataFrame) -> str:
    """Short stable hash of the displayed rows' (Source File, within-file position)
    identities. Folded into the data-editor key so the editor remounts whenever the
    underlying row SET changes — including a delete-save where the top-100 view
    backfills to the same row count — discarding any stale positional edit that would
    otherwise re-target a different on-disk row. It is unchanged across autofill
    reruns (same data), so in-progress edits are preserved."""
    if df is None or df.empty or "Source File" not in df.columns or "_source_pos" not in df.columns:
        return "empty"
    pairs = "|".join(
        f"{sf}:{sp}"
        for sf, sp in zip(df["Source File"].astype(str), df["_source_pos"].astype(str))
    )
    return hashlib.md5(pairs.encode("utf-8")).hexdigest()[:12]


def _apply_export_filters(df: pd.DataFrame, applied: dict) -> pd.DataFrame:
    """Apply the in-memory date / fiscal-year / period / user / department refinements
    to an already date-windowed frame."""
    out = df.copy()
    out["Entry Date"] = pd.to_datetime(out["Entry Date"], errors="coerce").dt.date
    date_from = applied.get("date_from")
    date_to = applied.get("date_to")
    if date_from is not None and date_to is not None:
        out = out[(out["Entry Date"] >= date_from) & (out["Entry Date"] <= date_to)]
    if applied.get("year"):
        out = out[pd.to_numeric(out["Fiscal Year"], errors="coerce").eq(int(applied["year"])).fillna(False)]
    if applied.get("period"):
        out = out[out["Fiscal Period"].fillna("").astype(str).str.strip().eq(applied["period"])]
    if applied.get("user"):
        out = out[out["Full Name"].fillna("").astype(str).str.strip().eq(applied["user"])]
    if applied.get("dept"):
        out = out[out["Department"].fillna("").astype(str).str.strip().eq(applied["dept"])]
    return out


def _export_data_date_bounds(base_dir: Path) -> tuple[date, date] | None:
    """Min/max entry date across ALL saved files, parsed from filenames only (no
    parquet reads, so it's cheap). Each file's name date IS the entry date of every
    row in it. Returns None when there are no dated files."""
    dates: list[date] = []
    for path in _iter_time_allocation_files(base_dir):
        day = _date_from_ta_filename(path)
        if day is not None:
            dates.append(day)
    if not dates:
        return None
    return min(dates), max(dates)


def _enter_full_export_load(prefix: str, base_dir: Path) -> None:
    """Switch an admin table from the collapsed top-100 view to the full-history view:
    set ``applied`` to the full data date range, which on the next run loads every entry
    and reveals the filter form (whose date inputs then default to that full range — see
    ``_export_filter_panel``). Triggered by the Load All Data button."""
    bounds = _export_data_date_bounds(base_dir)
    if bounds is None:
        low = high = _today_eastern()
    else:
        low, high = bounds
    st.session_state[f"{prefix}_applied"] = {
        "date_from": low,
        "date_to": high,
        "year": "",
        "period": "",
        "user": "",
        "dept": "",
        "sig": (low.isoformat(), high.isoformat()),
    }
    try:
        st.rerun(scope="fragment")
    except Exception:
        st.rerun()


def _export_filter_panel(prefix: str, base_dir: Path) -> tuple[pd.DataFrame, dict | None]:
    """Shared admin-table data source + collapsed/expanded gating.

    Returns ``(base_df, applied)``. ``applied`` is None until the user clicks **Load
    All Data**; while None the table is *collapsed*: ``base_df`` is just the most-recent
    rows (``_load_recent_exports``) so the table opens cheaply, NO filter form is shown,
    and a Load All Data button (rendered here) plus a "showing 100 most-recent" caption
    invite the user to load everything. Clicking it sets ``applied`` to the full data date
    range, which *expands* the table: ``base_df`` becomes the date-windowed load for the
    chosen range (shown with a progress bar, cached per session by the window so unrelated
    reruns don't reload) and the filter form is rendered. Fiscal columns are attached. The
    caller refines ``base_df`` with ``_apply_export_filters`` and renders its own table.
    """
    applied = st.session_state.get(f"{prefix}_applied")

    # Collapsed initial view: only the most-recent rows, no filter form. The Load All
    # Data button loads the full history and reveals the filters on the next run.
    if applied is None:
        base_df = _attach_fiscal_columns(_load_recent_exports(base_dir))
        if not base_df.empty:
            st.caption(
                "Showing the 100 most-recent entries only. "
                "To view all entries and use filters, click **Load All Data**."
            )
            if st.button("Load All Data", type="primary", key=f"{prefix}_load_all_btn"):
                _enter_full_export_load(prefix, base_dir)
        return base_df, applied

    # Expanded view: load the chosen window (cached per session by its date range so
    # unrelated reruns don't reload) and render the filter form below.
    sig = applied.get("sig")
    if st.session_state.get(f"{prefix}_loaded_sig") != sig:
        progress_bar = st.progress(0.0, text="Loading entries…")
        try:
            loaded = _load_exports_window(
                base_dir, applied["date_from"], applied["date_to"], progress_bar
            )
        finally:
            progress_bar.empty()
        st.session_state[f"{prefix}_loaded_df"] = loaded
        st.session_state[f"{prefix}_loaded_sig"] = sig
    base_df = st.session_state.get(f"{prefix}_loaded_df", pd.DataFrame())

    base_df = _attach_fiscal_columns(base_df)

    today = _today_eastern()
    periods = utils.load_fiscal_periods()
    if periods is not None and not periods.empty:
        bound_min = min(periods["StartDate"].tolist())
        bound_max = max(periods["EndDate"].tolist())
    else:
        bound_min = today
        bound_max = today
    entry_dates = (
        pd.to_datetime(base_df["Entry Date"], errors="coerce").dropna()
        if not base_df.empty
        else pd.Series([], dtype="datetime64[ns]")
    )
    data_min = entry_dates.min().date() if not entry_dates.empty else today
    data_max = entry_dates.max().date() if not entry_dates.empty else today
    widget_min = min(bound_min, data_min, today)
    widget_max = max(bound_max, data_max, today)
    # The filter form only appears after Load All Data (the full history is already
    # loaded), so seed the date inputs to the full data extent — the table and the inputs
    # both read "everything" until the admin narrows the range. This seeds the FIRST
    # render only; the date widget owns its value (persisted under {prefix}_from/_to)
    # afterward, so subsequent Apply Filters / saves keep the admin's chosen range.
    default_from = data_min
    default_to = data_max

    # Drop a stored date the (possibly shifted) bounds no longer allow, so
    # st.date_input doesn't raise on an out-of-range value.
    for date_key in (f"{prefix}_from", f"{prefix}_to"):
        stored_date = st.session_state.get(date_key)
        if isinstance(stored_date, date) and not (widget_min <= stored_date <= widget_max):
            st.session_state.pop(date_key, None)

    def _opts(column: str) -> list[str]:
        if base_df.empty or column not in base_df.columns:
            return [""]
        values = sorted(
            {str(v).strip() for v in base_df[column].dropna().astype(str).tolist() if str(v).strip()}
        )
        return [""] + values

    year_opts = (
        [""]
        + [
            str(y)
            for y in sorted(
                {int(y) for y in pd.to_numeric(base_df["Fiscal Year"], errors="coerce").dropna().tolist()}
            )
        ]
        if not base_df.empty
        else [""]
    )
    period_opts = _opts("Fiscal Period")
    user_opts = _opts("Full Name")
    dept_opts = _opts("Department")

    # Drop any stored selection that's no longer valid for the current data set.
    for state_key, opts in (
        (f"{prefix}_year", year_opts),
        (f"{prefix}_period", period_opts),
        (f"{prefix}_user", user_opts),
        (f"{prefix}_dept", dept_opts),
    ):
        if st.session_state.get(state_key) not in opts:
            st.session_state[state_key] = ""

    with st.form(f"{prefix}_form"):
        c1, c2, c3, c4 = st.columns(4)
        date_from = c1.date_input(
            "From", value=default_from, min_value=widget_min, max_value=widget_max, key=f"{prefix}_from"
        )
        date_to = c2.date_input(
            "To", value=default_to, min_value=widget_min, max_value=widget_max, key=f"{prefix}_to"
        )
        sel_year = c3.selectbox("Fiscal Year", options=year_opts, key=f"{prefix}_year")
        sel_period = c4.selectbox("Fiscal Period", options=period_opts, key=f"{prefix}_period")
        c5, c6, _c7, _c8 = st.columns(4)
        sel_user = c5.selectbox("User", options=user_opts, key=f"{prefix}_user")
        sel_dept = c6.selectbox("Department", options=dept_opts, key=f"{prefix}_dept")
        submitted = st.form_submit_button("Apply Filters", type="primary")

    if submitted:
        low, high = (date_from, date_to) if date_from <= date_to else (date_to, date_from)
        st.session_state[f"{prefix}_applied"] = {
            "date_from": low,
            "date_to": high,
            "year": sel_year,
            "period": sel_period,
            "user": sel_user,
            "dept": sel_dept,
            "sig": (low.isoformat(), high.isoformat()),
        }
        try:
            st.rerun(scope="fragment")
        except Exception:
            st.rerun()

    return base_df, applied


@st.fragment
def render_exports_view() -> None:
    """Render admin export view: top 100 most-recent rows by default with a Load All Data
    button; the filter form appears (and the full history loads) once it is clicked."""
    if not utils.is_current_user_admin():
        st.info("Sorry, you don't have access to this section")
        return

    base_df, applied = _export_filter_panel("ta_export", TIME_ALLOCATION_DIR)
    if base_df.empty:
        st.info(
            "No time-allocation exports found."
            if applied is None
            else "No entries match the current filters."
        )
        return

    if applied is None:
        filtered = base_df.copy()
    else:
        filtered = _apply_export_filters(base_df, applied)

    filtered = filtered.copy()
    filtered["Entry Date"] = pd.to_datetime(filtered["Entry Date"], errors="coerce").dt.date

    # Total Minutes mirrors the Time column (HH:MM[:SS]) converted to whole minutes.
    filtered["Total Minutes"] = (
        filtered["Time"].fillna("").astype(str).map(utils.parse_hhmmss).clip(lower=0).div(60).round().astype(int)
    )

    column_order = [
        "Entry Date",
        "Fiscal Year",
        "Fiscal Period",
        "User",
        "Full Name",
        "Department",
        "Account",
        "Customer Code",
        "Time",
        "Total Minutes",
        "Channel",
        "Source File",
    ]
    filtered = filtered[[c for c in column_order if c in filtered.columns]]

    display_df = filtered.drop(columns=["User", "Source File"], errors="ignore")
    st.dataframe(display_df, hide_index=True, width="stretch")

    # The app runs locally on the user's own Windows machine, so we save the
    # CSV straight to their Downloads folder rather than relying on a browser
    # download (st.download_button was silently doing nothing for users).
    if st.button("Download CSV", key="ta_export_download_csv", disabled=filtered.empty):
        file_name = f"time_allocation_export_{utils.to_eastern(utils.now_utc()):%Y%m%d_%H%M%S}.csv"
        try:
            downloads_dir = _resolve_downloads_dir()
            downloads_dir.mkdir(parents=True, exist_ok=True)
            out_path = downloads_dir / file_name
            out_path.write_bytes(filtered.to_csv(index=False).encode("utf-8"))
            st.success(f"Saved {len(filtered):,} rows to {out_path}")
            LOGGER.info("Saved time-allocation export (%d rows) to '%s'", len(filtered), out_path)
        except Exception as exc:
            LOGGER.exception("Failed to save time-allocation export: %s", exc)
            st.error(f"Could not save the export: {exc}")


def _count_admin_editor_changes(
    original: pd.DataFrame, edited: pd.DataFrame
) -> tuple[int, int]:
    """Return (edit_count, delete_count) by comparing the editor against the seed data."""
    if len(original) != len(edited):
        return (0, 0)

    edits = 0
    deletes = 0
    for i in range(len(original)):
        o = original.iloc[i]
        e = edited.iloc[i]
        if bool(e.get("Delete", False)):
            deletes += 1
            continue
        old_account = str(o.get("Account") or "").strip()
        new_account = str(e.get("Account") or "").strip()
        old_code = str(o.get("Customer Code") or "").strip()
        new_code = str(e.get("Customer Code") or "").strip()
        old_channel = str(o.get("Channel") or "").strip()
        new_channel = str(e.get("Channel") or "").strip()
        old_seconds = utils.parse_hhmmss(str(o.get("Time") or ""))
        new_seconds = utils.parse_hhmmss(str(e.get("Time") or ""))
        if (
            old_account != new_account
            or old_code != new_code
            or old_channel != new_channel
            or old_seconds != new_seconds
        ):
            edits += 1
    return (edits, deletes)


def _apply_admin_editor_changes(original: pd.DataFrame, edited: pd.DataFrame) -> None:
    """Diff the editor against the seed data and apply per-file changes by source position."""
    if len(original) != len(edited):
        st.error("Editor row count changed unexpectedly; reload the section and retry.")
        return

    file_changes: dict[str, dict[int, dict | None]] = {}
    edit_count = 0
    delete_count = 0

    for i in range(len(original)):
        o = original.iloc[i]
        e = edited.iloc[i]
        source_file = str(o.get("Source File") or "").strip()
        try:
            source_pos = int(o.get("_source_pos"))
        except (TypeError, ValueError):
            source_pos = -1
        if not source_file or source_pos < 0:
            continue

        if bool(e.get("Delete", False)):
            file_changes.setdefault(source_file, {})[source_pos] = None
            delete_count += 1
            continue

        new_account = str(e.get("Account") or "").strip()
        new_code = str(e.get("Customer Code") or "").strip()
        new_channel = str(e.get("Channel") or "").strip()
        new_time_raw = str(e.get("Time") or "").strip()
        new_seconds = utils.parse_hhmmss(new_time_raw)

        old_account = str(o.get("Account") or "").strip()
        old_code = str(o.get("Customer Code") or "").strip()
        old_channel = str(o.get("Channel") or "").strip()
        old_seconds = utils.parse_hhmmss(str(o.get("Time") or ""))

        if (
            new_account == old_account
            and new_code == old_code
            and new_channel == old_channel
            and new_seconds == old_seconds
        ):
            continue

        if not new_account:
            st.error(f"Row {i + 1}: Account is required.")
            return
        if new_seconds <= 0:
            st.error(f"Row {i + 1}: Time must be greater than 00:00 (got '{new_time_raw}').")
            return
        if not new_channel:
            st.error(f"Row {i + 1}: Channel is required.")
            return

        file_changes.setdefault(source_file, {})[source_pos] = {
            "Account": new_account,
            "Customer Code": new_code or None,
            "Time": utils.format_hhmmss(new_seconds),
            "Channel": new_channel,
        }
        edit_count += 1

    if not file_changes:
        st.info("No changes detected.")
        return

    saved_files = 0
    failed_files = 0
    for source_file, changes in file_changes.items():
        full_path = TIME_ALLOCATION_DIR / source_file
        try:
            file_df = pd.read_parquet(full_path)
        except Exception as exc:
            LOGGER.exception("Admin editor: failed to read '%s': %s", full_path, exc)
            st.error(f"Failed to read {source_file}: {exc}")
            failed_files += 1
            continue

        # Normalize so every current schema column (e.g. a newly added
        # 'Customer Code') exists before we write into it by position.
        file_df = _ensure_time_allocation_columns(file_df)

        drop_positions: list[int] = []
        for pos, change in changes.items():
            if pos < 0 or pos >= len(file_df):
                LOGGER.warning("Admin editor: out-of-range source pos %s in '%s'", pos, full_path)
                continue
            if change is None:
                drop_positions.append(pos)
            else:
                for col, val in change.items():
                    file_df.iat[pos, file_df.columns.get_loc(col)] = val

        if drop_positions:
            file_df = file_df.drop(file_df.index[drop_positions]).reset_index(drop=True)

        try:
            if file_df.empty:
                full_path.unlink(missing_ok=True)
            else:
                normalized = _ensure_time_allocation_columns(file_df)
                utils.atomic_write_parquet(normalized, full_path, schema=_current_time_allocation_schema())
            saved_files += 1
        except Exception as exc:
            LOGGER.exception("Admin editor: failed to write '%s': %s", full_path, exc)
            st.error(f"Failed to save {source_file}: {exc}")
            failed_files += 1

    LOGGER.info(
        "Admin edited time-allocation entries | admin='%s' edits=%s deletes=%s files_saved=%s files_failed=%s",
        utils.get_os_user(),
        edit_count,
        delete_count,
        saved_files,
        failed_files,
    )
    _invalidate_admin_export_tables()
    load_time_allocation_user_window.clear()
    _invalidate_input_seed()

    if failed_files and not saved_files:
        st.session_state["ta_admin_editor_status"] = {
            "level": "error",
            "message": f"Save failed for {failed_files} file(s). Check logs.",
        }
    else:
        level = "success" if not failed_files else "info"
        suffix = f"; {failed_files} file(s) failed" if failed_files else ""
        st.session_state["ta_admin_editor_status"] = {
            "level": level,
            "message": (
                f"Saved {edit_count} edit(s) and {delete_count} deletion(s) "
                f"across {saved_files} file(s){suffix}."
            ),
        }
    st.rerun()


def _autofill_admin_editor_codes(editor_key: str, edited: pd.DataFrame, account_lookup: dict) -> bool:
    """
    Mirror the Input tab's autofill inside the admin data editor.

    When a row's Account (Reporting Name) is edited, fill its Customer Code with
    the first matching code; when Customer Code is edited, fill its Account with
    the matching Reporting Name. Mutates the data_editor's pending-edit state and
    returns True when a rerun is needed to surface the autofilled value.
    """
    state = st.session_state.get(editor_key)
    if not isinstance(state, dict):
        return False
    edited_rows = state.get("edited_rows") or {}
    changed = False
    for row_idx, changes in list(edited_rows.items()):
        try:
            i = int(row_idx)
        except (TypeError, ValueError):
            continue
        if i < 0 or i >= len(edited):
            continue
        account_edited = "Account" in changes
        code_edited = "Customer Code" in changes
        if account_edited and not code_edited:
            reporting_name = str(edited.iloc[i].get("Account") or "").strip()
            want_code = account_lookup["rn_to_first_code"].get(reporting_name, "")
            current_code = str(edited.iloc[i].get("Customer Code") or "").strip()
            if want_code and want_code != current_code:
                changes["Customer Code"] = want_code
                changed = True
        elif code_edited and not account_edited:
            customer_code = str(edited.iloc[i].get("Customer Code") or "").strip()
            want_name = account_lookup["code_to_rn"].get(customer_code, "")
            current_name = str(edited.iloc[i].get("Account") or "").strip()
            if want_name and want_name != current_name:
                changes["Account"] = want_name
                changed = True
    return changed


@st.fragment
def render_admin_data_editor_view(account_lookup: dict) -> None:
    """Admin-only editable table for any (user, day) entries, unrestricted by the Input tab's editing window."""
    st.subheader("Edit Entries", anchor=False)
    st.caption(
        "Edit Account, Customer Code, Time, or Channel inline, or tick Delete to remove rows. "
        "Editing Account or Customer Code autofills its paired value. "
        "Admins can edit any date here, regardless of the Input tab's editing window. "
        "Hidden rows within the same source file (filtered out by Department) are preserved on save."
    )

    status = st.session_state.pop("ta_admin_editor_status", None)
    if isinstance(status, dict):
        message = str(status.get("message") or "").strip()
        level = str(status.get("level") or "").lower()
        if message:
            if level == "success":
                st.success(message)
            elif level == "error":
                st.error(message)
            else:
                st.info(message)

    base_df, applied = _export_filter_panel("ta_admin_editor", TIME_ALLOCATION_DIR)
    if base_df.empty:
        st.info(
            "No time-allocation entries found."
            if applied is None
            else "No entries match the current filters."
        )
        return

    # Stable within-file position must be computed on the full loaded set (whole
    # files) before filtering, so rows hidden by the filter are not silently
    # deleted on save and edits target the right on-disk row. Fiscal columns were
    # already attached by the filter panel.
    base_df = base_df.copy()
    base_df["_source_pos"] = base_df.groupby("Source File").cumcount()

    if applied is None:
        filtered = base_df
    else:
        filtered = _apply_export_filters(base_df, applied)

    if filtered.empty:
        st.info("No entries match the current filters.")
        return

    filtered = filtered.reset_index(drop=True)
    filtered["Account"] = filtered["Account"].fillna("").astype(str)
    filtered["Customer Code"] = filtered["Customer Code"].fillna("").astype(str)
    filtered["Time"] = filtered["Time"].fillna("").astype(str)
    filtered["Channel"] = filtered["Channel"].fillna("").astype(str)

    visible_columns = [
        "Entry Date",
        "Fiscal Year",
        "Fiscal Period",
        "Full Name",
        "Department",
        "Account",
        "Customer Code",
        "Time",
        "Channel",
    ]
    editor_seed = filtered[visible_columns].copy()
    editor_seed["Delete"] = False

    base_accounts = account_lookup["reporting_names"]
    existing_accounts = [
        a for a in filtered["Account"].dropna().astype(str).str.strip().unique().tolist() if a
    ]
    account_choices = list(base_accounts) + [a for a in existing_accounts if a not in base_accounts]
    base_codes = account_lookup["customer_codes"]
    existing_codes = [
        c for c in filtered["Customer Code"].dropna().astype(str).str.strip().unique().tolist() if c
    ]
    code_choices = [""] + list(base_codes) + [c for c in existing_codes if c not in base_codes]
    existing_channels = [
        c for c in filtered["Channel"].dropna().astype(str).str.strip().unique().tolist() if c
    ]
    channel_choices = list(CHANNEL_OPTIONS) + [
        c for c in existing_channels if c not in CHANNEL_OPTIONS
    ]

    column_config = {
        "Entry Date": st.column_config.DateColumn("Entry Date", format="MM/DD/YYYY", disabled=True),
        "Fiscal Year": st.column_config.NumberColumn("Fiscal Year", format="%d", disabled=True),
        "Fiscal Period": st.column_config.TextColumn("Fiscal Period", disabled=True),
        "Full Name": st.column_config.TextColumn("Full Name", disabled=True),
        "Department": st.column_config.TextColumn("Department", disabled=True),
        "Account": st.column_config.SelectboxColumn("Reporting Name", options=account_choices, required=True),
        "Customer Code": st.column_config.SelectboxColumn("Customer Code", options=code_choices, required=False),
        "Time": st.column_config.TextColumn("Time", help="HH:MM or HH:MM:SS"),
        "Channel": st.column_config.SelectboxColumn("Channel", options=channel_choices, required=True),
        "Delete": st.column_config.CheckboxColumn("Delete", default=False),
    }

    # Remount the editor whenever the displayed row SET changes — by filter AND by the
    # row identities themselves — so a stale positional edit never reapplies to a
    # different on-disk row. (After a delete-save the top-100 view backfills to the same
    # row count, so keying on len alone would keep the editor mounted and re-target the
    # wrong row.) The signature is stable across autofill reruns, preserving edits.
    editor_key = f"ta_admin_editor_table_{_filter_state_token(applied)}_{_editor_seed_signature(filtered)}"
    edited = st.data_editor(
        editor_seed,
        column_config=column_config,
        hide_index=True,
        num_rows="fixed",
        width="stretch",
        key=editor_key,
    )

    # Mirror the Input tab's Reporting Name <-> Customer Code autofill.
    if _autofill_admin_editor_codes(editor_key, edited, account_lookup):
        st.rerun()

    edit_count, delete_count = _count_admin_editor_changes(editor_seed, edited)

    btn_col, info_col = st.columns([1, 5])
    save_clicked = btn_col.button(
        "Save Changes",
        type="primary",
        disabled=(edit_count == 0 and delete_count == 0),
        key="ta_admin_editor_save_btn",
    )
    if edit_count or delete_count:
        info_col.caption(f"{edit_count} edit(s), {delete_count} deletion(s) pending")

    if save_clicked:
        _apply_admin_editor_changes(filtered, edited)


ENTRY_FIELD_TYPES = ["text", "number", "list", "date"]


def _load_entry_fields() -> list[dict]:
    """Return the saved entry-field definitions from Time Allocation Tool settings."""
    settings = utils.load_time_allocation_settings()
    raw = settings.get("entry_fields") or []
    out: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        field_id = str(item.get("id") or "").strip()
        name = str(item.get("name") or "").strip()
        type_ = str(item.get("type") or "text").strip().lower()
        if type_ not in ENTRY_FIELD_TYPES:
            type_ = "text"
        required = bool(item.get("required") or False)
        options_raw = item.get("options") or []
        options = [str(o).strip() for o in options_raw if str(o).strip()] if isinstance(options_raw, list) else []
        if not field_id or not name:
            continue
        out.append({
            "id": field_id,
            "name": name,
            "type": type_,
            "required": required,
            "options": options,
        })
    return out


def _save_entry_fields(fields: list[dict]) -> None:
    """Persist the entry-field definitions to Time Allocation Tool settings."""
    settings = utils.load_time_allocation_settings()
    settings["entry_fields"] = fields
    utils.save_time_allocation_settings(settings)


def _parse_options_text(text: object) -> list[str]:
    """Split a one-per-line options text area into a clean list."""
    return [line.strip() for line in str(text or "").splitlines() if line.strip()]


def _entry_field_state_keys(field_id: str) -> dict:
    return {
        "name": f"taef_name_{field_id}",
        "type": f"taef_type_{field_id}",
        "required": f"taef_required_{field_id}",
        "options": f"taef_options_{field_id}",
    }


def _validate_entry_field(
    name: str, type_: str, options: list[str], existing_names_lower: set[str]
) -> str | None:
    """Return a validation error message, or None if the field definition is valid."""
    if not name:
        return "Field Name is required."
    if name.lower() in existing_names_lower:
        return f"Field Name '{name}' is already in use."
    if type_ not in ENTRY_FIELD_TYPES:
        return f"Field Type must be one of {ENTRY_FIELD_TYPES}."
    if type_ == "list" and not options:
        return "List-type fields require at least one option."
    return None


def _add_entry_field(name: object, type_: object, required: object, options_text: object) -> None:
    """Validate inputs from the Add Field form, append a new field, and persist."""
    saved_fields = _load_entry_fields()
    clean_name = str(name or "").strip()
    clean_type = str(type_ or "text").strip().lower()
    options = _parse_options_text(options_text) if clean_type == "list" else []
    existing_lower = {f["name"].lower() for f in saved_fields}

    err = _validate_entry_field(clean_name, clean_type, options, existing_lower)
    if err:
        st.error(err)
        return

    new_field = {
        "id": uuid.uuid4().hex,
        "name": clean_name,
        "type": clean_type,
        "required": bool(required),
        "options": options,
    }
    saved_fields.append(new_field)
    try:
        _save_entry_fields(saved_fields)
    except Exception as exc:
        LOGGER.exception("Failed to add entry field '%s': %s", clean_name, exc)
        st.error(f"Failed to save: {exc}")
        return

    for key in ("taef_new_name", "taef_new_type", "taef_new_required", "taef_new_options"):
        st.session_state.pop(key, None)

    LOGGER.info(
        "Admin added entry field | name='%s' type=%s required=%s by user='%s'",
        clean_name, clean_type, bool(required), utils.get_os_user(),
    )
    st.session_state["ta_entry_fields_status"] = {
        "level": "success",
        "message": f"Added field '{clean_name}'.",
    }
    st.rerun()


def _apply_entry_field_changes(saved_fields: list[dict]) -> None:
    """Read inline edits from session state, validate, and persist all definitions."""
    new_fields: list[dict] = []
    seen_lower: set[str] = set()
    errors: list[str] = []

    for field in saved_fields:
        field_id = field["id"]
        keys = _entry_field_state_keys(field_id)
        name = str(st.session_state.get(keys["name"], "")).strip()
        type_ = str(st.session_state.get(keys["type"], "text")).strip().lower()
        required = bool(st.session_state.get(keys["required"], False))
        options = _parse_options_text(st.session_state.get(keys["options"], "")) if type_ == "list" else []

        err = _validate_entry_field(name, type_, options, seen_lower)
        if err:
            errors.append(f"{field['name'] or '(unnamed)'}: {err}")
            continue
        seen_lower.add(name.lower())

        new_fields.append({
            "id": field_id,
            "name": name,
            "type": type_,
            "required": required,
            "options": options,
        })

    if errors:
        for msg in errors:
            st.error(msg)
        return

    try:
        _save_entry_fields(new_fields)
    except Exception as exc:
        LOGGER.exception("Failed to save entry-field changes: %s", exc)
        st.error(f"Failed to save: {exc}")
        return

    LOGGER.info(
        "Admin updated entry-field definitions | count=%s by user='%s'",
        len(new_fields), utils.get_os_user(),
    )
    st.session_state["ta_entry_fields_status"] = {
        "level": "success",
        "message": f"Saved {len(new_fields)} field definition(s).",
    }
    st.rerun()


def _count_rows_with_custom_field(field_id: str) -> int:
    """Count rows across all parquet files that have non-null data for this custom field."""
    col_name = f"cf_{field_id}"
    total = 0
    for path in _iter_time_allocation_files(TIME_ALLOCATION_DIR):
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable file during field-data scan '%s': %s", path, exc)
            continue
        if col_name in df.columns:
            total += int(df[col_name].notna().sum())
    return total


def _execute_entry_field_delete(field_id: str, field_name: str) -> None:
    """Remove the field definition and purge its column from every parquet file."""
    saved_fields = _load_entry_fields()
    remaining = [f for f in saved_fields if f["id"] != field_id]
    if len(remaining) == len(saved_fields):
        return

    try:
        _save_entry_fields(remaining)
    except Exception as exc:
        LOGGER.exception("Failed to remove entry field '%s' from definitions: %s", field_name, exc)
        st.error(f"Failed to delete: {exc}")
        return

    col_name = f"cf_{field_id}"
    files_touched = 0
    rows_purged = 0
    for path in _iter_time_allocation_files(TIME_ALLOCATION_DIR):
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable file during field purge '%s': %s", path, exc)
            continue
        if col_name not in df.columns:
            continue
        rows_purged += int(df[col_name].notna().sum())
        df = df.drop(columns=[col_name])
        try:
            if df.empty:
                path.unlink(missing_ok=True)
            else:
                normalized = _ensure_time_allocation_columns(df)
                utils.atomic_write_parquet(normalized, path, schema=_current_time_allocation_schema())
            files_touched += 1
        except Exception as exc:
            LOGGER.exception("Failed to purge column '%s' from '%s': %s", col_name, path, exc)

    _invalidate_admin_export_tables()
    load_time_allocation_user_window.clear()
    _invalidate_input_seed()

    for k in _entry_field_state_keys(field_id).values():
        st.session_state.pop(k, None)

    LOGGER.info(
        "Admin deleted entry field | name='%s' id=%s files_touched=%s rows_purged=%s by user='%s'",
        field_name, field_id, files_touched, rows_purged, utils.get_os_user(),
    )
    if rows_purged > 0:
        message = (
            f"Deleted field '{field_name}' and purged {rows_purged:,} value(s) "
            f"across {files_touched} file(s)."
        )
    else:
        message = f"Deleted field '{field_name}'."
    st.session_state["ta_entry_fields_status"] = {
        "level": "success",
        "message": message,
    }


def _delete_entry_field(field_id: str) -> None:
    """Initiate field deletion: if entries already have data, open a confirm dialog; otherwise delete directly."""
    saved_fields = _load_entry_fields()
    target = next((f for f in saved_fields if f["id"] == field_id), None)
    if target is None:
        return

    affected = _count_rows_with_custom_field(field_id)
    if affected > 0:
        st.session_state["taef_delete_payload"] = {
            "field_id": field_id,
            "field_name": target["name"],
            "affected_count": affected,
        }
        st.session_state["taef_delete_confirm_open"] = True
        st.session_state["taef_delete_confirm_rendered"] = False
    else:
        _execute_entry_field_delete(field_id, target["name"])


@st.dialog("Delete Field?")
def confirm_entry_field_delete_dialog() -> None:
    """Confirmation modal for deleting a custom field that has saved data."""
    payload = st.session_state.get("taef_delete_payload")
    if not isinstance(payload, dict):
        st.error("No pending deletion.")
        st.session_state["taef_delete_confirm_open"] = False
        st.session_state["taef_delete_confirm_rendered"] = False
        st.session_state["taef_delete_payload"] = None
        return

    field_name = str(payload.get("field_name") or "")
    affected = int(payload.get("affected_count") or 0)
    field_id = str(payload.get("field_id") or "")

    st.caption(f"**Field:** {field_name}")
    st.warning(
        f"{affected:,} entry/entries currently have data for this field. "
        "Deleting will permanently remove that data from every parquet file. "
        "This cannot be undone."
    )

    left, right = st.columns(2)
    with left:
        if st.button(
            "Delete & Purge Data",
            type="primary",
            width="stretch",
            key="taef_confirm_purge_btn",
        ):
            _execute_entry_field_delete(field_id, field_name)
            st.session_state["taef_delete_confirm_open"] = False
            st.session_state["taef_delete_confirm_rendered"] = False
            st.session_state["taef_delete_payload"] = None
            st.rerun()
    with right:
        if st.button("Cancel", width="stretch", key="taef_cancel_purge_btn"):
            st.session_state["taef_delete_confirm_open"] = False
            st.session_state["taef_delete_confirm_rendered"] = False
            st.session_state["taef_delete_payload"] = None
            st.rerun()


@st.fragment
def render_entry_fields_editor_view() -> None:
    """Admin-only editor for the manual-entry field definitions users fill in per entry."""
    if "taef_delete_confirm_open" not in st.session_state:
        st.session_state["taef_delete_confirm_open"] = False
    if "taef_delete_confirm_rendered" not in st.session_state:
        st.session_state["taef_delete_confirm_rendered"] = False
    if "taef_delete_payload" not in st.session_state:
        st.session_state["taef_delete_payload"] = None

    st.subheader("Entry Fields", anchor=False)
    st.caption(
        "Define the manual-entry fields users fill in for each time-allocation entry. "
        "Fields render on the Input tab and persist into the parquet files. "
        "Deleting a field that has saved data prompts a confirmation and then purges "
        "that column from every entry."
    )

    status = st.session_state.pop("ta_entry_fields_status", None)
    if isinstance(status, dict):
        message = str(status.get("message") or "").strip()
        level = str(status.get("level") or "").lower()
        if message:
            if level == "success":
                st.success(message)
            elif level == "error":
                st.error(message)
            else:
                st.info(message)

    saved_fields = _load_entry_fields()

    if saved_fields:
        st.markdown("**Existing Fields**")

        header_labels = [("Field Name", 3), ("Field Type", 2), ("Required", 1.5), ("", 1)]
        header_cols = st.columns([w for _, w in header_labels])
        for hcol, (label, _) in zip(header_cols, header_labels):
            if label:
                hcol.markdown(
                    f"<div class='ta-entry-col-header'>{label}</div>",
                    unsafe_allow_html=True,
                )

        for field in saved_fields:
            field_id = field["id"]
            keys = _entry_field_state_keys(field_id)

            if keys["name"] not in st.session_state:
                st.session_state[keys["name"]] = field["name"]
            if keys["type"] not in st.session_state:
                st.session_state[keys["type"]] = field["type"]
            if keys["required"] not in st.session_state:
                st.session_state[keys["required"]] = bool(field["required"])
            if keys["options"] not in st.session_state:
                st.session_state[keys["options"]] = "\n".join(field["options"])

            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 2, 1.5, 1])
                c1.text_input("Field Name", key=keys["name"], label_visibility="collapsed")
                c2.selectbox(
                    "Field Type",
                    options=ENTRY_FIELD_TYPES,
                    key=keys["type"],
                    label_visibility="collapsed",
                )
                c3.checkbox("Required", key=keys["required"], label_visibility="collapsed")
                if c4.button(
                    "Delete",
                    key=f"taef_delete_{field_id}",
                    icon=":material/delete_outline:",
                    type="tertiary",
                ):
                    _delete_entry_field(field_id)
                    st.rerun()

                if st.session_state.get(keys["type"]) == "list":
                    st.text_area(
                        "Options (one per line)",
                        key=keys["options"],
                        height=100,
                    )

        if st.button("Save Changes", type="primary", key="taef_save_changes_btn"):
            _apply_entry_field_changes(saved_fields)
    else:
        st.info("No entry fields are defined yet. Add one below.")

    st.markdown("**Add Field**")
    with st.container(border=True):
        nc1, nc2, nc3 = st.columns([3, 2, 1.5])
        nc1.text_input("Field Name", key="taef_new_name", label_visibility="collapsed", placeholder="Field Name")
        nc2.selectbox(
            "Field Type",
            options=ENTRY_FIELD_TYPES,
            key="taef_new_type",
            label_visibility="collapsed",
        )
        nc3.checkbox("Required", key="taef_new_required")
        if st.session_state.get("taef_new_type") == "list":
            st.text_area("Options (one per line)", key="taef_new_options", height=100)
        if st.button("Add Field", type="primary", icon=":material/add:", key="taef_add_btn"):
            _add_entry_field(
                st.session_state.get("taef_new_name", ""),
                st.session_state.get("taef_new_type", "text"),
                st.session_state.get("taef_new_required", False),
                st.session_state.get("taef_new_options", ""),
            )

    if st.session_state.get("taef_delete_confirm_open") and not st.session_state.get("taef_delete_confirm_rendered"):
        st.session_state["taef_delete_confirm_rendered"] = True
        confirm_entry_field_delete_dialog()


# --- Admin: add new entries (mirrors the Input tab; any date) ---

@st.cache_data(ttl=300)
def _user_login_lookup() -> dict[str, str]:
    """Map full name (lowercased) -> user login from users.parquet for the add-entry user picker."""
    lookup: dict[str, str] = {}
    for login, full_name in utils.load_user_fullname_map().items():
        key = str(full_name or "").strip().lower()
        if key and key not in lookup:
            lookup[key] = str(login or "").strip()
    return lookup


def _on_add_reporting_name_change(idx: int, lookup: dict) -> None:
    """Autofill the add-row's Customer Code when its Reporting Name selection changes."""
    reporting_name = str(st.session_state.get(f"ta_add_account_{idx}", "")).strip()
    st.session_state[f"ta_add_custcode_{idx}"] = lookup["rn_to_first_code"].get(reporting_name, "")


def _on_add_customer_code_change(idx: int, lookup: dict) -> None:
    """Autofill the add-row's Reporting Name when its Customer Code selection changes."""
    customer_code = str(st.session_state.get(f"ta_add_custcode_{idx}", "")).strip()
    reporting_name = lookup["code_to_rn"].get(customer_code, "")
    if reporting_name:
        st.session_state[f"ta_add_account_{idx}"] = reporting_name


def _delete_add_row(delete_idx: int) -> None:
    """Delete one row from the admin add-entry form state (shifts later rows up)."""
    count = int(st.session_state.get("ta_add_count", 1) or 1)
    if count <= 1 or delete_idx < 0 or delete_idx >= count:
        return

    entry_fields = _load_entry_fields()
    for idx in range(delete_idx, count - 1):
        st.session_state[f"ta_add_account_{idx}"] = st.session_state.get(f"ta_add_account_{idx + 1}", "")
        st.session_state[f"ta_add_custcode_{idx}"] = st.session_state.get(f"ta_add_custcode_{idx + 1}", "")
        st.session_state[f"ta_add_dur_h_{idx}"] = st.session_state.get(f"ta_add_dur_h_{idx + 1}", 0)
        st.session_state[f"ta_add_dur_m_{idx}"] = st.session_state.get(f"ta_add_dur_m_{idx + 1}", 0)
        st.session_state[f"ta_add_channel_{idx}"] = st.session_state.get(
            f"ta_add_channel_{idx + 1}", _default_channel()
        )
        for entry_field in entry_fields:
            next_key = f"ta_add_cf_{entry_field['id']}_{idx + 1}"
            st.session_state[f"ta_add_cf_{entry_field['id']}_{idx}"] = st.session_state.get(
                next_key, _default_for_field(entry_field)
            )

    last_idx = count - 1
    last_keys = [
        f"ta_add_account_{last_idx}",
        f"ta_add_custcode_{last_idx}",
        f"ta_add_dur_h_{last_idx}",
        f"ta_add_dur_m_{last_idx}",
        f"ta_add_channel_{last_idx}",
    ]
    for entry_field in entry_fields:
        last_keys.append(f"ta_add_cf_{entry_field['id']}_{last_idx}")
    for key in last_keys:
        st.session_state.pop(key, None)

    st.session_state["ta_add_count"] = count - 1


def _reset_add_form() -> None:
    """Clear the add-entry row widgets back to a single blank row (keeps the selected user/date)."""
    count = int(st.session_state.get("ta_add_count", 1) or 1)
    entry_fields = _load_entry_fields()
    for idx in range(count):
        for key in (
            f"ta_add_account_{idx}",
            f"ta_add_custcode_{idx}",
            f"ta_add_dur_h_{idx}",
            f"ta_add_dur_m_{idx}",
            f"ta_add_channel_{idx}",
        ):
            st.session_state.pop(key, None)
        for entry_field in entry_fields:
            st.session_state.pop(f"ta_add_cf_{entry_field['id']}_{idx}", None)
    st.session_state["ta_add_count"] = 1


def _save_admin_added_records(
    rows: list[dict[str, object]],
    parsed_seconds: list[int],
    user_login: str,
    full_name: str,
    department: str,
    entry_date: date,
) -> bool:
    """Append admin-entered rows to a user's saved entries for one day (any date)."""
    entry_fields = _load_entry_fields()
    new_records: list[dict[str, object]] = []
    for row, seconds in zip(rows, parsed_seconds):
        record: dict[str, object] = {
            "Entry Date": entry_date,
            "User": _empty_to_none(user_login),
            "Full Name": _empty_to_none(full_name),
            "Department": _empty_to_none(department),
            "Account": _empty_to_none(row.get("account")),
            "Customer Code": _empty_to_none(row.get("customer_code")),
            "Time": utils.format_hhmmss(int(max(0, seconds))),
            "Channel": _empty_to_none(row.get("channel")),
        }
        custom_values = row.get("custom_values") or {}
        for entry_field in entry_fields:
            col = f"cf_{entry_field['id']}"
            record[col] = _serialize_custom_value(entry_field, custom_values.get(entry_field["id"]))
        new_records.append(record)

    if not new_records:
        st.error("No rows to add.")
        return False

    new_df = _ensure_time_allocation_columns(pd.DataFrame(new_records))

    # Preserve any existing rows for this user/day so we append rather than replace.
    existing_frames: list[pd.DataFrame] = []
    for file_path in _iter_user_day_candidate_files(TIME_ALLOCATION_DIR, user_login, full_name, entry_date):
        try:
            file_df = pd.read_parquet(file_path)
        except Exception as exc:
            LOGGER.warning("Admin add: skipping unreadable file '%s': %s", file_path, exc)
            continue
        if file_df.empty:
            continue
        mask = _build_user_day_mask(file_df, user_login, full_name, entry_date)
        if bool(mask.any()):
            existing_frames.append(_ensure_time_allocation_columns(file_df.loc[mask].copy()))

    combined = (
        pd.concat([*existing_frames, new_df], ignore_index=True) if existing_frames else new_df
    )
    combined = _ensure_time_allocation_columns(combined)

    output_path = _get_time_allocation_daily_file(TIME_ALLOCATION_DIR, user_login, full_name, entry_date)
    try:
        removed_rows, touched_files = _replace_user_day_entries(
            TIME_ALLOCATION_DIR, user_login, full_name, entry_date
        )
        utils.atomic_write_parquet(combined, output_path, schema=_current_time_allocation_schema())
        _invalidate_admin_export_tables()
        load_time_allocation_user_window.clear()
        _invalidate_input_seed()
        LOGGER.info(
            "Admin added time-allocation entries | admin='%s' target='%s' entry_date=%s "
            "added_rows=%s preserved_rows=%s total_rows=%s touched_files=%s",
            utils.get_os_user(),
            user_login or full_name,
            entry_date,
            len(new_df),
            removed_rows,
            len(combined),
            touched_files,
        )
        st.session_state["ta_add_status"] = {
            "level": "success",
            "message": (
                f"Added {len(new_df)} entry/entries for {full_name or user_login} "
                f"on {entry_date:%m/%d/%Y}."
            ),
        }
        return True
    except Exception as exc:
        LOGGER.exception("Admin add: failed to save entries: %s", exc)
        st.error(f"Failed to save entries: {exc}")
        return False


@st.dialog("Add Allocation?")
def confirm_admin_add_entry_dialog() -> None:
    """Confirmation modal for admin-added time-allocation entries."""
    payload = st.session_state.get("ta_add_confirm_payload")
    if not isinstance(payload, dict):
        st.error("No pending entries found.")
        st.session_state["ta_add_confirm_open"] = False
        st.session_state["ta_add_confirm_rendered"] = False
        return

    entry_date_dt = pd.to_datetime(payload.get("entry_date"), errors="coerce")
    if pd.isna(entry_date_dt):
        st.error("Invalid entry date.")
        st.session_state["ta_add_confirm_open"] = False
        st.session_state["ta_add_confirm_rendered"] = False
        return
    entry_date = entry_date_dt.date()

    user_login = str(payload.get("user_login") or "")
    full_name = str(payload.get("full_name") or "")
    department = str(payload.get("department") or "")
    rows: list[dict[str, object]] = list(payload.get("rows") or [])
    parsed_seconds: list[int] = [int(max(0, s)) for s in list(payload.get("parsed_seconds") or [])]
    display_user = full_name.strip() if str(full_name).strip() else user_login

    st.caption(f"**User:** {display_user}")
    st.caption(f"**Department:** {department or 'N/A'}")
    st.caption(f"**Entry Date:** {entry_date:%m/%d/%Y}")
    st.divider()

    entry_fields = _load_entry_fields()
    preview_rows: list[dict[str, object]] = []
    for row, seconds in zip(rows, parsed_seconds):
        preview: dict[str, object] = {
            "Reporting Name": _empty_to_none(row.get("account")) or "",
            "Customer Code": _empty_to_none(row.get("customer_code")) or "",
            "Time": utils.format_hhmmss(seconds),
            "Channel": _empty_to_none(row.get("channel")) or "",
        }
        custom_values = row.get("custom_values") or {}
        for entry_field in entry_fields:
            preview[entry_field["name"]] = _format_field_value_for_preview(
                entry_field, custom_values.get(entry_field["id"])
            )
        preview_rows.append(preview)
    st.dataframe(pd.DataFrame(preview_rows), hide_index=True, width="stretch")
    st.caption("These rows are added to the user's existing entries for the day.")

    left, right = st.columns(2)
    with left:
        if st.button("Add Entries", type="primary", width="stretch", key="ta_add_confirm_submit_button"):
            if _save_admin_added_records(rows, parsed_seconds, user_login, full_name, department, entry_date):
                st.session_state["ta_add_confirm_open"] = False
                st.session_state["ta_add_confirm_rendered"] = False
                st.session_state["ta_add_confirm_payload"] = None
                # Reset on the next run, before the row widgets are re-created.
                st.session_state["ta_add_reset_pending"] = True
                st.rerun()
    with right:
        if st.button("Cancel", width="stretch", key="ta_add_confirm_cancel_button"):
            st.session_state["ta_add_confirm_open"] = False
            st.session_state["ta_add_confirm_rendered"] = False
            st.session_state["ta_add_confirm_payload"] = None
            st.rerun()


@st.fragment
def render_admin_add_entry_view(account_options: list[str], account_lookup: dict) -> None:
    """Admin-only form to add new entries for any user, on any date.

    Admins can backfill for any date: the This Week/Last Week window and the
    fiscal-period limit that constrain regular users do not apply here."""
    if "ta_add_confirm_open" not in st.session_state:
        st.session_state["ta_add_confirm_open"] = False
    if "ta_add_confirm_rendered" not in st.session_state:
        st.session_state["ta_add_confirm_rendered"] = False
    if "ta_add_confirm_payload" not in st.session_state:
        st.session_state["ta_add_confirm_payload"] = None

    st.subheader("Add Entries", anchor=False)

    st.caption(
        "Add new entries for any user on any date; rows are appended to that "
        "user's existing entries for the day."
    )

    status = st.session_state.pop("ta_add_status", None)
    if isinstance(status, dict):
        message = str(status.get("message") or "").strip()
        level = str(status.get("level") or "").lower()
        if message:
            if level == "success":
                st.success(message)
            elif level == "error":
                st.error(message)
            else:
                st.info(message)

    full_name_options = utils.load_all_user_full_names()
    if not full_name_options:
        st.warning("No users are available to add entries for.")
        return

    sel_col, date_col, dept_col = st.columns([3, 2, 2])
    with sel_col:
        selected_full_name = st.selectbox("User", options=[""] + full_name_options, key="ta_add_user")
    with date_col:
        today_et = _today_eastern()
        entry_date = st.date_input(
            "Entry Date",
            value=today_et,
            # Admins may backfill any date, so allow a wide range instead of the
            # fiscal-period / two-week window that limits regular users.
            min_value=date(today_et.year - 5, 1, 1),
            max_value=date(today_et.year + 1, 12, 31),
            format="MM/DD/YYYY",
            key="ta_add_date",
        )

    target_login = _user_login_lookup().get(str(selected_full_name).strip().lower(), "")
    # get_user_department early-returns on an empty login, so when we don't have one
    # pass the full name through as the lookup key to let its full-name fallback resolve.
    target_department = (
        utils.get_user_department(target_login or selected_full_name, full_name=selected_full_name) or ""
        if selected_full_name
        else ""
    )
    with dept_col:
        # No widget key: a disabled input reflects the current selection every rerun.
        st.text_input("Department", value=target_department, disabled=True)

    if st.session_state.pop("ta_add_reset_pending", False):
        _reset_add_form()

    pending_delete_idx = st.session_state.pop("ta_add_delete_idx", None)
    if pending_delete_idx is not None:
        _delete_add_row(int(pending_delete_idx))

    number_of_accounts = int(max(1, st.session_state.get("ta_add_count", 1) or 1))
    st.session_state["ta_add_count"] = number_of_accounts
    entry_fields = _load_entry_fields()

    for idx in range(number_of_accounts):
        if f"ta_add_account_{idx}" not in st.session_state:
            st.session_state[f"ta_add_account_{idx}"] = ""
        if f"ta_add_custcode_{idx}" not in st.session_state:
            st.session_state[f"ta_add_custcode_{idx}"] = ""
        if f"ta_add_dur_h_{idx}" not in st.session_state:
            st.session_state[f"ta_add_dur_h_{idx}"] = 0
        if f"ta_add_dur_m_{idx}" not in st.session_state:
            st.session_state[f"ta_add_dur_m_{idx}"] = 0
        if f"ta_add_channel_{idx}" not in st.session_state:
            st.session_state[f"ta_add_channel_{idx}"] = _default_channel()
        for entry_field in entry_fields:
            cf_key = f"ta_add_cf_{entry_field['id']}_{idx}"
            if cf_key not in st.session_state:
                st.session_state[cf_key] = _default_for_field(entry_field)

    st.caption(
        "Select a Reporting Name or Customer Code (they fill each other in), then pick the hours and minutes for each row."
    )

    header_labels = [
        ("Reporting Name", 3.2),
        ("Customer Code", 2.3),
        ("Time", 2.6),
        ("Channel", 2),
        ("", 1),
    ]
    header_cols = st.columns([w for _, w in header_labels])
    for hcol, (label, _) in zip(header_cols, header_labels):
        if label:
            hcol.markdown(f"<div class='ta-entry-col-header'>{label}</div>", unsafe_allow_html=True)

    rows: list[dict[str, object]] = []
    for idx in range(number_of_accounts):
        with st.container(border=True):
            c1, c2, c3, c4, c5 = st.columns([3.2, 2.3, 2.6, 2, 1])
            with c1:
                account_key = f"ta_add_account_{idx}"
                account = st.selectbox(
                    "Reporting Name",
                    options=_account_options_for_row(account_options, st.session_state.get(account_key, "")),
                    key=account_key,
                    label_visibility="collapsed",
                    on_change=_on_add_reporting_name_change,
                    args=(idx, account_lookup),
                )
            with c2:
                custcode_key = f"ta_add_custcode_{idx}"
                code_pool = _customer_code_pool_for_reporting_name(
                    account_lookup, st.session_state.get(account_key, "")
                )
                customer_code = st.selectbox(
                    "Customer Code",
                    options=_customer_code_options_for_row(
                        [""] + code_pool,
                        st.session_state.get(custcode_key, ""),
                    ),
                    key=custcode_key,
                    label_visibility="collapsed",
                    on_change=_on_add_customer_code_change,
                    args=(idx, account_lookup),
                )
            with c3:
                h_col, m_col = st.columns(2)
                with h_col:
                    hour_value = st.selectbox(
                        "Hours",
                        options=TIME_HOUR_OPTIONS,
                        key=f"ta_add_dur_h_{idx}",
                        label_visibility="collapsed",
                        format_func=lambda h: f"{h} hr",
                    )
                with m_col:
                    minute_value = st.selectbox(
                        "Minutes",
                        options=TIME_MINUTE_OPTIONS,
                        key=f"ta_add_dur_m_{idx}",
                        label_visibility="collapsed",
                        format_func=lambda m: f"{m:02d} min",
                    )
                duration = _hm_to_duration(hour_value, minute_value)
            with c4:
                channel = st.selectbox(
                    "Channel",
                    options=_channel_options_for_row(st.session_state.get(f"ta_add_channel_{idx}", "")),
                    key=f"ta_add_channel_{idx}",
                    label_visibility="collapsed",
                )
            with c5:
                if st.button(
                    "Delete",
                    key=f"ta_add_delete_btn_{idx}",
                    icon=":material/delete_outline:",
                    disabled=number_of_accounts <= 1,
                    type="tertiary",
                    width="content",
                ):
                    st.session_state["ta_add_delete_idx"] = idx
                    st.rerun(scope="fragment")

            custom_values: dict[str, object] = {}
            for chunk_start in range(0, len(entry_fields), 4):
                chunk = entry_fields[chunk_start:chunk_start + 4]
                cf_cols = st.columns(4)
                for slot, entry_field in enumerate(chunk):
                    with cf_cols[slot]:
                        custom_values[entry_field["id"]] = _render_custom_field_widget(
                            entry_field, idx, False, key_prefix="ta_add"
                        )

        rows.append(
            {
                "account": account,
                "customer_code": _effective_customer_code(account, customer_code),
                "duration": duration,
                "channel": channel,
                "custom_values": custom_values,
            }
        )

    add_col, save_col, _ = st.columns([1, 1, 6])
    with add_col:
        if st.button("Add Row", key="ta_add_add_row_btn", icon=":material/add:", type="secondary"):
            st.session_state["ta_add_count"] = number_of_accounts + 1
            st.rerun(scope="fragment")
    with save_col:
        add_clicked = st.button("Add Entries", type="primary", key="ta_add_submit")

    if add_clicked:
        errors: list[str] = []
        if not str(selected_full_name).strip():
            errors.append("Select a user to add entries for.")
        parsed_seconds: list[int] = []
        for i, row in enumerate(rows, start=1):
            if not _empty_to_none(row["account"]):
                errors.append(f"Reporting Name for row {i} is required.")
            seconds = utils.parse_hhmmss(str(row["duration"]))
            if seconds <= 0:
                errors.append(f"Time for row {i} must be greater than 0 hr 00 min.")
            parsed_seconds.append(max(0, seconds))
            row_custom = row.get("custom_values") or {}
            for entry_field in entry_fields:
                if not entry_field["required"]:
                    continue
                if not _is_field_value_present(entry_field, row_custom.get(entry_field["id"])):
                    errors.append(f"Row {i}: '{entry_field['name']}' is required.")
        if errors:
            for message in errors:
                st.error(message)
            return

        st.session_state["ta_add_confirm_payload"] = {
            "entry_date": entry_date.isoformat(),
            "user_login": target_login,
            "full_name": selected_full_name,
            "department": target_department,
            "rows": rows,
            "parsed_seconds": parsed_seconds,
        }
        st.session_state["ta_add_confirm_open"] = True
        st.session_state["ta_add_confirm_rendered"] = False
        st.rerun()

    if st.session_state["ta_add_confirm_open"] and not st.session_state["ta_add_confirm_rendered"]:
        st.session_state["ta_add_confirm_rendered"] = True
        confirm_admin_add_entry_dialog()


def render_auto_email_settings_view() -> None:
    """Admin-only controls for the automated 'missing time entries' reminder emails."""
    st.subheader("Automated Reminder Emails", anchor=False)
    st.caption(
        "Send automated weekly emails at 3PM each Friday notifying team members of "
        "missing or low time allocation for the current week."
    )

    status = st.session_state.pop("ta_auto_email_status", None)
    if isinstance(status, dict) and str(status.get("message") or "").strip():
        level = str(status.get("level") or "").lower()
        message = str(status["message"])
        if level == "success":
            st.success(message)
        elif level == "error":
            st.error(message)
        else:
            st.info(message)

    settings = utils.load_auto_email_settings()
    departments = utils.list_departments()
    existing = settings.get("departments") or {}
    is_dev = utils.is_current_user_developer()

    # Global send mechanics (from-address, pilot/test recipient, live toggle) are
    # developer-only. Non-developer admins only curate the per-department table
    # below; the developer-managed values are preserved untouched on their save.
    if is_dev:
        gc1, gc2 = st.columns(2)
        gc1.text_input(
            "Send from (shared mailbox)",
            value=settings.get("from_address") or utils.DEFAULT_AUTO_EMAIL_FROM,
            key="ta_ae_from",
            help="The Outlook mailbox reminders send as. Must be a mailbox the data-refresh machine can send from.",
        )
        gc2.text_input(
            "Pilot/test recipient",
            value=settings.get("test_recipient") or "",
            key="ta_ae_test",
            help="While 'Send live' is off, every reminder goes here instead of to employees.",
        )
        st.checkbox(
            "Send live to employees (off = pilot: send only to the test recipient)",
            value=bool(settings.get("live", False)),
            key="ta_ae_live",
        )
    else:
        mode = "Live — emailing employees" if settings.get("live") else "Pilot mode — not sending to employees"
        st.caption(f"Sending controls are managed by developers. Current status: **{mode}**.")

    st.markdown("**Departments**")
    st.caption(
        "‘Mgr recap’ also emails each manager in the department a summary of their own "
        "reports (matched by the Manager field) who have missing days."
    )
    dept_editor_df = None
    if not departments:
        st.info("No departments found in users.parquet yet.")
    else:
        # Compact checkbox grid: one slim row per department, two toggles. The grid
        # scrolls when the list is long, so no separate show-more control is needed.
        dept_rows = []
        for dept in departments:
            cfg = utils.normalize_auto_email_department(existing.get(dept))
            dept_rows.append(
                {"Department": dept, "Enabled": cfg["enabled"], "Mgr recap": cfg["manager_recap"]}
            )
        base_df = pd.DataFrame(dept_rows, columns=["Department", "Enabled", "Mgr recap"])
        visible_rows = min(len(departments), 9)
        dept_editor_df = st.data_editor(
            base_df,
            hide_index=True,
            use_container_width=True,
            disabled=["Department"],
            height=(visible_rows + 1) * 35 + 3,
            column_config={
                "Department": st.column_config.TextColumn("Department"),
                "Enabled": st.column_config.CheckboxColumn("Enabled", width="small"),
                "Mgr recap": st.column_config.CheckboxColumn("Mgr recap", width="small"),
            },
        )

    if st.button("Save Reminder Settings", type="primary", key="ta_ae_save"):
        # Read rendered rows from widget state; for departments not rendered this run
        # (collapsed under "Show More"), keep their saved config so a save while
        # collapsed never silently resets hidden departments.
        new_departments = {}
        if dept_editor_df is not None:
            for _, dept_row in dept_editor_df.iterrows():
                dept_name = str(dept_row["Department"])
                new_departments[dept_name] = utils.normalize_auto_email_department(
                    {
                        "enabled": bool(dept_row["Enabled"]),
                        "interval": "weekly",
                        "send_day": "Fri",
                        "manager_recap": bool(dept_row["Mgr recap"]),
                    }
                )
        if is_dev:
            payload = {
                "from_address": str(st.session_state.get("ta_ae_from") or utils.DEFAULT_AUTO_EMAIL_FROM).strip(),
                "live": bool(st.session_state.get("ta_ae_live", False)),
                "test_recipient": str(st.session_state.get("ta_ae_test") or "").strip(),
                "departments": new_departments,
            }
        else:
            # Non-developers can't see the send controls; preserve them as-is.
            payload = {
                "from_address": settings.get("from_address") or utils.DEFAULT_AUTO_EMAIL_FROM,
                "live": bool(settings.get("live", False)),
                "test_recipient": settings.get("test_recipient") or "",
                "departments": new_departments,
            }
        try:
            utils.save_auto_email_settings(payload)
            LOGGER.info(
                "Saved auto-email settings | live=%s enabled_depts=%s",
                payload["live"],
                [dept for dept, cfg in new_departments.items() if cfg["enabled"]],
            )
            st.session_state["ta_auto_email_status"] = {
                "level": "success",
                "message": "Reminder settings saved.",
            }
        except Exception as exc:
            LOGGER.exception("Failed to save auto-email settings: %s", exc)
            st.session_state["ta_auto_email_status"] = {
                "level": "error",
                "message": f"Failed to save reminder settings: {exc}",
            }
        st.rerun()


def render_admin_name_cleanup_view() -> None:
    """Admin/developer maintenance: repair entries saved under a Windows username.

    Pairs with the disconnected-save case — a user who submits time while offline
    can have their Windows login stored as the name (e.g. "jfitouri" instead of
    "Jennifer Fitouri"). Rather than blocking those submissions, this lets an admin
    or developer remap them to real names on demand. Only touches rows whose saved
    name is blank or equals the user's own login, so real names are never altered.
    Delegates to ta_store.repair_fullnames (Preview = dry run)."""
    st.subheader("Fix entry names")
    st.caption(
        "Repairs saved entries whose name was recorded as a Windows username "
        "(e.g. “jfitouri”) instead of the person’s full name (“Jennifer Fitouri”). "
        "This can happen when someone submits time while disconnected from the "
        "network drive. Safe to run anytime — it only changes rows where the saved "
        "name is blank or matches the user’s own login, and never alters real names."
    )
    col_preview, col_apply = st.columns(2)
    do_preview = col_preview.button(
        "Preview",
        width="stretch",
        key="ta_namefix_preview",
        help="Report how many entries would change, without modifying anything.",
    )
    do_apply = col_apply.button(
        "Fix names now",
        type="primary",
        width="stretch",
        key="ta_namefix_apply",
    )
    if do_preview or do_apply:
        mapping = utils.load_user_fullname_map()
        if not mapping:
            st.session_state["ta_namefix_summary"] = None
            st.error(
                "Couldn't load the user list from the network drive — are you "
                "connected? No changes were made."
            )
        else:
            dry = not do_apply
            with st.spinner("Scanning entries…" if dry else "Repairing entry names…"):
                summary = ta_store.repair_fullnames(TIME_ALLOCATION_DIR, mapping, dry_run=dry)
            if do_apply:
                _invalidate_admin_export_tables()
                LOGGER.info(
                    "Time allocation name cleanup applied by '%s' | scanned=%s changed=%s fixed=%s errors=%s",
                    utils.get_os_user(),
                    summary["files_scanned"],
                    summary["files_changed"],
                    summary["rows_fixed"],
                    len(summary["errors"]),
                )
            st.session_state["ta_namefix_summary"] = summary

    summary = st.session_state.get("ta_namefix_summary")
    if summary:
        verb = "Would fix" if summary["dry_run"] else "Fixed"
        if summary["rows_fixed"] == 0:
            st.success("No entries need fixing — all names look correct.")
        else:
            st.success(
                f"{verb} {summary['rows_fixed']} row(s) across "
                f"{summary['files_changed']} file(s)."
            )
            by_name = summary.get("by_name") or {}
            if by_name:
                st.dataframe(
                    pd.DataFrame(
                        sorted(by_name.items(), key=lambda kv: (-kv[1], kv[0])),
                        columns=["Corrected To", "Rows"],
                    ),
                    hide_index=True,
                    width="stretch",
                )
        if summary.get("errors"):
            st.warning(
                f"{len(summary['errors'])} file(s) couldn't be processed; see the app log."
            )


def render_admin_blank_code_cleanup_view() -> None:
    """Admin/developer maintenance: fill blank Customer Codes from the Reporting Name.

    Pairs with the fixed root cause (entries now always save a code when the
    Reporting Name has one — see _effective_customer_code). This cleans up the
    entries left behind with a blank code before that fix, assigning each the code
    that matches its Reporting Name — exactly what a fresh save would store. Only
    blanks are touched (an already-set code is never changed); a Reporting Name
    with no code in the accounts data is left blank. Delegates to
    ta_store.repair_blank_customer_codes (Preview = dry run)."""
    st.subheader("Fix blank Customer Codes")
    st.caption(
        "Fills a Customer Code onto saved entries that have a Reporting Name but a "
        "blank Customer Code, using the code that matches the Reporting Name. Safe "
        "to run anytime — it only fills blanks (never changes a code that's already "
        "set), and leaves a Reporting Name that has no code alone."
    )
    col_preview, col_apply = st.columns(2)
    do_preview = col_preview.button(
        "Preview",
        width="stretch",
        key="ta_codefix_preview",
        help="Report how many entries would change, without modifying anything.",
    )
    do_apply = col_apply.button(
        "Fix codes now",
        type="primary",
        width="stretch",
        key="ta_codefix_apply",
    )
    if do_preview or do_apply:
        mapping = _reporting_name_to_code_map()
        if not mapping:
            st.session_state["ta_codefix_summary"] = None
            st.error(
                "Couldn't load the accounts list from the network drive — are you "
                "connected? No changes were made."
            )
        else:
            dry = not do_apply
            with st.spinner("Scanning entries…" if dry else "Filling blank codes…"):
                summary = ta_store.repair_blank_customer_codes(
                    TIME_ALLOCATION_DIR, mapping, dry_run=dry
                )
            if do_apply:
                _invalidate_admin_export_tables()
                load_time_allocation_user_window.clear()
                LOGGER.info(
                    "Time allocation blank-code cleanup applied by '%s' | scanned=%s changed=%s fixed=%s errors=%s",
                    utils.get_os_user(),
                    summary["files_scanned"],
                    summary["files_changed"],
                    summary["rows_fixed"],
                    len(summary["errors"]),
                )
            st.session_state["ta_codefix_summary"] = summary

    summary = st.session_state.get("ta_codefix_summary")
    if summary:
        verb = "Would fill" if summary["dry_run"] else "Filled"
        if summary["rows_fixed"] == 0:
            st.success("No entries need fixing — every coded Reporting Name already has a Customer Code.")
        else:
            st.success(
                f"{verb} {summary['rows_fixed']} row(s) across "
                f"{summary['files_changed']} file(s)."
            )
            by_name = summary.get("by_name") or {}
            if by_name:
                st.dataframe(
                    pd.DataFrame(
                        sorted(by_name.items(), key=lambda kv: (-kv[1], kv[0])),
                        columns=["Reporting Name", "Rows"],
                    ),
                    hide_index=True,
                    width="stretch",
                )
        if summary.get("errors"):
            st.warning(
                f"{len(summary['errors'])} file(s) couldn't be processed; see the app log."
            )


def render_admin_settings_view(account_lookup: dict, account_options: list[str]) -> None:
    """Admin-only Time Allocation Tool settings: entry fields, add-entry form, and the Edit Entries table."""
    if not utils.is_current_user_admin():
        st.info("Sorry, you don't have access to this section.")
        return

    render_entry_fields_editor_view()

    st.divider()
    render_auto_email_settings_view()

    st.divider()
    render_admin_add_entry_view(account_options, account_lookup)

    st.divider()
    render_admin_data_editor_view(account_lookup)

    st.divider()
    render_admin_name_cleanup_view()

    st.divider()
    render_admin_blank_code_cleanup_view()


# Header (no divider under the title/subtitle — Time Allocation only)
utils.render_page_header(PAGE_TITLE, show_divider=False)

user_login = utils.get_os_user()
full_name = utils.get_full_name_for_user(None, user_login)
department = utils.get_user_department(user_login, full_name=full_name) or ""
is_admin_user = utils.is_current_user_admin()

account_lookup = _account_lookup_for_dir(str(PERSONNEL_DIR))
account_options = [""] + account_lookup["reporting_names"]

if is_admin_user:
    # on_change="rerun" makes the tabs track state so only the *selected* tab's
    # body executes (each TabContainer exposes `.open`). Without it Streamlit runs
    # every tab's content on every load, which made an admin pay the Exports and
    # Admin-Settings full-dataset reads even while sitting on the Input tab. The
    # first tab (Input) is selected by default, so it renders on initial load.
    input_tab, exports_tab, settings_tab = st.tabs(
        ["Input", "Exports", "Admin Settings"], width="stretch", on_change="rerun"
    )
    if input_tab.open:
        with input_tab:
            render_input_view(user_login, full_name, department, account_options, account_lookup)
    if exports_tab.open:
        with exports_tab:
            render_exports_view()
    if settings_tab.open:
        with settings_tab:
            render_admin_settings_view(account_lookup, account_options)
else:
    render_input_view(user_login, full_name, department, account_options, account_lookup)
