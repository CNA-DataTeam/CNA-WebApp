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

import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import streamlit as st
from streamlit_calendar import calendar as st_calendar

import config
import utils

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
/* Bordered card per entry */
[data-testid="stVerticalBlockBorderWrapper"] {
    padding: 0.55rem 0.75rem !important;
    border-radius: 8px !important;
    background-color: rgba(0, 177, 158, 0.04);
}
/* Header row that sits above the entry cards */
.ta-entry-col-header {
    font-size: 0.78rem;
    font-weight: 600;
    color: var(--cna-green);
    padding: 0 0.75rem;
}
/* Tighten % caption inside an entry card */
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
</style>
"""
st.markdown(_TA_COMPACT_CSS, unsafe_allow_html=True)

TIME_ALLOCATION_DIR = config.TIME_ALLOCATION_DIR
PERSONNEL_DIR = config.PERSONNEL_DIR
# The canonical channel set, listed in the default display order used when
# there isn't yet enough saved data to sort the dropdown by usage frequency.
CHANNEL_OPTIONS = [
    "Resupply",
    "Consolidated: Equipment",
    "Consolidated: Smallwares",
    "Express: Smallwares",
    "Express: Equipment",
    "Consolidated: Rollout",
    "Consolidated: Full",
    "Express: Full",
]
# Below this many total channel selections across all saved entries, the
# Channel dropdown uses CHANNEL_OPTIONS' defined order; at/above it, by usage
# frequency.
CHANNEL_FREQUENCY_SORT_THRESHOLD = 50

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
    login_key = _normalize_login(user_login)
    if login_key:
        return config.sanitize_log_user(login_key)
    fallback = str(full_name or "").strip().lower().replace(" ", "_")
    return config.sanitize_log_user(fallback or "unknown_user")


def _get_time_allocation_month_dir(base_dir: Path, entry_date: date) -> Path:
    """Return the year/month partition directory for an entry date."""
    return base_dir / f"year={entry_date.year:04d}" / f"month={entry_date.month:02d}"


def _get_time_allocation_daily_file(base_dir: Path, user_login: str, full_name: str, entry_date: date) -> Path:
    """Return the one-file-per-day parquet path for a user/date."""
    user_partition = _get_time_allocation_user_partition(user_login, full_name)
    return (
        _get_time_allocation_month_dir(base_dir, entry_date)
        / f"user={user_partition}"
        / f"time_allocation_{entry_date:%Y%m%d}.parquet"
    )


def _iter_time_allocation_files(base_dir: Path) -> list[Path]:
    """Return all saved time-allocation parquet files, including nested partitions."""
    if not base_dir.exists():
        return []
    return sorted((path for path in base_dir.rglob("*.parquet") if path.is_file()), reverse=True)


def _iter_time_allocation_day_candidate_files(base_dir: Path, entry_date: date) -> list[Path]:
    """Return candidate parquet files that may contain rows for one calendar day."""
    files: list[Path] = []
    seen: set[str] = set()

    month_dir = _get_time_allocation_month_dir(base_dir, entry_date)
    daily_name = f"time_allocation_{entry_date:%Y%m%d}.parquet"
    legacy_pattern = f"time_allocation_{entry_date:%Y%m%d}*.parquet"

    if month_dir.exists():
        for path in sorted(month_dir.rglob(daily_name), reverse=True):
            path_key = str(path).lower()
            if path.is_file() and path_key not in seen:
                files.append(path)
                seen.add(path_key)

    for path in sorted(base_dir.glob(legacy_pattern), reverse=True):
        path_key = str(path).lower()
        if path.is_file() and path_key not in seen:
            files.append(path)
            seen.add(path_key)

    return files


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


def _read_time_allocation_exports_from_files(file_paths: list[Path], base_dir: Path) -> pd.DataFrame:
    """Read and normalize saved time-allocation files into one DataFrame."""
    if not file_paths:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for file_path in file_paths:
        try:
            one = pd.read_parquet(file_path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable export file '%s': %s", file_path, exc)
            continue
        if one.empty:
            continue
        try:
            source_file = str(file_path.relative_to(base_dir))
        except ValueError:
            source_file = file_path.name
        one["Source File"] = source_file
        frames.append(one)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    expected_cols = [
        "Entry Date",
        "User",
        "Full Name",
        "Department",
        "Account",
        "Customer Code",
        "Time",
        "Channel",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA
    if "Source File" not in df.columns:
        df["Source File"] = ""
    df["Entry Date"] = pd.to_datetime(df["Entry Date"], errors="coerce").dt.date
    return df[expected_cols + ["Source File"]]


@st.cache_data(ttl=30, show_spinner="Loading saved allocations...")
def load_time_allocation_exports(base_dir: Path) -> pd.DataFrame:
    """Load all saved time-allocation parquet files from the output directory."""
    return _read_time_allocation_exports_from_files(_iter_time_allocation_files(base_dir), base_dir)


def _normalize_login(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.replace("/", "\\")
    if "\\" in text:
        text = text.split("\\")[-1]
    if "@" in text:
        text = text.split("@")[0]
    return text.strip()


def _filter_user_exports(exports_df: pd.DataFrame, user_login: str, full_name: str) -> pd.DataFrame:
    """Return exports filtered to the current user by login and full-name fallback."""
    if exports_df.empty:
        return exports_df.copy()

    login_key = _normalize_login(user_login)
    full_name_key = str(full_name or "").strip().lower()
    user_series = exports_df["User"].fillna("").astype(str).map(_normalize_login)
    mask = user_series.eq(login_key)
    if full_name_key:
        full_name_series = exports_df["Full Name"].fillna("").astype(str).str.strip().str.lower()
        mask = mask | full_name_series.eq(full_name_key)
    user_df = exports_df.loc[mask].copy()
    user_df["Entry Date"] = pd.to_datetime(user_df["Entry Date"], errors="coerce").dt.date
    user_df = user_df[user_df["Entry Date"].notna()].copy()
    return user_df


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
    if window_start is None or window_end is None or window_end < window_start:
        return pd.DataFrame()

    files: list[Path] = []
    seen: set[str] = set()
    current_day = window_start
    while current_day <= window_end:
        for path in _iter_time_allocation_day_candidate_files(base_dir, current_day):
            path_key = str(path).lower()
            if path_key not in seen:
                files.append(path)
                seen.add(path_key)
        current_day += timedelta(days=1)

    window_df = _read_time_allocation_exports_from_files(files, base_dir)
    if window_df.empty:
        return window_df

    filtered_df = _filter_user_exports(window_df, user_login, full_name)
    filtered_df = filtered_df[
        filtered_df["Entry Date"].ge(window_start) & filtered_df["Entry Date"].le(window_end)
    ].copy()
    return filtered_df


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


def _build_account_lookup(lookup_df: pd.DataFrame) -> dict:
    """
    Build the Customer Code <-> Reporting Name lookup structures from the
    accounts parquet DataFrame (columns: CustomerCode, ReportingName).

    Returns a dict with:
        reporting_names    - sorted unique Reporting Name values
        customer_codes     - sorted unique Customer Code values
        rn_to_first_code   - Reporting Name -> first (alphabetical) Customer Code
        code_to_rn         - Customer Code -> Reporting Name
    """
    rn_to_codes: dict[str, set[str]] = {}
    code_to_rn: dict[str, str] = {}
    if lookup_df is not None and not lookup_df.empty:
        for _, row in lookup_df.iterrows():
            rn = str(row.get("ReportingName") or "").strip()
            code = str(row.get("CustomerCode") or "").strip()
            if rn:
                rn_to_codes.setdefault(rn, set())
                if code:
                    rn_to_codes[rn].add(code)
            if code and code not in code_to_rn and rn:
                code_to_rn[code] = rn
    rn_to_first_code = {
        rn: sorted(codes)[0] for rn, codes in rn_to_codes.items() if codes
    }
    return {
        "reporting_names": sorted(rn_to_codes.keys()),
        "customer_codes": sorted(code_to_rn.keys()),
        "rn_to_first_code": rn_to_first_code,
        "code_to_rn": code_to_rn,
    }


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


def _parse_date_value(value: object) -> date | None:
    """Safely parse date-like values into a date object."""
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _today_eastern() -> date:
    """Return today's date in Eastern time."""
    return utils.to_eastern(utils.now_utc()).date()


def _get_current_period() -> dict | None:
    """Return the fiscal period containing today (Eastern), or None if not configured."""
    return utils.get_fiscal_period_for_date(_today_eastern())


def _most_recent_fiscal_year_start() -> date | None:
    """
    Return the start date of the most recent fiscal year.

    "Most recent" is the fiscal year containing today, or — when today falls
    outside every configured period — the latest fiscal year that has started.
    Returns None if no fiscal periods are configured.
    """
    periods = utils.load_fiscal_periods()
    if periods.empty:
        return None

    today = _today_eastern()
    current = _get_current_period()
    if current is not None:
        target_year = int(current["Year"])
    else:
        started = periods[periods["StartDate"] <= today]
        if started.empty:
            target_year = int(periods["Year"].min())
        else:
            target_year = int(started["Year"].max())

    year_periods = periods[periods["Year"].astype("Int64") == target_year]
    if year_periods.empty:
        return None
    return min(year_periods["StartDate"].tolist())


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
    window_rows: tuple[tuple[str, str, int], ...],
) -> list[dict[str, object]]:
    """Build calendar event payload from normalized window rows."""
    events: list[dict[str, object]] = []
    for row_idx, (entry_day_iso, account_name, seconds) in enumerate(window_rows):
        hours_value = max(0, int(seconds)) / 3600.0
        height_units = min(24, max(1, int(round(hours_value * 2))))  # 0.5-hour buckets
        day_value = _parse_date_value(entry_day_iso)
        if day_value is None:
            continue

        events.append(
            {
                "id": f"ta-{entry_day_iso}-{row_idx}",
                "title": f"{account_name} | {hours_value:.2f} hours",
                "start": entry_day_iso,
                "end": (day_value + timedelta(days=1)).isoformat(),
                "allDay": True,
                "display": "block",
                "backgroundColor": "#00B19E",
                "borderColor": "#00B19E",
                "textColor": "#ffffff",
                "classNames": ["ta-entry-event", f"ta-hu-{height_units}"],
                "extendedProps": {"entry_date": entry_day_iso},
            }
        )
    return events


@st.cache_data(ttl=30, show_spinner=False)
def _channel_frequency_counts() -> dict[str, int]:
    """Count how often each channel appears across all saved time-allocation entries."""
    exports_df = load_time_allocation_exports(TIME_ALLOCATION_DIR)
    if exports_df.empty or "Channel" not in exports_df.columns:
        return {}
    channels = exports_df["Channel"].dropna().astype(str).str.strip()
    channels = channels[channels != ""]
    if channels.empty:
        return {}
    return {str(k): int(v) for k, v in channels.value_counts().items()}


def _ordered_channel_options() -> list[str]:
    """
    Return the canonical channel options ordered for display.

    Under CHANNEL_FREQUENCY_SORT_THRESHOLD total selections across all channels,
    options are shown in CHANNEL_OPTIONS' defined order. At/above the threshold,
    they are sorted by usage frequency (most-used first), with alphabetical order
    as the tiebreak.
    """
    counts = _channel_frequency_counts()
    total = sum(counts.values())
    if total < CHANNEL_FREQUENCY_SORT_THRESHOLD:
        return list(CHANNEL_OPTIONS)
    return sorted(CHANNEL_OPTIONS, key=lambda channel: (-counts.get(channel, 0), channel))


def _default_channel() -> str:
    """Default channel for a new/blank entry row (the most frequently used option)."""
    ordered = _ordered_channel_options()
    return ordered[0] if ordered else (CHANNEL_OPTIONS[0] if CHANNEL_OPTIONS else "")


def _coerce_channel(value: object) -> str:
    """Return the saved channel verbatim if present so legacy values are preserved."""
    text = str(value or "").strip()
    return text if text else _default_channel()


def _channel_options_for_row(current_value: object) -> list[str]:
    """Ensure the row's current channel exists in the selectbox options (preserves legacy values)."""
    ordered = _ordered_channel_options()
    current = str(current_value or "").strip()
    if not current or current in ordered:
        return list(ordered)
    return [current, *ordered]


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


def render_input_day_selector(user_login: str, full_name: str) -> tuple[date, pd.DataFrame]:
    """Render clickable calendar and return selected day + selected day rows."""
    header_col, toggle_col = st.columns([2, 3])
    with header_col:
        st.subheader("Calendar", anchor=False)
    with toggle_col:
        view_mode_options = ["This Week", "This Period", "Last Week"]
        stored_view = st.session_state.get("ta_calendar_view", "This Week")
        if stored_view not in view_mode_options:
            stored_view = "This Week"
            st.session_state["ta_calendar_view"] = stored_view
        view_mode = st.radio(
            "View",
            options=view_mode_options,
            index=view_mode_options.index(stored_view),
            horizontal=True,
            key="ta_calendar_view",
            label_visibility="collapsed",
        )

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
    window_rows: list[tuple[str, str, int]] = []
    for _, row in window_df.reset_index(drop=True).iterrows():
        entry_day = row["Entry Date"]
        if not isinstance(entry_day, date):
            continue
        account_name = str(row.get("Account") or "").strip() or "(No Account)"
        seconds = max(0, int(row.get("seconds", 0)))
        window_rows.append((entry_day.isoformat(), account_name, seconds))
    calendar_events = _build_calendar_events(tuple(window_rows))

    num_days = (window_end - window_start).days + 1
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
        # Roomier rows for the multi-week period grid so day cells aren't squished.
        calendar_height = 90 + weeks_in_view * 120
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
        "dayMaxEventRows": 4,
        "headerToolbar": {"left": "", "center": "title", "right": ""},
        "height": calendar_height,
        "initialDate": calendar_initial_date,
    }
    if valid_range is not None:
        calendar_options["validRange"] = valid_range
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
        font-size: 0.9rem;
        font-weight: 600;
    }
    .fc .fc-toolbar {
        margin-bottom: 0.25rem;
    }
    .fc .fc-daygrid-day-frame {
        min-height: 48px;
        cursor: pointer;
    }
    .fc .fc-daygrid-day,
    .fc .fc-daygrid-day-top,
    .fc .fc-daygrid-day-number,
    .fc .fc-daygrid-bg-harness {
        cursor: pointer;
    }
    .fc .fc-daygrid-day-number {
        font-weight: 600;
        font-size: 0.78rem;
    }
    .fc .fc-daygrid-event {
        border-radius: 6px;
        padding: 1px 4px;
        cursor: pointer;
    }
    .fc .ta-entry-event {
        align-items: flex-start;
    }
    .fc .fc-event-title {
        font-weight: 600;
        white-space: normal;
        line-height: 1.15;
        font-size: 0.68rem;
    }
    """ + "\n" + height_rules + "\n" + selected_day_css
    calendar_widget_key = f"ta_input_calendar_{view_mode}_{num_days}"
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
                st.rerun()

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

    for idx in range(number_of_accounts):
        with st.container(border=True):
            c1, c2, c3, c4, c5 = st.columns([3.2, 2.3, 2.6, 2, 1])
            with c1:
                account_key = f"ta_detailed_account_{idx}"
                account = st.selectbox(
                    "Reporting Name",
                    options=_account_options_for_row(account_options, st.session_state.get(account_key, "")),
                    key=account_key,
                    disabled=editing_locked,
                    label_visibility="collapsed",
                    on_change=_on_reporting_name_change,
                    args=(idx, account_lookup),
                )
            with c2:
                custcode_key = f"ta_detailed_custcode_{idx}"
                customer_code = st.selectbox(
                    "Customer Code",
                    options=_customer_code_options_for_row(
                        [""] + account_lookup["customer_codes"],
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
                    else:
                        st.session_state["ta_detailed_delete_idx"] = idx
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
                "customer_code": customer_code,
                "duration": duration,
                "channel": channel,
                "custom_values": custom_values,
            }
        )

    add_col, save_col, _ = st.columns([1, 1, 6])
    with add_col:
        if st.button(
            "Add Row",
            key="ta_detailed_add_row_btn",
            icon=":material/add:",
            type="secondary",
            disabled=editing_locked,
        ):
            st.session_state["ta_detailed_count"] = number_of_accounts + 1
            st.rerun()

    with save_col:
        save_detailed_clicked = st.button(
            "Save Allocation",
            type="primary",
            key="ta_save_detailed",
            disabled=editing_locked,
        )

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


def _replace_user_day_entries(base_dir: Path, user_login: str, full_name: str, entry_date: date) -> tuple[int, int]:
    """Remove existing rows for (user, entry_date) across export files."""
    removed_rows = 0
    touched_files = 0
    for file_path in _iter_user_day_candidate_files(base_dir, user_login, full_name, entry_date):
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
        removed_rows, touched_files = _replace_user_day_entries(TIME_ALLOCATION_DIR, user_login, full_name, entry_date)
        utils.atomic_write_parquet(df, output_path, schema=_current_time_allocation_schema())
        load_time_allocation_exports.clear()
        _store_window_override(
            entry_date,
            _build_window_override_from_records(
                rows, parsed_seconds, user_login, full_name, department, entry_date
            ),
        )
        _invalidate_input_seed()
        LOGGER.info(
            "Saved time allocation export | rows=%s file='%s' user='%s' entry_date=%s replaced_rows=%s touched_files=%s",
            len(df),
            str(output_path),
            user_login,
            entry_date,
            removed_rows,
            touched_files,
        )
        if removed_rows > 0:
            _queue_input_status(
                "success",
                f"Updated {entry_date:%m/%d/%Y}: replaced {removed_rows} row(s) and saved {len(df)} row(s).",
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
        load_time_allocation_exports.clear()
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


@st.fragment
def render_exports_view() -> None:
    """Render admin export view with filters and CSV download."""
    if not utils.is_current_user_admin():
        st.info("Sorry, you don't have access to this section")
        return

    exports_df = load_time_allocation_exports(TIME_ALLOCATION_DIR)
    if exports_df.empty:
        st.info("No time-allocation exports found.")
        return

    exports_df = _attach_fiscal_columns(exports_df)

    entry_dt = pd.to_datetime(exports_df["Entry Date"], errors="coerce")
    valid_dates = entry_dt.dropna()
    today = _today_eastern()
    min_date = valid_dates.min().date() if not valid_dates.empty else today
    max_date = valid_dates.max().date() if not valid_dates.empty else min_date

    # Default the window to the most recent fiscal year through today.
    fiscal_year_start = _most_recent_fiscal_year_start()
    default_from = fiscal_year_start if fiscal_year_start is not None else min_date
    default_to = today
    widget_min = min(min_date, default_from)
    widget_max = max(max_date, default_to)
    default_from = max(widget_min, min(default_from, widget_max))
    default_to = max(widget_min, min(default_to, widget_max))

    year_options = sorted(
        {int(y) for y in pd.to_numeric(exports_df["Fiscal Year"], errors="coerce").dropna().tolist()}
    )
    year_choice_labels = [""] + [str(y) for y in year_options]

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        date_from = st.date_input(
            "From", value=default_from, min_value=widget_min, max_value=widget_max, key="ta_export_from"
        )
    with f2:
        date_to = st.date_input(
            "To", value=default_to, min_value=widget_min, max_value=widget_max, key="ta_export_to"
        )
    with f3:
        selected_year = st.selectbox(
            "Fiscal Year",
            options=year_choice_labels,
            key="ta_export_fiscal_year",
        )
    with f4:
        if selected_year:
            scoped = exports_df[
                pd.to_numeric(exports_df["Fiscal Year"], errors="coerce").eq(int(selected_year))
            ]
            period_pool = scoped["Fiscal Period"]
        else:
            period_pool = exports_df["Fiscal Period"]
        period_options = sorted(
            {p for p in period_pool.dropna().astype(str).str.strip().tolist() if p}
        )
        stored_period = st.session_state.get("ta_export_fiscal_period", "")
        if stored_period and stored_period not in period_options:
            st.session_state["ta_export_fiscal_period"] = ""
        selected_period = st.selectbox(
            "Fiscal Period",
            options=[""] + period_options,
            key="ta_export_fiscal_period",
        )

    g1, g2, _, _ = st.columns(4)
    full_name_options = sorted(
        name
        for name in exports_df["Full Name"].dropna().astype(str).str.strip().unique().tolist()
        if name
    )
    selected_full_name = g1.selectbox(
        "User",
        options=[""] + full_name_options,
        key="ta_export_user_full_name",
    )
    department_options = sorted(
        dept
        for dept in exports_df["Department"].dropna().astype(str).str.strip().unique().tolist()
        if dept
    )
    selected_department = g2.selectbox(
        "Department",
        options=[""] + department_options,
        key="ta_export_department",
    )

    filtered = exports_df.copy()
    filtered["Entry Date"] = pd.to_datetime(filtered["Entry Date"], errors="coerce").dt.date
    filtered = filtered[(filtered["Entry Date"] >= date_from) & (filtered["Entry Date"] <= date_to)]
    if selected_year:
        filtered = filtered[
            pd.to_numeric(filtered["Fiscal Year"], errors="coerce").eq(int(selected_year)).fillna(False)
        ]
    if selected_period:
        filtered = filtered[
            filtered["Fiscal Period"].fillna("").astype(str).str.strip().eq(selected_period)
        ]
    if selected_full_name:
        filtered = filtered[
            filtered["Full Name"].fillna("").astype(str).str.strip().eq(selected_full_name)
        ]
    if selected_department:
        filtered = filtered[
            filtered["Department"].fillna("").astype(str).str.strip().eq(selected_department)
        ]

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
    load_time_allocation_exports.clear()
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

    exports_df = load_time_allocation_exports(TIME_ALLOCATION_DIR)
    if exports_df.empty:
        st.info("No time-allocation entries found.")
        return

    # Stable within-file position must be computed before filtering so rows
    # outside the visible filter are not silently deleted on save.
    exports_df = exports_df.copy()
    exports_df["_source_pos"] = exports_df.groupby("Source File").cumcount()
    exports_df = _attach_fiscal_columns(exports_df)

    today = utils.to_eastern(utils.now_utc()).date()
    entry_dt = pd.to_datetime(exports_df["Entry Date"], errors="coerce")
    valid_dates = entry_dt.dropna()
    data_min = valid_dates.min().date() if not valid_dates.empty else today
    data_max = valid_dates.max().date() if not valid_dates.empty else today

    # Default the window to the most recent fiscal year through today.
    fiscal_year_start = _most_recent_fiscal_year_start()
    default_from = fiscal_year_start if fiscal_year_start is not None else data_min
    default_to = today
    widget_min = min(data_min, default_from)
    widget_max = max(data_max, default_to)
    default_from = max(widget_min, min(default_from, widget_max))
    default_to = max(widget_min, min(default_to, widget_max))

    year_options = sorted(
        {int(y) for y in pd.to_numeric(exports_df["Fiscal Year"], errors="coerce").dropna().tolist()}
    )
    year_choice_labels = [""] + [str(y) for y in year_options]

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        date_from = st.date_input(
            "From",
            value=default_from,
            min_value=widget_min,
            max_value=widget_max,
            key="ta_admin_editor_from",
        )
    with f2:
        date_to = st.date_input(
            "To",
            value=default_to,
            min_value=widget_min,
            max_value=widget_max,
            key="ta_admin_editor_to",
        )
    with f3:
        selected_year = st.selectbox(
            "Fiscal Year",
            options=year_choice_labels,
            key="ta_admin_editor_fiscal_year",
        )
    with f4:
        if selected_year:
            scoped = exports_df[
                pd.to_numeric(exports_df["Fiscal Year"], errors="coerce").eq(int(selected_year))
            ]
            period_pool = scoped["Fiscal Period"]
        else:
            period_pool = exports_df["Fiscal Period"]
        period_options = sorted(
            {p for p in period_pool.dropna().astype(str).str.strip().tolist() if p}
        )
        stored_period = st.session_state.get("ta_admin_editor_fiscal_period", "")
        if stored_period and stored_period not in period_options:
            st.session_state["ta_admin_editor_fiscal_period"] = ""
        selected_period = st.selectbox(
            "Fiscal Period",
            options=[""] + period_options,
            key="ta_admin_editor_fiscal_period",
        )

    g1, g2, _, _ = st.columns(4)
    full_name_options = sorted(
        name
        for name in exports_df["Full Name"].dropna().astype(str).str.strip().unique().tolist()
        if name
    )
    selected_full_name = g1.selectbox(
        "User",
        options=[""] + full_name_options,
        key="ta_admin_editor_user_full_name",
    )

    department_options = sorted(
        dept
        for dept in exports_df["Department"].dropna().astype(str).str.strip().unique().tolist()
        if dept
    )
    selected_department = g2.selectbox(
        "Department",
        options=[""] + department_options,
        key="ta_admin_editor_department",
    )

    filtered = exports_df.copy()
    filtered["Entry Date"] = pd.to_datetime(filtered["Entry Date"], errors="coerce").dt.date
    filtered = filtered[(filtered["Entry Date"] >= date_from) & (filtered["Entry Date"] <= date_to)]
    if selected_year:
        filtered = filtered[
            pd.to_numeric(filtered["Fiscal Year"], errors="coerce").eq(int(selected_year)).fillna(False)
        ]
    if selected_period:
        filtered = filtered[
            filtered["Fiscal Period"].fillna("").astype(str).str.strip().eq(selected_period)
        ]
    if selected_full_name:
        filtered = filtered[
            filtered["Full Name"].fillna("").astype(str).str.strip().eq(selected_full_name)
        ]
    if selected_department:
        filtered = filtered[
            filtered["Department"].fillna("").astype(str).str.strip().eq(selected_department)
        ]

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
    channel_choices = list(_ordered_channel_options()) + [
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

    # Remount the editor on filter change so stale pending edits don't bleed across views.
    editor_key = (
        f"ta_admin_editor_table_{date_from.isoformat()}_{date_to.isoformat()}"
        f"_{selected_year}_{selected_period}_{selected_full_name}_{selected_department}_{len(filtered)}"
    )
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

    load_time_allocation_exports.clear()
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


# --- Admin: add new entries (mirrors the Input tab; current fiscal period only) ---

@st.cache_data(ttl=300)
def _user_login_lookup() -> dict[str, str]:
    """Map full name (lowercased) -> user login from users.parquet for the add-entry user picker."""
    lookup: dict[str, str] = {}
    for login, full_name in utils.load_user_fullname_map().items():
        key = str(full_name or "").strip().lower()
        if key and key not in lookup:
            lookup[key] = str(login or "").strip()
    return lookup


def _current_period_bounds() -> tuple[date, date] | None:
    """Return (start, end) of the fiscal period containing today, or None if not configured."""
    period = _get_current_period()
    if not period:
        return None
    start = _parse_date_value(period.get("StartDate"))
    end = _parse_date_value(period.get("EndDate"))
    if start is None or end is None or end < start:
        return None
    return start, end


def _is_within_current_period(day: date) -> bool:
    """True when `day` falls inside the current fiscal period."""
    bounds = _current_period_bounds()
    if bounds is None:
        return False
    start, end = bounds
    return start <= day <= end


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
    """Append admin-entered rows to a user's saved entries for one day (current period only)."""
    if not _is_within_current_period(entry_date):
        bounds = _current_period_bounds()
        if bounds is None:
            st.error("No current fiscal period is configured, so entries can't be added.")
        else:
            start, end = bounds
            st.error(
                f"Entries can only be added within the current fiscal period "
                f"({start:%m/%d/%Y}–{end:%m/%d/%Y}). Cannot save for {entry_date:%m/%d/%Y}."
            )
        return False

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
        load_time_allocation_exports.clear()
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
    """Admin-only form to add new entries for any user, restricted to the current fiscal period."""
    if "ta_add_confirm_open" not in st.session_state:
        st.session_state["ta_add_confirm_open"] = False
    if "ta_add_confirm_rendered" not in st.session_state:
        st.session_state["ta_add_confirm_rendered"] = False
    if "ta_add_confirm_payload" not in st.session_state:
        st.session_state["ta_add_confirm_payload"] = None

    st.subheader("Add Entries", anchor=False)

    bounds = _current_period_bounds()
    if bounds is None:
        st.info(
            "No current fiscal period is configured, so entries can't be added here. "
            "Set up the current period to enable this section."
        )
        return
    period_start, period_end = bounds
    period_label = str((_get_current_period() or {}).get("PeriodName") or "").strip()
    period_suffix = f" ({period_label})" if period_label else ""
    st.caption(
        "Add new entries for any user; rows are appended to that user's existing entries "
        f"for the day. Limited to the current fiscal period{period_suffix}: "
        f"{period_start:%m/%d/%Y}–{period_end:%m/%d/%Y}."
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
        default_date = _today_eastern()
        if not (period_start <= default_date <= period_end):
            default_date = period_start
        entry_date = st.date_input(
            "Entry Date",
            value=default_date,
            min_value=period_start,
            max_value=period_end,
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
                customer_code = st.selectbox(
                    "Customer Code",
                    options=_customer_code_options_for_row(
                        [""] + account_lookup["customer_codes"],
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
                "customer_code": customer_code,
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
        if not _is_within_current_period(entry_date):
            errors.append("Entry Date must be within the current fiscal period.")
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


def render_admin_settings_view(account_lookup: dict, account_options: list[str]) -> None:
    """Admin-only Time Allocation Tool settings: entry fields, add-entry form, and the Edit Entries table."""
    if not utils.is_current_user_admin():
        st.info("Sorry, you don't have access to this section.")
        return

    render_entry_fields_editor_view()

    st.divider()
    render_admin_add_entry_view(account_options, account_lookup)

    st.divider()
    render_admin_data_editor_view(account_lookup)


# Header
utils.render_page_header(PAGE_TITLE)

user_login = utils.get_os_user()
full_name = utils.get_full_name_for_user(None, user_login)
department = utils.get_user_department(user_login, full_name=full_name) or ""
is_admin_user = utils.is_current_user_admin()

account_lookup = _build_account_lookup(utils.load_account_lookup(str(PERSONNEL_DIR)))
account_options = [""] + account_lookup["reporting_names"]

if is_admin_user:
    input_tab, exports_tab, settings_tab = st.tabs(
        ["Input", "Exports", "Admin Settings"], width="stretch"
    )
    with input_tab:
        render_input_view(user_login, full_name, department, account_options, account_lookup)
    with exports_tab:
        render_exports_view()
    with settings_tab:
        render_admin_settings_view(account_lookup, account_options)
else:
    render_input_view(user_login, full_name, department, account_options, account_lookup)
