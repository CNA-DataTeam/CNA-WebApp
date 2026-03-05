"""
pages/time-allocation-tool.py

Purpose:
    Capture employee time-allocation entries by account/channel and export results.

What it does:
    - Input mode (all users):
        * Auto-fills User and Department
        * Supports Simple and Detailed entry styles
        * Writes a parquet file per submission to config.TIME_ALLOCATION_DIR
    - Exports mode (admins only):
        * Loads saved parquet entries
        * Provides date/user filters and CSV export

Output schema:
    Entry Date | User | Full Name | Department | Account | Time | Channel
"""

from __future__ import annotations

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

st.set_page_config(page_title=PAGE_TITLE, layout="wide")
utils.log_page_open_once("time_allocation_tool_page", LOGGER)
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

TIME_ALLOCATION_DIR = config.TIME_ALLOCATION_DIR
PERSONNEL_DIR = config.PERSONNEL_DIR
CHANNEL_OPTIONS = ["Projects", "Resupply"]
DEFAULT_CHANNEL_INDEX = CHANNEL_OPTIONS.index("Resupply") if "Resupply" in CHANNEL_OPTIONS else 0

TIME_ALLOCATION_SCHEMA = pa.schema(
    [
        ("Entry Date", pa.date32()),
        ("User", pa.string()),
        ("Full Name", pa.string()),
        ("Department", pa.string()),
        ("Account", pa.string()),
        ("Time", pa.string()),
        ("Channel", pa.string()),
    ]
)


def _empty_to_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


@st.cache_data(ttl=30)
def load_time_allocation_exports(base_dir: Path) -> pd.DataFrame:
    """Load all saved time-allocation parquet files from the output directory."""
    files = sorted(base_dir.glob("*.parquet"), reverse=True)
    if not files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for file_path in files:
        try:
            one = pd.read_parquet(file_path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable export file '%s': %s", file_path, exc)
            continue
        if one.empty:
            continue
        one["Source File"] = file_path.name
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


def _allocate_percentages(total_seconds: int, row_seconds: list[int]) -> list[int]:
    """Convert row seconds to integer percentages that sum to 100 when possible."""
    if total_seconds <= 0 or not row_seconds:
        return [0 for _ in row_seconds]

    raw = [(max(0, sec) * 100.0 / total_seconds) for sec in row_seconds]
    floors = [int(value) for value in raw]
    remainder = max(0, 100 - sum(floors))
    order = sorted(range(len(raw)), key=lambda idx: (raw[idx] - floors[idx]), reverse=True)
    for idx in order[:remainder]:
        floors[idx] += 1
    return floors


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


def _parse_date_value(value: object) -> date | None:
    """Safely parse date-like values into a date object."""
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


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


def _coerce_channel(value: object) -> str:
    text = str(value or "").strip()
    return text if text in CHANNEL_OPTIONS else CHANNEL_OPTIONS[DEFAULT_CHANNEL_INDEX]


def _seed_input_state_for_day(selected_day: date, selected_day_df: pd.DataFrame, account_options: list[str]) -> None:
    """
    Initialize the input widgets from selected-day data.
    If no day data exists, reset to default blank state.
    """
    day_token = selected_day.isoformat()
    if st.session_state.get("ta_loaded_day_token") == day_token:
        return

    rows: list[dict[str, object]] = []
    if not selected_day_df.empty:
        for _, row in selected_day_df.iterrows():
            rows.append(
                {
                    "account": _coerce_account(row.get("Account"), account_options),
                    "channel": _coerce_channel(row.get("Channel")),
                    "time": str(row.get("Time") or "00:00"),
                }
            )

    if not rows:
        st.session_state["ta_simple_count"] = 1
        st.session_state["ta_simple_total_hours"] = 8.0
        st.session_state["ta_simple_account_0"] = ""
        st.session_state["ta_simple_pct_0"] = 0
        st.session_state["ta_simple_channel_0"] = CHANNEL_OPTIONS[DEFAULT_CHANNEL_INDEX]

        st.session_state["ta_detailed_count"] = 1
        st.session_state["ta_detailed_account_0"] = ""
        st.session_state["ta_detailed_duration_0"] = "00:00"
        st.session_state["ta_detailed_channel_0"] = CHANNEL_OPTIONS[DEFAULT_CHANNEL_INDEX]
        if "ta_input_type" not in st.session_state:
            st.session_state["ta_input_type"] = "Simple"
        st.session_state["ta_loaded_day_token"] = day_token
        return

    row_seconds = [max(0, utils.parse_hhmmss(row["time"])) for row in rows]
    total_seconds = int(sum(row_seconds))
    number_of_rows = len(rows)
    percentages = _allocate_percentages(total_seconds, row_seconds)
    total_hours = round(total_seconds / 3600.0, 2) if total_seconds > 0 else 0.0

    st.session_state["ta_simple_count"] = number_of_rows
    st.session_state["ta_simple_total_hours"] = total_hours if total_hours > 0 else 8.0
    st.session_state["ta_detailed_count"] = number_of_rows

    for idx, row in enumerate(rows):
        st.session_state[f"ta_simple_account_{idx}"] = row["account"]
        st.session_state[f"ta_simple_pct_{idx}"] = int(percentages[idx])
        st.session_state[f"ta_simple_channel_{idx}"] = row["channel"]

        st.session_state[f"ta_detailed_account_{idx}"] = row["account"]
        st.session_state[f"ta_detailed_duration_{idx}"] = row["time"]
        st.session_state[f"ta_detailed_channel_{idx}"] = row["channel"]

    # Default to Detailed only on first load to preserve exact loaded durations.
    if "ta_input_type" not in st.session_state:
        st.session_state["ta_input_type"] = "Detailed"
    st.session_state["ta_loaded_day_token"] = day_token


def _delete_simple_row(delete_idx: int) -> None:
    """Delete one row from Simple mode widget state."""
    count = int(st.session_state.get("ta_simple_count", 1) or 1)
    if count <= 1 or delete_idx < 0 or delete_idx >= count:
        return

    for idx in range(delete_idx, count - 1):
        st.session_state[f"ta_simple_account_{idx}"] = st.session_state.get(f"ta_simple_account_{idx + 1}", "")
        st.session_state[f"ta_simple_pct_{idx}"] = int(float(st.session_state.get(f"ta_simple_pct_{idx + 1}", 0) or 0))
        st.session_state[f"ta_simple_channel_{idx}"] = st.session_state.get(
            f"ta_simple_channel_{idx + 1}",
            CHANNEL_OPTIONS[DEFAULT_CHANNEL_INDEX],
        )

    last_idx = count - 1
    for key in (f"ta_simple_account_{last_idx}", f"ta_simple_pct_{last_idx}", f"ta_simple_channel_{last_idx}"):
        if key in st.session_state:
            del st.session_state[key]

    st.session_state["ta_simple_count"] = count - 1


def _delete_detailed_row(delete_idx: int) -> None:
    """Delete one row from Detailed mode widget state."""
    count = int(st.session_state.get("ta_detailed_count", 1) or 1)
    if count <= 1 or delete_idx < 0 or delete_idx >= count:
        return

    for idx in range(delete_idx, count - 1):
        st.session_state[f"ta_detailed_account_{idx}"] = st.session_state.get(f"ta_detailed_account_{idx + 1}", "")
        st.session_state[f"ta_detailed_duration_{idx}"] = st.session_state.get(f"ta_detailed_duration_{idx + 1}", "00:00")
        st.session_state[f"ta_detailed_channel_{idx}"] = st.session_state.get(
            f"ta_detailed_channel_{idx + 1}",
            CHANNEL_OPTIONS[DEFAULT_CHANNEL_INDEX],
        )

    last_idx = count - 1
    for key in (f"ta_detailed_account_{last_idx}", f"ta_detailed_duration_{last_idx}", f"ta_detailed_channel_{last_idx}"):
        if key in st.session_state:
            del st.session_state[key]

    st.session_state["ta_detailed_count"] = count - 1


def render_input_day_selector(user_login: str, full_name: str) -> tuple[date, pd.DataFrame]:
    """Render clickable 7-day calendar and return selected day + selected day rows."""
    st.subheader("Your Last 7 Days", anchor=False)

    today = utils.to_eastern(utils.now_utc()).date()
    day_window = [today - timedelta(days=offset) for offset in range(6, -1, -1)]
    window_start = day_window[0]
    window_end = day_window[-1]

    if "ta_input_selected_day" not in st.session_state:
        st.session_state["ta_input_selected_day"] = today
    selected_day = _parse_date_value(st.session_state.get("ta_input_selected_day", today)) or today
    st.session_state["ta_input_selected_day"] = selected_day
    if selected_day < window_start or selected_day > window_end:
        selected_day = today
        st.session_state["ta_input_selected_day"] = selected_day

    exports_df = load_time_allocation_exports(TIME_ALLOCATION_DIR)
    user_df = _filter_user_exports(exports_df, user_login, full_name)
    window_df = user_df[(user_df["Entry Date"] >= window_start) & (user_df["Entry Date"] <= window_end)].copy()
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

    calendar_options = {
        "initialView": "dayGridSevenDay",
        "views": {
            "dayGridSevenDay": {
                "type": "dayGrid",
                "duration": {"days": 7},
                "buttonText": "7 days",
            }
        },
        "editable": False,
        "selectable": False,
        "eventDisplay": "block",
        "dayMaxEventRows": 4,
        "headerToolbar": {"left": "", "center": "title", "right": ""},
        "height": 180,
        "initialDate": window_start.isoformat(),
    }
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
    calendar_widget_key = "ta_input_week_calendar"
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


def render_input_view(
    user_login: str,
    full_name: str,
    department: str,
    account_options: list[str],
) -> None:
    """Render input controls and save parquet entries."""
    if "ta_confirm_open" not in st.session_state:
        st.session_state.ta_confirm_open = False
    if "ta_confirm_rendered" not in st.session_state:
        st.session_state.ta_confirm_rendered = False
    if "ta_confirm_payload" not in st.session_state:
        st.session_state.ta_confirm_payload = None

    selected_day, selected_day_df = render_input_day_selector(user_login, full_name)
    _seed_input_state_for_day(selected_day, selected_day_df, account_options)

    pending_delete_idx = st.session_state.pop("ta_detailed_delete_idx", None)
    if pending_delete_idx is not None:
        _delete_detailed_row(int(pending_delete_idx))

    rows = []
    number_of_accounts = int(max(1, st.session_state.get("ta_detailed_count", 1) or 1))
    st.session_state["ta_detailed_count"] = number_of_accounts

    preview_seconds: list[int] = []
    for idx in range(number_of_accounts):
        duration_value = st.session_state.get(f"ta_detailed_duration_{idx}", "00:00")
        preview_seconds.append(max(0, utils.parse_hhmmss(str(duration_value))))
    total_preview_seconds = sum(preview_seconds)

    st.caption("Enter one duration per row in HH:MM or HH:MM:SS format.")
    for idx in range(number_of_accounts):
        row_seconds = preview_seconds[idx]
        row_percentage = (row_seconds * 100.0 / total_preview_seconds) if total_preview_seconds > 0 else 0.0

        c1, c2, c3, c4, c5 = st.columns([4, 2, 1.5, 2, 1.2])
        with c1:
            account_key = f"ta_detailed_account_{idx}"
            account = st.selectbox(
                "Account",
                options=_account_options_for_row(account_options, st.session_state.get(account_key, "")),
                key=account_key,
            )
        with c2:
            duration = st.text_input(
                "Time Duration",
                value="00:00",
                key=f"ta_detailed_duration_{idx}",
            )
        with c3:
            st.caption("% of Total Time")
            st.text(f"{row_percentage:.2f}%")
        with c4:
            channel = st.selectbox(
                "Channel",
                options=CHANNEL_OPTIONS,
                index=DEFAULT_CHANNEL_INDEX,
                key=f"ta_detailed_channel_{idx}",
            )
        with c5:
            st.space("small")
            if st.button(
                "Delete",
                key=f"ta_detailed_delete_btn_{idx}",
                icon=":material/delete_outline:",
                disabled=number_of_accounts <= 1,
                type="tertiary",
                width="content",
            ):
                st.session_state["ta_detailed_delete_idx"] = idx
                st.rerun()
        rows.append(
            {
                "account": account,
                "duration": duration,
                "channel": channel,
            }
        )

    add_col, save_col = st.columns([1, 1])
    with add_col:
        if st.button("Add Row", key="ta_detailed_add_row_btn", icon=":material/add:", type="secondary"):
            st.session_state["ta_detailed_count"] = number_of_accounts + 1
            st.rerun()

    with save_col:
        save_detailed_clicked = st.button("Save Allocation", type="primary", key="ta_save_detailed")

    if save_detailed_clicked:
        errors: list[str] = []
        parsed_seconds = []
        for idx, row in enumerate(rows, start=1):
            if not _empty_to_none(row["account"]):
                errors.append(f"Account {idx} is required.")
            seconds = utils.parse_hhmmss(str(row["duration"]))
            if seconds <= 0:
                errors.append(f"Time Duration for row {idx} must be a valid time greater than 00:00.")
            parsed_seconds.append(max(0, seconds))
        if errors:
            for message in errors:
                st.error(message)
            return

        st.session_state.ta_confirm_payload = {
            "entry_mode": "Detailed",
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


def _ensure_time_allocation_columns(df: pd.DataFrame) -> pd.DataFrame:
    fixed = df.copy()
    for col_name in TIME_ALLOCATION_SCHEMA.names:
        if col_name not in fixed.columns:
            fixed[col_name] = pd.NA
    fixed["Entry Date"] = pd.to_datetime(fixed["Entry Date"], errors="coerce").dt.date
    return fixed[TIME_ALLOCATION_SCHEMA.names].copy()


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
    for file_path in sorted(base_dir.glob("*.parquet")):
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
                utils.atomic_write_parquet(remaining, file_path, schema=TIME_ALLOCATION_SCHEMA)
        except Exception as exc:
            LOGGER.exception("Failed to update export file '%s' while replacing rows: %s", file_path, exc)
            raise
    return removed_rows, touched_files


def save_records(
    rows: list[dict[str, object]],
    parsed_seconds: list[int],
    user_login: str,
    full_name: str,
    department: str,
    entry_date: date,
) -> None:
    """Build and persist time-allocation records to a parquet file."""
    now_et = utils.to_eastern(utils.now_utc())

    records: list[dict[str, object]] = []
    for row, seconds in zip(rows, parsed_seconds):
        records.append(
            {
                "Entry Date": entry_date,
                "User": _empty_to_none(user_login),
                "Full Name": _empty_to_none(full_name),
                "Department": _empty_to_none(department),
                "Account": _empty_to_none(row.get("account")),
                "Time": utils.format_hhmmss(seconds),
                "Channel": _empty_to_none(row.get("channel")),
            }
        )

    df = _ensure_time_allocation_columns(pd.DataFrame(records))
    output_name = f"time_allocation_{entry_date:%Y%m%d}_{now_et:%H%M%S}_{uuid.uuid4().hex[:8]}.parquet"
    output_path = TIME_ALLOCATION_DIR / output_name
    try:
        removed_rows, touched_files = _replace_user_day_entries(TIME_ALLOCATION_DIR, user_login, full_name, entry_date)
        utils.atomic_write_parquet(df, output_path, schema=TIME_ALLOCATION_SCHEMA)
        load_time_allocation_exports.clear()
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
            st.success(f"Updated {entry_date:%m/%d/%Y}: replaced {removed_rows} row(s) and saved {len(df)} row(s).")
        else:
            st.success(f"Saved {len(df)} row(s) for {entry_date:%m/%d/%Y}.")
    except Exception as exc:
        LOGGER.exception("Failed to save time allocation export: %s", exc)
        st.error(f"Failed to save parquet file: {exc}")


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
    entry_mode = str(payload.get("entry_mode") or "")
    display_user = full_name.strip() if str(full_name).strip() else user_login

    st.caption(f"**User:** {display_user}")
    st.caption(f"**Department:** {department or 'N/A'}")
    st.caption(f"**Entry Date:** {entry_date:%m/%d/%Y}")
    st.caption(f"**Input Type:** {entry_mode}")
    if entry_mode == "Simple":
        total_hours = float(payload.get("total_hours") or 0.0)
        total_percentage = float(payload.get("total_percentage") or 0.0)
        st.caption(f"**Total Hours:** {total_hours:.2f}")
        st.caption(f"**Total Percentage:** {total_percentage:.2f}%")
    st.divider()

    preview_rows: list[dict[str, object]] = []
    for row, seconds in zip(rows, parsed_seconds):
        preview_rows.append(
            {
                "Account": _empty_to_none(row.get("account")) or "",
                "Time": utils.format_hhmmss(seconds),
                "Channel": _empty_to_none(row.get("channel")) or "",
            }
        )
    preview_df = pd.DataFrame(preview_rows)
    st.dataframe(preview_df, hide_index=True, width="stretch")

    left, right = st.columns(2)
    with left:
        if st.button("Confirm", type="primary", width="stretch", key="ta_confirm_submit_button"):
            save_records(rows, parsed_seconds, user_login, full_name, department, entry_date)
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


def render_exports_view() -> None:
    """Render admin export view with filters and CSV download."""
    if not utils.is_current_user_admin():
        st.info("Sorry, you don't have access to this section")
        return

    exports_df = load_time_allocation_exports(TIME_ALLOCATION_DIR)
    if exports_df.empty:
        st.info("No time-allocation exports found.")
        return

    entry_dt = pd.to_datetime(exports_df["Entry Date"], errors="coerce")
    valid_dates = entry_dt.dropna()
    min_date = valid_dates.min().date() if not valid_dates.empty else utils.to_eastern(utils.now_utc()).date()
    max_date = valid_dates.max().date() if not valid_dates.empty else min_date

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        date_from = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date, key="ta_export_from")
    with f2:
        date_to = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date, key="ta_export_to")

    full_name_options = sorted(
        name
        for name in exports_df["Full Name"].dropna().astype(str).str.strip().unique().tolist()
        if name
    )
    selected_full_name = f3.selectbox(
        "User",
        options=[""] + full_name_options,
        key="ta_export_user_full_name",
    )

    department_options = sorted(
        dept
        for dept in exports_df["Department"].dropna().astype(str).str.strip().unique().tolist()
        if dept
    )
    selected_department = f4.selectbox(
        "Department",
        options=[""] + department_options,
        key="ta_export_department",
    )

    filtered = exports_df.copy()
    filtered["Entry Date"] = pd.to_datetime(filtered["Entry Date"], errors="coerce").dt.date
    filtered = filtered[(filtered["Entry Date"] >= date_from) & (filtered["Entry Date"] <= date_to)]
    if selected_full_name:
        filtered = filtered[
            filtered["Full Name"].fillna("").astype(str).str.strip().eq(selected_full_name)
        ]
    if selected_department:
        filtered = filtered[
            filtered["Department"].fillna("").astype(str).str.strip().eq(selected_department)
        ]

    display_df = filtered.drop(columns=["User"], errors="ignore")
    st.dataframe(display_df, hide_index=True, width="stretch")

    csv_bytes = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"time_allocation_export_{utils.to_eastern(utils.now_utc()):%Y%m%d_%H%M%S}.csv",
        mime="text/csv",
        disabled=filtered.empty,
    )


# Header
utils.render_page_header(PAGE_TITLE, config.LOGO_PATH)

user_login = utils.get_os_user()
full_name = utils.get_full_name_for_user(None, user_login)
department = utils.get_user_department(user_login, full_name=full_name) or ""
is_admin_user = utils.is_current_user_admin()
account_options = [""] + utils.load_accounts(str(PERSONNEL_DIR))

if is_admin_user:
    input_tab, exports_tab = st.tabs(["Input", "Exports"], width="stretch")
    with input_tab:
        render_input_view(user_login, full_name, department, account_options)
    with exports_tab:
        render_exports_view()
else:
    render_input_view(user_login, full_name, department, account_options)

st.caption(f"\n\n\nApp version: {config.APP_VERSION}", text_alignment="center")
