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
from pathlib import Path

import pandas as pd
import pyarrow as pa
import streamlit as st

import config
import utils

LOGGER = utils.get_page_logger("Time Allocation Tool")

st.set_page_config(page_title="Time Allocation Tool", layout="wide")
utils.log_page_open_once("time_allocation_tool_page", LOGGER)
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

TIME_ALLOCATION_DIR = config.TIME_ALLOCATION_DIR
PERSONNEL_DIR = config.PERSONNEL_DIR
LOGO_PATH = config.LOGO_PATH
CHANNEL_OPTIONS = ["Projects", "Resupply"]

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


def render_input_view(
    user_login: str,
    full_name: str,
    department: str,
    account_options: list[str],
) -> None:
    """Render input controls and save parquet entries."""
    left, right = st.columns(2)
    with left:
        st.text_input("User", value=user_login, disabled=True)
    with right:
        st.text_input("Department", value=department, disabled=True)

    entry_mode = st.radio(
        "Input Type",
        options=["Simple", "Detailed"],
        horizontal=True,
        key="ta_input_type",
    )

    rows: list[dict[str, object]] = []
    parsed_seconds: list[int] = []

    if entry_mode == "Simple":
        top_a, top_b = st.columns(2)
        with top_a:
            total_hours = float(
                st.number_input(
                    "Number of Hours",
                    min_value=0.0,
                    value=8.0,
                    step=0.25,
                    key="ta_simple_total_hours",
                )
            )
        with top_b:
            number_of_accounts = int(
                st.number_input(
                    "Number of Accounts",
                    min_value=1,
                    max_value=50,
                    value=1,
                    step=1,
                    key="ta_simple_count",
                )
            )

        st.caption("Enter account allocations. Percentages should total 100%.")
        for idx in range(number_of_accounts):
            c1, c2, c3 = st.columns([4, 2, 2])
            with c1:
                account = st.selectbox(
                    f"Account {idx + 1}",
                    options=account_options,
                    key=f"ta_simple_account_{idx}",
                )
            with c2:
                percentage = float(
                    st.number_input(
                        f"% of Total Time {idx + 1}",
                        min_value=0.0,
                        max_value=100.0,
                        value=0.0,
                        step=1.0,
                        key=f"ta_simple_pct_{idx}",
                    )
                )
            with c3:
                channel = st.selectbox(
                    f"Channel {idx + 1}",
                    options=CHANNEL_OPTIONS,
                    key=f"ta_simple_channel_{idx}",
                )
            rows.append(
                {
                    "account": account,
                    "percentage": percentage,
                    "channel": channel,
                }
            )

        total_percentage = sum(float(r["percentage"]) for r in rows)
        st.caption(f"Current total percentage: {total_percentage:.2f}%")
        if abs(total_percentage - 100.0) > 0.01:
            st.warning("Total percentage must equal 100% before saving.")

        if st.button("Save Allocation", type="primary", key="ta_save_simple"):
            errors: list[str] = []
            if total_hours <= 0:
                errors.append("Number of hours must be greater than zero.")
            if abs(total_percentage - 100.0) > 0.01:
                errors.append("Percentages must total 100%.")
            for idx, row in enumerate(rows, start=1):
                if not _empty_to_none(row["account"]):
                    errors.append(f"Account {idx} is required.")
                if float(row["percentage"]) <= 0:
                    errors.append(f"% of Total Time for row {idx} must be greater than zero.")
            if errors:
                for message in errors:
                    st.error(message)
                return

            parsed_seconds = [
                int(round(total_hours * 3600.0 * float(row["percentage"]) / 100.0))
                for row in rows
            ]
            save_records(rows, parsed_seconds, user_login, full_name, department)

    else:
        number_of_accounts = int(
            st.number_input(
                "Number of Accounts",
                min_value=1,
                max_value=50,
                value=1,
                step=1,
                key="ta_detailed_count",
            )
        )
        st.caption("Enter one duration per row in HH:MM or HH:MM:SS format.")
        for idx in range(number_of_accounts):
            c1, c2, c3 = st.columns([4, 2, 2])
            with c1:
                account = st.selectbox(
                    f"Account {idx + 1}",
                    options=account_options,
                    key=f"ta_detailed_account_{idx}",
                )
            with c2:
                duration = st.text_input(
                    f"Time Duration {idx + 1}",
                    value="00:00",
                    key=f"ta_detailed_duration_{idx}",
                )
            with c3:
                channel = st.selectbox(
                    f"Channel {idx + 1}",
                    options=CHANNEL_OPTIONS,
                    key=f"ta_detailed_channel_{idx}",
                )
            rows.append(
                {
                    "account": account,
                    "duration": duration,
                    "channel": channel,
                }
            )

        if st.button("Save Allocation", type="primary", key="ta_save_detailed"):
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

            save_records(rows, parsed_seconds, user_login, full_name, department)


def save_records(
    rows: list[dict[str, object]],
    parsed_seconds: list[int],
    user_login: str,
    full_name: str,
    department: str,
) -> None:
    """Build and persist time-allocation records to a parquet file."""
    now_et = utils.to_eastern(utils.now_utc())
    entry_date = now_et.date()

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

    df = pd.DataFrame(records)
    output_name = f"time_allocation_{now_et:%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:8]}.parquet"
    output_path = TIME_ALLOCATION_DIR / output_name
    try:
        utils.atomic_write_parquet(df, output_path, schema=TIME_ALLOCATION_SCHEMA)
        load_time_allocation_exports.clear()
        LOGGER.info(
            "Saved time allocation export | rows=%s file='%s' user='%s'",
            len(df),
            str(output_path),
            user_login,
        )
        st.success(f"Saved {len(df)} row(s) to {output_path}.")
    except Exception as exc:
        LOGGER.exception("Failed to save time allocation export: %s", exc)
        st.error(f"Failed to save parquet file: {exc}")


def render_exports_view() -> None:
    """Render admin export view with filters and CSV download."""
    exports_df = load_time_allocation_exports(TIME_ALLOCATION_DIR)
    if exports_df.empty:
        st.info("No time-allocation exports found.")
        return

    entry_dt = pd.to_datetime(exports_df["Entry Date"], errors="coerce")
    valid_dates = entry_dt.dropna()
    min_date = valid_dates.min().date() if not valid_dates.empty else utils.to_eastern(utils.now_utc()).date()
    max_date = valid_dates.max().date() if not valid_dates.empty else min_date

    f1, f2 = st.columns(2)
    with f1:
        date_from = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date, key="ta_export_from")
    with f2:
        date_to = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date, key="ta_export_to")

    users = sorted(
        u
        for u in exports_df["User"].dropna().astype(str).str.strip().unique().tolist()
        if u
    )
    selected_users = st.multiselect("Users", options=users, default=users, key="ta_export_users")

    filtered = exports_df.copy()
    filtered["Entry Date"] = pd.to_datetime(filtered["Entry Date"], errors="coerce").dt.date
    filtered = filtered[(filtered["Entry Date"] >= date_from) & (filtered["Entry Date"] <= date_to)]
    if selected_users:
        filtered = filtered[filtered["User"].astype(str).isin(selected_users)]
    else:
        filtered = filtered.iloc[0:0]

    st.dataframe(filtered, hide_index=True, width="stretch")

    csv_bytes = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"time_allocation_export_{utils.to_eastern(utils.now_utc()):%Y%m%d_%H%M%S}.csv",
        mime="text/csv",
        disabled=filtered.empty,
    )


# Header
logo_b64 = utils.get_logo_base64(str(LOGO_PATH))
st.markdown(
    f"""
    <div class="header-row">
        <img class="header-logo" src="data:image/png;base64,{logo_b64}" />
        <h1 class="header-title">LS - Time Allocation Tool</h1>
    </div>
    """,
    unsafe_allow_html=True,
)
st.divider()

user_login = utils.get_os_user()
full_name = utils.get_full_name_for_user(None, user_login)
department = utils.get_user_department(user_login) or ""
is_admin_user = utils.is_current_user_admin()
account_options = [""] + utils.load_accounts(str(PERSONNEL_DIR))

if is_admin_user:
    page_mode = st.radio(
        "Mode",
        options=["Input", "Exports"],
        horizontal=True,
        key="ta_page_mode",
        label_visibility="collapsed",
    )
else:
    page_mode = "Input"

if page_mode == "Exports":
    render_exports_view()
else:
    render_input_view(user_login, full_name, department, account_options)

st.caption(f"\n\n\nApp version: {config.APP_VERSION}", text_alignment="center")
