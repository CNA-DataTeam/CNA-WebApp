"""
pages/admin-logs.py

Purpose:
    Admin-only page for reviewing application logs across users.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import re

import pandas as pd
import streamlit as st

import config
import utils


LOGGER = utils.get_page_logger("Admin Logs")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Logging")

st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
utils.log_page_open_once("admin_logs_page", LOGGER)

if not utils.is_current_user_admin():
    LOGGER.warning("Access denied for non-admin user '%s'.", utils.get_os_user())
    st.error("Access denied. This page is available to admin users only.")
    st.stop()

utils.render_page_header(PAGE_TITLE, config.LOGO_PATH)

LOGS_ROOT = Path(config.LOGS_ROOT_DIR)
LOG_FILE_NAME = str(config.LOG_USER_FILE_NAME)
LOG_PATTERN = re.compile(
    r"^\s*(?P<ts>[^|]+?)\s*\|\s*(?P<level>[A-Za-z]+)\s*\|\s*\[(?P<page>[^\]]+)\]\s*(?P<message>.*)$"
)


def _normalize_col_name(value: object) -> str:
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def _find_col(df: pd.DataFrame, aliases: list[str]) -> str | None:
    alias_set = {_normalize_col_name(a) for a in aliases}
    for col in df.columns:
        if _normalize_col_name(col) in alias_set:
            return str(col)
    return None


def _normalize_login(value: object) -> str:
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
def load_user_metadata_maps() -> tuple[dict[str, str], dict[str, str]]:
    """Map user login -> full name and department from users.parquet."""
    users_df = utils.load_users_table()
    if users_df.empty:
        return {}, {}

    user_col = _find_col(
        users_df,
        ["User", "UserLogin", "Login", "Username", "User Name", "NetworkLogin", "SamAccountName"],
    )
    full_name_col = _find_col(users_df, ["Full Name", "FullName", "Name"])
    dept_col = _find_col(users_df, ["Department", "Dept"])
    if not user_col:
        return {}, {}

    user_series = users_df[user_col].fillna("").astype(str).map(_normalize_login)
    if full_name_col:
        full_name_series = users_df[full_name_col].fillna("").astype(str).str.strip()
    else:
        full_name_series = pd.Series([""] * len(users_df), index=users_df.index)
    if dept_col:
        dept_series = users_df[dept_col].fillna("").astype(str).str.strip()
    else:
        dept_series = pd.Series([""] * len(users_df), index=users_df.index)

    full_name_map: dict[str, str] = {}
    department_map: dict[str, str] = {}
    for login, full_name, department in zip(user_series, full_name_series, dept_series):
        if not login:
            continue
        if full_name:
            full_name_map[login] = str(full_name).strip()
        if department:
            department_map[login] = str(department).strip()
    return full_name_map, department_map


@st.cache_data(ttl=60)
def load_all_logs(logs_root: Path, log_file_name: str, max_lines_per_file: int = 20000) -> pd.DataFrame:
    """Load and parse all per-user app logs under the logs root."""
    files = sorted(logs_root.glob(f"*/{log_file_name}"))
    if not files:
        return pd.DataFrame(columns=["Timestamp", "User", "Page", "Level", "Message", "RawLine"])

    rows: list[dict[str, object]] = []
    for file_path in files:
        user_key = file_path.parent.name
        try:
            lines = file_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception as exc:
            rows.append(
                {
                    "Timestamp": pd.NaT,
                    "User": user_key,
                    "Page": "Log Read Error",
                    "Level": "ERROR",
                    "Message": f"Failed to read log file '{file_path}': {exc}",
                    "RawLine": "",
                }
            )
            continue

        if max_lines_per_file > 0 and len(lines) > max_lines_per_file:
            lines = lines[-max_lines_per_file:]

        for line in lines:
            match = LOG_PATTERN.match(line)
            if match:
                ts_text = match.group("ts").strip()
                parsed_ts = pd.to_datetime(ts_text, errors="coerce")
                rows.append(
                    {
                        "Timestamp": parsed_ts,
                        "User": user_key,
                        "Page": match.group("page").strip(),
                        "Level": match.group("level").strip().upper(),
                        "Message": match.group("message").strip(),
                        "RawLine": line,
                    }
                )
            else:
                rows.append(
                    {
                        "Timestamp": pd.NaT,
                        "User": user_key,
                        "Page": "Unparsed",
                        "Level": "",
                        "Message": line.strip(),
                        "RawLine": line,
                    }
                )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["Timestamp", "User", "Page", "Level", "Message", "RawLine"])

    df = df.sort_values(["Timestamp", "User"], ascending=[False, True], na_position="last").reset_index(drop=True)
    return df


if not LOGS_ROOT.exists():
    st.error(f"Logs root not found:\n{LOGS_ROOT}")
    st.stop()

logs_df = load_all_logs(LOGS_ROOT, LOG_FILE_NAME)
if logs_df.empty:
    st.info("No logs found.")
    st.stop()

full_name_map, department_map = load_user_metadata_maps()
logs_df = logs_df.copy()
logs_df["User Login"] = logs_df["User"].fillna("").astype(str).str.strip()
logs_df["User"] = logs_df["User Login"].map(
    lambda login: full_name_map.get(_normalize_login(login), str(login).strip())
)
logs_df["Department"] = logs_df["User Login"].map(
    lambda login: department_map.get(_normalize_login(login), "")
)

top_a, top_b, top_c, top_d = st.columns(4)
with top_a:
    date_filter = st.selectbox("Date", options=["All", "Today", "This Week"], index=0)
with top_b:
    dept_options = ["All"] + sorted(
        d for d in logs_df["Department"].dropna().astype(str).str.strip().unique().tolist() if d
    )
    selected_department = st.selectbox("Department", options=dept_options, index=0)
with top_c:
    user_options = ["All"] + sorted(
        u for u in logs_df["User"].dropna().astype(str).str.strip().unique().tolist() if u
    )
    selected_user = st.selectbox("User", options=user_options, index=0)
with top_d:
    page_options = ["All"] + sorted(
        p for p in logs_df["Page"].dropna().astype(str).str.strip().unique().tolist() if p
    )
    selected_page = st.selectbox("Page", options=page_options, index=0)

filtered_df = logs_df.copy()
timestamp_series = pd.to_datetime(filtered_df["Timestamp"], errors="coerce")
if date_filter == "Today":
    today = datetime.now().date()
    mask = timestamp_series.dt.date.eq(today)
    filtered_df = filtered_df[mask.fillna(False)]
elif date_filter == "This Week":
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    day_values = timestamp_series.dt.date
    mask = day_values.ge(week_start) & day_values.le(today)
    filtered_df = filtered_df[mask.fillna(False)]

if selected_department != "All":
    filtered_df = filtered_df[filtered_df["Department"] == selected_department]
if selected_user != "All":
    filtered_df = filtered_df[filtered_df["User"] == selected_user]
if selected_page != "All":
    filtered_df = filtered_df[filtered_df["Page"] == selected_page]

st.caption(f"Rows: {len(filtered_df):,}")
display_df = filtered_df[["Timestamp", "User", "Department", "Page", "Level", "Message"]].copy()
display_df["Timestamp"] = pd.to_datetime(display_df["Timestamp"], errors="coerce")
st.dataframe(
    display_df,
    hide_index=True,
    width="stretch",
    column_config={
        "Timestamp": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm:ss"),
        "Message": st.column_config.TextColumn("Message", width="large"),
    },
)
