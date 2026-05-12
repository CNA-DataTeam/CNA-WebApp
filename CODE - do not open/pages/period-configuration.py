"""
pages/period-configuration.py

Purpose:
    Admin-only page for defining fiscal periods per year. Stores rows in a
    shared parquet file at {PERSONNEL_DIR}/fiscal_periods.parquet so that
    other pages can later consume them via utils.get_fiscal_period_for_date().

What it does:
    - Year selector
    - Editable table of periods (PeriodNumber / PeriodName / StartDate / EndDate)
    - Preset generators (12 monthly, 13 four-week, 4 quarterly)
    - Inline validation summary (sort, gaps, overlaps, invalid ranges)
    - Confirmation dialog before saving

Output schema (parquet):
    Year (int) | PeriodNumber (int) | PeriodName (str) | StartDate (date) | EndDate (date)
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

import config
import utils

LOGGER = utils.get_page_logger("Period Configuration")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Period Configuration")

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon=utils.get_app_icon())
utils.render_app_logo()
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
utils.log_page_open_once("period_configuration_page", LOGGER)

if not utils.is_current_user_admin():
    LOGGER.warning("Access denied for non-admin user '%s'.", utils.get_os_user())
    st.error("Access denied. This page is available to admin users only.")
    st.stop()

utils.render_page_header(PAGE_TITLE)

EDITOR_COLUMNS = ["PeriodNumber", "PeriodName", "StartDate", "EndDate"]


def _empty_editor_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "PeriodNumber": pd.Series(dtype="Int64"),
            "PeriodName": pd.Series(dtype="string"),
            "StartDate": pd.Series(dtype="object"),
            "EndDate": pd.Series(dtype="object"),
        }
    )


def _periods_to_editor_df(periods_df: pd.DataFrame) -> pd.DataFrame:
    """Strip the Year column and return the editor-shaped DataFrame."""
    if periods_df is None or periods_df.empty:
        return _empty_editor_df()
    out = periods_df[EDITOR_COLUMNS].copy()
    out["PeriodNumber"] = pd.to_numeric(out["PeriodNumber"], errors="coerce").astype("Int64")
    out["PeriodName"] = out["PeriodName"].fillna("").astype(str)
    out["StartDate"] = pd.to_datetime(out["StartDate"], errors="coerce").dt.date
    out["EndDate"] = pd.to_datetime(out["EndDate"], errors="coerce").dt.date
    return out.reset_index(drop=True)


def _generate_preset(year: int, preset: str) -> pd.DataFrame:
    """Return a DataFrame of periods for the chosen preset."""
    if preset == "12 Monthly Periods":
        rows = []
        for month in range(1, 13):
            start = date(year, month, 1)
            if month == 12:
                end = date(year, 12, 31)
            else:
                end = date(year, month + 1, 1) - timedelta(days=1)
            rows.append(
                {
                    "PeriodNumber": month,
                    "PeriodName": start.strftime("%B"),
                    "StartDate": start,
                    "EndDate": end,
                }
            )
        return pd.DataFrame(rows)

    if preset == "13 Four-Week Periods":
        rows = []
        start = date(year, 1, 1)
        for period in range(1, 14):
            if period < 13:
                end = start + timedelta(days=27)
            else:
                end = date(year, 12, 31)
            rows.append(
                {
                    "PeriodNumber": period,
                    "PeriodName": f"P{period}",
                    "StartDate": start,
                    "EndDate": end,
                }
            )
            start = end + timedelta(days=1)
        return pd.DataFrame(rows)

    if preset == "4 Quarters":
        rows = [
            {"PeriodNumber": 1, "PeriodName": "Q1", "StartDate": date(year, 1, 1), "EndDate": date(year, 3, 31)},
            {"PeriodNumber": 2, "PeriodName": "Q2", "StartDate": date(year, 4, 1), "EndDate": date(year, 6, 30)},
            {"PeriodNumber": 3, "PeriodName": "Q3", "StartDate": date(year, 7, 1), "EndDate": date(year, 9, 30)},
            {"PeriodNumber": 4, "PeriodName": "Q4", "StartDate": date(year, 10, 1), "EndDate": date(year, 12, 31)},
        ]
        return pd.DataFrame(rows)

    return _empty_editor_df()


def _validate(edited: pd.DataFrame, year: int) -> tuple[list[str], list[str]]:
    """Return (errors, warnings) for the current editor contents."""
    errors: list[str] = []
    warnings: list[str] = []

    if edited.empty:
        warnings.append("No periods defined yet.")
        return errors, warnings

    if edited["PeriodNumber"].isna().any():
        errors.append("Every row needs a Period Number.")
    if edited["StartDate"].isna().any() or edited["EndDate"].isna().any():
        errors.append("Every row needs both a Start Date and an End Date.")

    valid_rows = edited.dropna(subset=["PeriodNumber", "StartDate", "EndDate"]).copy()
    if valid_rows.empty:
        return errors, warnings

    bad_range = valid_rows[valid_rows["StartDate"] > valid_rows["EndDate"]]
    if not bad_range.empty:
        bad_list = ", ".join(str(int(p)) for p in bad_range["PeriodNumber"].tolist())
        errors.append(f"Start Date must be on or before End Date (bad rows: {bad_list}).")

    duplicates = valid_rows["PeriodNumber"].astype(int).duplicated()
    if duplicates.any():
        dup_list = ", ".join(
            str(int(p)) for p in valid_rows.loc[duplicates, "PeriodNumber"].tolist()
        )
        errors.append(f"Period Numbers must be unique (duplicates: {dup_list}).")

    out_of_year = valid_rows[
        (pd.to_datetime(valid_rows["StartDate"]).dt.year != int(year))
        | (pd.to_datetime(valid_rows["EndDate"]).dt.year != int(year))
    ]
    if not out_of_year.empty:
        warnings.append(
            f"{len(out_of_year)} period(s) have dates outside {year}. That's allowed but flagged."
        )

    sorted_rows = valid_rows.sort_values("PeriodNumber").reset_index(drop=True)
    for idx in range(len(sorted_rows) - 1):
        cur_end = sorted_rows.loc[idx, "EndDate"]
        next_start = sorted_rows.loc[idx + 1, "StartDate"]
        if next_start <= cur_end:
            warnings.append(
                f"Period {int(sorted_rows.loc[idx + 1, 'PeriodNumber'])} starts on or before "
                f"Period {int(sorted_rows.loc[idx, 'PeriodNumber'])} ends — overlap."
            )
        elif (next_start - cur_end).days > 1:
            warnings.append(
                f"Gap between Period {int(sorted_rows.loc[idx, 'PeriodNumber'])} and "
                f"Period {int(sorted_rows.loc[idx + 1, 'PeriodNumber'])}."
            )

    return errors, warnings


def _render_preview(edited: pd.DataFrame) -> None:
    if edited.empty:
        st.caption("Editor is empty.")
        return
    preview = edited.copy()
    preview["StartDate"] = pd.to_datetime(preview["StartDate"], errors="coerce").dt.strftime("%m/%d/%Y")
    preview["EndDate"] = pd.to_datetime(preview["EndDate"], errors="coerce").dt.strftime("%m/%d/%Y")
    st.dataframe(preview, hide_index=True, width="stretch")


@st.dialog("Save Fiscal Periods?")
def confirm_save_dialog() -> None:
    payload = st.session_state.get("pc_save_payload")
    if not isinstance(payload, dict):
        st.error("No pending save found.")
        st.session_state.pc_confirm_open = False
        return

    year = int(payload.get("year") or 0)
    edited = pd.DataFrame(payload.get("rows") or [])

    st.caption(f"**Year:** {year}")
    st.caption(f"**Period count:** {len(edited)}")
    _render_preview(edited)
    st.warning(
        f"Saving will replace all currently saved periods for {year}. "
        "Other years are not affected."
    )

    left, right = st.columns(2)
    with left:
        if st.button("Confirm Save", type="primary", width="stretch", key="pc_confirm_save_btn"):
            try:
                save_df = edited.copy()
                save_df["PeriodNumber"] = pd.to_numeric(save_df["PeriodNumber"], errors="coerce")
                save_df["StartDate"] = pd.to_datetime(save_df["StartDate"], errors="coerce").dt.date
                save_df["EndDate"] = pd.to_datetime(save_df["EndDate"], errors="coerce").dt.date
                path = utils.save_fiscal_periods_for_year(year, save_df)
                LOGGER.info(
                    "Saved fiscal periods | year=%s rows=%s path='%s'",
                    year,
                    len(save_df),
                    str(path),
                )
                st.session_state["pc_status"] = {
                    "level": "success",
                    "message": f"Saved {len(save_df)} period(s) for {year}.",
                }
                st.session_state.pc_confirm_open = False
                st.session_state.pc_save_payload = None
                st.session_state.pop("pc_editor_df", None)
                st.session_state.pop("pc_loaded_year", None)
                st.rerun()
            except Exception as exc:
                LOGGER.exception("Failed to save fiscal periods: %s", exc)
                st.error(f"Failed to save fiscal periods: {exc}")
    with right:
        if st.button("Cancel", width="stretch", key="pc_cancel_save_btn"):
            st.session_state.pc_confirm_open = False
            st.session_state.pc_save_payload = None
            st.rerun()


# -----------------------------------------------------------------------------
# Page body
# -----------------------------------------------------------------------------
status = st.session_state.pop("pc_status", None)
if isinstance(status, dict):
    level = str(status.get("level") or "").lower()
    message = str(status.get("message") or "")
    if message:
        if level == "success":
            st.success(message)
        elif level == "error":
            st.error(message)
        else:
            st.info(message)

if "pc_confirm_open" not in st.session_state:
    st.session_state.pc_confirm_open = False
if "pc_editor_version" not in st.session_state:
    st.session_state.pc_editor_version = 0


def _bump_editor_version() -> None:
    st.session_state.pc_editor_version = int(st.session_state.pc_editor_version) + 1

current_year = utils.to_eastern(utils.now_utc()).year
year_options = list(range(current_year - 3, current_year + 4))
default_year_index = year_options.index(current_year)

control_col, preset_col = st.columns([1, 2])
with control_col:
    selected_year = st.selectbox(
        "Year",
        options=year_options,
        index=default_year_index,
        key="pc_selected_year",
    )

with preset_col:
    p1, p2, p3, p4 = st.columns(4)
    with p1:
        if st.button("12 Monthly", width="stretch", key="pc_preset_monthly"):
            st.session_state["pc_editor_df"] = _generate_preset(int(selected_year), "12 Monthly Periods")
            st.session_state["pc_loaded_year"] = int(selected_year)
            _bump_editor_version()
            st.rerun()
    with p2:
        if st.button("13 × 4-Week", width="stretch", key="pc_preset_thirteen"):
            st.session_state["pc_editor_df"] = _generate_preset(int(selected_year), "13 Four-Week Periods")
            st.session_state["pc_loaded_year"] = int(selected_year)
            _bump_editor_version()
            st.rerun()
    with p3:
        if st.button("4 Quarters", width="stretch", key="pc_preset_quarters"):
            st.session_state["pc_editor_df"] = _generate_preset(int(selected_year), "4 Quarters")
            st.session_state["pc_loaded_year"] = int(selected_year)
            _bump_editor_version()
            st.rerun()
    with p4:
        if st.button("Clear All", width="stretch", key="pc_preset_clear", type="secondary"):
            st.session_state["pc_editor_df"] = _empty_editor_df()
            st.session_state["pc_loaded_year"] = int(selected_year)
            _bump_editor_version()
            st.rerun()

# Reload editor contents when the selected year changes (or on first load).
if st.session_state.get("pc_loaded_year") != int(selected_year):
    saved_for_year = utils.load_fiscal_periods_for_year(int(selected_year))
    st.session_state["pc_editor_df"] = _periods_to_editor_df(saved_for_year)
    st.session_state["pc_loaded_year"] = int(selected_year)
    _bump_editor_version()

editor_df = st.session_state.get("pc_editor_df")
if editor_df is None:
    editor_df = _empty_editor_df()
    st.session_state["pc_editor_df"] = editor_df

st.subheader(f"Periods for {int(selected_year)}", anchor=False)
st.caption(
    "Use the table below to define each fiscal period. Add or remove rows as needed. "
    "Period Numbers must be unique within the year."
)

edited_df = st.data_editor(
    editor_df,
    key=f"pc_editor_{int(selected_year)}_{int(st.session_state.pc_editor_version)}",
    num_rows="dynamic",
    width="stretch",
    column_config={
        "PeriodNumber": st.column_config.NumberColumn(
            "Period #",
            min_value=1,
            step=1,
            help="Sequential period number within the year.",
        ),
        "PeriodName": st.column_config.TextColumn(
            "Name",
            help="Optional human-readable label (e.g. 'P1', 'January', 'Q1').",
        ),
        "StartDate": st.column_config.DateColumn(
            "Start Date",
            format="MM/DD/YYYY",
        ),
        "EndDate": st.column_config.DateColumn(
            "End Date",
            format="MM/DD/YYYY",
        ),
    },
)

# Persist the live editor state so preset/clear flows have current data on rerun.
st.session_state["pc_editor_df"] = edited_df.copy()

errors, warnings = _validate(edited_df, int(selected_year))
if errors:
    for message in errors:
        st.error(message)
if warnings:
    for message in warnings:
        st.warning(message)

action_col_save, action_col_reload, _ = st.columns([1, 1, 6])
with action_col_save:
    save_disabled = bool(errors) or edited_df.dropna(
        subset=["PeriodNumber", "StartDate", "EndDate"]
    ).empty
    if st.button(
        "Save Configuration",
        type="primary",
        width="stretch",
        key="pc_save_btn",
        disabled=save_disabled,
    ):
        rows_payload = edited_df.dropna(subset=["PeriodNumber", "StartDate", "EndDate"]).copy()
        rows_payload["PeriodNumber"] = rows_payload["PeriodNumber"].astype(int)
        rows_payload["PeriodName"] = rows_payload["PeriodName"].fillna("").astype(str)
        rows_payload["StartDate"] = pd.to_datetime(rows_payload["StartDate"], errors="coerce").dt.date
        rows_payload["EndDate"] = pd.to_datetime(rows_payload["EndDate"], errors="coerce").dt.date
        st.session_state["pc_save_payload"] = {
            "year": int(selected_year),
            "rows": rows_payload.to_dict(orient="records"),
        }
        st.session_state.pc_confirm_open = True
        st.rerun()

with action_col_reload:
    if st.button("Reload Saved", width="stretch", key="pc_reload_btn", type="secondary"):
        utils.load_fiscal_periods.clear()
        saved_for_year = utils.load_fiscal_periods_for_year(int(selected_year))
        st.session_state["pc_editor_df"] = _periods_to_editor_df(saved_for_year)
        st.session_state["pc_loaded_year"] = int(selected_year)
        _bump_editor_version()
        st.rerun()

if st.session_state.pc_confirm_open:
    confirm_save_dialog()

st.caption(f"\n\n\nApp version: {config.APP_VERSION}", text_alignment="center")
