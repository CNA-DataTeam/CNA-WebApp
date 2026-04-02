"""
Purpose:
    Historical analytics view for completed task performance.
    Supports both Logistics Support and Data & Analytics versions via a toggle.
"""

import streamlit as st
import pandas as pd
import altair as alt
import config
import utils

LOGGER = utils.get_page_logger("Task Analytics")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Task Analytics")


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide")
utils.render_app_logo()
utils.log_page_open_once("task_analytics_page", LOGGER)
utils.log_page_open_once("da_task_analytics_page", LOGGER)


# ============================================================
# SHARED HELPERS
# ============================================================
def format_duration(seconds: float) -> str:
    if pd.isna(seconds):
        return "—"
    seconds = float(seconds)
    if seconds < 90:
        return f"{int(seconds)} sec"
    if seconds < 3600:
        return f"{round(seconds / 60, 1)} min"
    return f"{round(seconds / 3600, 2)} hr"


def _safe_col(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    if col not in df.columns:
        return pd.Series(default, index=df.index)
    return df[col].fillna(default).astype(str).str.strip()


# ============================================================
# LOGISTICS SUPPORT — FILTERS + CHARTS
# ============================================================
def main_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("Filters", anchor=False)

    c1, c2, c3, c4, c5 = st.columns([1.5, 1.8, 1.6, 1.2, 1.9])

    with c1:
        user_filter = st.selectbox(
            "User",
            options=["All"] + sorted(df["FullName"].dropna().unique().tolist()),
        )

    with c2:
        task_filter = st.multiselect(
            "Task",
            options=sorted(df["TaskName"].unique().tolist()),
            default=[],
        )

    with c3:
        cadence_filter = st.multiselect(
            "Cadence",
            options=sorted(df["TaskCadence"].dropna().unique().tolist()),
            default=[],
        )

    with c4:
        exclude_partial = (
            st.selectbox(
                "Partially complete?",
                options=["Exclude", "Include"],
                index=0,
            )
            == "Exclude"
        )

    with c5:
        date_range = st.date_input(
            "Date Range",
            value=(df["Date"].min(), df["Date"].max()),
        )

    filtered_df = df.copy()

    if task_filter:
        filtered_df = filtered_df[filtered_df["TaskName"].isin(task_filter)]

    if cadence_filter:
        filtered_df = filtered_df[filtered_df["TaskCadence"].isin(cadence_filter)]

    if exclude_partial:
        filtered_df = filtered_df[~filtered_df["PartiallyComplete"]]

    if isinstance(date_range, tuple):
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    filtered_df = filtered_df[
        (filtered_df["Date"] >= start_date) & (filtered_df["Date"] <= end_date)
    ]
    if user_filter != "All":
        filtered_df = filtered_df[filtered_df["FullName"] == user_filter]

    filter_signature = (
        user_filter,
        tuple(sorted(task_filter)),
        tuple(sorted(cadence_filter)),
        exclude_partial,
        str(start_date),
        str(end_date),
    )
    if st.session_state.get("_analytics_filter_signature") != filter_signature:
        st.session_state._analytics_filter_signature = filter_signature
        LOGGER.info(
            "Filters updated | user='%s' tasks=%s cadences=%s exclude_partial=%s date_range=%s..%s",
            user_filter,
            len(task_filter),
            len(cadence_filter),
            exclude_partial,
            start_date,
            end_date,
        )

    return filtered_df


def main_charts(filtered_df: pd.DataFrame) -> None:
    total_tasks = len(filtered_df)
    total_time = filtered_df["DurationSeconds"].sum()
    avg_time = filtered_df["DurationSeconds"].mean()

    k1, k2, k3 = st.columns(3)
    with k1:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{total_tasks}</div><div class="kpi-label">Tasks</div></div>',
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{format_duration(total_time)}</div><div class="kpi-label">Total Time</div></div>',
            unsafe_allow_html=True,
        )
    with k3:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{format_duration(avg_time)}</div><div class="kpi-label">Avg Time / Task</div></div>',
            unsafe_allow_html=True,
        )

    st.divider()

    time_df = (
        filtered_df.groupby("Date", as_index=False)
        .size()
        .rename(columns={"size": "Tasks"})
    )
    time_chart = (
        alt.Chart(time_df)
        .mark_line(point=True)
        .encode(x="Date:T", y="Tasks:Q", tooltip=["Date", "Tasks"])
        .properties(title="Tasks per Day")
    )
    st.altair_chart(time_chart, use_container_width=True)

    left, right = st.columns(2)
    with left:
        cad_df = (
            filtered_df.groupby("TaskCadence", as_index=False)["DurationSeconds"].sum()
        )
        cad_df["Hours"] = (cad_df["DurationSeconds"] / 3600).round(2)
        cad_chart = (
            alt.Chart(cad_df)
            .mark_bar()
            .encode(x="TaskCadence:N", y="Hours:Q", tooltip=["TaskCadence", "Hours"])
            .properties(title="Total Hours by Cadence")
        )
        st.altair_chart(cad_chart, use_container_width=True)

    with right:
        task_df = (
            filtered_df.groupby("TaskName", as_index=False)["DurationSeconds"]
            .sum()
            .nlargest(10, "DurationSeconds")
        )
        task_df["Hours"] = (task_df["DurationSeconds"] / 3600).round(2)
        task_chart = (
            alt.Chart(task_df)
            .mark_bar()
            .encode(
                x=alt.X("TaskName:N", sort="-y", title="Task"),
                y="Hours:Q",
                tooltip=["TaskName", "Hours"],
            )
            .properties(title="Top 10 Tasks by Total Hours")
        )
        st.altair_chart(task_chart, use_container_width=True)


def main() -> None:
    st.markdown(utils.get_global_css(), unsafe_allow_html=True)
    LOGGER.info("Render UI (Logistics Support).")

    user_ctx = utils.get_user_context()
    if not user_ctx.can_view_analytics:
        LOGGER.warning("Unauthorized analytics page access for user '%s'.", user_ctx.user_login)
        st.error("You are not authorized to view this page.")
        return

    utils.render_page_header(PAGE_TITLE)

    df = utils.load_completed_tasks_for_analytics(config.COMPLETED_TASKS_DIR)
    if df.empty:
        LOGGER.info("No completed task data available for analytics.")
        st.warning("No completed task data available.")
        return
    LOGGER.info("Loaded completed task history | rows=%s", len(df))

    if "PartiallyComplete" not in df.columns:
        df["PartiallyComplete"] = False
    else:
        df["PartiallyComplete"] = df["PartiallyComplete"].fillna(False).astype(bool)

    filtered_df = main_filters(df)
    LOGGER.info("Filter result | source_rows=%s filtered_rows=%s", len(df), len(filtered_df))

    if filtered_df.empty:
        LOGGER.info("No analytics data for selected filters.")
        st.info("No data for selected filters.")
        return

    main_charts(filtered_df)


# ============================================================
# DATA & ANALYTICS — FILTERS + CHARTS
# ============================================================
def da_main_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("Filters", anchor=False)

    c1, c2, c3, c4, c5, c6 = st.columns([1.5, 1.6, 1.6, 1.6, 1.2, 1.9])

    with c1:
        user_filter = st.selectbox(
            "User",
            options=["All"] + sorted(df["FullName"].dropna().unique().tolist()),
            key="da_analytics_user_filter",
        )

    with c2:
        stakeholder_opts = sorted(
            df["PrimaryStakeholder"].loc[df["PrimaryStakeholder"] != ""].unique().tolist()
        )
        stakeholder_filter = st.multiselect(
            "Primary Stakeholder",
            options=stakeholder_opts,
            default=[],
            key="da_analytics_stakeholder_filter",
        )

    with c3:
        dept_opts = sorted(
            df["Department"].loc[df["Department"] != ""].unique().tolist()
        )
        department_filter = st.multiselect(
            "Department",
            options=dept_opts,
            default=[],
            key="da_analytics_department_filter",
        )

    with c4:
        account_opts = sorted(
            df["Account"].loc[df["Account"] != ""].unique().tolist()
        )
        account_filter = st.multiselect(
            "Account",
            options=account_opts,
            default=[],
            key="da_analytics_account_filter",
        )

    with c5:
        exclude_partial = (
            st.selectbox(
                "Partially complete?",
                options=["Exclude", "Include"],
                index=0,
                key="da_analytics_partial_filter",
            )
            == "Exclude"
        )

    with c6:
        date_range = st.date_input(
            "Date Range",
            value=(df["Date"].min(), df["Date"].max()),
            key="da_analytics_date_range",
        )

    filtered_df = df.copy()

    if user_filter != "All":
        filtered_df = filtered_df[filtered_df["FullName"] == user_filter]

    if stakeholder_filter:
        filtered_df = filtered_df[filtered_df["PrimaryStakeholder"].isin(stakeholder_filter)]

    if department_filter:
        filtered_df = filtered_df[filtered_df["Department"].isin(department_filter)]

    if account_filter:
        filtered_df = filtered_df[filtered_df["Account"].isin(account_filter)]

    if exclude_partial:
        filtered_df = filtered_df[~filtered_df["PartiallyComplete"]]

    if isinstance(date_range, tuple):
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    filtered_df = filtered_df[
        (filtered_df["Date"] >= start_date) & (filtered_df["Date"] <= end_date)
    ]

    filter_signature = (
        user_filter,
        tuple(sorted(stakeholder_filter)),
        tuple(sorted(department_filter)),
        tuple(sorted(account_filter)),
        exclude_partial,
        str(start_date),
        str(end_date),
    )
    if st.session_state.get("da__analytics_filter_signature") != filter_signature:
        st.session_state.da__analytics_filter_signature = filter_signature
        LOGGER.info(
            "Filters updated | user='%s' stakeholders=%s depts=%s accounts=%s exclude_partial=%s date_range=%s..%s",
            user_filter,
            len(stakeholder_filter),
            len(department_filter),
            len(account_filter),
            exclude_partial,
            start_date,
            end_date,
        )

    return filtered_df


def da_main_charts(filtered_df: pd.DataFrame) -> None:
    total_tasks = len(filtered_df)
    total_time = filtered_df["DurationSeconds"].sum()
    avg_time = filtered_df["DurationSeconds"].mean()

    k1, k2, k3 = st.columns(3)
    with k1:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{total_tasks}</div><div class="kpi-label">Tasks</div></div>',
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{format_duration(total_time)}</div><div class="kpi-label">Total Time</div></div>',
            unsafe_allow_html=True,
        )
    with k3:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{format_duration(avg_time)}</div><div class="kpi-label">Avg Time / Task</div></div>',
            unsafe_allow_html=True,
        )

    st.divider()

    time_df = (
        filtered_df.groupby("Date", as_index=False)
        .size()
        .rename(columns={"size": "Tasks"})
    )
    time_chart = (
        alt.Chart(time_df)
        .mark_line(point=True)
        .encode(x="Date:T", y="Tasks:Q", tooltip=["Date", "Tasks"])
        .properties(title="Tasks per Day")
    )
    st.altair_chart(time_chart, use_container_width=True)

    left, right = st.columns(2)
    with left:
        dept_df = (
            filtered_df.loc[filtered_df["Department"] != ""]
            .groupby("Department", as_index=False)["DurationSeconds"]
            .sum()
        )
        dept_df["Hours"] = (dept_df["DurationSeconds"] / 3600).round(2)
        dept_chart = (
            alt.Chart(dept_df)
            .mark_bar()
            .encode(
                x=alt.X("Department:N", sort="-y"),
                y="Hours:Q",
                tooltip=["Department", "Hours"],
            )
            .properties(title="Total Hours by Department")
        )
        st.altair_chart(dept_chart, use_container_width=True)

    with right:
        task_df = (
            filtered_df.groupby("TaskName", as_index=False)["DurationSeconds"]
            .sum()
            .nlargest(10, "DurationSeconds")
        )
        task_df["Hours"] = (task_df["DurationSeconds"] / 3600).round(2)
        task_chart = (
            alt.Chart(task_df)
            .mark_bar()
            .encode(
                x=alt.X("TaskName:N", sort="-y", title="Task"),
                y="Hours:Q",
                tooltip=["TaskName", "Hours"],
            )
            .properties(title="Top 10 Tasks by Total Hours")
        )
        st.altair_chart(task_chart, use_container_width=True)


def da_main() -> None:
    st.markdown(utils.get_global_css(), unsafe_allow_html=True)
    LOGGER.info("Render UI (Data & Analytics).")

    user_ctx = utils.get_user_context()
    if not user_ctx.can_view_analytics:
        LOGGER.warning("Unauthorized analytics page access for user '%s'.", user_ctx.user_login)
        st.error("You are not authorized to view this page.")
        return

    utils.render_page_header(PAGE_TITLE)

    df = utils.load_all_completed_tasks(config.DA_COMPLETED_TASKS_DIR)
    if df.empty:
        LOGGER.info("No completed task data available for analytics.")
        st.warning("No completed task data available.")
        return
    LOGGER.info("Loaded completed task history | rows=%s", len(df))

    if "PartiallyComplete" not in df.columns:
        df["PartiallyComplete"] = False
    else:
        df["PartiallyComplete"] = df["PartiallyComplete"].fillna(False).astype(bool)

    df["DurationSeconds"] = pd.to_numeric(df.get("DurationSeconds", 0), errors="coerce").fillna(0)
    df["FullName"] = _safe_col(df, "FullName")
    df["TaskName"] = _safe_col(df, "TaskName")
    df["PrimaryStakeholder"] = _safe_col(df, "CoveringFor")
    df["Department"] = _safe_col(df, "Department")
    df["Account"] = _safe_col(df, "CompanyGroup")

    if "Date" not in df.columns:
        df["StartTimestampUTC"] = pd.to_datetime(df["StartTimestampUTC"], utc=True)
        df["Date"] = df["StartTimestampUTC"].dt.date

    filtered_df = da_main_filters(df)
    LOGGER.info("Filter result | source_rows=%s filtered_rows=%s", len(df), len(filtered_df))

    if filtered_df.empty:
        LOGGER.info("No analytics data for selected filters.")
        st.info("No data for selected filters.")
        return

    da_main_charts(filtered_df)


# ============================================================
# VERSION TOGGLE + RUN
# ============================================================
if "analytics_version" not in st.session_state:
    st.session_state.analytics_version = "logistics"

_v_ls, _v_da, _ = st.columns([1.3, 1.3, 6])
with _v_ls:
    if st.button(
        "Logistics Support",
        use_container_width=True,
        type="primary" if st.session_state.analytics_version == "logistics" else "secondary",
        key="analytics_v_ls",
    ):
        st.session_state.analytics_version = "logistics"
        st.rerun()
with _v_da:
    if st.button(
        "Data & Analytics",
        use_container_width=True,
        type="primary" if st.session_state.analytics_version == "da" else "secondary",
        key="analytics_v_da",
    ):
        st.session_state.analytics_version = "da"
        st.rerun()

if st.session_state.analytics_version == "logistics":
    main()
else:
    da_main()
