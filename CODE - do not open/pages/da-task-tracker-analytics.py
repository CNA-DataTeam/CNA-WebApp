"""
Purpose:
    Historical analytics view for completed task performance (Data & Analytics).

What it does:
    - Enforces access control.
    - Loads completed task history from D&A directories.
    - Provides filters: User, Primary Stakeholder, Department, Account, Partial, Date Range.
    - Renders KPIs and charts.
"""

import streamlit as st
import pandas as pd
import altair as alt
import config
import utils

LOGGER = utils.get_page_logger("DA Task Analytics")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Tasks Analytics")


# ============================================================
# PAGE CONFIG (SAFE AT IMPORT)
# ============================================================
st.set_page_config(
    page_title=PAGE_TITLE,
    layout="wide",
)
utils.render_app_logo()
utils.log_page_open_once("da_task_analytics_page", LOGGER)

# ============================================================
# LIGHT HELPERS (SAFE AT IMPORT)
# ============================================================
def format_duration(seconds: float) -> str:
    if pd.isna(seconds):
        return "\u2014"
    seconds = float(seconds)
    if seconds < 90:
        return f"{int(seconds)} sec"
    if seconds < 3600:
        return f"{round(seconds / 60, 1)} min"
    return f"{round(seconds / 3600, 2)} hr"


def _safe_col(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    """Return a cleaned string column, filling missing values."""
    if col not in df.columns:
        return pd.Series(default, index=df.index)
    return df[col].fillna(default).astype(str).str.strip()


# ============================================================
# SECTION 1 — FILTERS
# ============================================================
def main_filters(df: pd.DataFrame) -> pd.DataFrame:
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


# ============================================================
# SECTION 2 — KPIs + CHARTS
# ============================================================
def main_charts(filtered_df: pd.DataFrame) -> None:
    # ---------------- KPIs ----------------
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

    # ---------------- Time Series ----------------
    time_df = (
        filtered_df.groupby("Date", as_index=False)
        .size()
        .rename(columns={"size": "Tasks"})
    )

    time_chart = (
        alt.Chart(time_df)
        .mark_line(point=True)
        .encode(
            x="Date:T",
            y="Tasks:Q",
            tooltip=["Date", "Tasks"],
        )
        .properties(title="Tasks per Day")
    )

    st.altair_chart(time_chart, use_container_width=True)

    # ---------------- Breakdown Charts ----------------
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


# ============================================================
# MAIN ENTRY (ONLY PLACE HEAVY WORK STARTS)
# ============================================================
def main() -> None:
    st.markdown(utils.get_global_css(), unsafe_allow_html=True)
    LOGGER.info("Render UI.")

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

    # Ensure required columns exist with safe defaults
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

    filtered_df = main_filters(df)
    LOGGER.info("Filter result | source_rows=%s filtered_rows=%s", len(df), len(filtered_df))

    if filtered_df.empty:
        LOGGER.info("No analytics data for selected filters.")
        st.info("No data for selected filters.")
        return

    main_charts(filtered_df)


# ============================================================
# RUN
# ============================================================
main()
