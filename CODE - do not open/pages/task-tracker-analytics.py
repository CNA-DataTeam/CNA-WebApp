"""
Purpose:
    Historical analytics view for completed task performance.

What it does:
    - Enforces access control.
    - Loads ALL completed tasks from partitioned parquet.
    - Provides filters.
    - Renders KPIs and charts.
    - Shows user vs team performance when applicable.
"""

import streamlit as st
import pandas as pd
import altair as alt
import config
import utils

LOGGER = utils.get_page_logger("Task Analytics")


# ============================================================
# PAGE CONFIG (SAFE AT IMPORT)
# ============================================================
st.set_page_config(
    page_title="Task Tracker - Analytics",
    layout="wide",
)
utils.log_page_open_once("task_analytics_page", LOGGER)

# ============================================================
# LIGHT HELPERS (SAFE AT IMPORT)
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


@st.cache_data(ttl=300)
def load_targets_placeholder() -> pd.DataFrame:
    return pd.DataFrame({"TaskName": [], "TargetSeconds": []})


# ============================================================
# SECTION 1 — FILTERS
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

    if user_filter != "All":
        filtered_df = filtered_df[filtered_df["FullName"] == user_filter]

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

    return filtered_df, user_filter


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
        cad_df = (
            filtered_df.groupby("TaskCadence", as_index=False)["DurationSeconds"]
            .sum()
        )
        cad_df["Hours"] = (cad_df["DurationSeconds"] / 3600).round(2)

        cad_chart = (
            alt.Chart(cad_df)
            .mark_bar()
            .encode(
                x="TaskCadence:N",
                y="Hours:Q",
                tooltip=["TaskCadence", "Hours"],
            )
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


# ============================================================
# SECTION 3 — PERFORMANCE REVIEW
# ============================================================
def main_performance_review(filtered_df: pd.DataFrame, user_filter: str) -> None:
    if user_filter == "All":
        return

    st.divider()
    st.subheader("Performance Review", anchor=False)

    user_df = filtered_df[filtered_df["FullName"] == user_filter]
    team_df = filtered_df[filtered_df["FullName"] != user_filter]

    user_counts = (
        user_df.groupby("TaskName")
        .size()
        .reset_index(name="Completed")
    )

    team_avg = (
        team_df.groupby(["TaskName", "FullName"])
        .size()
        .groupby("TaskName")
        .mean()
        .reset_index(name="Team Average")
    )

    comp_df = (
        user_counts
        .merge(team_avg, on="TaskName", how="left")
        .assign(Target="TBD")
        .sort_values("Completed", ascending=False)
        .rename(columns={"TaskName": "Task"})
    )

    st.dataframe(comp_df, hide_index=True, width="stretch")


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

    df = utils.load_all_completed_tasks(config.COMPLETED_TASKS_DIR)
    if df.empty:
        LOGGER.info("No completed task data available for analytics.")
        st.warning("No completed task data available.")
        return
    LOGGER.info("Loaded completed task history | rows=%s", len(df))

    if "PartiallyComplete" not in df.columns:
        df["PartiallyComplete"] = False
    else:
        df["PartiallyComplete"] = df["PartiallyComplete"].fillna(False).astype(bool)

    logo_b64 = utils.get_logo_base64(str(config.LOGO_PATH))
    st.markdown(
        f"""
        <div class="header-row">
            <img class="header-logo" src="data:image/png;base64,{logo_b64}" />
            <h1 class="header-title">LS - Tasks Analytics</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.divider()

    filtered_df, user_filter = main_filters(df)
    LOGGER.info("Filter result | source_rows=%s filtered_rows=%s", len(df), len(filtered_df))

    if filtered_df.empty:
        LOGGER.info("No analytics data for selected filters.")
        st.info("No data for selected filters.")
        return

    main_charts(filtered_df)
    main_performance_review(filtered_df, user_filter)


# ============================================================
# RUN
# ============================================================
main()
