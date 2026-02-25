"""
home.py

Purpose:
    Landing page for the Logistics Support Streamlit suite.
    Styled to match Task Tracker and provides working navigation.
"""

import streamlit as st
from pathlib import Path
import base64

import config
import utils

LOGGER = utils.get_page_logger("Home")
IS_ADMIN_USER = utils.is_current_user_admin()

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="CNA Web App",
    layout="wide",
)
utils.log_page_open_once("home_page", LOGGER)
if "_home_render_logged" not in st.session_state:
    st.session_state._home_render_logged = True
    LOGGER.info("Render navigation cards.")
if "_home_sections_logged" not in st.session_state:
    st.session_state._home_sections_logged = True
    LOGGER.info(
        "Sections available | tasks=%s fedex=1 packaging=1 admin=%s",
        3 if IS_ADMIN_USER else 2,
        IS_ADMIN_USER,
    )

# ============================================================
# GLOBAL STYLING (MATCH TASK TRACKER — SAFE FOR SIDEBAR)
# ============================================================

st.markdown(utils.get_global_css(), unsafe_allow_html=True)

# ============================================================
# HEADER
# ============================================================

LOGO_PATH           = config.LOGO_PATH
logo_b64 = utils.get_logo_base64(str(LOGO_PATH))

st.markdown(
    f"""
    <div class="header-row">
        <img class="header-logo" src="data:image/png;base64,{logo_b64}" />
        <h1 class="header-title">Logistics Support App</h1>
    </div>
    """,
    unsafe_allow_html=True,
)

st.divider()

# ============================================================
# APPLICATION CARDS (REAL NAVIGATION)
# ============================================================
st.subheader("Tasks", anchor=False)

if IS_ADMIN_USER:
    spacer_l, col1, space_m, col2, space_n, col3, spacer_r = st.columns([0.3, 1.8, 0.3, 1.8, 0.3, 1.8, 0.3])
else:
    spacer_l, col1, space_m, col2, spacer_r = st.columns([0.4, 2, 0.4, 2, 0.4])
    col3 = None

with col1:
    st.page_link(
        "pages/task-tracker.py",
        label="**Tracker**",
        icon="🕒",
    )

    st.caption(
        "Log daily operational tasks, track elapsed time, manage task cadence, "
        "and view live activity from other Logistics Support team members in real time."
    )

with col2:
    st.page_link(
        "pages/task-tracker-analytics.py",
        label="**Analytics**",
        icon="📊",
    )

    st.caption(
        "Upcoming logistics and analytics tools designed to support reporting, "
        "automation, and operational visibility."
    )

if col3 is not None:
    with col3:
        st.page_link(
            "pages/tasks-management.py",
            label="**Management**",
            icon="🛠️",
        )
        st.caption(
            "Admin page to view and update tasks metadata, including task name, cadence, "
            "and active status."
        )

st.divider()

st.subheader("FedEx", anchor=False)

spacer_l, col1, space_m, col2, spacer_r = st.columns([0.4, 2, 0.4, 2, 0.4])

with col1:
    st.page_link(
        "pages/fedex-address-validator.py",
        label="**Address Validator**",
        icon="✅",
    )

    st.caption(
        "Log daily operational tasks, track elapsed time, manage task cadence, "
        "and view live activity from other Logistics Support team members in real time."
    )

st.divider()

st.subheader("Packaging", anchor=False)

spacer_l, col1, space_m, col2, spacer_r = st.columns([0.4, 2, 0.4, 2, 0.4])

with col1:
    st.page_link(
        "pages/packaging-estimator.py",
        label="**Packaging Estimator**",
        icon="📦",
    )

    st.caption(
        "Estimate package counts and grouped dimensions from uploaded or pasted item lists "
        "using SSAS verification, API packaging, and rule-based fallback logic."
    )

st.divider()

st.caption(
    "Use the sidebar to switch between applications at any time.",
    text_alignment="center",
)
