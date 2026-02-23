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

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Logistics Support App",
    layout="wide",
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

spacer_l, col1, space_m, col2, spacer_r = st.columns([0.4, 2, 0.4, 2, 0.4])

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
