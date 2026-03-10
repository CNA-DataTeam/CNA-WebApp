"""
home.py

Purpose:
    Landing page for the Logistics Support Streamlit suite.
    Styled to match the app and provides navigation cards.
"""

import streamlit as st

import config
import page_registry
import utils


LOGGER = utils.get_page_logger("Home")
IS_ADMIN_USER = utils.is_current_user_admin()
PAGE_TITLE = utils.get_registry_page_title(__file__, "Home")

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title=PAGE_TITLE,
    layout="wide",
)
utils.render_app_logo()
utils.log_page_open_once("home_page", LOGGER)
if "_home_render_logged" not in st.session_state:
    st.session_state._home_render_logged = True
    LOGGER.info("Render navigation cards.")
if "_home_sections_logged" not in st.session_state:
    st.session_state._home_sections_logged = True
    section_parts: list[str] = []
    for section_name, entries in page_registry.get_visible_sections(IS_ADMIN_USER):
        key = "".join(ch if ch.isalnum() else "_" for ch in section_name.strip().lower()).strip("_")
        section_parts.append(f"{key}={len(entries)}")
    LOGGER.info("Sections available | %s", " ".join(section_parts))

# ============================================================
# GLOBAL STYLING
# ============================================================
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

# ============================================================
# HEADER
# ============================================================
utils.render_page_header(PAGE_TITLE)

# ============================================================
# APPLICATION CARDS (REAL NAVIGATION)
# ============================================================
def render_nav_card(column, page_path: str, label: str, icon: str, caption: str) -> None:
    with column:
        _spacer_col, content_col = st.columns([0.08, 0.92], vertical_alignment="top")
        with content_col:
            st.space(size="small")
            st.page_link(
                page_path,
                label=label,
                icon=icon,
            )
            st.caption(caption)


visible_sections = page_registry.get_visible_sections(IS_ADMIN_USER)
for section_idx, (section_name, entries) in enumerate(visible_sections):
    st.subheader(section_name, anchor=False)
    for row_start in range(0, len(entries), 2):
        col1, col2 = st.columns(2)
        left_entry = entries[row_start]
        render_nav_card(
            col1,
            left_entry.path,
            f"**{left_entry.title}**",
            left_entry.icon,
            left_entry.caption,
        )

        right_idx = row_start + 1
        if right_idx < len(entries):
            right_entry = entries[right_idx]
            render_nav_card(
                col2,
                right_entry.path,
                f"**{right_entry.title}**",
                right_entry.icon,
                right_entry.caption,
            )
        else:
            with col2:
                st.empty()

    if section_idx < len(visible_sections) - 1:
        st.divider()

st.divider()
st.caption(
    "Use the sidebar to switch between applications at any time.",
    text_alignment="center",
)
