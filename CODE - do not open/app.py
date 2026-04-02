from pathlib import Path
import sys

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import streamlit as st

import page_registry
import utils

# Preload third-party component once from the main script context.
_AUTOREFRESH_PRELOAD_ERROR: Exception | None = None
try:
    import streamlit_autorefresh  # noqa: F401
except Exception as exc:
    _AUTOREFRESH_PRELOAD_ERROR = exc

LOGGER = utils.get_program_logger(__file__, "App")
LOGGER.info("App bootstrap started.")
if _AUTOREFRESH_PRELOAD_ERROR is not None:
    LOGGER.warning("Auto-refresh preload failed: %s", _AUTOREFRESH_PRELOAD_ERROR)

st.set_page_config(initial_sidebar_state="expanded")
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
utils.render_app_logo()

is_admin_user = utils.is_current_user_admin()
LOGGER.info("Admin access check | user='%s' is_admin=%s", utils.get_os_user(), is_admin_user)

home_entry = page_registry.get_home_page()
visible_sections = page_registry.get_visible_sections(is_admin_user)
home_page = st.Page(home_entry.path, title=home_entry.title, icon=home_entry.icon)
pages = {"": [home_page]}
sidebar_sections: list[tuple[str, list[st.Page]]] = []
for section_name, entries in visible_sections:
    section_pages = [
        st.Page(entry.path, title=entry.title, icon=entry.icon)
        for entry in entries
    ]
    pages[section_name] = section_pages
    sidebar_sections.append((section_name, section_pages))

navigation = st.navigation(pages, position="hidden")
with st.sidebar:
    st.page_link(home_page, use_container_width=True)
    for section_name, section_pages in sidebar_sections:
        section_active = any(p.title == navigation.title for p in section_pages)
        with st.expander(section_name, expanded=section_active):
            for page_obj in section_pages:
                st.page_link(page_obj, use_container_width=True)

LOGGER.info("Navigation initialized | current_page='%s'", navigation.title)
navigation.run()
