from pathlib import Path
import sys

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import streamlit as st
import utils
import page_registry

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
is_admin_user = utils.is_current_user_admin()
LOGGER.info("Admin access check | user='%s' is_admin=%s", utils.get_os_user(), is_admin_user)

# Define pages and their grouping for navigation
home_entry = page_registry.get_home_page()
pages = {
    "": [
        st.Page(str(APP_DIR / home_entry.path), title=home_entry.title),
    ],
}
for section_name, entries in page_registry.get_visible_sections(is_admin_user):
    pages[section_name] = [
        st.Page(str(APP_DIR / entry.path), title=entry.title)
        for entry in entries
    ]

# Initialize and run the navigation
navigation = st.navigation(pages)
LOGGER.info("Navigation initialized.")
navigation.run()
