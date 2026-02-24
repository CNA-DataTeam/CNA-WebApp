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

# Define pages and their grouping for navigation
pages = {
    "": [
        st.Page(str(APP_DIR / "pages" / "home.py"), title="Home"),
    ],
    "Tasks": [
        st.Page(str(APP_DIR / "pages" / "task-tracker.py"), title="Tracker"),
        st.Page(str(APP_DIR / "pages" / "task-tracker-analytics.py"), title="Analytics"),
    ],
    "FedEx": [
        st.Page(str(APP_DIR / "pages" / "fedex-address-validator.py"), title="Address Validator")
    ],
    "Packaging": [
        st.Page(str(APP_DIR / "pages" / "packaging-estimator.py"), title="Estimator"),
    ],
}

# Initialize and run the navigation
navigation = st.navigation(pages)
LOGGER.info("Navigation initialized.")
navigation.run()
