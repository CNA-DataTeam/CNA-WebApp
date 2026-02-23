import streamlit as st
import config
import utils

LOGGER = utils.get_page_logger("Navigation")
utils.log_page_open_once("navigation", LOGGER)

# Define pages and their grouping for navigation
pages = {
    "": [
        st.Page("pages/home.py", title="Home"),
    ],
    "Tasks": [
        st.Page("pages/task-tracker.py", title="Tracker"),
        st.Page("pages/task-tracker-analytics.py", title="Analytics"),
    ],
    "FedEx": [
        st.Page("pages/fedex-address-validator.py", title="Address Validator")
    ],
    "Packaging": [
        st.Page("pages/packaging-estimator.py", title="Estimator"),
    ],
}
if "_navigation_structure_logged" not in st.session_state:
    st.session_state._navigation_structure_logged = True
    LOGGER.info("Navigation configured with sections: %s", ", ".join(pages.keys()))

# Initialize and run the navigation
navigation = st.navigation(pages)
LOGGER.info("Rendering navigation container.")
navigation.run()
