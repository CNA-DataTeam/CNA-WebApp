import streamlit as st
import config
import utils

LOGGER = utils.get_program_logger("app", "App")
LOGGER.info("App bootstrap started.")

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

# Initialize and run the navigation
navigation = st.navigation(pages)
LOGGER.info("Navigation initialized.")
navigation.run()
