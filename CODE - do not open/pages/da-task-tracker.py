"""
pages/da-task-tracker.py

Thin entry point that serves the D&A version of the Task Tracker at /da-task-tracker.
All logic lives in task-tracker.py; this file sets the version flag and delegates.
"""
import streamlit as st

st.session_state._da_page_active = True

from pathlib import Path

_src = Path(__file__).with_name("task-tracker.py")
exec(compile(_src.read_text(encoding="utf-8"), str(_src), "exec"), {**globals(), "__file__": str(_src)})
