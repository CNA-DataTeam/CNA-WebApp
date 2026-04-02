"""
pages/fedex-address-validation-management.py

Purpose:
    Admin-only management page for FedEx Address Validation results.
    Displays all rows and allows clearing disputed flags.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd
import streamlit as st

import config
import utils


LOGGER = utils.get_page_logger("FedEx Validator Management")
PAGE_TITLE = utils.get_registry_page_title(__file__, "FedEx Validator Management")

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon=utils.get_app_icon())
utils.render_app_logo()
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
utils.log_page_open_once("fedex_validation_management_page", LOGGER)

if not utils.is_current_user_admin():
    LOGGER.warning("Access denied for non-admin user '%s'.", utils.get_os_user())
    st.error("Access denied. This page is available to admin users only.")
    st.stop()

utils.render_page_header(PAGE_TITLE)

PARQUET_PATH = Path(config.ADDRESS_VALIDATION_RESULTS_FILE).with_suffix(".parquet")
CSV_PATH = Path(config.ADDRESS_VALIDATION_RESULTS_FILE).with_suffix(".csv")
ROW_ID_COL = "__source_row_id"


def resolve_results_source() -> tuple[Path, Literal["parquet", "csv"]]:
    """Prefer parquet; fallback to csv for compatibility."""
    if PARQUET_PATH.exists():
        return PARQUET_PATH, "parquet"
    if CSV_PATH.exists():
        return CSV_PATH, "csv"
    raise FileNotFoundError(
        f"No results file found. Checked:\n- {PARQUET_PATH}\n- {CSV_PATH}"
    )


@st.cache_data(ttl=30)
def load_results(file_path: Path, file_type: Literal["parquet", "csv"]) -> pd.DataFrame:
    if file_type == "parquet":
        return pd.read_parquet(file_path)

    last_error: Exception | None = None
    for encoding in ["utf-8-sig", "utf-8", "cp1252", "latin1"]:
        try:
            return pd.read_csv(
                file_path,
                dtype=str,
                keep_default_na=False,
                encoding=encoding,
                sep=None,
                engine="python",
                on_bad_lines="skip",
            )
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Unable to read results file: {file_path}") from last_error


def save_results(df: pd.DataFrame, file_path: Path, file_type: Literal["parquet", "csv"]) -> None:
    if file_type == "parquet":
        df.to_parquet(file_path, index=False)
    else:
        df.to_csv(file_path, index=False, encoding="utf-8-sig")


def normalize_col_name(value: object) -> str:
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def find_column_by_alias(df: pd.DataFrame, aliases: list[str]) -> str | None:
    alias_set = {normalize_col_name(alias) for alias in aliases}
    for col in df.columns:
        if normalize_col_name(col) in alias_set:
            return str(col)
    return None


def find_disputed_column(df: pd.DataFrame) -> str | None:
    aliases = [
        "Disputed",
        "Is Disputed",
        "IsDisputed",
        "IsDispute",
        "IsDisputedFlag",
        "DisputedFlag",
    ]
    return find_column_by_alias(df, aliases)


def is_disputed_value(value: object) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        return float(value) != 0.0

    text = str(value).strip().lower()
    if text in {"", "0", "0.0", "false", "f", "no", "n", "off"}:
        return False
    if text in {"1", "true", "t", "yes", "y", "on", "disputed"}:
        return True
    try:
        return float(text) != 0.0
    except Exception:
        return False


def clear_disputed_flag(df: pd.DataFrame, disputed_col: str, source_row_ids: list[int]) -> pd.DataFrame:
    updated_df = df.copy()
    if not source_row_ids:
        return updated_df

    target_indices = [idx for idx in source_row_ids if 0 <= int(idx) < len(updated_df)]
    if not target_indices:
        return updated_df

    col_idx = updated_df.columns.get_loc(disputed_col)
    col_series = updated_df[disputed_col]
    if pd.api.types.is_bool_dtype(col_series):
        updated_df.iloc[target_indices, col_idx] = False
    elif pd.api.types.is_numeric_dtype(col_series):
        updated_df.iloc[target_indices, col_idx] = 0
    else:
        updated_df.iloc[target_indices, col_idx] = ""
    return updated_df


try:
    results_path, results_type = resolve_results_source()
except Exception as exc:
    LOGGER.exception("Failed to resolve results source: %s", exc)
    st.error(str(exc))
    st.stop()

if results_type == "csv":
    st.warning(
        "results.parquet was not found. This page is currently managing results.csv instead.",
        icon="\u26A0\uFE0F",
    )

try:
    full_df = load_results(results_path, results_type)
except Exception as exc:
    LOGGER.exception("Failed to load results file '%s': %s", results_path, exc)
    st.error(f"Failed to load results file:\n{results_path}\n\n{exc}")
    st.stop()

if full_df.empty:
    st.info("Results file is empty.")
    st.stop()

disputed_col = find_disputed_column(full_df)
if not disputed_col:
    st.error("No disputed flag column found in the results data.")
    st.stop()

work_df = full_df.copy().reset_index(drop=False).rename(columns={"index": ROW_ID_COL})
work_df["__disputed_state"] = work_df[disputed_col].map(
    lambda value: "Disputed" if is_disputed_value(value) else "Not Disputed"
)

classification_col = find_column_by_alias(work_df, ["Classification"])
service_type_col = find_column_by_alias(work_df, ["Service Type", "ServiceType", "Service"])
residential_match_col = find_column_by_alias(
    work_df,
    ["Residential Match", "ResidentialStatusMatch", "Match Type", "MatchType"],
)

f1, f2, f3, f4 = st.columns(4)
with f1:
    disputed_filter = st.selectbox(
        "Is Disputed",
        options=["All", "Disputed", "Not Disputed"],
        index=0,
    )
with f2:
    class_options = ["All"]
    if classification_col:
        class_values = work_df[classification_col].fillna("").astype(str).str.strip()
        class_options += sorted(v for v in class_values.unique().tolist() if v)
    selected_class = st.selectbox(
        "Classification",
        options=class_options,
        index=0,
        disabled=classification_col is None,
    )
with f3:
    service_options = ["All"]
    if service_type_col:
        service_values = work_df[service_type_col].fillna("").astype(str).str.strip()
        service_options += sorted(v for v in service_values.unique().tolist() if v)
    selected_service = st.selectbox(
        "Service Type",
        options=service_options,
        index=0,
        disabled=service_type_col is None,
    )
with f4:
    res_match_options = ["All"]
    if residential_match_col:
        res_match_values = work_df[residential_match_col].fillna("").astype(str).str.strip()
        res_match_options += sorted(v for v in res_match_values.unique().tolist() if v)
    selected_res_match = st.selectbox(
        "Residential Match",
        options=res_match_options,
        index=0,
        disabled=residential_match_col is None,
    )

filtered_work_df = work_df.copy()
if disputed_filter != "All":
    filtered_work_df = filtered_work_df[filtered_work_df["__disputed_state"] == disputed_filter]
if classification_col and selected_class != "All":
    class_series = filtered_work_df[classification_col].fillna("").astype(str).str.strip()
    filtered_work_df = filtered_work_df[class_series == selected_class]
if service_type_col and selected_service != "All":
    service_series = filtered_work_df[service_type_col].fillna("").astype(str).str.strip()
    filtered_work_df = filtered_work_df[service_series == selected_service]
if residential_match_col and selected_res_match != "All":
    res_series = filtered_work_df[residential_match_col].fillna("").astype(str).str.strip()
    filtered_work_df = filtered_work_df[res_series == selected_res_match]

display_df = filtered_work_df.drop(columns=[ROW_ID_COL, "__disputed_state"], errors="ignore")

selected_rows: list[int] = []
try:
    event = st.dataframe(
        display_df,
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="multi-row",
    )
    if event is not None and hasattr(event, "selection"):
        selected_rows = list(getattr(event.selection, "rows", []) or [])
except TypeError:
    st.dataframe(display_df, hide_index=True, width="stretch")
    row_options = list(range(len(display_df)))
    selected_rows = st.multiselect(
        "Select rows",
        options=row_options,
        format_func=lambda idx: f"{idx + 1}",
    )

selected_source_ids: list[int] = []
if selected_rows:
    safe_positions = [int(idx) for idx in selected_rows if 0 <= int(idx) < len(filtered_work_df)]
    selected_source_ids = filtered_work_df.iloc[safe_positions][ROW_ID_COL].astype(int).tolist()

st.caption(f"Rows: {len(display_df):,} | Disputed column: {disputed_col}")

btn_col, info_col = st.columns([1.2, 4])
with btn_col:
    clear_clicked = st.button(
        "Clear Is Disputed (selected)",
        type="primary",
        width="stretch",
        disabled=len(selected_source_ids) == 0,
    )
with info_col:
    if len(selected_source_ids) == 0:
        st.caption("Select one or more rows to clear the disputed flag.")
    else:
        st.caption(f"Selected rows: {len(selected_source_ids):,}")

if clear_clicked:
    try:
        updated_df = clear_disputed_flag(full_df, disputed_col, selected_source_ids)
        save_results(updated_df, results_path, results_type)
        load_results.clear()
        LOGGER.info(
            "Cleared disputed flag | file='%s' rows=%s",
            results_path,
            len(selected_source_ids),
        )
        st.success(f"Cleared disputed flag on {len(selected_source_ids):,} row(s).")
        st.rerun()
    except Exception as exc:
        LOGGER.exception("Failed clearing disputed flag: %s", exc)
        st.error(f"Failed to update results file: {exc}")
