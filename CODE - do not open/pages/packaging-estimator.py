"""
pages/packaging-estimator.py

Purpose:
    Streamlit page that accepts item input and returns all columns from the
    configured item-info parquet file for matching item numbers.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from io import BytesIO
import json
from pathlib import Path
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st

import config
import utils

# ============================================================
# PAGE CONFIG / HEADER
# ============================================================
PAGE_TITLE = utils.get_registry_page_title(__file__, "Packaging Estimator")
st.set_page_config(page_title=PAGE_TITLE, layout="wide")
utils.render_app_logo()
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
utils.render_page_header(PAGE_TITLE)

# ============================================================
# CONSTANTS
# ============================================================
INPUT_MODE_UPLOAD = "Upload File"
INPUT_MODE_PASTE = "Paste from Excel (tab-separated)"
DESTINATION_MODE_WAREHOUSE = "One of Our Warehouses"
DESTINATION_MODE_ADDRESS = "Specific Address"
US_STATE_CODES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]
PERISHABLE_TYPE_OPTIONS: list[tuple[str, str]] = [
    ("Alive", "A"),
    ("Deep Frozen", "Z"),
    ("Deep Refrigerated", "G"),
    ("Defrosted", "D"),
    ("Frozen", "F"),
    ("KeepCool", "K"),
    ("None", "N"),
    ("Refrigerated", "R"),
]
PERISHABLE_CODE_TO_LABEL = {code: label for label, code in PERISHABLE_TYPE_OPTIONS}
PERISHABLE_CODE_OPTIONS = [code for _, code in PERISHABLE_TYPE_OPTIONS]
RECOMMENDATION_EXECUTOR = ThreadPoolExecutor(max_workers=1)


# ============================================================
# CONFIG / LOGGING
# ============================================================
def load_packaging_config() -> dict[str, Any]:
    """Load packaging config from config.py with safe defaults."""
    runtime_cfg = config.PACKAGING_CONFIG if isinstance(config.PACKAGING_CONFIG, dict) else {}
    item_info_cfg = runtime_cfg.get("item_info", {}) if isinstance(runtime_cfg.get("item_info"), dict) else {}
    warehouse_cfg = runtime_cfg.get("warehouses", {}) if isinstance(runtime_cfg.get("warehouses"), dict) else {}
    shipping_calc_cfg = (
        runtime_cfg.get("shipping_calculator_api", {})
        if isinstance(runtime_cfg.get("shipping_calculator_api"), dict)
        else {}
    )
    ui_cfg = runtime_cfg.get("ui", {}) if isinstance(runtime_cfg.get("ui"), dict) else {}

    parquet_path = str(
        item_info_cfg.get(
            "parquet_path",
            r"\\therestaurantstore.com\920\Data\Logistics\Logistics App\Item Info\item_info.parquet",
        )
    ).strip()
    item_number_column = str(item_info_cfg.get("item_number_column", "ItemNumber")).strip() or "ItemNumber"
    warehouse_parquet_path = str(warehouse_cfg.get("parquet_path", "")).strip()
    warehouse_number_column = str(warehouse_cfg.get("warehouse_number_column", "LocationName")).strip() or "LocationName"
    shipping_calc_endpoint = str(
        shipping_calc_cfg.get(
            "endpoint",
            "https://shippingcalculator-api.dev.clarkinc.biz/calculate/calculateSF",
        )
    ).strip()
    shipping_calc_timeout_seconds = int(shipping_calc_cfg.get("timeout_seconds", 30) or 30)
    ids_public_company_code = (
        str(shipping_calc_cfg.get("ids_public_company_code", "WebstaurantStore")).strip()
        or "WebstaurantStore"
    )
    shipping_calc_user_id = int(shipping_calc_cfg.get("user_id", 130326) or 130326)
    shipping_calc_company_type = str(shipping_calc_cfg.get("company_type", "Warehouse")).strip() or "Warehouse"
    default_warehouse = int(ui_cfg.get("default_warehouse", 105) or 105)
    default_marginal_length = float(ui_cfg.get("default_marginal_length", 0.0) or 0.0)
    default_marginal_width = float(ui_cfg.get("default_marginal_width", 0.0) or 0.0)
    default_marginal_height = float(ui_cfg.get("default_marginal_height", 0.0) or 0.0)
    default_destination_state = str(ui_cfg.get("default_destination_state", "FL")).strip() or "FL"
    default_perishable_type = str(ui_cfg.get("default_perishable_type", "N")).strip() or "N"

    return {
        "item_info": {
            "parquet_path": parquet_path,
            "item_number_column": item_number_column,
        },
        "warehouses": {
            "parquet_path": warehouse_parquet_path,
            "warehouse_number_column": warehouse_number_column,
        },
        "shipping_calculator_api": {
            "endpoint": shipping_calc_endpoint,
            "timeout_seconds": shipping_calc_timeout_seconds,
            "ids_public_company_code": ids_public_company_code,
            "user_id": shipping_calc_user_id,
            "company_type": shipping_calc_company_type,
            "has_lift_gate": bool(shipping_calc_cfg.get("has_lift_gate", False)),
            "force_common_carrier": bool(shipping_calc_cfg.get("force_common_carrier", False)),
            "exclude_lift_gate_fee": bool(shipping_calc_cfg.get("exclude_lift_gate_fee", True)),
            "bypass_matrix": bool(shipping_calc_cfg.get("bypass_matrix", False)),
        },
        "ui": {
            "default_warehouse": default_warehouse,
            "default_marginal_length": default_marginal_length,
            "default_marginal_width": default_marginal_width,
            "default_marginal_height": default_marginal_height,
            "default_destination_state": default_destination_state,
            "default_perishable_type": default_perishable_type,
        },
    }


LOGGER = utils.get_page_logger("Packaging Estimator")
PAGE_CONFIG = load_packaging_config()
utils.log_page_open_once("packaging_estimator_page", LOGGER)
if "_packaging_render_logged" not in st.session_state:
    st.session_state._packaging_render_logged = True
    LOGGER.info("Render UI.")


# ============================================================
# SESSION STATE
# ============================================================
if "pe_loaded" not in st.session_state:
    st.session_state.pe_loaded = False
if "pe_results" not in st.session_state:
    st.session_state.pe_results = {}
if "pe_errors" not in st.session_state:
    st.session_state.pe_errors = []
if "pe_dimension_unit" not in st.session_state:
    st.session_state.pe_dimension_unit = "in"
if "pe_weight_unit" not in st.session_state:
    st.session_state.pe_weight_unit = "lb"
if "pe_refrigeration_required" not in st.session_state:
    st.session_state.pe_refrigeration_required = False
if "pe_perishable_rows" not in st.session_state:
    st.session_state.pe_perishable_rows = [{"item_number": "", "perishable_code": ""}]
if "pe_selected_package_view" not in st.session_state:
    st.session_state.pe_selected_package_view = "All Items"
if "pe_recommendation_future" not in st.session_state:
    st.session_state.pe_recommendation_future = None
if "pe_recommendation_signature" not in st.session_state:
    st.session_state.pe_recommendation_signature = ""
if "pe_recommendation_results" not in st.session_state:
    st.session_state.pe_recommendation_results = []


# ============================================================
# INPUT HELPERS
# ============================================================
@st.cache_data
def read_excel_sheet_names(file_bytes: bytes) -> list[str]:
    """Read workbook sheet names from uploaded Excel bytes."""
    with pd.ExcelFile(BytesIO(file_bytes)) as workbook:
        return [str(name).strip() for name in workbook.sheet_names]


@st.cache_data
def read_excel_bytes(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    """Read one worksheet from uploaded Excel bytes into a DataFrame."""
    return pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, dtype=str)


@st.cache_data
def read_csv_bytes(file_bytes: bytes) -> pd.DataFrame:
    """Read uploaded CSV bytes into a DataFrame."""
    try:
        return pd.read_csv(BytesIO(file_bytes), dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(BytesIO(file_bytes), dtype=str, encoding="latin-1")


def normalize_col_name(col_name: str) -> str:
    return "".join(ch for ch in str(col_name).lower() if ch.isalnum())


def find_matching_column(columns: list[str], candidates: list[str]) -> str | None:
    if not columns:
        return None

    normalized_map: dict[str, str] = {}
    for col in columns:
        normalized_map.setdefault(normalize_col_name(col), col)

    for candidate in candidates:
        if candidate in columns:
            return candidate
        normalized_candidate = normalize_col_name(candidate)
        if normalized_candidate in normalized_map:
            return normalized_map[normalized_candidate]

    return None


def find_default_column(columns: list[str], targets: set[str], fallback_index: int) -> int:
    normalized = [normalize_col_name(col) for col in columns]
    for idx, col_name in enumerate(normalized):
        if col_name in targets:
            return idx
    return min(fallback_index, max(0, len(columns) - 1))


def normalize_item_number(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return "".join(str(value).split()).upper()


def normalize_warehouse_number(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        warehouse_decimal = Decimal(text)
        if warehouse_decimal != warehouse_decimal.to_integral_value():
            return None
        warehouse_number = int(warehouse_decimal)
        return warehouse_number if warehouse_number > 0 else None
    except (InvalidOperation, ValueError):
        return None


def get_uploaded_item_numbers(input_df: pd.DataFrame) -> list[str]:
    if input_df.empty or "ItemNumber" not in input_df.columns:
        return []

    unique_item_numbers: list[str] = []
    seen: set[str] = set()
    for raw_item in input_df["ItemNumber"].tolist():
        item_number = normalize_item_number(raw_item)
        if item_number and item_number not in seen:
            seen.add(item_number)
            unique_item_numbers.append(item_number)
    return unique_item_numbers


def normalize_perishable_rows(
    rows: Any,
    valid_item_numbers: set[str],
) -> list[dict[str, str]]:
    normalized_rows: list[dict[str, str]] = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            item_number = normalize_item_number(row.get("item_number"))
            perishable_code = str(row.get("perishable_code", "")).strip().upper()
            if item_number and item_number not in valid_item_numbers:
                item_number = ""
            if perishable_code not in PERISHABLE_CODE_TO_LABEL:
                perishable_code = ""
            normalized_rows.append(
                {
                    "item_number": item_number,
                    "perishable_code": perishable_code,
                }
            )

    if not normalized_rows:
        normalized_rows = [{"item_number": "", "perishable_code": ""}]

    while len(normalized_rows) > 1:
        last_row = normalized_rows[-1]
        previous_row = normalized_rows[-2]
        if (
            not last_row["item_number"]
            and not last_row["perishable_code"]
            and not previous_row["item_number"]
            and not previous_row["perishable_code"]
        ):
            normalized_rows.pop()
        else:
            break

    if normalized_rows[-1]["item_number"] and normalized_rows[-1]["perishable_code"]:
        normalized_rows.append({"item_number": "", "perishable_code": ""})

    return normalized_rows


def build_perishable_override_map(rows: list[dict[str, str]]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for row in rows:
        item_number = normalize_item_number(row.get("item_number"))
        perishable_code = str(row.get("perishable_code", "")).strip().upper()
        if item_number and perishable_code in PERISHABLE_CODE_TO_LABEL:
            overrides[item_number] = perishable_code
    return overrides


def parse_quantity(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    text = str(value).strip().replace(",", "")
    if not text:
        return None

    try:
        quantity_decimal = Decimal(text)
    except (InvalidOperation, ValueError):
        return None

    if quantity_decimal != quantity_decimal.to_integral_value():
        return None

    quantity = int(quantity_decimal)
    if quantity <= 0:
        return None
    return quantity


def parse_pasted_input(raw_text: str) -> tuple[pd.DataFrame, list[str]]:
    """Parse tab-separated pasted rows into a standard input DataFrame."""
    errors: list[str] = []
    records: list[dict[str, Any]] = []
    lines = raw_text.splitlines()

    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue

        parts = line.split("\t")
        if len(parts) < 2:
            errors.append(
                f"Line {line_number}: expected tab-separated values ItemNumber<TAB>Quantity."
            )
            continue

        records.append(
            {
                "ItemNumber": parts[0],
                "Quantity": parts[1],
                "_RowNumber": line_number,
            }
        )

    if not records:
        return pd.DataFrame(columns=["ItemNumber", "Quantity", "_RowNumber"]), errors
    return pd.DataFrame(records), errors


def validate_and_aggregate_rows(input_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Validate rows and aggregate duplicates by ItemNumber."""
    errors: list[str] = []
    valid_rows: list[dict[str, Any]] = []

    for idx, row in input_df.reset_index(drop=True).iterrows():
        row_number = int(row.get("_RowNumber", idx + 1))
        item_number = normalize_item_number(row.get("ItemNumber"))
        quantity = parse_quantity(row.get("Quantity"))

        if not item_number:
            errors.append(f"Row {row_number}: ItemNumber is blank.")
            continue
        if quantity is None:
            errors.append(
                f"Row {row_number}: Quantity '{row.get('Quantity')}' is invalid (must be integer > 0)."
            )
            continue

        valid_rows.append({"ItemNumber": item_number, "Quantity": quantity})

    if not valid_rows:
        return pd.DataFrame(columns=["ItemNumber", "Quantity"]), errors

    clean_df = (
        pd.DataFrame(valid_rows)
        .groupby("ItemNumber", as_index=False)["Quantity"]
        .sum()
        .sort_values("ItemNumber")
        .reset_index(drop=True)
    )
    return clean_df, errors


# ============================================================
# ITEM-INFO PARQUET LOOKUP
# ============================================================
@st.cache_data(show_spinner=False)
def load_item_info_parquet(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    df.columns = [extract_bracketed_column_name(str(col)) for col in df.columns]
    return df


@st.cache_data(show_spinner=False)
def load_warehouses_parquet(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    df.columns = [extract_bracketed_column_name(str(col)) for col in df.columns]
    return df


def extract_bracketed_column_name(column_name: str) -> str:
    matches = re.findall(r"\[([^\]]+)\]", column_name)
    if matches:
        return matches[-1].strip()
    return str(column_name).strip()


def resolve_configured_column_name(
    configured_name: str,
    available_columns: list[str],
    configured_label: str,
) -> str:
    if configured_name in available_columns:
        return configured_name

    extracted_name = extract_bracketed_column_name(configured_name)
    if extracted_name in available_columns:
        return extracted_name

    normalized_target = normalize_col_name(extracted_name)
    for col in available_columns:
        if normalize_col_name(col) == normalized_target:
            return col

    raise ValueError(
        f"Configured {configured_label} '{configured_name}' was not found in parquet file columns."
    )


def resolve_item_column_name(configured_name: str, available_columns: list[str]) -> str:
    return resolve_configured_column_name(configured_name, available_columns, "item number column")


def find_warehouse_details(warehouse_number: int) -> dict[str, str] | None:
    warehouse_cfg = PAGE_CONFIG.get("warehouses", {})
    parquet_path = str(warehouse_cfg.get("parquet_path", "")).strip()
    if not parquet_path:
        return None

    parquet_file = Path(parquet_path)
    if not parquet_file.exists():
        return None

    warehouses_df = load_warehouses_parquet(parquet_path)
    if warehouses_df.empty:
        return None

    configured_col = str(warehouse_cfg.get("warehouse_number_column", "LocationName")).strip() or "LocationName"
    resolved_col = resolve_configured_column_name(
        configured_col,
        warehouses_df.columns.tolist(),
        "warehouse number column",
    )
    matched_df = warehouses_df[warehouses_df[resolved_col].map(normalize_warehouse_number) == int(warehouse_number)]
    if matched_df.empty:
        return None

    row = matched_df.iloc[0]
    return {
        col: ("" if pd.isna(row[col]) else str(row[col]).strip())
        for col in matched_df.columns
    }


def get_available_warehouse_options() -> tuple[list[int], dict[int, str]]:
    warehouse_cfg = PAGE_CONFIG.get("warehouses", {})
    parquet_path = str(warehouse_cfg.get("parquet_path", "")).strip()
    if not parquet_path:
        return [], {}

    parquet_file = Path(parquet_path)
    if not parquet_file.exists():
        return [], {}

    warehouses_df = load_warehouses_parquet(parquet_path)
    if warehouses_df.empty:
        return [], {}

    configured_col = str(warehouse_cfg.get("warehouse_number_column", "LocationName")).strip() or "LocationName"
    resolved_col = resolve_configured_column_name(
        configured_col,
        warehouses_df.columns.tolist(),
        "warehouse number column",
    )

    warehouse_options: list[tuple[int, str]] = []
    for row in warehouses_df.to_dict(orient="records"):
        warehouse_number = normalize_warehouse_number(row.get(resolved_col))
        if warehouse_number is None:
            continue
        details = {
            col: ("" if pd.isna(value) else str(value).strip())
            for col, value in row.items()
        }
        warehouse_options.append((warehouse_number, format_warehouse_option(details)))

    warehouse_options = sorted(set(warehouse_options), key=lambda item: item[0])
    return [warehouse_number for warehouse_number, _ in warehouse_options], dict(warehouse_options)


def format_warehouse_details(details: dict[str, str]) -> str:
    warehouse_name = str(details.get("LocationName", "")).strip()
    address = str(details.get("LocationAddress1", "")).strip()
    city = str(details.get("LocationCity", "")).strip()
    state = str(details.get("LocationState", "")).strip()
    postal_code = str(details.get("LocationZipCode", "")).strip()
    country = str(details.get("LocationCountry", "")).strip()

    locality = ", ".join(part for part in [city, state] if part)
    if postal_code:
        locality = f"{locality} {postal_code}".strip() if locality else postal_code

    details_parts = [part for part in [address, locality, country] if part]
    if not details_parts:
        return warehouse_name or "Warehouse details unavailable"

    if warehouse_name:
        return f"{warehouse_name}: " + " | ".join(details_parts)
    return " | ".join(details_parts)


def format_warehouse_option(details: dict[str, str]) -> str:
    warehouse_name = str(details.get("LocationName", "")).strip()
    city = str(details.get("LocationCity", "")).strip()
    state = str(details.get("LocationState", "")).strip()
    locality = ", ".join(part for part in [city, state] if part)
    if warehouse_name and locality:
        return f"{warehouse_name} - {locality}"
    if warehouse_name:
        return warehouse_name
    return locality or "Unknown warehouse"


def coerce_verified_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return bool(value)
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "t", "yes", "y"}


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, float) and pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, float) and pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def fetch_item_info_rows(clean_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    item_info_cfg = PAGE_CONFIG.get("item_info", {})
    parquet_path = str(item_info_cfg.get("parquet_path", "")).strip()
    item_col = str(item_info_cfg.get("item_number_column", "ItemNumber")).strip() or "ItemNumber"

    if not parquet_path:
        raise ValueError("Item-info parquet path is not configured.")

    item_info_df = load_item_info_parquet(parquet_path)
    resolved_item_col = resolve_item_column_name(item_col, item_info_df.columns.tolist())

    requested = clean_df.copy()
    requested["_NormalizedItem"] = requested["ItemNumber"].map(normalize_item_number)

    table_df = item_info_df.copy()
    table_df["_NormalizedItem"] = table_df[resolved_item_col].map(normalize_item_number)

    requested_item_set = set(requested["_NormalizedItem"].tolist())
    matched_df = table_df[table_df["_NormalizedItem"].isin(requested_item_set)].copy()
    matched_df = matched_df.drop(columns=["_NormalizedItem"]).reset_index(drop=True)

    matched_items = set(table_df.loc[table_df["_NormalizedItem"].isin(requested_item_set), "_NormalizedItem"].tolist())
    unmatched_df = requested[~requested["_NormalizedItem"].isin(matched_items)][["ItemNumber", "Quantity"]].copy()
    unmatched_df = unmatched_df.reset_index(drop=True)

    return matched_df, unmatched_df


def split_verified_and_non_verified(
    matched_df: pd.DataFrame,
    unmatched_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    verified_df = pd.DataFrame()
    non_verified_df = pd.DataFrame()
    has_is_verified = (not matched_df.empty) and ("IsVerified" in matched_df.columns)

    if has_is_verified:
        verification_flags = matched_df["IsVerified"].map(coerce_verified_value)
        verified_df = matched_df[verification_flags].copy().reset_index(drop=True)
        non_verified_df = matched_df[~verification_flags].copy().reset_index(drop=True)
    elif not matched_df.empty:
        non_verified_df = matched_df.copy().reset_index(drop=True)

    if not unmatched_df.empty:
        if not matched_df.empty:
            base_cols = matched_df.columns.tolist()
            unmatched_for_non_verified = pd.DataFrame(
                {col: [pd.NA] * len(unmatched_df) for col in base_cols}
            )
            if "ItemNumber" in unmatched_for_non_verified.columns:
                unmatched_for_non_verified["ItemNumber"] = unmatched_df["ItemNumber"].values
            if "Quantity" in unmatched_for_non_verified.columns:
                unmatched_for_non_verified["Quantity"] = unmatched_df["Quantity"].values
            if "IsVerified" in unmatched_for_non_verified.columns:
                unmatched_for_non_verified["IsVerified"] = False
        else:
            unmatched_for_non_verified = unmatched_df.copy()
            unmatched_for_non_verified["IsVerified"] = False

        non_verified_df = pd.concat([non_verified_df, unmatched_for_non_verified], ignore_index=True)

    return verified_df, non_verified_df


def normalize_country_code(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {"USA", "UNITED STATES", "UNITED STATES OF AMERICA"}:
        return "US"
    return normalized or "US"


def build_shipping_request_options(overrides: dict[str, Any] | None = None) -> dict[str, bool]:
    shipping_cfg = PAGE_CONFIG.get("shipping_calculator_api", {})
    defaults = {
        "has_lift_gate": bool(shipping_cfg.get("has_lift_gate", False)),
        "force_common_carrier": bool(shipping_cfg.get("force_common_carrier", False)),
        "exclude_lift_gate_fee": bool(shipping_cfg.get("exclude_lift_gate_fee", True)),
        "bypass_matrix": bool(shipping_cfg.get("bypass_matrix", False)),
    }
    if not isinstance(overrides, dict):
        return defaults

    normalized: dict[str, bool] = {}
    for key, default in defaults.items():
        raw_value = overrides.get(key, default)
        if isinstance(raw_value, bool):
            normalized[key] = raw_value
        else:
            normalized[key] = str(raw_value).strip().lower() in {"1", "true", "t", "yes", "y"}
    return normalized


def normalize_shipping_address(
    raw_address: dict[str, Any] | None,
    *,
    company_type_default: str | None = None,
) -> dict[str, Any]:
    address = raw_address if isinstance(raw_address, dict) else {}
    country_code = normalize_country_code(address.get("Country", "US"))
    default_company_type = str(company_type_default).strip() if company_type_default is not None else ""

    return {
        "StreetAddress1": str(address.get("StreetAddress1", "")).strip(),
        "StreetAddress2": str(address.get("StreetAddress2", "")).strip(),
        "City": str(address.get("City", "")).strip(),
        "State": str(address.get("State", "")).strip().upper(),
        "ZipCode": str(address.get("ZipCode", "")).strip(),
        "Country": country_code,
        "IsCommercial": bool(address.get("IsCommercial", True)),
        "IsDomestic": country_code == "US",
        "IsRestrictedExpeditedShipping": bool(address.get("IsRestrictedExpeditedShipping", False)),
        "CompanyType": str(address.get("CompanyType", default_company_type)).strip(),
    }


def validate_shipping_address(
    raw_address: dict[str, Any] | None,
    *,
    company_type_default: str | None = None,
) -> dict[str, Any]:
    address = normalize_shipping_address(raw_address, company_type_default=company_type_default)
    missing_labels = {
        "StreetAddress1": "street address",
        "City": "city",
        "State": "state/province",
        "ZipCode": "ZIP/postal code",
        "Country": "country",
    }
    missing_fields = [
        label for field, label in missing_labels.items() if not str(address.get(field, "")).strip()
    ]
    if missing_fields:
        raise ValueError(
            "Destination is missing required field(s): " + ", ".join(missing_fields) + "."
        )
    return address


def build_shipping_address_from_warehouse(warehouse_details: dict[str, str]) -> dict[str, Any]:
    if not warehouse_details:
        raise ValueError("Select a valid warehouse destination before running the estimate.")

    shipping_cfg = PAGE_CONFIG.get("shipping_calculator_api", {})
    return validate_shipping_address(
        {
            "StreetAddress1": warehouse_details.get("LocationAddress1", ""),
            "StreetAddress2": warehouse_details.get("LocationName", ""),
            "City": warehouse_details.get("LocationCity", ""),
            "State": warehouse_details.get("LocationState", ""),
            "ZipCode": warehouse_details.get("LocationZipCode", ""),
            "Country": warehouse_details.get("LocationCountry", "US"),
            "IsCommercial": True,
            "IsRestrictedExpeditedShipping": False,
            "CompanyType": str(shipping_cfg.get("company_type", "Warehouse")).strip() or "Warehouse",
        },
        company_type_default=str(shipping_cfg.get("company_type", "Warehouse")).strip() or "Warehouse",
    )


def format_shipping_address(address: dict[str, Any] | None) -> str:
    normalized = normalize_shipping_address(address)
    locality = ", ".join(part for part in [normalized["City"], normalized["State"]] if part)
    if normalized["ZipCode"]:
        locality = f"{locality} {normalized['ZipCode']}".strip() if locality else normalized["ZipCode"]

    description_parts = [
        normalized["StreetAddress1"],
        normalized["StreetAddress2"],
        locality,
        normalized["Country"],
    ]
    return " | ".join(part for part in description_parts if part) or "Destination details unavailable"


def format_shipping_request_options(options: dict[str, Any] | None) -> str:
    normalized = build_shipping_request_options(options if isinstance(options, dict) else None)
    return ", ".join(
        [
            f"HasLiftGate={normalized['has_lift_gate']}",
            f"ForceCommonCarrier={normalized['force_common_carrier']}",
            f"ExcludeLiftGateFee={normalized['exclude_lift_gate_fee']}",
            f"BypassMatrix={normalized['bypass_matrix']}",
        ]
    )


def get_preferred_row_value(
    row: dict[str, Any],
    *keys: str,
    default: Any = None,
) -> Any:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                continue
            return stripped
        return value
    return default


def normalize_shipping_type_code(
    shipping_type: Any,
    calculated_shipping_type: Any = None,
) -> str:
    normalized = str(shipping_type or "").strip().upper()
    if normalized in {"M", "P"}:
        return normalized

    calculated_normalized = normalize_col_name(str(calculated_shipping_type or ""))
    if calculated_normalized == "commoncarrier":
        return "P"
    if calculated_normalized in {"ground", "fasttrack"}:
        return "M"
    return "M"


def normalize_carrier_ship_type_code(
    carrier_ship_type: Any,
    calculated_shipping_type: Any = None,
) -> str:
    normalized = str(carrier_ship_type or "").strip().upper()
    if normalized in {"U", "C"}:
        return normalized

    calculated_normalized = normalize_col_name(str(calculated_shipping_type or ""))
    if calculated_normalized == "commoncarrier":
        return "C"
    return "U"


def build_shipping_item_records(
    source_df: pd.DataFrame,
    requested_df: pd.DataFrame,
    perishable_type_overrides: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    if source_df.empty:
        return []

    ui_cfg = PAGE_CONFIG.get("ui", {})
    item_cfg = PAGE_CONFIG.get("item_info", {})
    item_col = str(item_cfg.get("item_number_column", "ItemNumber")).strip() or "ItemNumber"
    resolved_item_col = resolve_item_column_name(item_col, source_df.columns.tolist())

    quantity_map = {
        normalize_item_number(row.ItemNumber): int(row.Quantity)
        for row in requested_df.itertuples(index=False)
    }

    default_marginal_length = float(ui_cfg.get("default_marginal_length", 0.0) or 0.0)
    default_marginal_width = float(ui_cfg.get("default_marginal_width", 0.0) or 0.0)
    default_marginal_height = float(ui_cfg.get("default_marginal_height", 0.0) or 0.0)
    default_perishable_type = str(ui_cfg.get("default_perishable_type", "N")).strip().upper() or "N"
    if default_perishable_type not in PERISHABLE_CODE_TO_LABEL:
        default_perishable_type = "N"

    normalized_overrides: dict[str, str] = {}
    if perishable_type_overrides:
        for raw_item_number, raw_perishable_code in perishable_type_overrides.items():
            item_number = normalize_item_number(raw_item_number)
            perishable_code = str(raw_perishable_code).strip().upper()
            if item_number and perishable_code in PERISHABLE_CODE_TO_LABEL:
                normalized_overrides[item_number] = perishable_code

    shipping_items: list[dict[str, Any]] = []
    for row in source_df.to_dict(orient="records"):
        item_number = normalize_item_number(row.get(resolved_item_col))
        if not item_number:
            continue
        quantity = int(quantity_map.get(item_number, 0))
        if quantity <= 0:
            continue

        calculated_shipping_type = get_preferred_row_value(row, "CalculatedShippingType", default="")
        shipping_type = normalize_shipping_type_code(
            get_preferred_row_value(row, "ShippingType"),
            calculated_shipping_type,
        )
        carrier_ship_type = normalize_carrier_ship_type_code(
            get_preferred_row_value(row, "CarrierShipType"),
            calculated_shipping_type,
        )
        length = to_float(get_preferred_row_value(row, "Length", "LengthInInches"), 0.0)
        width = to_float(get_preferred_row_value(row, "Width", "WidthInInches"), 0.0)
        height = to_float(get_preferred_row_value(row, "Height", "HeightInInches"), 0.0)
        volume = to_float(
            get_preferred_row_value(row, "Volume", "AverageVolume"),
            length * width * height,
        )
        perishable_type = normalized_overrides.get(item_number, default_perishable_type)
        vendor_id = str(get_preferred_row_value(row, "VendorCode", default="") or "").strip()
        vendor_drop_ship_zip = str(
            get_preferred_row_value(row, "VendorDropShipZip", default="") or ""
        ).strip()
        is_common_carrier = (
            carrier_ship_type == "C"
            or normalize_col_name(str(calculated_shipping_type or "")) == "commoncarrier"
        )

        shipping_items.append(
            {
                "item_number": item_number,
                "quantity": quantity,
                "length": length,
                "width": width,
                "height": height,
                "weight": to_float(get_preferred_row_value(row, "Weight", "WeightInPounds"), 0.0),
                "break_quantity": to_int(row.get("BreakQuantity"), 0),
                "shipping_type": shipping_type,
                "carrier_ship_type": carrier_ship_type,
                "is_common_carrier": is_common_carrier,
                "is_ormd": coerce_verified_value(get_preferred_row_value(row, "IsOrmd")),
                "is_hazardous": coerce_verified_value(get_preferred_row_value(row, "IsHazardous")),
                "is_free_shipping": coerce_verified_value(get_preferred_row_value(row, "IsFreeShipping")),
                "fits_liftgate": coerce_verified_value(
                    get_preferred_row_value(row, "FitsLiftgate", default=True)
                ),
                "volume": volume,
                "restricted_expedited_shipping": coerce_verified_value(
                    get_preferred_row_value(row, "RestrictedExpeditedShipping")
                ),
                "oversize_fee": to_float(get_preferred_row_value(row, "OversizeFee"), 0.0),
                "vendor_id": vendor_id,
                "vendor_drop_ship_zip": vendor_drop_ship_zip,
                "is_drop_ship": coerce_verified_value(get_preferred_row_value(row, "IsDropShip")),
                "perishable_type": perishable_type,
                "marginal_length": to_float(
                    get_preferred_row_value(row, "MarginalLength"),
                    default_marginal_length,
                ),
                "marginal_height": to_float(
                    get_preferred_row_value(row, "MarginalHeight"),
                    default_marginal_height,
                ),
                "marginal_width": to_float(
                    get_preferred_row_value(row, "MarginalWidth"),
                    default_marginal_width,
                ),
                "is_repack": coerce_verified_value(
                    get_preferred_row_value(row, "IsRepack", "IsRepackRequired")
                ),
                "is_repositional": coerce_verified_value(
                    get_preferred_row_value(row, "IsRepositional", "IsRepositionable")
                ),
                "can_be_nested": coerce_verified_value(
                    get_preferred_row_value(row, "CanBeNested", "Can Nest?")
                ),
                "is_air_restricted": coerce_verified_value(
                    get_preferred_row_value(row, "IsAirRestricted")
                ),
            }
        )

    return shipping_items


def build_shipping_calculator_payload(
    shipping_items: list[dict[str, Any]],
    shipping_address: dict[str, Any],
    shipping_request_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not shipping_items:
        return {}
    validated_address = validate_shipping_address(shipping_address)

    shipping_cfg = PAGE_CONFIG.get("shipping_calculator_api", {})
    request_options = build_shipping_request_options(shipping_request_options)
    products: list[dict[str, Any]] = []
    for item in shipping_items:
        products.append(
            {
                "KitComponentDetails": [],
                "ItemNumber": item["item_number"],
                "Weight": item["weight"],
                "Quantity": item["quantity"],
                "BreakQuantity": item["break_quantity"],
                "ShippingType": item["shipping_type"],
                "CarrierShipType": item["carrier_ship_type"],
                "PerishableType": item["perishable_type"],
                "IsCommonCarrier": item["is_common_carrier"],
                "IsOrmd": item["is_ormd"],
                "IsHazardous": item["is_hazardous"],
                "IsFreeShipping": item["is_free_shipping"],
                "ShippingRateOverrideAmount": 0,
                "IsM1T": False,
                "MaximumGroundQuantity": 0,
                "FitsLiftgate": item["fits_liftgate"],
                "Volume": item["volume"],
                "IsOutletItem": False,
                "RestrictedExpeditedShipping": item["restricted_expedited_shipping"],
                "FreightClass": 1,
                "OversizeFee": item["oversize_fee"],
                "VendorId": item["vendor_id"],
                "VendorDropShipZip": item["vendor_drop_ship_zip"],
                "VendorDropShipState": "",
                "VendorDropShipCity": "",
                "VendorDropShipAddressLine1": "",
                "IsDropShip": item["is_drop_ship"],
                "Length": item["length"],
                "Width": item["width"],
                "Height": item["height"],
                "MarginalLength": item["marginal_length"],
                "MarginalWidth": item["marginal_width"],
                "MarginalHeight": item["marginal_height"],
                "IsRepack": item["is_repack"],
                "IsRepositional": item["is_repositional"],
                "CanBeNested": item["can_be_nested"],
                "Cost": 0,
                "Price": 0,
                "CustomerPaidShipping": 0,
                "StackedQuantity": 0,
                "Stacks": 0,
                "IsCustomPack": False,
                "UseRepackProcess": False,
                "IsSinglePack": False,
                "IsStandardPackage": False,
                "OrderNumber": 0,
                "IsNetCostDiscount": False,
                "IsAccessory": False,
                "IsAirRestricted": item["is_air_restricted"],
                "IsLtlOversize": False,
            }
        )

    return {
        "IdsPublicCompanyCode": str(
            shipping_cfg.get("ids_public_company_code", "WebstaurantStore")
        ).strip() or "WebstaurantStore",
        "Products": products,
        "CustomerInfo": {
            "ShippingAddress": validated_address,
            "ExcludedCarriers": [],
            "HasLimitedAccessOverride": False,
            "UserId": int(shipping_cfg.get("user_id", 130326) or 130326),
        },
        "HasLiftGate": request_options["has_lift_gate"],
        "ForceCommonCarrier": request_options["force_common_carrier"],
        "ExcludeLiftGateFee": request_options["exclude_lift_gate_fee"],
        "BypassMatrix": request_options["bypass_matrix"],
    }


def post_json_request(
    url: str,
    payload: Any,
    timeout_seconds: int,
    accept: str = "application/json",
) -> tuple[Any, str]:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        url=url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": accept,
        },
    )

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
        try:
            return json.loads(raw), ""
        except Exception:
            return raw, ""
    except HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8")
        except Exception:
            error_body = str(exc)
        return None, f"HTTP {exc.code}: {error_body}"
    except URLError as exc:
        return None, f"Network error: {exc.reason}"
    except Exception as exc:
        return None, f"API call failed: {exc}"


def call_shipping_calculator_api(payload: dict[str, Any]) -> tuple[Any, str]:
    if not payload:
        return None, ""

    shipping_cfg = PAGE_CONFIG.get("shipping_calculator_api", {})
    endpoint = str(shipping_cfg.get("endpoint", "")).strip()
    timeout_seconds = int(shipping_cfg.get("timeout_seconds", 30) or 30)
    if not endpoint:
        return None, "Shipping calculator API endpoint is not configured."

    return post_json_request(
        url=endpoint,
        payload=payload,
        timeout_seconds=timeout_seconds,
        accept="text/plain",
    )


def is_ground_shipping_option(option: dict[str, Any]) -> bool:
    search_text = " ".join(
        str(option.get(key, "")).strip()
        for key in ("Method", "MethodName", "MatrixMethodName", "CarrierName", "Carrier", "Option Type")
    ).lower()
    return "ground" in search_text


def is_common_carrier_shipping_option(option: dict[str, Any]) -> bool:
    if bool(option.get("IsCommonCarrier")):
        return True
    search_text = " ".join(
        str(option.get(key, "")).strip()
        for key in ("Method", "MethodName", "MatrixMethodName", "CarrierName", "Carrier", "Option Type")
    ).lower()
    return "common carrier" in search_text


def get_shipping_option_preference_rank(option: dict[str, Any]) -> int:
    if is_ground_shipping_option(option):
        return 0
    if is_common_carrier_shipping_option(option):
        return 1
    return 2


def build_shipping_source_candidate(
    quote: dict[str, Any],
    option: dict[str, Any],
    option_type: str,
    quote_index: int,
) -> dict[str, Any]:
    warehouse_number = normalize_warehouse_number(quote.get("WarehouseNumber"))
    delivery_days = to_int(option.get("DeliveryDays"), 0)
    cost = to_float(option.get("Cost"), 0.0)
    warehouse_state = str(quote.get("OriginState", "")).strip().upper()

    return {
        "Warehouse Number": warehouse_number if warehouse_number is not None else "",
        "Warehouse Address": str(quote.get("OriginAddressLine1", "")).strip(),
        "Warehouse City": str(quote.get("OriginCity", "")).strip(),
        "Warehouse State": warehouse_state,
        "Warehouse Zip": str(quote.get("OriginZipCode", "")).strip(),
        "Warehouse Label": str(warehouse_number) if warehouse_number is not None else "",
        "Option Type": option_type,
        "Carrier": str(option.get("CarrierName", "")).strip() or option_type,
        "Method": str(option.get("Method", option.get("MethodName", option_type))).strip() or option_type,
        "Method Guid": str(option.get("MethodGuid", "")).strip(),
        "Cost": cost,
        "Delivery Days": delivery_days if delivery_days > 0 else "",
        "Identifier": str(option.get("Identifier", "")).strip(),
        "Quote Index": quote_index,
        "_SortCost": cost,
        "_SortDeliveryDays": delivery_days if delivery_days > 0 else 9999,
    }


def extract_shipping_source_candidates(
    api_response: Any,
    include_all_methods: bool = True,
) -> list[dict[str, Any]]:
    if not isinstance(api_response, dict):
        return []

    quotes = api_response.get("ShippingQuotePerZipCode")
    if not isinstance(quotes, list):
        return []

    candidates: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for quote_index, quote in enumerate(quotes, start=1):
        if not isinstance(quote, dict):
            continue
        if quote.get("HasResult") is False:
            continue

        option_groups = [
            ("Parcel", quote.get("Options", []), False),
            ("Common Carrier", quote.get("CommonCarrierOptions", []), True),
            ("FedEx", quote.get("FedExOptions", []), False),
        ]
        for option_type, options, accept_all in option_groups:
            if not isinstance(options, list):
                continue
            for option in options:
                if not isinstance(option, dict):
                    continue
                if not include_all_methods and not accept_all and not is_ground_shipping_option(option):
                    continue

                candidate = build_shipping_source_candidate(
                    quote,
                    option,
                    option_type,
                    quote_index=quote_index,
                )
                dedupe_token = candidate["Identifier"] or "|".join(
                    [
                        str(candidate["Warehouse Number"]),
                        candidate["Option Type"],
                        candidate["Carrier"],
                        candidate["Method"],
                        f"{candidate['Cost']:.4f}",
                    ]
                )
                dedupe_key = (candidate["Warehouse Number"], dedupe_token)
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                candidates.append(candidate)

    return sorted(
        candidates,
        key=lambda row: (
            row["_SortCost"],
            row["_SortDeliveryDays"],
            to_int(row.get("Warehouse Number"), 999999),
        ),
    )


def build_shipping_method_key(method: dict[str, Any] | None) -> str:
    if not isinstance(method, dict):
        return ""
    method_guid = str(method.get("Method Guid", "")).strip()
    method_name = str(method.get("Method Name", "")).strip()
    carrier = str(method.get("Carrier", "")).strip()
    return method_guid or "|".join(part for part in [method_name, carrier] if part)


def extract_available_shipping_methods(
    api_response: Any,
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    methods: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    if isinstance(api_response, dict):
        raw_methods = api_response.get("ShippingMethodsAvailableForUserSelection", [])
        if isinstance(raw_methods, list):
            for entry in raw_methods:
                if not isinstance(entry, dict):
                    continue
                method_name = str(entry.get("MethodName", entry.get("MatrixMethodName", ""))).strip()
                method_guid = str(entry.get("MethodGuid", "")).strip()
                if not method_name and not method_guid:
                    continue

                identifiers = [
                    str(value).strip()
                    for value in entry.get("ShippingQuoteOptionIdentifiers", [])
                    if str(value).strip()
                ]
                method = {
                    "Method Name": method_name or method_guid,
                    "Method Guid": method_guid,
                    "Carrier": "",
                    "Delivery Days": to_int(entry.get("DeliveryDays"), 0),
                    "Cost": to_float(entry.get("Cost"), 0.0),
                    "Identifiers": identifiers,
                }
                method_key = build_shipping_method_key(method)
                if method_key and method_key not in seen_keys:
                    seen_keys.add(method_key)
                    methods.append(method)

    if methods:
        return sorted(
            methods,
            key=lambda row: (
                to_float(row.get("Cost"), float("inf")),
                to_int(row.get("Delivery Days"), 9999),
                str(row.get("Method Name", "")),
            ),
        )

    grouped: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        identifier = str(candidate.get("Identifier", "")).strip()
        method = {
            "Method Name": str(candidate.get("Method", "")).strip(),
            "Method Guid": str(candidate.get("Method Guid", "")).strip(),
            "Carrier": str(candidate.get("Carrier", "")).strip(),
            "Delivery Days": to_int(candidate.get("Delivery Days"), 0),
            "Cost": to_float(candidate.get("Cost"), 0.0),
            "Identifiers": [identifier] if identifier else [],
        }
        method_key = build_shipping_method_key(method)
        if not method_key:
            continue

        existing = grouped.get(method_key)
        if existing is None or (
            to_float(method.get("Cost"), float("inf")),
            to_int(method.get("Delivery Days"), 9999),
        ) < (
            to_float(existing.get("Cost"), float("inf")),
            to_int(existing.get("Delivery Days"), 9999),
        ):
            grouped[method_key] = method
        elif identifier:
            merged_identifiers = set(existing.get("Identifiers", []))
            merged_identifiers.add(identifier)
            existing["Identifiers"] = sorted(merged_identifiers)

    return sorted(
        grouped.values(),
        key=lambda row: (
            to_float(row.get("Cost"), float("inf")),
            to_int(row.get("Delivery Days"), 9999),
            str(row.get("Method Name", "")),
        ),
    )


def format_shipping_method_option(method: dict[str, Any]) -> str:
    method_name = str(method.get("Method Name", "")).strip() or "Unknown Method"
    delivery_days = to_int(method.get("Delivery Days"), 0)
    cost = to_float(method.get("Internal Shipping Cost", method.get("Cost", 0.0)), 0.0)
    parts: list[str] = [method_name]
    if delivery_days > 0:
        parts.append(f"{delivery_days} day{'s' if delivery_days != 1 else ''}")
    if "Internal Shipping Cost" in method or cost > 0:
        parts.append(f"${cost:,.2f} cost")
    return " | ".join(parts)


def candidate_matches_shipping_method(candidate: dict[str, Any], method: dict[str, Any] | None) -> bool:
    if not method:
        return True

    method_identifiers = {str(value).strip() for value in method.get("Identifiers", []) if str(value).strip()}
    candidate_identifier = str(candidate.get("Identifier", "")).strip()
    if method_identifiers and candidate_identifier and candidate_identifier in method_identifiers:
        return True

    method_guid = str(method.get("Method Guid", "")).strip().lower()
    candidate_guid = str(candidate.get("Method Guid", "")).strip().lower()
    if method_guid and candidate_guid:
        return method_guid == candidate_guid

    method_name = str(method.get("Method Name", "")).strip().lower()
    candidate_method = str(candidate.get("Method", "")).strip().lower()
    if method_name and candidate_method:
        return method_name == candidate_method

    return False


def filter_shipping_candidates_by_method(
    candidates: list[dict[str, Any]],
    method: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not method:
        return list(candidates)
    return [candidate for candidate in candidates if candidate_matches_shipping_method(candidate, method)]


def select_shipping_source_candidate(
    candidates: list[dict[str, Any]],
    method: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    filtered_candidates = filter_shipping_candidates_by_method(candidates, method)
    if not filtered_candidates:
        filtered_candidates = candidates
    if not filtered_candidates:
        return None
    return sorted(
        filtered_candidates,
        key=lambda row: (
            row.get("_SortCost", float("inf")),
            row.get("_SortDeliveryDays", 9999),
            to_int(row.get("Warehouse Number"), 999999),
        ),
    )[0]


def get_default_shipping_method(
    available_methods: list[dict[str, Any]],
    selected_candidate: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not available_methods:
        return None
    preferred_methods = sorted(
        available_methods,
        key=lambda method: (
            to_float(method.get("Internal Shipping Cost", method.get("Cost", float("inf"))), float("inf")),
            to_int(method.get("Delivery Days"), 9999),
            str(method.get("Method Name", "")),
        ),
    )
    if not selected_candidate:
        return preferred_methods[0] if preferred_methods else available_methods[0]

    preferred_guid = str(selected_candidate.get("Method Guid", "")).strip().lower()
    preferred_identifier = str(selected_candidate.get("Identifier", "")).strip()
    preferred_method_name = str(selected_candidate.get("Method", "")).strip().lower()
    for method in available_methods:
        identifiers = {str(value).strip() for value in method.get("Identifiers", []) if str(value).strip()}
        method_guid = str(method.get("Method Guid", "")).strip().lower()
        method_name = str(method.get("Method Name", "")).strip().lower()
        if preferred_identifier and preferred_identifier in identifiers:
            return method
        if preferred_guid and method_guid and preferred_guid == method_guid:
            return method
        if preferred_method_name and method_name and preferred_method_name == method_name:
            return method
    return preferred_methods[0] if preferred_methods else available_methods[0]


def select_preferred_shipping_candidates(
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not candidates:
        return []

    grouped_candidates: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        group_key = (
            to_int(candidate.get("Quote Index"), 0),
            normalize_warehouse_number(candidate.get("Warehouse Number")),
        )
        grouped_candidates.setdefault(group_key, []).append(candidate)

    preferred_candidates: list[dict[str, Any]] = []
    for group_candidates in grouped_candidates.values():
        preferred_candidates.append(
            sorted(
                group_candidates,
                key=lambda candidate: (
                    to_float(candidate.get("Cost"), float("inf")),
                    to_int(candidate.get("Delivery Days"), 9999),
                    str(candidate.get("Method", "")),
                    str(candidate.get("Carrier", "")),
                ),
            )[0]
        )

    return sorted(
        preferred_candidates,
        key=lambda candidate: (
            to_int(candidate.get("Quote Index"), 0),
            normalize_warehouse_number(candidate.get("Warehouse Number")) or 999999,
        ),
    )


def build_source_warehouse_details(candidate: dict[str, Any] | None) -> dict[str, str]:
    if not candidate:
        return {}

    warehouse_number = normalize_warehouse_number(candidate.get("Warehouse Number"))
    if warehouse_number is not None:
        details = find_warehouse_details(warehouse_number)
        if details:
            return details

    return {
        "LocationName": str(candidate.get("Warehouse Number", "")).strip(),
        "LocationAddress1": str(candidate.get("Warehouse Address", "")).strip(),
        "LocationCity": str(candidate.get("Warehouse City", "")).strip(),
        "LocationState": str(candidate.get("Warehouse State", "")).strip(),
        "LocationZipCode": str(candidate.get("Warehouse Zip", "")).strip(),
        "LocationCountry": "US",
    }


def filter_internal_transfer_candidates(
    candidates: list[dict[str, Any]],
    destination_warehouse_number: int | None,
) -> list[dict[str, Any]]:
    if destination_warehouse_number is None:
        return list(candidates)
    return [
        candidate
        for candidate in candidates
        if normalize_warehouse_number(candidate.get("Warehouse Number")) != int(destination_warehouse_number)
    ]


def calculate_internal_shipping_cost(
    method: dict[str, Any] | None,
    candidates: list[dict[str, Any]],
    destination_warehouse_number: int | None,
) -> float:
    if method:
        raw_cost = to_float(method.get("Cost"), 0.0)
        if destination_warehouse_number is None:
            return raw_cost
        method_candidates = filter_shipping_candidates_by_method(candidates, method)
        excluded_cost = sum(
            to_float(candidate.get("Cost"), 0.0)
            for candidate in method_candidates
            if normalize_warehouse_number(candidate.get("Warehouse Number")) == int(destination_warehouse_number)
        )
        return max(0.0, raw_cost - excluded_cost)

    internal_candidates = filter_internal_transfer_candidates(candidates, destination_warehouse_number)
    chosen_candidate = select_shipping_source_candidate(internal_candidates)
    if chosen_candidate:
        return to_float(chosen_candidate.get("Cost"), 0.0)
    if candidates and destination_warehouse_number is not None:
        return 0.0
    return 0.0


def decorate_shipping_methods_for_destination(
    methods: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    destination_warehouse_number: int | None,
) -> list[dict[str, Any]]:
    decorated_methods: list[dict[str, Any]] = []
    for method in methods:
        method_copy = dict(method)
        method_candidates = filter_shipping_candidates_by_method(candidates, method_copy)
        included_candidates = filter_internal_transfer_candidates(method_candidates, destination_warehouse_number)
        method_copy["Internal Shipping Cost"] = calculate_internal_shipping_cost(
            method_copy,
            candidates,
            destination_warehouse_number,
        )
        method_copy["Included Source Warehouses"] = sorted(
            {
                to_int(candidate.get("Warehouse Number"), 0)
                for candidate in included_candidates
                if to_int(candidate.get("Warehouse Number"), 0) > 0
            }
        )
        method_copy["Excluded Source Warehouses"] = sorted(
            {
                to_int(candidate.get("Warehouse Number"), 0)
                for candidate in method_candidates
                if to_int(candidate.get("Warehouse Number"), 0) > 0
                and destination_warehouse_number is not None
                and to_int(candidate.get("Warehouse Number"), 0) == int(destination_warehouse_number)
            }
        )
        decorated_methods.append(method_copy)

    return sorted(
        decorated_methods,
        key=lambda row: (
            to_float(row.get("Internal Shipping Cost"), float("inf")),
            to_int(row.get("Delivery Days"), 9999),
            str(row.get("Method Name", "")),
        ),
    )


def build_shipping_source_options_df(
    candidates: list[dict[str, Any]],
    destination_warehouse_number: int | None = None,
) -> pd.DataFrame:
    if not candidates:
        return pd.DataFrame(
            columns=[
                "Source Warehouse",
                "Option Type",
                "Carrier",
                "Method",
                "Delivery Days",
                "API Cost",
                "Internal Shipping Cost",
            ]
        )

    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        warehouse_details = build_source_warehouse_details(candidate)
        warehouse_label = format_warehouse_option(warehouse_details)
        candidate_warehouse_number = normalize_warehouse_number(candidate.get("Warehouse Number"))
        candidate_cost = to_float(candidate.get("Cost"), 0.0)
        internal_cost = (
            0.0
            if destination_warehouse_number is not None
            and candidate_warehouse_number == int(destination_warehouse_number)
            else candidate_cost
        )
        rows.append(
            {
                "Source Warehouse": warehouse_label,
                "Option Type": candidate.get("Option Type", ""),
                "Carrier": candidate.get("Carrier", ""),
                "Method": candidate.get("Method", ""),
                "Delivery Days": candidate.get("Delivery Days", ""),
                "API Cost": candidate_cost,
                "Internal Shipping Cost": internal_cost,
            }
        )

    return pd.DataFrame(rows)


def extract_shipping_quote_for_candidate(
    api_response: Any,
    candidate: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not candidate or not isinstance(api_response, dict):
        return None

    quotes = api_response.get("ShippingQuotePerZipCode")
    if not isinstance(quotes, list):
        return None

    quote_index = to_int(candidate.get("Quote Index"), 0)
    if quote_index > 0 and quote_index <= len(quotes):
        quote = quotes[quote_index - 1]
        if isinstance(quote, dict):
            return quote

    candidate_identifier = str(candidate.get("Identifier", "")).strip()
    candidate_warehouse_number = normalize_warehouse_number(candidate.get("Warehouse Number"))
    for quote in quotes:
        if not isinstance(quote, dict):
            continue
        quote_warehouse_number = normalize_warehouse_number(quote.get("WarehouseNumber"))
        if candidate_warehouse_number is not None and quote_warehouse_number == candidate_warehouse_number:
            return quote

        for option_collection_name in ("Options", "FedExOptions", "CommonCarrierOptions"):
            option_collection = quote.get(option_collection_name, [])
            if not isinstance(option_collection, list):
                continue
            for option in option_collection:
                if not isinstance(option, dict):
                    continue
                if candidate_identifier and str(option.get("Identifier", "")).strip() == candidate_identifier:
                    return quote

    return None


def build_shipping_package_tables(
    api_response: Any,
    candidate: dict[str, Any] | None,
) -> tuple[pd.DataFrame, dict[int, pd.DataFrame]]:
    quote = extract_shipping_quote_for_candidate(api_response, candidate)
    if not quote:
        return pd.DataFrame(columns=["Package Number", "Items", "Length", "Width", "Height", "Volume", "Weight", "Container"]), {}

    raw_packages: list[dict[str, Any]] = []
    for collection_name in ("ParcelPackages", "CarrierPackages"):
        collection = quote.get(collection_name, [])
        if isinstance(collection, list):
            raw_packages.extend([package for package in collection if isinstance(package, dict)])

    if not raw_packages:
        return pd.DataFrame(columns=["Package Number", "Items", "Length", "Width", "Height", "Volume", "Weight", "Container"]), {}

    package_rows: list[dict[str, Any]] = []
    item_tables: dict[int, pd.DataFrame] = {}
    package_number = 0

    for package in raw_packages:
        repeat_count = max(1, to_int(package.get("PackageQuantity"), 1))
        length = to_float(package.get("Length"), 0.0)
        width = to_float(package.get("Width"), 0.0)
        height = to_float(package.get("Height"), 0.0)
        volume = to_float(package.get("Volume"), 0.0)
        weight = to_float(package.get("Weight"), 0.0)
        container_name = str(package.get("ContainerName", "")).strip()
        products = package.get("Products", [])

        item_rows: list[dict[str, Any]] = []
        if isinstance(products, list):
            for product in products:
                if not isinstance(product, dict):
                    continue
                item_number = normalize_item_number(product.get("ItemNumber"))
                quantity = to_float(product.get("Quantity"), 0.0)
                product_length = to_float(product.get("Length"), 0.0)
                product_width = to_float(product.get("Width"), 0.0)
                product_height = to_float(product.get("Height"), 0.0)
                per_unit_volume = to_float(
                    product.get("Volume"),
                    product_length * product_width * product_height,
                )
                total_volume = per_unit_volume * quantity
                total_weight = to_float(product.get("Weight"), 0.0) * quantity
                if item_number and quantity > 0:
                    item_rows.append(
                        {
                            "Item Number": item_number,
                            "Quantity": quantity,
                            "Volume": total_volume,
                            "Weight": total_weight,
                        }
                    )

        items_count = int(sum(to_float(item.get("Quantity"), 0.0) for item in item_rows))
        for _ in range(repeat_count):
            package_number += 1
            package_rows.append(
                {
                    "Package Number": package_number,
                    "Items": items_count,
                    "Length": length,
                    "Width": width,
                    "Height": height,
                    "Volume": volume,
                    "Weight": weight,
                    "Container": container_name,
                }
            )
            if item_rows:
                item_df = pd.DataFrame(item_rows).copy()
                item_df["Quantity"] = _safe_numeric(item_df["Quantity"]).fillna(0.0)
                item_df["Volume"] = _safe_numeric(item_df["Volume"]).fillna(0.0)
                item_df["Weight"] = _safe_numeric(item_df["Weight"]).fillna(0.0)
                item_df = (
                    item_df.groupby("Item Number", as_index=False)[["Quantity", "Volume", "Weight"]]
                    .sum()
                    .sort_values("Item Number")
                    .reset_index(drop=True)
                )
                item_tables[package_number] = item_df

    package_df = pd.DataFrame(package_rows)
    if package_df.empty:
        return pd.DataFrame(columns=["Package Number", "Items", "Length", "Width", "Height", "Volume", "Weight", "Container"]), {}

    package_df["Package Number"] = _safe_numeric(package_df["Package Number"]).fillna(0).astype(int)
    package_df["Items"] = _safe_numeric(package_df["Items"]).fillna(0).astype(int)
    package_df["Length"] = _safe_numeric(package_df["Length"]).fillna(0.0)
    package_df["Width"] = _safe_numeric(package_df["Width"]).fillna(0.0)
    package_df["Height"] = _safe_numeric(package_df["Height"]).fillna(0.0)
    package_df["Volume"] = _safe_numeric(package_df["Volume"]).fillna(0.0)
    package_df["Weight"] = _safe_numeric(package_df["Weight"]).fillna(0.0)
    package_df = package_df.sort_values("Package Number").reset_index(drop=True)
    return package_df, item_tables


def build_shipping_method_package_tables(
    api_response: Any,
    candidates: list[dict[str, Any]],
    method: dict[str, Any] | None,
    destination_warehouse_number: int | None = None,
) -> tuple[pd.DataFrame, dict[int, pd.DataFrame]]:
    method_candidates = filter_shipping_candidates_by_method(candidates, method)
    if not method_candidates:
        method_candidates = list(candidates)

    included_candidates = filter_internal_transfer_candidates(
        method_candidates,
        destination_warehouse_number,
    )
    if not included_candidates:
        return (
            pd.DataFrame(
                columns=[
                    "Package Number",
                    "Source Warehouse",
                    "Items",
                    "Length",
                    "Width",
                    "Height",
                    "Volume",
                    "Weight",
                    "Container",
                ]
            ),
            {},
        )

    combined_package_frames: list[pd.DataFrame] = []
    combined_item_tables: dict[int, pd.DataFrame] = {}
    next_package_number = 1

    for candidate in included_candidates:
        package_df, item_tables = build_shipping_package_tables(api_response, candidate)
        if package_df.empty:
            continue

        warehouse_number = normalize_warehouse_number(candidate.get("Warehouse Number"))
        renumbered_package_df = package_df.copy()
        number_map: dict[int, int] = {}
        for original_package_number in renumbered_package_df["Package Number"].astype(int).tolist():
            if original_package_number not in number_map:
                number_map[original_package_number] = next_package_number
                next_package_number += 1

        renumbered_package_df["Package Number"] = renumbered_package_df["Package Number"].map(
            lambda value: number_map.get(to_int(value, 0), to_int(value, 0))
        )
        renumbered_package_df.insert(
            1,
            "Source Warehouse",
            warehouse_number if warehouse_number is not None else "",
        )
        combined_package_frames.append(renumbered_package_df)

        for original_package_number, item_df in item_tables.items():
            new_package_number = number_map.get(to_int(original_package_number, 0))
            if not new_package_number or item_df.empty:
                continue
            combined_item_tables[new_package_number] = item_df.copy()

    if not combined_package_frames:
        return (
            pd.DataFrame(
                columns=[
                    "Package Number",
                    "Source Warehouse",
                    "Items",
                    "Length",
                    "Width",
                    "Height",
                    "Volume",
                    "Weight",
                    "Container",
                ]
            ),
            {},
        )

    combined_package_df = (
        pd.concat(combined_package_frames, ignore_index=True)
        .sort_values("Package Number")
        .reset_index(drop=True)
    )
    return combined_package_df, combined_item_tables


def build_shipping_package_allocation_df(
    api_response: Any,
    candidates: list[dict[str, Any]],
) -> pd.DataFrame:
    preferred_candidates = select_preferred_shipping_candidates(candidates if isinstance(candidates, list) else [])
    if not preferred_candidates:
        return pd.DataFrame(
            columns=[
                "Package Number",
                "Item Number",
                "Method",
                "Quantity",
                "Volume",
                "Source Warehouse",
                "Delivery Days",
                "Carrier",
                "Package Cost Allocation by Volume",
            ]
        )

    allocation_rows: list[dict[str, Any]] = []

    for candidate in preferred_candidates:
        if not isinstance(candidate, dict):
            continue

        package_df, item_tables = build_shipping_package_tables(api_response, candidate)
        if package_df.empty or not item_tables:
            continue

        candidate_cost = to_float(candidate.get("Cost"), 0.0)
        total_quote_package_volume = float(
            _safe_numeric(package_df.get("Volume", pd.Series(dtype="float64"))).fillna(0.0).sum()
        )
        source_warehouse_number = normalize_warehouse_number(candidate.get("Warehouse Number"))
        source_warehouse_display = str(source_warehouse_number or "").strip()
        method_name = str(candidate.get("Method", "")).strip()
        carrier_name = str(candidate.get("Carrier", "")).strip()
        delivery_days = to_int(candidate.get("Delivery Days"), 0)

        for package_row in package_df.to_dict(orient="records"):
            package_number = to_int(package_row.get("Package Number"), 0)
            if package_number <= 0:
                continue

            package_volume = to_float(package_row.get("Volume"), 0.0)
            estimated_package_cost = (
                candidate_cost * (package_volume / total_quote_package_volume)
                if total_quote_package_volume > 0 and package_volume > 0
                else 0.0
            )
            item_df = item_tables.get(package_number, pd.DataFrame())
            if item_df.empty:
                continue

            package_item_total_volume = float(
                _safe_numeric(item_df.get("Volume", pd.Series(dtype="float64"))).fillna(0.0).sum()
            )

            for item_row in item_df.to_dict(orient="records"):
                item_volume = to_float(item_row.get("Volume"), 0.0)
                allocated_cost = (
                    estimated_package_cost * (item_volume / package_item_total_volume)
                    if package_item_total_volume > 0 and item_volume > 0
                    else 0.0
                )
                allocation_rows.append(
                    {
                        "Package Number": package_number,
                        "Item Number": str(item_row.get("Item Number", "")).strip(),
                        "Method": method_name,
                        "Quantity": to_float(item_row.get("Quantity"), 0.0),
                        "Volume": item_volume,
                        "Source Warehouse": source_warehouse_display,
                        "Delivery Days": delivery_days,
                        "Carrier": carrier_name,
                        "Package Cost Allocation by Volume": allocated_cost,
                    }
                )

    if not allocation_rows:
        return pd.DataFrame(
            columns=[
                "Package Number",
                "Item Number",
                "Method",
                "Quantity",
                "Volume",
                "Source Warehouse",
                "Delivery Days",
                "Carrier",
                "Package Cost Allocation by Volume",
            ]
        )

    allocation_df = pd.DataFrame(allocation_rows)
    allocation_df["Package Number"] = _safe_numeric(allocation_df["Package Number"]).fillna(0).astype(int)
    allocation_df["Quantity"] = _safe_numeric(allocation_df["Quantity"]).fillna(0.0)
    allocation_df["Volume"] = _safe_numeric(allocation_df["Volume"]).fillna(0.0)
    allocation_df["Delivery Days"] = _safe_numeric(allocation_df["Delivery Days"]).fillna(0).astype(int)
    allocation_df["Package Cost Allocation by Volume"] = (
        _safe_numeric(allocation_df["Package Cost Allocation by Volume"]).fillna(0.0).round(2)
    )
    return allocation_df.sort_values(
        ["Source Warehouse", "Method", "Carrier", "Delivery Days", "Package Number", "Item Number"],
        kind="stable",
    ).reset_index(drop=True)


def normalize_api_response_to_df(api_response: Any) -> pd.DataFrame:
    if api_response is None:
        return pd.DataFrame()

    if isinstance(api_response, list):
        if not api_response:
            return pd.DataFrame()
        if all(isinstance(x, dict) for x in api_response):
            return pd.json_normalize(api_response)
        return pd.DataFrame({"value": api_response})

    if isinstance(api_response, dict):
        for key in ("packages", "Packages", "data", "results", "items"):
            nested = api_response.get(key)
            if isinstance(nested, dict):
                api_response = nested
                break
            if isinstance(nested, list) and nested and all(isinstance(x, dict) for x in nested):
                return pd.json_normalize(nested)

        package_rows: list[dict[str, Any]] = []
        for idx, (package_id, package) in enumerate(api_response.items(), start=1):
            if not isinstance(package, dict):
                continue
            package_num = to_int(package_id, -1)
            if package_num <= 0:
                package_num = to_int(
                    package.get(
                        "packageNumber",
                        package.get(
                            "PackageNumber",
                            package.get("package_number", package.get("Package Number", -1)),
                        ),
                    ),
                    -1,
                )
            if package_num <= 0:
                package_num = idx

            volume = to_float(
                package.get(
                    "volume",
                    package.get(
                        "Volume",
                        package.get("packageVolume", package.get("totalVolume", 0.0)),
                    ),
                ),
                0.0,
            )
            weight = to_float(
                package.get(
                    "weight",
                    package.get(
                        "Weight",
                        package.get("packageWeight", package.get("totalWeight", 0.0)),
                    ),
                ),
                0.0,
            )
            package_rows.append(
                {
                    "Package Number": package_num,
                    "Items": _extract_item_count(package),
                    "Volume": volume,
                    "Weight": weight,
                }
            )
        if package_rows:
            df = pd.DataFrame(package_rows)
            return df.sort_values("Package Number").reset_index(drop=True)

        for key in ("data", "results", "items"):
            value = api_response.get(key)
            if isinstance(value, list) and value and all(isinstance(x, dict) for x in value):
                return pd.json_normalize(value)

        return pd.json_normalize([api_response])

    return pd.DataFrame({"value": [str(api_response)]})


def _extract_item_count(package: dict[str, Any]) -> int:
    direct_candidates = [
        "itemCount",
        "ItemCount",
        "itemsCount",
        "ItemsCount",
        "quantity",
        "Quantity",
        "qty",
        "Qty",
        "totalItems",
        "TotalItems",
    ]
    for key in direct_candidates:
        if key in package:
            value = package.get(key)
            if isinstance(value, (list, dict)):
                continue
            return max(0, to_int(value, 0))

    collection_candidates = ["items", "Items", "itemNumbers", "ItemNumbers", "packageItems", "PackageItems"]
    quantity_keys = ["quantity", "Quantity", "qty", "Qty"]
    for key in collection_candidates:
        if key not in package:
            continue
        value = package.get(key)
        if isinstance(value, list):
            total = 0
            for entry in value:
                if isinstance(entry, dict):
                    qty = 0
                    for qty_key in quantity_keys:
                        if qty_key in entry:
                            qty = to_int(entry.get(qty_key), 0)
                            break
                    total += qty if qty > 0 else 1
                else:
                    total += 1
            return total
        if isinstance(value, dict):
            numeric_total = 0
            numeric_found = False
            for maybe_qty in value.values():
                qty = to_int(maybe_qty, -1)
                if qty >= 0:
                    numeric_total += qty
                    numeric_found = True
            if numeric_found:
                return max(0, numeric_total)
            return len(value)

    return 0


def build_package_details_pivot(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=["Package Number", "Items", "Volume", "Weight"])

    details_df = raw_df.copy()
    columns = details_df.columns.tolist()
    package_col = find_matching_column(
        columns,
        ["Package Number", "PackageNumber", "packageNumber", "package_number", "PackageId", "packageId", "id"],
    )
    items_col = find_matching_column(
        columns,
        ["Items", "ItemCount", "itemCount", "itemsCount", "Quantity", "quantity", "Qty", "qty", "totalItems"],
    )
    volume_col = find_matching_column(
        columns,
        ["Volume", "volume", "AverageVolume", "packageVolume", "totalVolume"],
    )
    weight_col = find_matching_column(
        columns,
        ["Weight", "weight", "WeightInPounds", "packageWeight", "totalWeight"],
    )

    if package_col:
        details_df["Package Number"] = details_df[package_col]
    else:
        details_df["Package Number"] = details_df.index + 1
    if items_col:
        details_df["Items"] = details_df[items_col]
    else:
        details_df["Items"] = 0
    if volume_col:
        details_df["Volume"] = details_df[volume_col]
    else:
        details_df["Volume"] = 0.0
    if weight_col:
        details_df["Weight"] = details_df[weight_col]
    else:
        details_df["Weight"] = 0.0

    package_numbers = _safe_numeric(details_df["Package Number"]).fillna(0).astype(int)
    if (package_numbers <= 0).any():
        next_package_num = max(0, int(package_numbers.max()))
        normalized_package_numbers: list[int] = []
        for package_num in package_numbers.tolist():
            if package_num > 0:
                normalized_package_numbers.append(package_num)
            else:
                next_package_num += 1
                normalized_package_numbers.append(next_package_num)
        package_numbers = pd.Series(normalized_package_numbers, index=details_df.index, dtype="int64")
    details_df["Package Number"] = package_numbers
    details_df["Items"] = _safe_numeric(details_df["Items"]).fillna(0)
    details_df["Volume"] = _safe_numeric(details_df["Volume"]).fillna(0.0)
    details_df["Weight"] = _safe_numeric(details_df["Weight"]).fillna(0.0)

    pivot_df = (
        details_df.pivot_table(
            index="Package Number",
            values=["Items", "Volume", "Weight"],
            aggfunc="sum",
        )
        .reset_index()
        .sort_values("Package Number")
        .reset_index(drop=True)
    )
    pivot_df["Items"] = pivot_df["Items"].round(0).astype(int)
    return pivot_df


def _pick_dict_value(record: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in record:
            return record.get(key)

    normalized_map: dict[str, Any] = {}
    for raw_key in record.keys():
        normalized_map.setdefault(normalize_col_name(str(raw_key)), raw_key)

    for key in keys:
        normalized_key = normalize_col_name(key)
        if normalized_key in normalized_map:
            return record.get(normalized_map[normalized_key])

    return default


def _extract_package_candidates(api_response: Any) -> list[tuple[Any, dict[str, Any]]]:
    if api_response is None:
        return []

    if isinstance(api_response, list):
        return [
            (index + 1, package)
            for index, package in enumerate(api_response)
            if isinstance(package, dict)
        ]

    if not isinstance(api_response, dict):
        return []

    for key in ("packages", "Packages", "data", "results", "items"):
        nested = api_response.get(key)
        if isinstance(nested, dict):
            if all(isinstance(value, dict) for value in nested.values()):
                return list(nested.items())
        if isinstance(nested, list):
            return [
                (index + 1, package)
                for index, package in enumerate(nested)
                if isinstance(package, dict)
            ]

    if all(isinstance(value, dict) for value in api_response.values()):
        return list(api_response.items())

    return [(1, api_response)]


def _extract_item_rows_from_package(package: dict[str, Any]) -> list[dict[str, Any]]:
    item_number_keys = ["itemNumber", "ItemNumber", "item", "Item", "sku", "Sku", "itemNo", "item_number"]
    quantity_keys = ["quantity", "Quantity", "qty", "Qty", "itemCount", "ItemCount", "count", "Count"]
    volume_keys = ["volume", "Volume", "itemVolume", "ItemVolume"]
    weight_keys = ["weight", "Weight", "itemWeight", "ItemWeight"]

    collection = _pick_dict_value(
        package,
        ["items", "Items", "packageItems", "PackageItems", "itemNumbers", "ItemNumbers", "contents", "Contents"],
    )

    item_rows: list[dict[str, Any]] = []

    if isinstance(collection, list):
        for index, entry in enumerate(collection, start=1):
            if isinstance(entry, dict):
                item_number = normalize_item_number(_pick_dict_value(entry, item_number_keys, ""))
                quantity = to_float(_pick_dict_value(entry, quantity_keys, 0.0), 0.0)
                volume = to_float(_pick_dict_value(entry, volume_keys, 0.0), 0.0)
                weight = to_float(_pick_dict_value(entry, weight_keys, 0.0), 0.0)
            else:
                item_number = normalize_item_number(entry)
                quantity = 1.0 if item_number else 0.0
                volume = 0.0
                weight = 0.0

            if quantity <= 0 and (item_number or volume > 0 or weight > 0):
                quantity = 1.0
            if not item_number and (quantity > 0 or volume > 0 or weight > 0):
                item_number = f"Item {index}"
            if item_number:
                item_rows.append(
                    {
                        "Item Number": item_number,
                        "Quantity": quantity,
                        "Volume": volume,
                        "Weight": weight,
                    }
                )

    elif isinstance(collection, dict):
        for index, (raw_item_key, raw_item_value) in enumerate(collection.items(), start=1):
            item_number = normalize_item_number(raw_item_key)
            quantity = 0.0
            volume = 0.0
            weight = 0.0

            if isinstance(raw_item_value, dict):
                quantity = to_float(_pick_dict_value(raw_item_value, quantity_keys, 0.0), 0.0)
                volume = to_float(_pick_dict_value(raw_item_value, volume_keys, 0.0), 0.0)
                weight = to_float(_pick_dict_value(raw_item_value, weight_keys, 0.0), 0.0)
            else:
                quantity = to_float(raw_item_value, 0.0)

            if quantity <= 0 and (item_number or volume > 0 or weight > 0):
                quantity = 1.0
            if not item_number and (quantity > 0 or volume > 0 or weight > 0):
                item_number = f"Item {index}"
            if item_number:
                item_rows.append(
                    {
                        "Item Number": item_number,
                        "Quantity": quantity,
                        "Volume": volume,
                        "Weight": weight,
                    }
                )

    if not item_rows:
        single_item_number = normalize_item_number(_pick_dict_value(package, item_number_keys, ""))
        if single_item_number:
            quantity = to_float(_pick_dict_value(package, quantity_keys, 1.0), 1.0)
            volume = to_float(_pick_dict_value(package, volume_keys, 0.0), 0.0)
            weight = to_float(_pick_dict_value(package, weight_keys, 0.0), 0.0)
            item_rows.append(
                {
                    "Item Number": single_item_number,
                    "Quantity": max(1.0, quantity),
                    "Volume": volume,
                    "Weight": weight,
                }
            )

    return item_rows


def build_package_matrix_tables(api_response: Any) -> tuple[pd.DataFrame, dict[int, pd.DataFrame]]:
    candidates = _extract_package_candidates(api_response)
    if not candidates:
        return pd.DataFrame(columns=["Package Number", "Quantity", "Volume", "Weight"]), {}

    package_rows: list[dict[str, Any]] = []
    item_rows_by_package: dict[int, list[dict[str, Any]]] = {}

    for index, (raw_package_key, package) in enumerate(candidates, start=1):
        package_number = to_int(raw_package_key, -1)
        if package_number <= 0:
            package_number = to_int(
                _pick_dict_value(
                    package,
                    ["packageNumber", "PackageNumber", "package_number", "Package Number", "id", "Id"],
                    -1,
                ),
                -1,
            )
        if package_number <= 0:
            package_number = index

        item_rows = _extract_item_rows_from_package(package)
        package_quantity = to_float(
            _pick_dict_value(
                package,
                ["quantity", "Quantity", "qty", "Qty", "itemCount", "ItemCount", "itemsCount", "totalItems"],
                0.0,
            ),
            0.0,
        )
        package_volume = to_float(
            _pick_dict_value(package, ["volume", "Volume", "packageVolume", "totalVolume"], 0.0),
            0.0,
        )
        package_weight = to_float(
            _pick_dict_value(package, ["weight", "Weight", "packageWeight", "totalWeight"], 0.0),
            0.0,
        )

        if package_quantity <= 0:
            if item_rows:
                package_quantity = float(sum(to_float(item.get("Quantity"), 0.0) for item in item_rows))
            else:
                package_quantity = float(_extract_item_count(package))
        if package_volume <= 0 and item_rows:
            package_volume = float(sum(to_float(item.get("Volume"), 0.0) for item in item_rows))
        if package_weight <= 0 and item_rows:
            package_weight = float(sum(to_float(item.get("Weight"), 0.0) for item in item_rows))

        package_rows.append(
            {
                "Package Number": package_number,
                "Quantity": package_quantity,
                "Volume": package_volume,
                "Weight": package_weight,
            }
        )
        if item_rows:
            item_rows_by_package.setdefault(package_number, []).extend(item_rows)

    package_df = pd.DataFrame(package_rows)
    if package_df.empty:
        return pd.DataFrame(columns=["Package Number", "Quantity", "Volume", "Weight"]), {}

    package_df["Package Number"] = _safe_numeric(package_df["Package Number"]).fillna(0).astype(int)
    package_df["Quantity"] = _safe_numeric(package_df["Quantity"]).fillna(0.0)
    package_df["Volume"] = _safe_numeric(package_df["Volume"]).fillna(0.0)
    package_df["Weight"] = _safe_numeric(package_df["Weight"]).fillna(0.0)
    package_df = (
        package_df.groupby("Package Number", as_index=False)[["Quantity", "Volume", "Weight"]]
        .sum()
        .sort_values("Package Number")
        .reset_index(drop=True)
    )

    item_tables: dict[int, pd.DataFrame] = {}
    for package_number, item_rows in item_rows_by_package.items():
        if not item_rows:
            continue
        item_df = pd.DataFrame(item_rows)
        if item_df.empty:
            continue
        item_df["Item Number"] = item_df["Item Number"].fillna("").map(normalize_item_number)
        item_df["Quantity"] = _safe_numeric(item_df["Quantity"]).fillna(0.0)
        item_df["Volume"] = _safe_numeric(item_df["Volume"]).fillna(0.0)
        item_df["Weight"] = _safe_numeric(item_df["Weight"]).fillna(0.0)
        item_df = (
            item_df.groupby("Item Number", as_index=False)[["Quantity", "Volume", "Weight"]]
            .sum()
            .sort_values("Item Number")
            .reset_index(drop=True)
        )
        item_tables[package_number] = item_df

    return package_df, item_tables


def format_quantity_value(value: Any) -> str:
    numeric_value = to_float(value, 0.0)
    if abs(numeric_value - round(numeric_value)) < 1e-9:
        return f"{int(round(numeric_value)):,}"
    return f"{numeric_value:,.2f}"


def format_quantity_volume_weight_dataframe(
    df: pd.DataFrame,
    dimension_unit: str,
    weight_unit: str,
) -> pd.DataFrame:
    if df.empty:
        return df

    display_df = df.copy()
    if "Quantity" in display_df.columns:
        display_df["Quantity"] = _safe_numeric(display_df["Quantity"]).fillna(0.0).map(format_quantity_value)

    return format_measurement_dataframe(display_df, dimension_unit, weight_unit)


def _dimension_multiplier(dimension_unit: str) -> float:
    return 1.0 if dimension_unit == "in" else 2.54


def _weight_multiplier(weight_unit: str) -> float:
    return 1.0 if weight_unit == "lb" else 0.45359237


def _volume_multiplier(dimension_unit: str) -> float:
    # API volume is in cubic inches; metric display should be cubic meters.
    return 1.0 if dimension_unit == "in" else 0.000016387064


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def format_measurement_dataframe(
    df: pd.DataFrame,
    dimension_unit: str,
    weight_unit: str,
) -> pd.DataFrame:
    if df.empty:
        return df

    dim_label = "in" if dimension_unit == "in" else "cm"
    weight_label = "lb" if weight_unit == "lb" else "kg"
    volume_label = "in³" if dimension_unit == "in" else "m³"
    dim_multiplier = _dimension_multiplier(dimension_unit)
    weight_multiplier = _weight_multiplier(weight_unit)
    volume_multiplier = _volume_multiplier(dimension_unit)

    display_df = df.copy()

    dimension_columns = [
        "LengthInInches",
        "WidthInInches",
        "HeightInInches",
        "Length",
        "Width",
        "Height",
        "length",
        "width",
        "height",
    ]
    weight_columns = ["WeightInPounds", "weight", "Weight"]
    volume_columns = ["AverageVolume", "volume", "Volume"]

    for col in dimension_columns:
        if col in display_df.columns:
            display_df[col] = (_safe_numeric(display_df[col]) * dim_multiplier).round(2)
    for col in weight_columns:
        if col in display_df.columns:
            display_df[col] = (_safe_numeric(display_df[col]) * weight_multiplier).round(2)
    for col in volume_columns:
        if col in display_df.columns:
            display_df[col] = (_safe_numeric(display_df[col]) * volume_multiplier).round(2)

    rename_map: dict[str, str] = {}
    if "LengthInInches" in display_df.columns:
        rename_map["LengthInInches"] = f"Length ({dim_label})"
    if "WidthInInches" in display_df.columns:
        rename_map["WidthInInches"] = f"Width ({dim_label})"
    if "HeightInInches" in display_df.columns:
        rename_map["HeightInInches"] = f"Height ({dim_label})"
    if "Length" in display_df.columns and f"Length ({dim_label})" not in display_df.columns:
        rename_map["Length"] = f"Length ({dim_label})"
    if "Width" in display_df.columns and f"Width ({dim_label})" not in display_df.columns:
        rename_map["Width"] = f"Width ({dim_label})"
    if "Height" in display_df.columns and f"Height ({dim_label})" not in display_df.columns:
        rename_map["Height"] = f"Height ({dim_label})"
    if "WeightInPounds" in display_df.columns:
        rename_map["WeightInPounds"] = f"Weight ({weight_label})"
    if "AverageVolume" in display_df.columns:
        rename_map["AverageVolume"] = f"Volume ({volume_label})"
    if "length" in display_df.columns and f"Length ({dim_label})" not in display_df.columns:
        rename_map["length"] = f"Length ({dim_label})"
    if "width" in display_df.columns and f"Width ({dim_label})" not in display_df.columns:
        rename_map["width"] = f"Width ({dim_label})"
    if "height" in display_df.columns and f"Height ({dim_label})" not in display_df.columns:
        rename_map["height"] = f"Height ({dim_label})"
    if "weight" in display_df.columns and f"Weight ({weight_label})" not in display_df.columns:
        rename_map["weight"] = f"Weight ({weight_label})"
    if "Weight" in display_df.columns and f"Weight ({weight_label})" not in display_df.columns:
        rename_map["Weight"] = f"Weight ({weight_label})"
    if "volume" in display_df.columns and f"Volume ({volume_label})" not in display_df.columns:
        rename_map["volume"] = f"Volume ({volume_label})"
    if "Volume" in display_df.columns and f"Volume ({volume_label})" not in display_df.columns:
        rename_map["Volume"] = f"Volume ({volume_label})"

    return display_df.rename(columns=rename_map)


def strip_shipping_charge_fields(value: Any) -> Any:
    excluded_fields = {
        "computednewrate",
        "computedrate",
        "customershippingprice",
        "customizedflatmarkup",
        "discount",
        "grossprofit",
        "itemgrossprofit",
        "listnetcharge",
        "markuppercent",
        "matrixgrossprofit",
        "matrixratecharge",
        "maxratecharge",
        "minratecharge",
        "netcharge",
        "originalgrossprofit",
        "productamountcharged",
        "productgrossprofit",
        "rate",
        "shippingnetcharge",
        "targetgrossprofit",
    }
    if isinstance(value, dict):
        return {
            key: strip_shipping_charge_fields(item)
            for key, item in value.items()
            if normalize_col_name(str(key)) not in excluded_fields
        }
    if isinstance(value, list):
        return [strip_shipping_charge_fields(item) for item in value]
    return value


# ============================================================
# PIPELINE
# ============================================================
def run_pipeline(standard_input_df: pd.DataFrame) -> dict[str, Any]:
    clean_df, row_errors = validate_and_aggregate_rows(standard_input_df)
    if clean_df.empty:
        return {
            "requested_df": pd.DataFrame(columns=["ItemNumber", "Quantity"]),
            "matched_df": pd.DataFrame(),
            "unmatched_df": pd.DataFrame(columns=["ItemNumber", "Quantity"]),
            "row_errors": row_errors or ["No valid rows found after validation."],
        }

    matched_df, unmatched_df = fetch_item_info_rows(clean_df)

    LOGGER.info(
        "Packaging load complete | requested=%s matched_rows=%s unmatched_items=%s errors=%s",
        len(clean_df),
        len(matched_df),
        len(unmatched_df),
        len(row_errors),
    )

    return {
        "requested_df": clean_df,
        "matched_df": matched_df,
        "unmatched_df": unmatched_df,
        "row_errors": row_errors,
    }


def reset_estimation_results(results: dict[str, Any]) -> dict[str, Any]:
    updated_results = dict(results)
    updated_results.update(
        {
            "shipping_items": [],
            "shipping_calc_payload": {},
            "shipping_calc_response": None,
            "shipping_calc_error": "",
            "shipping_source_candidates": [],
            "selected_source_candidate": {},
            "destination_mode": "",
            "destination_label": "",
            "destination_warehouse_number": "",
            "destination_warehouse_details": {},
            "shipping_address": {},
            "shipping_request_options": {},
            "source_warehouse_number": "",
            "source_warehouse_details": {},
            "destination_state": "",
        }
    )
    return updated_results


def run_estimation_for_results(
    base_results: dict[str, Any],
    destination_mode: str,
    shipping_address: dict[str, Any],
    destination_label: str = "",
    destination_warehouse_number: int | None = None,
    destination_warehouse_details: dict[str, str] | None = None,
    shipping_request_options: dict[str, Any] | None = None,
    perishable_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    validated_address = validate_shipping_address(shipping_address)
    destination_state = str(validated_address.get("State", "")).strip().upper()
    if not destination_state:
        raise ValueError("The selected destination is missing a destination state.")

    requested_df = base_results.get("requested_df", pd.DataFrame(columns=["ItemNumber", "Quantity"]))
    matched_df = base_results.get("matched_df", pd.DataFrame())
    if matched_df.empty:
        matched_df = base_results.get("verified_df", pd.DataFrame())

    shipping_items = build_shipping_item_records(
        matched_df,
        requested_df,
        perishable_type_overrides=perishable_overrides,
    )

    updated_results = reset_estimation_results(base_results)
    updated_results["shipping_items"] = shipping_items
    updated_results["destination_mode"] = str(destination_mode).strip()
    updated_results["destination_label"] = str(destination_label).strip() or format_shipping_address(validated_address)
    updated_results["destination_warehouse_number"] = (
        int(destination_warehouse_number) if destination_warehouse_number is not None else ""
    )
    updated_results["destination_warehouse_details"] = destination_warehouse_details or {}
    updated_results["shipping_address"] = validated_address
    updated_results["shipping_request_options"] = build_shipping_request_options(shipping_request_options)
    updated_results["destination_state"] = destination_state
    updated_results["perishable_overrides"] = perishable_overrides or {}

    if not shipping_items:
        return updated_results

    shipping_calc_payload = build_shipping_calculator_payload(
        shipping_items,
        validated_address,
        shipping_request_options=shipping_request_options,
    )
    shipping_calc_response, shipping_calc_error = call_shipping_calculator_api(shipping_calc_payload)
    shipping_source_candidates = extract_shipping_source_candidates(shipping_calc_response)
    selected_source_candidate = select_shipping_source_candidate(shipping_source_candidates)
    source_warehouse_number = (
        normalize_warehouse_number(selected_source_candidate.get("Warehouse Number"))
        if selected_source_candidate
        else None
    )
    source_warehouse_details = build_source_warehouse_details(selected_source_candidate)

    updated_results["shipping_calc_payload"] = shipping_calc_payload
    updated_results["shipping_calc_response"] = shipping_calc_response
    updated_results["shipping_calc_error"] = shipping_calc_error
    updated_results["shipping_source_candidates"] = shipping_source_candidates
    updated_results["selected_source_candidate"] = selected_source_candidate or {}
    updated_results["source_warehouse_number"] = source_warehouse_number or ""
    updated_results["source_warehouse_details"] = source_warehouse_details
    return updated_results


def build_all_items_summary_dataframe(
    package_item_tables: dict[int, pd.DataFrame],
    shipping_items: list[dict[str, Any]],
) -> pd.DataFrame:
    all_item_frames: list[pd.DataFrame] = []

    for item_df in package_item_tables.values():
        if item_df.empty:
            continue
        all_item_frames.append(item_df[["Item Number", "Quantity", "Volume", "Weight"]].copy())

    if all_item_frames:
        combined_df = pd.concat(all_item_frames, ignore_index=True)
        combined_df["Quantity"] = _safe_numeric(combined_df["Quantity"]).fillna(0.0)
        combined_df["Volume"] = _safe_numeric(combined_df["Volume"]).fillna(0.0)
        combined_df["Weight"] = _safe_numeric(combined_df["Weight"]).fillna(0.0)
        return (
            combined_df.groupby("Item Number", as_index=False)[["Quantity", "Volume", "Weight"]]
            .sum()
            .sort_values("Item Number")
            .reset_index(drop=True)
        )

    if not shipping_items:
        return pd.DataFrame(columns=["Item Number", "Quantity", "Volume", "Weight"])

    fallback_df = pd.DataFrame(
        [
            {
                "Item Number": item.get("item_number", ""),
                "Quantity": item.get("quantity", 0.0),
                "Volume": item.get("volume", 0.0),
                "Weight": item.get("weight", 0.0),
            }
            for item in shipping_items
        ]
    )
    if fallback_df.empty:
        return pd.DataFrame(columns=["Item Number", "Quantity", "Volume", "Weight"])

    fallback_df["Quantity"] = _safe_numeric(fallback_df["Quantity"]).fillna(0.0)
    fallback_df["Volume"] = _safe_numeric(fallback_df["Volume"]).fillna(0.0)
    fallback_df["Weight"] = _safe_numeric(fallback_df["Weight"]).fillna(0.0)
    return (
        fallback_df.groupby("Item Number", as_index=False)[["Quantity", "Volume", "Weight"]]
        .sum()
        .sort_values("Item Number")
        .reset_index(drop=True)
    )


def build_shipping_recommendation_signature(
    results: dict[str, Any],
    shipping_request_options: dict[str, Any] | None,
    selected_method: dict[str, Any] | None,
    perishable_overrides: dict[str, str] | None = None,
) -> str:
    requested_df = results.get("requested_df", pd.DataFrame(columns=["ItemNumber", "Quantity"]))
    requested_rows: list[dict[str, Any]] = []
    if not requested_df.empty:
        requested_rows = (
            requested_df[["ItemNumber", "Quantity"]]
            .fillna("")
            .sort_values("ItemNumber")
            .to_dict(orient="records")
        )

    payload = {
        "requested_rows": requested_rows,
        "overrides": sorted((perishable_overrides or {}).items()),
        "shipping_request_options": build_shipping_request_options(shipping_request_options),
        "selected_method_key": build_shipping_method_key(selected_method),
    }
    return json.dumps(payload, sort_keys=True)


def build_shipping_simulation_summary_row(
    simulation_results: dict[str, Any],
    selected_method: dict[str, Any] | None,
) -> dict[str, Any]:
    shipping_candidates = simulation_results.get("shipping_source_candidates", [])
    destination_warehouse_number = normalize_warehouse_number(
        simulation_results.get("destination_warehouse_number")
    )
    method_candidates = filter_shipping_candidates_by_method(shipping_candidates, selected_method)
    if not method_candidates:
        method_candidates = list(shipping_candidates)
    internal_candidates = filter_internal_transfer_candidates(
        method_candidates,
        destination_warehouse_number,
    )
    chosen_candidate = select_shipping_source_candidate(internal_candidates)
    chosen_source_details = (
        build_source_warehouse_details(chosen_candidate)
        if chosen_candidate
        else simulation_results.get("source_warehouse_details", {}) or {}
    )
    package_details_df, _ = build_shipping_method_package_tables(
        simulation_results.get("shipping_calc_response"),
        shipping_candidates,
        selected_method,
        destination_warehouse_number=destination_warehouse_number,
    )
    shipping_cost = calculate_internal_shipping_cost(
        selected_method,
        shipping_candidates,
        destination_warehouse_number,
    )
    selected_method_name = (
        str(selected_method.get("Method Name", "")).strip()
        if isinstance(selected_method, dict)
        else ""
    )

    total_packages = int(len(package_details_df)) if not package_details_df.empty else 0
    total_volume = (
        float(_safe_numeric(package_details_df.get("Volume", pd.Series(dtype="float64"))).fillna(0.0).sum())
        if not package_details_df.empty
        else 0.0
    )
    total_weight = (
        float(_safe_numeric(package_details_df.get("Weight", pd.Series(dtype="float64"))).fillna(0.0).sum())
        if not package_details_df.empty
        else 0.0
    )
    destination_details = simulation_results.get("destination_warehouse_details", {}) or {}

    return {
        "Destination Warehouse": simulation_results.get("destination_warehouse_number", ""),
        "Destination Location": format_warehouse_option(destination_details),
        "Source Warehouse": (
            chosen_candidate.get("Warehouse Number", "")
            if chosen_candidate
            else simulation_results.get("source_warehouse_number", "")
        ),
        "Source Location": format_warehouse_option(chosen_source_details),
        "Method": (
            str(chosen_candidate.get("Method", "")).strip()
            if chosen_candidate
            else selected_method_name or "No transfer required"
        ),
        "Option Type": str(chosen_candidate.get("Option Type", "")).strip() if chosen_candidate else "",
        "Delivery Days": to_int(chosen_candidate.get("Delivery Days"), 0) if chosen_candidate else 0,
        "Shipping Cost": shipping_cost,
        "Packages": total_packages,
        "Total Volume": total_volume,
        "Total Weight": total_weight,
        "Error": str(simulation_results.get("shipping_calc_error", "") or ""),
    }


def simulate_destination_warehouse_recommendation(
    base_results: dict[str, Any],
    destination_warehouse_number: int,
    shipping_request_options: dict[str, Any] | None,
    selected_method: dict[str, Any] | None,
    perishable_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    destination_warehouse_details = find_warehouse_details(destination_warehouse_number) or {}
    shipping_address = build_shipping_address_from_warehouse(destination_warehouse_details)
    simulation_results = run_estimation_for_results(
        base_results,
        destination_mode=DESTINATION_MODE_WAREHOUSE,
        shipping_address=shipping_address,
        destination_label=format_warehouse_details(destination_warehouse_details),
        destination_warehouse_number=destination_warehouse_number,
        destination_warehouse_details=destination_warehouse_details,
        shipping_request_options=shipping_request_options,
        perishable_overrides=perishable_overrides,
    )
    return build_shipping_simulation_summary_row(simulation_results, selected_method)


def run_destination_warehouse_simulations(
    base_results: dict[str, Any],
    destination_warehouse_numbers: list[int],
    shipping_request_options: dict[str, Any] | None,
    selected_method: dict[str, Any] | None,
    perishable_overrides: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    if not destination_warehouse_numbers:
        return []

    simulations: list[dict[str, Any]] = []
    max_workers = min(6, max(1, len(destination_warehouse_numbers)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                simulate_destination_warehouse_recommendation,
                base_results,
                warehouse_number,
                shipping_request_options,
                selected_method,
                perishable_overrides,
            ): warehouse_number
            for warehouse_number in destination_warehouse_numbers
        }
        for future in as_completed(future_map):
            warehouse_number = future_map[future]
            try:
                simulations.append(future.result())
            except Exception as exc:
                simulations.append(
                    {
                        "Destination Warehouse": warehouse_number,
                        "Destination Location": "",
                        "Source Warehouse": "",
                        "Source Location": "",
                        "Method": "",
                        "Option Type": "",
                        "Delivery Days": 0,
                        "Shipping Cost": 0.0,
                        "Packages": 0,
                        "Total Volume": 0.0,
                        "Total Weight": 0.0,
                        "Error": str(exc),
                    }
                )

    return sorted(simulations, key=lambda row: (to_int(row.get("Destination Warehouse"), 999999),))


def pick_cheapest_destination_recommendation(
    simulations: list[dict[str, Any]],
    current_destination_warehouse_number: int | None,
) -> dict[str, Any] | None:
    valid_rows = [
        row
        for row in simulations
        if not str(row.get("Error", "")).strip()
        and to_int(row.get("Destination Warehouse"), 0) != to_int(current_destination_warehouse_number, 0)
    ]
    if not valid_rows:
        return None

    return min(
        valid_rows,
        key=lambda row: (
            to_float(row.get("Shipping Cost"), float("inf")),
            to_int(row.get("Delivery Days"), 9999),
            to_int(row.get("Packages"), 9999),
            to_int(row.get("Destination Warehouse"), 999999),
        ),
    )


def pick_best_destination_recommendation(
    simulations: list[dict[str, Any]],
    current_destination_warehouse_number: int | None,
) -> dict[str, Any] | None:
    valid_rows = [
        row
        for row in simulations
        if not str(row.get("Error", "")).strip()
        and to_int(row.get("Destination Warehouse"), 0) != to_int(current_destination_warehouse_number, 0)
    ]
    if not valid_rows:
        return None

    ranking_df = pd.DataFrame(valid_rows).copy()
    ranking_df["PackagesRank"] = (
        pd.to_numeric(ranking_df.get("Packages"), errors="coerce")
        .fillna(9999)
        .rank(method="min", ascending=True)
    )
    ranking_df["ShippingCostRank"] = (
        pd.to_numeric(ranking_df.get("Shipping Cost"), errors="coerce")
        .fillna(float("inf"))
        .rank(method="min", ascending=True)
    )
    ranking_df["DeliveryDaysRank"] = (
        pd.to_numeric(ranking_df.get("Delivery Days"), errors="coerce")
        .fillna(9999)
        .rank(method="min", ascending=True)
    )
    ranking_df["CombinedRankScore"] = (
        ranking_df["PackagesRank"] + ranking_df["ShippingCostRank"] + ranking_df["DeliveryDaysRank"]
    )
    ranking_df["DestinationWarehouseSort"] = pd.to_numeric(
        ranking_df.get("Destination Warehouse"), errors="coerce"
    ).fillna(999999)

    return (
        ranking_df.sort_values(
            by=[
                "CombinedRankScore",
                "PackagesRank",
                "ShippingCostRank",
                "DeliveryDaysRank",
                "DestinationWarehouseSort",
            ],
            ascending=True,
            kind="stable",
        )
        .iloc[0]
        .to_dict()
    )


# ============================================================
# UI: INPUT
# ============================================================
input_col, spacer_col, preview_col = st.columns([1, 0.08, 1])
standard_input_df = pd.DataFrame(columns=["ItemNumber", "Quantity", "_RowNumber"])
input_parse_errors: list[str] = []
uploaded_df = pd.DataFrame()
upload_columns: list[str] = []
upload_has_valid_columns = False
default_item_idx = 0
default_qty_idx = 1
uploaded_file_bytes = b""
uploaded_file_ext = ""
excel_sheet_names: list[str] = []

with spacer_col:
    st.write("")

with input_col:
    st.subheader("Input", anchor=False)
    input_mode = st.radio(
        "Input Mode",
        [INPUT_MODE_UPLOAD, INPUT_MODE_PASTE],
        horizontal=True,
    )
    if st.session_state.get("_pe_last_input_mode") != input_mode:
        st.session_state._pe_last_input_mode = input_mode
        LOGGER.info("Input mode changed | mode='%s'", input_mode)

    if input_mode == INPUT_MODE_UPLOAD:
        uploaded_file = st.file_uploader("Upload file", type=["xlsx", "xls", "csv"])

        if uploaded_file is not None:
            uploaded_file_bytes = uploaded_file.getvalue()
            uploaded_file_ext = uploaded_file.name.lower().rsplit(".", 1)[-1] if "." in uploaded_file.name else ""
            file_signature = (uploaded_file.name, len(uploaded_file_bytes))
            if st.session_state.get("_pe_last_upload_signature") != file_signature:
                st.session_state._pe_last_upload_signature = file_signature
                LOGGER.info(
                    "File uploaded | name='%s' type='%s' size_bytes=%s",
                    uploaded_file.name,
                    uploaded_file_ext or "unknown",
                    len(uploaded_file_bytes),
                )

            try:
                if uploaded_file_ext in {"xlsx", "xls"}:
                    excel_sheet_names = read_excel_sheet_names(uploaded_file_bytes)
                elif uploaded_file_ext == "csv":
                    pass
                else:
                    input_parse_errors.append("Unsupported file type. Please upload .xlsx, .xls, or .csv.")
            except Exception as exc:
                uploaded_df = pd.DataFrame()
                LOGGER.exception("Could not inspect uploaded file: %s", exc)
                input_parse_errors.append(f"Could not inspect uploaded file: {exc}")
        else:
            st.caption("Upload an Excel or CSV file with item and quantity columns, then click Load.")

    else:
        pasted_text = st.text_area(
            "Paste ItemNumber and Quantity rows",
            height=220,
            placeholder="871WAR1913F\t5\n000123ABC\t2\n",
        )
        standard_input_df, input_parse_errors = parse_pasted_input(pasted_text)
        st.caption("Paste tab-separated rows copied from Excel. Empty lines are ignored.")

with preview_col:
    st.subheader("Preview", anchor=False)
    if input_mode == INPUT_MODE_UPLOAD and uploaded_file is not None:
        selected_sheet_name = ""

        if uploaded_file_ext in {"xlsx", "xls"}:
            if not excel_sheet_names:
                st.error("No worksheet found in the uploaded Excel file.")
            else:
                selected_sheet_name = st.selectbox(
                    "Worksheet",
                    options=excel_sheet_names,
                    index=0,
                    key="pe_sheet_name",
                )
                if st.session_state.get("_pe_last_sheet_name") != selected_sheet_name:
                    st.session_state._pe_last_sheet_name = selected_sheet_name
                    LOGGER.info("Worksheet selected | sheet='%s'", selected_sheet_name)
                try:
                    uploaded_df = read_excel_bytes(uploaded_file_bytes, selected_sheet_name)
                    uploaded_df.columns = [str(col).strip() for col in uploaded_df.columns]
                except Exception as exc:
                    uploaded_df = pd.DataFrame()
                    LOGGER.exception("Could not read worksheet '%s': %s", selected_sheet_name, exc)
                    input_parse_errors.append(f"Could not read worksheet '{selected_sheet_name}': {exc}")
        elif uploaded_file_ext == "csv":
            try:
                uploaded_df = read_csv_bytes(uploaded_file_bytes)
                uploaded_df.columns = [str(col).strip() for col in uploaded_df.columns]
            except Exception as exc:
                uploaded_df = pd.DataFrame()
                LOGGER.exception("Could not read uploaded CSV file: %s", exc)
                input_parse_errors.append(f"Could not read CSV file: {exc}")

        if uploaded_df.empty:
            st.info("Uploaded file has no rows.")
        elif len(uploaded_df.columns) < 2:
            st.error("Input must contain at least two columns: Item Number and Quantity.")
        else:
            upload_columns = uploaded_df.columns.tolist()
            default_item_idx = find_default_column(
                columns=upload_columns,
                targets={"itemnumber", "item", "itemno", "sku"},
                fallback_index=0,
            )
            default_qty_idx = find_default_column(
                columns=upload_columns,
                targets={"quantity", "qty"},
                fallback_index=1,
            )
            if default_qty_idx == default_item_idx and len(upload_columns) > 1:
                default_qty_idx = 1 if default_item_idx == 0 else 0
            upload_has_valid_columns = True
            if st.session_state.get("_pe_last_upload_rows") != len(uploaded_df):
                st.session_state._pe_last_upload_rows = len(uploaded_df)
                LOGGER.info(
                    "Input table ready | rows=%s columns=%s",
                    len(uploaded_df),
                    len(upload_columns),
                )

    if input_mode == INPUT_MODE_UPLOAD and upload_has_valid_columns:
        col_left, col_right = st.columns(2)
        with col_left:
            item_column = st.selectbox(
                "Item Number Column",
                options=upload_columns,
                index=default_item_idx,
                key="pe_item_column",
            )
        with col_right:
            qty_column = st.selectbox(
                "Quantity Column",
                options=upload_columns,
                index=default_qty_idx,
                key="pe_qty_column",
            )

        standard_input_df = uploaded_df[[item_column, qty_column]].copy()
        standard_input_df.columns = ["ItemNumber", "Quantity"]
        standard_input_df["_RowNumber"] = standard_input_df.index + 2
        selected_columns_signature = (item_column, qty_column)
        if st.session_state.get("_pe_last_column_selection") != selected_columns_signature:
            st.session_state._pe_last_column_selection = selected_columns_signature
            LOGGER.info(
                "Column mapping updated | item_col='%s' qty_col='%s'",
                item_column,
                qty_column,
            )

    if standard_input_df.empty:
        st.info("Preview will appear here after input is provided.")
    else:
        preview_df, _ = validate_and_aggregate_rows(standard_input_df)
        preview_display_df = (
            preview_df[["ItemNumber", "Quantity"]].copy()
            if not preview_df.empty
            else standard_input_df[["ItemNumber", "Quantity"]].copy()
        )
        st.caption(
            f"Preview shows the first 5 aggregated items only. {len(preview_display_df):,} item(s) are currently in scope."
        )
        st.dataframe(
            preview_display_df.head(5),
            width="stretch",
            hide_index=True,
        )


with input_col:
    has_input_rows = not standard_input_df.empty
    perishable_overrides: dict[str, str] = {}

    if not has_input_rows:
        st.session_state.pe_refrigeration_required = False
        st.session_state.pe_perishable_rows = [{"item_number": "", "perishable_code": ""}]
    else:
        uploaded_item_numbers = get_uploaded_item_numbers(standard_input_df)
        needs_refrigeration = st.toggle(
            "Do items need refrigeration?",
            key="pe_refrigeration_required",
        )

        if needs_refrigeration:
            valid_item_numbers = set(uploaded_item_numbers)
            perishable_rows = normalize_perishable_rows(
                st.session_state.get("pe_perishable_rows", []),
                valid_item_numbers,
            )

            if not uploaded_item_numbers:
                st.session_state.pe_perishable_rows = [{"item_number": "", "perishable_code": ""}]
                st.info("No uploaded item numbers available.")
            else:
                rows_before_render = len(perishable_rows)
                for idx, row in enumerate(perishable_rows):
                    row_item_col, row_type_col = st.columns([1.4, 1.3])
                    item_options = [""] + uploaded_item_numbers
                    current_item = row.get("item_number", "")
                    if current_item not in item_options:
                        current_item = ""
                    item_index = item_options.index(current_item)

                    type_options = [""] + PERISHABLE_CODE_OPTIONS
                    current_type = row.get("perishable_code", "")
                    if current_type not in type_options:
                        current_type = ""
                    type_index = type_options.index(current_type)

                    with row_item_col:
                        selected_item = st.selectbox(
                            f"Perishable Item {idx + 1}",
                            options=item_options,
                            index=item_index,
                            format_func=lambda item: "Select item..." if item == "" else item,
                            key=f"pe_perishable_item_{idx}",
                            label_visibility="collapsed",
                        )
                    with row_type_col:
                        selected_type = st.selectbox(
                            f"Perishable Type {idx + 1}",
                            options=type_options,
                            index=type_index,
                            format_func=lambda code: (
                                "Select type..."
                                if code == ""
                                else f"{PERISHABLE_CODE_TO_LABEL.get(code, code)} ({code})"
                            ),
                            key=f"pe_perishable_type_{idx}",
                            label_visibility="collapsed",
                        )

                    perishable_rows[idx] = {
                        "item_number": normalize_item_number(selected_item),
                        "perishable_code": str(selected_type).strip().upper(),
                    }

                perishable_rows = normalize_perishable_rows(perishable_rows, valid_item_numbers)
                st.session_state.pe_perishable_rows = perishable_rows
                if len(perishable_rows) > rows_before_render:
                    st.rerun()

                perishable_overrides = build_perishable_override_map(perishable_rows)

                selected_override_items = [
                    row["item_number"]
                    for row in perishable_rows
                    if row.get("item_number") and row.get("perishable_code")
                ]
                if len(selected_override_items) != len(set(selected_override_items)):
                    st.warning(
                        "Duplicate item selections detected. The last selection for each item will be used."
                    )
                if perishable_overrides:
                    st.caption(f"{len(perishable_overrides)} item(s) configured with perishable overrides.")

    load_disabled = not has_input_rows
    if st.button("Load Items", type="primary", width="content", disabled=load_disabled):
        LOGGER.info(
            "Item load requested | mode='%s' rows=%s refrigeration=%s overrides=%s",
            input_mode,
            len(standard_input_df),
            bool(st.session_state.get("pe_refrigeration_required", False)),
            len(perishable_overrides),
        )
        try:
            pipeline_results = run_pipeline(standard_input_df)
            requested_df = pipeline_results.get("requested_df", pd.DataFrame(columns=["ItemNumber", "Quantity"]))
            matched_df = pipeline_results.get("matched_df", pd.DataFrame())
            unmatched_df = pipeline_results.get("unmatched_df", pd.DataFrame(columns=["ItemNumber", "Quantity"]))
            verified_df, non_verified_df = split_verified_and_non_verified(matched_df, unmatched_df)

            pipeline_results["verified_df"] = verified_df
            pipeline_results["non_verified_df"] = non_verified_df
            pipeline_results["perishable_overrides"] = perishable_overrides
            pipeline_results = reset_estimation_results(pipeline_results)
            combined_errors = input_parse_errors + pipeline_results.get("row_errors", [])

            st.session_state.pe_loaded = True
            st.session_state.pe_results = pipeline_results
            st.session_state.pe_errors = combined_errors
            st.session_state.pe_selected_package_view = "All Items"
            if st.session_state.pe_recommendation_future is not None:
                st.session_state.pe_recommendation_future.cancel()
            st.session_state.pe_recommendation_future = None
            st.session_state.pe_recommendation_signature = ""
            st.session_state.pe_recommendation_results = []
            LOGGER.info(
                "Item load complete | requested=%s matched=%s unmatched=%s verified=%s overrides=%s parse_errors=%s",
                len(requested_df),
                len(matched_df),
                len(unmatched_df),
                len(verified_df),
                len(perishable_overrides),
                len(combined_errors),
            )

        except Exception as exc:
            LOGGER.exception("Item lookup failed: %s", exc)
            st.session_state.pe_loaded = False
            st.session_state.pe_results = {}
            st.session_state.pe_errors = [f"Lookup failed: {exc}"]


# ============================================================
# UI: OUTPUT
# ============================================================
if st.session_state.pe_errors:
    st.error(
        f"{len(st.session_state.pe_errors)} row(s) were rejected or could not be parsed. "
        "Review details below."
    )
    with st.expander("Validation details", expanded=False):
        for msg in st.session_state.pe_errors:
            st.write(f"- {msg}")

if st.session_state.pe_loaded and st.session_state.pe_results:
    results = st.session_state.pe_results
    requested_df = results.get("requested_df", pd.DataFrame(columns=["ItemNumber", "Quantity"]))
    matched_df = results.get("matched_df", pd.DataFrame())
    unmatched_df = results.get("unmatched_df", pd.DataFrame(columns=["ItemNumber", "Quantity"]))
    verified_df = results.get("verified_df", pd.DataFrame())
    non_verified_df = results.get("non_verified_df", pd.DataFrame())
    shipping_items = results.get("shipping_items", results.get("verified_items", []))
    shipping_calc_error = str(results.get("shipping_calc_error", "") or "")
    shipping_source_candidates = results.get("shipping_source_candidates", [])
    selected_source_candidate = results.get("selected_source_candidate", {})
    shipping_calc_payload = results.get("shipping_calc_payload", {})
    shipping_calc_response = results.get("shipping_calc_response", None)
    stored_destination_mode = str(results.get("destination_mode", "") or "")
    stored_destination_label = str(results.get("destination_label", "") or "")
    stored_destination_warehouse_number = results.get("destination_warehouse_number", "")
    stored_destination_warehouse_details = results.get("destination_warehouse_details", {})
    stored_shipping_address = results.get("shipping_address", {})
    stored_shipping_request_options = results.get("shipping_request_options", {})
    destination_state = str(results.get("destination_state", ""))
    source_warehouse_number = results.get("source_warehouse_number", "")
    source_warehouse_details = results.get("source_warehouse_details", {})
    current_destination_warehouse_number = normalize_warehouse_number(stored_destination_warehouse_number)
    ui_cfg = PAGE_CONFIG.get("ui", {})
    shipping_cfg = PAGE_CONFIG.get("shipping_calculator_api", {})
    default_warehouse = int(ui_cfg.get("default_warehouse", 105) or 105)
    warehouse_options, warehouse_labels = get_available_warehouse_options()
    default_destination_state = str(ui_cfg.get("default_destination_state", "FL")).strip() or "FL"
    default_company_type = str(shipping_cfg.get("company_type", "Warehouse")).strip() or "Warehouse"
    dimension_unit = str(st.session_state.get("pe_dimension_unit", "in"))
    weight_unit = str(st.session_state.get("pe_weight_unit", "lb"))
    volume_label = "in³" if dimension_unit == "in" else "m³"

    units_col_left, units_col_right = st.columns(2)
    with units_col_left:
        dimension_unit = st.radio(
            "Dimensions Unit",
            options=["in", "cm"],
            format_func=lambda u: "Inches (in)" if u == "in" else "Centimeters (cm)",
            horizontal=True,
            key="pe_dimension_unit",
        )
    with units_col_right:
        weight_unit = st.radio(
            "Weight Unit",
            options=["lb", "kg"],
            format_func=lambda u: "Pounds (lb)" if u == "lb" else "Kilograms (kg)",
            horizontal=True,
            key="pe_weight_unit",
        )

    st.divider()

    verified_count = len(verified_df)
    non_verified_count = max(0, len(matched_df) - len(verified_df)) + len(unmatched_df)

    verified_col, mid, non_verified_col = st.columns([5,0.5,5])
    with verified_col:
        verified_label_col, verified_count_col = st.columns([2.2, 1.0])
        with verified_label_col:
            st.markdown("**Verified Items**")
        with verified_count_col:
            st.markdown(f"**Count:** `{verified_count:,}`")

        if verified_df.empty:
            st.info("No verified items found.")
        else:
            st.dataframe(
                format_measurement_dataframe(verified_df, dimension_unit, weight_unit),
                width="stretch",
                hide_index=True,
            )

    with non_verified_col:
        non_verified_label_col, non_verified_count_col = st.columns([2.2, 1.0])
        with non_verified_label_col:
            st.markdown("**Non-Verified Items**")
        with non_verified_count_col:
            st.markdown(f"**Count:** `{non_verified_count:,}`")

        if non_verified_df.empty:
            st.info("No non-verified items found.")
        else:
            st.dataframe(
                format_measurement_dataframe(non_verified_df, dimension_unit, weight_unit),
                width="stretch",
                hide_index=True,
            )

    if not matched_df.empty:
        st.caption("Verified and non-verified tables are shown for reference only. The shipping calculator runs on all matched item records.")

    st.divider()
    st.subheader("Shipping Destination", anchor=False)
    destination_left_col, destination_right_col = st.columns([1.35, 1], vertical_alignment="top")
    destination_mode_options = [DESTINATION_MODE_WAREHOUSE, DESTINATION_MODE_ADDRESS]
    default_destination_mode = (
        stored_destination_mode
        if stored_destination_mode in destination_mode_options
        else DESTINATION_MODE_WAREHOUSE
        if warehouse_options
        else DESTINATION_MODE_ADDRESS
    )

    selected_destination_warehouse_number: int | None = None
    selected_destination_warehouse_details: dict[str, str] = {}

    with destination_left_col:
        selected_destination_mode = st.radio(
            "Ship To",
            options=destination_mode_options,
            index=destination_mode_options.index(default_destination_mode),
            horizontal=True,
            key="pe_destination_mode",
        )

        if selected_destination_mode == DESTINATION_MODE_WAREHOUSE:
            if warehouse_options:
                selected_warehouse_default = (
                    int(stored_destination_warehouse_number)
                    if str(stored_destination_warehouse_number).strip()
                    and int(stored_destination_warehouse_number) in warehouse_options
                    else default_warehouse
                    if default_warehouse in warehouse_options
                    else warehouse_options[0]
                )
                selected_default_index = warehouse_options.index(selected_warehouse_default)
                selected_destination_warehouse_number = int(
                    st.selectbox(
                        "Choose the warehouse destination",
                        options=warehouse_options,
                        index=selected_default_index,
                        format_func=lambda warehouse: warehouse_labels.get(int(warehouse), str(warehouse)),
                        key="pe_destination_warehouse",
                    )
                )
                selected_destination_warehouse_details = (
                    find_warehouse_details(selected_destination_warehouse_number) or {}
                )
                if selected_destination_warehouse_details:
                    st.caption(
                        f"Selected destination: {format_warehouse_details(selected_destination_warehouse_details)}"
                    )
                else:
                    st.warning("The selected warehouse destination could not be resolved from the warehouse lookup.")
            else:
                st.warning("Warehouses parquet could not be loaded, so warehouse destinations are unavailable.")

    normalized_stored_address = normalize_shipping_address(
        stored_shipping_address,
        company_type_default="",
    )
    manual_country = normalized_stored_address.get("Country", "US") or "US"
    manual_address_line1 = normalized_stored_address.get("StreetAddress1", "")
    manual_address_line2 = normalized_stored_address.get("StreetAddress2", "")
    manual_city = normalized_stored_address.get("City", "")
    manual_state = normalized_stored_address.get("State", "")
    manual_zip_code = normalized_stored_address.get("ZipCode", "")
    manual_is_commercial = bool(normalized_stored_address.get("IsCommercial", True))
    manual_is_restricted = bool(normalized_stored_address.get("IsRestrictedExpeditedShipping", False))

    with destination_left_col:
        if selected_destination_mode == DESTINATION_MODE_ADDRESS:
            manual_top_col, manual_mid_col, manual_right_col = st.columns(3)
            with manual_top_col:
                manual_address_line1 = st.text_input(
                    "Street Address 1",
                    value=manual_address_line1,
                    key="pe_ship_street1",
                )
            with manual_mid_col:
                manual_address_line2 = st.text_input(
                    "Street Address 2",
                    value=manual_address_line2,
                    key="pe_ship_street2",
                )
            with manual_right_col:
                manual_city = st.text_input(
                    "City",
                    value=manual_city,
                    key="pe_ship_city",
                )

            manual_bottom_left, manual_bottom_mid, manual_bottom_right = st.columns(3)
            with manual_bottom_left:
                manual_state_fallback = manual_state if manual_state in US_STATE_CODES else default_destination_state
                normalized_manual_country = normalize_country_code(manual_country or "US")
                if normalized_manual_country == "US":
                    manual_state = st.selectbox(
                        "State",
                        options=US_STATE_CODES,
                        index=US_STATE_CODES.index(manual_state_fallback),
                        key="pe_ship_state_us",
                    )
                else:
                    manual_state = st.text_input(
                        "State / Province",
                        value=manual_state,
                        key="pe_ship_state_other",
                    ).strip().upper()
            with manual_bottom_mid:
                manual_zip_code = st.text_input(
                    "ZIP / Postal Code",
                    value=manual_zip_code,
                    key="pe_ship_zip",
                )
            with manual_bottom_right:
                manual_country = st.text_input(
                    "Country",
                    value=manual_country,
                    key="pe_ship_country",
                ).strip().upper()
                normalized_manual_country = normalize_country_code(manual_country or "US")

            manual_option_left, manual_option_right = st.columns(2)
            with manual_option_left:
                manual_is_commercial = st.toggle(
                    "Commercial Address",
                    value=manual_is_commercial,
                    key="pe_ship_is_commercial",
                )
            with manual_option_right:
                manual_is_restricted = st.toggle(
                    "Restricted Expedited Shipping",
                    value=manual_is_restricted,
                    key="pe_ship_is_restricted",
                )
        else:
            normalized_manual_country = normalize_country_code(manual_country or "US")

    manual_shipping_address = {
        "StreetAddress1": manual_address_line1,
        "StreetAddress2": manual_address_line2,
        "City": manual_city,
        "State": manual_state,
        "ZipCode": manual_zip_code,
        "Country": normalized_manual_country,
        "IsCommercial": manual_is_commercial,
        "IsRestrictedExpeditedShipping": manual_is_restricted,
        "CompanyType": "",
    }

    with destination_right_col:
        st.markdown("**Shipping Calculator Options**")
        option_defaults = build_shipping_request_options(stored_shipping_request_options)
        option_left_col, option_right_col = st.columns(2)
        with option_left_col:
            selected_has_lift_gate = st.checkbox(
                "HasLiftGate",
                value=option_defaults["has_lift_gate"],
                key="pe_has_lift_gate",
            )
            selected_force_common_carrier = st.checkbox(
                "ForceCommonCarrier",
                value=option_defaults["force_common_carrier"],
                key="pe_force_common_carrier",
            )
        with option_right_col:
            selected_exclude_lift_gate_fee = st.checkbox(
                "ExcludeLiftGateFee",
                value=option_defaults["exclude_lift_gate_fee"],
                key="pe_exclude_lift_gate_fee",
            )
            selected_bypass_matrix = st.checkbox(
                "BypassMatrix",
                value=option_defaults["bypass_matrix"],
                key="pe_bypass_matrix",
            )

    selected_shipping_request_options = {
        "has_lift_gate": selected_has_lift_gate,
        "force_common_carrier": selected_force_common_carrier,
        "exclude_lift_gate_fee": selected_exclude_lift_gate_fee,
        "bypass_matrix": selected_bypass_matrix,
    }

    active_shipping_address: dict[str, Any] = {}
    active_destination_label = ""
    destination_validation_error = ""
    if selected_destination_mode == DESTINATION_MODE_WAREHOUSE:
        if selected_destination_warehouse_details:
            try:
                active_shipping_address = build_shipping_address_from_warehouse(
                    selected_destination_warehouse_details
                )
                active_destination_label = format_warehouse_details(selected_destination_warehouse_details)
            except ValueError as exc:
                destination_validation_error = str(exc)
        elif warehouse_options:
            destination_validation_error = "The selected warehouse destination could not be resolved."
    else:
        manual_has_input = any(
            str(manual_shipping_address.get(field, "")).strip()
            for field in ("StreetAddress1", "StreetAddress2", "City", "State", "ZipCode")
        )
        try:
            active_shipping_address = validate_shipping_address(
                manual_shipping_address,
                company_type_default="",
            )
            active_destination_label = format_shipping_address(active_shipping_address)
            if manual_has_input:
                st.caption(f"Destination preview: {active_destination_label}")
        except ValueError as exc:
            if manual_has_input:
                destination_validation_error = str(exc)

    if destination_validation_error:
        st.info(destination_validation_error)

    estimate_exists = bool(
        shipping_source_candidates
        or shipping_calc_error
        or shipping_calc_payload
        or str(stored_destination_label).strip()
    )
    current_request_signature = ""
    if active_shipping_address:
        current_request_signature = json.dumps(
            {
                "destination_mode": selected_destination_mode,
                "shipping_address": active_shipping_address,
                "shipping_request_options": build_shipping_request_options(selected_shipping_request_options),
            },
            sort_keys=True,
        )
    stored_request_signature = ""
    if stored_destination_mode or stored_shipping_address or stored_shipping_request_options:
        stored_request_signature = json.dumps(
            {
                "destination_mode": stored_destination_mode,
                "shipping_address": normalize_shipping_address(
                    stored_shipping_address,
                    company_type_default=(
                        default_company_type
                        if stored_destination_mode == DESTINATION_MODE_WAREHOUSE
                        else ""
                    ),
                ),
                "shipping_request_options": build_shipping_request_options(stored_shipping_request_options),
            },
            sort_keys=True,
        )
    if estimate_exists and current_request_signature and current_request_signature != stored_request_signature:
        st.info("Click Rerun Estimate to apply the updated destination or Shipping Calculator options.")

    run_estimate_disabled = matched_df.empty or not active_shipping_address
    estimate_button_label = "Rerun Estimate" if estimate_exists else "Run Estimate"
    if st.button(estimate_button_label, width="content", disabled=run_estimate_disabled, key="pe_run_estimate"):
        try:
            updated_results = run_estimation_for_results(
                results,
                destination_mode=selected_destination_mode,
                shipping_address=active_shipping_address,
                destination_label=active_destination_label,
                destination_warehouse_number=selected_destination_warehouse_number,
                destination_warehouse_details=selected_destination_warehouse_details,
                shipping_request_options=selected_shipping_request_options,
                perishable_overrides=perishable_overrides,
            )
            st.session_state.pe_results = updated_results
            st.session_state.pe_loaded = True
            st.session_state.pe_errors = input_parse_errors + updated_results.get("row_errors", [])
            st.session_state.pe_selected_package_view = "All Items"
            LOGGER.info(
                "Shipping estimate run complete | destination_mode=%s destination_warehouse=%s shipping_items=%s source_candidates=%s source_warehouse=%s shipping_error=%s",
                selected_destination_mode,
                selected_destination_warehouse_number or "",
                len(updated_results.get("shipping_items", [])),
                len(updated_results.get("shipping_source_candidates", [])),
                updated_results.get("source_warehouse_number", ""),
                bool(updated_results.get("shipping_calc_error", "")),
            )
            st.rerun()
        except Exception as exc:
            LOGGER.exception("Estimate rerun failed: %s", exc)
            st.error(f"Estimate failed: {exc}")

    st.divider()
    st.subheader("Shipping Calculator Results", anchor=False)
    selectable_methods = extract_available_shipping_methods(
        shipping_calc_response,
        shipping_source_candidates,
    )
    available_methods = decorate_shipping_methods_for_destination(
        selectable_methods,
        shipping_source_candidates,
        current_destination_warehouse_number if stored_destination_mode == DESTINATION_MODE_WAREHOUSE else None,
    )
    raw_quote_methods = decorate_shipping_methods_for_destination(
        extract_available_shipping_methods(None, shipping_source_candidates),
        shipping_source_candidates,
        current_destination_warehouse_number if stored_destination_mode == DESTINATION_MODE_WAREHOUSE else None,
    )
    selectable_method_keys = {
        build_shipping_method_key(method)
        for method in available_methods
        if build_shipping_method_key(method)
    }
    raw_only_methods = [
        method
        for method in raw_quote_methods
        if build_shipping_method_key(method)
        and build_shipping_method_key(method) not in selectable_method_keys
    ]
    restricted_expedited_items = sorted(
        {
            str(item.get("item_number", "")).strip()
            for item in shipping_items
            if str(item.get("item_number", "")).strip()
            and bool(item.get("restricted_expedited_shipping"))
        }
    )
    air_restricted_items = sorted(
        {
            str(item.get("item_number", "")).strip()
            for item in shipping_items
            if str(item.get("item_number", "")).strip()
            and bool(item.get("is_air_restricted"))
        }
    )

    if raw_only_methods:
        hidden_method_labels = ", ".join(
            format_shipping_method_option(method)
            for method in raw_only_methods
        )
        st.warning(
            "The API returned additional raw quote methods but did not make them available for user selection: "
            f"{hidden_method_labels}."
        )
        with st.expander("Why some methods are hidden", expanded=False):
            selectable_method_labels = ", ".join(
                format_shipping_method_option(method)
                for method in available_methods
            ) or "None"
            raw_method_labels = ", ".join(
                format_shipping_method_option(method)
                for method in raw_quote_methods
            ) or "None"
            st.markdown(f"**Selectable Methods:** {selectable_method_labels}")
            st.markdown(f"**Raw Quoted Methods:** {raw_method_labels}")
            if restricted_expedited_items:
                st.markdown(
                    "**RestrictedExpeditedShipping Items:** "
                    + ", ".join(f"`{item}`" for item in restricted_expedited_items)
                )
            if air_restricted_items:
                st.markdown(
                    "**IsAirRestricted Items:** "
                    + ", ".join(f"`{item}`" for item in air_restricted_items)
                )
            if not restricted_expedited_items and not air_restricted_items:
                st.caption(
                    "No request-level restricted items were flagged in the page data, so the API is likely applying"
                    " another eligibility rule before exposing selectable methods."
                )

    selected_method: dict[str, Any] | None = None
    if available_methods:
        available_method_map = {
            build_shipping_method_key(method): method
            for method in available_methods
        }
        available_method_keys = list(available_method_map.keys())
        default_method = get_default_shipping_method(available_methods, selected_source_candidate)
        if default_method is None:
            default_method = available_methods[0]
        default_method_key = build_shipping_method_key(default_method)
        default_method_index = (
            available_method_keys.index(default_method_key)
            if default_method_key in available_method_keys
            else 0
        )
        selected_method_key = st.selectbox(
            "Shipping Method",
            options=available_method_keys,
            index=default_method_index,
            format_func=lambda key: format_shipping_method_option(available_method_map[key]),
            key="pe_selected_shipping_method",
        )
        selected_method = available_method_map.get(selected_method_key)

    display_candidates = filter_shipping_candidates_by_method(shipping_source_candidates, selected_method)
    internal_display_candidates = filter_internal_transfer_candidates(
        display_candidates,
        current_destination_warehouse_number if stored_destination_mode == DESTINATION_MODE_WAREHOUSE else None,
    )
    display_source_candidate = select_shipping_source_candidate(internal_display_candidates)
    display_source_warehouse_number = (
        normalize_warehouse_number(display_source_candidate.get("Warehouse Number"))
        if display_source_candidate
        else current_destination_warehouse_number
        if selected_method
        and display_candidates
        and not internal_display_candidates
        and stored_destination_mode == DESTINATION_MODE_WAREHOUSE
        else None
    )
    display_source_warehouse_details = (
        build_source_warehouse_details(display_source_candidate)
        if display_source_candidate
        else stored_destination_warehouse_details
        if display_source_warehouse_number == current_destination_warehouse_number
        else {}
    )
    package_details_df, package_item_tables = build_shipping_method_package_tables(
        shipping_calc_response,
        shipping_source_candidates,
        selected_method,
        destination_warehouse_number=(
            current_destination_warehouse_number
            if stored_destination_mode == DESTINATION_MODE_WAREHOUSE
            else None
        ),
    )
    selected_internal_cost = calculate_internal_shipping_cost(
        selected_method,
        shipping_source_candidates,
        current_destination_warehouse_number if stored_destination_mode == DESTINATION_MODE_WAREHOUSE else None,
    )
    volume_label = "cu in" if dimension_unit == "in" else "m^3"

    if not shipping_items:
        st.info("No matched item records are available to send to the shipping calculator.")
    else:
        sourcing_summary_col, sourcing_options_col = st.columns([1.1, 1.9], vertical_alignment="top")
        with sourcing_summary_col:
            summary_lines: list[str] = []
            if stored_destination_mode:
                summary_lines.append(f"**Destination Type:** {stored_destination_mode}")
            if stored_destination_warehouse_number:
                summary_lines.append(f"**Destination Warehouse:** `{stored_destination_warehouse_number}`")
            if stored_destination_label:
                summary_lines.append(f"**Destination / Ship To:** {stored_destination_label}")
            elif isinstance(stored_shipping_address, dict) and stored_shipping_address:
                summary_lines.append(f"**Destination / Ship To:** {format_shipping_address(stored_shipping_address)}")
            if destination_state:
                summary_lines.append(f"**Destination State:** `{destination_state}`")
            if stored_shipping_request_options:
                summary_lines.append(
                    f"**Request Options:** `{format_shipping_request_options(stored_shipping_request_options)}`"
                )
            if display_source_warehouse_number:
                summary_lines.append(f"**Source Warehouse:** `{display_source_warehouse_number}`")
            if isinstance(display_source_warehouse_details, dict) and display_source_warehouse_details:
                summary_lines.append(
                    f"**Source Location:** {format_warehouse_details(display_source_warehouse_details)}"
                )
            if selected_method:
                selected_method_name = str(selected_method.get("Method Name", "")).strip()
                selected_carrier = str(display_source_candidate.get("Carrier", "")).strip() if display_source_candidate else ""
                selected_delivery_days = to_int(selected_method.get("Delivery Days"), 0)
                excluded_source_warehouses = selected_method.get("Excluded Source Warehouses", [])
                if selected_method_name:
                    summary_lines.append(
                        f"**Selected Method:** {selected_method_name}"
                    )
                if selected_carrier:
                    summary_lines.append(f"**Selected Carrier:** {selected_carrier}")
                summary_lines.append(f"**Selected Shipping Cost:** `${selected_internal_cost:,.2f}`")
                if excluded_source_warehouses:
                    summary_lines.append(
                        "**Excluded Local Warehouse Costs:** "
                        + ", ".join(f"`{warehouse}`" for warehouse in excluded_source_warehouses)
                    )
                elif (
                    stored_destination_mode == DESTINATION_MODE_WAREHOUSE
                    and current_destination_warehouse_number is not None
                    and display_candidates
                    and not internal_display_candidates
                ):
                    summary_lines.append("**Excluded Local Warehouse Costs:** All selected items are already stocked there")
                if selected_delivery_days > 0:
                    summary_lines.append(f"**Delivery Days:** `{selected_delivery_days}`")

            if summary_lines:
                st.markdown("  \n".join(summary_lines))
            else:
                st.info("Run Estimate to see the shipping summary.")

        with sourcing_options_col:
            if shipping_calc_error:
                st.error(shipping_calc_error)
            elif not shipping_calc_payload:
                st.info("Run Estimate to request shipping options.")
            elif not shipping_source_candidates:
                st.warning("Shipping calculator returned no shipping options.")
            else:
                if selected_method:
                    st.caption(
                        "Showing source warehouse options for the selected shipping method. "
                        "Internal Shipping Cost excludes quotes already stocked in the destination warehouse."
                    )
                else:
                    st.caption("Showing available source warehouse options returned by the shipping calculator.")
                st.dataframe(
                    build_shipping_source_options_df(
                        display_candidates or shipping_source_candidates,
                        current_destination_warehouse_number if stored_destination_mode == DESTINATION_MODE_WAREHOUSE else None,
                    ),
                    width="stretch",
                    hide_index=True,
                )

    recommendation_results: list[dict[str, Any]] = []
    current_destination_warehouse_number = normalize_warehouse_number(stored_destination_warehouse_number)
    can_run_recommendations = bool(
        stored_destination_mode == DESTINATION_MODE_WAREHOUSE
        and current_destination_warehouse_number is not None
        and warehouse_options
        and shipping_calc_payload
    )
    if can_run_recommendations:
        recommendation_signature = build_shipping_recommendation_signature(
            results,
            stored_shipping_request_options,
            selected_method,
            perishable_overrides=perishable_overrides,
        )
        if st.session_state.pe_recommendation_signature != recommendation_signature:
            if st.session_state.pe_recommendation_future is not None:
                st.session_state.pe_recommendation_future.cancel()
            st.session_state.pe_recommendation_signature = recommendation_signature
            st.session_state.pe_recommendation_results = []
            st.session_state.pe_recommendation_future = RECOMMENDATION_EXECUTOR.submit(
                run_destination_warehouse_simulations,
                dict(results),
                [
                    warehouse_number
                    for warehouse_number in warehouse_options
                    if warehouse_number != to_int(current_destination_warehouse_number, 0)
                ],
                dict(stored_shipping_request_options),
                dict(selected_method) if isinstance(selected_method, dict) else None,
                dict(perishable_overrides),
            )

        recommendation_future = st.session_state.pe_recommendation_future
        if recommendation_future is not None and recommendation_future.done():
            try:
                st.session_state.pe_recommendation_results = recommendation_future.result()
            except Exception as exc:
                LOGGER.exception("Destination warehouse simulations failed: %s", exc)
                st.session_state.pe_recommendation_results = [
                    {
                        "Destination Warehouse": "",
                        "Destination Location": "",
                        "Source Warehouse": "",
                        "Source Location": "",
                        "Method": "",
                        "Option Type": "",
                        "Delivery Days": 0,
                        "Shipping Cost": 0.0,
                        "Packages": 0,
                        "Total Volume": 0.0,
                        "Total Weight": 0.0,
                        "Error": str(exc),
                    }
                ]
            st.session_state.pe_recommendation_future = None
        recommendation_results = st.session_state.pe_recommendation_results
    else:
        if st.session_state.pe_recommendation_future is not None:
            st.session_state.pe_recommendation_future.cancel()
        st.session_state.pe_recommendation_future = None
        st.session_state.pe_recommendation_signature = ""
        st.session_state.pe_recommendation_results = []

    if stored_destination_mode == DESTINATION_MODE_WAREHOUSE:
        st.divider()
        st.subheader("Destination Warehouse Alternatives", anchor=False)
        cheapest_recommendation = pick_cheapest_destination_recommendation(
            recommendation_results,
            current_destination_warehouse_number,
        )
        best_recommendation = pick_best_destination_recommendation(
            recommendation_results,
            current_destination_warehouse_number,
        )

        if st.session_state.pe_recommendation_future is not None and not recommendation_results:
            st.info("Running alternative destination warehouse simulations in the background.")
        elif not recommendation_results:
            st.info("Alternative warehouse recommendations are not available yet.")
        else:
            recommendation_left, recommendation_right = st.columns(2, vertical_alignment="top")
            with recommendation_left:
                st.markdown("**Cheapest Alternative**")
                if cheapest_recommendation:
                    st.markdown(
                        f"**Destination Location:** {cheapest_recommendation.get('Destination Location', '')}  \n"
                        f"**Shipping Cost:** `${to_float(cheapest_recommendation.get('Shipping Cost'), 0.0):,.2f}`  \n"
                        f"**Packages:** `{to_int(cheapest_recommendation.get('Packages'), 0)}`  \n"
                        f"**Delivery Days:** `{to_int(cheapest_recommendation.get('Delivery Days'), 0)}`"
                    )
                else:
                    st.info("No cheaper alternative is available.")

            with recommendation_right:
                st.markdown("**Best Overall Alternative**")
                if best_recommendation:
                    st.markdown(
                        f"**Destination Location:** {best_recommendation.get('Destination Location', '')}  \n"
                        f"**Shipping Cost:** `${to_float(best_recommendation.get('Shipping Cost'), 0.0):,.2f}`  \n"
                        f"**Packages:** `{to_int(best_recommendation.get('Packages'), 0)}`  \n"
                        f"**Delivery Days:** `{to_int(best_recommendation.get('Delivery Days'), 0)}`"
                    )
                else:
                    st.info("No best-overall alternative is available.")

            recommendation_df = pd.DataFrame(recommendation_results)
            if not recommendation_df.empty:
                recommendation_df["Current Destination"] = (
                    pd.to_numeric(recommendation_df.get("Destination Warehouse"), errors="coerce").fillna(0).astype(int)
                    == to_int(current_destination_warehouse_number, 0)
                )
                for col in ["Shipping Cost", "Total Volume", "Total Weight"]:
                    if col in recommendation_df.columns:
                        recommendation_df[col] = _safe_numeric(recommendation_df[col]).fillna(0.0).round(2)
                if "Packages" in recommendation_df.columns:
                    recommendation_df["Packages"] = _safe_numeric(recommendation_df["Packages"]).fillna(0).astype(int)
                if "Delivery Days" in recommendation_df.columns:
                    recommendation_df["Delivery Days"] = (
                        _safe_numeric(recommendation_df["Delivery Days"]).fillna(0).astype(int)
                    )
                st.dataframe(recommendation_df, width="stretch", hide_index=True)

    st.divider()
    st.subheader("Package Summary", anchor=False)
    if not shipping_calc_payload:
        st.info("Run Estimate to see package details from the shipping calculator.")
    elif selected_method and display_candidates and not internal_display_candidates:
        st.info(
            "No transfer packages are shown for the selected shipping method because the related items are "
            "already stocked in the destination warehouse."
        )
    elif display_source_candidate is None:
        st.warning("No source warehouse could be matched for the selected shipping method.")
    elif package_details_df.empty:
        st.info("The selected shipping method did not return package detail rows for the internal transfer.")
    else:
        total_packages = int(len(package_details_df))
        total_volume = float((_safe_numeric(package_details_df["Volume"]) * _volume_multiplier(dimension_unit)).sum())
        total_weight = float((_safe_numeric(package_details_df["Weight"]) * _weight_multiplier(weight_unit)).sum())
        source_warehouse_series = pd.Series(
            package_details_df.get("Source Warehouse", pd.Series(dtype="object"))
        )
        warehouses_used = int(source_warehouse_series.map(normalize_warehouse_number).dropna().nunique())

        package_summary_metrics_col, package_summary_table_col = st.columns([1.1, 2.4], vertical_alignment="top")
        with package_summary_metrics_col:
            metric_left_col, metric_right_col = st.columns(2)
            with metric_left_col:
                st.metric("Total Packages", f"{total_packages:,}")
                st.metric("Total Weight", f"{total_weight:,.2f} {weight_unit}")
            with metric_right_col:
                st.metric("Total Volume", f"{total_volume:,.2f} {volume_label}")
                st.metric("Warehouses Used", f"{warehouses_used:,}")
        with package_summary_table_col:
            package_display_df = package_details_df.copy()
            if "Items" in package_display_df.columns:
                package_display_df["Items"] = package_display_df["Items"].astype(int)
            st.markdown("**Package Details**")
            st.dataframe(
                format_measurement_dataframe(package_display_df, dimension_unit, weight_unit),
                width="stretch",
                hide_index=True,
            )

        st.divider()
        st.subheader("Package Contents Summary", anchor=False)
        summary_options = ["All Items"] + [
            f"Package {package_number}" for package_number in sorted(package_item_tables.keys())
        ]
        if st.session_state.pe_selected_package_view not in summary_options:
            st.session_state.pe_selected_package_view = "All Items"

        selected_package_view = st.selectbox(
            "Filter by package",
            options=summary_options,
            key="pe_selected_package_view",
        )

        if selected_package_view == "All Items":
            summary_df = build_all_items_summary_dataframe(package_item_tables, shipping_items)
            st.caption("Showing all item quantities across the selected source warehouse's package result.")
        else:
            selected_package_number = to_int(selected_package_view.replace("Package ", ""), 0)
            summary_df = package_item_tables.get(
                selected_package_number,
                pd.DataFrame(columns=["Item Number", "Quantity", "Volume", "Weight"]),
            )
            st.caption(f"Showing the contents of package {selected_package_number}.")

        if summary_df.empty:
            st.info("No package contents were returned for the selected source warehouse.")
        else:
            st.dataframe(
                format_quantity_volume_weight_dataframe(summary_df, dimension_unit, weight_unit),
                width="stretch",
                hide_index=True,
            )

    if shipping_calc_payload:
        st.divider()
        st.subheader("API Details", anchor=False)
        request_col, response_col = st.columns(2, vertical_alignment="top")
        with request_col:
            with st.expander("Request Payload", expanded=False):
                st.json(shipping_calc_payload)
        with response_col:
            with st.expander("Response Body (Charges Hidden)", expanded=False):
                if isinstance(shipping_calc_response, (dict, list)):
                    st.json(strip_shipping_charge_fields(shipping_calc_response))
                elif shipping_calc_response is None:
                    st.info("No response body is available.")
                else:
                    st.code(str(shipping_calc_response))

        st.divider()
        st.subheader("Package Allocation Detail", anchor=False)
        allocation_df = build_shipping_package_allocation_df(
            shipping_calc_response,
            shipping_source_candidates,
        )
        if allocation_df.empty:
            st.info("No package allocation detail rows were returned by the shipping calculator response.")
        else:
            st.caption(
                "This table uses all raw quote candidates returned by the API. "
                "Package cost allocation is estimated by apportioning each quote cost across packages by package "
                "volume, then across items by each item's share of total item volume within the package."
            )

            allocation_filter_cols = st.columns(5)
            source_warehouse_options = sorted(
                value for value in allocation_df["Source Warehouse"].dropna().astype(str).unique().tolist() if value
            )
            method_options = sorted(
                value for value in allocation_df["Method"].dropna().astype(str).unique().tolist() if value
            )
            carrier_options = sorted(
                value for value in allocation_df["Carrier"].dropna().astype(str).unique().tolist() if value
            )
            delivery_day_options = sorted(
                int(value)
                for value in pd.to_numeric(allocation_df["Delivery Days"], errors="coerce").dropna().astype(int).unique().tolist()
            )
            package_number_options = sorted(
                int(value)
                for value in pd.to_numeric(allocation_df["Package Number"], errors="coerce").dropna().astype(int).unique().tolist()
            )

            selected_source_warehouse = allocation_filter_cols[0].selectbox(
                "Source Warehouse",
                options=["All", *source_warehouse_options],
                key="pe_alloc_filter_source_warehouse",
            )
            selected_method_filter = allocation_filter_cols[1].selectbox(
                "Method",
                options=["All", *method_options],
                key="pe_alloc_filter_method",
            )
            selected_carrier = allocation_filter_cols[2].selectbox(
                "Carrier",
                options=["All", *carrier_options],
                key="pe_alloc_filter_carrier",
            )
            selected_delivery_days = allocation_filter_cols[3].selectbox(
                "Delivery Days",
                options=["All", *delivery_day_options],
                key="pe_alloc_filter_delivery_days",
            )
            selected_package_number = allocation_filter_cols[4].selectbox(
                "Package Number",
                options=["All", *package_number_options],
                key="pe_alloc_filter_package_number",
                format_func=lambda package_number: (
                    package_number if package_number == "All" else f"Package {package_number}"
                ),
            )

            filtered_allocation_df = allocation_df.copy()
            if selected_source_warehouse != "All":
                filtered_allocation_df = filtered_allocation_df[
                    filtered_allocation_df["Source Warehouse"] == selected_source_warehouse
                ]
            if selected_method_filter != "All":
                filtered_allocation_df = filtered_allocation_df[
                    filtered_allocation_df["Method"] == selected_method_filter
                ]
            if selected_carrier != "All":
                filtered_allocation_df = filtered_allocation_df[
                    filtered_allocation_df["Carrier"] == selected_carrier
                ]
            if selected_delivery_days != "All":
                filtered_allocation_df = filtered_allocation_df[
                    filtered_allocation_df["Delivery Days"] == selected_delivery_days
                ]
            if selected_package_number != "All":
                filtered_allocation_df = filtered_allocation_df[
                    filtered_allocation_df["Package Number"] == selected_package_number
                ]

            if filtered_allocation_df.empty:
                st.info("No allocation rows match the selected filters.")
            else:
                quantity_values = _safe_numeric(filtered_allocation_df["Quantity"]).fillna(0.0)
                if (quantity_values % 1 == 0).all():
                    filtered_allocation_df["Quantity"] = quantity_values.astype(int)
                else:
                    filtered_allocation_df["Quantity"] = quantity_values.round(2)

                filtered_allocation_df = filtered_allocation_df[
                    [
                        "Package Number",
                        "Item Number",
                        "Method",
                        "Quantity",
                        "Volume",
                        "Source Warehouse",
                        "Delivery Days",
                        "Carrier",
                        "Package Cost Allocation by Volume",
                    ]
                ]
                filtered_allocation_df["Package Cost Allocation by Volume"] = (
                    _safe_numeric(filtered_allocation_df["Package Cost Allocation by Volume"]).fillna(0.0).round(2)
                )
                display_allocation_df = format_measurement_dataframe(
                    filtered_allocation_df,
                    dimension_unit,
                    weight_unit,
                )
                st.dataframe(
                    display_allocation_df,
                    width="stretch",
                    hide_index=True,
                )

    st.stop()

# ============================================================
# LEGACY PACKAGER REFERENCE
# ============================================================
# The Warehouse Packager setup is intentionally kept below as commented
# reference only. The page now runs against the Shipping Calculator API
# only.
#
# def build_packaging_payload(
#     verified_items: list[dict[str, Any]],
#     source_warehouse_number: int,
# ) -> list[dict[str, Any]]:
#     if not verified_items:
#         return []
#
#     payload: list[dict[str, Any]] = []
#     for item in verified_items:
#         payload.append(
#             {
#                 "warehouseNumber": int(source_warehouse_number),
#                 "itemNumber": item["item_number"],
#                 "quantity": item["quantity"],
#                 "length": item["length"],
#                 "width": item["width"],
#                 "height": item["height"],
#                 "weight": item["weight"],
#                 "isRepack": item["is_repack"],
#                 "isRepositional": item["is_repositional"],
#                 "breakQuantity": item["break_quantity"],
#                 "marginalLength": item["marginal_length"],
#                 "marginalHeight": item["marginal_height"],
#                 "marginalWidth": item["marginal_width"],
#                 "canBeNested": item["can_be_nested"],
#                 "volume": item["volume"],
#                 "perishableType": item["perishable_type"],
#             }
#         )
#
#     return payload
#
#
# def call_packaging_api(payload: list[dict[str, Any]], destination_state: str) -> tuple[Any, str]:
#     if not payload:
#         return None, ""
#
#     packager_endpoint = (
#         "https://shippingcalculator-api.dev.clarkinc.biz/api/warehousepackager/estimatePacking"
#     )
#     packager_timeout_seconds = 30
#     separator = "&" if "?" in packager_endpoint else "?"
#     url = f"{packager_endpoint}{separator}destinationState={quote(destination_state)}"
#     return post_json_request(url=url, payload=payload, timeout_seconds=packager_timeout_seconds)
