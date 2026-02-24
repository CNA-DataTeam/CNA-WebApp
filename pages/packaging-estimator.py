"""
pages/packaging-estimator.py

Purpose:
    Streamlit page that accepts item input and returns all columns from the
    configured item-info parquet file for matching item numbers.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from io import BytesIO
import json
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
st.set_page_config(page_title="Packaging Estimator", layout="wide")
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

logo_b64 = utils.get_logo_base64(str(config.LOGO_PATH))
st.markdown(
    f"""
    <div class="header-row">
        <img class="header-logo" src="data:image/png;base64,{logo_b64}" />
        <h1 class="header-title">LS - Packaging Estimator</h1>
    </div>
    """,
    unsafe_allow_html=True,
)
st.divider()

# ============================================================
# CONSTANTS
# ============================================================
INPUT_MODE_UPLOAD = "Upload Excel"
INPUT_MODE_PASTE = "Paste from Excel (tab-separated)"
US_STATE_CODES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


# ============================================================
# CONFIG / LOGGING
# ============================================================
def load_packaging_config() -> dict[str, Any]:
    """Load packaging config from config.py with safe defaults."""
    runtime_cfg = config.PACKAGING_CONFIG if isinstance(config.PACKAGING_CONFIG, dict) else {}
    item_info_cfg = runtime_cfg.get("item_info", {}) if isinstance(runtime_cfg.get("item_info"), dict) else {}
    api_cfg = runtime_cfg.get("api", {}) if isinstance(runtime_cfg.get("api"), dict) else {}
    ui_cfg = runtime_cfg.get("ui", {}) if isinstance(runtime_cfg.get("ui"), dict) else {}

    parquet_path = str(
        item_info_cfg.get(
            "parquet_path",
            r"\\therestaurantstore.com\920\Data\Logistics\Logistics App\Item Info\item_info.parquet",
        )
    ).strip()
    item_number_column = str(item_info_cfg.get("item_number_column", "ItemNumber")).strip() or "ItemNumber"
    api_endpoint = str(
        api_cfg.get(
            "endpoint",
            "https://shippingcalculator-api.dev.clarkinc.biz/api/warehousepackager/estimatePacking",
        )
    ).strip()
    api_timeout_seconds = int(api_cfg.get("timeout_seconds", 30) or 30)
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
        "api": {
            "endpoint": api_endpoint,
            "timeout_seconds": api_timeout_seconds,
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


LOGGER = utils.get_page_logger("Packaging Estimator Page")
PAGE_CONFIG = load_packaging_config()
utils.log_page_open_once("packaging_estimator_page", LOGGER)
if "_packaging_render_logged" not in st.session_state:
    st.session_state._packaging_render_logged = True
    LOGGER.info("Rendering packaging estimator page.")


# ============================================================
# SESSION STATE
# ============================================================
if "pe_loaded" not in st.session_state:
    st.session_state.pe_loaded = False
if "pe_results" not in st.session_state:
    st.session_state.pe_results = {}
if "pe_errors" not in st.session_state:
    st.session_state.pe_errors = []


# ============================================================
# INPUT HELPERS
# ============================================================
@st.cache_data
def read_excel_bytes(file_bytes: bytes) -> pd.DataFrame:
    """Read uploaded Excel bytes into a DataFrame."""
    return pd.read_excel(BytesIO(file_bytes), dtype=str)


def normalize_col_name(col_name: str) -> str:
    return "".join(ch for ch in str(col_name).lower() if ch.isalnum())


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


def extract_bracketed_column_name(column_name: str) -> str:
    matches = re.findall(r"\[([^\]]+)\]", column_name)
    if matches:
        return matches[-1].strip()
    return str(column_name).strip()


def resolve_item_column_name(configured_name: str, available_columns: list[str]) -> str:
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
        f"Configured item number column '{configured_name}' was not found in parquet file columns."
    )


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


def build_verified_payload(
    verified_df: pd.DataFrame,
    requested_df: pd.DataFrame,
    warehouse_number: int,
) -> list[dict[str, Any]]:
    if verified_df.empty:
        return []

    ui_cfg = PAGE_CONFIG.get("ui", {})
    item_cfg = PAGE_CONFIG.get("item_info", {})
    item_col = str(item_cfg.get("item_number_column", "ItemNumber")).strip() or "ItemNumber"
    resolved_item_col = resolve_item_column_name(item_col, verified_df.columns.tolist())

    quantity_map = {
        normalize_item_number(row.ItemNumber): int(row.Quantity)
        for row in requested_df.itertuples(index=False)
    }

    default_marginal_length = float(ui_cfg.get("default_marginal_length", 0.0) or 0.0)
    default_marginal_width = float(ui_cfg.get("default_marginal_width", 0.0) or 0.0)
    default_marginal_height = float(ui_cfg.get("default_marginal_height", 0.0) or 0.0)
    default_perishable_type = str(ui_cfg.get("default_perishable_type", "N")).strip() or "N"

    payload: list[dict[str, Any]] = []
    for row in verified_df.to_dict(orient="records"):
        item_number = normalize_item_number(row.get(resolved_item_col))
        if not item_number:
            continue
        quantity = int(quantity_map.get(item_number, 0))
        if quantity <= 0:
            continue

        length = to_float(row.get("LengthInInches"), 0.0)
        width = to_float(row.get("WidthInInches"), 0.0)
        height = to_float(row.get("HeightInInches"), 0.0)
        volume = to_float(row.get("AverageVolume"), length * width * height)

        payload.append(
            {
                "warehouseNumber": int(warehouse_number),
                "itemNumber": item_number,
                "quantity": quantity,
                "length": length,
                "width": width,
                "height": height,
                "weight": to_float(row.get("WeightInPounds"), 0.0),
                "isRepack": coerce_verified_value(row.get("IsRepackRequired")),
                "isRepositional": coerce_verified_value(row.get("IsRepositionable")),
                "breakQuantity": to_int(row.get("BreakQuantity"), 0),
                "marginalLength": default_marginal_length,
                "marginalHeight": default_marginal_height,
                "marginalWidth": default_marginal_width,
                "canBeNested": coerce_verified_value(row.get("Can Nest?")),
                "volume": volume,
                "perishableType": default_perishable_type,
            }
        )

    return payload


def call_packaging_api(payload: list[dict[str, Any]], destination_state: str) -> tuple[Any, str]:
    if not payload:
        return None, ""

    api_cfg = PAGE_CONFIG.get("api", {})
    endpoint = str(api_cfg.get("endpoint", "")).strip()
    timeout_seconds = int(api_cfg.get("timeout_seconds", 30) or 30)
    if not endpoint:
        return None, "API endpoint is not configured."

    separator = "&" if "?" in endpoint else "?"
    url = f"{endpoint}{separator}destinationState={quote(destination_state)}"
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        url=url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
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
        package_rows: list[dict[str, Any]] = []
        for package_id, package in api_response.items():
            if not isinstance(package, dict):
                continue
            package_num = to_int(package_id, 0)
            volume = to_float(package.get("volume", package.get("Volume", 0.0)), 0.0)
            weight = to_float(package.get("weight", package.get("Weight", 0.0)), 0.0)
            package_rows.append(
                {
                    "Package Number": package_num,
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

with spacer_col:
    st.write("")

with input_col:
    st.subheader("Input", anchor=False)
    ui_cfg = PAGE_CONFIG.get("ui", {})
    default_destination_state = str(ui_cfg.get("default_destination_state", "FL")).strip().upper() or "FL"
    default_warehouse = int(ui_cfg.get("default_warehouse", 105) or 105)
    state_default_index = (
        US_STATE_CODES.index(default_destination_state)
        if default_destination_state in US_STATE_CODES
        else US_STATE_CODES.index("FL")
    )

    selector_left, selector_right = st.columns(2)
    with selector_left:
        destination_state = st.selectbox(
            "Destination State",
            options=US_STATE_CODES,
            index=state_default_index,
            key="pe_destination_state",
        )
    with selector_right:
        warehouse_number = int(
            st.number_input(
                "Warehouse Number",
                min_value=1,
                step=1,
                value=default_warehouse,
                key="pe_warehouse_number",
            )
        )

    input_mode = st.radio(
        "Input Mode",
        [INPUT_MODE_UPLOAD, INPUT_MODE_PASTE],
        horizontal=True,
    )

    if input_mode == INPUT_MODE_UPLOAD:
        uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx", "xls"])

        if uploaded_file is not None:
            try:
                uploaded_df = read_excel_bytes(uploaded_file.getvalue())
                uploaded_df.columns = [str(col).strip() for col in uploaded_df.columns]
            except Exception as exc:
                uploaded_df = pd.DataFrame()
                LOGGER.exception("Could not read uploaded Excel file: %s", exc)
                input_parse_errors.append(f"Could not read Excel file: {exc}")

            if uploaded_df.empty:
                st.info("Uploaded file has no rows.")
            elif len(uploaded_df.columns) < 2:
                st.error("Excel input must contain at least two columns: Item Number and Quantity.")
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
        else:
            st.caption("Upload an Excel file with item and quantity columns, then click Load.")

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

    if standard_input_df.empty:
        st.info("Preview will appear here after input is provided.")
    else:
        st.caption("Preview shows the first 5 items only.")
        st.dataframe(
            standard_input_df[["ItemNumber", "Quantity"]].head(5),
            width="stretch",
            hide_index=True,
        )


with input_col:
    has_input_rows = not standard_input_df.empty
    if st.button("Load", type="primary", width="content", disabled=not has_input_rows):
        try:
            pipeline_results = run_pipeline(standard_input_df)
            requested_df = pipeline_results.get("requested_df", pd.DataFrame(columns=["ItemNumber", "Quantity"]))
            matched_df = pipeline_results.get("matched_df", pd.DataFrame())
            unmatched_df = pipeline_results.get("unmatched_df", pd.DataFrame(columns=["ItemNumber", "Quantity"]))
            verified_df, non_verified_df = split_verified_and_non_verified(matched_df, unmatched_df)
            payload = build_verified_payload(verified_df, requested_df, warehouse_number=warehouse_number)
            api_response, api_error = call_packaging_api(payload, destination_state=destination_state)

            pipeline_results["verified_df"] = verified_df
            pipeline_results["non_verified_df"] = non_verified_df
            pipeline_results["api_payload"] = payload
            pipeline_results["api_response"] = api_response
            pipeline_results["api_error"] = api_error
            pipeline_results["destination_state"] = destination_state
            pipeline_results["warehouse_number"] = warehouse_number
            combined_errors = input_parse_errors + pipeline_results.get("row_errors", [])

            st.session_state.pe_loaded = True
            st.session_state.pe_results = pipeline_results
            st.session_state.pe_errors = combined_errors

        except Exception as exc:
            LOGGER.exception("Packaging lookup failed: %s", exc)
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
    api_payload = results.get("api_payload", [])
    api_response = results.get("api_response", None)
    api_error = str(results.get("api_error", "") or "")
    destination_state = str(results.get("destination_state", ""))
    warehouse_number = results.get("warehouse_number", "")

    st.divider()
    st.subheader("Lookup Summary", anchor=False)
    col_requested, col_matched_rows, col_unmatched = st.columns(3)
    col_requested.metric("Requested Items", len(requested_df))
    col_matched_rows.metric("Matched Rows", len(matched_df))
    col_unmatched.metric("Unmatched Items", len(unmatched_df))

    st.divider()
    st.subheader("Item Info Results", anchor=False)
    verified_col, non_verified_col = st.columns(2)
    with verified_col:
        st.subheader("Verified Items", anchor=False)
        if verified_df.empty:
            st.info("No verified items found.")
        else:
            st.dataframe(verified_df, width="stretch", hide_index=True)

    with non_verified_col:
        st.subheader("Non-Verified Items", anchor=False)
        if non_verified_df.empty:
            st.info("No non-verified items found.")
        else:
            st.dataframe(non_verified_df, width="stretch", hide_index=True)

    st.divider()
    st.subheader("Packaging API", anchor=False)
    if not api_payload:
        st.info("No verified items available to send to the packaging API.")
    else:
        st.caption(f"Destination State: `{destination_state}` | Warehouse Number: `{warehouse_number}`")
        if api_error:
            st.error(api_error)
        else:
            st.write("API package summary:")
            response_df = normalize_api_response_to_df(api_response)
            if response_df.empty:
                st.info("API returned no rows.")
            else:
                total_volume = float(response_df["Volume"].sum()) if "Volume" in response_df.columns else 0.0
                total_weight = float(response_df["Weight"].sum()) if "Weight" in response_df.columns else 0.0
                card_col_1, card_col_2 = st.columns(2)
                card_col_1.metric("Total Dimension", f"{total_volume:,.2f}")
                card_col_2.metric("Total Weight", f"{total_weight:,.2f}")
                st.dataframe(response_df, width="stretch", hide_index=True)
