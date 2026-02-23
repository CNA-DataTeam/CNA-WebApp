"""
pages/packaging-estimator.py

Purpose:
    Streamlit page for package estimation from uploaded/pasted item rows.

Workflow:
    1) Load and validate input (ItemNumber + Quantity)
    2) Fetch verification flags (SSAS placeholder)
    3) Split verified vs unverified
    4) Call packaging API for verified rows only (API placeholder)
    5) Display normalized results
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from io import BytesIO
from logging.handlers import RotatingFileHandler
from pathlib import Path
import hashlib
import json
import logging
import math
import os
from typing import Any

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

SUMMARY_COLUMNS = ["ItemNumber", "Quantity", "IsVerified"]
PACKAGING_COLUMNS = [
    "PackageId",
    "ItemNumber",
    "Quantity",
    "PackageCount",
    "Length",
    "Width",
    "Height",
    "Weight",
]


# ============================================================
# CONFIG / LOGGING
# ============================================================
@st.cache_data
def load_packaging_config() -> dict[str, Any]:
    """Load packaging page config from config.json with safe defaults."""
    defaults: dict[str, Any] = {
        "ssas": {
            "connection": "",
            "database": "",
            "query": "",
            "access_token_env": "SSAS_ACCESS_TOKEN",
            "access_token_ttl_minutes": 55,
            "use_service_principal": False,
            "service_principal_tenant_env": "AZURE_TENANT_ID",
            "service_principal_client_id_env": "AZURE_CLIENT_ID",
            "service_principal_client_secret_env": "AZURE_CLIENT_SECRET",
            "service_principal_scope": "https://analysis.windows.net/powerbi/api/.default",
            "timeout_seconds": 60,
            "enable_mock": True,
        },
        "api": {
            "endpoint": "https://shippingcalculator-api.dev.clarkinc.biz/api/warehousepackager/estimatePackingRequirements",
            "timeout_seconds": 30,
            "enable_mock": True,
        },
        "logging": {
            "directory": "logs",
            "max_bytes": 1_048_576,
            "backup_count": 5,
        },
        "ui": {
            "default_warehouse": 920,
            "default_marginal_length": 0.0,
            "default_marginal_width": 0.0,
            "default_marginal_height": 0.0,
        },
    }
    config_path = Path("config.json")
    if not config_path.exists():
        return defaults

    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return defaults
    except Exception:
        return defaults

    for section, section_defaults in defaults.items():
        if section not in data or not isinstance(data[section], dict):
            data[section] = section_defaults
            continue
        for key, value in section_defaults.items():
            data[section].setdefault(key, value)
    return data


@st.cache_resource
def get_page_logger() -> logging.Logger:
    """Create a page logger using config.json logging settings."""
    page_config = load_packaging_config()
    logging_cfg = page_config.get("logging", {})
    log_dir = Path(str(logging_cfg.get("directory", "logs")))
    if not log_dir.is_absolute():
        log_dir = Path.cwd() / log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("packaging_estimator_page")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = RotatingFileHandler(
            filename=log_dir / "packaging_estimator.log",
            maxBytes=int(logging_cfg.get("max_bytes", 1_048_576)),
            backupCount=int(logging_cfg.get("backup_count", 5)),
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.propagate = False
    return logger


LOGGER = get_page_logger()
PAGE_CONFIG = load_packaging_config()


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
    """
    Validate rows against business rules and aggregate duplicates by ItemNumber.
    """
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
# SSAS PLACEHOLDER
# ============================================================
def _mock_verification_flags(items: list[str]) -> dict[str, bool]:
    flags: dict[str, bool] = {}
    for item in items:
        checksum = sum(ord(ch) for ch in item)
        flags[item] = (checksum % 5) != 0
    return flags


def _coerce_ssas_flag(value: Any) -> bool:
    if value is None or type(value).__name__ == "DBNull":
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return bool(value)
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "t", "yes", "y"}


def _normalize_field_name(field_name: str) -> str:
    return "".join(ch for ch in str(field_name).lower() if ch.isalnum())


def _extract_ssas_column_name(description_item: Any) -> str:
    if description_item is None:
        return ""
    if isinstance(description_item, (tuple, list)):
        if not description_item:
            return ""
        return str(description_item[0] or "")
    for attr_name in ("name", "column_name"):
        attr_value = getattr(description_item, attr_name, None)
        if attr_value is not None:
            return str(attr_value)
    return str(description_item)


def _resolve_ssas_field_ordinals(description: Any) -> tuple[int, int]:
    item_exact = {"itemnumber", "__itemnumber"}
    verified_exact = {"isverified", "verified", "__isverified"}
    item_contains = {"itemnumber"}
    verified_contains = {"isverified", "verified"}
    item_ordinal = -1
    verified_ordinal = -1
    columns = list(description or [])

    for idx, column in enumerate(columns):
        normalized_name = _normalize_field_name(_extract_ssas_column_name(column))
        if item_ordinal < 0 and normalized_name in item_exact:
            item_ordinal = idx
        if verified_ordinal < 0 and normalized_name in verified_exact:
            verified_ordinal = idx

    if item_ordinal >= 0 and verified_ordinal >= 0:
        return item_ordinal, verified_ordinal

    for idx, column in enumerate(columns):
        normalized_name = _normalize_field_name(_extract_ssas_column_name(column))
        if item_ordinal < 0 and any(token in normalized_name for token in item_contains):
            item_ordinal = idx
        if verified_ordinal < 0 and any(token in normalized_name for token in verified_contains):
            verified_ordinal = idx

    if item_ordinal < 0 or verified_ordinal < 0:
        raise ValueError(
            "Could not detect ItemNumber/IsVerified columns in SSAS query result."
        )
    return item_ordinal, verified_ordinal


def _acquire_service_principal_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: str,
) -> str:
    try:
        import msal  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "MSAL is not installed. Install dependency 'msal' for service principal auth."
        ) from exc

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=[scope])
    access_token = str(result.get("access_token", "")).strip()
    if access_token:
        return access_token

    error_text = str(result.get("error_description") or result.get("error") or "Unknown token error")
    raise RuntimeError(error_text)


def _resolve_ssas_access_token(ssas_cfg: dict[str, Any]) -> tuple[str, str]:
    access_token_env = str(ssas_cfg.get("access_token_env", "SSAS_ACCESS_TOKEN")).strip() or "SSAS_ACCESS_TOKEN"
    env_token = str(os.environ.get(access_token_env, "")).strip()
    if env_token:
        return env_token, f"env:{access_token_env}"

    if not bool(ssas_cfg.get("use_service_principal", False)):
        return "", "none"

    tenant_env = str(ssas_cfg.get("service_principal_tenant_env", "AZURE_TENANT_ID")).strip() or "AZURE_TENANT_ID"
    client_id_env = str(ssas_cfg.get("service_principal_client_id_env", "AZURE_CLIENT_ID")).strip() or "AZURE_CLIENT_ID"
    client_secret_env = (
        str(ssas_cfg.get("service_principal_client_secret_env", "AZURE_CLIENT_SECRET")).strip()
        or "AZURE_CLIENT_SECRET"
    )
    scope = (
        str(ssas_cfg.get("service_principal_scope", "https://analysis.windows.net/powerbi/api/.default")).strip()
        or "https://analysis.windows.net/powerbi/api/.default"
    )

    tenant_id = str(os.environ.get(tenant_env, "")).strip()
    client_id = str(os.environ.get(client_id_env, "")).strip()
    client_secret = str(os.environ.get(client_secret_env, "")).strip()
    if not tenant_id or not client_id or not client_secret:
        LOGGER.warning(
            "Service principal auth enabled, but env vars are missing. tenant=%s client_id=%s client_secret=%s",
            bool(tenant_id),
            bool(client_id),
            bool(client_secret),
        )
        return "", "service_principal_missing_env"

    try:
        token = _acquire_service_principal_token(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            scope=scope,
        )
        LOGGER.info(
            "SSAS access token acquired via service principal | tenant_env=%s client_id_env=%s",
            tenant_env,
            client_id_env,
        )
        return token, "service_principal"
    except Exception as exc:
        LOGGER.exception("Service principal token acquisition failed: %s", exc)
        return "", "service_principal_error"


def _iter_pyadomd_rows(cursor: Any) -> Any:
    fetchone = getattr(cursor, "fetchone", None)
    if callable(fetchone):
        while True:
            row = fetchone()
            if row is None:
                break
            yield row
        return

    fetchall = getattr(cursor, "fetchall", None)
    if callable(fetchall):
        for row in fetchall():
            yield row


def fetch_verification_flags(items: list[str]) -> dict[str, bool]:
    """
    Execute configured SSAS DAX query via pyadomd and map results back
    to requested item flags.
    """
    normalized_items = [normalize_item_number(item) for item in items]
    unique_items = [item for item in dict.fromkeys(normalized_items) if item]
    if not unique_items:
        return {}

    ssas_cfg = PAGE_CONFIG.get("ssas", {}) if isinstance(PAGE_CONFIG.get("ssas"), dict) else {}
    LOGGER.info(
        "SSAS verification start | requested_items=%s enable_mock=%s has_query=%s",
        len(unique_items),
        bool(ssas_cfg.get("enable_mock", True)),
        bool(str(ssas_cfg.get("query", "")).strip()),
    )
    if bool(ssas_cfg.get("enable_mock", True)):
        LOGGER.info("SSAS verification using mock path (enable_mock=true).")
        return _mock_verification_flags(unique_items)

    connection = str(ssas_cfg.get("connection", "")).strip()
    database = str(ssas_cfg.get("database", "")).strip()
    query = str(ssas_cfg.get("query", "")).strip()
    timeout_seconds = int(ssas_cfg.get("timeout_seconds", 60) or 60)
    access_token, token_source = _resolve_ssas_access_token(ssas_cfg)

    if not connection or not database or not query:
        LOGGER.warning(
            "SSAS configuration is incomplete. Falling back to deterministic verification. "
            "connection_set=%s database_set=%s query_set=%s",
            bool(connection),
            bool(database),
            bool(query),
        )
        return _mock_verification_flags(unique_items)

    flags = {item: False for item in unique_items}
    requested_items = set(unique_items)
    connection_string = (
        f"Provider=MSOLAP;Data Source={connection};Initial Catalog={database};Catalog={database};"
    )
    if access_token:
        connection_string = (
            f"Provider=MSOLAP;Data Source={connection};Initial Catalog={database};Catalog={database};"
            f"Password={access_token};Persist Security Info=True;User ID=app:;"
        )
    query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()[:12]

    try:
        LOGGER.info(
            "SSAS opening pyadomd connection | database=%s timeout=%s query_hash=%s token_source=%s token_present=%s",
            database,
            timeout_seconds,
            query_hash,
            token_source,
            bool(access_token),
        )
        try:
            from pyadomd import Pyadomd  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "pyadomd is not installed or failed to import. Install dependency 'pyadomd'."
            ) from exc

        rows_read = 0
        matched_items = 0
        with Pyadomd(connection_string) as conn:
            cursor = conn.cursor()
            try:
                LOGGER.info("SSAS executing query via pyadomd.")
                cursor.execute(query)
                item_ordinal, verified_ordinal = _resolve_ssas_field_ordinals(
                    getattr(cursor, "description", [])
                )
                for row in _iter_pyadomd_rows(cursor):
                    rows_read += 1
                    item_value = normalize_item_number(row[item_ordinal])
                    if item_value in requested_items:
                        matched_items += 1
                        flags[item_value] = flags[item_value] or _coerce_ssas_flag(
                            row[verified_ordinal]
                        )
            finally:
                close_fn = getattr(cursor, "close", None)
                if callable(close_fn):
                    try:
                        close_fn()
                    except Exception:
                        pass

        verified_true = sum(1 for v in flags.values() if v)
        LOGGER.info(
            "SSAS query complete | rows_read=%s matched_items=%s verified_true=%s requested_items=%s",
            rows_read,
            matched_items,
            verified_true,
            len(unique_items),
        )
    except Exception as exc:
        LOGGER.exception("SSAS verification lookup failed. Falling back to deterministic verification: %s", exc)
        return _mock_verification_flags(unique_items)

    return flags


@st.cache_data(show_spinner=False)
def fetch_verification_flags_cached(items_key: tuple[str, ...]) -> dict[str, bool]:
    """Cached wrapper for SSAS placeholder call keyed by normalized item tuple."""
    return fetch_verification_flags(list(items_key))


def add_verification_flags(clean_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    items_key = tuple(clean_df["ItemNumber"].tolist())
    verification_flags = fetch_verification_flags_cached(items_key)

    summary_df = clean_df.copy()
    summary_df["IsVerified"] = summary_df["ItemNumber"].map(verification_flags).fillna(False).astype(bool)

    verified_df = summary_df[summary_df["IsVerified"]].copy().reset_index(drop=True)
    unverified_df = summary_df[~summary_df["IsVerified"]].copy().reset_index(drop=True)
    return summary_df, verified_df, unverified_df


# ============================================================
# PACKAGING API PLACEHOLDER
# ============================================================
def stable_seed(item_number: str) -> int:
    return int(hashlib.sha256(item_number.encode("utf-8")).hexdigest()[:8], 16)


def build_packaging_payload(
    verified_rows: pd.DataFrame,
    destination_state: str,
) -> list[dict[str, Any]]:
    """Build placeholder payload schema for future API integration."""
    ui_cfg = PAGE_CONFIG.get("ui", {})
    default_warehouse = int(ui_cfg.get("default_warehouse", 105))
    default_marginal_length = float(ui_cfg.get("default_marginal_length", 0.0))
    default_marginal_width = float(ui_cfg.get("default_marginal_width", 0.0))
    default_marginal_height = float(ui_cfg.get("default_marginal_height", 0.0))

    payload: list[dict[str, Any]] = []
    for row in verified_rows.itertuples(index=False):
        seed = stable_seed(row.ItemNumber)
        length = 10 + (seed % 8)
        width = 8 + ((seed // 8) % 6)
        height = 4 + ((seed // 64) % 4)
        volume = length * width * height
        weight = round(max(1.0, volume / 200.0), 2)

        payload.append(
            {
                "warehouseNumber": default_warehouse,
                "itemNumber": row.ItemNumber,
                "quantity": int(row.Quantity),
                "length": float(length),
                "width": float(width),
                "height": float(height),
                "weight": float(weight),
                "isRepack": True,
                "isRepositional": True,
                "breakQuantity": 0,
                "marginalLength": default_marginal_length,
                "marginalHeight": default_marginal_height,
                "marginalWidth": default_marginal_width,
                "canBeNested": False,
                "volume": float(volume),
                "perishableType": "N",
            }
        )
    return payload


def mock_packaging_api_response(payload: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Build a mock API response using the published schema shape."""
    response: dict[str, dict[str, Any]] = {}
    for idx, item_payload in enumerate(payload, start=1):
        quantity = int(item_payload["quantity"])
        package_count = max(1, math.ceil(quantity / 4))
        box_dims = {
            "Width": float(item_payload["width"]) + 2.0,
            "Length": float(item_payload["length"]) + 2.0,
            "Height": float(item_payload["height"]) + 1.0,
        }

        response[f"package_{idx}"] = {
            "ContainedDimensions": {
                "Width": float(item_payload["width"]),
                "Length": float(item_payload["length"]),
                "Height": float(item_payload["height"]),
            },
            "ContainedItems": {item_payload["itemNumber"]: quantity},
            "Volume": float(item_payload["volume"]),
            "BoxDimensions": box_dims,
            "Weight": round(float(item_payload["weight"]) * package_count, 2),
            "IsTaped": True,
            "CumulativeVolume": float(item_payload["volume"]) * quantity,
            "TotalQuantity": quantity,
            "PackageCount": package_count,
            "ItemDetails": [
                {
                    "ItemNumber": item_payload["itemNumber"],
                    "IsCoolant": False,
                    "Quantity": quantity,
                    "Length": float(item_payload["length"]),
                    "Width": float(item_payload["width"]),
                    "Height": float(item_payload["height"]),
                    "Weight": float(item_payload["weight"]),
                }
            ],
        }
    return response


def normalize_packaging_response(response_json: dict[str, dict[str, Any]]) -> pd.DataFrame:
    """Normalize API response to tabular output."""
    rows: list[dict[str, Any]] = []
    for package_id, package in response_json.items():
        box_dims = package.get("BoxDimensions", {}) or {}
        package_count = int(package.get("PackageCount", 1) or 1)
        package_weight = float(package.get("Weight", 0.0) or 0.0)
        item_details = package.get("ItemDetails", []) or []

        for detail in item_details:
            rows.append(
                {
                    "PackageId": package_id,
                    "ItemNumber": str(detail.get("ItemNumber", "")),
                    "Quantity": int(detail.get("Quantity", 0) or 0),
                    "PackageCount": package_count,
                    "Length": float(box_dims.get("Length", 0.0) or 0.0),
                    "Width": float(box_dims.get("Width", 0.0) or 0.0),
                    "Height": float(box_dims.get("Height", 0.0) or 0.0),
                    "Weight": package_weight,
                }
            )

    if not rows:
        return pd.DataFrame(columns=PACKAGING_COLUMNS)
    return pd.DataFrame(rows, columns=PACKAGING_COLUMNS).sort_values(
        ["ItemNumber", "PackageId"]
    ).reset_index(drop=True)


def call_packaging_api(verified_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Input: dataframe with ItemNumber and Quantity (verified only), also create variables
    for all other variables needed by the query.
    Output: normalized dataframe of package results.
    Placeholder: return mock results with expected schema.
    """
    # TODO(API): Add authentication and real HTTP requests to configured endpoint.
    if verified_rows.empty:
        return pd.DataFrame(columns=PACKAGING_COLUMNS)

    destination_state = "FL"
    payload = build_packaging_payload(verified_rows, destination_state=destination_state)
    response_json = mock_packaging_api_response(payload)
    return normalize_packaging_response(response_json)


@st.cache_data(show_spinner=False)
def call_packaging_api_cached(verified_key: tuple[tuple[str, int], ...]) -> pd.DataFrame:
    verified_rows = pd.DataFrame(verified_key, columns=["ItemNumber", "Quantity"])
    return call_packaging_api(verified_rows)


# ============================================================
# PIPELINE
# ============================================================
def run_pipeline(standard_input_df: pd.DataFrame) -> dict[str, Any]:
    """Run the load -> verify -> split -> package workflow."""
    clean_df, row_errors = validate_and_aggregate_rows(standard_input_df)
    if clean_df.empty:
        return {
            "summary_df": pd.DataFrame(columns=SUMMARY_COLUMNS),
            "verified_df": pd.DataFrame(columns=SUMMARY_COLUMNS),
            "unverified_df": pd.DataFrame(columns=SUMMARY_COLUMNS),
            "packaging_df": pd.DataFrame(columns=PACKAGING_COLUMNS),
            "row_errors": row_errors or ["No valid rows found after validation."],
            "debug": {},
        }

    summary_df, verified_df, unverified_df = add_verification_flags(clean_df)

    packaging_df = pd.DataFrame(columns=PACKAGING_COLUMNS)
    payload_preview: list[dict[str, Any]] = []
    response_preview: dict[str, Any] = {}
    destination_state = "FL"
    if not verified_df.empty:
        verified_key = tuple(
            (row.ItemNumber, int(row.Quantity)) for row in verified_df.itertuples(index=False)
        )
        packaging_df = call_packaging_api_cached(verified_key)
        payload_preview = build_packaging_payload(verified_df, destination_state=destination_state)[:3]
        response_preview = mock_packaging_api_response(payload_preview[:1]) if payload_preview else {}

    LOGGER.info(
        "Packaging load complete | total=%s verified=%s unverified=%s errors=%s",
        len(summary_df),
        len(verified_df),
        len(unverified_df),
        len(row_errors),
    )

    debug_data = {
        "api_endpoint": PAGE_CONFIG.get("api", {}).get("endpoint", ""),
        "destination_state": destination_state,
        "payload_preview": payload_preview,
        "response_preview": response_preview,
    }
    return {
        "summary_df": summary_df,
        "verified_df": verified_df,
        "unverified_df": unverified_df,
        "packaging_df": packaging_df,
        "row_errors": row_errors,
        "debug": debug_data,
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
            runtime_cfg = load_packaging_config()
            runtime_ssas = runtime_cfg.get("ssas", {}) if isinstance(runtime_cfg.get("ssas"), dict) else {}
            page_ssas = PAGE_CONFIG.get("ssas", {}) if isinstance(PAGE_CONFIG.get("ssas"), dict) else {}
            LOGGER.info(
                "Load clicked | mode=%s rows=%s input_errors=%s page_enable_mock=%s runtime_enable_mock=%s runtime_has_query=%s",
                input_mode,
                len(standard_input_df),
                len(input_parse_errors),
                bool(page_ssas.get("enable_mock", True)),
                bool(runtime_ssas.get("enable_mock", True)),
                bool(str(runtime_ssas.get("query", "")).strip()),
            )
            if bool(page_ssas.get("enable_mock", True)) != bool(runtime_ssas.get("enable_mock", True)):
                LOGGER.warning(
                    "SSAS config mismatch between PAGE_CONFIG and current file config. "
                    "Clear Streamlit cache/restart to reload PAGE_CONFIG."
                )

            pipeline_results = run_pipeline(standard_input_df)
            combined_errors = input_parse_errors + pipeline_results.get("row_errors", [])

            st.session_state.pe_loaded = True
            st.session_state.pe_results = pipeline_results
            st.session_state.pe_errors = combined_errors
            LOGGER.info(
                "Load completed | summary=%s verified=%s unverified=%s row_errors=%s",
                len(pipeline_results.get("summary_df", pd.DataFrame())),
                len(pipeline_results.get("verified_df", pd.DataFrame())),
                len(pipeline_results.get("unverified_df", pd.DataFrame())),
                len(combined_errors),
            )

        except Exception as exc:
            LOGGER.exception("Packaging pipeline failed: %s", exc)
            st.session_state.pe_loaded = False
            st.session_state.pe_results = {}
            st.session_state.pe_errors = [f"Pipeline failed: {exc}"]


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
    summary_df = results.get("summary_df", pd.DataFrame(columns=SUMMARY_COLUMNS))
    verified_df = results.get("verified_df", pd.DataFrame(columns=SUMMARY_COLUMNS))
    unverified_df = results.get("unverified_df", pd.DataFrame(columns=SUMMARY_COLUMNS))
    packaging_df = results.get("packaging_df", pd.DataFrame(columns=PACKAGING_COLUMNS))
    debug_data = results.get("debug", {})

    st.divider()
    st.subheader("Input Summary", anchor=False)
    col_total, col_verified, col_unverified = st.columns(3)
    col_total.metric("Total Items", len(summary_df))
    col_verified.metric("Verified Items", len(verified_df))
    col_unverified.metric("Unverified Items", len(unverified_df))

    verified_col, unverified_col = st.columns(2)
    with verified_col:
        st.subheader("Verified Items", anchor=False)
        if verified_df.empty:
            st.info("No verified items found.")
        else:
            st.dataframe(
                verified_df[["ItemNumber", "Quantity"]],
                width="stretch",
                hide_index=True,
            )

    with unverified_col:
        st.subheader("Unverified Items", anchor=False)
        if unverified_df.empty:
            st.info("No unverified items found.")
        else:
            st.dataframe(
                unverified_df[["ItemNumber", "Quantity"]],
                width="stretch",
                hide_index=True,
            )

    st.divider()
    st.subheader("Packaging Results", anchor=False)
    if verified_df.empty:
        st.info("No verified items found. Packaging API was not called.")
    else:
        st.dataframe(packaging_df, width="stretch", hide_index=True)

    with st.expander("Debug (placeholder integration details)", expanded=False):
        st.write(f"API endpoint: `{debug_data.get('api_endpoint', '')}`")
        st.write(f"Destination state: `{debug_data.get('destination_state', '')}`")
        st.write("Payload preview:")
        st.json(debug_data.get("payload_preview", []))
        st.write("Response preview:")
        st.json(debug_data.get("response_preview", {}))
