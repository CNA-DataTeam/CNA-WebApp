"""
pages/sourcing-matrix.py

Streamlit page to run the Sourcing Matrix engine from the live
SharePoint-synced workbook and export a sourcing plan workbook.

This page imports the standalone package from the UNC engine root if it is
available on the current machine. It also supports a graceful message when
engine dependencies are not installed or the live workbook is not synced.
"""

from __future__ import annotations

import inspect
import os
import sys
from html import escape
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from textwrap import dedent
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import utils


ENGINE_ROOT = Path(
    r"\\therestaurantstore.com\920\Data\Reporting\Python Directory\Projects\Sourcing Matrix\sourcing_matrix_v4"
)
WORKBOOK_NAME = "Sourcing Matrix Export File.xlsx"
DEFAULT_WORKBOOK_PATH = Path.home() / "clarkinc.biz" / "Clark National Accounts - Resources" / WORKBOOK_NAME
KNOWN_WORKBOOK_PATHS = (
    DEFAULT_WORKBOOK_PATH,
    Path.home() / "clarkinc.biz" / "Clark National Accounts - Documents" / "Data and Analytics" / "Resources" / WORKBOOK_NAME,
)


def find_workbook_path() -> Path:
    """Find the SharePoint-synced Sourcing Matrix workbook on the current machine.

    Users may sync either the Resources folder directly or a higher-level SharePoint
    Documents folder, which creates different local folder structures. Prefer known
    paths first, then search under the current user's clarkinc.biz sync root.
    """
    env_path = os.environ.get("SOURCING_MATRIX_WORKBOOK_PATH")
    if env_path:
        candidate = Path(env_path).expanduser()
        if candidate.is_file():
            return candidate

    for candidate in KNOWN_WORKBOOK_PATHS:
        if candidate.is_file():
            return candidate

    search_root = Path.home() / "clarkinc.biz"
    if search_root.exists():
        try:
            matches = sorted(
                (path for path in search_root.rglob(WORKBOOK_NAME) if path.is_file()),
                key=lambda path: (len(path.parts), str(path).lower()),
            )
        except OSError:
            matches = []

        if matches:
            return matches[0]

    return DEFAULT_WORKBOOK_PATH


def render_workbook_missing_message() -> None:
    """Show actionable troubleshooting steps when the live workbook cannot be found."""
    st.warning(
        dedent(
            f"""
            Live Sourcing Matrix workbook not found.

            Open the **Troubleshooting guide** below and follow the **Workbook not found** steps.
            The most common fix is to sync the SharePoint Resources folder, open
            **{WORKBOOK_NAME}** once so it downloads locally, then rerun the tool.
            """
        ).strip()
    )


def render_troubleshooting_guide() -> None:
    """Render user-facing troubleshooting guidance for common Sourcing Matrix issues."""
    workbook_status = "Found" if WORKBOOK_PATH.exists() else "Missing"
    status_icon = "✅" if WORKBOOK_PATH.exists() else "⚠️"

    with st.expander("Troubleshooting guide", expanded=not WORKBOOK_PATH.exists()):
        st.markdown(
            dedent(
                f"""
                Use this section when the Sourcing Matrix page shows an error or warning.

                **Current workbook status:** {status_icon} {workbook_status}\
                **Detected workbook path:** `{WORKBOOK_PATH}`

                **Workbook not found / yellow workbook warning**

                1. Open the Clark National Accounts SharePoint folder.
                2. Navigate to **Documents > Data and Analytics > Resources**.
                3. Click **Sync** if the folder is not already synced.
                4. Open the synced folder in File Explorer.
                5. Open **{WORKBOOK_NAME}** once to make sure it is downloaded locally, then close it.
                6. Reopen or refresh the CNA Console and try again.
                7. If the warning still appears, right-click **{WORKBOOK_NAME}**, select **Copy as path**,
                   and compare that path to the detected workbook path above. If they are different, send
                   the copied path to marrocha@clarknationalaccounts.com along with any error message you are seeing.

                **Web order number not found**

                - Confirm the web order number was entered correctly.
                - Confirm the order was placed within the last 30 days. The live SharePoint workbook currently
                  only includes recent orders from that 30-day window.
                - Refresh/sync the SharePoint workbook if the order was placed recently and may not have been
                  pulled into the export file yet.

                **Workbook is open or locked**

                - Close **{WORKBOOK_NAME}** in Excel.
                - Wait a few seconds for OneDrive/SharePoint to finish syncing.
                - Run the Sourcing Matrix again.

                **Other issue**

                - Send the exact error message, the web order number used, and the detected workbook path above
                  to Mario Rocha.
                """
            ).strip()
        )


WORKBOOK_PATH = find_workbook_path()
LOGGER = utils.get_page_logger("Sourcing Matrix")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Sourcing Matrix")

# Shipping calculator API configuration
# Default from engine: https://shippingcalculator-api.dev.clarkinc.biz/calculate/calculateSF
# You can override with the SOURCING_MATRIX_SHIPPING_CALC_API_URL environment variable
# or configure below explicitly. Leave as None to use the engine's default (dev URL).
DEFAULT_SHIPPING_CALC_API_URL = "https://shippingcalculator-api.dev.clarkinc.biz/calculate/calculateSF"

HEADER_SUBTITLE = (
    "Generate the optimal sourcing plan for order(s) by comparing estimated sourcing costs "
    "across every staging warehouse."
)
DATA_AVAILABILITY_NOTE = (
    "Data availability note: the live SharePoint workbook used by this tool currently "
    "includes only orders placed in the last 30 days."
)
OLD_HEADER_SUBTITLES = {
    "Generate sourcing plans from the live SharePoint workbook.",
    "Generate sourcing plans from the live Sharepoint workbook.",
    "Generate sourcing plans from the live SharePoint workbook",
    "Generate sourcing plans from the live Sharepoint workbook",
}


def _patch_utils_header_subtitle() -> None:
    """Update the shared page-registry subtitle while preserving the standard CNA header."""
    seen: set[int] = set()

    def is_old_subtitle(value: Any) -> bool:
        return isinstance(value, str) and value.strip() in OLD_HEADER_SUBTITLES

    def replace_in_object(obj: Any) -> None:
        obj_id = id(obj)
        if obj_id in seen:
            return
        seen.add(obj_id)

        if isinstance(obj, dict):
            for key, value in list(obj.items()):
                if is_old_subtitle(value):
                    obj[key] = HEADER_SUBTITLE
                elif isinstance(value, (dict, list, tuple, set)) or hasattr(value, "__dict__"):
                    replace_in_object(value)
        elif isinstance(obj, list):
            for index, value in enumerate(list(obj)):
                if is_old_subtitle(value):
                    obj[index] = HEADER_SUBTITLE
                elif isinstance(value, (dict, list, tuple, set)) or hasattr(value, "__dict__"):
                    replace_in_object(value)
        elif isinstance(obj, set):
            replacement_needed = any(is_old_subtitle(value) for value in obj)
            if replacement_needed:
                obj.difference_update(OLD_HEADER_SUBTITLES)
                obj.add(HEADER_SUBTITLE)
            for value in list(obj):
                if isinstance(value, (dict, list, tuple, set)) or hasattr(value, "__dict__"):
                    replace_in_object(value)
        elif isinstance(obj, tuple):
            # Tuples are immutable; walk their nested values for mutable descendants.
            for value in obj:
                if isinstance(value, (dict, list, tuple, set)) or hasattr(value, "__dict__"):
                    replace_in_object(value)
        elif hasattr(obj, "__dict__") and not inspect.ismodule(obj) and not inspect.isfunction(obj) and not inspect.isclass(obj):
            for key, value in list(vars(obj).items()):
                if is_old_subtitle(value):
                    try:
                        setattr(obj, key, HEADER_SUBTITLE)
                    except Exception:
                        pass
                elif isinstance(value, (dict, list, tuple, set)) or hasattr(value, "__dict__"):
                    replace_in_object(value)

    for attr_name in dir(utils):
        if attr_name.startswith("__"):
            continue
        try:
            attr_value = getattr(utils, attr_name)
        except Exception:
            continue

        if is_old_subtitle(attr_value):
            try:
                setattr(utils, attr_name, HEADER_SUBTITLE)
            except Exception:
                pass
        elif isinstance(attr_value, (dict, list, tuple, set)) or hasattr(attr_value, "__dict__"):
            replace_in_object(attr_value)

    # Clear common cache decorators after patching registry-like objects.
    for attr_name in dir(utils):
        try:
            attr_value = getattr(utils, attr_name)
            cache_clear = getattr(attr_value, "cache_clear", None)
            if callable(cache_clear):
                cache_clear()
        except Exception:
            continue


def render_sourcing_matrix_page_header() -> None:
    """Render the standard CNA page header with the updated Sourcing Matrix subtitle.

    The shared console header owns the LOGISTICS/title/BETA styling. To keep that
    styling untouched, this wrapper only swaps the old subtitle text at render time.
    """
    _patch_utils_header_subtitle()

    def _replace_old_subtitle(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        updated = value
        for old_subtitle in OLD_HEADER_SUBTITLES:
            updated = updated.replace(old_subtitle, HEADER_SUBTITLE)
        return updated

    original_markdown = st.markdown
    original_write = st.write
    original_caption = st.caption

    def patched_markdown(body: Any, *args: Any, **kwargs: Any) -> Any:
        return original_markdown(_replace_old_subtitle(body), *args, **kwargs)

    def patched_write(*args: Any, **kwargs: Any) -> Any:
        patched_args = tuple(_replace_old_subtitle(arg) for arg in args)
        return original_write(*patched_args, **kwargs)

    def patched_caption(body: Any, *args: Any, **kwargs: Any) -> Any:
        return original_caption(_replace_old_subtitle(body), *args, **kwargs)

    try:
        st.markdown = patched_markdown  # type: ignore[assignment]
        st.write = patched_write  # type: ignore[assignment]
        st.caption = patched_caption  # type: ignore[assignment]

        # Prefer a native subtitle/description parameter if the shared utility supports one.
        try:
            signature = inspect.signature(utils.render_page_header)
            parameters = signature.parameters
            for subtitle_arg in ("subtitle", "description", "subline", "tagline"):
                if subtitle_arg in parameters:
                    utils.render_page_header(PAGE_TITLE, **{subtitle_arg: HEADER_SUBTITLE})
                    return
        except Exception:
            pass

        # Fallback: render the normal shared header while the output text is patched.
        utils.render_page_header(PAGE_TITLE)
    finally:
        st.markdown = original_markdown  # type: ignore[assignment]
        st.write = original_write  # type: ignore[assignment]
        st.caption = original_caption  # type: ignore[assignment]


def render_data_availability_note() -> None:
    """Render the compact 30-day data availability note below the page header."""
    st.markdown(
        """
        <style>
          .sm-data-availability-note {
            color: #64748B;
            font-size: 0.78rem;
            line-height: 1.35;
            margin: -0.65rem 0 1.05rem 0;
          }
          .sm-data-availability-note strong {
            color: #334E68;
            font-weight: 700;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="sm-data-availability-note"><strong>Data availability note:</strong> '
        f'{escape(DATA_AVAILABILITY_NOTE.replace("Data availability note: ", ""))}</div>',
        unsafe_allow_html=True,
    )


def ensure_engine_importable() -> tuple[bool, str]:
    """Ensure the standalone sourcing_matrix package can be imported."""
    if "sourcing_matrix" in sys.modules:
        return True, ""

    if ENGINE_ROOT.exists() and str(ENGINE_ROOT) not in sys.path:
        sys.path.insert(0, str(ENGINE_ROOT))

    try:
        import sourcing_matrix  # type: ignore
        return True, ""
    except Exception as exc:
        return False, str(exc or "Unknown import error")


def run_sourcing_export(
    excel_path: Path,
    order_number: str,
    additional_orders: list[str] | str | None,
    staging_warehouse: str | None,
    include_comparison: bool,
    call_api: bool,
    api_url: str | None,
    api_timeout: float,
    api_max_workers: int,
) -> tuple[str, bytes] | tuple[None, None]:
    """Run the sourcing engine and return the generated workbook bytes."""
    try:
        from sourcing_matrix.service import run_single_staging_export  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"Unable to import sourcing_matrix.service: {exc}") from exc

    with TemporaryDirectory(prefix="sourcing_matrix_") as temp_dir:
        output_name = f"SourcingPlan_{order_number}.xlsx"
        output_path = Path(temp_dir) / output_name
        product_kwargs: dict[str, Any] = {
            "excel_path": str(excel_path),
            "web_order_number": order_number,
            "additional_web_order_numbers": additional_orders,
            "staging_warehouse": staging_warehouse,
            "out_path": output_path,
            "include_all_staging_comparison": include_comparison,
            "call_api": call_api,
            "api_url": api_url,
            "api_timeout_seconds": api_timeout,
            "api_max_workers": api_max_workers,
        }
        result_path = run_single_staging_export(**product_kwargs)
        if not result_path.exists():
            raise RuntimeError(f"Sourcing engine did not produce expected output at: {result_path}")
        with open(result_path, "rb") as f:
            return result_path.name, f.read()


def trigger_download(file_name: str, file_bytes: bytes) -> None:
    """Store the generated workbook bytes in session state so the page can render a download button."""
    safe_name = file_name.replace('"', "_")
    st.session_state.sourcing_matrix_download_file_name = safe_name
    st.session_state.sourcing_matrix_download_bytes = file_bytes


def save_workbook_to_safe_location(file_name: str, file_bytes: bytes) -> Path:
    """Try the Desktop first, then Downloads, then a writable temp directory."""
    candidates = [
        Path.home() / "Desktop" / file_name,
        Path.home() / "Downloads" / file_name,
        Path(gettempdir()) / file_name,
    ]

    last_error: Exception | None = None
    for candidate in candidates:
        try:
            candidate.parent.mkdir(parents=True, exist_ok=True)
            candidate.write_bytes(file_bytes)
            return candidate
        except PermissionError as exc:
            last_error = exc
        except OSError as exc:
            last_error = exc

    if last_error is not None:
        raise PermissionError(
            f"Unable to save workbook to either Downloads or temp folder: {last_error}"
        ) from last_error

    raise RuntimeError("Unable to determine a writable destination for the workbook.")




def parse_additional_orders(value: str) -> list[str]:
    """Parse comma/semicolon/newline separated web order numbers from the page input."""
    if not value:
        return []
    parts = []
    for chunk in value.replace(";", ",").replace("\n", ",").split(","):
        order = chunk.strip().strip("'").strip('"')
        if order and order not in parts:
            parts.append(order)
    return parts


def _summary_sheet_to_dict(file_bytes: bytes) -> dict[str, Any]:
    """Read the SourcingPlan label/value summary into a dictionary."""
    try:
        summary = pd.read_excel(
            BytesIO(file_bytes),
            sheet_name="SourcingPlan",
            header=None,
            usecols="B:C",
        )
    except Exception:
        return {}

    values: dict[str, Any] = {}
    for _, row in summary.iterrows():
        label = row.iloc[0]
        value = row.iloc[1] if len(row) > 1 else None
        if pd.isna(label):
            continue
        label_text = str(label).strip()
        if not label_text or label_text in {"Sourcing Plan Summary", "API Execution", "Warehouse", "Label"}:
            continue
        values[label_text] = value
    return values


def _format_money(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return "N/A"
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def _format_percent(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.1%}"


def _to_float(value: Any) -> float | None:
    """Best-effort conversion for workbook numeric values."""
    try:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, str):
            cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
            if cleaned.upper().startswith("N/A") or cleaned == "":
                return None
            return float(cleaned)
        return float(value)
    except Exception:
        return None


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    if number is None:
        return None
    return int(round(number))


def _format_delta_money(value: float | None) -> str:
    if value is None:
        return None
    sign = "+" if value > 0 else ""
    return f"{sign}${value:,.2f} vs recommended"


def _format_delta_percent(value: float | None) -> str:
    if value is None:
        return None
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1%} vs recommended"


def _format_delta_points(value: float | None) -> str:
    if value is None:
        return None
    sign = "+" if value > 0 else ""
    return f"{sign}{value * 100:.1f} pts vs recommended"


def _format_delta_count(value: float | int | None, suffix: str) -> str | None:
    if value is None:
        return None
    number = int(round(float(value)))
    sign = "+" if number > 0 else ""
    return f"{sign}{number} {suffix} vs recommended"


def _read_sheet(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _determine_total_project_items(file_bytes: bytes, item_level: pd.DataFrame) -> int | None:
    """Determine the denominator for Available Items % in scenario cards/table."""
    product_validation = _read_sheet(file_bytes, "ProductDimensionValidation")
    if not product_validation.empty and "ItemNumber" in product_validation.columns:
        return int(product_validation["ItemNumber"].dropna().astype(str).str.strip().nunique())

    if not item_level.empty and "ItemNumber" in item_level.columns:
        rows = item_level.copy()
        if "Warehouse" in rows.columns:
            rows = rows[~rows["Warehouse"].astype(str).str.strip().eq("Kits")]
        return int(rows["ItemNumber"].dropna().astype(str).str.strip().nunique())

    return None


def _build_scenario_table(file_bytes: bytes, summary: dict[str, Any]) -> pd.DataFrame:
    """Build one scenario-level table used by both cards and ranking display."""
    cost_summary = _read_sheet(file_bytes, "ScenarioCostSummary")
    scenario_comparison = _read_sheet(file_bytes, "ScenarioComparison")
    item_level = _read_sheet(file_bytes, "ItemLevel")
    total_project_items = _determine_total_project_items(file_bytes, item_level)

    if cost_summary.empty:
        return pd.DataFrame()

    table = cost_summary.copy()
    if "ScenarioStagingWarehouse" not in table.columns:
        return pd.DataFrame()

    table["ScenarioStagingWarehouse"] = table["ScenarioStagingWarehouse"].astype(str).str.strip()

    if not scenario_comparison.empty and "StagingWarehouse" in scenario_comparison.columns:
        comparison = scenario_comparison.copy()
        comparison["StagingWarehouse"] = comparison["StagingWarehouse"].astype(str).str.strip()
        comparison_cols = [
            col for col in [
                "StagingWarehouse",
                "DistinctWarehousesUsed",
                "UnavailableItemCount",
                "KitItemCount",
                "TotalTransferDistance",
                "UsedWarehouses",
            ]
            if col in comparison.columns
        ]
        table = table.merge(
            comparison[comparison_cols].drop_duplicates(subset=["StagingWarehouse"]),
            how="left",
            left_on="ScenarioStagingWarehouse",
            right_on="StagingWarehouse",
        )

    # Normalize common numeric columns for cards/ranking calculations.
    for col in [
        "CostRank",
        "TotalEstimatedShippingCost",
        "SourceToStagingCost",
        "StagingToDestinationCost",
        "TransferTruckCostRemoved",
        "DistinctWarehousesUsed",
        "UnavailableItemCount",
        "KitItemCount",
        "TotalTransferDistance",
        "PayloadCount",
        "OKPayloadCount",
    ]:
        if col in table.columns:
            table[col] = table[col].apply(_to_float)

    if total_project_items:
        unavailable = table.get("UnavailableItemCount", pd.Series([None] * len(table))).fillna(0)
        table["AvailableItemsPct"] = (total_project_items - unavailable) / total_project_items
        table["TotalProjectItems"] = total_project_items
    else:
        table["AvailableItemsPct"] = None
        table["TotalProjectItems"] = None

    if "CostRank" in table.columns:
        table = table.sort_values("CostRank", na_position="last")

    recommended_staging = str(summary.get("Cost-Based Recommended Staging") or "").strip()
    if recommended_staging:
        recommended_rows = table[table["ScenarioStagingWarehouse"].eq(recommended_staging)]
    else:
        recommended_rows = pd.DataFrame()
    if recommended_rows.empty and "CostRank" in table.columns:
        recommended_rows = table[table["CostRank"].eq(table["CostRank"].min())]

    if not recommended_rows.empty:
        recommended = recommended_rows.iloc[0]
        rec_cost = _to_float(recommended.get("TotalEstimatedShippingCost"))
        rec_wh_count = _to_float(recommended.get("DistinctWarehousesUsed"))
        rec_available_pct = _to_float(recommended.get("AvailableItemsPct"))

        table["CostDeltaVsRecommended"] = table["TotalEstimatedShippingCost"].apply(
            lambda value: None if rec_cost in (None, 0) or _to_float(value) is None else _to_float(value) - rec_cost
        )
        table["CostPctDeltaVsRecommended"] = table["TotalEstimatedShippingCost"].apply(
            lambda value: None if rec_cost in (None, 0) or _to_float(value) is None else (_to_float(value) - rec_cost) / rec_cost
        )
        table["DistinctWarehousesDeltaVsRecommended"] = table["DistinctWarehousesUsed"].apply(
            lambda value: None if rec_wh_count is None or _to_float(value) is None else _to_float(value) - rec_wh_count
        )
        table["AvailableItemsPctDeltaVsRecommended"] = table["AvailableItemsPct"].apply(
            lambda value: None if rec_available_pct is None or _to_float(value) is None else _to_float(value) - rec_available_pct
        )
    else:
        table["CostDeltaVsRecommended"] = None
        table["CostPctDeltaVsRecommended"] = None
        table["DistinctWarehousesDeltaVsRecommended"] = None
        table["AvailableItemsPctDeltaVsRecommended"] = None

    return table


def _build_ranking_display(scenario_table: pd.DataFrame) -> pd.DataFrame:
    """Create the user-facing ranking table shown in the console page."""
    if scenario_table.empty:
        return pd.DataFrame()

    desired_cols = [
        "CostRank",
        "ScenarioStagingWarehouse",
        "TotalEstimatedShippingCost",
        "CostPctDeltaVsRecommended",
        "DistinctWarehousesUsed",
        "DistinctWarehousesDeltaVsRecommended",
        "SourceToStagingCost",
        "StagingToDestinationCost",
        "TransferTruckCostRemoved",
    ]
    existing_cols = [col for col in desired_cols if col in scenario_table.columns]
    display = scenario_table[existing_cols].copy()
    display = display.rename(columns={
        "CostRank": "Rank",
        "ScenarioStagingWarehouse": "Staging WH",
        "TotalEstimatedShippingCost": "Estimated Cost",
        "CostPctDeltaVsRecommended": "Cost Change vs Recommended",
        "DistinctWarehousesUsed": "Distinct WH Used",
        "DistinctWarehousesDeltaVsRecommended": "WH Change vs Recommended",
        "SourceToStagingCost": "Source → Staging",
        "StagingToDestinationCost": "Staging → Destination",
        "TransferTruckCostRemoved": "Transfer Truck Savings",
    })

    if "Rank" in display.columns:
        display["Rank"] = display["Rank"].apply(lambda value: "" if _to_int(value) is None else _to_int(value))
    if "Estimated Cost" in display.columns:
        display["Estimated Cost"] = display["Estimated Cost"].apply(_format_money)
    if "Cost Change vs Recommended" in display.columns:
        display["Cost Change vs Recommended"] = display["Cost Change vs Recommended"].apply(_format_delta_percent)
    if "Distinct WH Used" in display.columns:
        display["Distinct WH Used"] = display["Distinct WH Used"].apply(lambda value: "N/A" if _to_int(value) is None else _to_int(value))
    if "WH Change vs Recommended" in display.columns:
        display["WH Change vs Recommended"] = display["WH Change vs Recommended"].apply(lambda value: _format_delta_count(value, "WH"))
    for col in ["Source → Staging", "Staging → Destination", "Transfer Truck Savings"]:
        if col in display.columns:
            display[col] = display[col].apply(_format_money)

    return display


def _format_warehouse_value(value: Any) -> str:
    """Format warehouse values read from Excel as whole-number strings when possible."""
    if value is None or pd.isna(value):
        return ""
    number = _to_int(value)
    if number is not None:
        return str(number)
    return str(value).strip()


def _format_leg_type(value: Any) -> str:
    """Make leg types easier to read in the console table."""
    raw = "" if value is None or pd.isna(value) else str(value).strip()
    normalized = raw.replace(" ", "").lower()
    if normalized == "sourcetostaging":
        return "Source To Staging"
    if normalized == "stagingtodestination":
        return "Staging To Destination"
    return raw


def _build_flow_detail(file_bytes: bytes, selected_staging: str) -> pd.DataFrame:
    """Build a selected-scenario leg table that will later feed the map view."""
    detail = _read_sheet(file_bytes, "ScenarioCostDetail")
    if detail.empty or "ScenarioStagingWarehouse" not in detail.columns:
        return pd.DataFrame()

    rows = detail[detail["ScenarioStagingWarehouse"].astype(str).str.strip().eq(str(selected_staging))].copy()
    if rows.empty:
        return pd.DataFrame()

    cols = [
        "LegType",
        "SourceWarehouse",
        "DestinationWarehouse",
        "SelectedMethod",
        "AppliedSourcingCost",
        "FreeTransferTruckApplied",
        "TransferTruckCostRemoved",
        "ProductLineCount",
        "TotalQuantity",
    ]
    existing = [col for col in cols if col in rows.columns]
    rows = rows[existing].copy()

    if "FreeTransferTruckApplied" in rows.columns:
        free_transfer = rows["FreeTransferTruckApplied"].apply(
            lambda value: str(value).strip().lower() in {"true", "1", "yes", "y"}
            if isinstance(value, str)
            else bool(value) if not pd.isna(value) else False
        )
        if "SelectedMethod" in rows.columns:
            rows.loc[free_transfer, "SelectedMethod"] = "Waived Transfer Truck"
        if "AppliedSourcingCost" in rows.columns:
            rows.loc[free_transfer, "AppliedSourcingCost"] = 0

    rows = rows.rename(columns={
        "LegType": "Leg Type",
        "SourceWarehouse": "Source WH",
        "DestinationWarehouse": "Destination WH",
        "SelectedMethod": "Selected Method",
        "AppliedSourcingCost": "Applied Cost",
        "TransferTruckCostRemoved": "Transfer Truck Savings",
        "ProductLineCount": "Product Lines",
        "TotalQuantity": "Total Qty",
    })
    if "Leg Type" in rows.columns:
        rows["Leg Type"] = rows["Leg Type"].apply(_format_leg_type)
    for col in ["Source WH", "Destination WH"]:
        if col in rows.columns:
            rows[col] = rows[col].apply(_format_warehouse_value)
    for col in ["Applied Cost", "Transfer Truck Savings"]:
        if col in rows.columns:
            rows[col] = rows[col].apply(_format_money)
    for col in ["Product Lines", "Total Qty"]:
        if col in rows.columns:
            rows[col] = rows[col].apply(lambda value: "N/A" if _to_int(value) is None else _to_int(value))
    if "FreeTransferTruckApplied" in rows.columns:
        rows = rows.drop(columns=["FreeTransferTruckApplied"])
    return rows


def _is_fee_or_nonphysical_item(item_number: Any) -> bool:
    """Identify known non-physical staging/service fee items that should not drive transfers."""
    if item_number is None or pd.isna(item_number):
        return False
    normalized = str(item_number).strip().upper()
    if not normalized:
        return False
    # STG1T / STG5T style fee lines have no product dimensions and should not be
    # interpreted as physical inventory that needs to move through the staging plan.
    return normalized.endswith(("1T", "5T")) or "1T" in normalized or "5T" in normalized


def _normalize_column_name(value: Any) -> str:
    """Normalize generated/live workbook column names for resilient matching."""
    text = "" if value is None else str(value).strip()
    if "[" in text and "]" in text:
        text = text[text.rfind("[") + 1:text.rfind("]")]
    text = text.strip("[]")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _find_column_by_normalized_name(df_or_columns: Any, candidates: list[str]) -> str | None:
    """Find a dataframe column by comparing normalized names."""
    columns = list(getattr(df_or_columns, "columns", df_or_columns))
    candidate_keys = {_normalize_column_name(candidate) for candidate in candidates}
    for col in columns:
        if _normalize_column_name(col) in candidate_keys:
            return col
    return None


def _description_lookup_from_sheet(df: pd.DataFrame) -> dict[str, str]:
    """Return ItemNumber -> description when a sheet includes an item description column."""
    if df.empty:
        return {}

    item_col = _find_column_by_normalized_name(
        df,
        [
            "ItemNumber",
            "Item Number",
            "Item",
            "ProductNumber",
            "Product Number",
        ],
    )
    description_col = _find_column_by_normalized_name(
        df,
        [
            "ItemDescription",
            "Item Description",
            "Description",
            "ProductDescription",
            "Product Description",
            "ItemName",
            "Item Name",
            "ItemDesc",
            "Item Desc",
            "ProductName",
            "Product Name",
            "ProductTitle",
            "Product Title",
            "ItemTitle",
            "Item Title",
            "Name",
            "ShortDescription",
            "Short Description",
            "LongDescription",
            "Long Description",
            "Item Description USE",
            "ItemDescriptionUSE",
            "Title",
            "Product Display Name",
            "Display Name",
        ],
    )
    if not item_col or not description_col:
        return {}

    rows = df[[item_col, description_col]].copy()
    rows[item_col] = rows[item_col].astype(str).str.strip()
    rows[description_col] = rows[description_col].fillna("").astype(str).str.strip()
    rows = rows[rows[item_col].ne("") & rows[description_col].ne("")]
    if rows.empty:
        return {}

    return rows.drop_duplicates(subset=[item_col]).set_index(item_col)[description_col].to_dict()


@st.cache_data(show_spinner=False)
def _description_lookup_from_workbook_path(workbook_path_text: str, modified_at: float | None) -> dict[str, str]:
    """Return ItemNumber -> description from the live SharePoint export workbook when available.

    Power BI connected-table sheets often have title/blank rows above the actual
    header row. This reader scans the first rows of each sheet to find the row
    containing ItemNumber and ItemDescription instead of assuming row 1 headers.
    """
    workbook_path = Path(workbook_path_text)
    if not workbook_path.is_file():
        return {}

    preferred_sheets = [
        "ItemShippingDimensions",
        "SourcingMatrixBase",
        "sourcingmatrixbase",
        "SourcingMatrixBase30d",
        "sourcingmatrixbase30d",
        "OrderedItems",
        "Ordered Items",
        "CNA Orders",
    ]

    lookup: dict[str, str] = {}
    try:
        excel_file = pd.ExcelFile(workbook_path)
    except Exception:
        return lookup

    sheet_names = list(excel_file.sheet_names)
    ordered_sheet_names = [sheet for sheet in preferred_sheets if sheet in sheet_names]
    ordered_sheet_names.extend([sheet for sheet in sheet_names if sheet not in ordered_sheet_names])

    item_candidates = ["ItemNumber", "Item Number", "Item", "ProductNumber", "Product Number"]
    description_candidates = [
        "ItemDescription",
        "Item Description",
        "Description",
        "ProductDescription",
        "Product Description",
        "ItemName",
        "Item Name",
        "ItemDesc",
        "Item Desc",
        "ProductName",
        "Product Name",
        "ProductTitle",
        "Product Title",
        "ItemTitle",
        "Item Title",
        "Name",
        "ShortDescription",
        "Short Description",
        "LongDescription",
        "Long Description",
        "Item Description USE",
        "ItemDescriptionUSE",
        "Title",
        "Product Display Name",
        "Display Name",
    ]

    for sheet_name in ordered_sheet_names:
        header_row_index: int | None = None
        try:
            preview = pd.read_excel(workbook_path, sheet_name=sheet_name, header=None, nrows=25)
        except Exception:
            continue

        for idx, row in preview.iterrows():
            candidate_columns = [_normalize_column_name(value) for value in row.tolist()]
            if not any(candidate_columns):
                continue
            if (
                _find_column_by_normalized_name(candidate_columns, item_candidates) is not None
                and _find_column_by_normalized_name(candidate_columns, description_candidates) is not None
            ):
                header_row_index = int(idx)
                break

        if header_row_index is None:
            continue

        try:
            rows = pd.read_excel(workbook_path, sheet_name=sheet_name, header=header_row_index)
        except Exception:
            continue

        lookup.update(_description_lookup_from_sheet(rows))
        if lookup:
            # Stop at the first useful source so a less specific sheet does not overwrite descriptions.
            break

    return lookup

def _description_lookup_from_live_workbook() -> dict[str, str]:
    """Return ItemNumber -> description from the current live workbook path."""
    try:
        modified_at = WORKBOOK_PATH.stat().st_mtime if WORKBOOK_PATH.is_file() else None
    except Exception:
        modified_at = None
    return _description_lookup_from_workbook_path(str(WORKBOOK_PATH), modified_at)


EXCLUDED_MOVEMENT_TYPE = "Unavailable / Not Included / Excluded"


def _normalize_transfer_movement_type(value: Any) -> str:
    """Group non-movement lines together for a simpler transfer outline."""
    text = "" if value is None or pd.isna(value) else str(value).strip()
    if text in {"Unavailable / Not Included", "Fee / Excluded", "Kit / Excluded"}:
        return EXCLUDED_MOVEMENT_TYPE
    return text or EXCLUDED_MOVEMENT_TYPE


def _reason_for_not_available(item_number: Any, movement_type: str, missing_reason: str | None = None) -> str:
    """Return a short user-facing reason for non-available / excluded rows."""
    item_text = "" if item_number is None or pd.isna(item_number) else str(item_number).strip().upper()
    movement_text = "" if movement_type is None else str(movement_type).strip()

    if _is_fee_or_nonphysical_item(item_text):
        return (
            "No weight for item. Expected for item numbers representing service fees, "
            "custom items, or other non-physical lines."
        )
    if movement_text == "Kit / Excluded":
        return "Kit line excluded from item-level transfer movement."
    if missing_reason:
        return missing_reason
    return "Not enough stock available at eligible source warehouses."


def _build_transfer_cost_lookup(cost_detail: pd.DataFrame, selected_staging: str) -> dict[tuple[str, str], float | None]:
    """Return (source WH, staging WH) -> estimated transfer cost for the selected scenario."""
    if cost_detail.empty or "ScenarioStagingWarehouse" not in cost_detail.columns or "LegType" not in cost_detail.columns:
        return {}

    rows = cost_detail.copy()
    rows["ScenarioStagingWarehouse"] = rows["ScenarioStagingWarehouse"].apply(_format_warehouse_value)
    rows["LegType"] = rows["LegType"].astype(str).str.strip()
    rows = rows[
        rows["ScenarioStagingWarehouse"].eq(str(selected_staging).strip())
        & rows["LegType"].eq("SourceToStaging")
    ].copy()
    if rows.empty or "SourceWarehouse" not in rows.columns:
        return {}

    rows["Source WH"] = rows["SourceWarehouse"].apply(_format_warehouse_value)
    if "DestinationWarehouse" in rows.columns:
        rows["Staging WH"] = rows["DestinationWarehouse"].apply(_format_warehouse_value)
    else:
        rows["Staging WH"] = str(selected_staging).strip()

    def row_transfer_cost(row: pd.Series) -> float | None:
        free_value = row.get("FreeTransferTruckApplied") if "FreeTransferTruckApplied" in row.index else False
        is_free = (
            str(free_value).strip().lower() in {"true", "1", "yes", "y"}
            if isinstance(free_value, str)
            else bool(free_value) if not pd.isna(free_value) else False
        )
        if is_free:
            return 0.0
        for cost_col in ["AppliedSourcingCost", "SelectedCost"]:
            if cost_col in row.index:
                cost = _to_float(row.get(cost_col))
                if cost is not None:
                    return cost
        return None

    costs_by_key: dict[tuple[str, str], list[float | None]] = {}
    for _, row in rows.iterrows():
        key = (str(row.get("Source WH", "")).strip(), str(row.get("Staging WH", "")).strip())
        costs_by_key.setdefault(key, []).append(row_transfer_cost(row))

    result: dict[tuple[str, str], float | None] = {}
    for key, values in costs_by_key.items():
        numeric_values = [value for value in values if value is not None]
        result[key] = sum(numeric_values) if numeric_values else None
    return result


def _build_transfer_outline(file_bytes: bytes, selected_staging: str) -> pd.DataFrame:
    """Build a selected-scenario item movement table from generated workbook sheets.

    ScenarioPayloadPreview provides scenario-specific source-to-staging item
    movements and the final staging-to-destination item list. ItemLevel identifies
    unavailable and kit rows, while ProductDimensionValidation provides requested
    quantities and missing-dimension status for all ordered items.
    """
    if not file_bytes:
        return pd.DataFrame()

    payload_preview = _read_sheet(file_bytes, "ScenarioPayloadPreview")
    scenario_items = _read_sheet(file_bytes, "ScenarioItemWarehouses")
    item_level = _read_sheet(file_bytes, "ItemLevel")
    product_validation = _read_sheet(file_bytes, "ProductDimensionValidation")
    scenario_cost_detail = _read_sheet(file_bytes, "ScenarioCostDetail")

    if product_validation.empty and payload_preview.empty and scenario_items.empty and item_level.empty:
        return pd.DataFrame()

    selected_staging_text = str(selected_staging).strip()
    transfer_cost_lookup = _build_transfer_cost_lookup(scenario_cost_detail, selected_staging_text)

    quantity_lookup: dict[str, Any] = {}
    dimension_status_lookup: dict[str, str] = {}
    exclusion_reason_lookup: dict[str, str] = {}
    description_lookup: dict[str, str] = _description_lookup_from_live_workbook()

    for candidate_sheet in [product_validation, payload_preview, scenario_items, item_level]:
        description_lookup.update(_description_lookup_from_sheet(candidate_sheet))

    if not product_validation.empty and "ItemNumber" in product_validation.columns:
        products = product_validation.copy()
        products["ItemNumber"] = products["ItemNumber"].astype(str).str.strip()
        if "TotalQuantity" in products.columns:
            quantity_lookup = products.set_index("ItemNumber")["TotalQuantity"].to_dict()

        for _, row in products.iterrows():
            item_number = str(row.get("ItemNumber", "")).strip()
            if not item_number:
                continue

            missing_critical_value = row.get("MissingCriticalProductFields") if "MissingCriticalProductFields" in products.columns else False
            missing_critical = False if pd.isna(missing_critical_value) else bool(missing_critical_value)
            status = "" if pd.isna(row.get("Status")) else str(row.get("Status")).strip()
            missing_fields = "" if pd.isna(row.get("MissingFieldList")) else str(row.get("MissingFieldList")).strip()

            if missing_critical or status.lower().startswith("missing"):
                if _is_fee_or_nonphysical_item(item_number):
                    dimension_status_lookup[item_number] = "Fee / Excluded"
                    exclusion_reason_lookup[item_number] = _reason_for_not_available(item_number, "Fee / Excluded")
                elif missing_fields:
                    dimension_status_lookup[item_number] = f"Needs Review - missing {missing_fields}"
                    exclusion_reason_lookup[item_number] = f"Missing product information: {missing_fields}"
                else:
                    dimension_status_lookup[item_number] = "Needs Review - missing product dimensions"
                    exclusion_reason_lookup[item_number] = "Missing product dimensions needed by the shipping calculator."

    unavailable_items: set[str] = set()
    kit_items: set[str] = set()
    if not item_level.empty and {"Warehouse", "ItemNumber"}.issubset(item_level.columns):
        item_rows = item_level.copy()
        item_rows["Warehouse"] = item_rows["Warehouse"].astype(str).str.strip()
        item_rows["ItemNumber"] = item_rows["ItemNumber"].astype(str).str.strip()
        unavailable_items = set(
            item_rows.loc[item_rows["Warehouse"].eq("Unavailable Items"), "ItemNumber"]
            .dropna()
            .astype(str)
            .str.strip()
        )
        kit_items = set(
            item_rows.loc[item_rows["Warehouse"].eq("Kits"), "ItemNumber"]
            .dropna()
            .astype(str)
            .str.strip()
        )

    outline_rows: list[dict[str, Any]] = []
    transferred_items: set[str] = set()
    final_item_quantities: dict[str, Any] = {}
    nonphysical_quantities: dict[str, Any] = {}

    def add_outline_row(
        movement_type: str,
        source_wh: str,
        item_number: str,
        qty: Any,
        estimated_transfer_cost: Any = None,
        reason: str | None = None,
    ) -> None:
        normalized_movement_type = _normalize_transfer_movement_type(movement_type)
        row: dict[str, Any] = {
            "Movement Type": normalized_movement_type,
            "Source WH": source_wh,
            "Staging WH": selected_staging_text,
            "Item Number": item_number,
            "Item Description": description_lookup.get(item_number, "N/A"),
            "Qty": qty,
        }
        if estimated_transfer_cost is not None:
            row["Estimated Transfer Cost"] = estimated_transfer_cost
        if normalized_movement_type == EXCLUDED_MOVEMENT_TYPE:
            row["Reason for Not Available"] = reason or _reason_for_not_available(item_number, movement_type)
        outline_rows.append(row)


    if not payload_preview.empty and {"ScenarioStagingWarehouse", "LegType", "ItemNumber", "Quantity"}.issubset(payload_preview.columns):
        payload_rows = payload_preview.copy()
        payload_rows["ScenarioStagingWarehouse"] = payload_rows["ScenarioStagingWarehouse"].apply(_format_warehouse_value)
        payload_rows["LegType"] = payload_rows["LegType"].astype(str).str.strip()
        payload_rows["ItemNumber"] = payload_rows["ItemNumber"].astype(str).str.strip()
        payload_rows = payload_rows[payload_rows["ScenarioStagingWarehouse"].eq(selected_staging_text)].copy()

        if not payload_rows.empty:
            fee_rows = payload_rows[payload_rows["ItemNumber"].apply(_is_fee_or_nonphysical_item)].copy()
            if not fee_rows.empty:
                nonphysical_quantities = fee_rows.groupby("ItemNumber")["Quantity"].max().to_dict()

        physical_payload_rows = payload_rows[~payload_rows["ItemNumber"].apply(_is_fee_or_nonphysical_item)].copy()

        final_rows = physical_payload_rows[physical_payload_rows["LegType"].eq("StagingToDestination")].copy()
        if not final_rows.empty:
            final_item_quantities = final_rows.groupby("ItemNumber")["Quantity"].sum().to_dict()

        transfer_rows = physical_payload_rows[physical_payload_rows["LegType"].eq("SourceToStaging")].copy()
        if not transfer_rows.empty:
            transfer_rows["Source WH"] = transfer_rows.get("SourceWarehouse", "").apply(_format_warehouse_value)
            grouped_transfers = (
                transfer_rows
                .groupby(["Source WH", "ItemNumber"], as_index=False)
                .agg({"Quantity": "sum"})
            )

            for _, row in grouped_transfers.iterrows():
                item_number = str(row.get("ItemNumber", "")).strip()
                if not item_number:
                    continue
                transferred_items.add(item_number)
                movement_type = dimension_status_lookup.get(item_number, "Transfer To Staging")
                if movement_type == "Fee / Excluded":
                    nonphysical_quantities[item_number] = quantity_lookup.get(item_number, row.get("Quantity"))
                    continue
                if movement_type.startswith("Needs Review"):
                    movement_type = "Needs Review"
                source_wh_text = str(row.get("Source WH", ""))
                transfer_cost = transfer_cost_lookup.get((source_wh_text, selected_staging_text))
                add_outline_row(
                    movement_type="Transfer To Staging" if movement_type == "Transfer To Staging" else movement_type,
                    source_wh=source_wh_text,
                    item_number=item_number,
                    qty=row.get("Quantity"),
                    estimated_transfer_cost=transfer_cost,
                )

    # Fallback for older exports that do not include ScenarioPayloadPreview.
    if not transferred_items and not scenario_items.empty and {"OrderID", "ItemNumber", "WarehouseNumber"}.issubset(scenario_items.columns):
        scenario_rows = scenario_items.copy()
        scenario_rows["OrderID"] = scenario_rows["OrderID"].astype(str)
        scenario_rows["ScenarioStagingWarehouse"] = scenario_rows["OrderID"].str.extract(r"^\d+_(\d+)_", expand=False).fillna("")
        scenario_rows = scenario_rows[scenario_rows["ScenarioStagingWarehouse"].astype(str).str.strip().eq(selected_staging_text)].copy()

        if not scenario_rows.empty:
            scenario_rows["ItemNumber"] = scenario_rows["ItemNumber"].astype(str).str.strip()
            scenario_rows["Source WH"] = scenario_rows["WarehouseNumber"].apply(_format_warehouse_value)
            qty_col = "TotalQuantityFromWarehouse" if "TotalQuantityFromWarehouse" in scenario_rows.columns else None

            for _, row in scenario_rows.iterrows():
                item_number = str(row.get("ItemNumber", "")).strip()
                if not item_number:
                    continue
                qty = quantity_lookup.get(item_number, row.get(qty_col) if qty_col else None)
                if _is_fee_or_nonphysical_item(item_number):
                    nonphysical_quantities[item_number] = qty
                    continue
                transferred_items.add(item_number)
                source_wh_text = str(row.get("Source WH", ""))
                transfer_cost = transfer_cost_lookup.get((source_wh_text, selected_staging_text))
                add_outline_row(
                    movement_type="Transfer To Staging",
                    source_wh=source_wh_text,
                    item_number=item_number,
                    qty=qty,
                    estimated_transfer_cost=transfer_cost,
                )

    all_item_numbers: set[str] = set()
    if final_item_quantities:
        all_item_numbers = set(final_item_quantities.keys())
    elif quantity_lookup:
        all_item_numbers = set(quantity_lookup.keys())
    elif not item_level.empty and "ItemNumber" in item_level.columns:
        all_item_numbers = set(item_level["ItemNumber"].dropna().astype(str).str.strip())

    for item_number in sorted(all_item_numbers):
        if item_number in transferred_items or item_number in unavailable_items or item_number in kit_items:
            continue
        if _is_fee_or_nonphysical_item(item_number):
            nonphysical_quantities[item_number] = quantity_lookup.get(item_number, final_item_quantities.get(item_number))
            continue

        qty = final_item_quantities.get(item_number, quantity_lookup.get(item_number))
        movement_type = "Needs Review" if item_number in dimension_status_lookup else "Sourced From Staging"
        add_outline_row(
            movement_type=movement_type,
            source_wh=selected_staging_text,
            item_number=item_number,
            qty=qty,
        )

    for item_number in sorted(unavailable_items):
        add_outline_row(
            movement_type="Unavailable / Not Included",
            source_wh="N/A",
            item_number=item_number,
            qty=quantity_lookup.get(item_number, "N/A"),
            reason=exclusion_reason_lookup.get(
                item_number,
                _reason_for_not_available(item_number, "Unavailable / Not Included"),
            ),
        )

    for item_number in sorted(nonphysical_quantities):
        add_outline_row(
            movement_type="Fee / Excluded",
            source_wh="N/A",
            item_number=item_number,
            qty=quantity_lookup.get(item_number, nonphysical_quantities.get(item_number, "N/A")),
            reason=exclusion_reason_lookup.get(
                item_number,
                _reason_for_not_available(item_number, "Fee / Excluded"),
            ),
        )

    for item_number in sorted(kit_items):
        add_outline_row(
            movement_type="Kit / Excluded",
            source_wh="N/A",
            item_number=item_number,
            qty=quantity_lookup.get(item_number, "N/A"),
            reason=exclusion_reason_lookup.get(
                item_number,
                _reason_for_not_available(item_number, "Kit / Excluded"),
            ),
        )

    if not outline_rows:
        return pd.DataFrame()

    display = pd.DataFrame(outline_rows)

    movement_order = {
        "Transfer To Staging": 1,
        "Sourced From Staging": 2,
        "Needs Review": 3,
        EXCLUDED_MOVEMENT_TYPE: 4,
    }
    display["_MovementSort"] = display["Movement Type"].map(movement_order).fillna(99)

    if "Qty" in display.columns:
        display["Qty"] = display["Qty"].apply(lambda value: "N/A" if _to_int(value) is None else _to_int(value))
    if "Estimated Transfer Cost" in display.columns:
        display["Estimated Transfer Cost"] = display["Estimated Transfer Cost"].apply(_to_float)

    display = display.sort_values(["_MovementSort", "Source WH", "Item Number"]).drop(columns=["_MovementSort"])
    desired_columns = [
        "Movement Type",
        "Source WH",
        "Staging WH",
        "Item Number",
        "Item Description",
        "Qty",
        "Estimated Transfer Cost",
        "Reason for Not Available",
    ]
    return display[[col for col in desired_columns if col in display.columns]]


def _build_transfer_outline_summary(transfer_outline: pd.DataFrame) -> pd.DataFrame:
    """Summarize item-level movements into one row per transfer/source group."""
    if transfer_outline.empty:
        return pd.DataFrame()

    rows = transfer_outline.copy()
    rows["_QtyNumeric"] = rows["Qty"].apply(_to_float).fillna(0)
    if "Estimated Transfer Cost" in rows.columns:
        rows["_TransferCostNumeric"] = rows["Estimated Transfer Cost"].apply(_to_float)
    else:
        rows["_TransferCostNumeric"] = None

    summary = (
        rows
        .groupby(["Movement Type", "Source WH", "Staging WH"], as_index=False)
        .agg(
            **{
                "Item Lines": ("Item Number", "nunique"),
                "Total Qty": ("_QtyNumeric", "sum"),
                "Estimated Transfer Cost": ("_TransferCostNumeric", "max"),
            }
        )
    )

    movement_order = {
        "Transfer To Staging": 1,
        "Sourced From Staging": 2,
        "Needs Review": 3,
        EXCLUDED_MOVEMENT_TYPE: 4,
    }
    summary["_MovementSort"] = summary["Movement Type"].map(movement_order).fillna(99)
    summary["Total Qty"] = summary["Total Qty"].apply(lambda value: int(round(float(value))) if pd.notna(value) else "N/A")
    summary["Estimated Transfer Cost"] = summary.apply(
        lambda row: _format_money(row["Estimated Transfer Cost"])
        if row["Movement Type"] == "Transfer To Staging" and _to_float(row["Estimated Transfer Cost"]) is not None
        else "N/A",
        axis=1,
    )
    return summary.sort_values(["_MovementSort", "Source WH"]).drop(columns=["_MovementSort"])


def _render_scrollable_detail_table(df: pd.DataFrame) -> None:
    """Render centered item details with scrolling so long item lists are not cut off."""
    if df.empty:
        return

    display = df.reset_index(drop=True).fillna("N/A").copy()
    for col in ["Estimated Transfer Cost"]:
        if col in display.columns:
            display[col] = display[col].apply(lambda value: "N/A" if _to_float(value) is None else _format_money(value))

    height = min(max(96 + len(display.index) * 34, 170), 500)
    html = display.to_html(index=False, escape=True, classes="sm-detail-table")
    components.html(
        f"""
        <html>
        <head>
        <style>
          html, body {{
            margin: 0;
            padding: 0;
            font-family: 'Aptos', 'Segoe UI', Arial, sans-serif;
            color: #0F172A;
            background: transparent;
            overflow: hidden;
          }}
          .detail-wrap {{
            width: 100%;
            max-height: {height - 24}px;
            overflow: auto;
            border: 1px solid #BFE8EA;
            border-radius: 6px;
          }}
          table.sm-detail-table {{
            width: 100%;
            border-collapse: collapse;
            table-layout: auto;
            font-size: 14px;
          }}
          table.sm-detail-table thead th {{
            position: sticky;
            top: 0;
            z-index: 1;
            background: #D9F0F2;
            color: #334E68;
            font-weight: 700;
            text-align: center !important;
            vertical-align: middle !important;
            border: 1px solid #BFE8EA;
            padding: 8px 8px;
            white-space: nowrap;
          }}
          table.sm-detail-table tbody td {{
            text-align: center !important;
            vertical-align: middle !important;
            border: 1px solid #BFE8EA;
            padding: 8px 8px;
          }}
          table.sm-detail-table tbody tr:nth-child(even) {{
            background: #FBFEFF;
          }}
          table.sm-detail-table tbody tr:hover {{
            background: #F1FBFC;
          }}
        </style>
        </head>
        <body>
          <div class="detail-wrap">{html}</div>
        </body>
        </html>
        """,
        height=height,
        scrolling=False,
    )


def render_transfer_outline(transfer_outline: pd.DataFrame) -> None:
    """Render a grouped transfer outline with expandable item-level details."""
    if transfer_outline.empty:
        return

    st.markdown("#### Transfer outline")
    st.caption(
        "This view summarizes each source-to-staging movement and lets you expand a movement group "
        "to see the specific items, descriptions, and quantities. Unavailable, not-included, and "
        "excluded fee/service lines such as STG1T or STG5T are grouped together because they are "
        "not physical inventory movements."
    )

    summary = _build_transfer_outline_summary(transfer_outline)
    if summary.empty:
        return

    _render_centered_table(summary)
    st.caption("Open a movement group below to review the item-level detail.")

    base_detail_columns = ["Item Number", "Item Description", "Qty"]

    for _, group_row in summary.iterrows():
        movement_type = str(group_row.get("Movement Type", "")).strip()
        source_wh = str(group_row.get("Source WH", "")).strip()
        staging_wh = str(group_row.get("Staging WH", "")).strip()
        item_lines = _to_int(group_row.get("Item Lines"))
        total_qty = _to_int(group_row.get("Total Qty"))

        detail_rows = transfer_outline[
            transfer_outline["Movement Type"].astype(str).eq(movement_type)
            & transfer_outline["Source WH"].astype(str).eq(source_wh)
            & transfer_outline["Staging WH"].astype(str).eq(staging_wh)
        ].copy()
        if detail_rows.empty:
            continue

        if source_wh and source_wh != "N/A":
            movement_label = f"{movement_type}: {source_wh} → {staging_wh}"
        else:
            movement_label = f"{movement_type}: {staging_wh}"

        item_text = "N/A" if item_lines is None else f"{item_lines} item(s)"
        qty_text = "N/A" if total_qty is None else f"Qty {total_qty}"
        detail_columns = list(base_detail_columns)
        if movement_type == EXCLUDED_MOVEMENT_TYPE:
            detail_columns.append("Reason for Not Available")

        with st.expander(f"{movement_label} • {item_text} • {qty_text}"):
            _render_scrollable_detail_table(detail_rows[[col for col in detail_columns if col in detail_rows.columns]])


def _render_selected_scenario_cost_warning(selected: pd.Series) -> None:
    """Show a neutral note when some rows do not return shipping-calculator costs."""
    complete_value = selected.get("CompleteCostScenario") if "CompleteCostScenario" in selected.index else None
    missing_payloads = _to_int(selected.get("PayloadsMissingAppliedCost")) if "PayloadsMissingAppliedCost" in selected.index else None
    issue_list = "" if pd.isna(selected.get("IssueList")) else str(selected.get("IssueList")).strip()

    complete = True
    if complete_value is not None and not pd.isna(complete_value):
        if isinstance(complete_value, str):
            complete = complete_value.strip().lower() in {"true", "1", "yes", "y"}
        else:
            complete = bool(complete_value)

    if complete and not missing_payloads and not issue_list:
        return

    missing_text = f" {missing_payloads} payload(s) did not return an applied shipping cost." if missing_payloads else ""
    issue_text = ""
    if issue_list:
        issue_preview = issue_list if len(issue_list) <= 240 else f"{issue_list[:237]}..."
        issue_text = f" Calculator note: {issue_preview}"

    note_html = (
        "<div style='border-left: 4px solid #94A3B8; background: #F8FAFC; "
        "padding: 0.75rem 0.9rem; margin: 0.65rem 0 1rem 0; "
        "color: #334155; font-size: 0.88rem; line-height: 1.45;'>"
        "<strong>Note:</strong> Some item rows did not return a shipping-calculator cost."
        f"{escape(missing_text + issue_text)} "
        "This is expected when non-physical fee/service items such as <strong>STG1T</strong> "
        "or <strong>STG5T</strong> are included. Those rows are shown under "
        f"<strong>{escape(EXCLUDED_MOVEMENT_TYPE)}</strong> in the Transfer outline and are not "
        "treated as physical inventory movements."
        "</div>"
    )
    st.markdown(note_html, unsafe_allow_html=True)


def _normalize_location_key(value: Any) -> str:
    """Normalize warehouse numbers / ZIP codes read from Excel."""
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if text.endswith(".0"):
        text = text[:-2]
    return text.replace(" ", "")


# Approximate coordinates for warehouses and common destination ZIP/postal codes.
# These are used as a no-internet fallback. When pgeocode is installed, the page
# can also resolve US/Canada postal codes dynamically.
STATIC_LOCATION_COORDINATES: dict[str, tuple[float, float]] = {
    "104": (39.2804, -76.5305),  # Baltimore, MD
    "110": (29.7355, -94.9774),  # Baytown, TX
    "851": (40.0379, -76.3055),  # Lancaster, PA
    "853": (39.2371, -119.5929),  # Dayton, NV
    "854": (34.0039, -96.3708),  # Durant, OK
    "855": (39.5096, -76.1641),  # Aberdeen, MD
    "856": (32.1237, -81.4854),  # Ellabell, GA
    "857": (39.5613, -119.4716),  # McCarran, NV
    "871": (39.6529, -78.7625),  # Cumberland, MD
    "872": (31.5785, -84.1557),  # Albany, GA
    "873": (39.2371, -119.5929),  # Dayton, NV
    "875": (37.3281, -87.4989),  # Madisonville, KY
    "876": (41.0037, -76.4549),  # Bloomsburg, PA
    "17601": (40.0379, -76.3055),
    "89403": (39.2371, -119.5929),
    "89437": (39.5613, -119.4716),
    "74701": (34.0039, -96.3708),
    "31705": (31.5785, -84.1557),
    "42431": (37.3281, -87.4989),
    "21224": (39.2804, -76.5305),
    "21001": (39.5096, -76.1641),
    "21502": (39.6529, -78.7625),
    "17815": (41.0037, -76.4549),
    "77523": (29.7355, -94.9774),
    "31308": (32.1237, -81.4854),
    "32829": (28.4830, -81.2490),  # Orlando, FL
    "C1N4K7": (46.3982, -63.7895),  # Summerside, PEI
}


@st.cache_data(show_spinner=False)
def _postal_code_coordinates(postal_code: str, country: str | None) -> tuple[float, float] | None:
    """Resolve postal-code coordinates using pgeocode if available, otherwise static fallback."""
    postal_key = _normalize_location_key(postal_code)
    if not postal_key:
        return None

    if postal_key in STATIC_LOCATION_COORDINATES:
        return STATIC_LOCATION_COORDINATES[postal_key]

    # Canadian postal codes are sometimes better resolved by the first three chars.
    if len(postal_key) >= 3 and postal_key[:3] in STATIC_LOCATION_COORDINATES:
        return STATIC_LOCATION_COORDINATES[postal_key[:3]]

    country_code = (str(country).strip().upper() if country else "US")
    if country_code not in {"US", "CA"}:
        return None

    try:
        import pgeocode  # type: ignore
    except Exception:
        return None

    try:
        query_code = postal_key[:3] if country_code == "CA" else postal_key[:5]
        result = pgeocode.Nominatim(country_code).query_postal_code(query_code)
        lat = getattr(result, "latitude", None)
        lon = getattr(result, "longitude", None)
        if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
            return None
        return float(lat), float(lon)
    except Exception:
        return None


def _coordinates_from_row(
    row: pd.Series,
    warehouse_col: str | None,
    city_col: str | None,
    state_col: str | None,
    zip_col: str | None,
    country_col: str | None,
) -> tuple[float, float] | None:
    """Resolve coordinates from a payload row using warehouse number, then ZIP/postal code."""
    if warehouse_col and warehouse_col in row:
        warehouse_key = _normalize_location_key(row.get(warehouse_col))
        if warehouse_key in STATIC_LOCATION_COORDINATES:
            return STATIC_LOCATION_COORDINATES[warehouse_key]

    postal_code = row.get(zip_col) if zip_col and zip_col in row else None
    country = row.get(country_col) if country_col and country_col in row else None
    coords = _postal_code_coordinates(str(postal_code), str(country) if country is not None else None)
    if coords:
        return coords

    city = str(row.get(city_col, "")).strip() if city_col and city_col in row and pd.notna(row.get(city_col)) else ""
    state = str(row.get(state_col, "")).strip() if state_col and state_col in row and pd.notna(row.get(state_col)) else ""
    lookup_key = _normalize_location_key(f"{city},{state}")
    return STATIC_LOCATION_COORDINATES.get(lookup_key)


def _build_map_rows(file_bytes: bytes, selected_staging: str) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Build point and route data for the selected scenario map."""
    payload = _read_sheet(file_bytes, "ScenarioPayloadPreview")
    detail = _read_sheet(file_bytes, "ScenarioCostDetail")
    if payload.empty or "ScenarioStagingWarehouse" not in payload.columns:
        return pd.DataFrame(), pd.DataFrame(), 0

    payload_rows = payload[
        payload["ScenarioStagingWarehouse"].astype(str).str.strip().eq(str(selected_staging))
    ].copy()
    if payload_rows.empty:
        return pd.DataFrame(), pd.DataFrame(), 0

    if not detail.empty and "PayloadId" in detail.columns and "PayloadId" in payload_rows.columns:
        detail_rows = detail[
            detail["ScenarioStagingWarehouse"].astype(str).str.strip().eq(str(selected_staging))
        ].copy()
        leg_keys = [
            "PayloadId",
            "LegType",
            "SourceWarehouse",
            "DestinationWarehouse",
            "AppliedSourcingCost",
            "TransferTruckCostRemoved",
            "ProductLineCount",
            "TotalQuantity",
        ]
        detail_rows = detail_rows[[col for col in leg_keys if col in detail_rows.columns]].drop_duplicates(
            subset=["PayloadId"] if "PayloadId" in detail_rows.columns else None
        )
        first_payload = payload_rows.drop_duplicates(subset=["PayloadId"]) if "PayloadId" in payload_rows.columns else payload_rows.drop_duplicates()
        rows = detail_rows.merge(first_payload, how="left", on="PayloadId", suffixes=("", "_payload"))
    else:
        rows = payload_rows.drop_duplicates(subset=["PayloadId"]) if "PayloadId" in payload_rows.columns else payload_rows.drop_duplicates()

    points: dict[str, dict[str, Any]] = {}
    routes: list[dict[str, Any]] = []
    missing_routes = 0

    def add_point(key: str, label: str, role: str, coords: tuple[float, float] | None, hover: str) -> None:
        if not coords:
            return
        if key not in points:
            points[key] = {
                "Label": label,
                "Role": role,
                "Latitude": coords[0],
                "Longitude": coords[1],
                "Hover": hover,
            }

    for _, row in rows.iterrows():
        leg_type = str(row.get("LegType", row.get("LegType_payload", ""))).strip()
        source_wh = _normalize_location_key(row.get("SourceWarehouse", row.get("SourceWarehouse_payload")))
        dest_wh = _normalize_location_key(row.get("DestinationWarehouse", row.get("DestinationWarehouse_payload")))
        is_final_leg = leg_type == "StagingToDestination" or not dest_wh

        source_coords = _coordinates_from_row(
            row,
            "SourceWarehouse",
            "SourceCity",
            "SourceState",
            "SourceZipCode",
            "DestinationCountry",
        )

        if is_final_leg:
            dest_coords = _coordinates_from_row(
                row,
                None,
                "DestinationCity",
                "DestinationState",
                "DestinationZipCode",
                "DestinationCountry",
            )
            dest_label = "Customer Destination"
            dest_role = "Destination"
            dest_city = row.get("DestinationCity", "")
            dest_zip = row.get("DestinationZipCode", "")
            dest_hover = f"Customer Destination<br>{dest_city} {dest_zip}"
        else:
            dest_coords = _coordinates_from_row(
                row,
                "DestinationWarehouse",
                "DestinationCity",
                "DestinationState",
                "DestinationZipCode",
                "DestinationCountry",
            )
            dest_label = f"Staging WH {dest_wh}" if dest_wh == str(selected_staging) else f"Warehouse {dest_wh}"
            dest_role = "Staging" if dest_wh == str(selected_staging) else "Warehouse"
            dest_hover = f"{dest_label}<br>{row.get('DestinationCity', '')} {row.get('DestinationZipCode', '')}"

        source_role = "Staging" if source_wh == str(selected_staging) else "Source"
        source_label = f"Staging WH {source_wh}" if source_role == "Staging" else f"Source WH {source_wh}"
        source_hover = f"{source_label}<br>{row.get('SourceCity', '')} {row.get('SourceZipCode', '')}"

        add_point(f"WH_{source_wh}", source_label, source_role, source_coords, source_hover)
        add_point("DESTINATION" if is_final_leg else f"WH_{dest_wh}", dest_label, dest_role, dest_coords, dest_hover)

        if not source_coords or not dest_coords:
            missing_routes += 1
            continue

        applied_cost = _to_float(row.get("AppliedSourcingCost"))
        transfer_removed = _to_float(row.get("TransferTruckCostRemoved")) or 0
        product_lines = _to_int(row.get("ProductLineCount"))
        total_qty = _to_int(row.get("TotalQuantity"))
        route_label = (
            f"{leg_type}<br>{source_label} → {dest_label}"
            f"<br>Applied cost: {_format_money(applied_cost)}"
            f"<br>Product lines: {'N/A' if product_lines is None else product_lines}"
            f"<br>Total qty: {'N/A' if total_qty is None else total_qty}"
        )
        if transfer_removed:
            route_label += f"<br>Transfer truck savings: {_format_money(transfer_removed)}"

        routes.append({
            "LegType": leg_type,
            "SourceLabel": source_label,
            "DestinationLabel": dest_label,
            "SourceLatitude": source_coords[0],
            "SourceLongitude": source_coords[1],
            "DestinationLatitude": dest_coords[0],
            "DestinationLongitude": dest_coords[1],
            "Hover": route_label,
        })

    return pd.DataFrame(points.values()), pd.DataFrame(routes), missing_routes


def render_scenario_map(summary_payload: dict[str, Any], selected_staging: str) -> None:
    """Render an interactive route map for the selected staging scenario."""
    try:
        import plotly.graph_objects as go  # type: ignore
    except Exception:
        st.info(
            "Map view is available after the optional `plotly` package is installed. "
            "The sourcing summary and Excel output are still available."
        )
        return

    file_bytes = summary_payload.get("file_bytes")
    if not file_bytes:
        return

    point_df, route_df, missing_routes = _build_map_rows(file_bytes, str(selected_staging))
    if point_df.empty or route_df.empty:
        st.info(
            "Map view is not available for this scenario because route coordinates could not be resolved. "
            "The shipment leg table below still shows the source-to-staging and staging-to-destination legs."
        )
        return

    fig = go.Figure()

    for _, route in route_df.iterrows():
        is_final = route["LegType"] == "StagingToDestination"
        fig.add_trace(go.Scattergeo(
            lon=[route["SourceLongitude"], route["DestinationLongitude"]],
            lat=[route["SourceLatitude"], route["DestinationLatitude"]],
            mode="lines",
            line={
                "width": 4 if is_final else 2,
                "color": "#2E7D32" if is_final else "#1E88E5",
                "dash": "solid" if is_final else "dot",
            },
            opacity=0.85 if is_final else 0.6,
            hoverinfo="text",
            text=route["Hover"],
            name="Staging → Destination" if is_final else "Source → Staging transfer",
            showlegend=False,
        ))

    role_styles = {
        "Source": {"color": "#1E88E5", "size": 10, "symbol": "circle"},
        "Staging": {"color": "#2E7D32", "size": 18, "symbol": "square"},
        "Destination": {"color": "#C62828", "size": 14, "symbol": "diamond"},
        "Warehouse": {"color": "#6A1B9A", "size": 10, "symbol": "circle"},
    }
    for role, rows in point_df.groupby("Role"):
        style = role_styles.get(role, role_styles["Warehouse"])
        fig.add_trace(go.Scattergeo(
            lon=rows["Longitude"],
            lat=rows["Latitude"],
            mode="markers+text",
            text=rows["Label"],
            textposition="top center",
            hoverinfo="text",
            hovertext=rows["Hover"],
            marker={
                "size": style["size"],
                "color": style["color"],
                "symbol": style["symbol"],
                "line": {"width": 1, "color": "white"},
            },
            name=role,
        ))

    lat_values = pd.concat([route_df["SourceLatitude"], route_df["DestinationLatitude"], point_df["Latitude"]]).dropna()
    lon_values = pd.concat([route_df["SourceLongitude"], route_df["DestinationLongitude"], point_df["Longitude"]]).dropna()

    # Use explicit lat/lon ranges instead of Plotly's fitbounds so the route map
    # fills the available console width more naturally in full-screen mode.
    lat_min = float(lat_values.min())
    lat_max = float(lat_values.max())
    lon_min = float(lon_values.min())
    lon_max = float(lon_values.max())
    lat_pad = max((lat_max - lat_min) * 0.18, 1.5)
    lon_pad = max((lon_max - lon_min) * 0.18, 3.0)

    fig.update_layout(
        height=500,
        autosize=True,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        legend={"orientation": "h", "yanchor": "bottom", "y": 0.01, "xanchor": "left", "x": 0.01},
        geo={
            "domain": {"x": [0, 1], "y": [0, 1]},
            "scope": "north america",
            "projection_type": "mercator",
            "showland": True,
            "landcolor": "#F5F5F5",
            "showocean": True,
            "oceancolor": "#E3F2FD",
            "showlakes": True,
            "lakecolor": "#E3F2FD",
            "showcountries": True,
            "countrycolor": "#BDBDBD",
            "showsubunits": True,
            "subunitcolor": "#DDDDDD",
            "lataxis": {"range": [lat_min - lat_pad, lat_max + lat_pad]},
            "lonaxis": {"range": [lon_min - lon_pad, lon_max + lon_pad]},
        },
    )

    st.markdown("#### Selected scenario route map")
    st.caption(
        "Dotted blue lines show source-to-staging transfer legs. The solid green line shows the final staging-to-destination shipment. The green square marks the selected staging warehouse. "
        "The map updates when you select a different staging scenario."
    )
    st.plotly_chart(fig, use_container_width=True, config={"responsive": True, "displayModeBar": False})

    if missing_routes:
        st.caption(
            f"Note: {missing_routes} route leg(s) could not be mapped because coordinates were unavailable. "
            "Those legs still appear in the shipment legs table below."
        )



def _is_nonzero_delta(value: Any) -> bool:
    """Return True when a scenario comparison delta should be displayed."""
    number = _to_float(value)
    return number is not None and abs(number) > 0.000001


def _comparison_color(delta: float | None, positive_is_good: bool) -> str:
    """Return a red/green color for comparison values."""
    if delta is None or not _is_nonzero_delta(delta):
        return "#334155"
    is_good = (delta > 0 and positive_is_good) or (delta < 0 and not positive_is_good)
    return "#2E7D32" if is_good else "#C62828"


def _comparison_arrow(delta: float | None) -> str:
    """Return an up/down arrow for non-zero comparison values."""
    if delta is None or not _is_nonzero_delta(delta):
        return ""
    return "▲" if delta > 0 else "▼"


def _format_abs_money(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"${abs(value):,.2f}"


def _format_abs_percent(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{abs(value):.1%}"


def _format_abs_points(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{abs(value) * 100:.1f} pts"


def _format_abs_count(value: float | int | None, suffix: str) -> str:
    if value is None:
        return "N/A"
    return f"{abs(int(round(float(value))))} {suffix}"


def _comparison_line_html(delta: float | int | None, display_text: str, positive_is_good: bool) -> str:
    """Return small comparison HTML for card subtitles."""
    number = _to_float(delta)
    if number is None or not _is_nonzero_delta(number):
        return ""
    color = _comparison_color(number, positive_is_good)
    arrow = _comparison_arrow(number)
    return f'<span style="color:{color}; font-weight:700;">{arrow} {escape(display_text)}</span><span style="color:#64748B;"> vs recommended</span>'


def _comparison_main_value_html(value_text: str, delta: float | int | None, positive_is_good: bool) -> str:
    """Return main metric value HTML with an arrow embedded when there is a comparison delta."""
    number = _to_float(delta)
    safe_value = escape(value_text)
    if number is None or not _is_nonzero_delta(number):
        return safe_value
    color = _comparison_color(number, positive_is_good)
    arrow = _comparison_arrow(number)
    return f'<span style="color:{color};">{arrow} {safe_value}</span>'


def _inject_metric_card_css() -> None:
    """Inject consistent card styling for the sourcing recommendation summary."""
    st.markdown(
        """
        <style>
        .sm-metric-card {
            border: 1px solid #BFE8EA;
            border-top: 3px solid #1FB5AD;
            background: #FFFFFF;
            padding: 0.9rem 1rem;
            min-height: 118px;
            height: 118px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            box-sizing: border-box;
            margin-bottom: 0.7rem;
        }
        .sm-metric-label {
            color: #334E68;
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            line-height: 1.15;
        }
        .sm-metric-value {
            color: #243B53;
            font-size: 1.65rem;
            font-weight: 800;
            line-height: 1.1;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .sm-metric-subtitle {
            color: #64748B;
            font-size: 0.76rem;
            min-height: 1.05rem;
            line-height: 1.2;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_metric_card(title: str, value_html: str, subtitle_html: str = "") -> None:
    """Render a consistent metric card using HTML so cards keep equal heights."""
    st.markdown(
        f"""
        <div class="sm-metric-card">
          <div class="sm-metric-label">{escape(title)}</div>
          <div class="sm-metric-value">{value_html}</div>
          <div class="sm-metric-subtitle">{subtitle_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_centered_table(df: pd.DataFrame) -> None:
    """Render a compact static table with centered values and no index column."""
    if df is None or df.empty:
        return

    try:
        display = df.reset_index(drop=True).fillna("").copy()
        html = display.to_html(index=False, escape=False, classes="sm-centered-table")
        row_count = len(display.index)
        # Keep the embedded HTML frame close to the actual table height so Streamlit
        # does not leave a large blank area below short tables.
        height = min(max(72 + (row_count * 36), 150), 520)
        components.html(
            f"""
            <html>
            <head>
            <style>
              html, body {{
                margin: 0;
                padding: 0;
                overflow-x: auto;
                overflow-y: hidden;
                font-family: 'Aptos', 'Segoe UI', Arial, sans-serif;
                color: #0F172A;
                background: transparent;
              }}
              .table-wrap {{
                width: 100%;
                overflow-x: auto;
                padding-bottom: 4px;
              }}
              table.sm-centered-table {{
                width: 100%;
                min-width: 860px;
                border-collapse: collapse;
                table-layout: auto;
                font-size: 14px;
              }}
              table.sm-centered-table thead th {{
                background: #D9F0F2;
                color: #334E68;
                font-weight: 700;
                text-align: center !important;
                vertical-align: middle !important;
                border: 1px solid #BFE8EA;
                padding: 7px 8px;
                white-space: nowrap;
              }}
              table.sm-centered-table tbody td {{
                text-align: center !important;
                vertical-align: middle !important;
                border: 1px solid #BFE8EA;
                padding: 7px 8px;
                white-space: nowrap;
              }}
              table.sm-centered-table tbody tr:nth-child(even) {{
                background: #FBFEFF;
              }}
              table.sm-centered-table tbody tr:hover {{
                background: #F1FBFC;
              }}
            </style>
            </head>
            <body>
              <div class="table-wrap">{html}</div>
            </body>
            </html>
            """,
            height=height,
            scrolling=False,
        )
    except Exception:
        try:
            st.dataframe(df.reset_index(drop=True), use_container_width=True, hide_index=True)
        except TypeError:
            st.dataframe(df.reset_index(drop=True), use_container_width=True)


def _build_console_summary(file_bytes: bytes) -> dict[str, Any]:
    """Extract the key values we want to show inside the console page."""
    summary = _summary_sheet_to_dict(file_bytes)
    scenario_table = _build_scenario_table(file_bytes, summary)
    ranking_display = _build_ranking_display(scenario_table)

    recommended_staging = summary.get("Cost-Based Recommended Staging")
    recommended_cost = summary.get("Cost-Based Recommended Staging Cost")

    # If the workbook summary is missing a recommended value, fall back to rank 1.
    if (not recommended_staging or pd.isna(recommended_staging)) and not scenario_table.empty:
        rank_one = scenario_table.head(1).iloc[0]
        recommended_staging = rank_one.get("ScenarioStagingWarehouse")
        recommended_cost = rank_one.get("TotalEstimatedShippingCost")

    return {
        "summary": summary,
        "recommended_staging": None if recommended_staging is None else str(recommended_staging).strip(),
        "recommended_cost": recommended_cost,
        "scenario_table": scenario_table,
        "ranking": ranking_display,
        "file_bytes": file_bytes,
    }


def render_console_summary(summary_payload: dict[str, Any]) -> None:
    """Render an interactive in-app summary after the workbook is generated."""
    if not summary_payload:
        return

    st.markdown("### Sourcing recommendation summary")
    st.caption(
        "The recommended staging warehouse is selected by default. Use the scenario selector below "
        "to review another staging option; the cards and detail table will update to match your selection."
    )

    scenario_table = summary_payload.get("scenario_table")
    if not isinstance(scenario_table, pd.DataFrame) or scenario_table.empty:
        # Fallback to the older static summary if scenario-level data is unavailable.
        col1, col2 = st.columns(2)
        col1.metric("Recommended Staging WH", str(summary_payload.get("recommended_staging") or "N/A"))
        col2.metric("Estimated Sourcing Cost", _format_money(summary_payload.get("recommended_cost")))
        return

    options = scenario_table["ScenarioStagingWarehouse"].dropna().astype(str).tolist()
    recommended_staging = str(summary_payload.get("recommended_staging") or "").strip()
    default_index = options.index(recommended_staging) if recommended_staging in options else 0

    selected_staging = st.selectbox(
        "Review staging scenario",
        options,
        index=default_index,
        format_func=lambda value: f"{value} (recommended)" if value == recommended_staging else value,
        key="sourcing_matrix_selected_staging_scenario",
    )

    selected_rows = scenario_table[scenario_table["ScenarioStagingWarehouse"].astype(str).eq(str(selected_staging))]
    if selected_rows.empty:
        st.warning("Selected staging scenario could not be found in the generated workbook.")
        return

    selected = selected_rows.iloc[0]
    selected_cost = _to_float(selected.get("TotalEstimatedShippingCost"))
    selected_wh_count = _to_int(selected.get("DistinctWarehousesUsed"))
    selected_available_pct = _to_float(selected.get("AvailableItemsPct"))
    unavailable_items = _to_int(selected.get("UnavailableItemCount"))

    cost_delta = _to_float(selected.get("CostDeltaVsRecommended"))
    cost_pct_delta = _to_float(selected.get("CostPctDeltaVsRecommended"))
    wh_delta = _to_float(selected.get("DistinctWarehousesDeltaVsRecommended"))
    available_delta = _to_float(selected.get("AvailableItemsPctDeltaVsRecommended"))

    _inject_metric_card_css()

    selected_subtitle = ""
    if str(selected_staging) != recommended_staging and recommended_staging:
        selected_subtitle = f"Recommended WH is {escape(recommended_staging)}"

    cost_delta_line = _comparison_line_html(cost_delta, _format_abs_money(cost_delta), positive_is_good=False)
    wh_delta_line = _comparison_line_html(wh_delta, _format_abs_count(wh_delta, "WH"), positive_is_good=False)
    available_delta_line = _comparison_line_html(available_delta, _format_abs_points(available_delta), positive_is_good=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        _render_metric_card("Selected Staging WH", escape(str(selected_staging)), selected_subtitle)
    with col2:
        _render_metric_card("Estimated Sourcing Cost", escape(_format_money(selected_cost)), cost_delta_line)
    with col3:
        _render_metric_card(
            "Distinct Warehouses Used",
            escape("N/A" if selected_wh_count is None else str(selected_wh_count)),
            wh_delta_line,
        )
    with col4:
        _render_metric_card("Available Items %", escape(_format_percent(selected_available_pct)), available_delta_line)

    cost_pct_value = "0.0%" if not _is_nonzero_delta(cost_pct_delta) else _format_abs_percent(cost_pct_delta)
    cost_pct_html = _comparison_main_value_html(cost_pct_value, cost_pct_delta, positive_is_good=False)

    sub_col1, sub_col2, sub_col3 = st.columns(3)
    with sub_col1:
        _render_metric_card("Unavailable Items", escape("N/A" if unavailable_items is None else str(unavailable_items)))
    with sub_col2:
        _render_metric_card("Cost Difference", cost_pct_html, "vs recommended")
    with sub_col3:
        _render_metric_card("Transfer Truck Savings", escape(_format_money(selected.get("TransferTruckCostRemoved"))))

    _render_selected_scenario_cost_warning(selected)

    render_scenario_map(summary_payload, str(selected_staging))

    transfer_outline = _build_transfer_outline(summary_payload.get("file_bytes"), str(selected_staging))
    render_transfer_outline(transfer_outline)

    ranking = summary_payload.get("ranking")
    if isinstance(ranking, pd.DataFrame) and not ranking.empty:
        st.markdown("#### Estimated cost ranking by staging warehouse")
        _render_centered_table(ranking)

    flow_detail = _build_flow_detail(summary_payload.get("file_bytes"), str(selected_staging))
    if not flow_detail.empty:
        st.markdown("#### Selected scenario shipment legs")
        st.caption(
            "This leg-level view shows the selected scenario's transfer and final shipment legs. "
            "Source-to-staging legs represent warehouse transfers; staging-to-destination is the final shipment."
        )
        _render_centered_table(flow_detail)



st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon=utils.get_app_icon())
utils.render_app_logo()
utils.log_page_open_once("sourcing_matrix_page", LOGGER)
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
render_sourcing_matrix_page_header()
render_data_availability_note()


engine_available, engine_error = ensure_engine_importable()

if not WORKBOOK_PATH.exists():
    render_workbook_missing_message()

if not ENGINE_ROOT.exists():
    st.warning(
        "The sourcing engine is not accessible from this machine. Please contact the CNA Analytics team if this persists."
    )

if not engine_available:
    st.error(
        "The sourcing engine package could not be imported.\n"
        f"Import error: {engine_error}\n"
        "Install its dependencies and ensure the package root is accessible."
    )

order_number = st.text_input(
    "Primary Web Order Number",
    help="Enter the main WebOrderNumber for the project. This order supplies the final shipment destination address.",
)
additional_orders_text = st.text_area(
    "Additional Web Order Numbers (optional)",
    help=(
        "Optional. Enter additional web order numbers separated by commas. The tool will add their "
        "items to the sourcing plan but will use the primary order's destination address for the shipment."
    ),
    placeholder="Example: 126189032, 126189033",
    height=80,
)
st.caption(
    "Additional orders contribute item demand only. The primary web order number's destination address "
    "is used for the combined sourcing plan and final shipment."
)
staging_options = ["", "851", "853", "854", "857", "872", "875"]
staging_warehouse = st.selectbox(
    "Preferred Staging Warehouse",
    staging_options,
    format_func=lambda value: "(Compare all staging warehouses)" if value == "" else value,
)

render_troubleshooting_guide()

with st.expander("Engine and deployment notes"):
    st.markdown(
        """
        - The live workbook is discovered from the current Windows user's synced SharePoint folder.
        - This page reads product shipping/dimensions from `ItemShippingDimensions` and warehouse addresses from `WarehouseInfo` in the workbook. Runtime SQL/database permissions are not required for the normal Sourcing Matrix run.
        """
    )

# Advanced: override API URL and timeout if needed
api_url_override = None
with st.expander("Advanced Options"):
    api_url_override = st.text_input(
        "Shipping Calculator API URL (leave empty to use the same engine default as the standalone tool)",
        value="",
        placeholder=DEFAULT_SHIPPING_CALC_API_URL,
    )
    api_timeout_seconds = st.number_input(
        "Shipping Calculator Timeout (seconds)",
        min_value=30.0,
        max_value=600.0,
        value=180.0,
        step=30.0,
        help="Increase this for larger orders that may take longer for the API to respond.",
    )

selected_api_url = api_url_override.strip() if api_url_override and api_url_override.strip() else None

st.caption(
    f"**Using API URL**: `{selected_api_url or 'engine default - same as standalone CLI test'}`\n"
    f"**Using timeout**: `{api_timeout_seconds} sec`"
)

actual_api_url = selected_api_url

run_button = st.button("Generate Sourcing Plan", type="primary")

if run_button:
    if not order_number.strip():
        st.error("Enter a Primary Web Order Number before running the sourcing export.")
    elif not WORKBOOK_PATH.exists():
        render_workbook_missing_message()
    elif not engine_available:
        st.error("Cannot run the sourcing engine because the package is unavailable or not importable.")
    else:
        try:
            additional_orders = parse_additional_orders(additional_orders_text)
            with st.spinner("Running the Sourcing Matrix engine..."):
                filename, file_bytes = run_sourcing_export(
                    excel_path=WORKBOOK_PATH,
                    order_number=order_number.strip(),
                    additional_orders=additional_orders,
                    staging_warehouse=staging_warehouse or None,
                    include_comparison=True,
                    call_api=True,
                    api_url=actual_api_url,
                    api_timeout=api_timeout_seconds,
                    api_max_workers=4,
                )
            LOGGER.info(f"Sourcing Matrix export completed: {filename} ({len(file_bytes)} bytes)")
            trigger_download(filename, file_bytes)
            st.session_state.sourcing_matrix_summary_payload = _build_console_summary(file_bytes)
            if additional_orders:
                st.info(
                    f"Included {1 + len(additional_orders)} web order numbers: "
                    f"{order_number.strip()}, {', '.join(additional_orders)}. "
                    "The primary order destination was used for the combined shipment."
                )
            st.success(f"Sourcing plan generated: {filename}")
        except Exception as exc:
            LOGGER.exception("Sourcing Matrix run failed.")
            st.error(f"Sourcing Matrix run failed: {exc}")

if st.session_state.get("sourcing_matrix_summary_payload"):
    render_console_summary(st.session_state.sourcing_matrix_summary_payload)

if st.session_state.get("sourcing_matrix_download_bytes") is not None:
    file_bytes = st.session_state.sourcing_matrix_download_bytes
    file_name = st.session_state.sourcing_matrix_download_file_name

    st.markdown("### Excel sourcing plan ready")
    st.caption(
        "Use the button below to save the generated sourcing plan workbook locally. "
        "The app will try to save it to your Desktop first, then Downloads if needed."
    )

    if st.button("💾 Save Excel Sourcing Plan", type="primary"):
        try:
            saved_path = save_workbook_to_safe_location(file_name, file_bytes)
            st.success(f"Workbook saved to: {saved_path}")
            LOGGER.info(f"Sourcing Matrix workbook saved to: {saved_path}")
        except Exception as exc:
            st.error(
                "Unable to save the workbook locally. "
                "Please close any open Excel file with the same name and try again. "
                f"Error: {exc}"
            )
            LOGGER.exception("Failed to save Sourcing Matrix workbook.")
