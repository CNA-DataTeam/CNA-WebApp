"""
pages/stocking-agreement-generator.py

Generate stocking agreements as Word or PDF from app-managed templates.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from typing import Any

import pandas as pd
import streamlit as st

import config
import stocking_agreement_service
import utils


LOGGER = utils.get_page_logger("Stocking Agreement Generator")
PAGE_TITLE = utils.get_registry_page_title(__file__, "Stocking Agreement Generator")

st.set_page_config(page_title=PAGE_TITLE, layout="wide")
utils.render_app_logo()
utils.log_page_open_once("stocking_agreement_generator_page", LOGGER)
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

MONEY_PLACES = Decimal("0.01")
GENERAL_OUTPUT_KEY = "stocking_agreement_general_output"
CONSUMABLES_OUTPUT_KEY = "stocking_agreement_consumables_output"


def _blank_general_items() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Line Description": "", "EA Sell": None, "Qty": None},
            {"Line Description": "", "EA Sell": None, "Qty": None},
        ]
    )


def _blank_consumables_items() -> pd.DataFrame:
    return pd.DataFrame([{"Line Description": "", "EA Sell": None, "Qty": None}])


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        if pd.isna(value):
            return Decimal("0")
    except Exception:
        pass

    text = str(value).strip().replace(",", "").replace("$", "")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def _format_currency(value: Decimal) -> str:
    quantized = value.quantize(MONEY_PLACES, rounding=ROUND_HALF_UP)
    return f"${quantized:,.2f}"


def _format_quantity(value: Decimal) -> str:
    quantized = value.quantize(MONEY_PLACES, rounding=ROUND_HALF_UP)
    if quantized == quantized.to_integral():
        return f"{int(quantized):,}"
    normalized = format(quantized.normalize(), "f")
    return normalized.rstrip("0").rstrip(".")


def _sanitize_filename(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    return text.strip("._") or "stocking_agreement"


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_line_items(
    edited_df: pd.DataFrame,
    expected_columns: list[str],
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    if edited_df is None or edited_df.empty:
        working_df = pd.DataFrame(columns=expected_columns)
    else:
        working_df = edited_df.copy()

    for col in expected_columns:
        if col not in working_df.columns:
            working_df[col] = None
    working_df = working_df[expected_columns].copy()

    normalized_rows: list[dict[str, Any]] = []
    display_rows: list[dict[str, Any]] = []
    for _, row in working_df.iterrows():
        description = _clean_text(row.get("Line Description"))
        ea_sell = _to_decimal(row.get("EA Sell"))
        qty = _to_decimal(row.get("Qty"))
        if not description and ea_sell == 0 and qty == 0:
            continue

        extended_sell = (ea_sell * qty).quantize(MONEY_PLACES, rounding=ROUND_HALF_UP)
        normalized_rows.append(
            {
                "description": description,
                "ea_sell_decimal": ea_sell,
                "qty_decimal": qty,
                "extended_sell_decimal": extended_sell,
                "ea_sell": _format_currency(ea_sell),
                "qty": _format_quantity(qty),
                "extended_sell": _format_currency(extended_sell),
            }
        )
        display_rows.append(
            {
                "Line Description": description,
                "EA Sell": _format_currency(ea_sell),
                "Qty": _format_quantity(qty),
                "Extended Sell": _format_currency(extended_sell),
            }
        )

    return normalized_rows, pd.DataFrame(display_rows)


def _serialize_pricing_rows(
    line_items: list[dict[str, Any]],
    include_extended_sell: bool,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in line_items:
        row = {
            "description": item["description"],
            "ea_sell": item["ea_sell"],
            "qty": item["qty"],
        }
        if include_extended_sell:
            row["extended_sell"] = item["extended_sell"]
        rows.append(row)
    return rows


def _build_general_context(
    project_name: str,
    account_name: str,
    client_name: str,
    executive_summary: str,
    line_items: list[dict[str, Any]],
    freight: Decimal,
    tax: Decimal,
    shipping_method: str,
    payment_terms: str,
    invoicing: str,
    required_delivery_date: date,
    required_ship_date: date,
    documentation_requirements: str,
    addon_services_requested: str,
    change_window_text: str,
    termination_charge_text: str,
) -> dict[str, Any]:
    item_subtotal = sum((item["extended_sell_decimal"] for item in line_items), Decimal("0"))
    order_total = item_subtotal + freight + tax

    return {
        "project_name": project_name,
        "account_name": account_name,
        "client_name": client_name,
        "executive_summary": executive_summary,
        "pricing_rows": _serialize_pricing_rows(line_items, include_extended_sell=True),
        "item_subtotal": _format_currency(item_subtotal),
        "freight": _format_currency(freight),
        "tax": _format_currency(tax),
        "order_total": _format_currency(order_total),
        "shipping_method": shipping_method,
        "payment_terms": payment_terms,
        "invoicing": invoicing,
        "required_delivery_date": required_delivery_date.strftime("%m/%d/%Y"),
        "required_ship_date": required_ship_date.strftime("%m/%d/%Y"),
        "documentation_requirements": documentation_requirements,
        "addon_services_requested": addon_services_requested,
        "change_window_text": change_window_text,
        "termination_charge_text": termination_charge_text,
    }


def _build_consumables_context(
    project_name: str,
    account_name: str,
    order_type: str,
    item_summary: str,
    purpose_details: str,
    primary_item_number: str,
    cases_each: str,
    location_count: str,
    total_units: str,
    storage_term_days: int,
    line_items: list[dict[str, Any]],
    required_start_date: str,
    required_end_date: str,
    billing_account_name: str,
    billing_address: str,
) -> dict[str, Any]:
    purpose_text = purpose_details.strip()
    if purpose_text and not purpose_text.endswith((".", "!", "?")):
        purpose_text = f"{purpose_text}."

    return {
        "project_name": project_name,
        "account_name": account_name,
        "order_type": order_type,
        "item_summary": item_summary,
        "purpose_details": purpose_text,
        "primary_item_number": primary_item_number,
        "cases_each": cases_each,
        "location_count": location_count,
        "total_units": total_units,
        "storage_term_days": storage_term_days,
        "pricing_rows": _serialize_pricing_rows(line_items, include_extended_sell=False),
        "required_start_date": required_start_date,
        "required_end_date": required_end_date,
        "billing_account_name": billing_account_name,
        "billing_address": billing_address.replace("\r\n", "\n").replace("\r", "\n"),
    }


def _render_downloads(output_key: str) -> None:
    payload = st.session_state.get(output_key)
    if not payload:
        return

    st.success("Agreement files are ready to download.")
    docx_name, docx_bytes = payload["docx"]
    pdf_payload = payload.get("pdf")
    pdf_error = payload.get("pdf_error")

    docx_col, pdf_col = st.columns(2)
    with docx_col:
        st.download_button(
            "Download Word (.docx)",
            data=docx_bytes,
            file_name=docx_name,
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            width="stretch",
            key=f"{output_key}_docx",
        )
    with pdf_col:
        if pdf_payload:
            pdf_name, pdf_bytes = pdf_payload
            st.download_button(
                "Download PDF (.pdf)",
                data=pdf_bytes,
                file_name=pdf_name,
                mime="application/pdf",
                width="stretch",
                key=f"{output_key}_pdf",
            )
        else:
            st.button("Download PDF (.pdf)", disabled=True, width="stretch", key=f"{output_key}_pdf_disabled")

    if pdf_error:
        error_name, error_message = pdf_error
        st.warning(f"PDF generation is unavailable right now: {error_name}: {error_message}")


def _render_general_tab() -> None:
    st.caption(
        "Fill out the general project agreement and generate a Word or PDF copy. "
        "Pricing rows expand as needed in the generated document."
    )

    with st.form("general_resupply_agreement_form"):
        info_col1, info_col2, info_col3 = st.columns(3)
        with info_col1:
            project_name = st.text_input("Project Name", max_chars=120)
        with info_col2:
            account_name = st.text_input("Account", max_chars=120)
        with info_col3:
            client_name = st.text_input("Client Name", max_chars=120)

        executive_summary = st.text_area(
            "Executive Summary",
            height=120,
            placeholder="Describe the scope of the project and the purpose of the agreement.",
        )

        st.subheader("Pricing & Payment")
        general_items_df = st.data_editor(
            _blank_general_items(),
            hide_index=True,
            width="stretch",
            num_rows="dynamic",
            key="stocking_general_line_items",
            column_config={
                "Line Description": st.column_config.TextColumn("Line Description", required=False),
                "EA Sell": st.column_config.NumberColumn("EA Sell", min_value=0.0, step=0.01, format="%.2f"),
                "Qty": st.column_config.NumberColumn("Qty", min_value=0.0, step=1.0, format="%.2f"),
            },
        )
        general_line_items, general_preview_df = _normalize_line_items(
            general_items_df,
            ["Line Description", "EA Sell", "Qty"],
        )

        freight_col, tax_col = st.columns(2)
        with freight_col:
            freight_value = _to_decimal(st.number_input("Freight", min_value=0.0, step=0.01, format="%.2f"))
        with tax_col:
            tax_value = _to_decimal(st.number_input("Tax", min_value=0.0, step=0.01, format="%.2f"))

        item_subtotal = sum((item["extended_sell_decimal"] for item in general_line_items), Decimal("0"))
        order_total = item_subtotal + freight_value + tax_value
        metric_col1, metric_col2, metric_col3 = st.columns(3)
        with metric_col1:
            st.metric("Item Subtotal", _format_currency(item_subtotal))
        with metric_col2:
            st.metric("Freight + Tax", _format_currency(freight_value + tax_value))
        with metric_col3:
            st.metric("Order Total", _format_currency(order_total))

        if not general_preview_df.empty:
            st.dataframe(general_preview_df, hide_index=True, width="stretch")

        st.subheader("Project Details")
        detail_col1, detail_col2, detail_col3 = st.columns(3)
        with detail_col1:
            shipping_method = st.text_input("Shipping Method")
        with detail_col2:
            payment_terms = st.text_input("Payment Method / Terms")
        with detail_col3:
            invoicing = st.text_input("Invoicing")

        timeline_col1, timeline_col2 = st.columns(2)
        with timeline_col1:
            required_delivery_date = st.date_input("Required Delivery Date", value=date.today())
        with timeline_col2:
            required_ship_date = st.date_input("Required Ship Date", value=date.today())

        documentation_requirements = st.text_area("Documentation Requirements", height=90)
        addon_services_requested = st.text_area("Add-On Services Requested", height=90)

        with st.expander("Terms Overrides"):
            change_window_text = st.text_input(
                "Change Request Window",
                value="up to 21 days from the signed date of this agreement",
            )
            termination_charge_text = st.text_input(
                "Termination Charge Text",
                value="consisting of ten percent (10%) of the Project Price",
            )

        generate_clicked = st.form_submit_button("Generate General Resupply Agreement", type="primary", width="stretch")

    if not generate_clicked:
        _render_downloads(GENERAL_OUTPUT_KEY)
        return

    if not project_name or not account_name or not client_name:
        st.error("Project Name, Account, and Client Name are required.")
        return
    if not executive_summary:
        st.error("Executive Summary is required.")
        return
    if not general_line_items:
        st.error("Add at least one pricing row before generating the agreement.")
        return

    output_stem = _sanitize_filename(f"{project_name}_general_resupply")
    context = _build_general_context(
        project_name=project_name,
        account_name=account_name,
        client_name=client_name,
        executive_summary=executive_summary,
        line_items=general_line_items,
        freight=freight_value,
        tax=tax_value,
        shipping_method=shipping_method,
        payment_terms=payment_terms,
        invoicing=invoicing,
        required_delivery_date=required_delivery_date,
        required_ship_date=required_ship_date,
        documentation_requirements=documentation_requirements,
        addon_services_requested=addon_services_requested,
        change_window_text=change_window_text,
        termination_charge_text=termination_charge_text,
    )

    with st.spinner("Generating agreement files..."):
        try:
            st.session_state[GENERAL_OUTPUT_KEY] = stocking_agreement_service.render_agreement_documents(
                template_key="general_resupply",
                context=context,
                output_stem=output_stem,
            )
            LOGGER.info(
                "Generated general resupply agreement | project='%s' account='%s' rows=%s",
                project_name,
                account_name,
                len(general_line_items),
            )
        except Exception as exc:
            LOGGER.exception("General resupply agreement generation failed: %s", exc)
            st.error(f"Agreement generation failed: {type(exc).__name__}: {exc}")
            return

    _render_downloads(GENERAL_OUTPUT_KEY)


def _render_consumables_tab() -> None:
    st.caption(
        "Fill out the consumables stocking agreement and generate a Word or PDF copy. "
        "Pricing rows expand as needed in the generated document."
    )

    with st.form("consumables_agreement_form"):
        info_col1, info_col2, info_col3 = st.columns(3)
        with info_col1:
            project_name = st.text_input("Project Name", max_chars=120, key="cons_project_name")
        with info_col2:
            account_name = st.text_input("Account", max_chars=120, key="cons_account_name")
        with info_col3:
            order_type = st.selectbox("Order Type", ["Resupply", "Consolidated"], key="cons_order_type")

        item_col1, item_col2 = st.columns(2)
        with item_col1:
            item_summary = st.text_input(
                "Item Number and Item Description",
                placeholder="Example: 12345 - 12 oz. Compostable Cup",
            )
        with item_col2:
            primary_item_number = st.text_input("Primary Item Number")

        purpose_details = st.text_area(
            "Purpose Details",
            height=100,
            placeholder="Example: LTO rollout for spring menu launch.",
        )

        if order_type == "Consolidated":
            ship_col1, ship_col2, ship_col3 = st.columns(3)
            with ship_col1:
                cases_each = st.text_input("Cases / EA Per Store")
            with ship_col2:
                location_count = st.text_input("# of Locations")
            with ship_col3:
                total_units = st.text_input("Total Number")
        else:
            cases_each = ""
            location_count = ""
            total_units = ""

        storage_term_days = int(st.number_input("Storage Term (Days)", min_value=1, step=1, value=90))

        st.subheader("Pricing & Payment")
        consumables_items_df = st.data_editor(
            _blank_consumables_items(),
            hide_index=True,
            width="stretch",
            num_rows="dynamic",
            key="stocking_consumables_line_items",
            column_config={
                "Line Description": st.column_config.TextColumn("Line Description", required=False),
                "EA Sell": st.column_config.NumberColumn("EA Sell", min_value=0.0, step=0.01, format="%.2f"),
                "Qty": st.column_config.NumberColumn("Qty", min_value=0.0, step=1.0, format="%.2f"),
            },
        )
        consumables_line_items, consumables_preview_df = _normalize_line_items(
            consumables_items_df,
            ["Line Description", "EA Sell", "Qty"],
        )
        if not consumables_preview_df.empty:
            st.dataframe(consumables_preview_df, hide_index=True, width="stretch")

        st.subheader("Timeline & Billing")
        billing_col1, billing_col2 = st.columns(2)
        with billing_col1:
            required_start_date = st.text_input("Required Start Date", value="When inventory lands")
        with billing_col2:
            required_end_date = st.text_input(
                "Required End Date",
                value=f"{storage_term_days}-days from start date",
            )

        billing_account_name = st.text_input("Name on Account Being Charged")
        billing_address = st.text_area("Account Billing Address", height=110)

        generate_clicked = st.form_submit_button("Generate Consumables Agreement", type="primary", width="stretch")

    if not generate_clicked:
        _render_downloads(CONSUMABLES_OUTPUT_KEY)
        return

    if not project_name or not account_name:
        st.error("Project Name and Account are required.")
        return
    if not item_summary or not primary_item_number:
        st.error("Item summary and primary item number are required.")
        return
    if not purpose_details:
        st.error("Purpose Details are required.")
        return
    if order_type == "Consolidated" and (not cases_each or not location_count or not total_units):
        st.error("Cases / EA, number of locations, and total number are required for consolidated orders.")
        return
    if not consumables_line_items:
        st.error("Add at least one pricing row before generating the agreement.")
        return
    if not billing_account_name or not billing_address:
        st.error("Billing account name and billing address are required.")
        return

    output_stem = _sanitize_filename(f"{project_name}_consumables")
    context = _build_consumables_context(
        project_name=project_name,
        account_name=account_name,
        order_type=order_type,
        item_summary=item_summary,
        purpose_details=purpose_details,
        primary_item_number=primary_item_number,
        cases_each=cases_each,
        location_count=location_count,
        total_units=total_units,
        storage_term_days=storage_term_days,
        line_items=consumables_line_items,
        required_start_date=required_start_date,
        required_end_date=required_end_date,
        billing_account_name=billing_account_name,
        billing_address=billing_address,
    )

    with st.spinner("Generating agreement files..."):
        try:
            st.session_state[CONSUMABLES_OUTPUT_KEY] = stocking_agreement_service.render_agreement_documents(
                template_key="consumables",
                context=context,
                output_stem=output_stem,
            )
            LOGGER.info(
                "Generated consumables agreement | project='%s' account='%s' order_type='%s' rows=%s",
                project_name,
                account_name,
                order_type,
                len(consumables_line_items),
            )
        except Exception as exc:
            LOGGER.exception("Consumables agreement generation failed: %s", exc)
            st.error(f"Agreement generation failed: {type(exc).__name__}: {exc}")
            return

    _render_downloads(CONSUMABLES_OUTPUT_KEY)


utils.render_page_header(PAGE_TITLE)

try:
    stocking_agreement_service.ensure_templates_ready()
except Exception as exc:
    LOGGER.exception("Template preparation failed: %s", exc)
    st.error(f"Agreement templates are not ready: {type(exc).__name__}: {exc}")
else:
    general_tab, consumables_tab = st.tabs(["General Resupply", "Consumables"], width="stretch")
    with general_tab:
        _render_general_tab()
    with consumables_tab:
        _render_consumables_tab()

st.caption(f"\n\n\nApp version: {config.APP_VERSION}", text_alignment="center")
