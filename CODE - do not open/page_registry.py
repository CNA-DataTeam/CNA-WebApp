"""
page_registry.py

Single source of truth for app navigation and home-page cards.

Three-tier hierarchy: Function -> Department -> PageEntry.
The Admin function has no department tier (uses "" as the dept key).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PageEntry:
    path: str
    title: str
    icon: str
    caption: str
    admin_only: bool = False
    quote: str = ""
    beta: bool = False


HOME_PAGE = PageEntry(
    path="pages/home.py",
    title="Home",
    icon="\U0001F3E0",
    caption="Landing page.",
    quote="Quick links to every tool in the suite.",
)


# Function ordering (top-level tier in the sidebar and home page).
SECTION_ORDER = [
    "Tools",
    "Reports",
    "Admin",
]

# Department ordering within each Function. Departments not listed here fall to
# the end in insertion order. Admin has no departments -- it uses "" as a
# sentinel dept key and is rendered without a middle tier.
DEPARTMENT_ORDER: dict[str, list[str]] = {
    "Admin": [""],
    "Tools": ["Company", "Logistics", "Sales", "Data & Analytics"],
    "Reports": ["Logistics", "Data & Analytics"],
}


# Function -> Department -> [PageEntry]
SECTION_PAGES: dict[str, dict[str, list[PageEntry]]] = {
    "Admin": {
        "": [
            PageEntry(
                path="pages/admin-logs.py",
                title="Logging",
                icon="\U0001F4DC",
                caption="Filter and review application logs by user and page.",
                admin_only=True,
                quote="Review application logs by user and page.",
            ),
            PageEntry(
                path="pages/tasks-management.py",
                title="Management",
                icon="\U0001F6E0️",
                caption="Admin page to maintain tasks metadata and review users.parquet.",
                admin_only=True,
                quote="Maintain task definitions, targets, and task logs.",
            ),
            PageEntry(
                path="pages/fedex-address-validation-management.py",
                title="FedEx Validator Results",
                icon="\U0001F4CB",
                caption="Review all validation results and clear disputed flags.",
                admin_only=True,
                quote="Review address-validation results and clear disputed flags.",
            ),
            PageEntry(
                path="pages/period-configuration.py",
                title="Period Configuration",
                icon="\U0001F5D3️",
                caption="Define fiscal periods per year for use across the app.",
                admin_only=True,
                quote="Define the fiscal periods used across the app.",
            ),
        ],
    },
    "Tools": {
        "Company": [
            PageEntry(
                path="pages/time-allocation-tool.py",
                title="Time Allocation Tool",
                icon="⏱️",
                caption="Capture account-level time allocations by percentage or detailed duration.",
                quote="Log how your time splits across accounts each day.",
            ),
        ],
        "Logistics": [
            PageEntry(
                path="pages/fedex-address-validator.py",
                title="FedEx Address Validator",
                icon="✅",
                caption="Validate addresses and export standardized results for review.",
                quote="Spot address-validation results worth disputing with FedEx.",
                beta=True,
            ),
            PageEntry(
                path="pages/task-tracker.py",
                title="Logistics Task Tracker",
                icon="\U0001F552",
                caption="Log daily operational tasks, track elapsed time, manage cadence, and view live activity.",
                quote="Time your daily tasks and see live team activity.",
            ),
            PageEntry(
                path="pages/packaging-estimator.py",
                title="Packaging Estimator",
                icon="\U0001F4E6",
                caption="Estimate package counts and grouped dimensions from item lists.",
                quote="Match items to shipping and package estimates.",
                beta=True,
            ),
            PageEntry(
                path="pages/sourcing-matrix.py",
                title="Sourcing Matrix",
                icon="📊",
                caption="Run the Sourcing Matrix engine and export plan workbooks.",
                quote="Generate sourcing plans from the live SharePoint workbook.",
                beta=True,
            ),
        ],
        "Sales": [
            PageEntry(
                path="pages/stocking-agreement-generator.py",
                title="Stocking Agreement",
                icon="\U0001F4DD",
                caption="Fill agreement templates and export polished Word or PDF documents.",
                quote="Generate Word and PDF agreements from templates.",
            ),
        ],
        "Data & Analytics": [
            PageEntry(
                path="pages/da-task-tracker.py",
                title="D&A Task Tracker",
                icon="\U0001F552",
                caption="Log daily D&A tasks, track elapsed time, and view live activity.",
                quote="Time your daily tasks and see live team activity.",
            ),
        ],
    },
    "Reports": {
        "Logistics": [
            PageEntry(
                path="pages/task-tracker-analytics.py",
                title="Logistics Task Analytics",
                icon="\U0001F4CA",
                caption="Review team performance and task completion trends with filters for user and date.",
                quote="Historical performance and trends for completed tasks.",
            ),
        ],
        "Data & Analytics": [
            PageEntry(
                path="pages/da-task-tracker-analytics.py",
                title="D&A Task Analytics",
                icon="\U0001F4CA",
                caption="Review D&A team performance and task completion trends.",
                quote="Historical performance and trends for D&A completed tasks.",
            ),
        ],
    },
}


def _ordered_departments(function_name: str, depts: dict[str, list[PageEntry]]) -> list[str]:
    """Return department keys in their configured order, with unknown depts last."""
    configured = DEPARTMENT_ORDER.get(function_name, [])
    ordered = [d for d in configured if d in depts]
    extras = [d for d in depts.keys() if d not in configured]
    return ordered + extras


def get_visible_sections(
    is_admin_user: bool,
) -> list[tuple[str, list[tuple[str, list[PageEntry]]]]]:
    """Return Function -> [(Department, [PageEntry])] for visible pages.

    Departments and Functions with no visible entries (after admin_only
    filtering) are omitted entirely.
    """
    visible: list[tuple[str, list[tuple[str, list[PageEntry]]]]] = []
    for function_name in SECTION_ORDER:
        depts = SECTION_PAGES.get(function_name, {})
        allowed_depts: list[tuple[str, list[PageEntry]]] = []
        for dept_name in _ordered_departments(function_name, depts):
            entries = depts.get(dept_name, [])
            allowed = [e for e in entries if is_admin_user or not e.admin_only]
            if allowed:
                allowed_depts.append((dept_name, allowed))
        if allowed_depts:
            visible.append((function_name, allowed_depts))
    return visible


def iter_all_pages() -> list[PageEntry]:
    """Flat iterator over every registered page entry, regardless of visibility."""
    out: list[PageEntry] = []
    for depts in SECTION_PAGES.values():
        for entries in depts.values():
            out.extend(entries)
    return out


def get_home_page() -> PageEntry:
    """Return home-page metadata for navigation and title consistency."""
    return HOME_PAGE
