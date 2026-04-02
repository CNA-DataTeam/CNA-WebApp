"""
page_registry.py

Single source of truth for app navigation and home-page cards.
Add new pages here so they appear in both app.py and home.py.
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


HOME_PAGE = PageEntry(
    path="pages/home.py",
    title="Home",
    icon="\U0001F3E0",
    caption="Landing page.",
    quote='"There\'s no place like home screen."',
)


SECTION_ORDER = [
    "Admin Tools",
    "Task Tracker",
    "Work in Progress",
]

SECTION_PAGES: dict[str, list[PageEntry]] = {
    "Admin Tools": [
        PageEntry(
            path="pages/tasks-management.py",
            title="Management",
            icon="\U0001F6E0\uFE0F",
            caption="Admin page to maintain tasks metadata and review users.parquet.",
            admin_only=True,
        ),
        PageEntry(
            path="pages/admin-logs.py",
            title="Logging",
            icon="\U0001F4DC",
            caption="Filter and review application logs by user and page.",
            admin_only=True,
        ),
        PageEntry(
            path="pages/fedex-address-validation-management.py",
            title="FedEx Validator Results",
            icon="\U0001F4CB",
            caption="Review all validation results and clear disputed flags.",
            admin_only=True,
        ),
    ],
    "Task Tracker": [
        PageEntry(
            path="pages/task-tracker.py",
            title="Task Tracker",
            icon="\U0001F552",
            caption="Log daily operational tasks, track elapsed time, manage cadence, and view live activity.",
            quote='"Big Brother is watching you"',
        ),
        PageEntry(
            path="pages/task-tracker-analytics.py",
            title="Task Analytics",
            icon="\U0001F4CA",
            caption="Review team performance and task completion trends with filters for user and date.",
            quote='"Show me the metrics!"',
        ),
    ],
    "Work in Progress": [
        PageEntry(
            path="pages/packaging-estimator.py",
            title="Packaging Estimator",
            icon="\U0001F4E6",
            caption="Estimate package counts and grouped dimensions from item lists.",
            quote='"We\'re gonna need a bigger box."',
        ),
        PageEntry(
            path="pages/time-allocation-tool.py",
            title="Time Allocation Tool (TAT)",
            icon="\u23F1\uFE0F",
            caption="Capture account-level time allocations by percentage or detailed duration.",
            quote='"Putting labor where it belongs."',
        ),
        PageEntry(
            path="pages/fedex-address-validator.py",
            title="FedEx Address Validator",
            icon="\u2705",
            caption="Validate addresses and export standardized results for review.",
            quote='"Don\'t let em overcharge us!"',
        ),
        PageEntry(
            path="pages/stocking-agreement-generator.py",
            title="Stocking Agreement Generator",
            icon="\U0001F4DD",
            caption="Fill agreement templates and export polished Word or PDF documents.",
            quote='"Contracts without the copy-and-paste pain."',
        ),
    ],
}


def get_visible_sections(is_admin_user: bool) -> list[tuple[str, list[PageEntry]]]:
    """Return ordered sections with pages visible to current user."""
    visible: list[tuple[str, list[PageEntry]]] = []
    for section in SECTION_ORDER:
        entries = SECTION_PAGES.get(section, [])
        allowed = [entry for entry in entries if is_admin_user or not entry.admin_only]
        if allowed:
            visible.append((section, allowed))
    return visible


def get_home_page() -> PageEntry:
    """Return home-page metadata for navigation and title consistency."""
    return HOME_PAGE
