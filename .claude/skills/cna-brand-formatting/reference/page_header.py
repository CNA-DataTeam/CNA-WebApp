"""
Verbatim target for utils.render_page_header() and its page_registry helpers.

The standard page header is: a small uppercase eyebrow label, a large
left-aligned Poppins title, a green->teal accent bar, and an optional kicker
intro line.

render_page_header depends on two registry helpers:
  - get_registry_page_section(page_title) -> the nav section name (used as the
    eyebrow text; falls back to "CNA Console")
  - get_registry_page_quote(page_title)   -> a one-line quote (used as kicker)

INTEGRATION NOTES:
  - utils.py must `import html` (render_page_header uses html.escape).
  - `from functools import lru_cache` is required for the helpers below.
  - If the app's page_registry.py ALREADY has equivalent section/quote
    helpers, reuse them — do not duplicate.
  - The helpers expect page_registry to expose:
        SECTION_PAGES: dict[str, list[PageEntry]]   # section name -> entries
        HOME_PAGE: PageEntry                        # the landing page entry
    where each PageEntry has at least `path`, `title`, and optionally `quote`.
  - If the app's registry has a different shape (no sections, no `quote`
    field), DO NOT restructure the registry. Let the helpers return "" —
    render_page_header then simply omits the eyebrow/kicker, which is fine.
    You can also pass `eyebrow=` explicitly at call sites.
"""

import html
from functools import lru_cache

import streamlit as st


def render_page_header(
    page_title: str,
    show_divider: bool = True,
    eyebrow: str | None = None,
) -> None:
    """Render the standard page header.

    Layout follows the CNA brand microsite: a small uppercase eyebrow label,
    a large left-aligned Poppins title, a brand accent bar, and a kicker
    intro line. The eyebrow defaults to the page's section name from
    page_registry (falling back to "CNA Console"); the kicker uses the
    page's registry quote.
    """
    safe_title = html.escape(str(page_title).strip() or "Page")
    if eyebrow is None:
        eyebrow = get_registry_page_section(page_title) or "CNA Console"
    safe_eyebrow = html.escape(str(eyebrow).strip())
    safe_kicker = html.escape(get_registry_page_quote(page_title))
    eyebrow_html = (
        f'<div class="cna-eyebrow"><span class="dot"></span>{safe_eyebrow}</div>'
        if safe_eyebrow
        else ""
    )
    kicker_html = (
        f'<div class="header-kicker">{safe_kicker}</div>' if safe_kicker else ""
    )
    st.markdown(
        f"""
        <div class="cna-pageheader">
            {eyebrow_html}
            <h1 class="header-title">{safe_title}</h1>
            <div class="header-accent"></div>
            {kicker_html}
        </div>
        """,
        unsafe_allow_html=True,
    )
    if show_divider:
        st.divider()


@lru_cache(maxsize=1)
def _registry_section_map() -> dict[str, str]:
    """Build a normalized page-title -> section-name map from page_registry."""
    try:
        import page_registry
    except Exception:
        return {}
    mapping: dict[str, str] = {}
    section_pages = getattr(page_registry, "SECTION_PAGES", {})
    if not isinstance(section_pages, dict):
        section_pages = {}
    for section_name, entries in section_pages.items():
        for entry in entries:
            title = str(getattr(entry, "title", "")).strip()
            if title:
                mapping[title] = str(section_name).strip()
    return mapping


def get_registry_page_section(page_title: str) -> str:
    """Resolve the section a page belongs to from page_registry."""
    return _registry_section_map().get(str(page_title).strip(), "")


@lru_cache(maxsize=1)
def _registry_quote_map() -> dict[str, str]:
    """Build a normalized title->quote map from page_registry."""
    try:
        import page_registry
    except Exception:
        return {}
    mapping: dict[str, str] = {}
    section_pages = getattr(page_registry, "SECTION_PAGES", {})
    if not isinstance(section_pages, dict):
        section_pages = {}
    for entries in section_pages.values():
        for entry in entries:
            title = str(getattr(entry, "title", "")).strip()
            quote = str(getattr(entry, "quote", "")).strip()
            if title and quote:
                mapping[title] = quote
    home_entry = getattr(page_registry, "HOME_PAGE", None)
    if home_entry is not None:
        title = str(getattr(home_entry, "title", "")).strip()
        quote = str(getattr(home_entry, "quote", "")).strip()
        if title and quote:
            mapping[title] = quote
    return mapping


def get_registry_page_quote(page_title: str) -> str:
    """Resolve page quote from page_registry using the page title."""
    return _registry_quote_map().get(str(page_title).strip(), "")
