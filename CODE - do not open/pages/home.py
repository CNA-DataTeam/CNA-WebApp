"""
home.py

Purpose:
    Landing page for the Logistics Support Streamlit suite.
    Styled to match the app and provides navigation cards.
"""

import streamlit as st

import config
import page_registry
import utils


LOGGER = utils.get_page_logger("Home")
IS_ADMIN_USER = utils.is_current_user_admin()
PAGE_TITLE = utils.get_registry_page_title(__file__, "Home")

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title=PAGE_TITLE,
    layout="wide",
    page_icon=utils.get_app_icon(),
)
utils.render_app_logo()
utils.log_page_open_once("home_page", LOGGER)
if "_home_render_logged" not in st.session_state:
    st.session_state._home_render_logged = True
    LOGGER.info("Render navigation cards.")
if "_home_sections_logged" not in st.session_state:
    st.session_state._home_sections_logged = True
    section_parts: list[str] = []
    for section_name, dept_buckets in page_registry.get_visible_sections(IS_ADMIN_USER):
        key = "".join(ch if ch.isalnum() else "_" for ch in section_name.strip().lower()).strip("_")
        total = sum(len(entries) for _, entries in dept_buckets)
        section_parts.append(f"{key}={total}")
    LOGGER.info("Sections available | %s", " ".join(section_parts))

# ============================================================
# GLOBAL STYLING
# ============================================================
st.markdown(utils.get_global_css(), unsafe_allow_html=True)

# Home-page-only treatment: turn the bordered nav containers into accent
# cards, and restore ~22px of top breathing room. The global CSS zeroes
# .block-container padding-top and the home page has no .cna-pageheader, so
# without this rule the first element sits flush against the top of the page.
st.markdown(
    """
    <style>
    [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] {
        border-top: 3px solid var(--cna-green) !important;
        background: var(--cna-white) !important;
        position: relative !important;
        cursor: pointer !important;
        transition: box-shadow 0.2s ease, transform 0.2s ease,
                    border-top-color 0.2s ease !important;
    }
    [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"]:hover {
        box-shadow: var(--cna-card-shadow) !important;
        transform: translateY(-2px) !important;
        border-top-color: var(--cna-teal) !important;
    }
    [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"]
        [data-testid="stPageLink"] p {
        font-family: var(--cna-heading) !important;
        font-weight: 600 !important;
        font-size: 1.05rem !important;
        color: var(--cna-navy) !important;
    }
    /* Stretched-link: make the whole bordered card clickable, not just the
       page-link text. The page link's <a> stays in normal flow (so its label
       renders in place); its transparent ::after overlay expands to cover the
       entire card, so a click anywhere on the card triggers navigation. */
    [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"]
        [data-testid="stPageLink"] a::after {
        content: "" !important;
        position: absolute !important;
        inset: 0 !important;
        z-index: 1 !important;
    }
    [data-testid="stMain"] .block-container {
        padding-top: 22px !important;
    }
    .cna-section-head {
        margin: 8px 0 4px 0;
    }
    .cna-section-head h2 {
        margin: 0 !important;
        font-size: clamp(1.4rem, 2.4vw, 1.9rem);
    }
    /* Department sub-header between the function heading and its card grid. */
    .cna-home-dept {
        margin: 18px 0 6px 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# APPLICATION CARDS (REAL NAVIGATION)
# ============================================================
def render_nav_card(column, entry: page_registry.PageEntry) -> None:
    with column:
        with st.container(border=True):
            if entry.beta:
                # Anchored top-right corner of the bordered card via absolute
                # positioning. See .cna-home-card-badge in get_global_css.
                st.markdown(
                    '<span class="cna-home-card-badge">BETA</span>',
                    unsafe_allow_html=True,
                )
            st.page_link(
                entry.path,
                label=f"**{entry.title}**",
                icon=entry.icon,
            )
            st.caption(entry.caption)


def _render_card_grid(entries: list[page_registry.PageEntry]) -> None:
    """Lay out cards in a 2-column grid, filling left-to-right."""
    for row_start in range(0, len(entries), 2):
        col1, col2 = st.columns(2)
        render_nav_card(col1, entries[row_start])
        right_idx = row_start + 1
        if right_idx < len(entries):
            render_nav_card(col2, entries[right_idx])
        else:
            with col2:
                st.empty()


visible_sections = page_registry.get_visible_sections(IS_ADMIN_USER)
for section_idx, (function_name, dept_buckets) in enumerate(visible_sections):
    st.markdown(
        f"""
        <div class="cna-section-head">
            <h2>{function_name}</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )
    for dept_name, entries in dept_buckets:
        if dept_name:
            st.markdown(
                f'<div class="cna-home-dept"><div class="cna-eyebrow"><span class="dot"></span>{dept_name}</div></div>',
                unsafe_allow_html=True,
            )
        _render_card_grid(entries)

    if section_idx < len(visible_sections) - 1:
        st.divider()

st.divider()
st.caption(
    "Use the sidebar to switch between applications at any time.",
    text_alignment="center",
)
