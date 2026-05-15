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
    for section_name, entries in page_registry.get_visible_sections(IS_ADMIN_USER):
        key = "".join(ch if ch.isalnum() else "_" for ch in section_name.strip().lower()).strip("_")
        section_parts.append(f"{key}={len(entries)}")
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
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# APPLICATION CARDS (REAL NAVIGATION)
# ============================================================
def render_nav_card(column, page_path: str, label: str, icon: str, caption: str) -> None:
    with column:
        with st.container(border=True):
            st.page_link(
                page_path,
                label=label,
                icon=icon,
            )
            st.caption(caption)


visible_sections = page_registry.get_visible_sections(IS_ADMIN_USER)
for section_idx, (section_name, entries) in enumerate(visible_sections):
    st.markdown(
        f"""
        <div class="cna-section-head">
            <div class="cna-eyebrow"><span class="dot"></span>Section {section_idx + 1:02d}</div>
            <h2>{section_name}</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )
    for row_start in range(0, len(entries), 2):
        col1, col2 = st.columns(2)
        left_entry = entries[row_start]
        render_nav_card(
            col1,
            left_entry.path,
            f"**{left_entry.title}**",
            left_entry.icon,
            left_entry.caption,
        )

        right_idx = row_start + 1
        if right_idx < len(entries):
            right_entry = entries[right_idx]
            render_nav_card(
                col2,
                right_entry.path,
                f"**{right_entry.title}**",
                right_entry.icon,
                right_entry.caption,
            )
        else:
            with col2:
                st.empty()

    if section_idx < len(visible_sections) - 1:
        st.divider()

st.divider()
st.caption(
    "Use the sidebar to switch between applications at any time.",
    text_alignment="center",
)
