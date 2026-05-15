"""
Home / landing page treatment.

The reference app's home page is a LIGHTWEIGHT NAVIGATION GRID with:
  - NO hero / banner / big centered title
  - NO render_page_header() call
  - Navigation cards built from bordered st.container(border=True) blocks,
    restyled into accent cards via the page-scoped CSS below
  - Eyebrow + <h2> section headers above each group of cards
  - ~22px of top breathing room (restored by the page-scoped rule, because
    the global CSS zeroes .block-container padding-top and the home page has
    no .cna-pageheader to supply spacing)

If the older app's home page has a hero / banner / large title, REMOVE it.
If its DOM structure differs (cards not from st.container(border=True), no
section grouping), adapt the selectors/markup — the goal is: accent cards,
eyebrow section labels, no hero, ~22px top breathing room.

--------------------------------------------------------------------------
STEP 1 — Inject this PAGE-SCOPED CSS on the home page only, AFTER the global
CSS injection. (It is NOT part of get_global_css(); it lives in home.py.)
--------------------------------------------------------------------------
"""

import streamlit as st

st.markdown(
    """
    <style>
    [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] {
        border-top: 3px solid var(--cna-green) !important;
        background: var(--cna-white) !important;
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
    /* Breathing room at the top of the home page — the global rule zeroes
       .block-container padding-top; this restores it just for this page,
       matching the top spacing other pages get from .cna-pageheader. */
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

# --------------------------------------------------------------------------
# STEP 2 — Section headers above each group of cards use the eyebrow device
# plus an <h2>. Render one per section (idx is 1-based).
# --------------------------------------------------------------------------
idx = 1
section_name = "Example Section"
st.markdown(
    f"""
    <div class="cna-section-head">
        <div class="cna-eyebrow"><span class="dot"></span>Section {idx:02d}</div>
        <h2>{section_name}</h2>
    </div>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------------------------------
# NOTE on the breathing-room rule: `[data-testid="stMain"] .block-container`
# has higher specificity than the global `.block-container` rule, so it
# cleanly overrides the global `padding-top: 0 !important`. This rule is
# REQUIRED — without it the first element sits flush against the top of the
# window because the home page has no .cna-pageheader.
# --------------------------------------------------------------------------
