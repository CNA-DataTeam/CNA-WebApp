"""
Verbatim target for utils.get_global_css().

Replace the body of the app's existing global-CSS function with this (or add
this function if none exists). Requirements:
  - Must be decorated with @st.cache_data.
  - Must return one big <style>...</style> string.
  - Every page injects it via: st.markdown(utils.get_global_css(), unsafe_allow_html=True)

Adapt ONLY the data-testid names if the app's Streamlit version differs from
1.57 (verify against the rendered DOM). Do not change values, sizes, or hex
codes. If the older function held app-specific rules for widgets that still
exist, MERGE them in rather than dropping them.

KEEP-INTACT NOTES (see SKILL.md Part 7 for the full rationale):
  - The @import line must stay first (loads Poppins / Work Sans / JetBrains Mono).
  - The style-container collapse rule
        [data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] > style)
    is load-bearing. It MUST use a descendant combinator for the style block,
    never a brittle direct-child chain — Streamlit nests extra width/latex
    wrapper divs between the element container and the markdown container.
  - .block-container { padding-top: 0 !important; } is intentional; pages get
    their top breathing room from .cna-pageheader instead.
"""


@st.cache_data
def get_global_css() -> str:
    """Return global CSS styling for the app (cached).

    Built around the CNA brand system (see CNA Brand Guidelines 2025):
    - Primary colors: CNA Green #00B19A, CNA Teal #06828D
    - Secondary: CNA Navy #002E65, CNA Turquoise #08B4C5,
      CNA Sky #D0ECEE, CNA Sky Lite #EFFAFA
    - Typography: Poppins (Bold/SemiBold) for headings/titles,
      Work Sans (Regular/Medium/SemiBold + Italic) for body and lead-ins
    The :root custom properties below are the single source of truth for
    brand color tokens used across the app's CSS and inline page styles.
    """
    return """
    <style>
    /* Import brand fonts — Poppins (headings), Work Sans (body/lead-ins),
       JetBrains Mono (data: KPI values, timers, codes) */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@500;600;700;800&family=Work+Sans:ital,wght@0,400;0,500;0,600;1,400;1,500&family=JetBrains+Mono:wght@400;500&display=swap');

    /* ===================================================================
       CNA brand design tokens — single source of truth for brand styling.
       Mirrors the CNA Brand Guidelines microsite design system.
       =================================================================== */
    :root {
        --cna-green: #00B19A;
        --cna-green-dark: #00917F;
        --cna-teal: #06828D;
        --cna-teal-dark: #056B74;
        --cna-navy: #002E65;
        --cna-navy-soft: #334F82;   /* body copy on light surfaces */
        --cna-turquoise: #08B4C5;
        --cna-sky: #D0ECEE;
        --cna-sky-lite: #EFFAFA;
        --cna-white: #FFFFFF;
        --cna-ink: #1F2A44;
        --cna-muted: #6B7F9A;
        --cna-rule: #B9DDE1;        /* universal hairline border */
        --cna-rule-soft: #E1F1F3;
        --cna-border: #B9DDE1;
        --cna-surface: #EFFAFA;
        --cna-danger: #D64550;
        --cna-danger-dark: #B23640;
        --cna-heading: 'Poppins', 'Helvetica Neue', Arial, sans-serif;
        --cna-body: 'Work Sans', 'Helvetica Neue', Arial, sans-serif;
        --cna-mono: 'JetBrains Mono', 'Menlo', 'Consolas', monospace;
        --cna-card-shadow: 0 14px 30px rgba(0, 46, 101, 0.10);
        --cna-page-pad: 3rem;   /* main content side padding (page gutter) */
    }

    /* Base font settings */
    html, body, [class*="css"] {
        font-family: var(--cna-body);
        color: var(--cna-navy-soft);
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] li {
        color: var(--cna-navy-soft);
    }
    [data-testid="stMarkdownContainer"] li::marker { color: var(--cna-green); }
    h1, h2, h3, h4, h5, h6 {
        font-family: var(--cna-heading);
        color: var(--cna-navy);
        letter-spacing: -0.02em;
    }
    h1 { font-weight: 800; }
    h2 { font-weight: 700; letter-spacing: -0.01em; }
    h3, h4, h5, h6 { font-weight: 600; letter-spacing: -0.01em; }
    strong, b { color: var(--cna-navy); }

    /* Eyebrow — the brand's signature label device (uppercase, letter-spaced,
       teal/green). Use .cna-eyebrow on a small div above a heading. */
    .cna-eyebrow {
        font-family: var(--cna-body);
        font-weight: 600;
        font-size: 0.69rem;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: var(--cna-teal);
        margin-bottom: 0.35rem;
    }
    .cna-eyebrow .dot {
        display: inline-block;
        width: 6px; height: 6px;
        background: var(--cna-green);
        border-radius: 50%;
        margin-right: 9px;
        vertical-align: middle;
    }
    /* Legacy alias kept for any existing callers */
    .section-leadin {
        font-family: var(--cna-body);
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 0.8rem;
        color: var(--cna-teal);
        margin-bottom: 0.15rem;
    }

    /* Branded links */
    a, a:visited { color: var(--cna-teal); text-underline-offset: 3px; }
    a:hover { color: var(--cna-green); }

    /* Branded dividers — soft rule line */
    hr, [data-testid="stDivider"] hr {
        border: none !important;
        border-top: 1px solid var(--cna-rule) !important;
        background: transparent !important;
    }

    /* Hide default Streamlit footer */
    footer {visibility: hidden;}
    /* Hide default Streamlit hamburger menu (3 vertical dots) */
    [data-testid="stMainMenu"] {
        display: none !important;
    }
    /* Collapse Streamlit's default top header band so page content runs to
       the very top of the page. The header element is kept (zero height,
       transparent, overflow visible) so the toolbar's sidebar-expand
       control still works when the sidebar is collapsed. */
    [data-testid="stHeader"] {
        background: transparent !important;
        height: 0 !important;
        min-height: 0 !important;
        overflow: visible !important;
        box-shadow: none !important;
        border-bottom: none !important;
    }
    [data-testid="stDecoration"] {
        display: none !important;
    }
    [data-testid="stToolbar"] {
        top: 0 !important;
        right: 0 !important;
    }
    /* Keep the sidebar-expand control legible wherever it floats */
    [data-testid="stExpandSidebarButton"] {
        z-index: 1000 !important;
    }
    /* Collapse element containers that only hold an injected <style> block.
       Each st.markdown(<style>) call still creates a flow element with a
       vertical-block gap; left visible, they push the first real content
       down from the top of the page. A <style> still applies its CSS even
       when its container is display:none.
       Note: Streamlit nests extra width/latex wrappers between the element
       container and the markdown container, so this must match the style
       block as a descendant (not a fixed direct-child chain) or it silently
       stops collapsing — which is what reopens the top-of-page gap. */
    [data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] > style) {
        display: none !important;
    }
    /* Main content padding. Side padding is the page gutter that content
       sits inside. Top padding is 0 so page content runs to the top;
       bottom padding is 0 so content reaches the bottom of the page. */
    .block-container {
        padding-top: 0 !important;
        padding-left: var(--cna-page-pad);
        padding-right: var(--cna-page-pad);
        padding-bottom: 0;
    }
    /* Hide Deploy button (last header button in toolbar) */
    [data-testid="stToolbar"] button[data-testid="stBaseButton-header"]:last-of-type {
        display: none !important;
    }
    /* Sidebar navigation sizing */
    [data-testid="stSidebar"] [data-testid="stPageLink"] p,
    [data-testid="stSidebar"] [data-testid="stPageLink"] span {
        font-size: 0.92rem !important;
    }
    /* Tighten sidebar page links inside expanders */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stPageLink"] {
        padding-top: 0 !important;
        padding-bottom: 0 !important;
        min-height: unset !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stVerticalBlock"] {
        gap: 16px !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] {
        padding-bottom: 0 !important;
        margin-bottom: 10px !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] .stElementContainer {
        margin-bottom: 0 !important;
        padding: 0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child [data-testid="stVerticalBlock"] {
        gap: 0 !important;
        height: 20px !important;
        overflow: visible !important;
    }
    /* Thin dividers between page rows inside expanders */
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] > [data-testid="stVerticalBlock"] > div {
        position: relative !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] > [data-testid="stVerticalBlock"] > div::before {
        content: "" !important;
        position: absolute !important;
        top: -12px !important;
        left: 0 !important;
        right: 0 !important;
        height: 1px !important;
        background: var(--cna-sky) !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpanderDetails"] > [data-testid="stVerticalBlock"] > div:first-child::before {
        display: none !important;
    }
    /* Sidebar page link icons — nudge up to align with text */
    [data-testid="stSidebar"] [data-testid="stPageLink"] [data-testid="stIconMaterial"],
    [data-testid="stSidebar"] [data-testid="stPageLink"] span[class*="icon"] {
        position: relative !important;
        top: -1px !important;
    }
    /* Favorite star: hover toggle and positioning */
    .star-toggle {
        position: relative !important;
        top: -2px !important;
        width: 20px !important;
        height: 34px !important;
        pointer-events: none !important;
    }
    .star-toggle img {
        position: absolute !important;
        top: 0 !important;
        left: 0 !important;
    }
    .star-toggle .star-hover {
        visibility: hidden !important;
    }
    /* Hover on the parent COLUMN (not .star-toggle) because the invisible
       button sits on top with z-index and intercepts all pointer events.
       Hovering the button still bubbles :hover up to the column ancestor. */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child:hover .star-default {
        visibility: hidden !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child:hover .star-hover {
        visibility: visible !important;
    }
    /* Favorite star: column is the positioned ancestor for the absolute button */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child {
        position: relative !important;
    }
    /* Reset all intermediate wrapper divs so position:absolute on the button
       anchors to the column, not a nested Streamlit container. */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child div {
        position: static !important;
    }
    /* Invisible button covers the full column area for click capture. */
    [data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stColumn"]:last-child [data-testid="stBaseButton-tertiary"] {
        position: absolute !important;
        inset: 0 !important;
        opacity: 0 !important;
        z-index: 10 !important;
        width: 100% !important;
        height: 100% !important;
        cursor: pointer !important;
        min-height: unset !important;
        padding: 0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
        font-size: 0.72rem !important;
    }
    /* Settings popover — clean dropdown style (popover body portals outside sidebar) */
    [data-testid="stPopoverBody"] {
        padding: 4px 0 !important;
    }
    [data-testid="stPopoverBody"] [data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    [data-testid="stPopoverBody"] .stElementContainer {
        border-bottom: 1px solid var(--cna-sky) !important;
    }
    [data-testid="stPopoverBody"] .stElementContainer:last-child {
        border-bottom: none !important;
    }
    [data-testid="stPopoverBody"] [data-testid="stBaseButton-tertiary"] {
        padding: 2px 8px !important;
        min-height: unset !important;
        border-radius: 0 !important;
    }
    [data-testid="stPopoverBody"] [data-testid="stBaseButton-tertiary"] p {
        font-size: 0.85rem !important;
        text-align: left !important;
    }
    /* Sidebar page-link hover — soft sky highlight */
    [data-testid="stSidebar"] [data-testid="stPageLink"] a:hover {
        background-color: var(--cna-sky-lite) !important;
        border-radius: 6px !important;
    }
    /* Active sidebar page link — brand teal accent */
    [data-testid="stSidebar"] [data-testid="stPageLink"] a[aria-current="page"] {
        background-color: var(--cna-sky) !important;
        border-radius: 6px !important;
    }
    [data-testid="stSidebar"] [data-testid="stPageLink"] a[aria-current="page"] p {
        color: var(--cna-teal-dark) !important;
        font-weight: 600 !important;
    }

    /* ===================================================================
       Page header — eyebrow + large left-aligned title + kicker intro
       =================================================================== */
    /* Give the page header breathing room from the top of the page. */
    .cna-pageheader {
        margin: 22px 0 4px 0;
    }
    .cna-pageheader .header-title {
        margin: 0 !important;
        padding: 0 !important;
        text-align: left;
        color: var(--cna-navy);
        font-family: var(--cna-heading);
        font-weight: 800;
        font-size: clamp(1.9rem, 3.4vw, 2.7rem);
        line-height: 1.05;
        letter-spacing: -0.02em;
    }
    .cna-pageheader .header-kicker {
        text-align: left;
        font-family: var(--cna-body);
        font-weight: 400;
        font-size: 1.02rem;
        line-height: 1.55;
        color: var(--cna-navy-soft);
        margin: 10px 0 0 0;
        max-width: 760px;
    }
    /* Brand accent bar under the page title */
    .header-accent {
        width: 56px;
        height: 4px;
        margin: 14px 0 0 0;
        background: linear-gradient(90deg, var(--cna-green), var(--cna-teal));
    }
    /* Legacy header markup support (centered) */
    .header-row {
        display: flex;
        align-items: center;
        gap: 14px;
        margin-top: 10px;
        margin-bottom: 6px;
    }
    .header-subtitle {
        font-style: italic;
        font-weight: 400;
        color: var(--cna-teal);
    }

    /* ===================================================================
       Card system — sharp corners, hairline rule border, accent top edge
       =================================================================== */
    .cna-card, .app-card {
        border: 1px solid var(--cna-rule);
        border-radius: 0;
        padding: 20px 22px;
        background-color: var(--cna-white);
        transition: box-shadow 0.2s ease, border-color 0.2s ease,
                    transform 0.2s ease;
    }
    .cna-card--accent, .app-card {
        border-top: 3px solid var(--cna-green);
    }
    .cna-card:hover, .app-card:hover {
        box-shadow: var(--cna-card-shadow);
        transform: translateY(-2px);
    }
    .cna-card--accent:hover, .app-card:hover {
        border-top-color: var(--cna-teal);
    }
    .app-title {
        font-family: var(--cna-heading);
        font-size: 1.05rem;
        font-weight: 600;
        color: var(--cna-navy);
        margin-bottom: 6px;
    }
    .app-desc {
        color: var(--cna-muted);
        font-size: 0.875rem;
        margin-bottom: 14px;
    }
    /* Callout / note box — sky-lite fill with a 3px left accent */
    .cna-note {
        background: var(--cna-sky-lite);
        border: 1px solid var(--cna-rule);
        border-left: 3px solid var(--cna-teal);
        border-radius: 0;
        padding: 16px 20px;
        font-size: 0.9rem;
        color: var(--cna-navy-soft);
        margin: 12px 0;
    }
    .cna-note.is-green { border-left-color: var(--cna-green); }
    .cna-note strong { color: var(--cna-navy); }

    /* Sharp corners for card-like containers (inputs/buttons keep theme
       radius). Targets bordered st.container blocks and expanders. */
    [data-testid="stVerticalBlockBorderWrapper"],
    [data-testid="stExpander"] details,
    [data-testid="stExpander"] summary,
    .stDataFrame, [data-testid="stTable"],
    [data-testid="stMetric"],
    [data-testid="stNotification"],
    [data-testid="stAlert"] {
        border-radius: 0 !important;
    }
    [data-testid="stVerticalBlockBorderWrapper"] {
        border-color: var(--cna-rule) !important;
    }
    /* st.metric — give it the card treatment with a mono value */
    [data-testid="stMetric"] {
        background: var(--cna-white);
        border: 1px solid var(--cna-rule);
        border-top: 3px solid var(--cna-green);
        padding: 16px 18px;
    }
    [data-testid="stMetricValue"] {
        font-family: var(--cna-mono);
        color: var(--cna-navy);
    }
    [data-testid="stMetricLabel"] {
        font-family: var(--cna-body);
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        font-size: 0.72rem !important;
        color: var(--cna-teal) !important;
    }
    /* Tabs — squared active indicator in brand green */
    [data-testid="stTabs"] [data-baseweb="tab-highlight"] {
        background-color: var(--cna-green) !important;
    }
    /* Primary buttons — branded hover polish */
    [data-testid="stBaseButton-primary"] {
        transition: background-color 0.15s ease-in-out,
                    box-shadow 0.15s ease-in-out;
    }
    [data-testid="stBaseButton-primary"]:hover {
        background-color: var(--cna-green-dark) !important;
        border-color: var(--cna-green-dark) !important;
        box-shadow: 0 4px 12px rgba(0, 177, 154, 0.28);
    }
    /* Timer display + label (task tracker) — mono value per data convention */
    .timer-display {
        font-family: var(--cna-mono);
        font-size: 42px;
        font-weight: 500;
        color: var(--cna-navy);
        line-height: 1.1;
        letter-spacing: -0.01em;
    }
    .timer-label {
        font-family: var(--cna-body);
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        color: var(--cna-teal);
        margin-top: 4px;
    }
    /* Timer blinking colon */
    @keyframes blink { 50% { opacity: 0; } }
    .blink-colon {
        animation: blink 1s steps(1, start) infinite;
        color: var(--cna-green);
    }
    /* Live activity pulse dot */
    .live-activity-pulse {
        display: inline-block;
        width: 12px;
        height: 12px;
        background-color: var(--cna-danger);
        border-radius: 100%;
        margin-right: 2px;
        animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(1.2); }
    }
    /* Reset button style — destructive action stays red per UX convention */
    .reset-button div > button {
        background-color: var(--cna-danger) !important;
        color: white !important;
        border: none !important;
    }
    .reset-button div > button:hover {
        background-color: var(--cna-danger-dark) !important;
    }
    .reset-button div > button:focus {
        box-shadow: none !important;
    }
    /* Hide autorefresh iframe (used for timer) */
    iframe[title="streamlit_autorefresh.st_autorefresh"] {
        display: none;
    }
    /* Dataframe header style */
    .stDataFrame thead th {
        font-weight: 700 !important;
        color: var(--cna-navy) !important;
    }
    /* KPI card styling (analytics pages) — sharp card, accent edge, mono value */
    .kpi-card {
        background-color: var(--cna-white);
        border: 1px solid var(--cna-rule);
        border-top: 3px solid var(--cna-green);
        padding: 20px 18px;
        border-radius: 0;
        text-align: center;
        transition: box-shadow 0.2s ease, transform 0.2s ease,
                    border-top-color 0.2s ease;
    }
    .kpi-card:hover {
        box-shadow: var(--cna-card-shadow);
        transform: translateY(-2px);
        border-top-color: var(--cna-teal);
    }
    .kpi-value {
        font-family: var(--cna-mono);
        font-size: 30px;
        font-weight: 500;
        color: var(--cna-navy);
        letter-spacing: -0.01em;
    }
    .kpi-label {
        color: var(--cna-teal);
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin-top: 4px;
    }
    </style>
    """
