---
name: cna-brand-formatting
description: Apply the CNA Console brand/formatting system (CNA Brand Guidelines 2025) to a Streamlit app — the .streamlit/config.toml theme, the utils.get_global_css() CSS layer, utils.render_page_header(), and the home-page accent-card treatment. Use when porting CNA branding to another/older version of the app, restyling pages to match the reference app, or adding a new page that must follow the brand system. Styling only — never changes business logic, data flows, or file structure.
---

# CNA Console Brand Formatting

## Purpose & when to use

This skill ports the **CNA Brand Guidelines (2025)** visual system onto a Streamlit
app. Invoke it when:

- Updating an older/other version of the CNA Console to match the current
  reference app's look.
- Restyling existing pages to the brand system.
- Adding a new page that must follow brand conventions.

**This is a styling/formatting task only.** Do not change business logic, data
flows, parquet schemas, UNC paths, page workflows, or file structure. Touch only
theming, CSS, the page-header component, and the home page's visual treatment.

The brand system has three layers:
1. `.streamlit/config.toml` — Streamlit's native theme tokens
2. `utils.get_global_css()` — the app's custom CSS layer (single source of truth
   for brand styling), injected on every page via `st.markdown(..., unsafe_allow_html=True)`
3. `utils.render_page_header(...)` — the standard page header component, plus the
   `page_registry` helper functions it depends on

## Bundled reference files (verbatim code)

The exact code to apply lives in this skill's `reference/` folder. Read each one
when you reach the corresponding part below:

- `reference/config.toml` — the complete `[theme]` block for `.streamlit/config.toml`
- `reference/global_css.py` — the complete `get_global_css()` function
- `reference/page_header.py` — `render_page_header()` + the `page_registry` section/quote helpers
- `reference/home_page.py` — the home page's page-scoped CSS + section-header markup

Apply the bundled code verbatim. The only thing you may adapt is `data-testid`
selector names, and only if the target app's Streamlit version requires it
(see Part 0).

---

## Part 0 — Pre-flight checks (do these first)

1. **Find the equivalent files.** Locate the target app's `.streamlit/config.toml`,
   its `utils.py` (or whichever shared module holds helpers — look for an existing
   `get_global_css()` or similar), and its `page_registry.py` if one exists. Report
   what you find before editing.
2. **Check the Streamlit version** (`pip show streamlit` or `streamlit version`).
   The bundled CSS targets **Streamlit 1.57** `data-testid` names (e.g. `stMain`,
   `stSidebar`, `stElementContainer`, `stMarkdownContainer`,
   `stVerticalBlockBorderWrapper`, `stPageLink`, `stExpander`, `stMetric`,
   `stBaseButton-primary`, `stToolbar`, `stHeader`, `stDecoration`). If the target
   runs an **older Streamlit**, some testids differ — verify each selector against
   the actual rendered DOM and adapt. Do not blindly paste onto an old version.
3. **Do not commit anything.** Make the edits and report back; the user handles
   building/committing.

---

## Part 1 — `.streamlit/config.toml`

Replace the `[theme]` block with the contents of `reference/config.toml` (merge —
keep any non-theme sections the app already has).

Key points: `baseFontSize = 17`; `baseRadius`/`buttonRadius = "small"` (inputs and
buttons keep a soft radius — cards get squared to `0` via CSS). If the target's
Streamlit rejects unknown granular keys (`redColor`, `chartCategoricalColors`,
etc.), drop only the unsupported ones but always keep `base`, `primaryColor`,
`backgroundColor`, `secondaryBackgroundColor`, `textColor`, `borderColor`.

---

## Part 2 — Brand spec reference (color + type tokens)

These are the canonical values. They are encoded as CSS custom properties in
`:root` (Part 3); reuse them (`var(--cna-green)` etc.) for any new styling.

### Color tokens

| Token | Hex | Use |
|---|---|---|
| `--cna-green` | `#00B19A` | Primary brand, accent top edges, hover borders |
| `--cna-green-dark` | `#00917F` | Primary button hover |
| `--cna-teal` | `#06828D` | Eyebrows, links, secondary accents, KPI/metric labels |
| `--cna-teal-dark` | `#056B74` | Active sidebar link text |
| `--cna-navy` | `#002E65` | Headings, `strong`/`b`, KPI/metric/timer values |
| `--cna-navy-soft` | `#334F82` | Body copy on light surfaces |
| `--cna-turquoise` | `#08B4C5` | Accent |
| `--cna-sky` | `#D0ECEE` | Active sidebar link bg, dataframe header bg |
| `--cna-sky-lite` | `#EFFAFA` | Callout fill, secondary background, sidebar hover |
| `--cna-white` | `#FFFFFF` | Card fill |
| `--cna-ink` | `#1F2A44` | config `textColor` |
| `--cna-muted` | `#6B7F9A` | `.app-desc` captions |
| `--cna-rule` / `--cna-border` | `#B9DDE1` | Universal hairline border |
| `--cna-rule-soft` | `#E1F1F3` | Softer rule |
| `--cna-danger` | `#D64550` | Destructive actions (reset button, live pulse dot) |
| `--cna-danger-dark` | `#B23640` | Destructive hover |
| card shadow | `0 14px 30px rgba(0,46,101,0.10)` | Card hover elevation |

### Typography

- **Headings** — Poppins (weights 500/600/700/800). `h1` = 800, `h2` = 700,
  `h3–h6` = 600. Letter-spacing `-0.02em` on `h1`, `-0.01em` on `h2–h6`.
  Color `--cna-navy`.
- **Body** — Work Sans (400/500/600 + italic 400/500). Body text color
  `--cna-navy-soft`.
- **Data** (KPI values, timers, metric values, code) — JetBrains Mono (400/500).
- All three families load via a Google Fonts `@import` at the top of the CSS.

### Per-element type specs

- `config.toml`: `baseFontSize 17`; `headingFontSizes
  ["2.75rem","2.25rem","1.75rem","1.5rem","1.25rem","1rem"]`; `headingFontWeights
  [700,600,600,600,600,600]`.
- `.cna-eyebrow` — `0.69rem`, weight 600, letter-spacing `0.18em`, uppercase,
  color teal; signature `.dot` is a 6×6px green circle.
- `.cna-pageheader .header-title` — `clamp(1.9rem, 3.4vw, 2.7rem)`, weight 800,
  line-height 1.05, letter-spacing `-0.02em`, color navy.
- `.cna-pageheader .header-kicker` — `1.02rem`, weight 400, line-height 1.55,
  color navy-soft, `max-width: 760px`.
- `.header-accent` — `56px × 4px` bar, `linear-gradient(90deg, green, teal)`.
- `.app-title` — `1.05rem`, weight 600, navy. `.app-desc` — `0.875rem`, muted.
- `.cna-note` — `0.9rem`, navy-soft; sky-lite fill, 1px rule border, 3px left
  teal accent (`.is-green` → green accent).
- `.timer-display` — `42px` mono, weight 500, navy. `.timer-label` — `0.78rem`,
  weight 600, uppercase, letter-spacing `0.14em`, teal.
- `.kpi-value` — `30px` mono, weight 500, navy. `.kpi-label` — `0.72rem`,
  weight 600, uppercase, letter-spacing `0.12em`, teal.
- `st.metric` label — `0.72rem`, weight 600, uppercase, letter-spacing `0.06em`,
  teal; value rendered in mono navy.
- Sidebar page links — `0.92rem`; sidebar captions — `0.72rem`.

---

## Part 3 — `utils.get_global_css()`

Find (or create) the target app's global-CSS function and replace its body with
the contents of `reference/global_css.py`. Requirements:

- Must be `@st.cache_data`-decorated, return one big `<style>...</style>` string,
  and be injected on every page via
  `st.markdown(utils.get_global_css(), unsafe_allow_html=True)`.
- Adapt `data-testid` names **only** if the Streamlit version requires it.
- If the older function held app-specific rules for widgets that still exist,
  **merge them in** — do not drop styling you can't account for.

### Things in this CSS that must stay intact

- **The `@import` line stays first** — it loads Poppins / Work Sans / JetBrains Mono.
- **The style-container collapse rule** is load-bearing:
  `[data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] > style) { display: none !important; }`.
  Every `st.markdown("<style>…")` call creates an invisible flow element that
  still contributes a vertical-block gap; this rule collapses those so they
  don't push the first real element down from the top of the page. It **must use
  a descendant combinator** for the style block (`[data-testid="stMarkdownContainer"] > style`),
  **never** a fixed `stElementContainer > stMarkdown > stMarkdownContainer > style`
  direct-child chain — Streamlit nests extra width/latex wrapper `<div>`s in
  between, so a strict direct-child chain silently matches nothing and the
  top-of-page gap reappears. `:has()` needs a reasonably modern browser engine;
  if the host (or its embedded WebView) is old, verify support.
- **`.block-container { padding-top: 0 !important; }` is intentional** — combined
  with the collapsed `stHeader` it makes content run to the very top. Pages get
  their top breathing room from `.cna-pageheader`'s `margin: 22px 0 4px 0`.

---

## Part 4 — `render_page_header()` and `page_registry` helpers

The standard page header is an **eyebrow + large left-aligned Poppins title +
green→teal accent bar + optional kicker line**.

Add/replace `render_page_header` plus its two registry helpers using
`reference/page_header.py`. Notes:

- It depends on `get_registry_page_section(page_title)` (→ nav section name, used
  as eyebrow text) and `get_registry_page_quote(page_title)` (→ a one-line quote,
  used as the kicker). **If the target's `page_registry.py` already has equivalent
  helpers, reuse them** instead of duplicating.
- The helpers expect `page_registry` to expose `SECTION_PAGES: dict[str,
  list[PageEntry]]` and `HOME_PAGE`, with each `PageEntry` carrying `path`,
  `title`, and optionally `quote`.
- If the target registry has a different shape (no sections, no `quote` field),
  **do not restructure the registry.** The helpers returning `""` simply means no
  eyebrow/kicker is shown — that is acceptable. You can also pass `eyebrow=`
  explicitly at call sites.
- Ensure `utils.py` has `import html` and `from functools import lru_cache`.

---

## Part 5 — Per-page integration pattern

Every page should follow this opening pattern (adapt to each page's existing
structure — the key is the four styling calls, in this order, near the top):

```python
st.set_page_config(
    page_title=PAGE_TITLE,
    layout="wide",
    page_icon=utils.get_app_icon(),   # custom favicon on every page
)
utils.render_app_logo()                                    # logo in Streamlit chrome
st.markdown(utils.get_global_css(), unsafe_allow_html=True) # inject brand CSS
utils.render_page_header(PAGE_TITLE)                        # eyebrow + title + accent bar
```

- If `utils.get_app_icon()` / `utils.render_app_logo()` don't exist, add them
  (they return/render the app's icon and logo asset paths and no-op gracefully if
  the file is missing). If the app already has its own logo/icon mechanism, keep it.
- Go through **every page file** and: (a) ensure `get_global_css()` is injected,
  (b) replace any old bespoke title/header markup with `utils.render_page_header(PAGE_TITLE)`,
  (c) remove now-redundant old CSS blocks that the global CSS supersedes.
- Replace hard-coded colors/fonts in page-level inline styles with the
  `var(--cna-*)` tokens. For inline styles inside iframes (e.g. a calendar
  component's `custom_css`) the CSS variables won't resolve — use the literal hex
  there with the token name in a comment.

---

## Part 6 — Home / landing page

The reference home page is a **lightweight navigation grid with no hero banner and
no `render_page_header` call** — it deliberately has neither. Apply the equivalent
treatment using `reference/home_page.py`:

1. **No hero / no big header block.** If the older home page has a hero, banner,
   or large centered title, remove it.
2. **Navigation cards** are bordered `st.container(border=True)` blocks; the
   page-scoped CSS turns them into accent cards (3px green top edge → teal on
   hover, hover lift + shadow). Inject that page-scoped `st.markdown` block on the
   home page only, **after** the global CSS.
3. **Section headers** above each group of cards use the eyebrow device + an `<h2>`
   (`.cna-section-head` markup in the reference file).
4. **The `[data-testid="stMain"] .block-container { padding-top: 22px !important; }`
   rule is required** — the global CSS zeroes `.block-container` top padding, and
   the home page has no `.cna-pageheader` to supply breathing room, so without
   this rule the first element sits flush against the top of the window. Its
   higher specificity cleanly overrides the global `.block-container` rule.

If the target's home page DOM differs (cards not from `st.container(border=True)`,
no section grouping), adapt the selectors/markup to whatever it actually uses —
the goal is: accent cards, eyebrow section labels, no hero, ~22px top breathing room.

---

## Part 7 — Critical gotchas (do not skip)

- **`get_global_css()` is `@st.cache_data`-cached.** After editing it you must
  clear the cache (or fully restart the app) to see changes — editing the string
  alone won't refresh a running session.
- **The style-collapse `:has()` rule is the most fragile piece.** If after the
  port any page shows a visible empty gap at the very top, that rule isn't
  matching — re-inspect the live DOM, confirm the `stElementContainer` /
  `stMarkdownContainer` testids, and confirm `<style>` is a direct child of
  `stMarkdownContainer`. Use a descendant combinator, never a brittle
  direct-child chain.
- **`data-testid` names are Streamlit-version-specific.** On an older Streamlit,
  expect to adjust selector names; verify against the rendered DOM.
- **Don't touch business logic.** Parquet schemas, UNC paths, page workflows,
  data-loader caching — all out of scope. Styling only.
- **Merge, don't clobber.** If the older `utils.py` / `config.toml` / pages
  already contain app-specific rules or settings, preserve them alongside the
  brand system.

---

## Part 8 — Verification checklist

After the port, manually confirm:

- [ ] App launches without errors; every page injects the global CSS.
- [ ] Headings render in Poppins; body text in Work Sans; KPI values / timers /
      `st.metric` values in JetBrains Mono.
- [ ] Primary color is CNA Green `#00B19A`; links are CNA Teal `#06828D`.
- [ ] Each non-home page shows the eyebrow + large left-aligned title +
      green→teal accent bar (`render_page_header`).
- [ ] No empty gap at the top of any page; content runs cleanly under the
      (collapsed) Streamlit header.
- [ ] Home page: no hero, accent navigation cards (green top edge, hover lift),
      eyebrow section labels, ~22px top breathing room.
- [ ] Bordered containers, expanders, dataframes, `st.metric`, alerts have
      **sharp (0-radius) corners**; inputs and buttons keep their **small radius**.
- [ ] Sidebar: active page link has a sky highlight + teal text; hover gives a
      soft sky-lite highlight.
- [ ] Primary buttons darken to `#00917F` on hover with a subtle green glow.

Report back what files you changed and anything you had to adapt for the
Streamlit version before the user builds/commits.
