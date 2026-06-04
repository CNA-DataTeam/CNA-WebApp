---
name: gotchas_time_allocation_input_fragment
description: Time allocation input area is an st.fragment; nested run_every fragments blank the whole page — use a client-side ticker instead
metadata:
  type: project
---

`pages/time-allocation-tool.py`'s Input tab renders inside `@st.fragment render_input_view`. Things to know before touching its rerun/refresh mechanics:

1. **Never put an auto-rerunning (`run_every=`) fragment inside the input fragment/column.** A nested `run_every` fragment fires on its own timer and races with active data entry: while a user is editing, its mistimed auto-rerun sends the frontend a delta it can't place against the (dynamic, row-count-changing) element tree and **the entire app goes white — sidebar, header, everything.** Symptom: intermittent full-white-page "sometimes while inputting data." This actually happened with the "time since last refresh" caption.

   **Fix / correct pattern:** for time-based UI that must tick on its own, update it **client-side** with a tiny `streamlit.components.v1.html` iframe running `setInterval` — zero server reruns, no fragment races. The refresh caption does this: the parent mirrors the cached `loaded_at` into `st.session_state["ta_account_loaded_at"]`, and the iframe computes elapsed time from that base in the browser. (`st.markdown` can't run `<script>` — Streamlit strips it — so an iframe component is required for client JS.)

2. **User actions in the input area rerun fragment-scoped via `_rerun_input_fragment()` (`st.rerun(scope="fragment")`), not a full `st.rerun()`** — keeps the heavy module body from re-executing. Exceptions that open a dialog (single-row delete confirm) deliberately use a full `st.rerun()`.

3. **The account lookup is re-resolved from cache at the fragment top each run** (`_account_lookup_for_dir(...)`), shadowing the passed-in args. Required because `@st.fragment` replays the *original* call arguments on a fragment rerun — so reading the cached lookup in-body is what lets the Refresh Account Data button surface freshly-cleared data without a full reload.

4. **Widget-state resets are deferred to the top of the next run.** Streamlit forbids mutating a widget's `session_state` after it's instantiated, so the refresh button (below the rows) can't blank a row's `ta_detailed_account_*`/`ta_detailed_custcode_*` inline — it queues indices in `ta_account_refresh_reset_idxs` and the top of the fragment applies them before the widgets render (same pattern as `ta_detailed_delete_idx`).

**How to apply:** any "live updating" element on this page should tick client-side, not via a server-side `run_every` fragment. See also [[gotchas_time_allocation_editing]].
