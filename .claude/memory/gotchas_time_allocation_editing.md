---
name: gotchas_time_allocation_editing
description: Time allocation tool editing window rules and channel-order sort behavior — both are non-obvious and easy to break
metadata:
  type: project
---

`pages/time-allocation-tool.py` has two behaviors that look like bugs but are intentional:

1. **Editing window is strict: This Week or Last Week only.** Entries can be added/edited only for Mon-Fri of the current or previous week (`_editable_window()` / `_is_editable_day()`). The This Period calendar view is read-only. There is **no grace-period buffer**. Admins bypass this restriction on the Input tab; the admin Edit Entries table can change any date.

2. **Channel dropdown order switches modes at 50 saved selections.** New rows default to the first channel option (Resupply by default). The dropdown is shown in `CHANNEL_OPTIONS`' fixed defined order until there are 50+ total saved channel selections in history, at which point it sorts by usage frequency. Channels are `Projects` and `Resupply`.

3. **Saving a day replaces all prior rows for the same user+date.** This happens across existing export files before writing the new file — not just the latest one. (Also documented in CLAUDE.md's data layout section.)

4. **Reporting Name and Customer Code autofill each other.** When a Reporting Name has multiple codes, the first alphabetically wins. This pairing logic also runs in the admin Edit Entries data editor.

5. **Admin "Add Entries" form (Admin Settings tab) APPENDS; it does not replace.** Unlike the Input tab's `save_records` (which replaces all of a user's rows for a day), `_save_admin_added_records` reads the user's existing rows for the day, concatenates the new rows, then writes — so it adds to the day instead of wiping it. It also targets *any* user (picked from `users.parquet` via `_user_login_lookup`) and is **restricted to the current fiscal period** (`_current_period_bounds` / `_is_within_current_period`), not the This Week/Last Week window. It reuses the Input tab's row widgets via `_render_custom_field_widget(..., key_prefix="ta_add")` and its own `ta_add_*` session keys.

6. **Calendar "+X more" popover is fixed via injected CSS, not the package.** `streamlit_calendar` calls `Streamlit.setFrameHeight()` only once on mount (`useEffect(...,[])`, no arg), so the component iframe height is locked to the calendar's initial height and never grows. FullCalendar's default more-popover (`.fc-popover.fc-more-popover`) is `position:absolute` with no `max-height`, so on a day with many entries it overflows that fixed iframe and gets clipped with no way to scroll — the original bug. Fix lives in `_CALENDAR_MORE_POPOVER_CSS`, appended to the calendar's `custom_css` (which lands inside the iframe): pin the popover with `position:fixed` (iframe-viewport-relative), cap it to `100vh`, and make `.fc-popover-body` scroll. This works because (a) FC 6.1.9 portals the popover into `.fc-view-harness` — inside the styled wrapper, so the nested `custom_css` selectors match — and (b) no calendar ancestor uses `transform`, so `fixed` escapes the harness's `overflow:hidden`. **Do NOT patch the streamlit_calendar package** — repair/update reinstall `requirements.txt` and would wipe it. If you "tidy" the calendar CSS, keep the fixed-position + scrollable-body popover rules or the clipping bug returns.

**Why these matter:** The editing window in particular has tripped people up — they think it's broken when really they're trying to edit a day outside the allowed range. The channel-order rule means dev/test environments behave differently from prod (which has more history).

**How to apply:** Before "fixing" any of these, confirm the user actually wants the rule changed vs. is running into the expected behavior. The 50-row threshold and the editable-window window are deliberate UX choices, not bugs.
