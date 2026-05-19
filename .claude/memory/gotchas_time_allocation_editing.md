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

**Why these matter:** The editing window in particular has tripped people up — they think it's broken when really they're trying to edit a day outside the allowed range. The channel-order rule means dev/test environments behave differently from prod (which has more history).

**How to apply:** Before "fixing" any of these, confirm the user actually wants the rule changed vs. is running into the expected behavior. The 50-row threshold and the editable-window window are deliberate UX choices, not bugs.
