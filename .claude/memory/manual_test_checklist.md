---
name: manual_test_checklist
description: Smoke tests to run after meaningful edits — there are no automated tests in this repo
metadata:
  type: reference
---

There are no automated tests in this repo. Use these manual smoke tests after meaningful edits.

## Baseline (run for any change)

- `setup.bat` still installs successfully on a fresh clone
- `StartApp.bat` still launches the app
- Home page navigation still works
- Page titles and quotes still render correctly
- Logs still write to the expected user folder under `config.LOGS_ROOT_DIR`

## Task workflow

- Task tracker start / pause / resume / end / upload works
- Live activity appears and clears correctly
- Archived paused task can be resumed and deleted
- Analytics still loads historical data
- **Test BOTH LS and DA versions** — they share a file but have separate code paths ([see LS vs DA differences](gotchas_task_tracker_ls_vs_da.md))

## Admin workflow

- Management page loads tasks, targets, users, and task log
- Task definition updates persist
- Task target edits persist
- Task log edits and deletes persist correctly
- Admin Logs page still parses log lines

## Packaging

- Upload mode works for Excel and CSV
- Paste mode parses tab-separated rows
- Unmatched items are reported
- Destination selection works (warehouse and manual address)
- Estimate runs and response renderers do not crash

## FedEx

- Validator filters still work
- Dispute file generates
- Email draft flow opens (default handler OR Outlook COM fallback)
- Mark disputed / clear disputed workflows persist correctly
- Remember: ["Mark as Disputed" acts on all visible rows](gotchas_fedex_validator.md), not a selected subset

## Stocking agreements

- Both tabs (General Resupply, Consumables) generate DOCX
- PDF either generates or fails gracefully with a warning (not a hard error)
- Pricing table rows render correctly (template index sensitivity — see CLAUDE.md "Stocking agreement templates")

## Time allocation

- Day saves replace prior rows for same user+date across files
- Editing window restriction (This Week / Last Week only) holds — [editing rules](gotchas_time_allocation_editing.md)
- Admin export filters work
- Admin "Add Entries" form (Admin Settings tab): user picker lists all known users; date picker is bounded to the current fiscal period; adding appends to (does not replace) the user's existing rows for that day, and the new rows appear in Edit Entries / Exports
