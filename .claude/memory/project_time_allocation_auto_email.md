---
name: project_time_allocation_auto_email
description: How the Time Allocation "missing time entries" reminder emails are designed, sent, and gated
metadata:
  type: project
---

Auto-reminder feature: emails employees whose **total logged account time for the
CURRENT week (Mon–Fri) is under 20 hours** (`WEEKLY_THRESHOLD_HOURS` in
notify_missing_time; checked Friday 3 PM via `ta_store.work_week_bounds` +
`ta_store.total_seconds`). Employee body is a fixed "Leadership Team" message.

**Where it runs / how it sends.** There is no central server. The job rides the
same machine + scheduled-task pattern as `refresh_data.py` (see
[[project_accounts_source_sharepoint_constraints]]): JR's machine, interactive
logon, `pythonw.exe`. New script `CODE - do not open/notify_missing_time.py`
(Friday-only task at 15:00, registered via `scripts/register_notify_task.ps1` —
the email reports time "as of 3:00 PM"; the morning 09:10 data refresh keeps
users.parquet fresh). It sends via
**classic Outlook COM `.Send()`** (`outlook_mailer.py`) AS the shared mailbox
`CNAConsole@clarknationalaccounts.com` — auto-mapped on JR's profile, so the
sender uses `SendUsingAccount` (account match), falling back to
`SentOnBehalfOfName`. Requires classic desktop Outlook (New/Web Outlook has no COM).

**Settings.** Per-department, default **OFF**, stored as an `auto_email` block
inside `time_allocation_settings.json` (helpers `utils.load_auto_email_settings`
/ `save_auto_email_settings` / `normalize_auto_email_department`). Admin UI is
`render_auto_email_settings_view()` in `time-allocation-tool.py` (Admin Settings
tab). Knobs: per-dept enabled + manager_recap (cadence is weekly-only and send day is
always Friday — `interval`='weekly' and `send_day`='Fri' are both forced in
`normalize_auto_email_department`, and the scheduled task runs Fridays only);
global from_address, test_recipient, and a master `live` flag.

**Manager recap:** when a dept's `manager_recap` is on, each manager in that dept
(IsManager truthy, has email) gets a weekly-total summary of ALL their reports
(not just under-threshold ones), each report's weekly hours shown and flagged if
under 20h. Reports matched by the `Manager` name field (== manager's Full Name),
with `ManagerEmployeeNumber == manager's EmployeeNumber` as a fallback
(`_reports_to` in notify_missing_time). Multi-manager depts partition correctly;
managers with no matching reports are skipped. It rides the same
enabled/cadence/live/pilot/test/dry-run logic as individual reminders (live → the
manager; pilot/test → the test recipient). Reports whose manager isn't in the dept
go uncovered (edge case). The three GLOBAL controls
(from_address, test_recipient, live) are **developer-only** via
`utils.is_current_user_developer()` — non-developer admins see only the per-dept
table, and their Save preserves the dev-managed globals (don't read absent widget
keys for them). Developer is a new `Developer` flag in users.parquet, mapped in
`startup.load_users_excel` column_map (aliases incl "developer?") and coerced via
`_to_flag`; it's separate from `isAdmin`. Helpers: `utils.is_user_developer` /
`is_current_user_developer` (mirror the admin ones). Departments list comes
from distinct `Department` values in users.parquet (`utils.list_departments`).

**Safety tiers in notify_missing_time.py:** `--dry-run` (log only), `--test
[--to]` (real per-employee emails redirected to the test recipient), default
scheduled run = silent until `live=true`; while `live=false` it sends only a
per-dept DIGEST preview to test_recipient (or just logs if none). Live runs write
a per-dept "last sent" marker (`time_allocation_notify_state.json` on
PERSONNEL_DIR) so cadence isn't repeated; `--force` bypasses the cadence gate.

**Why default-OFF matters:** users.parquet has ~181 people across ~20
departments, but most departments don't use the tool at all (everyone shows 0
rows). Enabling a non-using department would nag its whole staff. Only enable
teams that are supposed to log time.

**Shared read layer:** the on-disk partition convention now lives in
`CODE - do not open/time_allocation_store.py` (normalize_login, daily_file,
window read, missing_weekdays, previous_work_week). `time-allocation-tool.py`'s
private helpers delegate to it so the page and the headless job share ONE source
of truth for the layout. Verified: login→partition matching is by the `User`
login (NOT name initials — e.g. "Abigail Risser" logs in as `ahummel`).

**Outlook "Send As" gotcha (hard-won).** `outlook_mailer.py` drives CLASSIC
Outlook via COM (New/Web Outlook has no COM). To send AS `CNAConsole`, the shared
mailbox MUST be added to **classic** Outlook (it can be present in New Outlook yet
absent from classic — that was the whole bug). The mailer finds the mounted store
whose owner SMTP matches the from-address, reads the owner as a real Exchange
(`Type == "EX"`) directory object, and stamps `PR_SENT_REPRESENTING_*` (entryid +
name + addrtype "EX" + the X.500 DN) from it. Setting `SentOnBehalfOfName` to the
raw SMTP string does NOT work: Exchange runs the Send-ON-BEHALF check (not
Send-As) and bounces with `MapiExceptionSendAsDenied` ("permission to send on
behalf of"). Full Access / auto-map ≠ a usable COM send identity; the mounted
store's EX owner is what the manual From-dropdown uses. NDRs from "System
Administrator" are the automated postmaster bounce to your own inbox, not a real
admin. `.Send()` returning OK only means it reached the Outbox — verify via Sent
(repr_email should be the `/o=ExchangeLabs/...cn=...-CNAConsole` DN, not SMTP) +
no NDR. Also: don't trial-and-error live sends — it spooks people with bounces.
