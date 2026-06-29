---
name: project_developer_view_as_role
description: is_current_user_admin/developer return an EFFECTIVE role honoring a developer-only "View as" override
metadata:
  type: project
---

`utils.is_current_user_admin()` and `utils.is_current_user_developer()` no longer
read the raw users.parquet flag directly — they return the **effective role** via
`utils.effective_role()`, which is `'user' | 'admin' | 'developer'` (highest actual
flag wins: developer > admin > user).

**View-as override:** an ACTUAL developer can preview the app as user/admin/developer
from the **Settings dropdown** in the sidebar (`app.py`). It sets session-state key
`_dev_view_as_role` (handled via `?settings_action=view_as_<role>` →
`utils.set_view_as_role`). `effective_role()` honors that override ONLY when the OS
user is a real developer (`is_actual_developer()`), so a non-developer can never
escalate by injecting session state. Because nav visibility (`app.py` →
`get_visible_sections(is_current_user_admin())`) and every admin/dev page gate run
through these helpers, the switch hides/shows pages and dev-only controls app-wide
with no per-page changes.

**Impersonate a specific user:** Settings → "View as User…" opens a dialog
(`_show_view_as_user_dialog`) listing all users (`utils.list_user_logins()` from
users.parquet). Picking one sets session key `_dev_view_as_user` (login) via
`utils.set_view_as_user`; `effective_role()` then returns THAT user's role
(`_role_for_login`). It takes precedence over the plain role override, and the two
setters clear each other so only one is active. `utils.view_as_user_login()` returns
the impersonated login (None for non-devs). This is permissions-only — it does NOT
change `get_os_user()` identity (data/logging still run as the real OS user). The
generic "View as User" role link was replaced by this picker; "View as Admin" /
"View as Developer" remain quick generic previews.

**Gotchas:**
- To check the RAW flag regardless of view-as (e.g. to decide whether to show the
  view-as control itself), use `is_actual_developer()` or
  `is_user_admin/is_user_developer(get_os_user())` — NOT the `is_current_*` helpers.
  The view-as rows are gated on `is_actual_developer()` so a developer viewing as
  "user" can always switch back.
- `_view_as_override()` is safe outside a Streamlit runtime (returns None), so
  headless callers always get the actual role.
- `Developer` is a users.parquet column (see [[project_time_allocation_auto_email]]
  for how it's mapped in `startup.load_users_excel`).
