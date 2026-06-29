---
name: project_user_profile_offline_cache
description: Local per-user cache of the current user's users.parquet row — written by startup when connected, used as an offline fallback so identity resolves without the network drive
metadata:
  type: project
---

When the network drive is unreachable, `users.parquet` can't be read, so the app
used to fall back to the raw Windows login (e.g. "jfitouri" instead of "Jennifer
Fitouri") — and that login could get written into saved data (notably Time
Allocation entries). Fix: a local, per-user fallback cache of the current user's
own `users.parquet` row.

**Module:** `CODE - do not open/user_profile_cache.py` (Streamlit-free; stdlib +
pandas). Stores `user_profile_cache.json` beside the app code (per-user local
install, like favorites.json) — gitignored, untracked so it survives Repair's
`git reset --hard`. The stored login is re-checked on read, so a cache from a
different account is ignored.

**Write:** `startup.py main()` (runs every launch via StartApp.bat) reads the
authoritative network `users.parquet` and calls `save_current_user_profile()`.
When connected it refreshes; when offline the read fails and the previous cache
is left intact. Best-effort — wrapped so it never blocks startup. NOT added to
`refresh_data.py` (that runs headless as a service account, wrong user context).

**Read / fallback:** `utils._cached_profile_value(login, *aliases)` resolves a
field from the cache, but ONLY for the current OS user (the only row cached).
Wired into `get_full_name_for_user` (after the network map misses),
`get_user_department` (before final ""), and `is_user_admin` / `is_user_developer`
(when `load_users_table()` is empty). Because `effective_role()` →
`_role_for_login()` calls those, the current user's admin/developer status also
survives offline.

**Integration / safety:**
- Strictly additive — online, the network map/table resolve first and the cache
  paths aren't reached, so zero behavior change when connected.
- `import user_profile_cache` in utils is wrapped in try/except (sets it to None)
  so a missing/half-pulled module can never brick app load; `_cached_profile_value`
  no-ops when it's None.
- Existing users: cache is created on their first connected launch after updating;
  until then, today's behavior. New users: same — primes on first connected launch.
  No cache (new hire not in roster, or first launch offline) → graceful raw-login
  fallback, same as before.

Related: [[project_network_drive_indicator]] (the sidebar online/offline dot),
[[project_developer_view_as_role]] (effective_role, which this preserves offline).
