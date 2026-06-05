---
name: project_accounts_source_sharepoint_constraints
description: Constraints/options for refreshing accounts+users data without requiring each end user to have SharePoint synced locally
metadata:
  type: project
---

The "Refresh Account Data" button (Time Allocation page,
`_refresh_account_data` in `pages/time-allocation-tool.py`) errors for users
without the CNA SharePoint synced locally: it calls
`startup.regenerate_accounts_parquet(force=True)` -> `get_paths()` ->
`find_task_tracker_root()`, which only probes `Path.home()`-rooted synced
folders (`config.TASK_TRACKER_ROOT_HINTS` / `POTENTIAL_ROOTS`) and has no UNC
fallback. Source Excels: `CNA Personnel - Temporary.xlsx` (sheet "CNA Personnel")
and `TasksAndTargets.xlsx` (sheet "Users").

**Key asymmetry:** every *consumer* already reads the cached parquet from the
UNC share `config.PERSONNEL_DIR` (`utils.load_account_lookup`, `load_accounts`,
`load_users_table`, `is_user_admin`, etc.). Only the *producer* (the source
Excel read) needs SharePoint. So the read path is already share-based; only
regeneration is coupled to per-user sync. See [[project_accounts_parquet_daily_regen]].

**Hard constraints stated by the user (2026-06):**
- The source Excel must STAY in SharePoint — cannot be moved/copied out to UNC.
- Tenant allows only org-scoped ("people in your organization") sharing links,
  NOT anonymous "anyone with the link" links. Org-scoped links still require a
  sign-in token — a bare `requests.get` gets the login page, not the file bytes.
- No custom Azure AD app registration is allowed.
- It is acceptable if non-org / unauthenticated users get the error.

**Viable options under those constraints:**
1. MSAL + a Microsoft FIRST-PARTY public client ID (e.g. Graph CLI app
   `14d82eec-204b-4c2f-b7e8-296a70dab67e`) — registers nothing on our side;
   token is acquired for the already-signed-in user (silent via WAM or one-time
   sign-in), then `GET /shares/u!{base64(link)}/driveItem/content` -> bytes ->
   pandas. Only blocker: tenant consent policy for that app + `Files.Read.All`.
   Adds `msal` to requirements.
2. Centralize the producer (one synced machine / existing synced launches writes
   the parquet to the share); end-user button becomes a safe "Reload latest"
   that just re-reads the share + clears caches. No link, no Azure, no new deps.
3. Power Automate "HTTP request received" flow returns the file bytes; app does
   `requests.get(flow_url)`. No registration/MSAL, but the HTTP trigger needs
   Power Automate PREMIUM licensing.

**Why:** username/password (ROPC) and browser-cookie extraction die under MFA and
are insecure — not options. A token always flows through *an* Azure AD client app,
but it need not be one we register (that's what option 1 exploits).

**RESOLVED (2026-06-05): Option 2 implemented.** The probe showed the tenant
requires admin consent for the first-party Graph CLI client (AADSTS admin-approval),
and the user ruled out any Azure app, so Option 1 was dropped. What shipped:
- The Time Allocation **"Refresh Account Data"** button keeps its label, but
  `_refresh_account_data()` no longer rebuilds from source: it clears
  `utils.load_account_lookup` / `utils.load_accounts` / `_account_lookup_for_dir`
  and re-reads the parquet from `config.PERSONNEL_DIR` (works for every user, synced
  or not; the old SharePoint error is gone). It guards a transient empty read (keeps
  current selections + shows an error instead of blanking valid rows).
- New producer `CODE - do not open/refresh_data.py` force-rebuilds accounts+users
  parquet reusing startup's engine; fails silently (exit 0) if source unreachable.
- Windows Scheduled Task **"CNA Console Data Refresh"** runs it on JR's machine
  twice daily on weekdays (09:10 / 15:10), `-LogonType Interactive` (OneDrive only
  mounts in a logged-on session), windowless via `pythonw.exe`.
- `startup.main()`'s per-launch regen was left intact (bonus producer on any synced
  launch), failing silently as before.
- Hardening (adversarial review): `startup.save_parquet` writes atomically (temp +
  os.replace); `regenerate_accounts_parquet` deletes old dated files only AFTER the
  new write and skips writing an empty source (closes the empty/torn-read window);
  `refresh_data.py` guards `import startup` so a missing config can't make it raise.
  Users-cache refresh (`load_users_table` etc.) was deliberately left to the producer
  + launch/TTL, NOT wired into the account-focused button. `delete_old_parquet_files`
  in startup.py is now unused (dead helper, left in place).

**Caveat:** freshness depends on JR's machine being logged on at 09:10/15:10; if it's
off, consumers keep using the last good parquet (nothing breaks, just stale).
`msal` was NOT added to requirements (only used for the throwaway probe; uninstalled).
