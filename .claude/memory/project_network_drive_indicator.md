---
name: project_network_drive_indicator
description: Sidebar connected/disconnected indicator — how network-drive reachability is detected and why a socket probe (not a UNC path check) is used
metadata:
  type: project
---

The sidebar shows a network-drive connection indicator at the very top (first
element inside `with st.sidebar:` in `app.py`): green dot + "Connected" when the
`\\therestaurantstore.com\920` share is reachable, red dot + ⚠ + "Disconnected"
when it isn't. Hover tooltip (native `title` attr, same pattern as the pin
links) tells offline users to turn on the VPN or connect at an office.

Implementation lives in `utils.py`:
- `get_network_drive_host()` parses the UNC host out of `config.COMPLETED_TASKS_DIR`
  (and fallbacks) so it follows config.py instead of hardcoding the hostname.
- `is_network_drive_connected()` tests reachability with a **bounded TCP connect
  to SMB port 445** (1.5s timeout), NOT `os.path.exists()` on the UNC path.
- `render_sidebar_connection_status(page_key=...)` does the render + caching.
- CSS classes `.cna-conn-status` / `.cna-conn-online` / `.cna-conn-offline` live
  in `get_global_css()`.

**Why a socket probe, not a path check:** `os.path.exists()` / any I/O on an
unreachable UNC path can block for many seconds (SMB connect timeout) and would
stall every page load; a worker thread doing that I/O can't be killed and leaks.
A socket with an explicit timeout is fast when online (~0.1s) and bounded when
offline. Reachability of 445 is a good proxy for "can read the share" on this
internal network (on VPN/office it answers, off it doesn't).

**Refresh cadence:** intentionally NOT a live poll. The result is cached in
`st.session_state["_conn_status_cache"]` keyed by the active page title, so it
re-checks on page load/reload and on navigation to a different page, but reuses
across same-page reruns — so an offline user isn't charged the connect timeout
on every widget interaction. Pass `page_key=None` to force a fresh check.

Related: [[project_time_allocation_load_perf]] (same "don't do slow network I/O
in the main rerun path" principle).
