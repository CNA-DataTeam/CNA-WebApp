from pathlib import Path
from urllib.parse import quote as _url_quote
import base64
import html
import json
import os
import shutil
import subprocess
import sys
import threading
import time

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import streamlit as st

import page_registry
import utils

UPDATE_FLAG = APP_DIR / ".update_available"

# Preload third-party component once from the main script context.
_AUTOREFRESH_PRELOAD_ERROR: Exception | None = None
try:
    import streamlit_autorefresh  # noqa: F401
except Exception as exc:
    _AUTOREFRESH_PRELOAD_ERROR = exc

LOGGER = utils.get_program_logger(__file__, "App")
LOGGER.info("App bootstrap started.")
if _AUTOREFRESH_PRELOAD_ERROR is not None:
    LOGGER.warning("Auto-refresh preload failed: %s", _AUTOREFRESH_PRELOAD_ERROR)

st.set_page_config(initial_sidebar_state="expanded", page_icon=utils.get_app_icon())
st.markdown(utils.get_global_css(), unsafe_allow_html=True)
utils.render_app_logo()


# -----------------------------------------------------------------
# Update notification — blocks app usage until user clicks Update Now
# -----------------------------------------------------------------
# Build artifacts that may still be tracked in clones from before they were
# gitignored. Discarding local changes is safe because setup.bat regenerates
# them on demand. Once a clone has pulled the commit that untracks them,
# these checkouts become silent no-ops.
_REGENERATED_TRACKED_ARTIFACTS = (
    "CNA Web App.exe",
    "CODE - do not open/installer/CNA Web App.spec",
)

_LAUNCHER_EXE = ROOT_DIR / "CNA Web App.exe"
_SETUP_BAT = ROOT_DIR / "setup.bat"
_REPAIR_BAT = ROOT_DIR / "repair.bat"
_REQUIREMENTS_FILE = APP_DIR / "requirements.txt"
_VENV_DIR = ROOT_DIR / ".venv"


def _find_uv() -> str | None:
    """Locate the uv executable. Mirrors setup.bat's :LOCATE_UV search order:
    PATH first, then every known on-disk install location.

    uv's standalone-installer default has drifted across versions: current
    0.11.x installs to %USERPROFILE%\\.local\\bin, older builds used
    %LOCALAPPDATA%\\uv\\bin. setup.bat also pins fresh installs to
    ROOT_DIR\\.uv\\bin via UV_INSTALL_DIR, so we check that first.
    """
    found = shutil.which("uv")
    if found:
        return found
    candidates = [
        ROOT_DIR / ".uv" / "bin" / "uv.exe",        # pinned by setup.bat (UV_INSTALL_DIR)
        Path.home() / ".local" / "bin" / "uv.exe",  # modern uv default (~/.local/bin)
    ]
    for env_var in ("LOCALAPPDATA", "APPDATA"):      # legacy uv locations
        base = os.environ.get(env_var)
        if base:
            candidates.append(Path(base) / "uv" / "bin" / "uv.exe")
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def _refresh_dependencies() -> None:
    """Run `uv pip install -r requirements.txt` after a successful pull.

    Catches commits that add or upgrade Python dependencies — the routine
    pull updates requirements.txt but doesn't install. uv is fast on no-op
    installs (~1-2s), so we run unconditionally rather than diffing the
    file. Best-effort: failures are logged and the user can recover via
    Settings > Repair App if a dependency lands broken.
    """
    if not _REQUIREMENTS_FILE.exists() or not _VENV_DIR.exists():
        return
    uv_exe = _find_uv()
    if uv_exe is None:
        LOGGER.warning(
            "uv not on PATH or in standard locations; "
            "skipping post-pull dependency refresh."
        )
        return
    env = {**os.environ, "VIRTUAL_ENV": str(_VENV_DIR)}
    try:
        result = subprocess.run(
            [uv_exe, "pip", "install", "--link-mode", "copy",
             "-r", str(_REQUIREMENTS_FILE)],
            cwd=ROOT_DIR,
            capture_output=True,
            timeout=300,
            env=env,
        )
        if result.returncode != 0:
            LOGGER.warning(
                "Post-pull dependency refresh failed (rc=%s): %s",
                result.returncode,
                result.stderr.decode("utf-8", errors="replace"),
            )
    except Exception as exc:
        LOGGER.warning("Post-pull dependency refresh exception: %s", exc)


def _rebuild_launcher_if_missing() -> None:
    """If the pull deleted a previously-tracked exe, run setup.bat to rebuild.

    Best-effort and silent. setup.bat detects the missing exe and triggers
    PyInstaller. If anything goes wrong, the user will get an explicit
    "exe is missing" error on next launch from setup.bat's verification step.
    """
    if _LAUNCHER_EXE.exists() or not _SETUP_BAT.exists():
        return
    try:
        subprocess.run(
            ["cmd.exe", "/c", str(_SETUP_BAT), "/silent"],
            cwd=ROOT_DIR,
            capture_output=True,
            timeout=600,
        )
    except Exception:
        pass


def _apply_update() -> tuple[bool, str]:
    """Pull latest code, clear caches, remove flag.

    Returns (success, error_message). On failure the update flag is preserved
    so the user is prompted to retry on next launch.
    """
    for artifact in _REGENERATED_TRACKED_ARTIFACTS:
        try:
            subprocess.run(
                ["git", "checkout", "--", artifact],
                cwd=ROOT_DIR,
                capture_output=True,
                timeout=15,
            )
        except Exception:
            pass

    try:
        pull = subprocess.run(
            ["git", "pull", "--ff-only"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except Exception as exc:
        return False, f"git pull invocation failed: {exc}"

    if pull.returncode != 0:
        message = (pull.stderr or pull.stdout or "git pull --ff-only failed").strip()
        return False, message

    _refresh_dependencies()
    _rebuild_launcher_if_missing()

    try:
        for d in ROOT_DIR.rglob("__pycache__"):
            if d.is_dir():
                shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass

    UPDATE_FLAG.unlink(missing_ok=True)
    return True, ""


def _launch_repair() -> None:
    """Spawn repair.bat in a detached console window and exit the app.

    repair.bat takes over from here: it force-kills the launcher exe so
    file handles release, resets the working tree to origin/main (or HEAD
    if offline), runs setup.bat to rebuild missing artifacts, and relaunches
    the app. All output is teed to repair.log at the project root.

    Wired to the Settings > Repair App button.
    """
    if not _REPAIR_BAT.exists():
        st.error(f"Repair script not found at {_REPAIR_BAT}.")
        LOGGER.error("Repair button clicked but repair.bat is missing at %s", _REPAIR_BAT)
        return
    try:
        # `start ""` detaches: cmd.exe exits immediately and the new
        # console window survives our os._exit() below. The empty
        # title argument is required by `start`'s quoting rules.
        subprocess.Popen(
            ["cmd.exe", "/c", "start", "", str(_REPAIR_BAT)],
            cwd=ROOT_DIR,
            close_fds=True,
        )
    except Exception as exc:
        LOGGER.error("Failed to spawn repair.bat: %s", exc)
        st.error(f"Could not start repair: {exc}")
        return
    # Hard-exit so we don't keep file handles open while repair.bat
    # rebuilds the launcher. repair.bat also force-kills CNA Web App.exe
    # as a safety net for the parent launcher process.
    LOGGER.info("Launching Repair App; exiting Streamlit process.")
    os._exit(0)


# -----------------------------------------------------------------
# Mid-session update watcher + non-blocking "update available" banner
# -----------------------------------------------------------------
# The startup check (check_updates.py) handles updates present at launch. This
# covers updates that land WHILE the app is open: a background watcher polls
# origin on a timer and flips a flag; a dismissible banner then surfaces on the
# user's next normal interaction — no modal, no forced refresh, never interrupts
# active work. The disruptive pull+restart only happens if the user clicks it.
_UPDATE_WATCH_INTERVAL_SECONDS = 900  # poll origin ~every 15 min while running


@st.cache_resource(show_spinner=False)
def _update_watcher() -> dict:
    """Singleton background watcher (one daemon thread per server process).

    Polls origin for a new commit via check_updates.remote_is_ahead() — a
    read-only `git ls-remote`, no working-tree changes — and writes the result
    into a shared dict the main script reads. The thread makes NO Streamlit calls
    (only git + dict mutation), which is the rule for Streamlit background threads.
    """
    state = {"available": False}
    try:
        import check_updates
    except Exception as exc:
        LOGGER.warning("Update watcher could not import check_updates: %s", exc)
        return state

    def _loop() -> None:
        while True:
            time.sleep(_UPDATE_WATCH_INTERVAL_SECONDS)
            try:
                state["available"] = bool(check_updates.remote_is_ahead())
            except Exception:
                pass

    threading.Thread(target=_loop, daemon=True, name="cna-update-watcher").start()
    return state


def _run_update_now() -> None:
    """Download + stage the pending update (used by the soft banner).

    _apply_update() pulls the new code to disk, refreshes deps, and clears the
    flag — but Streamlit keeps running the already-loaded modules, so an st.rerun()
    would NOT actually load the new code. The update therefore takes full effect on
    the next launch (the every-launch startup check guarantees that). We tell the
    user exactly that rather than claiming an in-place restart, hide the banner, and
    rerun only to clear it."""
    with st.spinner("Downloading update..."):
        ok, err = _apply_update()
    if ok:
        _update_watcher()["available"] = False
        st.session_state["_soft_update_dismissed"] = True
        st.session_state["_post_update_notice"] = (
            "Update installed — it takes full effect the next time you open the app."
        )
        st.rerun()
    else:
        LOGGER.error("Mid-session update failed: %s", err)
        st.error(f"Update failed:\n\n```\n{err}\n```")


def _render_soft_update_banner() -> None:
    """Render the non-blocking mid-session update banner, when appropriate.

    Shows ONLY when (a) the watcher has seen a new commit, (b) the blocking launch
    flag .update_available is NOT set (that case is owned by the modal dialog
    above, which st.stop()s before we reach here), and (c) the user hasn't
    dismissed it this session. Calling this also starts the watcher on first run.
    """
    watcher = _update_watcher()
    if UPDATE_FLAG.exists() or st.session_state.get("_soft_update_dismissed"):
        return
    if not watcher.get("available"):
        return

    with st.container(border=True):
        message_col, update_col, later_col = st.columns([6, 1.7, 1.3])
        message_col.markdown(
            "🔄&nbsp;&nbsp;**A new version of CNA Console is available.** "
            "Update when you're at a good stopping point.",
            unsafe_allow_html=True,
        )
        if update_col.button(
            "Update now", type="primary", width="stretch", key="_soft_update_now_btn"
        ):
            _run_update_now()
        if later_col.button("Later", width="stretch", key="_soft_update_later_btn"):
            st.session_state["_soft_update_dismissed"] = True
            st.rerun()


def _check_for_updates_manual():
    """Run git fetch and return True if updates are available."""
    try:
        subprocess.run(
            ["git", "fetch", "--prune"],
            cwd=ROOT_DIR,
            capture_output=True,
            timeout=30,
        )
        result = subprocess.run(
            ["git", "status", "-uno"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if "behind" in result.stdout:
            from datetime import date
            UPDATE_FLAG.write_text(date.today().isoformat())
            return True
        return False
    except Exception:
        return False


if UPDATE_FLAG.exists():
    @st.dialog("Update Available", width="small")
    def _update_dialog():
        st.markdown("A new version of CNA Console is available.")
        st.markdown("Please update to continue.")
        if st.button("Update Now", type="primary", use_container_width=True):
            with st.spinner("Updating..."):
                ok, err = _apply_update()
            if ok:
                st.success("Updated! Restarting...")
                import time
                time.sleep(1)
                st.rerun()
            else:
                LOGGER.error("Auto-update failed: %s", err)
                st.error(f"Update failed:\n\n```\n{err}\n```")
                st.caption(
                    "The update flag has been kept so you can retry on next launch. "
                    "If this keeps happening, try **Repair App** below "
                    "or share the error above."
                )
        st.divider()
        st.caption(
            "Update keeps failing? **Repair App** does a full reset and rebuild "
            "(~1-2 min). The app will restart automatically."
        )
        if st.button(
            "Repair App",
            use_container_width=True,
            key="_update_dialog_repair",
        ):
            _launch_repair()

    _update_dialog()
    st.stop()


is_admin_user = utils.is_current_user_admin()
LOGGER.info("Admin access check | user='%s' is_admin=%s", utils.get_os_user(), is_admin_user)

# -----------------------------------------------------------------
# Favorites persistence
# -----------------------------------------------------------------
FAVORITES_FILE = APP_DIR / "favorites.json"


def _load_favorites() -> list[str]:
    if FAVORITES_FILE.exists():
        try:
            data = json.loads(FAVORITES_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []


def _save_favorites(favs: list[str]):
    FAVORITES_FILE.write_text(json.dumps(favs, indent=2), encoding="utf-8")


def _toggle_favorite(page_path: str):
    favs = _load_favorites()
    if page_path in favs:
        favs.remove(page_path)
    else:
        favs.append(page_path)
    _save_favorites(favs)


# Pin icons (inline Material Symbols push_pin SVGs). fill="currentColor" lets
# CSS drive the color — brand green when favorited, muted gray when not.
_ASSETS_DIR = APP_DIR / "assets"
_PIN_FILLED_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" '
    'width="18" height="18" fill="currentColor">'
    '<path d="M16,9V4l1,0c0.55,0,1-0.45,1-1v0c0-0.55-0.45-1-1-1H7C6.45,2,6,2.45,6,3v0 '
    'c0,0.55,0.45,1,1,1l1,0v5c0,1.66-1.34,3-3,3v2h5.97v7l1,1l1-1v-7H19v-2C17.34,12,16,10.66,16,9z"/>'
    '</svg>'
)
_PIN_OUTLINED_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" '
    'width="18" height="18" fill="currentColor">'
    '<path d="M14,4v5c0,1.12,0.37,2.16,1,3H9c0.65-0.86,1-1.9,1-3V4H14 M17,2H7C6.45,2,6,2.45,6,3 '
    'c0,0.55,0.45,1,1,1c0,0,1,0,1,0v5c0,1.66-1.34,3-3,3v2h5.97v7l1,1l1-1v-7H19v-2c0,0-1,0-1,0 '
    'c-1.66,0-3-1.34-3-3V4c0,0,1,0,1,0c0.55,0,1-0.45,1-1C18,2.45,17.55,2,17,2L17,2z"/>'
    '</svg>'
)


def _page_url(page_obj: "st.Page", page_paths: dict) -> str:
    """Derive the URL path for a Streamlit MPA Page object.

    Used by the hybrid HTML sidebar rows where we render our own <a> tags
    instead of st.page_link so we can fully control the row's DOM (page name
    + BETA badge layout). Streamlit's MPA router picks up clicks on these
    anchors as long as the href matches a registered page URL.
    """
    url_path = getattr(page_obj, "url_path", None)
    if url_path is not None:
        return f"/{url_path}" if url_path else "/"
    # Fallback for older Streamlit versions — derive from file stem.
    rel_path = page_paths.get(page_obj, "")
    if rel_path:
        return f"/{Path(rel_path).stem}"
    return "/"


# -----------------------------------------------------------------
# Build navigation pages
# -----------------------------------------------------------------
# Registry is now a 3-tier hierarchy: Function -> Department -> PageEntry.
# Admin uses "" as a sentinel dept key so it renders without a middle tier.
home_entry = page_registry.get_home_page()
visible_sections = page_registry.get_visible_sections(is_admin_user)
home_page = st.Page(home_entry.path, title=home_entry.title)
pages = {"": [home_page]}
# Track icons and paths separately — passing icon to st.Page() overrides the favicon
page_icons: dict[st.Page, str] = {home_page: home_entry.icon}
page_paths: dict[st.Page, str] = {home_page: home_entry.path}
page_beta: dict[st.Page, bool] = {home_page: False}
path_to_page: dict[str, st.Page] = {home_entry.path: home_page}
# Nested sidebar structure: [(function_name, [(dept_name, [st.Page])])]
sidebar_sections: list[tuple[str, list[tuple[str, list[st.Page]]]]] = []
for function_name, dept_buckets in visible_sections:
    function_pages: list[st.Page] = []
    dept_rows: list[tuple[str, list[st.Page]]] = []
    for dept_name, entries in dept_buckets:
        dept_pages: list[st.Page] = []
        for entry in entries:
            page = st.Page(entry.path, title=entry.title)
            page_icons[page] = entry.icon
            page_paths[page] = entry.path
            page_beta[page] = bool(getattr(entry, "beta", False))
            path_to_page[entry.path] = page
            dept_pages.append(page)
            function_pages.append(page)
        dept_rows.append((dept_name, dept_pages))
    pages[function_name] = function_pages
    sidebar_sections.append((function_name, dept_rows))

navigation = st.navigation(pages, position="hidden")
# --- Handle sidebar URL-param actions ---
# The HTML dropdown area uses URL params for actions that need a Python
# round-trip: ?fav_toggle=<path> toggles a page's favorite state;
# ?dept_toggle=<name> opens/closes a dept dropdown (state persisted in
# session_state.cna_open_depts).
if "fav_toggle" in st.query_params:
    _toggle_favorite(st.query_params["fav_toggle"])
    del st.query_params["fav_toggle"]
    st.rerun()
# Settings dropdown actions (HTML <a> links with ?settings_action=NAME).
# Each handler does its work, optionally queues a toast for the next render,
# then clears the param and reruns. Same URL-round-trip pattern as the pin
# toggle — slight reload per click but full visual control over the Settings
# dropdown UI.
if "settings_action" in st.query_params:
    _action = st.query_params["settings_action"]
    del st.query_params["settings_action"]
    if _action == "check_updates":
        if not _check_for_updates_manual():
            st.session_state["_settings_toast"] = "You're up to date!"
        # If an update IS available, _check_for_updates_manual writes
        # .update_available; the existing dialog block above picks it up
        # automatically on the next render.
    elif _action == "clear_cache":
        st.cache_data.clear()
        st.session_state["_settings_toast"] = "Cache cleared"
    elif _action == "repair":
        st.session_state["_show_repair_dialog"] = True
    elif _action in ("view_as_admin", "view_as_developer"):
        _role = _action[len("view_as_"):]
        utils.set_view_as_role(_role)
        st.session_state["_settings_toast"] = f"Viewing as {_role}"
    elif _action == "view_as_user_picker":
        st.session_state["_show_view_as_user_dialog"] = True
    # "refresh" is implicit — the rerun below IS the refresh.
    st.rerun()
# Note: dept dropdowns now use native HTML <details>/<summary>, so toggling
# is handled instantly by the browser with zero Streamlit rerun. The
# trade-off: open/closed state is NOT persisted across reruns (when the page
# re-renders, depts revert to their default — active dept open, others
# closed). For an internal nav that's an acceptable trade vs. the full
# refresh on every click that the URL-param approach caused.

user_favorites = _load_favorites()

# Resolve the currently-active page so it can be appended to the favorites
# stack as an implicit "current page" entry when it isn't already saved.
current_page_obj = next(
    (p for p in page_paths if p.title == navigation.title),
    None,
)
current_page_path = page_paths.get(current_page_obj) if current_page_obj else None


with st.sidebar:
    # --- Top row: Home + favorited pages ---
    # Icons show on top-level sidebar links (Home, favorites, Admin pages)
    # and are suppressed inside dept dropdowns where they crowd the text.
    # The container key gives the favorites block a stable CSS hook
    # (`.st-key-cna_favorites_section`) so the active-page highlight can be
    # scoped to this section only — dept dropdowns and Admin rows stay
    # visually neutral when the current page is in them.
    with st.container(key="cna_favorites_section"):
        st.page_link(home_page, icon=page_icons[home_page], use_container_width=True)
        for fav_path in user_favorites:
            fav_page = path_to_page.get(fav_path)
            if fav_page:
                st.page_link(fav_page, icon=page_icons[fav_page], use_container_width=True)

        # Always include the current page in the favorites stack. If it's
        # already a saved favorite (or it's the Home page rendered above),
        # skip to avoid a duplicate row — the existing entry already
        # highlights as active via the scoped [aria-current="page"] CSS.
        # Otherwise render an extra bare page_link that morphs to whatever
        # page the user is viewing.
        if (
            current_page_obj is not None
            and current_page_path != home_entry.path
            and current_page_path not in user_favorites
        ):
            st.page_link(
                current_page_obj,
                icon=page_icons[current_page_obj],
                use_container_width=True,
            )

    # --- Section dropdowns with pin toggles ---
    # Function (top tier) -> Department (middle tier, flat expander when
    # named) -> Page link. Dept-dropdown rows use a HYBRID HTML approach:
    # pin column on the left is a real Streamlit button (invisible overlay
    # on a pin SVG), and the link column is a custom HTML <a> tag with the
    # page name and optional BETA badge as flex siblings. The flex layout
    # gives us perfect, automatic vertical alignment with no positioning
    # gymnastics \u2014 the entire link is one HTML block under our control.
    # Streamlit's MPA router intercepts clicks on anchors whose href matches
    # a registered page URL, so navigation behaves the same as st.page_link.
    def _build_row_html(page_obj, *, is_first: bool) -> str:
        # Returns the HTML for one dept-dropdown row: pin link + title link
        # + optional BETA badge. Caller composes multiple of these into the
        # dept body. Divider drawn above all rows except the first.
        p_path = page_paths[page_obj]
        is_fav = p_path in user_favorites
        is_beta = page_beta.get(page_obj, False)
        url = _page_url(page_obj, page_paths)
        title_html = html.escape(page_obj.title)
        icon_raw = page_icons.get(page_obj) or ""
        icon_html = (
            f'<span class="cna-link-icon">{html.escape(icon_raw)}</span>'
            if icon_raw
            else ""
        )
        badge_html = (
            '<span class="cna-link-beta">BETA</span>' if is_beta else ""
        )
        default_svg = _PIN_FILLED_SVG if is_fav else _PIN_OUTLINED_SVG
        hover_svg = _PIN_OUTLINED_SVG if is_fav else _PIN_FILLED_SVG
        pin_tip = "Remove from favorites" if is_fav else "Add to favorites"
        pin_class = "cna-pin-link is-favorited" if is_fav else "cna-pin-link"
        fav_toggle_url = f"?fav_toggle={_url_quote(p_path, safe='')}"
        divider_html = "" if is_first else '<div class="cna-row-divider"></div>'
        return (
            f"{divider_html}"
            f'<div class="cna-droprow">'
            f'<a class="{pin_class}" href="{fav_toggle_url}" '
            f'title="{pin_tip}" target="_self">'
            f'<span class="cna-pin-default">{default_svg}</span>'
            f'<span class="cna-pin-hover">{hover_svg}</span>'
            f"</a>"
            f'<a class="cna-title-link" href="{url}" target="_self">'
            f"{icon_html}"
            f'<span class="cna-link-title">{title_html}</span>'
            f"{badge_html}"
            f"</a>"
            f"</div>"
        )

    def _build_dept_html(dept_name: str, dept_pages: list, is_open: bool) -> str:
        # Returns the HTML for one dept dropdown using NATIVE HTML
        # <details>/<summary>. The browser handles open/close on click
        # instantly — no Streamlit rerun, no URL navigation, no flicker.
        # `open` attribute sets the initial state on first render; the
        # user's subsequent toggles live in the browser and aren't
        # persisted server-side.
        rows_html = "".join(
            _build_row_html(p, is_first=(i == 0))
            for i, p in enumerate(dept_pages)
        )
        open_attr = " open" if is_open else ""
        dept_safe = html.escape(dept_name)
        return (
            f'<details class="cna-dept"{open_attr}>'
            f'<summary class="cna-dept-header">'
            f'<span class="cna-dept-name">{dept_safe}</span>'
            f'<span class="cna-dept-chevron">&#9656;</span>'
            f"</summary>"
            f'<div class="cna-dept-body">{rows_html}</div>'
            f"</details>"
        )

    def _render_dept_dropdowns_html(dept_rows) -> None:
        # Composes the entire dept-dropdown area for the active function as
        # ONE st.markdown call. Default open state: only the dept containing
        # the currently-viewed page is open; the rest start collapsed.
        active_dept = next(
            (
                dn for dn, pages in dept_rows
                if dn and any(p.title == navigation.title for p in pages)
            ),
            None,
        )
        parts = []
        for dept_name, dept_pages in dept_rows:
            if not dept_name:
                continue
            parts.append(
                _build_dept_html(dept_name, dept_pages, dept_name == active_dept)
            )
        if parts:
            st.markdown("".join(parts), unsafe_allow_html=True)

    # --- Function pills + Department expanders ---
    # Pills at the top of the sidebar switch which Function is being browsed.
    # Below the pills, only the active Function's Departments render (as flat
    # expanders). Admin has no Department tier, so its pages render directly
    # under the pills when Admin is active.
    function_names = [fn for fn, _ in sidebar_sections]

    if function_names:
        # Detect which Function the current page belongs to. Used as the
        # active pill on initial load and re-synced whenever the user
        # navigates to a page in a different Function.
        active_from_page = next(
            (
                fn for fn, depts in sidebar_sections
                if any(
                    p.title == navigation.title
                    for _, pages_in_dept in depts
                    for p in pages_in_dept
                )
            ),
            function_names[0],
        )

        pill_key = "_sidebar_active_function"
        nav_tracking_key = "_sidebar_last_nav_title"
        # On navigation, follow the new page's Function so the pill stays in
        # sync with the visible page. Pill clicks (without navigation) leave
        # this alone so the user can browse other Functions without losing
        # their place.
        if st.session_state.get(nav_tracking_key) != navigation.title:
            st.session_state[nav_tracking_key] = navigation.title
            st.session_state[pill_key] = active_from_page
        elif pill_key not in st.session_state:
            st.session_state[pill_key] = active_from_page

        st.pills(
            "Section",
            function_names,
            selection_mode="single",
            label_visibility="collapsed",
            key=pill_key,
        )
        active_function = st.session_state.get(pill_key) or active_from_page

        active_dept_rows = next(
            (depts for fn, depts in sidebar_sections if fn == active_function),
            [],
        )
        has_dropdowns = any(dn for dn, _ in active_dept_rows)
        if has_dropdowns:
            # Tools/Reports: render dept dropdowns + their rows as ONE HTML
            # block. Zero Streamlit primitives in this area — full visual
            # control. Dept toggle state and pin toggles flow through URL
            # params handled at the top of this file.
            _render_dept_dropdowns_html(active_dept_rows)
        else:
            # Admin: no dropdowns. Bare st.page_link rows match the
            # favorites stack (tight default spacing, native active-page
            # highlight, no per-row column wrapper).
            for dept_name, dept_pages in active_dept_rows:
                for page_obj in dept_pages:
                    st.page_link(
                        page_obj,
                        icon=page_icons[page_obj],
                        use_container_width=True,
                    )

    # Settings dropdown — HTML <details> styled the same way as the dept
    # dropdowns. Each action is an <a> link to ?settings_action=NAME,
    # handled at the top of this file.
    #
    # Developer-only "View as" rows let an actual developer preview the app as a
    # regular user / admin / developer. Gated on the REAL developer flag (not the
    # effective role) so a developer previewing as "user" can still switch back.
    view_as_html = ""
    if utils.is_actual_developer():
        current_view = utils.effective_role()
        impersonated_login = utils.view_as_user_login()
        if impersonated_login:
            impersonated_name = utils.load_user_fullname_map().get(impersonated_login, impersonated_login)
            user_row_label = f"View as User: {impersonated_name} &#10003;"
        else:
            user_row_label = "View as User…"
        admin_active = (not impersonated_login) and current_view == "admin"
        dev_active = (not impersonated_login) and current_view == "developer"
        view_as_rows = (
            f'<a class="cna-action-row" href="?settings_action=view_as_user_picker" target="_self">'
            f'{user_row_label}</a>'
            '<div class="cna-row-divider"></div>'
            f'<a class="cna-action-row" href="?settings_action=view_as_admin" target="_self">'
            f'View as Admin{" &#10003;" if admin_active else ""}</a>'
            '<div class="cna-row-divider"></div>'
            f'<a class="cna-action-row" href="?settings_action=view_as_developer" target="_self">'
            f'View as Developer{" &#10003;" if dev_active else ""}</a>'
        )
        view_as_html = (
            '<div class="cna-row-divider"></div>'
            '<div class="cna-action-row" style="opacity:0.55;font-size:0.7rem;'
            'text-transform:uppercase;letter-spacing:0.04em;pointer-events:none;">View As</div>'
            '<div class="cna-row-divider"></div>'
            + view_as_rows
        )

    st.markdown(
        '<details class="cna-dept cna-settings-dept">'
        '<summary class="cna-dept-header">'
        '<span class="cna-dept-name">Settings</span>'
        '<span class="cna-dept-chevron">&#9656;</span>'
        '</summary>'
        '<div class="cna-dept-body">'
        '<a class="cna-action-row" href="?settings_action=check_updates" target="_self">'
        'Check for Updates</a>'
        '<div class="cna-row-divider"></div>'
        '<a class="cna-action-row" href="?settings_action=refresh" target="_self">'
        'Refresh</a>'
        '<div class="cna-row-divider"></div>'
        '<a class="cna-action-row" href="?settings_action=clear_cache" target="_self">'
        'Clear Cache</a>'
        '<div class="cna-row-divider"></div>'
        '<a class="cna-action-row" href="?settings_action=repair" target="_self">'
        'Repair App</a>'
        + view_as_html +
        '</div>'
        '</details>',
        unsafe_allow_html=True,
    )
    # Surface any queued message from a settings action via st.toast.
    if "_settings_toast" in st.session_state:
        st.toast(st.session_state.pop("_settings_toast"))

    # --- Network-drive connection indicator (pinned bottom-left) ---
    # Rendered last and wrapped in a keyed container so the CSS hook
    # (.st-key-cna_conn_status_wrap, margin-top:auto) pins it to the
    # bottom-left corner of the sidebar. Re-checks reachability on each page
    # load/reload and on navigation to a different page (keyed by the active
    # page title); reused across same-page reruns so widget interactions don't
    # repeat the probe.
    with st.container(key="cna_conn_status_wrap"):
        utils.render_sidebar_connection_status(page_key=navigation.title)

if st.session_state.get("_show_repair_dialog"):
    @st.dialog("Repair App", width="small")
    def _repair_dialog():
        st.markdown(
            "Resets your installation to the latest version, rebuilds the "
            "launcher, and restarts the app. Usually takes 1-2 minutes."
        )
        st.caption(
            "**Developers:** any uncommitted local changes will be discarded."
        )
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button(
                "Repair",
                type="primary",
                use_container_width=True,
                key="_repair_confirm",
            ):
                _launch_repair()
        with col_no:
            if st.button(
                "Cancel",
                use_container_width=True,
                key="_repair_cancel",
            ):
                st.session_state["_show_repair_dialog"] = False
                st.rerun()
    _repair_dialog()

if st.session_state.get("_show_view_as_user_dialog"):
    @st.dialog("View as User", width="small")
    def _view_as_user_dialog():
        st.caption("Preview the site with another user's permissions (developer only).")
        users = utils.list_user_logins()  # [(login, full_name), ...]
        if not users:
            st.info("No users found in users.parquet.")
            if st.button("Close", use_container_width=True, key="_vau_close_empty"):
                st.session_state["_show_view_as_user_dialog"] = False
                st.rerun()
            return

        logins = [login for login, _ in users]
        names = {login: name for login, name in users}
        current = utils.view_as_user_login()
        index = logins.index(current) if current in logins else 0
        selected = st.selectbox(
            "Select a user",
            options=logins,
            index=index,
            format_func=lambda login: f"{names.get(login, login)} ({login})",
        )
        if utils.is_user_developer(selected):
            sel_role = "developer"
        elif utils.is_user_admin(selected):
            sel_role = "admin"
        else:
            sel_role = "user"
        st.caption(f"This user's permissions: **{sel_role}**")

        col_apply, col_cancel = st.columns(2)
        with col_apply:
            if st.button("Apply", type="primary", use_container_width=True, key="_vau_apply"):
                utils.set_view_as_user(selected)
                st.session_state["_show_view_as_user_dialog"] = False
                st.session_state["_settings_toast"] = f"Viewing as {names.get(selected, selected)}"
                st.rerun()
        with col_cancel:
            if st.button("Cancel", use_container_width=True, key="_vau_cancel"):
                st.session_state["_show_view_as_user_dialog"] = False
                st.rerun()
        if utils.view_as_user_login():
            if st.button("Reset to my own role", use_container_width=True, key="_vau_reset"):
                utils.set_view_as_role("developer")
                st.session_state["_show_view_as_user_dialog"] = False
                st.session_state["_settings_toast"] = "Back to your own role"
                st.rerun()
    _view_as_user_dialog()

LOGGER.info("Navigation initialized | current_page='%s'", navigation.title)
# One-time confirmation after a mid-session "Update now" (set just before its rerun).
_post_update_notice = st.session_state.pop("_post_update_notice", None)
if _post_update_notice:
    st.toast(_post_update_notice, icon=":material/check_circle:")
# Non-blocking banner if a new version landed while the app was open (appears on
# the user's next interaction; never forces an update or refreshes their work).
_render_soft_update_banner()
navigation.run()
