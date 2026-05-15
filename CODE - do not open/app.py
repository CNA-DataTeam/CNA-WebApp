from pathlib import Path
import base64
import json
import os
import subprocess
import sys

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
UPDATE_CHECK_FILE = APP_DIR / ".last_update_check"

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

    _rebuild_launcher_if_missing()

    try:
        import shutil
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
                    "If this persists, share the error above."
                )

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


# Star icon images (40x40 PNGs rendered at 20x20 for crispness)
_ASSETS_DIR = APP_DIR / "assets"
_STAR_FILLED_B64 = base64.b64encode((_ASSETS_DIR / "star_filled.png").read_bytes()).decode()
_STAR_HOLLOW_B64 = base64.b64encode((_ASSETS_DIR / "star_hollow.png").read_bytes()).decode()
_STAR_IMG = '<img src="data:image/png;base64,{b64}" width="20" height="20" style="vertical-align:middle">'
STAR_FILLED_HTML = _STAR_IMG.format(b64=_STAR_FILLED_B64)
STAR_HOLLOW_HTML = _STAR_IMG.format(b64=_STAR_HOLLOW_B64)


# -----------------------------------------------------------------
# Build navigation pages
# -----------------------------------------------------------------
home_entry = page_registry.get_home_page()
visible_sections = page_registry.get_visible_sections(is_admin_user)
home_page = st.Page(home_entry.path, title=home_entry.title)
pages = {"": [home_page]}
# Track icons and paths separately — passing icon to st.Page() overrides the favicon
page_icons: dict[st.Page, str] = {home_page: home_entry.icon}
page_paths: dict[st.Page, str] = {home_page: home_entry.path}
path_to_page: dict[str, st.Page] = {home_entry.path: home_page}
sidebar_sections: list[tuple[str, list[st.Page]]] = []
for section_name, entries in visible_sections:
    section_pages = []
    for entry in entries:
        page = st.Page(entry.path, title=entry.title)
        page_icons[page] = entry.icon
        page_paths[page] = entry.path
        path_to_page[entry.path] = page
        section_pages.append(page)
    pages[section_name] = section_pages
    sidebar_sections.append((section_name, section_pages))

# Hidden pages for URL routing only (not shown in sidebar)
pages["_routing"] = [
    st.Page("pages/da-task-tracker.py", title="Task Tracker"),
]

navigation = st.navigation(pages, position="hidden")
user_favorites = _load_favorites()

with st.sidebar:
    # --- Top row: Home + favorited pages ---
    st.page_link(home_page, icon=page_icons[home_page], use_container_width=True)
    for fav_path in user_favorites:
        fav_page = path_to_page.get(fav_path)
        if fav_page:
            st.page_link(fav_page, icon=page_icons[fav_page], use_container_width=True)

    # --- Section dropdowns with star toggles ---
    for section_name, section_pages in sidebar_sections:
        section_active = any(p.title == navigation.title for p in section_pages)
        with st.expander(section_name, expanded=section_active):
            for page_obj in section_pages:
                p_path = page_paths[page_obj]
                is_fav = p_path in user_favorites
                link_col, star_col = st.columns([0.88, 0.12])
                with link_col:
                    st.page_link(page_obj, icon=page_icons[page_obj], use_container_width=True)
                with star_col:
                    default_b64 = _STAR_FILLED_B64 if is_fav else _STAR_HOLLOW_B64
                    hover_b64 = _STAR_HOLLOW_B64 if is_fav else _STAR_FILLED_B64
                    star_tip = "Remove from favorites" if is_fav else "Add to favorites"
                    st.markdown(
                        f'<span class="star-toggle" title="{star_tip}" '
                        f'style="display:block; margin-top:2px; cursor:pointer">'
                        f'<img class="star-default" src="data:image/png;base64,{default_b64}" width="20" height="20">'
                        f'<img class="star-hover" src="data:image/png;base64,{hover_b64}" width="20" height="20">'
                        f'</span>',
                        unsafe_allow_html=True,
                    )
                    if st.button(
                        "\u200b",
                        key=f"fav_{p_path}",
                        type="tertiary",
                    ):
                        _toggle_favorite(p_path)
                        st.rerun()

    st.divider()
    with st.popover("Settings", use_container_width=True):
        if st.button("Check for Updates", use_container_width=True, type="tertiary"):
            with st.spinner("Checking..."):
                has_update = _check_for_updates_manual()
            if has_update:
                st.rerun()
            else:
                st.success("You're up to date!")
        if st.button("Refresh", use_container_width=True, type="tertiary"):
            st.rerun()
        if st.button("Clear Cache", use_container_width=True, type="tertiary"):
            st.cache_data.clear()
            st.rerun()
        if st.button("Repair App", use_container_width=True, type="tertiary"):
            st.session_state["_show_repair_dialog"] = True

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

LOGGER.info("Navigation initialized | current_page='%s'", navigation.title)
navigation.run()
