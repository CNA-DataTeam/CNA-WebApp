"""
Standalone .exe compiled by PyInstaller.

Runs StartApp.bat --no-launch (config decrypt, update check, startup.py),
then imports and runs launch_app.py directly in THIS process so the .exe's
embedded icon is what Windows shows in the taskbar.
"""

import ctypes
import os
import subprocess
import sys
from pathlib import Path


def main():
    # When running as a PyInstaller exe, sys.executable is the exe itself.
    if getattr(sys, "frozen", False):
        root = Path(sys.executable).resolve().parent
    else:
        root = Path(__file__).resolve().parent.parent

    code_dir = root / "CODE - do not open"
    bat = code_dir / "StartApp.bat"

    if not bat.exists():
        ctypes.windll.user32.MessageBoxW(
            None,
            f"Could not find StartApp.bat at:\n{bat}\n\nPlease reinstall the app.",
            "CNA Web App - Error",
            0x10,
        )
        sys.exit(1)

    # Skip running StartApp.bat here — launch_app.py will run it in the
    # background while the splash screen is already visible, so the user
    # isn't staring at nothing for 30+ seconds.

    # ── Launch the pywebview app in THIS process ──
    # Add the venv's site-packages so we can import webview, streamlit, etc.
    venv_site = root / ".venv" / "Lib" / "site-packages"
    sys.path.insert(0, str(venv_site))
    sys.path.insert(0, str(code_dir))
    sys.path.insert(0, str(root))

    # Help Windows find DLLs from venv packages (e.g. webview2 loader)
    venv_scripts = root / ".venv" / "Scripts"
    if hasattr(os, "add_dll_directory"):
        for dll_dir in [str(venv_site), str(venv_scripts)]:
            try:
                os.add_dll_directory(dll_dir)
            except OSError:
                pass

    os.environ["PYTHONPATH"] = f"{root};{code_dir}"
    os.chdir(str(root))

    try:
        import launch_app
        launch_app.main()
    except Exception as exc:
        ctypes.windll.user32.MessageBoxW(
            None,
            f"Failed to launch the app:\n\n{exc}\n\nPlease try running StartApp.bat directly.",
            "CNA Web App - Error",
            0x10,
        )
        sys.exit(1)
    finally:
        # Release the app folder so Windows allows deletion.
        # Without this, the .exe process holds the CWD handle until exit.
        try:
            os.chdir(os.environ.get("TEMP", "C:\\"))
        except OSError:
            pass


if __name__ == "__main__":
    main()
