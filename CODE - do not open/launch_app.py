"""
PyWebView launcher for CNA Web App.

Opens Streamlit in a native OS window with a white title bar, built-in
loading screen, and custom icon. Kills the Streamlit server when the
window is closed.

Startup flow:
  1. Show splash screen immediately (so the user isn't staring at nothing)
  2. Run StartApp.bat --no-launch (config decrypt, update check, startup.py)
     while the splash is visible
  3. Start Streamlit server
  4. Redirect to the app once ready
"""

import ctypes
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

# Set AppUserModelID BEFORE importing webview or creating any windows.
# This gives the app its own taskbar identity so the icon isn't inherited
# from pythonw.exe.
ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("CNA.WebApp.1")

import webview

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
VENV_DIR = ROOT_DIR / ".venv"
APP_FILE = APP_DIR / "app.py"
ICON_FILE = ROOT_DIR / "cna_icon.ico"
STREAMLIT_PORT = 8501
APP_URL = f"http://localhost:{STREAMLIT_PORT}"

# Module-level handle so cleanup can find it after webview.start() returns
_server_proc: subprocess.Popen | None = None

LOADING_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CNA Web App</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@600;700&family=Work+Sans:wght@400;500&display=swap" rel="stylesheet">
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      background: #f5f5f5;
      font-family: 'Work Sans', sans-serif;
      color: #333;
      overflow: hidden;
    }
    .container {
      text-align: center;
      animation: fadeIn 0.6s ease-out;
    }
    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(12px); }
      to   { opacity: 1; transform: translateY(0); }
    }
    .app-title {
      font-family: 'Poppins', sans-serif;
      font-size: 2.4rem;
      font-weight: 700;
      color: #1a1a2e;
      margin-bottom: 8px;
    }
    .app-subtitle {
      font-size: 0.95rem;
      color: #888;
      margin-bottom: 48px;
    }
    .spinner {
      width: 44px;
      height: 44px;
      margin: 0 auto 28px;
      border: 3.5px solid #e0e0e0;
      border-top-color: #16a085;
      border-radius: 50%;
      animation: spin 0.85s linear infinite;
    }
    @keyframes spin {
      to { transform: rotate(360deg); }
    }
    .status {
      font-size: 0.95rem;
      color: #666;
      min-height: 1.4em;
      transition: opacity 0.3s ease;
    }
    .status.fade { opacity: 0; }
  </style>
</head>
<body>
  <div class="container">
    <div class="app-title">CNA Web App</div>
    <div class="app-subtitle">Clark National Accounts</div>
    <div class="spinner"></div>
    <div class="status" id="status">Starting up...</div>
  </div>
  <script>
    const statusEl = document.getElementById('status');
    const messages = [
      'Starting up...',
      'Checking for updates...',
      'Loading dependencies...',
      'Preparing workspace...',
      'Almost ready...',
    ];
    let msgIndex = 0;
    setInterval(() => {
      statusEl.classList.add('fade');
      setTimeout(() => {
        msgIndex = (msgIndex + 1) % messages.length;
        statusEl.textContent = messages[msgIndex];
        statusEl.classList.remove('fade');
      }, 300);
    }, 3000);
  </script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_port_in_use(port: int) -> bool:
    """Check if a port is already listening."""
    try:
        with socket.create_connection(("localhost", port), timeout=1):
            return True
    except OSError:
        return False


def wait_for_server(port: int, timeout: int = 120) -> bool:
    """Block until Streamlit is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        if is_port_in_use(port):
            return True
        time.sleep(0.5)
    return False


def run_setup():
    """Run StartApp.bat --no-launch (config decrypt, update check, startup.py)."""
    bat = APP_DIR / "StartApp.bat"
    if not bat.exists():
        return
    subprocess.run(
        ["cmd.exe", "/c", str(bat), "--no-launch"],
        cwd=str(ROOT_DIR),
        creationflags=subprocess.CREATE_NO_WINDOW,
    )


def start_streamlit() -> subprocess.Popen | None:
    """Start the Streamlit server in the background. Returns the process."""
    pythonw = VENV_DIR / "Scripts" / "pythonw.exe"
    if not pythonw.exists():
        pythonw = VENV_DIR / "Scripts" / "python.exe"

    env = {**os.environ, "PYTHONPATH": f"{ROOT_DIR};{APP_DIR}"}

    return subprocess.Popen(
        [
            str(pythonw), "-m", "streamlit", "run", str(APP_FILE),
            f"--server.port={STREAMLIT_PORT}",
            "--server.headless=true",
            "--browser.gatherUsageStats=false",
        ],
        cwd=str(ROOT_DIR),
        env=env,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )


def set_window_icon_and_style():
    """Set the window icon and title bar color via Win32 / DWM APIs."""
    try:
        user32 = ctypes.windll.user32

        hwnd = None
        for _ in range(10):
            hwnd = user32.FindWindowW(None, "CNA Web App")
            if hwnd:
                break
            time.sleep(0.3)

        if not hwnd:
            return

        # Remove title text and icon from the title bar
        user32.SetWindowTextW(hwnd, "")
        GWL_EXSTYLE = -20
        WS_EX_DLGMODALFRAME = 0x00000001
        ex_style = user32.GetWindowLongW(hwnd, GWL_EXSTYLE)
        user32.SetWindowLongW(hwnd, GWL_EXSTYLE, ex_style | WS_EX_DLGMODALFRAME)
        user32.SetWindowPos(
            hwnd, 0, 0, 0, 0, 0,
            0x0020 | 0x0002 | 0x0001 | 0x0004,  # FRAMECHANGED|NOMOVE|NOSIZE|NOZORDER
        )

        # Set title bar color via DWM (#eeeeee → BGR 0x00EEEEEE)
        DWMWA_CAPTION_COLOR = 35
        color = ctypes.c_int(0x00EEEEEE)  # COLORREF in BGR
        ctypes.windll.dwmapi.DwmSetWindowAttribute(
            hwnd, DWMWA_CAPTION_COLOR,
            ctypes.byref(color), ctypes.sizeof(color),
        )

        # Set icon
        if not ICON_FILE.exists():
            return

        WM_SETICON = 0x0080
        ICON_SMALL = 0
        ICON_BIG = 1
        IMAGE_ICON = 1
        LR_LOADFROMFILE = 0x0010

        icon_path_str = str(ICON_FILE)

        hicon_big = user32.LoadImageW(
            None, icon_path_str, IMAGE_ICON, 48, 48, LR_LOADFROMFILE,
        )
        hicon_small = user32.LoadImageW(
            None, icon_path_str, IMAGE_ICON, 16, 16, LR_LOADFROMFILE,
        )

        if hicon_big:
            user32.SendMessageW(hwnd, WM_SETICON, ICON_BIG, hicon_big)
        if hicon_small:
            user32.SendMessageW(hwnd, WM_SETICON, ICON_SMALL, hicon_small)
    except Exception:
        pass


def setup_and_redirect(window: webview.Window):
    """Run setup steps while splash is visible, start Streamlit, then redirect."""
    global _server_proc
    set_window_icon_and_style()

    # Run config decrypt, update check, startup.py while splash is showing
    run_setup()

    # Start the Streamlit server
    _server_proc = start_streamlit()

    # Wait for it to be ready, then navigate
    if wait_for_server(STREAMLIT_PORT):
        time.sleep(2)
        window.load_url(APP_URL)
    else:
        window.evaluate_js(
            "document.getElementById('status').textContent = "
            "'Error: Server failed to start. Please restart the app.';"
        )


def on_shown(window: webview.Window):
    """Called when window is shown — set icon and title bar color."""
    set_window_icon_and_style()


def main():
    global _server_proc

    # If the app is already running, just open a window to it
    already_running = is_port_in_use(STREAMLIT_PORT)

    # Determine the icon path
    icon_path = str(ICON_FILE) if ICON_FILE.exists() else None

    if already_running:
        # Server is already up — open directly
        window = webview.create_window(
            "CNA Web App",
            APP_URL,
            width=1280,
            height=900,
            min_size=(800, 600),
        )
    else:
        # Show loading screen immediately, do setup + start server in background
        window = webview.create_window(
            "CNA Web App",
            html=LOADING_HTML,
            width=1280,
            height=900,
            min_size=(800, 600),
        )

    # Start the GUI — blocks until window is closed
    webview.start(
        setup_and_redirect if not already_running else on_shown,
        window,
        icon=icon_path,
    )

    # Window closed — stop the server process tree if we started it
    if _server_proc is not None:
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(_server_proc.pid)],
                creationflags=subprocess.CREATE_NO_WINDOW,
                timeout=10,
            )
        except Exception:
            try:
                _server_proc.kill()
            except Exception:
                pass


if __name__ == "__main__":
    main()
