"""
refresh_data.py

Centralized data producer for the CNA Console.

Regenerates the cached accounts + users parquet files on the network share
(``config.PERSONNEL_DIR``) from the SharePoint-synced source Excel files, using the
SAME engine the app runs at startup (``startup.regenerate_accounts_parquet`` /
``startup.load_users_excel``). It forces a fresh rebuild every run so source edits
are picked up promptly (``force=True``), unlike the per-launch startup step which
skips when today's file already exists.

Why this exists:
    End users may not have the CNA SharePoint synced locally, so they cannot read
    the source Excel. They only ever READ the parquet from the share. This script is
    meant to run on ONE reliably-synced machine via a Windows Scheduled Task (twice
    daily on weekdays) so the shared parquet stays fresh for everyone. The in-app
    "Reload latest" button then just re-reads that parquet.

Important:
    OneDrive/SharePoint sync only mounts in an interactive, logged-on Windows
    session, so the scheduled task must run while the user is logged on (locked is
    fine). If the source is unreachable this script logs and exits 0 — consumers keep
    using the last good parquet, so nothing breaks. It never raises to the caller.

    It does NOT pull git updates and does NOT touch tasks.parquet (admin-managed).

Run manually:
    .venv\\Scripts\\python.exe "CODE - do not open\\refresh_data.py"
"""

import os
import sys
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
ROOT_DIR = CODE_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

# Best-effort: make sure config.py exists by decrypting config.enc. Fail-open — if
# the key is unavailable we proceed with whatever config.py is already on disk,
# exactly like StartApp.bat does. decrypt() calls sys.exit on failure, so guard it.
try:
    import config_manager

    config_manager.decrypt()
except SystemExit:
    pass
except Exception:
    pass

try:
    import startup
except Exception as exc:
    # Honor the "never raises / fail silent" contract even this early: if config.py is
    # absent AND the decrypt above failed, ``import startup`` (which imports config)
    # would die here with no app logger available and pythonw swallowing output. Log to
    # a plain temp file and exit 0 so the scheduled task records a clean no-op.
    try:
        import tempfile
        from datetime import datetime

        _bootstrap_log = Path(tempfile.gettempdir()) / "cna_refresh_data_bootstrap.log"
        with open(_bootstrap_log, "a", encoding="utf-8") as _fh:
            _fh.write(f"{datetime.now().isoformat()} bootstrap import failed: {exc}\n")
    except Exception:
        pass
    sys.exit(0)


def main() -> int:
    os.environ.setdefault("STARTUP_CALLER", "refresh_data.py (scheduled)")
    startup.LOGGER.info("Data refresh (scheduled producer) starting.")
    try:
        startup.log_run_context()
    except Exception:
        pass

    try:
        output_dir, tracker_xlsx, accounts_xlsx = startup.get_paths()
    except Exception as exc:
        # Source not reachable on this machine (e.g. SharePoint not synced). Log and
        # exit cleanly; the shared parquet is left untouched.
        startup.LOGGER.error("Data refresh: could not resolve source paths: %s", exc)
        return 0

    # Accounts — force a fresh rebuild so source edits are picked up every run.
    try:
        startup.regenerate_accounts_parquet(
            force=True, output_dir=output_dir, accounts_xlsx=accounts_xlsx
        )
        startup.LOGGER.info("Data refresh: accounts parquet rebuilt.")
    except Exception as exc:
        startup.LOGGER.error("Data refresh: failed to rebuild accounts parquet: %s", exc)

    # Users — mirror the startup behavior (rewrite users.parquet from the Users sheet).
    try:
        users_df = startup.load_users_excel(tracker_xlsx)
        if not users_df.empty:
            startup.save_parquet(users_df, output_dir, "users.parquet")
            startup.LOGGER.info("Data refresh: users parquet rebuilt.")
        else:
            startup.LOGGER.warning(
                "Data refresh: users Excel produced no rows; users.parquet left as-is."
            )
    except Exception as exc:
        startup.LOGGER.error("Data refresh: failed to rebuild users parquet: %s", exc)

    startup.LOGGER.info("Data refresh complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
