"""
startup.py

Purpose:
    Daily startup/cache preparation for the Logistics Support app.

What it does:
    - Locates the locally-synced Task-Tracker folder (SharePoint/OneDrive).
    - Writes/refreshes derived datasets into cached parquet files on the network:
        * accounts_<YYYY-MM-DD>.parquet (daily)
        * tasks.parquet (latest)
        * users.parquet (latest)
    - Avoids rework if today's accounts parquet already exists.

Utils used:
    - None (startup runs as a standalone prep step; avoids importing Streamlit).

Inputs:
    - config.POTENTIAL_ROOTS / config.DOCUMENT_LIBRARIES / config.RELATIVE_APP_PATH
      to discover the local Task-Tracker root
    - Excel sources:
        * TasksAndTargets.xlsx (Tasks sheet, Users sheet)
        * CNA Personnel - Temporary.xlsx (CNA Personnel sheet)

Outputs:
    - Parquet files written to config.PERSONNEL_DIR
    - Logs written to config.LOG_FILE
"""

import logging
import pandas as pd
from datetime import date, datetime
from pathlib import Path
import getpass
import os
import platform
import subprocess
import sys
import config

def setup_logging() -> None:
    """Configure logging to file."""
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=str(config.LOG_FILE),
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def get_os_user() -> str:
    """Get current OS username."""
    return getpass.getuser()

def get_parent_command(parent_pid: int) -> str:
    """Best-effort lookup of the parent process command line."""
    if parent_pid <= 0:
        return ""
    try:
        if os.name == "nt":
            cmd = [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    f'$p = Get-CimInstance Win32_Process -Filter "ProcessId={parent_pid}"; '
                    'if ($p) { $p.CommandLine }'
                ),
            ]
        else:
            cmd = ["ps", "-o", "command=", "-p", str(parent_pid)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5, check=False)
        return result.stdout.strip()
    except Exception:
        return ""

def log_run_context() -> None:
    """Log who ran startup and what launched it."""
    parent_pid = os.getppid()
    parent_cmd = get_parent_command(parent_pid)
    caller_hint = os.environ.get("STARTUP_CALLER", "")
    logging.info(
        "Run context | started=%s | user=%s | host=%s | script=%s | cwd=%s | argv=%s | parent_pid=%s | parent_cmd=%s | startup_caller=%s",
        datetime.now().astimezone().isoformat(),
        get_os_user(),
        platform.node(),
        str(Path(__file__).resolve()),
        str(Path.cwd()),
        " ".join(sys.argv),
        parent_pid,
        parent_cmd,
        caller_hint,
    )

def find_task_tracker_root() -> Path:
    """Find the Task-Tracker root folder from synced SharePoint locations."""
    # Preferred direct roots (new local sync structure)
    for candidate in getattr(config, "TASK_TRACKER_ROOT_HINTS", []):
        if candidate.exists():
            return candidate

    # Legacy SharePoint/OneDrive discovery fallback
    for root in config.POTENTIAL_ROOTS:
        for lib in config.DOCUMENT_LIBRARIES:
            candidate = root / lib / config.RELATIVE_APP_PATH
            if candidate.exists():
                return candidate
    raise FileNotFoundError(
        "Task-Tracker folder not found. Make sure CNA SharePoint is synced locally."
    )

def get_paths() -> tuple[Path, Path, Path]:
    """
    Determine output directory and source Excel file paths.
    Returns:
        output_dir: Directory for output parquet files (UNC network path).
        tasks_xlsx: Path to the Task-Tracker Excel file (TasksAndTargets.xlsx).
        accounts_xlsx: Path to the accounts Excel file (CNA Personnel - Temporary.xlsx).
    """
    task_tracker_root = find_task_tracker_root()
    output_dir = config.PERSONNEL_DIR  # UNC path for cached data
    tasks_xlsx = task_tracker_root / config.TASKS_XLSX_NAME
    accounts_xlsx = task_tracker_root.parents[2] / "Data and Analytics" / "Resources" / config.ACCOUNTS_XLSX_NAME
    return output_dir, tasks_xlsx, accounts_xlsx

def get_todays_filename(prefix: str) -> str:
    """Generate a filename with today's date for the given prefix."""
    return f"{prefix}_{date.today().isoformat()}.parquet"

def todays_file_exists(output_dir: Path, prefix: str) -> bool:
    """Check if today's parquet file with prefix already exists in output_dir."""
    return (output_dir / get_todays_filename(prefix)).exists()

def delete_old_parquet_files(output_dir: Path, prefix: str) -> None:
    """Delete all parquet files matching prefix in the output directory."""
    for file in output_dir.glob(f"{prefix}_*.parquet"):
        try:
            file.unlink()
            logging.info(f"Deleted old file: {file.name}")
        except Exception as e:
            logging.error(f"Error deleting {file.name}: {e}")

def load_accounts_excel(path: Path) -> pd.DataFrame:
    """Load accounts data from Excel and return relevant columns."""
    df = pd.read_excel(path, sheet_name="CNA Personnel", engine="openpyxl")
    result = df[["Company Group USE", "CustomerCode"]].copy()
    result["Company Group USE"] = result["Company Group USE"].astype(str).str.strip()
    result["CustomerCode"] = result["CustomerCode"].astype(str).str.strip()
    # Remove rows where both columns are NaN (after conversion to string)
    result = result[(result["Company Group USE"] != "nan") | (result["CustomerCode"] != "nan")]
    return result

def load_tasks_excel(path: Path) -> pd.DataFrame:
    """Load active tasks from the Tasks Excel file (Tasks sheet)."""
    df = pd.read_excel(path, sheet_name="Tasks", engine="openpyxl")
    if df.empty:
        return pd.DataFrame()
    # Filter active tasks and clean text fields
    df_active = df[df["IsActive"].astype(int) == 1].copy()
    df_active["TaskName"] = df_active["TaskName"].astype(str).str.strip()
    df_active["TaskCadence"] = df_active["TaskCadence"].astype(str).str.strip().str.title()
    return df_active

def load_users_excel(path: Path) -> pd.DataFrame:
    """Load user login to full name mapping from the Tasks Excel file (Users sheet)."""
    df = pd.read_excel(path, sheet_name="Users", engine="openpyxl")
    if df.empty:
        return pd.DataFrame()
    cols = {str(c).strip().lower(): c for c in df.columns}
    user_col = cols.get("user")
    full_col = cols.get("full name") or cols.get("fullname")
    if not user_col or not full_col:
        return pd.DataFrame()
    df_users = df[[user_col, full_col]].copy().dropna(subset=[user_col])
    df_users.columns = ["User", "Full Name"]
    df_users["User"] = df_users["User"].astype(str).str.strip()
    df_users["Full Name"] = df_users["Full Name"].astype(str).str.strip()
    return df_users

def save_parquet(df: pd.DataFrame, output_dir: Path, filename: str) -> Path:
    """Save DataFrame to Parquet file with given filename in output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    df.to_parquet(output_path, index=False)
    return output_path

def main() -> None:
    """Main startup routine."""
    setup_logging()
    log_run_context()
    logging.info(f"Running startup check: {date.today().isoformat()}")
    try:
        output_dir, tasks_xlsx, accounts_xlsx = get_paths()
    except Exception as e:
        logging.error(f"Initialization failed: {e}")
        return
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Tasks Excel: {tasks_xlsx}")
    logging.info(f"Accounts Excel: {accounts_xlsx}")
    # Handle accounts data
    if todays_file_exists(output_dir, "accounts"):
        logging.info(f"Today's accounts file already exists: {get_todays_filename('accounts')}")
    else:
        logging.info("Preparing accounts data...")
        delete_old_parquet_files(output_dir, "accounts")
        try:
            accounts_df = load_accounts_excel(accounts_xlsx)
        except Exception as e:
            logging.error(f"Failed to load accounts Excel: {e}")
            return
        output_path = save_parquet(accounts_df, output_dir, get_todays_filename("accounts"))
        logging.info(f"Saved accounts data: {output_path}")
    # Handle tasks data (overwrite daily)
    try:
        tasks_df = load_tasks_excel(tasks_xlsx)
        if not tasks_df.empty:
            save_parquet(tasks_df, output_dir, "tasks.parquet")
            logging.info("Saved tasks data: tasks.parquet")
    except Exception as e:
        logging.error(f"Failed to load tasks Excel: {e}")
    # Handle users data (login to full name mapping)
    try:
        users_df = load_users_excel(tasks_xlsx)
        if not users_df.empty:
            save_parquet(users_df, output_dir, "users.parquet")
            logging.info("Saved users data: users.parquet")
    except Exception as e:
        logging.error(f"Failed to load users Excel: {e}")
    logging.info("Startup check complete.")

if __name__ == "__main__":
    main()
