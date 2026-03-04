"""
startup.py

Purpose:
    Daily startup/cache preparation for the Logistics Support app.
Hello Jordan
What it does:
    - Locates the locally-synced Task-Tracker folder (SharePoint/OneDrive).
    - Writes/refreshes derived datasets into cached parquet files on the network:
        * accounts_<YYYY-MM-DD>.parquet (daily)
        * users.parquet (latest)
    - Avoids rework if today's accounts parquet already exists.
    - Does NOT regenerate tasks.parquet. Task definitions are managed only
      through the Tasks Management page.

Utils used:
    - None (startup runs as a standalone prep step; avoids importing Streamlit).

Inputs:
    - config.POTENTIAL_ROOTS / config.DOCUMENT_LIBRARIES / config.RELATIVE_APP_PATH
      to discover the local Task-Tracker root
    - Excel sources:
        * TasksAndTargets.xlsx (Users sheet)
        * CNA Personnel - Temporary.xlsx (CNA Personnel sheet)

Outputs:
    - Parquet files written to config.PERSONNEL_DIR
    - Logs written to config.LOG_FILE
"""

import pandas as pd
from datetime import date, datetime
from pathlib import Path
import getpass
import os
import platform
import subprocess
import sys

CODE_DIR = Path(__file__).resolve().parent
ROOT_DIR = CODE_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

import app_logging
import config

LOGGER = app_logging.get_logger(__file__, "Startup Process")

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
    LOGGER.info(
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
        tracker_xlsx: Path to the Task-Tracker Excel file (TasksAndTargets.xlsx)
            used for Users sheet refresh.
        accounts_xlsx: Path to the accounts Excel file (CNA Personnel - Temporary.xlsx).
    """
    task_tracker_root = find_task_tracker_root()
    output_dir = config.PERSONNEL_DIR  # UNC path for cached data
    tracker_xlsx = task_tracker_root / config.TASKS_XLSX_NAME
    accounts_xlsx = task_tracker_root.parents[2] / "Data and Analytics" / "Resources" / config.ACCOUNTS_XLSX_NAME
    return output_dir, tracker_xlsx, accounts_xlsx

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
            LOGGER.info("Deleted old file: %s", file.name)
        except Exception as e:
            LOGGER.error("Error deleting %s: %s", file.name, e)

def load_accounts_excel(path: Path) -> pd.DataFrame:
    """Load accounts data from Excel and return relevant columns."""
    df = pd.read_excel(path, sheet_name="CNA Personnel", engine="openpyxl")
    result = df[["Company Group USE", "CustomerCode"]].copy()
    result["Company Group USE"] = result["Company Group USE"].astype(str).str.strip()
    result["CustomerCode"] = result["CustomerCode"].astype(str).str.strip()
    # Remove rows where both columns are NaN (after conversion to string)
    result = result[(result["Company Group USE"] != "nan") | (result["CustomerCode"] != "nan")]
    return result

def load_users_excel(path: Path) -> pd.DataFrame:
    """Load users metadata from the Tasks Excel Users sheet."""
    df = pd.read_excel(path, sheet_name="Users", engine="openpyxl")
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    def _normalize_col(value: str) -> str:
        return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())

    normalized_cols = {_normalize_col(c): c for c in df.columns}

    def _find_col(candidates: list[str]) -> str | None:
        for candidate in candidates:
            match = normalized_cols.get(_normalize_col(candidate))
            if match:
                return match
        return None

    def _clean_text(value: object) -> str | None:
        if pd.isna(value):
            return None
        text = str(value).strip()
        return text if text else None

    def _clean_id_text(value: object) -> str | None:
        if pd.isna(value):
            return None
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if pd.isna(value):
                return None
            if float(value).is_integer():
                return str(int(value))
        text = str(value).strip()
        return text if text else None

    def _to_flag(value: object) -> int:
        if pd.isna(value):
            return 0
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return 1 if float(value) != 0 else 0
        text = str(value).strip().lower()
        if text in {"1", "true", "t", "yes", "y", "on"}:
            return 1
        if text in {"0", "false", "f", "no", "n", "off", ""}:
            return 0
        return 0

    column_map: list[tuple[str, list[str]]] = [
        ("EmployeeNumber", ["employee number", "employee id"]),
        ("StartDate", ["start date", "hire date"]),
        ("First Name", ["first name", "firstname"]),
        ("Last Name", ["last name", "lastname"]),
        ("Full Name", ["full name", "fullname", "name"]),
        ("Email", ["email", "email address"]),
        ("Role", ["role", "title"]),
        ("IsManager", ["ismanager", "is manager", "manager flag"]),
        ("Department", ["department", "dept"]),
        ("Manager", ["manager", "manager name"]),
        ("ManagerEmployeeNumber", ["manageremployeenumber", "manager employee number", "manager id"]),
        ("isAdmin", ["isadmin", "is admin", "admin", "isadministrator", "is administrator"]),
        ("User", ["user", "user login", "username", "login", "samaccountname"]),
    ]

    out = pd.DataFrame(index=df.index)
    for target_col, aliases in column_map:
        source_col = _find_col([target_col] + aliases)
        if source_col:
            out[target_col] = df[source_col]
        else:
            out[target_col] = pd.NA

    out["EmployeeNumber"] = out["EmployeeNumber"].map(_clean_id_text)
    out["ManagerEmployeeNumber"] = out["ManagerEmployeeNumber"].map(_clean_id_text)
    for col in ["First Name", "Last Name", "Full Name", "Email", "Role", "Department", "Manager", "User"]:
        out[col] = out[col].map(_clean_text)

    out["StartDate"] = pd.to_datetime(out["StartDate"], errors="coerce")
    out["IsManager"] = out["IsManager"].map(_to_flag).astype(int)
    out["isAdmin"] = out["isAdmin"].map(_to_flag).astype(int)

    # Fill missing full-name values from First/Last when possible.
    first = out["First Name"].fillna("")
    last = out["Last Name"].fillna("")
    fallback_name = (first + " " + last).str.strip().replace("", pd.NA)
    missing_full_name = out["Full Name"].isna() | out["Full Name"].astype(str).str.strip().eq("")
    out.loc[missing_full_name, "Full Name"] = fallback_name[missing_full_name]

    out = out[out["User"].notna()].copy()
    return out

def save_parquet(df: pd.DataFrame, output_dir: Path, filename: str) -> Path:
    """Save DataFrame to Parquet file with given filename in output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    df.to_parquet(output_path, index=False)
    return output_path

def main() -> None:
    """Main startup routine."""
    LOGGER.info("Session started.")
    log_run_context()
    LOGGER.info("Running startup check: %s", date.today().isoformat())
    try:
        output_dir, tracker_xlsx, accounts_xlsx = get_paths()
    except Exception as e:
        LOGGER.error("Initialization failed: %s", e)
        return
    LOGGER.info("Output directory: %s", output_dir)
    LOGGER.info("Users source Excel: %s", tracker_xlsx)
    LOGGER.info("Accounts Excel: %s", accounts_xlsx)
    # Handle accounts data
    if todays_file_exists(output_dir, "accounts"):
        LOGGER.info("Today's accounts file already exists: %s", get_todays_filename("accounts"))
    else:
        LOGGER.info("Preparing accounts data...")
        delete_old_parquet_files(output_dir, "accounts")
        try:
            accounts_df = load_accounts_excel(accounts_xlsx)
        except Exception as e:
            LOGGER.error("Failed to load accounts Excel: %s", e)
            return
        output_path = save_parquet(accounts_df, output_dir, get_todays_filename("accounts"))
        LOGGER.info("Saved accounts data: %s", output_path)
    # Handle users data (login to full name mapping)
    try:
        users_df = load_users_excel(tracker_xlsx)
        if not users_df.empty:
            save_parquet(users_df, output_dir, "users.parquet")
            LOGGER.info("Saved users data: users.parquet")
    except Exception as e:
        LOGGER.error("Failed to load users Excel: %s", e)
    LOGGER.info("Startup check complete.")

if __name__ == "__main__":
    main()
