"""
time_allocation_store.py

Headless data-access layer for Time Allocation entries.

This module owns the on-disk convention for time-allocation parquet files:

    {TIME_ALLOCATION_DIR}/year=YYYY/month=MM/user=<key>/time_allocation_YYYYMMDD.parquet

It is intentionally Streamlit-free so non-page programs (e.g. the scheduled
``notify_missing_time.py`` reminder job) can read the same data the Time
Allocation page reads, WITHOUT re-deriving the partition layout. The page
(``pages/time-allocation-tool.py``) delegates its private read helpers to the
functions here so there is a single source of truth for the storage convention.

Anything that changes the partition layout, filenames, or the base column set
must change it HERE — both the page and the reminder job follow this module.
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config

LOGGER = logging.getLogger("time_allocation_store")

# The 8 base columns every saved time-allocation file carries (custom cf_* fields
# may follow, but readers here only need the base set).
BASE_COLUMNS: tuple[str, ...] = (
    "Entry Date",
    "User",
    "Full Name",
    "Department",
    "Account",
    "Customer Code",
    "Time",
    "Channel",
)


# ---------------------------------------------------------------------------
# Identity / partition-path primitives
# ---------------------------------------------------------------------------
def normalize_login(value: object) -> str:
    """Normalize a login to a stable key (strip domain / UPN, lowercase)."""
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.replace("/", "\\")
    if "\\" in text:
        text = text.split("\\")[-1]
    if "@" in text:
        text = text.split("@")[0]
    return text.strip()


def user_partition(user_login: str, full_name: str = "") -> str:
    """Return a stable folder-safe user key for time-allocation storage."""
    login_key = normalize_login(user_login)
    if login_key:
        return config.sanitize_log_user(login_key)
    fallback = str(full_name or "").strip().lower().replace(" ", "_")
    return config.sanitize_log_user(fallback or "unknown_user")


def month_dir(base_dir: Path, entry_date: date) -> Path:
    """Return the year/month partition directory for an entry date."""
    base_dir = Path(base_dir)
    return base_dir / f"year={entry_date.year:04d}" / f"month={entry_date.month:02d}"


def daily_file(base_dir: Path, user_login: str, full_name: str, entry_date: date) -> Path:
    """Return the one-file-per-day parquet path for a user/date."""
    partition = user_partition(user_login, full_name)
    return (
        month_dir(base_dir, entry_date)
        / f"user={partition}"
        / f"time_allocation_{entry_date:%Y%m%d}.parquet"
    )


def iter_user_window_candidate_files(
    base_dir: Path,
    user_login: str,
    full_name: str,
    window_start: date,
    window_end: date,
) -> list[Path]:
    """Candidate parquet files for one user across a date window.

    Resolves each day's targeted per-user partition file with a single stat, and
    lists legacy flat root files ONCE (indexed by the date parsed from each
    filename) instead of re-globbing the root per day. Mirrors the page helper so
    downstream user/date filtering is unchanged.
    """
    base_dir = Path(base_dir)
    files: list[Path] = []
    seen: set[str] = set()

    prefix = "time_allocation_"
    legacy_by_day: dict[date, list[Path]] = {}
    if base_dir.exists():
        for path in base_dir.glob(f"{prefix}*.parquet"):
            if not path.is_file():
                continue
            token = path.stem[len(prefix):len(prefix) + 8]
            try:
                legacy_day = date(int(token[:4]), int(token[4:6]), int(token[6:8]))
            except (ValueError, IndexError):
                continue
            legacy_by_day.setdefault(legacy_day, []).append(path)

    current_day = window_start
    while current_day <= window_end:
        user_path = daily_file(base_dir, user_login, full_name, current_day)
        if user_path.is_file():
            path_key = str(user_path).lower()
            if path_key not in seen:
                files.append(user_path)
                seen.add(path_key)
        for path in sorted(legacy_by_day.get(current_day, []), reverse=True):
            path_key = str(path).lower()
            if path_key not in seen:
                files.append(path)
                seen.add(path_key)
        current_day += timedelta(days=1)

    return files


def read_exports_from_files(file_paths: list[Path], base_dir: Path) -> pd.DataFrame:
    """Read and normalize saved time-allocation files into one DataFrame."""
    if not file_paths:
        return pd.DataFrame()

    base_dir = Path(base_dir)
    frames: list[pd.DataFrame] = []
    for file_path in file_paths:
        try:
            one = pd.read_parquet(file_path)
        except Exception as exc:
            LOGGER.warning("Skipping unreadable export file '%s': %s", file_path, exc)
            continue
        if one.empty:
            continue
        try:
            source_file = str(file_path.relative_to(base_dir))
        except ValueError:
            source_file = file_path.name
        one["Source File"] = source_file
        frames.append(one)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    expected_cols = list(BASE_COLUMNS)
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA
    if "Source File" not in df.columns:
        df["Source File"] = ""
    df["Entry Date"] = pd.to_datetime(df["Entry Date"], errors="coerce").dt.date
    return df[expected_cols + ["Source File"]]


def filter_user_exports(exports_df: pd.DataFrame, user_login: str, full_name: str) -> pd.DataFrame:
    """Return exports filtered to one user by login and full-name fallback."""
    if exports_df.empty:
        return exports_df.copy()

    login_key = normalize_login(user_login)
    full_name_key = str(full_name or "").strip().lower()
    user_series = exports_df["User"].fillna("").astype(str).map(normalize_login)
    mask = user_series.eq(login_key)
    if full_name_key:
        full_name_series = exports_df["Full Name"].fillna("").astype(str).str.strip().str.lower()
        mask = mask | full_name_series.eq(full_name_key)
    user_df = exports_df.loc[mask].copy()
    user_df["Entry Date"] = pd.to_datetime(user_df["Entry Date"], errors="coerce").dt.date
    user_df = user_df[user_df["Entry Date"].notna()].copy()
    return user_df


def load_user_window(
    base_dir: Path,
    user_login: str,
    full_name: str,
    window_start: date,
    window_end: date,
) -> pd.DataFrame:
    """Load one user's saved allocations for a bounded (date, date) window."""
    if not isinstance(window_start, date) or not isinstance(window_end, date):
        return pd.DataFrame()
    if window_end < window_start:
        return pd.DataFrame()

    files = iter_user_window_candidate_files(
        base_dir, user_login, full_name, window_start, window_end
    )
    window_df = read_exports_from_files(files, base_dir)
    if window_df.empty:
        return window_df

    filtered_df = filter_user_exports(window_df, user_login, full_name)
    filtered_df = filtered_df[
        filtered_df["Entry Date"].ge(window_start) & filtered_df["Entry Date"].le(window_end)
    ].copy()
    return filtered_df


# ---------------------------------------------------------------------------
# Work-week + gap detection
# ---------------------------------------------------------------------------
def work_week_bounds(reference: date) -> tuple[date, date]:
    """Return (Monday, Friday) of the work week containing `reference`."""
    monday = reference - timedelta(days=reference.weekday())
    return monday, monday + timedelta(days=4)


def previous_work_week(reference: date) -> tuple[date, date]:
    """Return (Monday, Friday) of the work week BEFORE the one containing `reference`."""
    this_monday, _ = work_week_bounds(reference)
    last_monday = this_monday - timedelta(days=7)
    return last_monday, last_monday + timedelta(days=4)


def _parse_hhmmss(text: object) -> int:
    """Parse an HH:MM or HH:MM:SS duration string into seconds (0 on failure)."""
    raw = str(text or "").strip()
    if not raw:
        return 0
    parts = raw.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return 0
    if len(nums) == 3:
        h, m, s = nums
    elif len(nums) == 2:
        h, m = nums
        s = 0
    elif len(nums) == 1:
        h = nums[0]
        m = s = 0
    else:
        return 0
    return h * 3600 + m * 60 + s


def daily_totals(window_df: pd.DataFrame) -> dict[date, int]:
    """Sum logged seconds per Entry Date from a loaded user window."""
    totals: dict[date, int] = {}
    if window_df is None or window_df.empty:
        return totals
    for entry_day, time_value in zip(window_df["Entry Date"], window_df["Time"]):
        if entry_day is None or pd.isna(entry_day):
            continue
        day = entry_day if isinstance(entry_day, date) else pd.to_datetime(entry_day, errors="coerce")
        if day is None or pd.isna(day):
            continue
        if not isinstance(day, date):
            day = day.date()
        totals[day] = totals.get(day, 0) + _parse_hhmmss(time_value)
    return totals


def missing_weekdays(
    window_df: pd.DataFrame,
    window_start: date,
    window_end: date,
) -> list[date]:
    """Return Mon-Fri dates in [start, end] with no logged time (total <= 0)."""
    totals = daily_totals(window_df)
    missing: list[date] = []
    current = window_start
    while current <= window_end:
        if current.weekday() < 5 and totals.get(current, 0) <= 0:
            missing.append(current)
        current += timedelta(days=1)
    return missing


def total_seconds(window_df: pd.DataFrame) -> int:
    """Total logged seconds across a loaded user window (all rows in the window)."""
    return sum(daily_totals(window_df).values())


# ---------------------------------------------------------------------------
# Maintenance — repair a 'Full Name' that was saved as the raw Windows login
# ---------------------------------------------------------------------------
def _read_own_columns(path: Path) -> "pa.Table":
    """Read a parquet file's OWN stored columns into a table.

    Reads the bytes first and parses from an in-memory buffer for two reasons,
    both of which matter on Windows network shares:
      * No partition discovery — a path-based read (pq.read_table / ds) would
        auto-append the Hive partition columns (year/month/user) inferred from the
        directory, which we'd then bake back into the file on write.
      * No lingering OS file handle — an open handle on the destination makes the
        atomic os.replace() fail with PermissionError (WinError 5).
    """
    data = Path(path).read_bytes()
    return pq.read_table(pa.BufferReader(data))


def all_export_files(base_dir: Path) -> list[Path]:
    """Every saved time-allocation parquet: partitioned files + legacy flat-root."""
    base_dir = Path(base_dir)
    if not base_dir.exists():
        return []
    found: list[Path] = []
    found.extend(base_dir.glob("year=*/month=*/user=*/*.parquet"))
    found.extend(base_dir.glob("time_allocation_*.parquet"))
    unique: list[Path] = []
    seen: set[str] = set()
    for path in found:
        if not path.is_file():
            continue
        key = str(path).lower()
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def repair_fullnames(
    base_dir: Path,
    login_to_fullname: dict,
    *,
    dry_run: bool = False,
) -> dict:
    """Repair rows whose 'Full Name' was saved as the user's Windows login.

    For each row, when its 'User' login maps to a known full name AND the stored
    'Full Name' is blank or equals that same login, set the real full name. ONLY
    the 'Full Name' column is rewritten; every other column and the file's parquet
    schema are preserved exactly (the column is swapped in-place on the Arrow
    table). Idempotent, and never alters a value that doesn't match the bug
    signature — so real names (which contain a space and never equal a login) are
    left untouched.

    `login_to_fullname`: map of login -> full name. Keys are normalized here, so
    either raw or pre-lowercased keys work (e.g. utils.load_user_fullname_map()).

    Returns a summary dict:
        files_scanned, files_changed, rows_fixed (ints),
        by_name {full name: rows}, errors [(path, message)], dry_run (bool).
    """
    base_dir = Path(base_dir)
    norm_map: dict[str, str] = {}
    for raw_login, raw_name in (login_to_fullname or {}).items():
        key = normalize_login(raw_login)
        name = str(raw_name or "").strip()
        if key and name:
            norm_map[key] = name

    summary: dict = {
        "files_scanned": 0,
        "files_changed": 0,
        "rows_fixed": 0,
        "by_name": {},
        "errors": [],
        "dry_run": bool(dry_run),
    }
    if not norm_map:
        return summary

    for path in all_export_files(base_dir):
        summary["files_scanned"] += 1
        try:
            table = _read_own_columns(path)
        except Exception as exc:
            summary["errors"].append((str(path), f"read failed: {exc}"))
            continue

        by_lower = {str(c).strip().lower(): c for c in table.column_names}
        user_col = by_lower.get("user")
        full_col = by_lower.get("full name") or by_lower.get("fullname")
        if not user_col or not full_col:
            continue  # not an entry file (no identity columns) — skip

        logins = table.column(user_col).to_pylist()
        names = table.column(full_col).to_pylist()
        new_names = list(names)
        file_by_name: dict[str, int] = {}
        for i, (login, name) in enumerate(zip(logins, names)):
            real = norm_map.get(normalize_login(login))
            if not real:
                continue  # login not in the roster — can't safely remap
            current = "" if name is None else str(name).strip()
            if current == real:
                continue  # already correct
            login_key = normalize_login(login)
            if current == "" or normalize_login(current) == login_key:
                new_names[i] = real
                file_by_name[real] = file_by_name.get(real, 0) + 1

        file_fixes = sum(file_by_name.values())
        if not file_fixes:
            continue

        if not dry_run:
            try:
                field = table.schema.field(full_col)
                new_col = pa.array(new_names, type=field.type)
                full_idx = table.column_names.index(full_col)
                new_table = table.set_column(full_idx, field, new_col)
                tmp = path.with_suffix(path.suffix + ".tmp")
                pq.write_table(new_table, tmp)
                os.replace(tmp, path)  # atomic swap; readers never see a half file
            except Exception as exc:
                summary["errors"].append((str(path), f"write failed: {exc}"))
                continue  # don't count fixes we couldn't persist

        summary["rows_fixed"] += file_fixes
        summary["files_changed"] += 1
        for k, v in file_by_name.items():
            summary["by_name"][k] = summary["by_name"].get(k, 0) + v

    return summary


def repair_blank_customer_codes(
    base_dir: Path,
    reporting_name_to_code: dict,
    *,
    dry_run: bool = False,
) -> dict:
    """Fill a blank 'Customer Code' from its row's 'Account' (Reporting Name).

    For each row whose 'Customer Code' is blank/missing AND whose 'Account'
    (Reporting Name) maps to a known code, set that code. ONLY the 'Customer Code'
    column is rewritten; every other column and the file's parquet schema are
    preserved exactly (the column is swapped in-place on the Arrow table).
    Idempotent, and never overwrites a code that is already present — so a
    hand-picked code is left untouched. A reporting name absent from the map (or a
    blank name) has nothing to fill from and is left blank.

    `reporting_name_to_code`: map of Reporting Name -> Customer Code to fill in.
    Matched on the stripped name, with a case-insensitive fallback.

    Returns a summary dict:
        files_scanned, files_changed, rows_fixed (ints),
        by_name {reporting name: rows}, errors [(path, message)], dry_run (bool).
    """
    base_dir = Path(base_dir)
    exact_map: dict[str, str] = {}
    lower_map: dict[str, str] = {}
    for raw_name, raw_code in (reporting_name_to_code or {}).items():
        name = str(raw_name or "").strip()
        code = str(raw_code or "").strip()
        if name and code:
            exact_map.setdefault(name, code)
            lower_map.setdefault(name.lower(), code)

    summary: dict = {
        "files_scanned": 0,
        "files_changed": 0,
        "rows_fixed": 0,
        "by_name": {},
        "errors": [],
        "dry_run": bool(dry_run),
    }
    if not exact_map:
        return summary

    def _code_for(name: object) -> str:
        key = str(name or "").strip()
        if not key:
            return ""
        return exact_map.get(key) or lower_map.get(key.lower(), "")

    for path in all_export_files(base_dir):
        summary["files_scanned"] += 1
        try:
            table = _read_own_columns(path)
        except Exception as exc:
            summary["errors"].append((str(path), f"read failed: {exc}"))
            continue

        by_lower = {str(c).strip().lower(): c for c in table.column_names}
        account_col = by_lower.get("account")
        code_col = by_lower.get("customer code") or by_lower.get("customercode")
        if not account_col or not code_col:
            continue  # not an entry file (no account/code columns) — skip

        accounts = table.column(account_col).to_pylist()
        codes = table.column(code_col).to_pylist()
        new_codes = list(codes)
        file_by_name: dict[str, int] = {}
        for i, (account, code) in enumerate(zip(accounts, codes)):
            current = "" if code is None else str(code).strip()
            if current:
                continue  # already has a code — never overwrite
            fill = _code_for(account)
            if not fill:
                continue  # reporting name not in the map — nothing to fill from
            new_codes[i] = fill
            name_key = str(account or "").strip()
            file_by_name[name_key] = file_by_name.get(name_key, 0) + 1

        file_fixes = sum(file_by_name.values())
        if not file_fixes:
            continue

        if not dry_run:
            try:
                field = table.schema.field(code_col)
                new_col = pa.array(new_codes, type=field.type)
                code_idx = table.column_names.index(code_col)
                new_table = table.set_column(code_idx, field, new_col)
                tmp = path.with_suffix(path.suffix + ".tmp")
                pq.write_table(new_table, tmp)
                os.replace(tmp, path)  # atomic swap; readers never see a half file
            except Exception as exc:
                summary["errors"].append((str(path), f"write failed: {exc}"))
                continue  # don't count fixes we couldn't persist

        summary["rows_fixed"] += file_fixes
        summary["files_changed"] += 1
        for k, v in file_by_name.items():
            summary["by_name"][k] = summary["by_name"].get(k, 0) + v

    return summary
