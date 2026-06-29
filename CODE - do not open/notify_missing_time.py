"""
notify_missing_time.py

Scheduled producer for Time Allocation "missing entries" reminder emails.

For each department that an admin has ENABLED (Time Allocation > Admin Settings),
this finds every employee whose total logged account time for the CURRENT week
(Mon-Fri) is under 20 hours, and emails them a reminder from the shared CNA Console
mailbox via Outlook. When a department's manager recap is on, each manager also gets
a weekly-total summary of their own reports, with anyone under 20h flagged. Settings
(per-department enable, send day, from-address, pilot toggle) live in users-readable
time_allocation_settings.json and are loaded via utils.load_auto_email_settings().

Execution model (mirrors refresh_data.py):
    Meant to run on ONE reliably-synced, logged-on machine via a Windows
    Scheduled Task every weekday morning. It reads the shared parquet on the UNC
    share and sends through that machine's classic Outlook. OneDrive/UNC + Outlook
    COM only work in an interactive logged-on session. It never raises to the
    caller — on any failure it logs and exits 0.

Safety tiers:
    (default, no flags)  Respect settings. While live=false NOTHING goes to
                         employees: if a test recipient is set, one per-department
                         DIGEST preview is sent to them; otherwise it only logs.
                         While live=true, employees are emailed and a per-dept
                         "last sent" marker is written so cadence isn't repeated.
    --test [--to ADDR]   Send the REAL per-employee emails, but redirected to the
                         test recipient (or --to). Ignores cadence + live. No marker.
    --dry-run            Compute and log the plan only. Never opens Outlook. No marker.
    --force              In a default/live run, bypass the cadence/send-day gate
                         (manual "run now"). Still requires live=true to email staff.
    --department NAME    Restrict to one department (treated as enabled for test/dry).

Run manually:
    .venv\\Scripts\\python.exe "CODE - do not open\\notify_missing_time.py" --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
ROOT_DIR = CODE_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))


def _bootstrap_log(message: str) -> None:
    """Last-resort log when the app logger isn't importable yet."""
    try:
        path = Path(tempfile.gettempdir()) / "cna_notify_missing_time_bootstrap.log"
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(f"{datetime.now().isoformat()} {message}\n")
    except Exception:
        pass


# Best-effort: ensure config.py exists by decrypting config.enc (fail-open, like
# refresh_data.py / StartApp.bat). decrypt() calls sys.exit on failure, so guard it.
try:
    import config_manager

    config_manager.decrypt()
except SystemExit:
    pass
except Exception:
    pass

try:
    import config
    import utils
    import time_allocation_store as ta_store
except Exception as exc:  # honor the never-raises contract even this early
    _bootstrap_log(f"bootstrap import failed: {exc}")
    sys.exit(0)


WEEKDAY_ABBR = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _weekday_abbr(value: date) -> str:
    return WEEKDAY_ABBR[value.weekday()]


def _parse_iso_date(value: object) -> date | None:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _format_day_long(value: date) -> str:
    """e.g. 'Monday, June 2' (Windows strftime has no %-d)."""
    return f"{value:%A, %B} {value.day}"


def _format_window(start: date, end: date) -> str:
    return f"{start:%b} {start.day}–{end:%b} {end.day}"


def _first_name(full_name: str) -> str:
    name = str(full_name or "").strip()
    return name.split()[0] if name else "there"


# Weekly logged-account-time threshold. Employees whose total for the current week
# (Mon-Fri) is under this many hours get a reminder; managers see them flagged.
WEEKLY_THRESHOLD_HOURS = 20
_WEEKLY_THRESHOLD_SECONDS = WEEKLY_THRESHOLD_HOURS * 3600


def _format_hours(seconds: int) -> str:
    return f"{max(0, int(seconds)) / 3600:.1f} h"


def _load_notify_state(path: Path) -> dict:
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def _save_notify_state(path: Path, state: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as exc:
        _bootstrap_log(f"failed to write notify state '{path}': {exc}")


def _build_employee_email(display_name: str) -> tuple[str, str]:
    subject = "Action needed: Weekly account time below threshold"
    html = f"""<html><body style="font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#1f2933;">
<p>{_first_name(display_name)},</p>
<p>As of 3:00 PM today, your logged account time for the week is below the expected
weekly threshold.</p>
<p>Please update your time entries before the end of the day today. This is an important
part of our weekly closeout process and directly supports account-level cost allocation,
profitability analysis, staffing decisions, and overall visibility into how we are
supporting customers.</p>
<p>If your lower total is accurate due to PTO, travel, internal meetings, or other
non-account-specific work, please confirm that your entries are complete and reflect the
week appropriately.</p>
<p>Thanks for getting this updated before the week closes.</p>
<p>- The Leadership Team</p>
</body></html>"""
    return subject, html


def _wrap_pilot(html: str, intended_email: str) -> str:
    banner = (
        '<div style="font-family:Calibri,Arial,sans-serif;font-size:10pt;background:#fff4e5;'
        'border:1px solid #f0b429;border-radius:6px;padding:8px 12px;margin-bottom:12px;color:#8a5300;">'
        f"<b>PILOT PREVIEW</b> &mdash; live sending is OFF. This would have been sent to "
        f"<b>{intended_email}</b>.</div>"
    )
    return banner + html


def _build_digest_email(dept: str, under_plan: list[tuple[str, str, int]], start: date, end: date) -> tuple[str, str]:
    window = _format_window(start, end)
    subject = f"[Pilot] Weekly time reminders — {dept} ({window})"
    rows = ""
    for display_name, email, total in under_plan:
        rows += (
            f'<tr><td style="padding:4px 10px;border-bottom:1px solid #e4e7eb;">{display_name}</td>'
            f'<td style="padding:4px 10px;border-bottom:1px solid #e4e7eb;">{email}</td>'
            f'<td style="padding:4px 10px;border-bottom:1px solid #e4e7eb;">{_format_hours(total)}</td></tr>'
        )
    html = f"""<html><body style="font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#1f2933;">
<p><b>Pilot preview</b> for <b>{dept}</b>. Live sending is OFF, so no employees were emailed.
When you turn on <i>Send live to employees</i>, each person below is emailed directly.</p>
<p>Employees under the {WEEKLY_THRESHOLD_HOURS}-hour weekly threshold for the current week ({window}):</p>
<table style="border-collapse:collapse;font-size:10.5pt;">
<tr><th style="text-align:left;padding:4px 10px;border-bottom:2px solid #cbd2d9;">Employee</th>
<th style="text-align:left;padding:4px 10px;border-bottom:2px solid #cbd2d9;">Email</th>
<th style="text-align:left;padding:4px 10px;border-bottom:2px solid #cbd2d9;">Hours this week</th></tr>
{rows}</table>
<p style="color:#52606d;">CNA Console (automated reminder — pilot mode)</p>
</body></html>"""
    return subject, html


def _normalize_name(value: object) -> str:
    return str(value or "").strip().lower()


def _reports_to(report: dict, manager: dict) -> bool:
    """True when `report` rolls up to `manager` — matched on the Manager NAME field
    (report's Manager == manager's Full Name), with ManagerEmployeeNumber as a
    secondary match when both are present."""
    r_mgr_name = _normalize_name(report.get("manager_name"))
    m_full = _normalize_name(manager.get("full_name"))
    if r_mgr_name and m_full and r_mgr_name == m_full:
        return True
    r_mgr_num = str(report.get("mgr_emp_num") or "").strip()
    m_num = str(manager.get("emp_num") or "").strip()
    if r_mgr_num and m_num and r_mgr_num == m_num:
        return True
    return False


def _build_manager_summary_email(
    manager_name: str,
    dept: str,
    report_rows: list[tuple[str, int, bool]],
    start: date,
    end: date,
) -> tuple[str, str]:
    window = _format_window(start, end)
    subject = f"Weekly team time summary — {dept} ({window})"
    rows = ""
    for employee_name, total, under in report_rows:
        flag = (
            f'<span style="color:#cf1124;font-weight:600;">&#9888; Below {WEEKLY_THRESHOLD_HOURS}h</span>'
            if under else ""
        )
        hours_style = "color:#cf1124;font-weight:600;" if under else ""
        rows += (
            f'<tr><td style="padding:4px 10px;border-bottom:1px solid #e4e7eb;">{employee_name}</td>'
            f'<td style="padding:4px 10px;border-bottom:1px solid #e4e7eb;{hours_style}">{_format_hours(total)}</td>'
            f'<td style="padding:4px 10px;border-bottom:1px solid #e4e7eb;">{flag}</td></tr>'
        )
    html = f"""<html><body style="font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#1f2933;">
<p>Hi {_first_name(manager_name)},</p>
<p>Here are the logged account hours for your team in <b>{dept}</b> for the current week
({window}).</p>
<table style="border-collapse:collapse;font-size:10.5pt;">
<tr><th style="text-align:left;padding:4px 10px;border-bottom:2px solid #cbd2d9;">Employee</th>
<th style="text-align:left;padding:4px 10px;border-bottom:2px solid #cbd2d9;">Hours this week</th>
<th style="text-align:left;padding:4px 10px;border-bottom:2px solid #cbd2d9;">Status</th></tr>
{rows}</table>
<p style="color:#52606d;">CNA Console (automated reminder)</p>
</body></html>"""
    return subject, html


def _mode_label(args: argparse.Namespace) -> str:
    if args.dry_run:
        return "dry-run"
    if args.test:
        return "test"
    return "scheduled"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Send Time Allocation missing-entry reminders.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and log only; never send.")
    parser.add_argument("--test", action="store_true", help="Send real emails to the test recipient only.")
    parser.add_argument("--to", default="", help="Override test recipient for --test.")
    parser.add_argument("--force", action="store_true", help="Bypass the cadence/send-day gate in a live run.")
    parser.add_argument("--department", default="", help="Restrict to one department.")
    parser.add_argument("--limit", type=int, default=0, help="Cap recipients per department (0 = no cap). Handy for tests.")
    args = parser.parse_args(argv)

    os.environ.setdefault("STARTUP_CALLER", "notify_missing_time.py (scheduled)")
    logger = utils.get_program_logger("notify_missing_time.py", "Auto Email")

    try:
        settings = utils.load_auto_email_settings()
        users_df = utils.load_users_table()
    except Exception as exc:
        logger.error("Auto-email: failed to load settings/users: %s", exc)
        return 0

    if users_df is None or users_df.empty:
        logger.warning("Auto-email: users.parquet empty/unavailable; nothing to do.")
        return 0

    today = utils.to_eastern(utils.now_utc()).date()
    win_start, win_end = ta_store.work_week_bounds(today)
    logger.info(
        "Auto-email run | mode=%s window=%s..%s today=%s live=%s",
        _mode_label(args), win_start, win_end, today, settings.get("live"),
    )

    dept_col = utils._find_column_by_alias(users_df, ["Department", "Dept"])
    email_col = utils._find_column_by_alias(users_df, ["Email", "EmailAddress", "E-mail"])
    user_col = utils._find_column_by_alias(
        users_df, ["User", "UserLogin", "Login", "Username", "User Name", "NetworkLogin", "SamAccountName"]
    )
    name_col = utils._find_column_by_alias(users_df, ["Full Name", "FullName", "Name"])
    ismgr_col = utils._find_column_by_alias(users_df, ["IsManager", "Is Manager", "Manager Flag"])
    mgrname_col = utils._find_column_by_alias(users_df, ["Manager", "Manager Name"])
    empnum_col = utils._find_column_by_alias(users_df, ["EmployeeNumber", "Employee Number", "Employee ID"])
    mgrempnum_col = utils._find_column_by_alias(
        users_df, ["ManagerEmployeeNumber", "Manager Employee Number", "Manager ID"]
    )
    if not dept_col or not email_col:
        logger.error("Auto-email: users.parquet missing Department or Email column; aborting.")
        return 0

    from_address = settings.get("from_address") or utils.DEFAULT_AUTO_EMAIL_FROM
    live = bool(settings.get("live"))
    test_recipient = (args.to or settings.get("test_recipient") or "").strip()
    dept_settings = settings.get("departments") or {}

    base_dir = Path(config.TIME_ALLOCATION_DIR)
    state_path = Path(config.PERSONNEL_DIR) / "time_allocation_notify_state.json"
    state = _load_notify_state(state_path)

    distinct = sorted({str(v).strip() for v in users_df[dept_col].dropna() if str(v).strip()})
    if args.department:
        wanted = args.department.strip().lower()
        distinct = [d for d in distinct if d.lower() == wanted]
        if not distinct:
            logger.warning("Auto-email: department '%s' not found in users.parquet.", args.department)

    # Import the sender lazily so --dry-run works on machines without Outlook/pywin32.
    mailer = None
    if not args.dry_run:
        try:
            import outlook_mailer as mailer
        except Exception as exc:
            logger.error("Auto-email: Outlook sender unavailable (%s); falling back to dry-run.", exc)
            mailer = None

    def _try_send(to_addr: str, subject: str, html: str, label: str) -> int:
        """Send one mail; return 1 on success, 0 on failure (logged)."""
        try:
            mode = mailer.send_html_mail(to_addr, subject, html, from_address=from_address)
            logger.info("Sent %s | to=%s from=%s", label, to_addr, mode)
            return 1
        except Exception as exc:
            logger.error("Failed %s | to=%s: %s", label, to_addr, exc)
            return 0

    emails_sent = 0
    for dept in distinct:
        cfg = utils.normalize_auto_email_department(dept_settings.get(dept))
        forced_dept = bool(args.department)
        if not cfg["enabled"] and not forced_dept:
            continue

        # Cadence gate applies only to a normal scheduled run (not --test/--dry-run/--force).
        if not args.dry_run and not args.test and not args.force:
            if _weekday_abbr(today) != cfg["send_day"]:
                logger.info("Dept '%s' not due today (send_day=%s).", dept, cfg["send_day"])
                continue
            last_sent = _parse_iso_date(state.get(dept))
            min_days = 13 if cfg["interval"] == "biweekly" else 6
            if last_sent and (today - last_sent).days < min_days:
                logger.info("Dept '%s' already notified %s (< %s days ago); skipping.", dept, last_sent, min_days)
                continue

        # Build a record per department member (role + manager linkage + weekly total).
        dept_rows = users_df[users_df[dept_col].astype(str).str.strip().str.lower() == dept.lower()]
        records: list[dict] = []
        for _, row in dept_rows.iterrows():
            email = str(row.get(email_col) or "").strip()
            login = str(row.get(user_col) or "").strip() if user_col else ""
            full = str(row.get(name_col) or "").strip() if name_col else ""
            window_df = ta_store.load_user_window(base_dir, login, full, win_start, win_end)
            total = ta_store.total_seconds(window_df)
            records.append({
                "name": full or login or email,
                "full_name": full,
                "email": email,
                "is_manager": utils._coerce_bool_like(row.get(ismgr_col)) if ismgr_col else False,
                "manager_name": str(row.get(mgrname_col) or "").strip() if mgrname_col else "",
                "emp_num": str(row.get(empnum_col) or "").strip() if empnum_col else "",
                "mgr_emp_num": str(row.get(mgrempnum_col) or "").strip() if mgrempnum_col else "",
                "total_seconds": total,
                "under": total < _WEEKLY_THRESHOLD_SECONDS,
            })

        # Individual reminder plan: anyone with an email whose weekly total is under threshold.
        under_plan = [(r["name"], r["email"], r["total_seconds"]) for r in records if r["email"] and r["under"]]
        for r in records:
            if r["under"] and not r["email"]:
                logger.info("Dept '%s': %s is under %sh this week but has no email on file.",
                            dept, r["name"], WEEKLY_THRESHOLD_HOURS)
        logger.info("Dept '%s': %s employee(s) under the %sh weekly threshold.",
                    dept, len(under_plan), WEEKLY_THRESHOLD_HOURS)
        limited_plan = under_plan[: args.limit] if (args.limit and args.limit > 0) else under_plan

        # --- Individual reminders ---
        if args.dry_run or mailer is None:
            for name, email, total in limited_plan:
                logger.info("[dry-run] would email %s (%s) | %.1fh this week", name, email, total / 3600)
        elif args.test or not live:
            # Pilot/test: never email employees directly. --test redirects each real
            # email to the test recipient; a normal pilot run sends ONE digest preview.
            # Either path also gets one clean sample of the genuine employee email.
            if not test_recipient:
                logger.warning("Dept '%s': no test recipient configured; pilot send skipped.", dept)
            else:
                if args.test:
                    for name, email, total in limited_plan:
                        subject, html = _build_employee_email(name)
                        emails_sent += _try_send(test_recipient, subject, _wrap_pilot(html, email),
                                                 f"test individual (intended {email})")
                elif under_plan:
                    subject, html = _build_digest_email(dept, under_plan, win_start, win_end)
                    emails_sent += _try_send(test_recipient, subject, html, f"pilot digest dept={dept}")
                # One clean copy — exactly what a real employee would receive (no pilot
                # banner) — so the tester previews the genuine email.
                if under_plan:
                    sample_name = under_plan[0][0]
                    subject, html = _build_employee_email(sample_name)
                    emails_sent += _try_send(test_recipient, subject, html,
                                             f"sample employee email (as {sample_name})")
        else:
            for name, email, total in under_plan:
                subject, html = _build_employee_email(name)
                emails_sent += _try_send(email, subject, html, "reminder")

        # --- Manager summary (optional, per department) ---
        # Each manager gets the weekly total for ALL of their reports (matched by the
        # Manager field), with reports under the threshold flagged. Live -> the manager;
        # pilot/test -> the test recipient.
        if cfg.get("manager_recap"):
            managers = [r for r in records if r["is_manager"] and r["email"]]
            for mgr in managers:
                reports = [r for r in records if (not r["is_manager"]) and _reports_to(r, mgr)]
                if not reports:
                    continue
                report_rows = sorted(
                    [(r["name"], r["total_seconds"], r["under"]) for r in reports],
                    key=lambda item: item[1],
                )
                under_count = sum(1 for _, _, u in report_rows if u)
                subject, html = _build_manager_summary_email(mgr["name"], dept, report_rows, win_start, win_end)
                if args.dry_run or mailer is None:
                    logger.info("[dry-run] manager summary to %s (%s) | %s report(s), %s under %sh",
                                mgr["name"], mgr["email"], len(report_rows), under_count, WEEKLY_THRESHOLD_HOURS)
                elif args.test or not live:
                    if test_recipient:
                        emails_sent += _try_send(test_recipient, subject, _wrap_pilot(html, mgr["email"]),
                                                 f"manager summary (intended {mgr['email']})")
                    else:
                        logger.warning("Dept '%s': no test recipient; manager summary skipped.", dept)
                else:
                    emails_sent += _try_send(mgr["email"], subject, html, f"manager summary to {mgr['email']}")

        # --- Cadence marker (only a real, live, scheduled run records it) ---
        if live and not args.dry_run and not args.test:
            state[dept] = today.isoformat()
            _save_notify_state(state_path, state)
            logger.info("Dept '%s' marked notified for %s.", dept, today)

    logger.info("Auto-email run complete | emails_sent=%s", emails_sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
