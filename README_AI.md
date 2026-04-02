# README_AI

This file is for AI-assisted maintenance of the `CNA-WebApp` repository.

It documents how the project actually works today so changes can be made safely, consistently, and with a low chance of breaking internal workflows.

## 1. Project Summary

This repository is a Windows-first internal Streamlit suite for Logistics Support work.

At a high level it provides:
- A home page and centralized sidebar navigation
- A task timer / live activity tracker
- Historical task analytics
- Admin maintenance for task definitions, targets, users, and task log edits
- A packaging estimator that matches uploaded items to item-info data and then calls a shipping calculator API
- A time allocation tool with daily replacement behavior and admin exports
- A FedEx address validation dispute workflow
- A stocking agreement generator that fills Word templates and optionally converts them to PDF
- Admin log review

The app is designed around cached parquet datasets, shared UNC network storage, and lightweight Streamlit pages with common styling.

## 2. Important Ground Rules

Before changing code, keep these project-specific rules in mind:

1. `CODE - do not open/` is the real application source directory.
   The name is misleading, but launcher scripts and imports depend on it. Do not rename or move it casually.

2. `StartApp.bat` and `setup.bat` assume the current layout.
   If you move files like `app.py`, `startup.py`, or `requirements.txt`, you must update the batch files too.

3. Shared storage conventions matter.
   A lot of pages read and write parquet or CSV files on internal UNC paths from `config.py`. Do not change schemas, filenames, or partition layouts unless you update every consumer.

4. `tasks.parquet` is admin-managed.
   `startup.py` deliberately refreshes accounts and users only. It does not rebuild `tasks.parquet`; the Management page owns task definitions.

5. After writes, caches usually need clearing.
   The code relies heavily on `@st.cache_data`. When you modify persisted files, clear the relevant cached loaders or rerun the app flow.

6. Reuse shared helpers instead of page-local reinvention.
   Common patterns already live in `utils.py`, `app_logging.py`, `page_registry.py`, and `config.py`.

7. Keep admin gating in place.
   Admin-only pages both hide from non-admin users in navigation and also enforce access in the page itself.

## 3. Repository Layout

Root:
- `config.py`
  Central runtime config, UNC paths, packaging config, log path helpers.
- `setup.bat`
  Creates `.venv` and installs dependencies from `CODE - do not open/requirements.txt`.
- `StartApp.bat`
  Optionally pulls latest git changes, runs `startup.py`, launches Streamlit on port `8501`.
- `.venv/`
  Local Python environment created by `setup.bat`.
- `CODE - do not open/`
  Actual app source.

Main source folder:
- `app.py`
  Streamlit app shell and navigation bootstrap.
- `startup.py`
  Prepares daily cached personnel datasets.
- `page_registry.py`
  Single source of truth for pages, sections, icons, captions, quotes, and admin visibility.
- `utils.py`
  Shared helpers for styling, identity, logging access, parquet I/O, live activity, completed tasks, users, accounts, targets, and admin detection.
- `app_logging.py`
  Per-user shared log file setup.
- `stocking_agreement_service.py`
  Template preparation, docxtpl rendering, pricing-table population, Word-to-PDF conversion.
- `pages/`
  Streamlit pages.
- `templates/stocking_agreements/`
  Canonical Word templates used by the agreement generator.
- `scripts/`
  Support scripts.
- `docs/`
  Older internal documentation set.

## 4. Launch And Runtime Flow

### Setup
`setup.bat`:
- Finds or installs `uv` (Astral's Python package manager)
- Installs Python 3.11 via `uv python install 3.11` if not already present
- Creates `.venv` with `uv venv --python 3.11` if missing
- Installs dependencies from `CODE - do not open/requirements.txt` using `uv pip install --link-mode copy` (copy mode required for OneDrive compatibility)
- Creates a `CNA Web App.lnk` shortcut using `cna_icon.ico`

### App launch
`StartApp.bat`:
- Sets `ROOT_DIR`, `CODE_DIR`, `APP_FILE`, `STARTUP_FILE`
- Adds root and code directories to `PYTHONPATH`
- Syncs `config.py` from the network share (`\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.py`)
- Writes launcher logs to shared logs if available, else local fallback
- Checks git remote, fetches, and fast-forward pulls
- Clears `__pycache__` directories after pull
- Runs `startup.py`
- Starts Streamlit with `pythonw -m streamlit run ... --server.port=8501`
- Opens in Microsoft Edge app mode (`msedge --app=...`) for a standalone window without browser chrome

### Force close
`ForceCloseApp.bat`:
- Finds the process listening on port 8501 via `netstat`
- Kills it with `taskkill`

### Installer
`CODE - do not open/installer/CNA-WebApp-Setup.iss`:
- Inno Setup script that compiles to a standalone `.exe` installer
- Installs Git via winget if missing, clones the repo, runs `setup.bat`, copies `config.py`
- Project files install to `%LOCALAPPDATA%\CNA-WebApp`; shortcut location is user-chosen

### Startup job
`CODE - do not open/startup.py`:
- Locates the locally synced Task-Tracker / SharePoint root
- Reads:
  - `TasksAndTargets.xlsx` for the `Users` sheet
  - `CNA Personnel - Temporary.xlsx` for account mappings
- Writes:
  - `accounts_<YYYY-MM-DD>.parquet`
  - `users.parquet`
- Intentionally does not regenerate `tasks.parquet`

## 5. Navigation Model

`app.py` uses:
- `page_registry.get_home_page()`
- `page_registry.get_visible_sections(is_admin_user)`
- `st.navigation(..., position="hidden")`
- a custom sidebar built from `st.page_link`

This means:
- Adding, renaming, or reordering pages should go through `page_registry.py`
- The home page cards and sidebar both depend on the same registry
- Admin-only visibility is controlled there, but page-level access checks still matter

Sidebar sections use `st.expander()` for collapsible dropdowns, with the active section auto-expanded.

Current sections in `page_registry.py`:
- `Admin Tools`
- `Task Tracker`
- `Work in Progress`

Current registered pages:
- `pages/home.py`
- `pages/tasks-management.py`
- `pages/admin-logs.py`
- `pages/fedex-address-validation-management.py`
- `pages/task-tracker.py` (combined Logistics Support and Data & Analytics with version toggle)
- `pages/da-task-tracker.py` (thin wrapper providing `/da-task-tracker` URL for D&A version)
- `pages/task-tracker-analytics.py` (combined analytics with version toggle)
- `pages/packaging-estimator.py`
- `pages/time-allocation-tool.py`
- `pages/fedex-address-validator.py`
- `pages/stocking-agreement-generator.py`

Hidden pages (registered in `app.py` for URL routing, not shown in sidebar):
- `pages/da-task-tracker.py` — provides `/da-task-tracker` URL so the D&A version persists on page refresh

## 6. Shared Styling, Fonts, And UI Conventions

Global UI styling comes from `utils.get_global_css()`.

Current UI font stack:
- Headings: `Poppins`
- Body text: `Work Sans`
- Both are imported from Google Fonts in the shared CSS

Shared UI conventions:
- Every page usually calls:
  - `st.set_page_config(..., page_icon=utils.get_app_icon())` — custom icon on all pages
  - `utils.render_app_logo()`
  - `st.markdown(utils.get_global_css(), unsafe_allow_html=True)`
  - `utils.render_page_header(PAGE_TITLE)`
  - `utils.log_page_open_once(...)`
- Page titles resolve from `page_registry.py` using `utils.get_registry_page_title(...)`
- Page quotes also come from `page_registry.py` and are rendered under the title
- The Streamlit footer is hidden
- Sidebar page-link text is slightly resized
- Task tracker uses a blinking timer colon and a pulsing live-activity dot
- Analytics KPI cards use a shared gray card style

Brand assets:
- Logo path comes from `config.LOGO_PATH`
- `utils.render_app_logo()` uses `st.logo(...)` when available
- `utils.get_app_icon()` returns the path to `icon.png` (root) for use as favicon/page icon
- `cna_icon.ico` (root) is used for the Windows shortcut icon

Word template font details:
- `stocking_agreement_service.py` uses `DEFAULT_FONT_NAME = "Sofia Pro"`
- Default fallback size is `7 pt`
- That font logic applies to generated Word content, not the Streamlit UI

## 7. Logging Model

`app_logging.py` creates one shared log file per user:
- Log root: `config.LOGS_ROOT_DIR`
- Log filename: `AppLogs.log`
- User-specific folder: `config.get_log_dir_for_user()`

Message format:
- `timestamp | level | [context] message`

Most pages create loggers through:
- `utils.get_page_logger(page_name)`
- `utils.get_program_logger(source_file, context_name)`

Admin review page:
- `pages/admin-logs.py` loads all user log files under the logs root
- Maps logins to full names and departments via `users.parquet`
- Supports filters for date, department, user, and page

## 8. Data Storage And Schemas

Important configured storage locations in `config.py`:
- `COMPLETED_TASKS_DIR`
- `LIVE_ACTIVITY_DIR`
- `ARCHIVED_TASKS_DIR`
- `PERSONNEL_DIR`
- `TIME_ALLOCATION_DIR`
- `ADDRESS_VALIDATION_RESULTS_FILE`
- `TASK_TARGETS_CSV_PATH`

### Completed task records
Written by the Task Tracker page.

Partition layout:
- `user=<user_key>/year=<YYYY>/month=<MM>/day=<DD>/*.parquet`

Primary schema in `utils.PARQUET_SCHEMA`:
- `TaskID`
- `UserLogin`
- `FullName`
- `TaskName`
- `TaskCadence`
- `CompanyGroup`
- `IsCoveringFor`
- `CoveringFor`
- `Notes`
- `PartiallyComplete`
- `StartTimestampUTC`
- `EndTimestampUTC`
- `DurationSeconds`
- `UploadTimestampUTC`
- `AppVersion`

### Live activity
One tiny parquet file per active user:
- `LIVE_ACTIVITY_DIR/user=<user_key>.parquet`

Used to restore session state and show who is currently working on what.

### Archived paused tasks
One parquet per archived paused timer:
- `ARCHIVED_TASKS_DIR/user=<user_key>/archive_<timestamp>_<id>.parquet`

### Personnel data
Produced mostly by `startup.py`:
- `accounts_<YYYY-MM-DD>.parquet`
- `users.parquet`
- `tasks.parquet`
- `taskstargets.csv`

### Time allocation exports
One parquet per save event:
- `time_allocation_<YYYYMMDD>_<HHMMSS>_<id>.parquet`

Behavior note:
- saving a day replaces all existing entries for the same user and date across export files before writing the new file

### FedEx validation results
Base configured file:
- `config.ADDRESS_VALIDATION_RESULTS_FILE`

Pages use:
- `.csv` for the main validator page
- `.parquet` preferred, with `.csv` fallback, for the admin results-management page

## 9. Shared Utility Layer

`utils.py` is the main internal platform layer.

Important responsibilities:
- Time helpers:
  - UTC/Eastern conversion
  - `HH:MM` and `HH:MM:SS` parsing/formatting
  - relative "time ago" formatting
- Identity:
  - current OS user
  - login normalization
  - department lookup
  - admin detection from `users.parquet`
- Styling:
  - global CSS
  - page headers
  - logo rendering
- Safe parquet writes:
  - `atomic_write_parquet(...)`
- Task storage:
  - `build_out_dir(...)`
  - completed task loaders
  - analytics loaders
- Live activity:
  - save/update/load/delete helpers
- Archived tasks:
  - save/load/delete helpers
- Metadata:
  - active tasks
  - account list
  - full-name maps
- Target computation:
  - `compute_monthly_task_targets(...)`
  - `sync_tasks_parquet_targets(...)`
  - `save_task_target(...)`

Important performance choices in `utils.py`:
- `@st.cache_data` on most read-heavy functions
- `pyarrow.dataset` for partitioned parquet reads
- selective column reads where possible
- atomic temp-file replacement for writes

When changing persistence logic, preserve these patterns.

## 10. Page-By-Page Behavior

### Home (`pages/home.py`)

Purpose:
- Landing page for the suite

What it does:
- Resolves visible sections through `page_registry.py`
- Renders simple navigation cards in two-column rows
- Reuses shared page header and styling

Notes:
- This page is intentionally lightweight
- If you add a page to the registry, it will appear here automatically if visible

### Management (`pages/tasks-management.py`)

Purpose:
- Admin-only control center for task definitions, monthly targets, submitted task logs, and users

Top-level tabs:
- `Logistics - Support`
- `Project Services` (currently placeholder only)
- `General`

Logistics tabs:
- `Task Definition`
- `Task Targets`
- `Task Log`

General tab:
- `Users`

Task Definition behavior:
- Loads `tasks.parquet`
- Normalizes columns such as `TaskName`, `TaskCadence`, `IsActive`
- Allows filtering by task name and cadence
- Supports row selection for deletion
- Supports adding a task name + cadence
- Uses a confirmation dialog before overwriting `tasks.parquet`

Task Targets behavior:
- Loads `taskstargets.csv`
- Filters by year and month
- Allows editing only the `Target` column for existing rows
- Includes an "Add Target" form
- Computes default targets using:
  - number of assigned users
  - cadence
  - business days / approximate weeks
- Saves through `utils.save_task_target(...)`

Task Log behavior:
- Reads all completed-task parquet files
- Allows filtering by date range, task, user, and notes
- Allows editing:
  - `Duration`
  - `Notes`
- Supports row deletion with confirmation
- Persists edits back to original source parquet files

Users behavior:
- Reads `users.parquet`
- Supports search, department filter, admin-status filter
- Read-only display

Change risk notes:
- This page writes back into live production parquet and CSV files
- If you change column names or write logic, verify every consuming page

### Logging (`pages/admin-logs.py`)

Purpose:
- Admin-only log viewer

What it does:
- Reads per-user `AppLogs.log` files
- Parses lines with regex
- Maps user folders to full name and department
- Filters by date, department, user, and page
- Shows parsed log entries in a dataframe

### FedEx Validator Results (`pages/fedex-address-validation-management.py`)

Purpose:
- Admin-only review and cleanup page for validation results

What it does:
- Prefers `results.parquet`, falls back to `results.csv`
- Detects the disputed flag column with alias matching
- Filters by:
  - disputed state
  - classification
  - service type
  - residential match
- Lets admin select rows and clear the disputed flag

Key behavior:
- It preserves source row IDs while filtering so writes map back to the original dataframe correctly

### Task Tracker (`pages/task-tracker.py` and `pages/da-task-tracker.py`)

Purpose:
- Main day-to-day task timing page, combining Logistics Support and Data & Analytics versions

Architecture:
- Both versions live in `task-tracker.py` with a version toggle at the top
- LS session state keys are unprefixed; DA keys are prefixed with `da_`
- `da-task-tracker.py` is a thin wrapper that sets `_da_page_active` in session state and exec's `task-tracker.py` (with `encoding="utf-8"`)
- Version toggle buttons use `st.switch_page()` to navigate between `/task-tracker` (LS) and `/da-task-tracker` (DA)
- The version is determined on each render from which page file is active, not from persistent session state

What it does:
- Loads:
  - current user and full name
  - covering-for user list from `users.parquet` (LS) or primary stakeholder text input (DA)
  - accounts from daily accounts parquet
  - active tasks from `tasks.parquet` (LS) or free-text task input (DA)
  - department dropdown (DA only)
- Supports timer states:
  - idle
  - running
  - paused
  - ended
- Supports actions:
  - start
  - pause
  - resume
  - end
  - archive paused task
  - upload completed task
  - reset
- Lets the user mark a task partially complete
- Broadcasts live activity in near real time through a per-user parquet file
- Restores an in-progress session from the live-activity file on refresh
- Shows:
  - other users' live activity
  - today's completed activity

DA-specific features:
- Today's Activity uses `st.data_editor` with a Delete checkbox column for deleting own tasks
- Partially complete tasks can be resumed: selecting one and clicking "Resume task" deletes the old entry and pre-populates all fields (task name, account, stakeholder, department, notes)
- Resume only available when tracker is idle, task is own, and task is marked partially complete
- DA parquet schema includes a `Department` column (defined as `DA_PARQUET_SCHEMA`)

Timing behavior:
- Uses UTC internally
- Displays Eastern time to users
- Excludes paused time from elapsed duration
- Allows duration override before final upload

Live updates:
- `st.fragment(run_every=30)` for the team live-activity table
- `streamlit_autorefresh` for the running timer display every 10 seconds

Persistence behavior:
- Completed uploads write one parquet file into the partitioned completed-task lake
- LS upload also attempts `utils.sync_tasks_parquet_targets()` (DA does not)
- Archived tasks go into a separate per-user archive folder
- LS and DA use separate directory constants (`LS_COMPLETED_TASKS_DIR` / `DA_COMPLETED_TASKS_DIR`, etc.)

### Tasks Analytics (`pages/task-tracker-analytics.py`)

Purpose:
- Historical performance view for completed tasks

What it does:
- Uses `utils.get_user_context()` for access
- Loads analytics-ready completed task history
- Filters by:
  - user
  - task
  - cadence
  - partial-completion inclusion
  - date range
- Displays:
  - total tasks
  - total time
  - average time per task
  - tasks per day line chart
  - total hours by cadence
  - top 10 tasks by total hours

Implementation notes:
- Uses Altair
- Expects at least the analytics columns produced by `utils.load_completed_tasks_for_analytics(...)`

### Packaging Estimator (`pages/packaging-estimator.py`)

Purpose:
- Match uploaded items to internal item-info data and estimate shipping / package outcomes using the shipping calculator API

High-level workflow:
1. User provides items via upload or pasted tab-separated rows
2. Page normalizes and validates item numbers and quantities
3. Duplicate items are aggregated
4. Item numbers are matched against the configured item-info parquet
5. Results are split into warehouse-sourced vs drop-ship reference tables
6. User selects a shipping destination
7. Page builds a shipping calculator payload and calls the API
8. Response is summarized into origin cards, item detail, and package allocation detail

Input modes:
- Upload file
  - Excel or CSV
  - sheet selection for Excel
  - header-row detection / override
  - user maps item and quantity columns
- Paste from Excel
  - expects `ItemNumber<TAB>Quantity`

Validation behavior:
- Blank or invalid rows are rejected
- Quantities must parse as integers greater than zero
- Duplicate item numbers are aggregated

Reference-data behavior:
- Item info comes from `config.PACKAGING_CONFIG["item_info"]["parquet_path"]`
- Warehouse lookup comes from `config.PACKAGING_CONFIG["warehouses"]["path"]`

Special shipping features:
- perishable overrides per item
- destination can be:
  - one of company warehouses
  - a manually entered address
- shipping request options include:
  - `HasLiftGate`
  - `ForceCommonCarrier`
  - `ExcludeLiftGateFee`
  - `BypassMatrix`

API behavior:
- Payload is built in `build_shipping_calculator_payload(...)`
- API POST is done with `urllib.request`
- response parsing extracts shipping source candidates, methods, packages, origins, costs, and package allocation estimates

What the UI shows:
- requested / matched / unmatched outcomes
- warehouse-sourced item table
- drop-ship item table
- shipping destination controls
- shipping calculator overview cards by origin
- detailed items shipping from each origin
- request payload and response body expander
- package allocation detail table

Important caveat:
- `pages/packaging-estimator.py` reads runtime API settings from a `shipping_calculator_api` section
- `config.py` currently defines packaging transport settings under `PACKAGING_CONFIG["api"]`
- because of that mismatch, some config values may fall back to hardcoded defaults unless both sides are aligned

Legacy note:
- `CODE - do not open/config.json` appears to be legacy packaging config and is not the current source of truth
- the bottom of the page also contains commented "legacy packager reference" code

### Time Allocation Tool (`pages/time-allocation-tool.py`)

Purpose:
- Capture account-level time allocation by day and export saved results

Main behavior:
- Auto-detects:
  - current login
  - full name
  - department
- Loads account list from personnel data
- Shows a 7-day clickable calendar using `streamlit-calendar`
- Lets user enter row-based allocations in the current implementation's detailed mode
- Confirms before saving
- Writes one parquet file per save
- Replaces prior rows for the same user and date across existing export files

Data schema:
- `Entry Date`
- `User`
- `Full Name`
- `Department`
- `Account`
- `Time`
- `Channel`

Channels:
- `Projects`
- `Resupply`

Admin exports tab:
- date range filter
- user filter
- department filter
- table display
- CSV download

### FedEx Address Validator (`pages/fedex-address-validator.py`)

Purpose:
- Review address-validation results that likely justify residential-fee disputes with FedEx

What it does:
- Loads the configured `results.csv`
- Normalizes invoice dates and tracking numbers
- Automatically filters to:
  - non-disputed rows
  - residential-match statuses needing review (`mismatch`, `mixed`)
- Removes noisy columns for display
- Supports filters for:
  - residential match
  - classification
  - service type
  - invoice date range
- Shows metrics:
  - order count
  - total dispute amount
- Provides actions on currently visible rows:
  - generate dispute Excel file
  - open email draft to FedEx
  - mark displayed rows as disputed

Important behavior:
- "Mark as Disputed" acts on all currently visible rows, not a manually selected subset
- source row identity is preserved through `__source_row_id`
- Excel generation uses `openpyxl` to format currency columns
- Email flow prefers default mail handler, then tries Outlook COM automation

### Stocking Agreement Generator (`pages/stocking-agreement-generator.py`)

Purpose:
- Generate Word and PDF agreement documents from app-managed templates

Tabs:
- `General Resupply`
- `Consumables`

General Resupply workflow:
- collects project, account, client, executive summary
- collects pricing rows with EA Sell and Qty
- computes item subtotal, freight + tax, total order
- collects project details, timeline, documentation requirements, add-on services, and term overrides
- renders a Word agreement and optionally a PDF

Consumables workflow:
- collects project, account, order type
- item summary and purpose
- optional consolidated-order-specific fields
- pricing rows
- timeline and billing information
- renders a Word agreement and optionally a PDF

Template service details:
- `stocking_agreement_service.ensure_templates_ready()` guarantees canonical template files exist
- rendering uses `docxtpl`
- final pricing rows are also injected by direct table-row replacement after template rendering
- PDF conversion uses Microsoft Word COM automation

Canonical templates:
- `CODE - do not open/templates/stocking_agreements/general_resupply_template.docx`
- `CODE - do not open/templates/stocking_agreements/consumables_template.docx`

Template maintenance notes:
- the service can rebuild canonical templates from legacy source templates if they exist
- `scripts/build_stocking_agreement_templates.py` forces a rebuild
- `_cleanup_legacy_template_files()` removes old source/temp artifacts after canonical templates are ready

## 11. Template Details

The agreement generator is more complex than a normal form page, so here are the practical rules:

1. Canonical template files are the real runtime assets.
   The app expects the two `.docx` files in `templates/stocking_agreements/`.

2. Table indexes matter.
   The service references specific Word table positions like `doc.tables[3]`, `doc.tables[5]`, etc. Template structure changes can break rendering even if placeholders still exist.

3. Pricing rows are padded for template rendering.
   `MIN_TEMPLATE_PRICING_ROWS = 6`.

4. PDF generation is optional at runtime.
   If Word COM automation fails, the DOCX still downloads and the page shows a PDF warning instead of hard failing.

5. Placeholder and paragraph matching are literal-ish.
   Functions like `_replace_first_paragraph_containing(...)` depend on recognizable source text.

## 12. Performance And Efficiency Focus

The project is intentionally optimized for internal operational speed more than framework purity.

Current efficiency patterns:
- `@st.cache_data` on file reads, lookup tables, analytics loads, Excel parsing, and warehouse metadata
- `pyarrow.dataset` for large partitioned parquet reads instead of manual per-file loops when possible
- atomic parquet writes via temp file + replace
- small single-user live-activity files instead of a large shared state table
- startup precomputes accounts and users to avoid repeated Excel reads at app runtime
- completed tasks are partitioned by user and date to limit read scope
- task tracker recent activity reads only today's partition
- analytics loader reads only columns needed by that page
- packaging input is normalized and aggregated before any heavy lookup or API call

When adding features, try to preserve this style:
- cache reads
- write atomically
- minimize columns
- minimize scan scope
- avoid repeated network file reads in the main rerun path

## 13. Known Quirks And Gotchas

1. `config.py` is in `.gitignore` and auto-synced from the network share on each launch via `StartApp.bat`.
   The repo is public so config.py must never be committed.

2. `config.json` is effectively legacy.
   Do not assume packaging settings come from there.

3. `scripts/validate_lake.py` looks stale.
   It references older schema names such as `UserName` instead of `UserLogin`.

4. The packaging page contains more helper infrastructure than the visible UI currently exposes.
   There are simulation and recommendation helpers that appear ready for extension, but the present workflow centers on `Load Items` and `Run Estimate`.

5. The app assumes Windows.
   Batch files, Outlook automation, and Word-to-PDF conversion are Windows-specific.

6. Shared-path access is not optional.
   Many pages will appear broken if the UNC paths or synced SharePoint data are unavailable.

## 14. How To Make Common Changes Safely

### Add a new page
1. Create `CODE - do not open/pages/<new-page>.py`
2. Follow the standard page pattern:
   - `st.set_page_config(..., page_icon=utils.get_app_icon())`
   - `utils.render_app_logo()`
   - `st.markdown(utils.get_global_css(), unsafe_allow_html=True)`
   - `utils.render_page_header(PAGE_TITLE)`
   - `LOGGER = utils.get_page_logger(...)`
3. Register it in `page_registry.py`
4. If admin-only, set `admin_only=True`
5. Add the correct page-level permission check inside the page too

### Change navigation labels, icons, captions, or quotes
- Edit `page_registry.py`
- Do not hardcode duplicate titles across pages if the registry is already the source of truth

### Change shared visual styling
- Prefer `utils.get_global_css()`
- Avoid page-by-page CSS duplication unless the page truly needs a custom one-off treatment

### Add a new persisted field to task records
1. Update `utils.PARQUET_SCHEMA`
2. Update `build_task_record(...)`
3. Update task-log loaders and editors if the field should be visible/admin-editable
4. Update analytics loaders if the field matters there
5. Verify older parquet files still load gracefully

### Change admin permissions
- The current admin source of truth is `users.parquet`
- `utils.is_user_admin(...)` looks for flexible aliases and role-like columns
- Keep alias tolerance unless you are also locking the upstream schema

### Change stocking agreement templates
1. Preserve table ordering unless you also update `stocking_agreement_service.py`
2. Preserve or intentionally update placeholder text / paragraph anchors
3. Re-test both General Resupply and Consumables
4. Re-test DOCX and PDF generation

## 15. Manual Test Checklist

There are no obvious automated tests in this repo, so use manual smoke tests after meaningful edits.

Baseline checks:
- `setup.bat` still installs successfully
- `StartApp.bat` still launches the app
- home page navigation still works
- page titles and quotes still render correctly
- logs still write to the expected user folder

Task workflow checks:
- task tracker start / pause / resume / end / upload works
- live activity appears and clears correctly
- archived paused task can be resumed and deleted
- analytics still loads historical data

Admin workflow checks:
- management page loads tasks, targets, users, and task log
- task definition updates persist
- task target edits persist
- task log edits and deletes persist correctly
- admin logs page still parses log lines

Packaging checks:
- upload mode works for Excel and CSV
- paste mode still parses tab-separated rows
- unmatched items are reported
- destination selection works
- estimate runs and response renderers do not crash

FedEx checks:
- validator filters still work
- dispute file generates
- email draft flow still opens
- mark disputed / clear disputed workflows still persist

Agreement checks:
- both tabs generate DOCX
- PDF either generates or fails gracefully with warning

## 16. Quick Reference: Best Files To Read First

If you need to understand or change the app quickly, start here:
- `config.py`
- `CODE - do not open/app.py`
- `CODE - do not open/page_registry.py`
- `CODE - do not open/utils.py`
- `CODE - do not open/pages/task-tracker.py`
- `CODE - do not open/pages/tasks-management.py`
- `CODE - do not open/pages/packaging-estimator.py`
- `CODE - do not open/stocking_agreement_service.py`

## 17. Bottom Line

This project is not a generic website. It is an internal operations app built around shared parquet data, UNC paths, Streamlit reruns, and a handful of critical workflows that people likely use every day.

The safest way to modify it is:
- keep navigation centralized in `page_registry.py`
- keep styling centralized in `utils.py`
- preserve parquet schemas and write paths
- clear caches after writes
- verify each affected page manually
- be especially careful with `tasks.parquet`, `users.parquet`, `taskstargets.csv`, and the Word templates
