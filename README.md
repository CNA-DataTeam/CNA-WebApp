# CNA WebApp

Internal Streamlit application for Clark National Accounts logistics and data workflows.

## What It Includes
- Task tracking with live activity broadcasting (Logistics Support and Data & Analytics versions)
- Task analytics and performance dashboards
- Task and user administration
- Packaging estimation with shipping calculator API
- Time allocation entry and export
- FedEx address validation review
- Stocking agreement generation

## Quick Start (New Install)

**Option A — Installer (recommended):**
1. Download and run `CNA-WebApp-Setup.exe`
2. Follow the prompts — Git, Python, and all dependencies are installed automatically
3. Open the app using the **CNA Web App** shortcut

**Option B — Manual:**
1. Install [Git](https://git-scm.com)
2. Clone the repo: `git clone https://github.com/CNA-DataTeam/CNA-WebApp.git`
3. Run `setup.bat` once (installs uv, Python 3.11, virtual environment, dependencies, and creates a shortcut)
4. Copy `config.py` from `\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.py` into the repo root
5. Run `StartApp.bat` to launch the app (config.py is auto-synced from the network share on each launch)

## Updating
App updates are pulled automatically from GitHub each time `StartApp.bat` runs. No manual action needed.

## Utility Scripts
- `StartApp.bat` — Syncs config, pulls latest code, launches the app in Edge app mode
- `setup.bat` — First-time setup (also safe to re-run)
- `ForceCloseApp.bat` — Stops the running Streamlit process

## Notes
- The app opens in Microsoft Edge as a standalone window (no browser tabs/address bar)
- The application depends on internal shared files, network locations, and synced business data
- Main source code lives in `CODE - do not open/`
- Maintainer and AI change instructions live in `README_AI.md`
