# Setup & Local Use

## Requirements (one-time)
- **Python 3.11.x** installed system-wide and on PATH.
- **Git for Windows** installed and on PATH.

If any of these are missing, contact IT.

## First-Time Setup (one-time)
1. Create a folder anywhere on your machine (recommended: `C:\\Users\\yourusername\\TaskTracker`).
2. Copy these files into that folder:
   - `setup.bat`
   - `Start_Task_Tracker.bat`
3. Double-click `setup.bat`.

This will:
- Download the app from GitHub
- Create a local Python environment
- Install all required dependencies

## Running the application
To start the app, double-click `Start_Task_Tracker.bat`.

On every launch, the app:
- Checks for updates automatically
- Starts the application
- Opens your browser

## Secrets file (required)
Each user must have this file:
`App.streamlit\\secrets.toml`

This file is local only and not shared.
