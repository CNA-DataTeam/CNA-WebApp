@echo off
setlocal EnableDelayedExpansion
title CNA Console - Repair

REM ============================================================
REM CNA Console -- Repair App
REM
REM Effectively "reinstall in place." Triggered by the in-app
REM Settings > Repair App button, or runnable manually by
REM double-clicking this file. Designed to recover from:
REM   - Local git changes blocking pull (modify-vs-pull conflicts)
REM   - Missing / AV-quarantined launcher exe or _internal/
REM   - Corrupted venv or missing pip packages
REM   - Stale config.py out of sync with config.enc
REM   - Missing config.key on local disk
REM   - Corrupted __pycache__ / stale update markers
REM   - User stuck on a commit that was later fixed upstream
REM
REM Output is teed to repair.log at the project root. Step
REM headers echo to the console so the user sees live progress.
REM Safe to run repeatedly. Does NOT touch config.py, config.key,
REM .venv, favorites.json, or any other gitignored local state.
REM ============================================================

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "LOG=%ROOT%\repair.log"
set "EXE=%ROOT%\CNA Web App.exe"

echo === CNA Console Repair started: %date% %time% === > "%LOG%"

REM ------------------------------------------------------------
REM Sanity checks: this must be a git working tree with git on PATH.
REM ------------------------------------------------------------
if not exist "%ROOT%\.git" (
  call :STEP "ERROR: This folder is not a git repository."
  call :STEP "Repair only works inside a CNA-WebApp clone."
  echo.
  pause
  exit /b 1
)
where git >nul 2>&1
if errorlevel 1 (
  call :STEP "ERROR: 'git' was not found on PATH."
  call :STEP "Install Git from https://git-scm.com or re-run the installer."
  echo.
  pause
  exit /b 1
)

REM ------------------------------------------------------------
REM Close the running app so the launcher exe is unlocked. No-op
REM if the user ran this manually from Explorer.
REM ------------------------------------------------------------
call :STEP "Closing the running app..."
taskkill /F /IM "CNA Web App.exe" >nul 2>&1

set /a _waited=0
:WAIT_FOR_EXIT
tasklist /FI "IMAGENAME eq CNA Web App.exe" 2>nul | findstr /I "CNA Web App.exe" >nul
if not errorlevel 1 (
  timeout /t 1 /nobreak >nul
  set /a _waited+=1
  if !_waited! GEQ 30 (
    call :STEP "WARNING: Launcher still running after 30s. Continuing anyway."
    goto :AFTER_WAIT
  )
  goto WAIT_FOR_EXIT
)
:AFTER_WAIT
call :STEP "App closed."

REM ------------------------------------------------------------
REM Reset git state to the latest known-good code.
REM   - origin/main when online (most-fixed version)
REM   - HEAD when offline (last-known-good local commit)
REM ------------------------------------------------------------
call :STEP "Fetching latest from GitHub..."
git -C "%ROOT%" fetch --prune >> "%LOG%" 2>&1
if errorlevel 1 (
  call :STEP "(offline -- resetting working tree to last known good commit)"
  git -C "%ROOT%" reset --hard HEAD >> "%LOG%" 2>&1
) else (
  call :STEP "Resetting working tree to origin/main..."
  git -C "%ROOT%" reset --hard origin/main >> "%LOG%" 2>&1
)

REM ------------------------------------------------------------
REM Clear caches and stale markers.
REM ------------------------------------------------------------
call :STEP "Clearing __pycache__ and stale update markers..."
for /d /r "%ROOT%" %%d in (__pycache__) do @if exist "%%d" rmdir /s /q "%%d" 2>nul
del /q "%ROOT%\CODE - do not open\.update_available" 2>nul
del /q "%ROOT%\CODE - do not open\.last_update_check" 2>nul

REM ------------------------------------------------------------
REM Run setup.bat in silent mode. setup.bat handles:
REM   - venv creation / package install (uv)
REM   - config.key copy + config.enc decrypt
REM   - PyInstaller rebuild of CNA Web App.exe + _internal/ when
REM     either is missing (per the SKIP_BUILD condition)
REM   - launcher artifact verification
REM ------------------------------------------------------------
call :STEP "Running setup (rebuilds launcher + venv as needed)..."
echo. >> "%LOG%"
echo === setup.bat output === >> "%LOG%"
call "%ROOT%\setup.bat" /silent >> "%LOG%" 2>&1
set "SETUP_RC=!errorlevel!"
if not "!SETUP_RC!"=="0" (
  call :STEP "ERROR: setup.bat exited with code !SETUP_RC!."
  call :STEP "Repair stopped. See repair.log next to this window."
  call :STEP "The app was NOT relaunched -- start it manually after fixing the issue."
  echo.
  pause
  exit /b !SETUP_RC!
)

REM ------------------------------------------------------------
REM Relaunch and exit. Brief delay so the user sees completion
REM before the console window auto-closes.
REM ------------------------------------------------------------
call :STEP "Relaunching CNA Console..."
start "" "%EXE%"

call :STEP "=== Repair complete ==="
timeout /t 5 >nul
exit /b 0

REM ------------------------------------------------------------
REM :STEP -- echo a message to both the console and the log.
REM ------------------------------------------------------------
:STEP
echo %~1
echo [%date% %time%] %~1 >> "%LOG%"
exit /b 0
