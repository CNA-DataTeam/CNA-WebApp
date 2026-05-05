@echo off
REM ============================================================
REM Repair.bat — One-shot rescue for a stuck CNA Web App install
REM ============================================================
REM Use this when the in-app updater fails to apply updates because
REM build artifacts in this clone diverge from origin (typically
REM seen on developer machines that ran RebuildExe.bat locally).
REM
REM What it does:
REM   1. Discards local modifications to known-regenerated tracked
REM      files (the launcher exe and PyInstaller spec).
REM   2. Runs `git pull --ff-only` to bring this clone up to date.
REM   3. Reports success or shows the underlying git error.
REM
REM Safe to run repeatedly. Does NOT touch config.py, config.key,
REM .venv, or any other local-only / gitignored files.
REM ============================================================

setlocal EnableDelayedExpansion
title CNA Web App - Repair

set "ROOT_DIR=%~dp0"
if "%ROOT_DIR:~-1%"=="\" set "ROOT_DIR=%ROOT_DIR:~0,-1%"

echo.
echo ============================================================
echo CNA Web App - Repair
echo ============================================================
echo Working directory:
echo   %ROOT_DIR%
echo.

REM Verify this is a git working tree
if not exist "%ROOT_DIR%\.git" (
  echo ERROR: This folder is not a git repository.
  echo Repair.bat only works inside a CNA-WebApp clone.
  echo.
  pause
  exit /b 1
)

REM Verify git is on PATH
where git >nul 2>&1
if errorlevel 1 (
  echo ERROR: 'git' was not found on PATH.
  echo Install Git from https://git-scm.com or re-run the installer.
  echo.
  pause
  exit /b 1
)

cd /d "%ROOT_DIR%"

echo Step 1/2: Discarding local changes to regenerated build artifacts...
git checkout -- "CNA Web App.exe" 2>nul
git checkout -- "CODE - do not open\installer\CNA Web App.spec" 2>nul
echo   done.
echo.

echo Step 2/2: Pulling latest from origin...
git pull --ff-only
set "PULL_RC=%ERRORLEVEL%"
echo.

if "%PULL_RC%"=="0" (
  echo ============================================================
  echo Repair complete. The app should now update normally on next launch.
  echo ============================================================
) else (
  echo ============================================================
  echo Pull failed with exit code %PULL_RC%. See the error above.
  echo If the message mentions 'local changes would be overwritten',
  echo a different file is dirty. Share the output with the dev team.
  echo ============================================================
)

echo.
pause
exit /b %PULL_RC%
