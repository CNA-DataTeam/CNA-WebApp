@echo off
setlocal
title Logistics Support App Setup

REM ============================================================
REM ROOT / PATHS
REM ============================================================
set "ROOT_DIR=%~dp0"
if "%ROOT_DIR:~-1%"=="\" set "ROOT_DIR=%ROOT_DIR:~0,-1%"
set "CODE_DIR=%ROOT_DIR%\CODE - do not open"

set "VENV_DIR=%ROOT_DIR%\.venv"
set "REQ_FILE=%CODE_DIR%\requirements.txt"

REM ============================================================
REM VALIDATE PYTHON
REM ============================================================
where python >nul 2>&1
if errorlevel 1 (
  echo ERROR: Python not found in PATH.
  pause
  exit /b 1
)

for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set "PY_VER=%%v"
for /f "tokens=1,2 delims=." %%a in ("%PY_VER%") do (
  set "PY_MAJOR=%%a"
  set "PY_MINOR=%%b"
)
if not "%PY_MAJOR%"=="3" (
  echo WARNING: Python 3.11 is required. Found Python %PY_VER%.
  echo The app may not work correctly. Press any key to continue anyway, or close this window to cancel.
  pause
  goto VENV
)
if %PY_MINOR% LSS 11 (
  echo WARNING: Python 3.11 is required. Found Python %PY_VER%.
  echo The app may not work correctly. Press any key to continue anyway, or close this window to cancel.
  pause
)
if %PY_MINOR% GTR 11 (
  echo WARNING: Python 3.11 is required. Found Python %PY_VER%.
  echo The app may not work correctly. Press any key to continue anyway, or close this window to cancel.
  pause
)

:VENV

REM ============================================================
REM CREATE VENV (IF MISSING)
REM ============================================================
if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo Creating virtual environment...
  python -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo ERROR: Failed to create virtual environment.
    pause
    exit /b 1
  )
) else (
  echo Virtual environment already exists.
)

REM ============================================================
REM INSTALL DEPENDENCIES
REM ============================================================
if not exist "%REQ_FILE%" (
  echo ERROR: requirements.txt not found in CODE directory.
  pause
  exit /b 1
)

echo Installing dependencies...
"%VENV_DIR%\Scripts\python.exe" -m pip install --upgrade pip
"%VENV_DIR%\Scripts\python.exe" -m pip install -r "%REQ_FILE%"

if errorlevel 1 (
  echo ERROR: Dependency installation failed.
  pause
  exit /b 1
)

REM ============================================================
REM DONE
REM ============================================================
echo.
echo ============================================
echo Setup complete.
echo Run StartApp.bat to launch the application.
echo ============================================
pause
exit /b 0
