@echo off
setlocal EnableDelayedExpansion
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
REM FIND OR INSTALL UV
REM ============================================================
set "UV_EXE="

where uv >nul 2>&1
if not errorlevel 1 (
  set "UV_EXE=uv"
  goto UV_FOUND
)

if exist "%LOCALAPPDATA%\uv\bin\uv.exe" (
  set "UV_EXE=%LOCALAPPDATA%\uv\bin\uv.exe"
  goto UV_FOUND
)

if exist "%APPDATA%\uv\bin\uv.exe" (
  set "UV_EXE=%APPDATA%\uv\bin\uv.exe"
  goto UV_FOUND
)

echo uv not found. Installing automatically...
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
if errorlevel 1 (
  echo ERROR: Failed to install uv. Check your internet connection and try again.
  pause
  exit /b 1
)

if exist "%LOCALAPPDATA%\uv\bin\uv.exe" (
  set "UV_EXE=%LOCALAPPDATA%\uv\bin\uv.exe"
  goto UV_FOUND
)
if exist "%APPDATA%\uv\bin\uv.exe" (
  set "UV_EXE=%APPDATA%\uv\bin\uv.exe"
  goto UV_FOUND
)

echo ERROR: uv was installed but could not be located. Please restart this script.
pause
exit /b 1

:UV_FOUND
echo Found uv: %UV_EXE%

REM ============================================================
REM ENSURE PYTHON 3.11
REM ============================================================
echo Ensuring Python 3.11 is available...
"%UV_EXE%" python install 3.11
if errorlevel 1 (
  echo ERROR: Failed to install Python 3.11.
  pause
  exit /b 1
)

REM ============================================================
REM CREATE VENV (IF MISSING)
REM ============================================================
if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo Creating virtual environment...
  "%UV_EXE%" venv "%VENV_DIR%" --python 3.11
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
set "VIRTUAL_ENV=%VENV_DIR%"
"%UV_EXE%" pip install -r "%REQ_FILE%"
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
