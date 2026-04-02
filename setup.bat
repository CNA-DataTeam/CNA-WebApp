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

REM Refresh PATH from registry so the installer's changes take effect in this session
for /f "tokens=2*" %%a in ('reg query "HKCU\Environment" /v PATH 2^>nul') do set "USER_PATH=%%b"
if defined USER_PATH set "PATH=%PATH%;%USER_PATH%"

if exist "%LOCALAPPDATA%\uv\bin\uv.exe" (
  set "UV_EXE=%LOCALAPPDATA%\uv\bin\uv.exe"
  goto UV_FOUND
)
if exist "%APPDATA%\uv\bin\uv.exe" (
  set "UV_EXE=%APPDATA%\uv\bin\uv.exe"
  goto UV_FOUND
)

where uv >nul 2>&1
if not errorlevel 1 (
  set "UV_EXE=uv"
  goto UV_FOUND
)

for /f "delims=" %%i in ('where /R "%USERPROFILE%" uv.exe 2^>nul') do (
  set "UV_EXE=%%i"
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
"%UV_EXE%" python list 2>&1 | findstr /C:"cpython-3.11" >nul
if not errorlevel 1 (
  echo Python 3.11 already installed. Skipping.
  goto VENV
)
echo Installing Python 3.11...
"%UV_EXE%" python install 3.11
if errorlevel 1 (
  echo ERROR: Failed to install Python 3.11.
  pause
  exit /b 1
)

REM ============================================================
REM CREATE VENV (IF MISSING)
REM ============================================================
:VENV
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
"%UV_EXE%" pip install --link-mode copy -r "%REQ_FILE%"
if errorlevel 1 (
  echo ERROR: Dependency installation failed.
  pause
  exit /b 1
)

REM ============================================================
REM CREATE DESKTOP SHORTCUT
REM ============================================================
set "SHORTCUT_PATH=%ROOT_DIR%\CNA Web App.lnk"
set "ICON_FILE=%ROOT_DIR%\cna_icon.ico"

echo Creating shortcut...
powershell -Command "$s=(New-Object -COM WScript.Shell).CreateShortcut('%SHORTCUT_PATH%'); $s.TargetPath='wscript.exe'; $s.Arguments='\"%ROOT_DIR%\StartApp.vbs\"'; $s.WorkingDirectory='%ROOT_DIR%'; if(Test-Path '%ICON_FILE%'){$s.IconLocation='%ICON_FILE%,0'}; $s.WindowStyle=1; $s.Save()" >nul 2>&1
if exist "%SHORTCUT_PATH%" (
  echo Shortcut created: CNA Web App.lnk
  echo Pin it to your taskbar by double-clicking it, then right-clicking its taskbar icon.
) else (
  echo WARNING: Could not create shortcut. You can still run StartApp.bat directly.
)

REM ============================================================
REM DONE
REM ============================================================
echo.
echo ============================================
echo Setup complete.
echo Run StartApp.vbs to launch the application.
echo ============================================
pause
exit /b 0
