@echo off
setlocal EnableDelayedExpansion
title Rebuild CNA Web App.exe

REM ============================================================
REM PATHS
REM ============================================================
set "ROOT_DIR=%~dp0"
if "%ROOT_DIR:~-1%"=="\" set "ROOT_DIR=%ROOT_DIR:~0,-1%"
set "CODE_DIR=%ROOT_DIR%\CODE - do not open"
set "VENV_DIR=%ROOT_DIR%\.venv"
set "ICON_FILE=%ROOT_DIR%\cna_icon.ico"
set "STUB_FILE=%CODE_DIR%\stub_launcher.py"
set "EXE_FILE=%ROOT_DIR%\CNA Web App.exe"

REM ============================================================
REM VALIDATE VENV
REM ============================================================
if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo ERROR: Virtual environment not found. Run setup.bat first.
  pause
  exit /b 1
)

if not exist "%STUB_FILE%" (
  echo ERROR: stub_launcher.py not found at %STUB_FILE%
  pause
  exit /b 1
)

REM ============================================================
REM ENSURE PYINSTALLER IS INSTALLED
REM ============================================================
echo Checking for PyInstaller...
"%VENV_DIR%\Scripts\python.exe" -c "import PyInstaller" >nul 2>&1
if errorlevel 1 (
  echo PyInstaller not found. Installing...
  "%VENV_DIR%\Scripts\pip.exe" install pyinstaller >nul 2>&1
  if errorlevel 1 (
    echo Trying with uv...
    where uv >nul 2>&1
    if not errorlevel 1 (
      set "VIRTUAL_ENV=%VENV_DIR%"
      uv pip install pyinstaller
    ) else (
      echo ERROR: Could not install PyInstaller.
      pause
      exit /b 1
    )
  )
  "%VENV_DIR%\Scripts\python.exe" -c "import PyInstaller" >nul 2>&1
  if errorlevel 1 (
    echo ERROR: PyInstaller installation failed.
    pause
    exit /b 1
  )
  echo PyInstaller installed successfully.
) else (
  echo PyInstaller is already installed.
)

REM ============================================================
REM REMOVE OLD EXE
REM ============================================================
if exist "%EXE_FILE%" (
  echo Removing old exe...
  del /f "%EXE_FILE%" >nul 2>&1
)

REM ============================================================
REM BUILD
REM ============================================================
echo Building CNA Web App.exe...
"%VENV_DIR%\Scripts\pyinstaller.exe" --onefile --noconsole --icon="%ICON_FILE%" --name="CNA Web App" --distpath="%ROOT_DIR%" --specpath="%CODE_DIR%\installer" --workpath="%CODE_DIR%\installer\build" "%STUB_FILE%"

if not exist "%EXE_FILE%" (
  echo.
  echo ERROR: Build failed. Check the output above for details.
  pause
  exit /b 1
)

echo.
echo ============================================
echo Build complete: CNA Web App.exe
echo ============================================
pause
exit /b 0
