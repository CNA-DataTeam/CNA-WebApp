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
set "BUILD_DIST=%CODE_DIR%\installer\dist"

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
REM REMOVE OLD BUILD ARTIFACTS
REM ============================================================
if exist "%EXE_FILE%" (
  echo Removing old exe...
  del /f "%EXE_FILE%" >nul 2>&1
)
if exist "%ROOT_DIR%\_internal" (
  echo Removing old _internal...
  rmdir /s /q "%ROOT_DIR%\_internal" >nul 2>&1
)
if exist "%BUILD_DIST%" (
  rmdir /s /q "%BUILD_DIST%" >nul 2>&1
)
REM Remove old spec file so PyInstaller doesn't reuse stale onefile config
if exist "%CODE_DIR%\installer\CNA Web App.spec" (
  del /f "%CODE_DIR%\installer\CNA Web App.spec" >nul 2>&1
)

REM ============================================================
REM BUILD (onedir — no temp extraction, much faster cold start)
REM ============================================================
echo Building CNA Web App.exe (onedir mode)...
"%VENV_DIR%\Scripts\pyinstaller.exe" --onedir --noconsole --icon="%ICON_FILE%" --name="CNA Web App" --distpath="%BUILD_DIST%" --specpath="%CODE_DIR%\installer" --workpath="%CODE_DIR%\installer\build" "%STUB_FILE%"

REM Move exe and _internal from build subdir to project root
if exist "%BUILD_DIST%\CNA Web App\CNA Web App.exe" (
  echo Moving build output to project root...
  move /Y "%BUILD_DIST%\CNA Web App\CNA Web App.exe" "%ROOT_DIR%\"
  if errorlevel 1 (
    echo ERROR: Failed to move exe to project root.
    pause
    exit /b 1
  )
  REM Use xcopy for _internal — more reliable than move for directories on OneDrive
  xcopy /E /I /Y /Q "%BUILD_DIST%\CNA Web App\_internal" "%ROOT_DIR%\_internal"
  if errorlevel 1 (
    echo ERROR: Failed to copy _internal to project root.
    pause
    exit /b 1
  )
  rmdir /s /q "%BUILD_DIST%" 2>nul
) else (
  echo.
  echo ERROR: Build failed. Check the output above for details.
  pause
  exit /b 1
)

if not exist "%EXE_FILE%" (
  echo.
  echo ERROR: Build failed. exe not found after move.
  pause
  exit /b 1
)

if not exist "%ROOT_DIR%\_internal" (
  echo.
  echo ERROR: Build failed. _internal folder not found after copy.
  pause
  exit /b 1
)

echo.
echo ============================================
echo Build complete: CNA Web App.exe + _internal/
echo ============================================
pause
exit /b 0
