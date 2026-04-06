@echo off
setlocal EnableDelayedExpansion
title Logistics Support App

REM ============================================================
REM ROOT / PATHS
REM ============================================================
set "CODE_DIR=%~dp0"
if "%CODE_DIR:~-1%"=="\" set "CODE_DIR=%CODE_DIR:~0,-1%"
for %%i in ("%CODE_DIR%\..") do set "ROOT_DIR=%%~fi"
set "APP_FILE=%CODE_DIR%\app.py"
set "STARTUP_FILE=%CODE_DIR%\startup.py"

set "VENV_DIR=%ROOT_DIR%\.venv"
set "STREAMLIT_PORT=8501"
cd /d "%ROOT_DIR%"

if defined PYTHONPATH (
  set "PYTHONPATH=%ROOT_DIR%;%CODE_DIR%;%PYTHONPATH%"
) else (
  set "PYTHONPATH=%ROOT_DIR%;%CODE_DIR%"
)
set "STREAMLIT_CONFIG_DIR=%CODE_DIR%\.streamlit"

REM Skip pause commands when called with --no-launch (hidden window from .exe)
set "SILENT=0"
if "%~1"=="--no-launch" set "SILENT=1"

set "LOG_BASE=\\therestaurantstore.com\920\Data\Logistics\Logistics App\Logs"
set "LOG_DIR=%LOG_BASE%\%USERNAME%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1
if not exist "%LOG_DIR%" (
  set "LOG_DIR=%ROOT_DIR%\logs\%USERNAME%"
  if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
)
set "LOG_FILE=%LOG_DIR%\StartApp.log"

REM ============================================================
REM LOG HEADER
REM ============================================================
(
  echo ============================================================
  echo [%date% %time%] [Launcher] Session start
  echo ROOT_DIR=%ROOT_DIR%
  echo CODE_DIR=%CODE_DIR%
  echo VENV_DIR=%VENV_DIR%
  echo PORT=%STREAMLIT_PORT%
  echo ============================================================
) >> "%LOG_FILE%"

REM ============================================================
REM VALIDATION
REM ============================================================
if not exist "%APP_FILE%" (
  echo [%date% %time%] [Launcher] ERROR: app.py not found>> "%LOG_FILE%"
  echo ERROR: app.py not found in CODE directory.
  if "%SILENT%"=="0" pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo [%date% %time%] [Launcher] ERROR: venv missing>> "%LOG_FILE%"
  echo ERROR: Virtual environment not found. Run setup.bat first.
  if "%SILENT%"=="0" pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

REM ============================================================
REM DECRYPT CONFIG (FAIL-OPEN — keeps existing config.py if decrypt fails)
REM ============================================================
set "LOCAL_CONFIG=%ROOT_DIR%\config.py"
set "CONFIG_ENC=%CODE_DIR%\config.enc"
set "KEY_FILE=%CODE_DIR%\config.key"
set "NETWORK_KEY=\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.key"

REM Copy key from network share if missing locally
if not exist "%KEY_FILE%" (
  if exist "%NETWORK_KEY%" (
    call :LOG "Copying config key from network share..."
    copy /Y "%NETWORK_KEY%" "%KEY_FILE%" >nul 2>&1
  )
)

if exist "%CONFIG_ENC%" (
  if exist "%KEY_FILE%" (
    call :LOG "Decrypting config..."
    "%VENV_DIR%\Scripts\python.exe" "%CODE_DIR%\config_manager.py" decrypt >> "%LOG_FILE%" 2>&1
    if errorlevel 1 (
      call :LOG "WARNING: Config decryption failed. Using existing local copy."
    ) else (
      call :LOG "Config decrypted successfully."
    )
  ) else (
    call :LOG "WARNING: Config key not found. Using existing local copy."
  )
) else (
  call :LOG "WARNING: config.enc not found. Using existing local copy."
)

if not exist "%LOCAL_CONFIG%" (
  echo ERROR: config.py not found and could not be decrypted.
  call :LOG "ERROR: config.py missing and decryption unavailable."
  if "%SILENT%"=="0" pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

REM ============================================================
REM BACKGROUND GIT UPDATE CHECK
REM ============================================================
call :LOG "Starting background update check..."
start /B "" "%VENV_DIR%\Scripts\pythonw.exe" "%CODE_DIR%\check_updates.py"

REM ============================================================
REM RUN STARTUP.PY
REM ============================================================
if not exist "%STARTUP_FILE%" (
  call :LOG "ERROR: startup.py not found."
  echo ERROR: startup.py not found in CODE directory.
  if "%SILENT%"=="0" pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

call :LOG "Running startup.py..."
set "STARTUP_CALLER=StartApp.bat"
"%VENV_DIR%\Scripts\python.exe" "%STARTUP_FILE%" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  call :LOG "ERROR: startup.py failed."
  echo ERROR: startup.py failed. Check logs for details.
  if "%SILENT%"=="0" pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)
call :LOG "startup.py completed."

REM ============================================================
REM LAUNCH APP (unless --no-launch was passed by the .exe stub)
REM ============================================================
if "%~1"=="--no-launch" (
  call :LOG "Setup complete (no-launch mode, caller will handle app launch)."
  echo.>> "%LOG_FILE%"
  exit /b 0
)

call :LOG "Launching app..."
start "" "%VENV_DIR%\Scripts\pythonw.exe" "%CODE_DIR%\launch_app.py"
call :LOG "App launched."
echo.>> "%LOG_FILE%"
exit /b 0

REM ============================================================
REM LOG FUNCTION
REM ============================================================
:LOG
echo [%date% %time%] [Launcher] %~1>> "%LOG_FILE%"
echo %~1
exit /b 0
