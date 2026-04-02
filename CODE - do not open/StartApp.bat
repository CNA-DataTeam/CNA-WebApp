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
  pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo [%date% %time%] [Launcher] ERROR: venv missing>> "%LOG_FILE%"
  echo ERROR: Virtual environment not found. Run setup.bat first.
  pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

REM ============================================================
REM EDGE PROFILE SETUP
REM ============================================================
set "EDGE_PROFILE=CNA-WebApp-Edge"
set "EDGE_DATA=%TEMP%\%EDGE_PROFILE%"
set "EDGE_FLAGS=--user-data-dir="%EDGE_DATA%" --no-first-run --disable-features=msEdgeOnRampFRE"

REM ============================================================
REM CHECK IF ALREADY RUNNING
REM ============================================================
netstat -ano | findstr /R /C:":%STREAMLIT_PORT% .*LISTENING" >nul
if %ERRORLEVEL%==0 (
  call :LOG "App already running. Opening browser."
  start "" msedge --app="http://localhost:%STREAMLIT_PORT%" %EDGE_FLAGS%
  echo.>> "%LOG_FILE%"
  exit /b 0
)

REM ============================================================
REM OPEN SPLASH SCREEN IMMEDIATELY
REM ============================================================
if exist "%CODE_DIR%\splash.html" (
  call :LOG "Opening splash screen..."
  start "" msedge --app="file:///%CODE_DIR:\=/%/splash.html" %EDGE_FLAGS%
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
  pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

REM ============================================================
REM BACKGROUND GIT UPDATE CHECK
REM ============================================================
call :LOG "Starting background update check..."
start /B "" "%VENV_DIR%\Scripts\pythonw.exe" "%CODE_DIR%\check_updates.py"

REM ============================================================
REM LAUNCH STREAMLIT
REM ============================================================
call :LOG "Launching Streamlit..."

"%VENV_DIR%\Scripts\python.exe" -c "import streamlit" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  call :LOG "ERROR: Streamlit not installed."
  echo ERROR: Streamlit missing. Run setup.bat.
  pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

if not exist "%STARTUP_FILE%" (
  call :LOG "ERROR: startup.py not found."
  echo ERROR: startup.py not found in CODE directory.
  pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

call :LOG "Running startup.py..."
set "STARTUP_CALLER=StartApp.bat"
"%VENV_DIR%\Scripts\python.exe" "%STARTUP_FILE%" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  call :LOG "ERROR: startup.py failed."
  echo ERROR: startup.py failed. Check logs for details.
  pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)
call :LOG "startup.py completed."

call :LOG "Starting server..."
start "" /B "%VENV_DIR%\Scripts\pythonw.exe" -m streamlit run "%APP_FILE%" ^
  --server.port=%STREAMLIT_PORT% ^
  --server.headless=true ^
  --browser.gatherUsageStats=false ^
  >> "%LOG_FILE%" 2>&1

REM If splash wasn't available earlier, open app directly now
if not exist "%CODE_DIR%\splash.html" (
  timeout /t 2 >nul
  call :LOG "Opening app..."
  start "" msedge --app="http://localhost:%STREAMLIT_PORT%" %EDGE_FLAGS%
)

REM Wait for Edge to start, then poll until all its windows are closed
call :LOG "Monitoring app window..."
timeout /t 5 >nul
:WAIT_CLOSE
timeout /t 3 >nul
wmic process where "name='msedge.exe' and commandline like '%%%EDGE_PROFILE%%%'" get processid 2>nul | findstr /r "[0-9]" >nul
if not errorlevel 1 goto WAIT_CLOSE

REM Edge closed — stop Streamlit
call :LOG "App window closed. Stopping server..."
for /f "tokens=5" %%p in ('netstat -ano ^| findstr /R ":%STREAMLIT_PORT% .*LISTENING" 2^>nul') do (
  taskkill /F /PID %%p >nul 2>&1
)
call :LOG "Server stopped."
echo.>> "%LOG_FILE%"
exit /b 0

REM ============================================================
REM LOG FUNCTION
REM ============================================================
:LOG
echo [%date% %time%] [Launcher] %~1>> "%LOG_FILE%"
echo %~1
exit /b 0
