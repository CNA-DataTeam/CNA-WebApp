@echo off
setlocal EnableDelayedExpansion
title Logistics Support App

REM ============================================================
REM ROOT / PATHS
REM ============================================================
set "ROOT_DIR=%~dp0"
if "%ROOT_DIR:~-1%"=="\" set "ROOT_DIR=%ROOT_DIR:~0,-1%"
set "CODE_DIR=%ROOT_DIR%\CODE - do not open"
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
REM SYNC CONFIG FROM NETWORK (FAIL-OPEN)
REM ============================================================
set "NETWORK_CONFIG=\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.py"
set "LOCAL_CONFIG=%ROOT_DIR%\config.py"

call :LOG "Syncing config.py from network..."
if exist "%NETWORK_CONFIG%" (
  copy /Y "%NETWORK_CONFIG%" "%LOCAL_CONFIG%" >nul 2>&1
  if errorlevel 1 (
    call :LOG "WARNING: config.py copy failed. Using existing local copy."
  ) else (
    call :LOG "config.py updated from network."
  )
) else (
  call :LOG "WARNING: Network config not reachable. Using existing local copy."
)

if not exist "%LOCAL_CONFIG%" (
  echo ERROR: config.py not found and network is unreachable. Cannot start app.
  call :LOG "ERROR: config.py missing and network unreachable."
  pause
  echo.>> "%LOG_FILE%"
  exit /b 1
)

REM ============================================================
REM OPTIONAL GIT UPDATE (FAIL-OPEN)
REM ============================================================
call :LOG "Starting Git check..."

where git >nul 2>&1
if errorlevel 1 (
  call :LOG "Git not available. Skipping updates."
  goto LAUNCH
)

if not exist "%ROOT_DIR%\.git" (
  call :LOG "Not a git repo. Skipping updates."
  goto LAUNCH
)

pushd "%ROOT_DIR%" >> "%LOG_FILE%" 2>&1

call :LOG "Testing Git remote..."
git ls-remote --heads origin >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  call :LOG "Git remote/auth failed. Skipping updates."
  popd
  goto LAUNCH
)

call :LOG "Fetching updates..."
git fetch --prune >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  call :LOG "Git fetch failed. Skipping updates."
  popd
  goto LAUNCH
)

git status -uno | findstr /C:"behind" >nul
if errorlevel 1 (
  call :LOG "No updates detected."
) else (
  call :LOG "Updates detected. Pulling..."
  git pull --ff-only >> "%LOG_FILE%" 2>&1
  if errorlevel 1 (
    call :LOG "Git pull failed. Using local version."
  ) else (
    call :LOG "Git pull successful."
    call :LOG "Clearing Python bytecode cache..."
    for /d /r "%ROOT_DIR%" %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d" >nul 2>&1
    call :LOG "Bytecode cache cleared."
  )
)

popd

REM ============================================================
REM LAUNCH STREAMLIT
REM ============================================================
:LAUNCH
call :LOG "Launching Streamlit..."

netstat -ano | findstr /R /C:":%STREAMLIT_PORT% .*LISTENING" >nul
if %ERRORLEVEL%==0 (
  call :LOG "App already running. Opening browser."
  start "" "http://localhost:%STREAMLIT_PORT%"
  echo.>> "%LOG_FILE%"
  exit /b 0
)

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

timeout /t 2 >nul
start "" "http://localhost:%STREAMLIT_PORT%"
echo.>> "%LOG_FILE%"
exit /b 0

REM ============================================================
REM LOG FUNCTION
REM ============================================================
:LOG
echo [%date% %time%] [Launcher] %~1>> "%LOG_FILE%"
echo %~1
exit /b 0
