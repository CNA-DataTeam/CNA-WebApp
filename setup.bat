@echo off
setlocal EnableDelayedExpansion
title Logistics Support App Setup

REM ============================================================
REM SILENT MODE (skip pause commands — used by installer)
REM ============================================================
set "SILENT=0"
if /i "%~1"=="/silent" set "SILENT=1"
if /i "%~1"=="--silent" set "SILENT=1"

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
  if "%SILENT%"=="0" pause
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
if "%SILENT%"=="0" pause
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
  if "%SILENT%"=="0" pause
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
    if "%SILENT%"=="0" pause
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
  if "%SILENT%"=="0" pause
  exit /b 1
)

echo Installing dependencies...
set "VIRTUAL_ENV=%VENV_DIR%"
"%UV_EXE%" pip install --link-mode copy -r "%REQ_FILE%"
if errorlevel 1 (
  echo ERROR: Dependency installation failed.
  if "%SILENT%"=="0" pause
  exit /b 1
)

REM ============================================================
REM DECRYPT CONFIG
REM ============================================================
set "KEY_FILE=%CODE_DIR%\config.key"
set "NETWORK_KEY=\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.key"
set "CONFIG_ENC=%CODE_DIR%\config.enc"

if not exist "%KEY_FILE%" (
  if exist "%NETWORK_KEY%" (
    echo Copying config key from network share...
    copy /Y "%NETWORK_KEY%" "%KEY_FILE%" >nul 2>&1
  ) else (
    echo WARNING: Config key not found on network share.
  )
)

if exist "%CONFIG_ENC%" (
  if exist "%KEY_FILE%" (
    echo Decrypting config...
    "%VENV_DIR%\Scripts\python.exe" "%CODE_DIR%\config_manager.py" decrypt
    if errorlevel 1 (
      echo WARNING: Config decryption failed.
    )
  )
) else (
  echo WARNING: config.enc not found in repo.
)

REM ============================================================
REM BUILD LAUNCHER EXE (if _internal/ is missing)
REM ============================================================
set "EXE_FILE=%ROOT_DIR%\CNA Web App.exe"
set "INTERNAL_DIR=%ROOT_DIR%\_internal"
set "BUILD_DIST=%CODE_DIR%\installer\dist"
if not exist "%INTERNAL_DIR%" (
  echo _internal folder missing — building launcher exe...
  "%VENV_DIR%\Scripts\python.exe" -c "import PyInstaller" >nul 2>&1
  if errorlevel 1 (
    echo Installing PyInstaller...
    set "VIRTUAL_ENV=%VENV_DIR%"
    "%UV_EXE%" pip install pyinstaller
  )
  if exist "%BUILD_DIST%" rmdir /s /q "%BUILD_DIST%" >nul 2>&1
  echo Building CNA Web App.exe (onedir)...
  "%VENV_DIR%\Scripts\pyinstaller.exe" --onedir --noconsole --icon="%ROOT_DIR%\cna_icon.ico" --name="CNA Web App" --distpath="%BUILD_DIST%" --specpath="%CODE_DIR%\installer" --workpath="%CODE_DIR%\installer\build" "%CODE_DIR%\stub_launcher.py"
  if exist "%BUILD_DIST%\CNA Web App\CNA Web App.exe" (
    move /Y "%BUILD_DIST%\CNA Web App\CNA Web App.exe" "%ROOT_DIR%\" >nul
    xcopy /E /I /Y /Q "%BUILD_DIST%\CNA Web App\_internal" "%ROOT_DIR%\_internal"
    rmdir /s /q "%BUILD_DIST%" 2>nul
    echo Launcher exe built successfully.
  ) else (
    echo WARNING: Failed to build launcher exe. You can run RebuildExe.bat manually.
  )
)

REM ============================================================
REM CREATE DESKTOP SHORTCUT
REM ============================================================
set "SHORTCUT_PATH=%ROOT_DIR%\CNA Web App.lnk"
set "ICON_FILE=%ROOT_DIR%\cna_icon.ico"

if exist "%EXE_FILE%" (
  echo Creating shortcut to CNA Web App.exe...
  powershell -Command "$s=(New-Object -COM WScript.Shell).CreateShortcut('%SHORTCUT_PATH%'); $s.TargetPath='%EXE_FILE%'; $s.WorkingDirectory='%ROOT_DIR%'; if(Test-Path '%ICON_FILE%'){$s.IconLocation='%ICON_FILE%,0'}; $s.WindowStyle=1; $s.Save()" >nul 2>&1
) else (
  echo Creating shortcut to StartApp.vbs fallback...
  powershell -Command "$s=(New-Object -COM WScript.Shell).CreateShortcut('%SHORTCUT_PATH%'); $s.TargetPath='wscript.exe'; $s.Arguments='\"%ROOT_DIR%\StartApp.vbs\"'; $s.WorkingDirectory='%ROOT_DIR%'; if(Test-Path '%ICON_FILE%'){$s.IconLocation='%ICON_FILE%,0'}; $s.WindowStyle=1; $s.Save()" >nul 2>&1
)
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
if "%SILENT%"=="0" timeout /t 5 >nul
exit /b 0
