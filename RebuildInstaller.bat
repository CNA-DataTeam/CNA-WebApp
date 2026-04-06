@echo off
REM ============================================================
REM RebuildInstaller.bat — Compile the Inno Setup installer
REM ============================================================
REM Uses ISCC.exe (Inno Setup Command-Line Compiler) to build
REM CNA-WebApp-Setup.exe into the installer-output folder.
REM ============================================================

setlocal EnableDelayedExpansion

set "ROOT_DIR=%~dp0"
set "ISS_FILE=%ROOT_DIR%CODE - do not open\installer\CNA-WebApp-Setup.iss"
set "OUTPUT_DIR=%ROOT_DIR%installer-output"

REM --- Locate ISCC.exe ---
set "ISCC="

if exist "%LOCALAPPDATA%\Programs\Inno Setup 6\ISCC.exe" (
    set "ISCC=%LOCALAPPDATA%\Programs\Inno Setup 6\ISCC.exe"
    goto :found
)
if exist "%ProgramFiles%\Inno Setup 6\ISCC.exe" (
    set "ISCC=%ProgramFiles%\Inno Setup 6\ISCC.exe"
    goto :found
)
if exist "%ProgramFiles(x86)%\Inno Setup 6\ISCC.exe" (
    set "ISCC=%ProgramFiles(x86)%\Inno Setup 6\ISCC.exe"
    goto :found
)

REM Try PATH as last resort
where iscc >nul 2>&1
if not errorlevel 1 (
    for /f "tokens=*" %%I in ('where iscc') do set "ISCC=%%I"
    goto :found
)

echo ERROR: Inno Setup (ISCC.exe) not found.
echo Install Inno Setup 6 from https://jrsoftware.org/isinfo.php
exit /b 1

:found
echo Using ISCC: %ISCC%

if not exist "%ISS_FILE%" (
    echo ERROR: ISS file not found: %ISS_FILE%
    exit /b 1
)

REM --- Ensure output directory exists ---
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

REM --- Compile ---
echo Compiling installer...
"%ISCC%" /Qp /O"%OUTPUT_DIR%" "%ISS_FILE%"
if errorlevel 1 (
    echo.
    echo Compile failed. Retrying with TEMP output to work around antivirus...
    "%ISCC%" /Qp /O"%TEMP%" "%ISS_FILE%"
    if errorlevel 1 (
        echo ERROR: Compile failed even with TEMP output.
        exit /b 1
    )
    echo Copying from TEMP to installer-output...
    copy /Y "%TEMP%\CNA-WebApp-Setup.exe" "%OUTPUT_DIR%\CNA-WebApp-Setup.exe" >nul
)

echo.
echo Installer built: %OUTPUT_DIR%\CNA-WebApp-Setup.exe
