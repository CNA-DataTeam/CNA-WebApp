@echo off
setlocal
title Clone CNA-WebApp Repository

set "REPO_URL=https://github.com/CNA-DataTeam/CNA-WebApp.git"
set "ROOT_DIR=%~dp0"
if "%ROOT_DIR:~-1%"=="\" set "ROOT_DIR=%ROOT_DIR:~0,-1%"

echo This will delete and re-clone the repo at:
echo   %ROOT_DIR%
echo.
set /p CONFIRM="Are you sure? (Y/N): "
if /i not "%CONFIRM%"=="Y" (
  echo Cancelled.
  pause
  exit /b 0
)

echo.
echo Cloning into temp directory...
git clone "%REPO_URL%" "%ROOT_DIR%_clone_temp"
if errorlevel 1 (
  echo ERROR: Clone failed. Check your internet connection and that Git is installed.
  if exist "%ROOT_DIR%_clone_temp" rmdir /s /q "%ROOT_DIR%_clone_temp"
  pause
  exit /b 1
)

echo Removing old files...
for /d %%D in ("%ROOT_DIR%\*") do (
  if /i not "%%~nxD"==".venv" (
    rmdir /s /q "%%D"
  )
)
for %%F in ("%ROOT_DIR%\*") do (
  if /i not "%%~nxF"=="CloneRepository.bat" (
    del /f /q "%%F"
  )
)

echo Moving cloned files into place...
xcopy /E /I /Y /Q "%ROOT_DIR%_clone_temp\*" "%ROOT_DIR%\"
rmdir /s /q "%ROOT_DIR%_clone_temp"

echo.
echo Done. Repository refreshed at %ROOT_DIR%
pause
