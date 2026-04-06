@echo off
title Force Close App

echo Stopping CNA Web App processes...

REM ---- Kill the launcher .exe ----
taskkill /F /IM "CNA Web App.exe" >nul 2>&1

REM ---- Kill Streamlit on port 8501 (process tree) ----
set "STREAMLIT_PORT=8501"
netstat -ano | findstr ":%STREAMLIT_PORT% .*LISTENING" > "%TEMP%\cna_pids.txt" 2>nul
for /f "tokens=5" %%p in (%TEMP%\cna_pids.txt) do (
  echo Stopping Streamlit process tree (PID %%p^)...
  taskkill /F /T /PID %%p >nul 2>&1
)
del "%TEMP%\cna_pids.txt" >nul 2>&1

REM ---- Kill any orphaned cmd.exe/python processes from this app ----
REM Write a PowerShell script to a temp file to avoid pipe-parsing issues in cmd.exe
echo Get-CimInstance Win32_Process ^| Where-Object { $_.CommandLine -match 'CNA-WebApp' -and $_.ProcessId -ne $PID -and ($_.Name -match 'python' -or ($_.Name -eq 'cmd.exe' -and $_.CommandLine -match 'setup\.bat')) } ^| ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue } > "%TEMP%\cna_cleanup.ps1"
powershell -NoProfile -ExecutionPolicy Bypass -File "%TEMP%\cna_cleanup.ps1" >nul 2>&1
del "%TEMP%\cna_cleanup.ps1" >nul 2>&1

echo.
echo App stopped.
pause
