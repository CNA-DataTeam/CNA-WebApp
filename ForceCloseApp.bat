@echo off
setlocal
title Force Close App

set "STREAMLIT_PORT=8501"

echo Looking for app running on port %STREAMLIT_PORT%...

for /f "tokens=5" %%p in ('netstat -ano ^| findstr /R ":%STREAMLIT_PORT% .*LISTENING" 2^>nul') do (
  echo Stopping process %%p...
  taskkill /F /PID %%p >nul 2>&1
)

echo App stopped.
timeout /t 2 >nul
