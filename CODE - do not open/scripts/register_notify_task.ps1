<#
register_notify_task.ps1

Registers (or removes) the Windows Scheduled Task that runs the Time Allocation
reminder job, notify_missing_time.py. Mirrors the existing "CNA Console Data
Refresh" task: runs windowless via pythonw.exe, with an INTERACTIVE logon type
(OneDrive/UNC + Outlook COM only work in a logged-on session), every Friday
at 3:00 PM (15:00) — the reminder email reports time "as of 3:00 PM"; the morning
data refresh keeps users.parquet fresh.

The reminder job itself does NOT email anyone until an admin turns a department
ON and flips "Send live to employees" in Time Allocation > Admin Settings, so it
is safe to register ahead of go-live.

Usage (run in PowerShell on the scheduled machine, as the logged-on user):
    powershell -ExecutionPolicy Bypass -File "CODE - do not open\scripts\register_notify_task.ps1"
    powershell -ExecutionPolicy Bypass -File "...\register_notify_task.ps1" -Time 15:00
    powershell -ExecutionPolicy Bypass -File "...\register_notify_task.ps1" -Unregister
#>

[CmdletBinding()]
param(
    [string]$Time = "15:00",
    [string]$TaskName = "CNA Console Time Allocation Reminders",
    [switch]$Unregister
)

$ErrorActionPreference = "Stop"

# Resolve repo root: this script is at <root>\CODE - do not open\scripts\
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$CodeDir   = Split-Path -Parent $ScriptDir
$RootDir   = Split-Path -Parent $CodeDir

$Pythonw = Join-Path $RootDir ".venv\Scripts\pythonw.exe"
$Target  = Join-Path $CodeDir "notify_missing_time.py"

if ($Unregister) {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "Removed scheduled task '$TaskName'."
    } else {
        Write-Host "Scheduled task '$TaskName' was not found."
    }
    return
}

if (-not (Test-Path $Pythonw)) { throw "pythonw.exe not found at $Pythonw (run setup.bat first)." }
if (-not (Test-Path $Target))  { throw "notify_missing_time.py not found at $Target." }

$Action = New-ScheduledTaskAction -Execute $Pythonw `
    -Argument "`"$Target`"" `
    -WorkingDirectory $RootDir

$Trigger = New-ScheduledTaskTrigger -Weekly `
    -DaysOfWeek Friday `
    -At $Time

# Run as the current interactive user; Limited (non-elevated) is enough.
$Principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask -TaskName $TaskName `
    -Action $Action -Trigger $Trigger -Principal $Principal -Settings $Settings `
    -Description "Sends Time Allocation 'missing time entries' reminder emails. Safe until a department is enabled and 'Send live' is on." | Out-Null

Write-Host "Registered scheduled task '$TaskName' (Fridays at $Time)."
Write-Host "  Runs: $Pythonw `"$Target`""
Write-Host "  It stays silent until a department is enabled AND 'Send live to employees' is on."
