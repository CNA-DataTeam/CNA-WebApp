<#
.SYNOPSIS
  Reproduce the "untrusted mount point" (Windows error 448) RedirectionGuard
  failure on a machine that does NOT enforce it by default.

.DESCRIPTION
  On affected machines, Windows applies the RedirectionGuard process mitigation
  (ProcessRedirectionTrustPolicy / EnforceRedirectionTrust) to the downloaded
  installer because the .exe carries Mark-of-the-Web -- and the mitigation is
  INHERITED by every child process. uv then cannot traverse the junction to its
  managed Python interpreter and dies with os error 448 during "uv pip install"
  ("Failed to inspect Python interpreter ... The path cannot be traversed
  because it contains an untrusted mount point").

  Most dev machines don't auto-apply that mitigation, so they can't reproduce
  the bug. This script forces the condition: it opts the CURRENT process into
  EnforceRedirectionTrust, then launches a target (the installer, setup.bat, or
  any command) as a DIRECT child, which inherits the mitigation -- identical to
  the real affected machines.

  HOW THE FACTS WERE ESTABLISHED (all verified on build 26200):
    * A process that opts into EnforceRedirectionTrust is blocked (448) from
      following a junction created by a non-admin user.
    * The mitigation IS inherited by child processes (so the installer's uv
      child inherits it -> 448).
    * A scheduled task created by a mitigated process does NOT inherit it (the
      task is spawned by the Task Scheduler service) -- which is why the planned
      fix re-parents setup.bat through a one-shot scheduled task.

  USING THIS TO VALIDATE THE FIX:
    * Unfixed installer / setup.bat run via this script  -> should FAIL with 448.
    * Fixed installer (re-parents setup via Task Scheduler) run via this script
      -> should COMPLETE, because the scheduler task escapes the mitigation.

  REQUIREMENTS:
    * Run NON-ELEVATED. EnforceRedirectionTrust only blocks junctions created by
      non-admin users; an elevated run would NOT reproduce the failure.
    * Kernel must support the mitigation (Win10 2004+ / Win11). Use -SelfTest to
      confirm before launching anything.

.PARAMETER Target
  Path to the executable or .bat/.cmd to run under the mitigation
  (e.g. the installer .exe, or a clone's setup.bat).

.PARAMETER Arguments
  Optional argument string passed to Target.

.PARAMETER SelfTest
  Don't launch anything -- just create a junction, enable the mitigation, and
  try to traverse it. Confirms the machine can reproduce the 448 block.

.EXAMPLE
  # Confirm this machine can reproduce the block:
  .\simulate-redirection-guard.ps1 -SelfTest

.EXAMPLE
  # Reproduce the installer failure end-to-end:
  .\simulate-redirection-guard.ps1 -Target "$env:USERPROFILE\Downloads\CNA-Console-Installer.exe"

.EXAMPLE
  # Reproduce the setup.bat failure directly in an existing clone:
  .\simulate-redirection-guard.ps1 -Target "C:\CNA-WebApp\setup.bat"
#>
param(
  [string]$Target,
  [string]$Arguments = '',
  [switch]$SelfTest
)

$ErrorActionPreference = 'Stop'

if (-not $SelfTest -and -not $Target) {
  throw "Provide -Target <exe/bat> to run under the mitigation, or use -SelfTest to just prove the block."
}

# --- Guard: must be non-elevated (elevated junctions are 'admin-created' = trusted) ---
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)
if ($isAdmin) {
  Write-Warning "Running ELEVATED. EnforceRedirectionTrust only blocks junctions created by NON-admin users, so this will NOT reproduce the failure. Re-run in a normal (non-admin) PowerShell."
  return
}

# --- RedirectionGuard opt-in (ProcessRedirectionTrustPolicy = 16, EnforceRedirectionTrust = 0x1) ---
if (-not ('RedirGuard' -as [type])) {
  Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class RedirGuard {
  [DllImport("kernel32.dll", SetLastError=true)]
  static extern bool SetProcessMitigationPolicy(int policy, ref uint buffer, IntPtr length);
  public static bool Enable() {
    uint flags = 1; // EnforceRedirectionTrust
    return SetProcessMitigationPolicy(16, ref flags, (IntPtr)4);
  }
}
"@
}

function Enable-Guard {
  $ok  = [RedirGuard]::Enable()
  $err = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
  if (-not $ok) {
    throw "Could not enable RedirectionGuard (SetProcessMitigationPolicy failed, lastErr=$err). This Windows build may not allow setting it at runtime."
  }
  Write-Host "[+] RedirectionGuard (EnforceRedirectionTrust) enabled on this process (PID $PID)." -ForegroundColor Green
}

# ============================== SELF-TEST ==============================
if ($SelfTest) {
  $base   = Join-Path $env:TEMP ("rgsim_" + [guid]::NewGuid().ToString('N'))
  $tgtDir = Join-Path $base 'target'
  $link   = Join-Path $base 'link'
  New-Item -ItemType Directory -Path $tgtDir -Force | Out-Null
  Set-Content -Path (Join-Path $tgtDir 'probe.txt') -Value 'hello'
  cmd /c mklink /J "$link" "$tgtDir" | Out-Null
  Enable-Guard
  try {
    [IO.File]::ReadAllText((Join-Path $link 'probe.txt')) | Out-Null
    Write-Host "[!] TRAVERSAL SUCCEEDED -- this machine did NOT block it. It can't reproduce the failure" -ForegroundColor Yellow
    Write-Host "    (kernel not enforcing the mitigation, or the build is too old)." -ForegroundColor Yellow
  } catch {
    Write-Host "[OK] TRAVERSAL BLOCKED: $($_.Exception.Message.Trim())" -ForegroundColor Green
    Write-Host "     This machine reproduces the RedirectionGuard 448 condition." -ForegroundColor Green
  } finally {
    Remove-Item $base -Recurse -Force -ErrorAction SilentlyContinue
  }
  return
}

# ================================ RUN =================================
if (-not (Test-Path $Target)) { throw "Target not found: $Target" }
$Target = (Resolve-Path $Target).Path
$ext = [IO.Path]::GetExtension($Target).ToLowerInvariant()

# UseShellExecute=$false => the target is a DIRECT child of this (mitigated)
# process, so it inherits EnforceRedirectionTrust. (A shell-launched child can
# be re-parented and would NOT inherit it, defeating the simulation.)
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.UseShellExecute  = $false
$psi.WorkingDirectory = [IO.Path]::GetDirectoryName($Target)
if ($ext -eq '.bat' -or $ext -eq '.cmd') {
  $psi.FileName  = "$env:SystemRoot\System32\cmd.exe"
  $psi.Arguments = '/c "' + $Target + '" ' + $Arguments
} else {
  $psi.FileName  = $Target
  $psi.Arguments = $Arguments
}

Write-Host "[*] Target : $Target"   -ForegroundColor Cyan
Write-Host "[*] Args   : $Arguments" -ForegroundColor Cyan
Enable-Guard
Write-Host "[*] Launching target as a direct child (it will INHERIT the mitigation)..." -ForegroundColor Cyan
$p = [System.Diagnostics.Process]::Start($psi)
$p.WaitForExit()
Write-Host ""
Write-Host "[*] Target exited with code $($p.ExitCode)." -ForegroundColor Cyan
Write-Host "    Unfixed installer / setup.bat  -> expect a 448 'untrusted mount point' failure" -ForegroundColor DarkGray
Write-Host "    during 'Installing dependencies'." -ForegroundColor DarkGray
Write-Host "    Fixed installer (setup re-parented via Task Scheduler) -> should complete." -ForegroundColor DarkGray
