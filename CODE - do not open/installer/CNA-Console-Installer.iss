; ============================================================
; CNA Console Installer — Inno Setup Script
; ============================================================
; Compile this with Inno Setup 6+ to produce the installer exe.
; Download Inno Setup: https://jrsoftware.org/isinfo.php
;
; What this installer does:
;   1. Checks for Git — installs via winget if missing
;   2. Clones the CNA-WebApp repo to the chosen directory (includes CNA Web App.exe)
;   3. Runs setup.bat (installs uv, Python 3.11, venv, dependencies, creates shortcut to .exe)
;   4. Copies config.key from the network share
; ============================================================

#define MyAppName "CNA Console"
#define MyAppVersion "1.0"
#define MyAppPublisher "Clark National Accounts"
#define MyRepoURL "https://github.com/CNA-DataTeam/CNA-WebApp.git"
#define MyNetworkKey "\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.key"

[Setup]
AppId={{B8F3A2D1-7E4C-4A9B-8D6F-1C2E3F4A5B6C}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={localappdata}\CNA-WebApp
; Always offer the {localappdata} default. Without this, Inno's default
; UsePreviousAppDir=yes reuses a path remembered from a prior install
; (e.g. a stale C:\Users\Public\CNA-WebApp), overriding DefaultDirName.
UsePreviousAppDir=no
DisableProgramGroupPage=yes
; Keep the directory page enabled; the default ({localappdata}) works for
; most users. The failure mode we guard against is Windows RedirectionGuard:
; when this installer carries Mark-of-the-Web, the mitigation is inherited by
; every child process and blocks uv from traversing the junction to its
; managed Python (error 448) during "uv pip install". To avoid it, setup.bat
; is run OUTSIDE this installer's (mitigated) process tree — first via a
; one-shot scheduled task, then (if task execution is policy-blocked) via the
; Windows shell (explorer.exe), each with a ~90s start-heartbeat so a dead
; method fails over fast instead of hanging. See TrySetupViaScheduledTask /
; TrySetupViaExplorer in [Code]. If setup still can't finish, a guided dialog
; walks the user through unblocking the installer or running setup by hand.
DisableDirPage=no
OutputDir=..\..\installer-output
OutputBaseFilename=CNA-Console-Installer
SetupIconFile=..\..\cna_icon.ico
UninstallDisplayIcon={app}\cna_icon.ico
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
DefaultGroupName={#MyAppName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Code]

var
  StatusLabel: TNewStaticText;
  GFailInstallDir: String;

// -------------------------------------------------------
// Helper: run a command and wait for it to finish
// -------------------------------------------------------
function RunAndWait(const Cmd, Params, WorkDir: String): Integer;
var
  ResultCode: Integer;
begin
  if not Exec(Cmd, Params, WorkDir, SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    ResultCode := -1;
  Result := ResultCode;
end;

// -------------------------------------------------------
// Helper: check if a command exists in PATH
// -------------------------------------------------------
function CommandExists(const Cmd: String): Boolean;
var
  ResultCode: Integer;
begin
  Result := Exec('cmd.exe', '/c where ' + Cmd + ' >nul 2>&1', '', SW_HIDE, ewWaitUntilTerminated, ResultCode)
            and (ResultCode = 0);
end;

// -------------------------------------------------------
// Helper: locate git.exe on disk at any known install path.
// Returns the full path to git.exe, or '' if not found.
//
// Why this is necessary: after winget installs Git, the installer's PATH
// is still the stale parent-process PATH and `git` won't resolve via
// `where git` even though Git is on disk. We also can't assume Git
// landed under C:\Program Files\Git — winget defaults to user-scope
// (%LOCALAPPDATA%\Programs\Git) when the installer is unelevated, which
// is our default since PrivilegesRequired=lowest.
// -------------------------------------------------------
function FindGitExe(): String;
var
  Candidate: String;
begin
  // 1. System-wide install (machine-scope winget, traditional installer)
  if FileExists('C:\Program Files\Git\cmd\git.exe') then
  begin
    Result := 'C:\Program Files\Git\cmd\git.exe';
    Exit;
  end;
  // 2. 32-bit fallback (rare)
  if FileExists('C:\Program Files (x86)\Git\cmd\git.exe') then
  begin
    Result := 'C:\Program Files (x86)\Git\cmd\git.exe';
    Exit;
  end;
  // 3. User-scope install (winget default when unelevated — most likely
  //    location after this installer runs the winget install step)
  Candidate := ExpandConstant('{localappdata}\Programs\Git\cmd\git.exe');
  if FileExists(Candidate) then
  begin
    Result := Candidate;
    Exit;
  end;
  Result := '';
end;

// -------------------------------------------------------
// Helper: update status label and progress bar
// -------------------------------------------------------
procedure SetProgress(const Percent: Integer);
begin
  with WizardForm.ProgressGauge do
  begin
    Min := 0;
    Max := 100;
    Position := Percent;
  end;
end;

procedure UpdateStatus(const Msg: String);
begin
  if StatusLabel <> nil then
  begin
    StatusLabel.Caption := Msg;
    StatusLabel.Update;
  end;
  Log(Msg);
end;

// -------------------------------------------------------
// Create a status label on the installing page
// -------------------------------------------------------
procedure InitializeWizard;
begin
  StatusLabel := TNewStaticText.Create(WizardForm);
  StatusLabel.Parent := WizardForm.InstallingPage;
  StatusLabel.Left := 0;
  StatusLabel.Top := WizardForm.StatusLabel.Top + WizardForm.StatusLabel.Height + ScaleY(12);
  StatusLabel.Width := WizardForm.InstallingPage.Width;
  StatusLabel.AutoSize := False;
  StatusLabel.WordWrap := True;
  StatusLabel.Caption := '';
end;

// -------------------------------------------------------
// Prevent install into a non-empty directory that isn't
// already a CNA-WebApp clone
// -------------------------------------------------------
function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;
  if CurPageID = wpSelectDir then
  begin
    if DirExists(ExpandConstant('{app}')) and
       not FileExists(ExpandConstant('{app}\setup.bat')) then
    begin
      if not DirExists(ExpandConstant('{app}\.git')) then
      begin
        // Directory exists but isn't our repo — warn user
        Result := (MsgBox(
          'The selected folder already exists and does not appear to contain CNA Console. ' +
          'Files may be overwritten.' + #13#10#13#10 +
          'Continue anyway?',
          mbConfirmation, MB_YESNO) = IDYES);
      end;
    end;
  end;
end;

// -------------------------------------------------------
// Run setup.bat OUTSIDE this installer's process tree.
//
// When the downloaded installer carries Mark-of-the-Web, Windows applies the
// RedirectionGuard mitigation to it, and that mitigation is INHERITED by every
// child process. Under it, uv cannot traverse the junction to its managed
// Python and fails with error 448 during "uv pip install". The escape is to
// have setup.bat spawned by something that is NOT a child of this installer:
//   1. A one-shot scheduled task (spawned by the Task Scheduler service).
//   2. The Windows shell via explorer.exe (the already-running desktop shell
//      launches it — also not our child). More reliable than scheduled tasks
//      on locked-down standard-user machines where task EXECUTION is blocked
//      by policy even though /create + /run report success.
//   3. Direct in-process run as a last resort (works only if the installer
//      itself isn't MOTW-mitigated — e.g. it was Unblocked or run from an
//      intranet share).
//
// FIELD-LEARNED FAILURE (June 2026): on locked-down profiles the scheduled
// task is created and /run succeeds, but the task never actually executes, so
// the old single-method version dead-waited the full 20-minute completion
// timeout before failing. The wrapper now writes a START heartbeat the moment
// it launches; WaitForSetup gives up after ~90s if that heartbeat never
// appears (the task didn't run) and the caller fails over to the next method
// immediately — instead of staring at a hidden process for 20 minutes.
//
// The wrapper .bat + markers live under {commonappdata} (C:\ProgramData\...),
// a path with no spaces, so schtasks /tr needs no fragile quoting.
// -------------------------------------------------------

// Write the self-contained setup wrapper: announce it started, cd to the
// install dir, run setup silently with the log captured, then ALWAYS record a
// completion marker with the exit code (so we can tell finished-vs-still-going
// and success-vs-failure).
procedure WriteSetupWrapper(const Wrapper, InstallDir, SetupBat, SetupLog, StartMarker, DoneMarker: String);
begin
  SaveStringToFile(Wrapper,
    '@echo off' + #13#10 +
    'echo STARTED>"' + StartMarker + '"' + #13#10 +
    'cd /d "' + InstallDir + '"' + #13#10 +
    'call "' + SetupBat + '" /silent > "' + SetupLog + '" 2>&1' + #13#10 +
    'echo DONE_%errorlevel%>"' + DoneMarker + '"' + #13#10, False);
end;

// Wait for a launched wrapper. Returns:
//   True  — setup completed (DoneMarker present).
//   False — either the wrapper never started (no StartMarker within
//           StartTimeoutSecs → caller should try the next launch method) or it
//           started but didn't finish within DoneTimeoutSecs.
function WaitForSetup(const StartMarker, DoneMarker: String;
                      StartTimeoutSecs, DoneTimeoutSecs: Integer): Boolean;
var
  Waited: Integer;
begin
  Result := False;

  // Phase 1 — fast failover: prove the wrapper actually launched.
  Waited := 0;
  while (not FileExists(StartMarker)) and (Waited < StartTimeoutSecs) do
  begin
    Sleep(1000);
    Waited := Waited + 1;
  end;
  if not FileExists(StartMarker) then
    Exit;  // never started — bail now so the caller can try another method

  // Phase 2 — it's genuinely running; wait (longer) for completion and keep
  // the status line moving so a multi-minute build never looks frozen.
  Waited := 0;
  while (not FileExists(DoneMarker)) and (Waited < DoneTimeoutSecs) do
  begin
    UpdateStatus('Installing Python, dependencies and building the app... ('
                 + IntToStr(Waited) + 's elapsed)');
    Sleep(2000);
    Waited := Waited + 2;
  end;

  Result := FileExists(DoneMarker);
end;

// Attempt 1: one-shot scheduled task. Returns True only if setup completed.
function TrySetupViaScheduledTask(const Wrapper, StartMarker, DoneMarker: String): Boolean;
var
  ResultCode: Integer;
begin
  Result := False;
  DeleteFile(StartMarker);
  DeleteFile(DoneMarker);

  Exec('schtasks.exe', '/delete /tn "CNAConsoleSetup" /f', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  if (not Exec('schtasks.exe',
        '/create /tn "CNAConsoleSetup" /tr "' + Wrapper + '" /sc ONCE /st 23:59 /f',
        '', SW_HIDE, ewWaitUntilTerminated, ResultCode)) or (ResultCode <> 0) then
    Exit;
  if (not Exec('schtasks.exe', '/run /tn "CNAConsoleSetup"',
        '', SW_HIDE, ewWaitUntilTerminated, ResultCode)) or (ResultCode <> 0) then
  begin
    Exec('schtasks.exe', '/delete /tn "CNAConsoleSetup" /f', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
    Exit;
  end;

  // 90s to start (generous; a task normally starts in <2s) then up to 20 min
  // to finish once it has proven it's running.
  Result := WaitForSetup(StartMarker, DoneMarker, 90, 1200);
  Exec('schtasks.exe', '/delete /tn "CNAConsoleSetup" /f', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

// Attempt 2: launch via the Windows shell. explorer.exe hands the wrapper to
// the already-running desktop shell, which is NOT a child of this installer —
// so the setup it launches escapes the inherited RedirectionGuard mitigation,
// same as the scheduled task, but works even where task execution is policy-
// blocked. The wrapper runs in a visible console, which also gives the user
// real progress. Returns True only if setup completed.
function TrySetupViaExplorer(const Wrapper, StartMarker, DoneMarker: String): Boolean;
var
  ResultCode: Integer;
begin
  Result := False;
  DeleteFile(StartMarker);
  DeleteFile(DoneMarker);
  if not Exec('explorer.exe', '"' + Wrapper + '"', '', SW_SHOWNORMAL, ewNoWait, ResultCode) then
    Exit;
  Result := WaitForSetup(StartMarker, DoneMarker, 90, 1200);
end;

// -------------------------------------------------------
// "Open Install Folder" button handler for the manual-finish dialog.
// -------------------------------------------------------
procedure OpenInstallFolderClick(Sender: TObject);
var
  ErrorCode: Integer;
begin
  Exec('explorer.exe', '"' + GFailInstallDir + '"', '', SW_SHOWNORMAL, ewNoWait, ErrorCode);
end;

// -------------------------------------------------------
// Friendly "finish setup by hand" dialog, shown when the install could not
// complete automatically — for ANY setup reason. Plain step-by-step
// instructions plus a button that opens the install folder in Explorer.
// -------------------------------------------------------
procedure ShowSetupFailedDialog(const InstallDir, Details: String);
var
  Form: TSetupForm;
  Steps: TNewStaticText;
  DetailBox: TNewMemo;
  OpenBtn, CloseBtn: TNewButton;
begin
  GFailInstallDir := InstallDir;
  Form := CreateCustomForm(ScaleX(500), ScaleY(442), False, False);
  try
    Form.Caption := 'CNA Console — one quick step to finish';
    Form.Position := poScreenCenter;
    Form.BorderStyle := bsDialog;

    Steps := TNewStaticText.Create(Form);
    Steps.Parent := Form;
    Steps.Left := ScaleX(20);
    Steps.Top := ScaleY(18);
    Steps.Width := Form.ClientWidth - ScaleX(40);
    Steps.AutoSize := False;
    Steps.Height := ScaleY(280);
    Steps.WordWrap := True;
    Steps.Caption :=
      'Setup could not finish automatically — but this is quick to fix.' + #13#10#13#10 +
      'MOST LIKELY FIX — unblock the installer, then run it again:' + #13#10 +
      '    1.  Find the CNA-Console-Installer file you downloaded.' + #13#10 +
      '    2.  Right-click it  >  Properties.' + #13#10 +
      '    3.  Check the "Unblock" box (bottom-right)  >  OK.' + #13#10 +
      '    4.  Run the installer again.' + #13#10#13#10 +
      'IF THAT DOESN''T WORK — finish the last step by hand:' + #13#10 +
      '    1.  Click "Open Install Folder" below.' + #13#10 +
      '    2.  Double-click the file named  setup  (may show as "setup.bat").' + #13#10 +
      '    3.  A black window runs for a few minutes, then closes itself.' + #13#10 +
      '    4.  Open "CNA Console" from your taskbar or Start Menu.' + #13#10#13#10 +
      'Stuck? Contact Jordan Ramsey at jramsey@clarknationalaccounts.com.';

    DetailBox := TNewMemo.Create(Form);
    DetailBox.Parent := Form;
    DetailBox.Left := ScaleX(20);
    DetailBox.Top := ScaleY(304);
    DetailBox.Width := Form.ClientWidth - ScaleX(40);
    DetailBox.Height := ScaleY(80);
    DetailBox.ReadOnly := True;
    DetailBox.ScrollBars := ssVertical;
    DetailBox.Text := 'Technical details (for support):' + #13#10 + Details;

    OpenBtn := TNewButton.Create(Form);
    OpenBtn.Parent := Form;
    OpenBtn.Caption := 'Open Install Folder';
    OpenBtn.Left := ScaleX(20);
    OpenBtn.Top := Form.ClientHeight - ScaleY(42);
    OpenBtn.Width := ScaleX(170);
    OpenBtn.Height := ScaleY(28);
    OpenBtn.OnClick := @OpenInstallFolderClick;

    CloseBtn := TNewButton.Create(Form);
    CloseBtn.Parent := Form;
    CloseBtn.Caption := 'Close';
    CloseBtn.Left := Form.ClientWidth - ScaleX(110);
    CloseBtn.Top := Form.ClientHeight - ScaleY(42);
    CloseBtn.Width := ScaleX(90);
    CloseBtn.Height := ScaleY(28);
    CloseBtn.ModalResult := mrOK;
    CloseBtn.Default := True;

    Form.ActiveControl := CloseBtn;
    Form.ShowModal;
  finally
    Form.Free;
  end;
end;

// -------------------------------------------------------
// Stop any leftover processes from a previous install.
//
// The symptom this fixes: a Streamlit server from an OUTDATED install is
// still listening on port 8501. launch_app.py treats an already-listening
// 8501 as "the app is already running" and just opens a window to that
// stale server — so after a successful reinstall the user keeps seeing the
// OLD version. We kill, identified precisely (mirrors ForceCloseApp.bat):
//   1. The launcher  "CNA Web App.exe"  — distinctive image name.
//   2. Whatever owns TCP 8501 (the Streamlit server), by PID + tree.
//   3. Any python/cmd whose command line carries "CODE - do not open"
//      (the app.py path fragment, unique to this app and present in the
//      Streamlit launch command at any install location) or "CNA-WebApp".
// All best-effort and quiet; an absent process is success. Runs as the
// current user, which owns the stale processes, so no elevation needed.
// -------------------------------------------------------
procedure KillExistingAppProcesses();
var
  ResultCode: Integer;
  PsCmd: String;
begin
  // 1. Launcher exe by its distinctive image name.
  Exec('taskkill.exe', '/F /IM "CNA Web App.exe"', '',
       SW_HIDE, ewWaitUntilTerminated, ResultCode);

  // 2 + 3. Port-8501 owner (the Streamlit server) and any python/cmd whose
  //        command line carries this app's unique path fragments. Done in one
  //        PowerShell pass; powershell.exe itself is neither python nor
  //        cmd.exe, so the literal patterns below don't match this process.
  PsCmd :=
    '$ErrorActionPreference=''SilentlyContinue'';' +
    'foreach($c in Get-NetTCPConnection -LocalPort 8501 -State Listen){' +
      'taskkill /F /T /PID $c.OwningProcess | Out-Null};' +
    'Get-CimInstance Win32_Process | Where-Object {' +
      '($_.Name -match ''python'' -or $_.Name -eq ''cmd.exe'') -and ' +
      '$_.CommandLine -and ' +
      '($_.CommandLine -match ''CODE - do not open'' -or $_.CommandLine -match ''CNA-WebApp'')' +
    '} | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }';
  Exec('powershell.exe',
       '-NoProfile -ExecutionPolicy Bypass -Command "' + PsCmd + '"', '',
       SW_HIDE, ewWaitUntilTerminated, ResultCode);

  // Give the OS a moment to release port 8501 and file handles before we
  // pull/clone into the install dir.
  Sleep(1500);
end;

// -------------------------------------------------------
// Main install logic
// -------------------------------------------------------
procedure CurStepChanged(CurStep: TSetupStep);
var
  InstallDir, ConfigSrc, ConfigDst, SetupBat, GitExePath, CloneLog, SetupLog: String;
  PythonDll, VenvPython, MissingArtifacts, LogTail: String;
  SetupWorkDir, Wrapper, StartMarker, DoneMarker: String;
  CloneLogContent, SetupLogContent: AnsiString;
  ResultCode, WingetCode, LogLen: Integer;
  SetupRan: Boolean;
begin
  if CurStep <> ssInstall then
    Exit;

  InstallDir := ExpandConstant('{app}');

  SetProgress(0);

  // ---- Step 0: stop any running/leftover copy of the app ----
  // Must happen before clone/pull (releases file handles on the install dir)
  // and well before the finish-page "Launch" so the new launcher starts a
  // FRESH server instead of attaching to an outdated one still on port 8501.
  UpdateStatus('Closing any running copy of CNA Console...');
  KillExistingAppProcesses();

  // ---- Step 1: Git ----
  UpdateStatus('Checking for Git...');
  SetProgress(2);

  // Look on disk first — this finds pre-existing installs at any of the
  // known locations (system-wide, 32-bit, user-scope). Falls back to PATH
  // lookup for portable / custom installs.
  GitExePath := FindGitExe();
  if (GitExePath = '') and CommandExists('git') then
    GitExePath := 'git';

  if GitExePath = '' then
  begin
    if not CommandExists('winget') then
    begin
      MsgBox(
        'Git is not installed, and winget is not available on this machine.' + #13#10#13#10 +
        'Please install Git manually from https://git-scm.com and re-run this installer.',
        mbError, MB_OK);
      WizardForm.Close;
      Exit;
    end;

    UpdateStatus('Installing Git via winget (this may take a minute)...');
    SetProgress(5);
    // --scope user matches our unelevated installer (PrivilegesRequired=lowest)
    // so Git lands at a predictable location: %LOCALAPPDATA%\Programs\Git.
    WingetCode := RunAndWait('cmd.exe',
      '/c winget install --id Git.Git -e --silent --scope user --accept-package-agreements --accept-source-agreements',
      '');

    // PATH inherited by the installer is still stale at this point — locate
    // git.exe directly on disk instead of relying on `where git`.
    GitExePath := FindGitExe();
    if GitExePath = '' then
    begin
      MsgBox(
        'Git could not be installed automatically (winget exit code: ' + IntToStr(WingetCode) + ').' + #13#10#13#10 +
        'Please install Git manually from https://git-scm.com and re-run this installer.',
        mbError, MB_OK);
      WizardForm.Close;
      Exit;
    end;
  end;

  // ---- Step 2: Clone repo ----
  // Capture stdout+stderr to a log file so we can surface the real git error
  // on failure (network/proxy/cert issues, "destination already exists", etc.)
  // instead of the generic "Failed to clone" message.
  //
  // The outer "" wrapping around the entire /c argument is the standard cmd
  // quoting trick: with multiple quoted args (exe path + URL + dest), cmd
  // strips the outermost quote pair, leaving the inner quoted exe path
  // intact. Without it, a git.exe path containing spaces (e.g. user folder
  // 'John Doe' or 'C:\Program Files\...') ends up tokenized at the space
  // and cmd tries to run a non-existent file.
  SetProgress(15);
  CloneLog := ExpandConstant('{tmp}') + '\cna-git.log';
  SaveStringToFile(CloneLog, '', False);

  if FileExists(InstallDir + '\setup.bat') then
  begin
    UpdateStatus('Existing installation detected — cleaning state and pulling latest...');
    // Discard local modifications to regenerated build artifacts before pull.
    // Without this, `git pull` aborts with "Your local changes would be
    // overwritten" whenever the launcher exe was rebuilt locally.
    ResultCode := RunAndWait('cmd.exe',
      '/c ""' + GitExePath + '" checkout -- "CNA Web App.exe" >>"' + CloneLog + '" 2>&1 & ' +
      '"' + GitExePath + '" checkout -- "CODE - do not open\installer\CNA Web App.spec" >>"' + CloneLog + '" 2>&1 & ' +
      '"' + GitExePath + '" pull --ff-only >>"' + CloneLog + '" 2>&1"',
      InstallDir);
  end
  else
  begin
    UpdateStatus('Cloning CNA Console from GitHub...');
    SetProgress(20);
    ResultCode := RunAndWait('cmd.exe',
      '/c ""' + GitExePath + '" clone "{#MyRepoURL}" "' + InstallDir + '" >"' + CloneLog + '" 2>&1"',
      '');
  end;

  if not FileExists(InstallDir + '\setup.bat') then
  begin
    CloneLogContent := '';
    if FileExists(CloneLog) then
      LoadStringFromFile(CloneLog, CloneLogContent);
    if Length(CloneLogContent) = 0 then
      CloneLogContent := '(git produced no output — check network/proxy)';
    MsgBox(
      'Failed to clone the repository.' + #13#10#13#10 +
      'Git path: ' + GitExePath + #13#10 +
      'Exit code: ' + IntToStr(ResultCode) + #13#10 + #13#10 +
      'Git output:' + #13#10 + Copy(String(CloneLogContent), 1, 2000),
      mbError, MB_OK);
    WizardForm.Close;
    Exit;
  end;

  // ---- Step 3: Patch setup.bat then run it ----
  SetProgress(35);
  SetupBat := InstallDir + '\setup.bat';

  // Replace bare "pause" commands with non-blocking timeout so the hidden
  // window never hangs waiting for a keypress.  This handles older versions
  // of setup.bat that were cloned before the /silent flag was added.
  UpdateStatus('Preparing setup script...');
  Exec('cmd.exe',
       '/c cd /d "' + InstallDir + '" && powershell -NoProfile -Command "' +
       '(Get-Content ''setup.bat'' -Raw) -replace ''(?m)^\s*pause\s*$'', ''timeout /t 3 /nobreak >nul 2>nul'' ' +
       '| Set-Content ''setup.bat'' -NoNewline"',
       InstallDir, SW_HIDE, ewWaitUntilTerminated, ResultCode);

  UpdateStatus('Running setup (installing Python, dependencies, creating shortcut)...');
  SetupLog := ExpandConstant('{tmp}') + '\cna-setup.log';
  SaveStringToFile(SetupLog, '', False);

  // Run setup OUT of this installer's (possibly Mark-of-the-Web-mitigated)
  // process tree so uv can traverse its Python junction. Try, in order:
  //   1. a one-shot scheduled task (Task Scheduler service spawns it);
  //   2. the Windows shell via explorer.exe (more reliable when task execution
  //      is policy-blocked — the failure we saw in the field);
  //   3. a direct in-process run as a last resort (works only if this
  //      installer isn't itself mitigated, e.g. it was Unblocked / run from an
  //      intranet share).
  // Each writes a START heartbeat so a method that can't actually run is
  // detected in ~90s and we move on, instead of dead-waiting the full
  // completion timeout. Validation below is the backstop for all paths.
  SetupWorkDir := ExpandConstant('{commonappdata}') + '\CNAConsoleSetup';
  ForceDirectories(SetupWorkDir);
  Wrapper := SetupWorkDir + '\run_setup.bat';
  StartMarker := SetupWorkDir + '\setup_started.marker';
  DoneMarker := SetupWorkDir + '\setup_done.marker';
  WriteSetupWrapper(Wrapper, InstallDir, SetupBat, SetupLog, StartMarker, DoneMarker);

  SetupRan := TrySetupViaScheduledTask(Wrapper, StartMarker, DoneMarker);
  if not SetupRan then
  begin
    Log('Scheduled task did not run setup; trying via the Windows shell (explorer).');
    UpdateStatus('Running setup (alternate method)...');
    SetupRan := TrySetupViaExplorer(Wrapper, StartMarker, DoneMarker);
  end;
  if not SetupRan then
  begin
    Log('Out-of-process setup unavailable; running setup directly in-process.');
    Exec('cmd.exe',
         '/c cd /d "' + InstallDir + '" && call "' + SetupBat + '" /silent > "' + SetupLog + '" 2>&1',
         InstallDir, SW_HIDE, ewWaitUntilTerminated, ResultCode);
  end;

  // Post-condition validation (backstop for ANY setup failure — the
  // scheduled-task path, the in-process fallback, antivirus quarantine of
  // _internal\python311.dll, a failed PyInstaller build, etc.). If the files
  // the launcher needs at runtime aren't present, show the guided
  // manual-finish dialog rather than declaring success.
  PythonDll := InstallDir + '\_internal\python311.dll';
  VenvPython := InstallDir + '\.venv\Scripts\python.exe';
  if (not FileExists(PythonDll)) or (not FileExists(VenvPython)) then
  begin
    MissingArtifacts := '';
    if not FileExists(VenvPython) then
      MissingArtifacts := MissingArtifacts + '  - ' + VenvPython + #13#10;
    if not FileExists(PythonDll) then
      MissingArtifacts := MissingArtifacts + '  - ' + PythonDll + #13#10;

    SetupLogContent := '';
    if FileExists(SetupLog) then
      LoadStringFromFile(SetupLog, SetupLogContent);
    if Length(SetupLogContent) = 0 then
      SetupLogContent := '(setup produced no captured output)';

    // Tail of the setup log — errors are usually near the end.
    LogTail := String(SetupLogContent);
    LogLen := Length(LogTail);
    if LogLen > 1500 then
      LogTail := '...' + Copy(LogTail, LogLen - 1496, 1500);

    ShowSetupFailedDialog(InstallDir,
      'Missing files:' + #13#10 + MissingArtifacts + #13#10 +
      'setup output (tail):' + #13#10 + LogTail);
  end;

  // ---- Step 4: Copy config key ----
  SetProgress(90);
  UpdateStatus('Copying encryption key from network share...');
  ConfigSrc := '{#MyNetworkKey}';
  ConfigDst := InstallDir + '\CODE - do not open\config.key';
  if FileExists(ConfigSrc) then
  begin
    CopyFile(ConfigSrc, ConfigDst, False);
    if FileExists(ConfigDst) then
      Log('config.key copied successfully.')
    else
      Log('WARNING: config.key copy failed.');
  end
  else
    Log('WARNING: Network key not found at ' + ConfigSrc + '. Key will be copied on first launch.');

  SetProgress(100);
  UpdateStatus('Installation complete!');
end;

// -------------------------------------------------------
// Offer to launch the app after install
// -------------------------------------------------------
procedure CurPageChanged(CurPageID: Integer);
begin
  if CurPageID = wpFinished then
    WizardForm.RunList.Visible := True;
end;

[Icons]
Name: "{userprograms}\{#MyAppName}"; Filename: "{app}\CNA Web App.exe"; WorkingDir: "{app}"; IconFilename: "{app}\cna_icon.ico"; Comment: "Launch CNA Console"

[Run]
Filename: "{app}\CNA Web App.exe"; WorkingDir: "{app}"; Description: "Launch CNA Console"; Flags: nowait postinstall skipifsilent
