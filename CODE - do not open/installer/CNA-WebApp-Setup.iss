; ============================================================
; CNA Web App Installer — Inno Setup Script
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

#define MyAppName "CNA Web App"
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
DisableProgramGroupPage=yes
DisableDirPage=yes
OutputDir=..\..\installer-output
OutputBaseFilename=CNA-WebApp-Setup
SetupIconFile=..\..\cna_icon.ico
UninstallDisplayIcon={localappdata}\CNA-WebApp\cna_icon.ico
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
          'The selected folder already exists and does not appear to contain the CNA Web App. ' +
          'Files may be overwritten.' + #13#10#13#10 +
          'Continue anyway?',
          mbConfirmation, MB_YESNO) = IDYES);
      end;
    end;
  end;
end;

// -------------------------------------------------------
// Main install logic
// -------------------------------------------------------
procedure CurStepChanged(CurStep: TSetupStep);
var
  InstallDir, ConfigSrc, ConfigDst, SetupBat, GitExePath, CloneLog, SetupLog: String;
  PythonDll, VenvPython, MissingArtifacts, LogTail: String;
  CloneLogContent, SetupLogContent: AnsiString;
  ResultCode, WingetCode, LogLen: Integer;
begin
  if CurStep <> ssInstall then
    Exit;

  InstallDir := ExpandConstant('{app}');
  SetProgress(0);

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
    UpdateStatus('Cloning CNA Web App from GitHub...');
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
  Exec('cmd.exe',
       '/c cd /d "' + InstallDir + '" && call "' + SetupBat + '" /silent > "' + SetupLog + '" 2>&1',
       InstallDir, SW_HIDE, ewWaitUntilTerminated, ResultCode);

  if ResultCode <> 0 then
    Log('setup.bat returned non-zero exit code: ' + IntToStr(ResultCode));

  // Post-condition validation: setup.bat is best-effort and historically
  // swallows several failure modes (PyInstaller build, move/xcopy, antivirus
  // quarantine of _internal\python311.dll). Verify the artifacts the launcher
  // actually needs at runtime before reporting success.
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
      SetupLogContent := '(setup.bat produced no captured output)';

    // Show the tail of the setup log — errors are usually near the end.
    LogTail := String(SetupLogContent);
    LogLen := Length(LogTail);
    if LogLen > 2500 then
      LogTail := '...' + Copy(LogTail, LogLen - 2496, 2500);

    MsgBox(
      'Setup finished, but the app is not ready to launch.' + #13#10#13#10 +
      'Missing required files:' + #13#10 + MissingArtifacts + #13#10 +
      'setup.bat exit code: ' + IntToStr(ResultCode) + #13#10#13#10 +
      'This usually means antivirus quarantined a file during install ' +
      '(Windows Defender frequently flags PyInstaller-built executables). ' +
      'Check your antivirus quarantine and whitelist ' + InstallDir + ', ' +
      'then re-run setup.bat from that folder.' + #13#10#13#10 +
      'setup.bat output (tail):' + #13#10 + LogTail,
      mbError, MB_OK);
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
Name: "{userprograms}\{#MyAppName}"; Filename: "{app}\CNA Web App.exe"; WorkingDir: "{app}"; IconFilename: "{app}\cna_icon.ico"; Comment: "Launch CNA Web App"

[Run]
Filename: "{app}\CNA Web App.exe"; WorkingDir: "{app}"; Description: "Launch CNA Web App"; Flags: nowait postinstall skipifsilent
