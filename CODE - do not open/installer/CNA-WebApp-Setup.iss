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
DisableProgramGroupPage=yes
; Keep the directory page enabled. The default ({localappdata}) works for
; most users. On machines where the user profile is a mounted container
; (FSLogix) or AppData is junctioned to a network share, every path under
; the profile sits behind a Windows "untrusted mount point" and uv fails
; with error 448 while building the venv. A pre-check at the start of
; installation (see CurStepChanged) detects a reparse point anywhere in
; the install path's ancestry and offers to relaunch Setup into
; C:\Users\Public\CNA-WebApp, which is off-profile. Post-install
; validation still catches the 448 case as a backstop.
DisableDirPage=no
OutputDir=..\..\installer-output
OutputBaseFilename=CNA-WebApp-Setup
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

// -------------------------------------------------------
// Win32 API imports — used by the untrusted-mount-point
// pre-check and to terminate this instance for a relaunch.
// -------------------------------------------------------
function GetFileAttributesW(lpFileName: String): Cardinal;
  external 'GetFileAttributesW@kernel32.dll stdcall';
procedure TerminateInstaller(uExitCode: Cardinal);
  external 'ExitProcess@kernel32.dll stdcall';

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
          'The selected folder already exists and does not appear to contain CNA Console. ' +
          'Files may be overwritten.' + #13#10#13#10 +
          'Continue anyway?',
          mbConfirmation, MB_YESNO) = IDYES);
      end;
    end;
  end;
end;

// -------------------------------------------------------
// Helper: is this exact path a reparse point (junction,
// symlink, mounted container)? Returns False for paths
// that don't exist.
// -------------------------------------------------------
function IsReparsePoint(const Path: String): Boolean;
var
  Attrs: Cardinal;
begin
  Attrs := GetFileAttributesW(Path);
  // $FFFFFFFF = INVALID_FILE_ATTRIBUTES, $400 = FILE_ATTRIBUTE_REPARSE_POINT
  Result := (Attrs <> $FFFFFFFF) and ((Attrs and $400) <> 0);
end;

// -------------------------------------------------------
// Helper: does the install path — or ANY of its ancestor
// directories — sit on a reparse point? This is what makes
// uv fail with Windows error 448 ("untrusted mount point"):
// a process can't traverse a junction / mounted container it
// doesn't trust. The usual culprit is a corporate roaming
// profile or FSLogix container at C:\Users\<user>, which
// poisons every path beneath it regardless of install dir.
// -------------------------------------------------------
function InstallPathHasReparsePoint(const StartPath: String): Boolean;
var
  Cur, Prev: String;
begin
  Result := False;
  Cur := StartPath;
  repeat
    if IsReparsePoint(Cur) then
    begin
      Result := True;
      Exit;
    end;
    Prev := Cur;
    Cur := ExtractFileDir(Cur);
  until (Cur = Prev) or (Length(Cur) < 3);
end;

// -------------------------------------------------------
// Main install logic
// -------------------------------------------------------
procedure CurStepChanged(CurStep: TSetupStep);
var
  InstallDir, ConfigSrc, ConfigDst, SetupBat, GitExePath, CloneLog, SetupLog: String;
  PythonDll, VenvPython, MissingArtifacts, LogTail, Tip: String;
  CloneLogContent, SetupLogContent: AnsiString;
  ResultCode, WingetCode, LogLen: Integer;
  Btns: TArrayOfString;
begin
  if CurStep <> ssInstall then
    Exit;

  InstallDir := ExpandConstant('{app}');

  // ---- Step 0: untrusted-mount-point pre-check ----
  // Before doing ANY work, make sure the install path can actually be
  // traversed. If a reparse point sits anywhere in its ancestry (roaming
  // profile / FSLogix container / AppData junctioned to a network share),
  // uv will later fail with Windows error 448 while building the venv.
  // Catch it now and offer to relaunch into an off-profile location.
  if InstallPathHasReparsePoint(InstallDir) then
  begin
    SetArrayLength(Btns, 2);
    Btns[0] := 'OK';
    Btns[1] := 'Exit';
    if TaskDialogMsgBox(
         'This install location can''t be used',
         'The folder you selected:' + #13#10 +
         '    ' + InstallDir + #13#10#13#10 +
         'sits behind a Windows "untrusted mount point". Your Windows user ' +
         'profile is most likely stored on a network share or a mounted ' +
         'container — common with corporate roaming profiles or FSLogix. ' +
         'Installing here will fail when Setup builds the Python environment ' +
         '(Windows error 448).' + #13#10#13#10 +
         'Setup can install to an off-profile location that avoids this:' + #13#10 +
         '    C:\Users\Public\CNA-WebApp' + #13#10#13#10 +
         'Click OK to restart Setup at that location now, or Exit to quit.',
         mbError, MB_OKCANCEL, Btns, 0) = IDOK then
    begin
      // Relaunch this same installer pointed at the off-profile path. The
      // fresh instance re-runs this very check, which will pass for Public.
      Exec(ExpandConstant('{srcexe}'),
           '/SILENT /DIR="C:\Users\Public\CNA-WebApp"',
           '', SW_SHOW, ewNoWait, ResultCode);
    end;
    // Either way this instance is done — it must not install to the bad
    // path. On OK, the relaunched instance has already taken over.
    TerminateInstaller(0);
  end;

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

    // Pick a tailored hint based on what's in the log. Two common failure
    // modes we know about:
    //   - "untrusted mount point" (Windows error 448): corporate folder
    //     redirection on AppData. Fix: install to a non-redirected dir.
    //   - Default: antivirus quarantine of a PyInstaller-built file.
    if Pos('untrusted mount point', LowerCase(LogTail)) > 0 then
      Tip := 'The install location is behind a Windows "untrusted mount point" ' +
             '(Windows error 448). This happens when the user profile is a ' +
             'mounted container (FSLogix) or AppData is redirected to a network ' +
             'share — uv cannot traverse into the virtual environment.' + #13#10#13#10 +
             'Fix: re-run this installer and install to a location outside your ' +
             'user profile instead — for example:' + #13#10 +
             '  C:\Users\Public\CNA-WebApp'
    else
      Tip := 'This usually means antivirus quarantined a file during install ' +
             '(Windows Defender frequently flags PyInstaller-built executables). ' +
             'Check your antivirus quarantine and whitelist ' + InstallDir + ', ' +
             'then re-run setup.bat from that folder.';

    MsgBox(
      'Setup finished, but the app is not ready to launch.' + #13#10#13#10 +
      'Missing required files:' + #13#10 + MissingArtifacts + #13#10 +
      'setup.bat exit code: ' + IntToStr(ResultCode) + #13#10#13#10 +
      Tip + #13#10#13#10 +
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
Name: "{userprograms}\{#MyAppName}"; Filename: "{app}\CNA Web App.exe"; WorkingDir: "{app}"; IconFilename: "{app}\cna_icon.ico"; Comment: "Launch CNA Console"

[Run]
Filename: "{app}\CNA Web App.exe"; WorkingDir: "{app}"; Description: "Launch CNA Console"; Flags: nowait postinstall skipifsilent
