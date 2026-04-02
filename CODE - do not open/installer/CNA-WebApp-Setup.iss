; ============================================================
; CNA Web App Installer — Inno Setup Script
; ============================================================
; Compile this with Inno Setup 6+ to produce the installer exe.
; Download Inno Setup: https://jrsoftware.org/isinfo.php
;
; What this installer does:
;   1. Checks for Git — installs via winget if missing
;   2. Clones the CNA-WebApp repo to the chosen directory
;   3. Runs setup.bat (installs uv, Python 3.11, venv, dependencies, shortcut)
;   4. Copies config.py from the network share
; ============================================================

#define MyAppName "CNA Web App"
#define MyAppVersion "1.0"
#define MyAppPublisher "Clark National Accounts"
#define MyRepoURL "https://github.com/CNA-DataTeam/CNA-WebApp.git"
#define MyNetworkConfig "\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.py"

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
// Helper: update status label on the installing page
// -------------------------------------------------------
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
  InstallDir, ConfigSrc, ConfigDst, SetupBat: String;
  ResultCode: Integer;
  GitInstalled: Boolean;
begin
  if CurStep <> ssInstall then
    Exit;

  InstallDir := ExpandConstant('{app}');

  // ---- Step 1: Git ----
  UpdateStatus('Checking for Git...');
  GitInstalled := CommandExists('git');

  if not GitInstalled then
  begin
    UpdateStatus('Installing Git via winget (this may take a minute)...');
    ResultCode := RunAndWait('cmd.exe',
      '/c winget install --id Git.Git -e --silent --accept-package-agreements --accept-source-agreements',
      '');

    // Refresh PATH from registry so we can find git
    ResultCode := RunAndWait('cmd.exe', '/c git --version', '');
    if ResultCode <> 0 then
    begin
      // Try common install path directly
      if FileExists('C:\Program Files\Git\cmd\git.exe') then
        GitInstalled := True
      else
      begin
        MsgBox(
          'Git could not be installed automatically.' + #13#10#13#10 +
          'Please install Git manually from https://git-scm.com and re-run this installer.',
          mbError, MB_OK);
        WizardForm.Close;
        Exit;
      end;
    end
    else
      GitInstalled := True;
  end;

  // ---- Step 2: Clone repo ----
  if FileExists(InstallDir + '\setup.bat') then
  begin
    UpdateStatus('Existing installation detected — pulling latest...');
    if GitInstalled and CommandExists('git') then
      RunAndWait('cmd.exe', '/c cd /d "' + InstallDir + '" && git pull', InstallDir)
    else
      RunAndWait('cmd.exe',
        '/c cd /d "' + InstallDir + '" && "C:\Program Files\Git\cmd\git.exe" pull',
        InstallDir);
  end
  else
  begin
    UpdateStatus('Cloning CNA Web App from GitHub...');
    if GitInstalled and CommandExists('git') then
      RunAndWait('cmd.exe',
        '/c git clone "{#MyRepoURL}" "' + InstallDir + '"', '')
    else
      RunAndWait('cmd.exe',
        '/c "C:\Program Files\Git\cmd\git.exe" clone "{#MyRepoURL}" "' + InstallDir + '"', '');
  end;

  if not FileExists(InstallDir + '\setup.bat') then
  begin
    MsgBox(
      'Failed to clone the repository. Please check your internet connection and try again.',
      mbError, MB_OK);
    WizardForm.Close;
    Exit;
  end;

  // ---- Step 3: Run setup.bat ----
  UpdateStatus('Running setup (installing Python, dependencies, creating shortcut)...');
  SetupBat := InstallDir + '\setup.bat';
  // Run setup.bat in its own visible window so user can see progress
  Exec('cmd.exe', '/c cd /d "' + InstallDir + '" && call "' + SetupBat + '"',
       InstallDir, SW_SHOW, ewWaitUntilTerminated, ResultCode);

  if ResultCode <> 0 then
    Log('setup.bat returned non-zero exit code: ' + IntToStr(ResultCode));

  // ---- Step 4: Copy config.py ----
  UpdateStatus('Copying config.py from network share...');
  ConfigSrc := '{#MyNetworkConfig}';
  ConfigDst := InstallDir + '\config.py';
  if FileExists(ConfigSrc) then
  begin
    CopyFile(ConfigSrc, ConfigDst, False);
    if FileExists(ConfigDst) then
      Log('config.py copied successfully.')
    else
      Log('WARNING: config.py copy failed.');
  end
  else
    Log('WARNING: Network config not found at ' + ConfigSrc + '. config.py will be synced on first launch.');

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
Name: "{userprograms}\{#MyAppName}"; Filename: "wscript.exe"; Parameters: """{app}\StartApp.vbs"""; WorkingDir: "{app}"; IconFilename: "{app}\cna_icon.ico"; Comment: "Launch CNA Web App"

[Run]
Filename: "wscript.exe"; Parameters: """{app}\StartApp.vbs"""; WorkingDir: "{app}"; Description: "Launch CNA Web App"; Flags: nowait postinstall skipifsilent
