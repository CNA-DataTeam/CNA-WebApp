# CNA Console — Install Troubleshooting (Untrusted Mount Point / Error 448)

Call-usable guide. The error is `OS error 448 — "The path cannot be traversed
because it contains an untrusted mount point"`, thrown by `uv` while building the
Python venv. It appears as **"Setup finished, but the app is not ready to launch."**

---

## 0. Before anything: make sure they have the CURRENT installer

The fix (junction probe + auto-relaunch) shipped in release **v1.0.1** on
**2026-05-20 ~5:32 PM ET**. Anything downloaded before that is the old installer
*without* the probe.

**Have them re-download fresh:**
`https://github.com/CNA-DataTeam/CNA-WebApp/releases/latest/download/CNA-Console-Installer.exe`

---

## 1. Two 10-second probes (run these first)

Open **Command Prompt** (cmd, *not* PowerShell). If you must use PowerShell,
prefix each line with `cmd /c`.

**Probe A — is the profile actually affected? (reproduce the failure)**
```
mklink /J "%LOCALAPPDATA%\_probe" "%LOCALAPPDATA%"
rmdir "%LOCALAPPDATA%\_probe"
```
On an affected machine, the first line errors with the untrusted-mount message.

**Probe B — does the off-profile fallback work?**
```
mklink /J "C:\Users\Public\_probe" "C:\Users\Public"
rmdir "C:\Users\Public\_probe"
```
Success here means the fix's target location (`C:\Users\Public\CNA-WebApp`) will work.

---

## 2. Decision tree

```
Probe A fails (448), Probe B succeeds   → NORMAL affected machine.
                                          Install to Public (Section 3). This is the
                                          designed happy path.

Probe A fails, Probe B ALSO fails        → Read the error text first (Section 4).
                                          Test one more local path (C:\Temp).
                                          If all local paths fail → IT (Section 4).

Probe A and B both succeed, but the      → It's NOT mount points. Stop chasing 448.
install still fails                        Diagnose the real error (Section 5).
```

---

## 3. Happy path — install to an off-profile location

**Option A (let the installer do it):** Run the current installer with the default
location. The probe catches the bad path, you click **OK**, and it silently
relaunches into `C:\Users\Public\CNA-WebApp`. Because it relaunches `/SILENT`,
there's no "launch now" checkbox at the end — start the app from the **Start Menu
shortcut ("CNA Console")** or `CNA Console.lnk` in the install folder.

**Option B (skip the round-trip):** On the installer's directory page, manually type
`C:\Users\Public\CNA-WebApp` (or any local NTFS path outside the profile, e.g.
`C:\CNA-WebApp`). Same outcome, no relaunch.

> Leftover files in `%LOCALAPPDATA%\CNA-WebApp` from a prior failed attempt do **not**
> interfere with a Public install — see FAQ. Cleaning them up is harmless but not required.

---

## 4. Both probes fail — read the error, then escalate

`mklink` fails for different reasons. **Read the actual message:**

| Message | Meaning | Action |
|---|---|---|
| "...untrusted mount point" (448) | Real mount-point block at that location | Test `C:\Temp` (below); if it fails too → IT |
| "Access is denied" / blocked by security software | Policy blocks junctions machine-wide | IT |
| "Local NTFS volumes are required..." | Path isn't on a local NTFS volume | Use a different local path |
| "Cannot create a file when that file already exists" | Stale probe folder | `rmdir` it, retry — not a real failure |

**Try one more genuinely-local path before calling IT:**
```
mkdir C:\Temp 2>nul
mklink /J "C:\Temp\_probe" "C:\Temp"
rmdir "C:\Temp\_probe"
```
If this works, install there. If **every** local NTFS path fails, it's an IT issue.

**What to tell IT (use this exact wording):**
> "A profile-virtualization or security policy is blocking the creation of
> **directory junctions (NTFS reparse points)** on this machine. Our app's Python
> setup (`uv`) requires this. We need either an exclusion that permits junction
> creation, or a local, non-virtualized folder where it's allowed."

---

## 5. Both probes pass but install still fails — it's NOT mount points

Pivot. Two ways to find the real error fast:

**5a. Read the failure dialog — it self-diagnoses.**
- **"Failed to clone the repository"** → git / network / proxy / SSL-inspection issue.
  The dialog prints the git output.
- **"Setup finished, but the app is not ready to launch"** → shows which files are
  missing + a tailored tip. If the tip mentions **antivirus** (not 448), it's almost
  certainly **Windows Defender quarantining `_internal\python311.dll`**. Fix: restore
  it from Protection History, add the install folder to Defender exclusions, re-run.

**5b. Re-run setup manually to SEE the error (highest-value move).**
By the time either dialog appears, the repo is **already cloned** to the install
folder. Re-run setup *without* `/silent` so it stops on errors and prints everything:
```
cd /d C:\Users\Public\CNA-WebApp
setup.bat
```
Watch which step fails. Likely culprits when junctions work:
- **AV quarantine** during the PyInstaller build (missing `python311.dll`).
- **Proxy / SSL inspection** blocking a download — the `uv` installer (`astral.sh`),
  the Python 3.11 download, or PyPI packages. The output shows the failing URL.

> ⚠️ The installer's own logs (`cna-git.log`, `cna-setup.log`) live in Inno Setup's
> temp dir, which is **deleted when Setup exits**. Either copy them out *while the
> error dialog is still open*, or just use the manual `setup.bat` re-run above.

---

## 6. Manual setup.bat works, but the installer fails EVERY time

This feels contradictory (same script, same machine) but is a classic trap:
**`setup.bat` is idempotent and silently skips the step that throws 448.**

- `setup.bat` prints **"Python 3.11 already installed. Skipping."** and **"Virtual
  environment already exists."** when a prior attempt left a `.venv` / `.uv\python`
  behind. The 448 comes from `uv venv` / `uv python install` — the exact steps it skips.
  So a manual re-run can "work" simply because it never ran the failing operation.

**Ask on the call:** *"When you ran setup.bat by hand, did it say 'Creating virtual
environment' or 'Virtual environment already exists'?"* If "already exists," the manual
run never tested the failing step.

**Decisive test — force a true from-scratch run:**
```
cd /d <the folder where manual setup worked>
rmdir /s /q .venv
rmdir /s /q .uv
setup.bat
```
- Now throws 448 → the location genuinely can't build the venv. The bare `mklink`
  probe only proves junction *creation*; uv must also *traverse into* the junction to
  extract Python, and a mount can block traversal even when creation works.
  → **Install to Public.**
- Still works → it's an execution-*context* difference, not location:
  1. **Elevation** — was the installer run "as administrator"? An elevated installer
     resolves `%LOCALAPPDATA%`/`%USERPROFILE%` to a *different* profile than your manual
     test. Run it by normal double-click, or install to `C:\Users\Public\CNA-WebApp`
     (same path regardless of who runs it).
  2. **Different target folder** — confirm the installer's target matches the folder you
     tested. If a relaunch already built a working install in Public, just use that one.

**Escape hatch:** if manual setup.bat works, you don't need the installer at all. It
only installs Git, clones, runs setup.bat, and copies config.key. A manual install is:
```
git clone https://github.com/CNA-DataTeam/CNA-WebApp.git "C:\Users\Public\CNA-WebApp"
cd /d "C:\Users\Public\CNA-WebApp"
setup.bat
```
setup.bat copies config.key from the share and creates the shortcut itself. Done.

---

## FAQ

**Q: First install to LocalAppData failed with 448. Without cleaning up, I retried at
Public and it also failed. Could the leftover LocalAppData files be the cause?**

**No.** The installer keys everything off the *chosen* install folder. The probe tests
Public, the clone goes to Public, and `setup.bat` pins uv's Python-install dir, cache
dir, and venv all under Public. A Public install never reads the old LocalAppData
files — they're inert. If Public *also* fails with 448, it almost certainly means
**Public itself can't host junctions** on that machine (whole `C:\Users` or disk is
virtualized, or a machine-wide policy block) — prove it with the bare Probe B, which
uses zero CNA files. Cleaning up LocalAppData is harmless but won't fix a real Public
block.

---

## Appendix — why this happens

`uv` builds the venv by creating a **directory junction** for the Python minor-version
link (its transparent patch-upgrade mechanism). When the Windows profile is a mounted
container (**FSLogix**) or AppData is redirected to a network share, the default path
(`%LOCALAPPDATA%\CNA-WebApp`) sits inside that mount, and Windows blocks junction
creation there → error 448. An FSLogix VHD mount has **no** reparse-point attribute,
which is why the installer must *test* junction creation (`mklink /J`) rather than
sniff path attributes. `UV_LINK_MODE=copy` does **not** help — it only governs package
installs, not the minor-version junction.
