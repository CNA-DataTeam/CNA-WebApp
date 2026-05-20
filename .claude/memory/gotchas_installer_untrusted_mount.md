---
name: Installer untrusted-mount-point (error 448) detection
description: Why the installer's untrusted-mount pre-check must be a functional junction probe, not a reparse-point attribute check
metadata:
  type: project
---

On corporate machines where the Windows profile is a mounted container (FSLogix) or AppData is redirected to a network share, `uv venv` fails with `os error 448` ("The path cannot be traversed because it contains an untrusted mount point") at the step **"Failed to create Python minor version link directory"**.

**Root cause:** uv builds the venv by creating a **directory junction** for the Python minor-version link (its transparent patch-upgrade mechanism — e.g. `cpython-3.11` → `cpython-3.11.15-...` under `UV_PYTHON_INSTALL_DIR`). Creating a junction whose path is inside an untrusted mount is what the filesystem blocks with 448. `UV_LINK_MODE=copy` does NOT help — that only governs package installs into site-packages, not the minor-version junction. Pinning `UV_PYTHON_INSTALL_DIR`/`UV_CACHE_DIR` under the install tree doesn't help either if the install tree is itself inside the mount.

**The detection trap (fixed May 2026):** The installer's pre-check originally walked the install path's ancestry calling `GetFileAttributesW` looking for `FILE_ATTRIBUTE_REPARSE_POINT`. An FSLogix profile is a mounted **VHD volume**, not a reparse point — so the attribute check never fires, the pre-check passes, and uv dies halfway through. The post-install validator then catches it (the "Setup finished, but the app is not ready" dialog).

**Why:** you cannot reliably *infer* the untrusted-mount condition from path attributes. You must *perform the operation*.

**How to apply:** the fix in `CNA-Console-Installer.iss` is `CanCreateJunction(Dir)` — it actually runs `mklink /J` in the install dir (no admin / developer mode needed for junctions) and checks the result. If it can't create one, Setup relaunches itself `/SILENT /DIR="C:\Users\Public\CNA-WebApp"` (off-profile, junctions allowed). There's a loop guard: if we're already at the Public path and it STILL fails, show a hard error instead of relaunching forever. See [[project_installer_flow.md]] for the rest of the install sequence.
