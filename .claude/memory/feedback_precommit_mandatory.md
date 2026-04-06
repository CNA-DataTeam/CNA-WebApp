---
name: Pre-commit steps are non-negotiable
description: Never use soft language like "best-effort" or "skip if unavailable" for pre-commit build steps — all three are mandatory
type: feedback
---

All three pre-commit steps (rebuild exe, encrypt config, rebuild installer) are mandatory. Do not qualify any of them as "best-effort", "optional", or "skip if not installed".

**Why:** The user explicitly corrected this in April 2026. The installer rebuild was originally documented as "best-effort — skip if Inno Setup not installed", but the user pointed out that running a `.bat` file is straightforward and shouldn't be treated as optional. `RebuildInstaller.bat` now auto-installs Inno Setup via winget if missing, so there's no reason to skip it.

**How to apply:** When documenting or implementing pre-commit steps, treat all three as equally required. If a step fails, stop and report — don't silently skip. If a tool (like Inno Setup) is missing, install it automatically rather than skipping the step.
