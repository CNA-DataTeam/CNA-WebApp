---
name: legacy_artifacts
description: Files in the repo that look authoritative but are stale or legacy — don't trust them as source of truth
metadata:
  type: project
---

A few files in this repo look load-bearing but are actually legacy or stale:

1. **`CODE - do not open/config.json`** — appears to be legacy packaging config. Not the current source of truth. Packaging settings come from `config.py`'s `PACKAGING_CONFIG` (with the [shipping API key mismatch caveat](gotchas_packaging_estimator_config.md)).

2. **`scripts/validate_lake.py`** — references older schema names like `UserName` instead of `UserLogin`. If you run it as a sanity check, expect it to throw on column lookups. Either skip it or fix the schema references first.

3. **Bottom of `pages/packaging-estimator.py`** — contains commented "legacy packager reference" code. Don't treat it as documentation of current behavior.

4. **Older docs in `CODE - do not open/docs/`** — predate the current architecture in places. Cross-check with the live code before relying on them.

**Why this matters:** These show up in grep results and can mislead. The repo has had several architecture passes and not everything got cleaned up.

**How to apply:** If something contradicts the live code or CLAUDE.md, the live code wins. Don't extend or "fix" the legacy artifacts above without first checking whether they're still in use — they may be safe to delete instead.
