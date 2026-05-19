---
name: gotchas_packaging_estimator_config
description: Packaging estimator reads a shipping_calculator_api config section that doesn't match where config.py defines those settings — values fall back to hardcoded defaults
metadata:
  type: project
---

`pages/packaging-estimator.py` reads runtime API settings from a `shipping_calculator_api` section, but `config.py` currently defines those transport settings under `PACKAGING_CONFIG["api"]`. Because of the mismatch, some config values silently fall back to hardcoded defaults in the page.

**Why:** This is a latent bug — changing the API URL/timeout/credentials in `config.py` may have no effect because the page never reads from that key. Reported as a known caveat but not yet fixed.

**How to apply:**
- If a packaging API config change "doesn't take effect," check both `PACKAGING_CONFIG["api"]` in `config.py` AND the `shipping_calculator_api` reads in the page — they need to be unified.
- Don't assume `config.json` (at `CODE - do not open/config.json`) is the source of truth either — it's [legacy](legacy_artifacts.md).
- Reference data (item info, warehouses) IS read from `PACKAGING_CONFIG` and works correctly. The mismatch is only for the shipping calculator API transport settings.

Also worth knowing: the page contains simulation and recommendation helpers more elaborate than the visible UI exposes. The active workflow is `Load Items` → `Run Estimate`; the rest is half-wired infrastructure. Don't assume something is in use just because it's defined.
