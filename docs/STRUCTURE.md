# Proposed Documentation Layout

This repository currently keeps all guidance in `README.md`. To make documentation easier to maintain and find, use the layout below and place documents in the `docs/` directory.

## Recommended placement
- `README.md` → short project overview + links to deeper docs.
- `docs/README.md` → documentation index and entry point.
- `docs/SETUP.md` → first-time setup and local run instructions.
- `docs/TROUBLESHOOTING.md` → common issues and fixes.
- `docs/OPERATIONS.md` → day-to-day operational notes (logs, scheduled jobs, data locations).
- `docs/ARCHITECTURE.md` → data flow, storage layout, and module boundaries.

## Why this structure?
- Reduces the root README length.
- Keeps instructions organized by purpose.
- Makes onboarding and updates simpler for non-developers.
