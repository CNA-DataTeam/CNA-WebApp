---
name: route1-github-to-azure-flip
description: How the desktop app's self-update is migrated from public GitHub to the private Azure repo cna-console — graft seed (no force-push), unconditional marker-gated flip
metadata:
  type: project
---

Route 1 of the Azure migration moves the desktop app's self-update source from the public GitHub repo (`CNA-DataTeam/CNA-WebApp`, anonymous pulls) to the **private** Azure DevOps cloud repo `https://dev.azure.com/ClarkAssociates/CNA%20Reporting%20and%20Analytics/_git/cna-console`. Mechanism verified end-to-end in a local sandbox rehearsal (2026-06-29).

Mechanism:
- **The flip:** `check_updates.py` gains `_maybe_flip_to_azure()` — idempotent, marker-gated (`.migrated_to_azure` in `APP_DIR`), fail-open — called **unconditionally at the top of `run_check()`**, BEFORE the `remote_is_ahead()` early-return. It must NOT be gated behind a successful pull (the original code-review suggestion of the 240-243 post-pull block): the flip code arrives *in* the final GitHub pull, so the run that pulls it executes the OLD code; by the next launch there's nothing to pull, so a post-pull-gated flip would never fire. It checks `origin` contains `github.com`, then `git remote set-url origin <azure>`, sets local `credential.helper=manager` + `credential.https://dev.azure.com.useHttpPath=true` (see [[azure-devops-gcm-usehttppath]]), writes the marker.
- **Delivery:** the flip ships as the *final* commit to the public GitHub repo. Users pull it via the normal "Update Available" dialog; the next launch flips them silently to Azure. GitHub stays frozen until adoption is complete, then retired.
- **Seeding Azure (no force-push):** the account **lacks the Git ForcePush permission** on the Azure repo (`TF401027`), so `main` can't be overwritten. Instead **graft**: from the app history, `git merge -s ours --allow-unrelated-histories <azure-current-tip>` (result tree = app; second parent = the Backstage "Initial commit"), then a *normal* push — it fast-forwards from Azure's current tip (no force) AND keeps users' GitHub commits as ancestors of the new tip, so their post-flip `pull --ff-only` still fast-forwards cleanly.

**Why:** updates are seamless today only because GitHub is public (anonymous pulls). Azure is private, so the flip must also wire up silent GCM auth. The graft is the only seed method available without ForcePush.

**Remaining UX piece — per-user first sign-in:** after the flip, each user's *first* Azure pull needs their own cached Entra credential. The sandbox proved the silent steady-state (credential already cached on the dev machine); the in-app "sign in once to keep updating" prompt (`app.py`) for the very first Azure auth still needs building and testing (clear the credential / test on a fresh machine to exercise it). On a fresh machine the background `check_updates` runs with `GIT_TERMINAL_PROMPT=0`, so a no-cached-cred pull would fail silently unless GCM's GUI engages or the app surfaces the prompt.

**How to apply:** reuse `scratchpad/flip_inject.py` (injects the flip into a check_updates.py copy) for the real Phase 1 edit. Rehearse any change to the flip by rebuilding the fake-GitHub→Azure sandbox at a SHORT path (Windows MAX_PATH breaks local bare-repo pushes under the long scratchpad path). Plan lives in `~/.claude/plans/this-app-is-currently-spicy-pony.md` (Route 1 section).
