---
name: azure-devops-gcm-usehttppath
description: dev.azure.com pulls via Git Credential Manager need credential.useHttpPath=true or GCM can't find the org and git falls back to a terminal prompt
metadata:
  type: project
---

For the GitHub→Azure self-update migration, the app's update remote moves to the private Azure DevOps **cloud** repo `https://dev.azure.com/ClarkAssociates/CNA Reporting and Analytics/_git/cna-console`. A plain `git ls-remote`/`pull` against a `dev.azure.com/{org}/...` URL **drops to a terminal username/password prompt** instead of the expected one-time browser sign-in — even with GCM installed and `credential.helper=manager` set.

Cause (confirmed via GCM trace): GCM auto-selects the "Azure Repos" provider but, by default, only receives `host=dev.azure.com` with **no path**, so it can't determine the org name and bails with *"Cannot determine the organization name for this 'dev.azure.com' remote URL."* git then falls back to its built-in prompt — which **silently fails** in the background update watcher because that runs with `GIT_TERMINAL_PROMPT=0`.

**Why it matters:** the whole "sign in once, silent after" update model depends on GCM engaging and caching the Entra credential in Windows Credential Manager. Without the org, GCM never engages, so updates would just stop with no visible error.

**How to apply:** set `git config --global credential.https://dev.azure.com.useHttpPath true` (scoped to the host — affects nothing else). Then the first pull opens an Entra ID browser sign-in (cached per-org, so it covers every repo in the `ClarkAssociates` org); subsequent pulls are silent. The one-time GitHub→Azure flip (`_maybe_flip_to_azure()` in `check_updates.py`) **must set this on every user's machine** as part of re-pointing `origin`, alongside `credential.helper manager`. Verified on Git 2.53 / GCM 2.7.3: with the flag set, the "cannot determine organization" error disappears and GCM proceeds to OAuth (a non-interactive probe then reports only "interactivity disabled", i.e. it would have prompted). Note: GCM's `azure-repos` provider hint already exists for the on-prem `tfs.clarkinc.biz` host, but that is a separate server and irrelevant to the cloud migration.
