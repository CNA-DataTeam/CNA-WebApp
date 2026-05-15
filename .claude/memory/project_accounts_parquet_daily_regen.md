---
name: project_accounts_parquet_daily_regen
description: New columns added to startup.py's accounts extraction only appear after the daily accounts parquet regenerates
metadata:
  type: project
---

`startup.py` writes `accounts_<YYYY-MM-DD>.parquet` once per day and skips
regeneration if today's file already exists. So when you add a column to
`load_accounts_excel` (e.g. `Reporting Name`, added alongside `Company Group USE`
and `CustomerCode`), existing same-day parquet files will NOT have it until the
file is regenerated — either next calendar day or by deleting today's file from
the personnel share so startup rebuilds it.

**Why:** Consumers like `utils.load_account_lookup` must degrade gracefully
(it falls back to `Company Group USE` when `Reporting Name` is absent), and
testers can be confused when a new column "isn't there yet."

**How to apply:** After changing accounts extraction, expect a one-day lag for
the column to appear in production, or delete the current `accounts_*.parquet`
to force an immediate rebuild. Always make readers tolerate the old schema.
