---
name: gotchas_time_allocation_name_cleanup
description: Time Allocation "Fix entry names" cleanup — repairs Full Name saved as a Windows login; plus the two parquet-rewrite traps it must avoid
metadata:
  type: project
---

`ta_store.repair_fullnames(base_dir, login_to_fullname, dry_run=)` repairs saved
Time Allocation rows whose **"Full Name" was stored as the user's Windows login**
(the disconnected-save bug — see [[project_user_profile_offline_cache]]). For each
row, when its "User" login maps to a known full name AND the stored name is blank
or equals that same login, it sets the real name. Only the "Full Name" column is
rewritten; it never alters a value that doesn't match the bug signature (real
names contain a space and never equal a login), so it's safe + idempotent.

**On-demand UI:** Time Allocation tool → **Admin Settings tab → "Fix entry names"**
(`render_admin_name_cleanup_view`). Gated by `is_current_user_admin()`, which is
true for admins AND developers. Preview = dry run; "Fix names now" applies. Design
choice: we do NOT block disconnected submissions — let them save under the login,
then an admin/developer remaps later with this button.

**Two parquet-rewrite traps (both hit during the first run — don't repeat):**

1. **Partition columns get baked in.** Files live in a Hive tree
   `year=YYYY/month=MM/user=<key>/time_allocation_YYYYMMDD.parquet`. A path-based
   read (`pq.read_table(path)` or `ds.dataset`) AUTO-DISCOVERS year/month/user from
   the directory and appends them as columns. Writing that table back bakes those
   columns INTO the file, which then fails dataset reads with "Unable to merge:
   Field year has incompatible types: int32 vs dictionary". The app's exports
   fast-path uses `ds.dataset(..., partitioning="hive")`, so this really breaks it.
2. **Open handle blocks the atomic replace.** On the UNC share, an open read handle
   (e.g. `pq.ParquetFile(path)` kept alive) makes `os.replace(tmp, path)` fail with
   `PermissionError [WinError 5]`.

Fix for BOTH: `ta_store._read_own_columns(path)` reads the bytes first and parses
from an in-memory `pa.BufferReader` — no partition discovery, no lingering handle.
Always use it (not `pq.read_table`/`ParquetFile.read`) when rewriting these files.

History: the initial run baked partition cols into 3 files (bash/jfitouri/krothfus
2026-06); a one-off `.select()` strip restored them to the 8 base columns. Verified
clean afterward (whole-tree `ds.dataset` reads 2510 rows, repair dry-run 0 errors).
