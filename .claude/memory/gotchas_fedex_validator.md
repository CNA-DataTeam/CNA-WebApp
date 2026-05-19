---
name: gotchas_fedex_validator
description: Non-obvious behavior in the FedEx address validator page — "Mark as Disputed" scope, source row identity, email fallback chain
metadata:
  type: project
---

`pages/fedex-address-validator.py` has a few behaviors that surprise people:

1. **"Mark as Disputed" acts on all currently visible rows, not a selected subset.** Whatever filters are applied is what gets marked. If you want to mark a single row, narrow the filters first.

2. **Source row identity is preserved through a hidden `__source_row_id` column.** Filtering and re-displaying still maps writes back to the original dataframe correctly. Don't drop or rename this column.

3. **Email flow has a fallback chain**: it prefers the default mail handler (`mailto:` URL), then falls back to Outlook COM automation. If a developer tries to "simplify" this to one path it will break on machines without the other.

4. **Dispute Excel uses openpyxl for currency formatting.** Plain pandas-to-excel loses the formatting.

5. **The admin page (`fedex-address-validation-management.py`) prefers `.parquet` and falls back to `.csv`** for results, while the validator page itself uses `.csv` only. Keep both in sync if you change file format.

**Why these matter:** All four behaviors are easy to miss when reading the page top-to-bottom because they're spread across helpers. The "visible rows" scope in particular has caused user confusion before.

**How to apply:** Before touching filter logic, the disputed flag write path, the row-id column, or the email/Excel generation paths, re-read these to make sure you're not breaking an assumption.
