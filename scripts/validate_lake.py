from pathlib import Path
import pyarrow.parquet as pq
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import app_logging

ROOT = Path("logistics_task_log")

EXPECTED = [
    "TaskID",
    "UserName",
    "TaskName",
    "TaskCadence",
    "StartTimestampUTC",
    "EndTimestampUTC",
    "DurationSeconds",
    "UploadTimestampUTC",
    "AppVersion",
]

bad = 0
LOGGER = app_logging.get_logger(__file__, "Validation Script")
LOGGER.info("Session started.")
for f in ROOT.rglob("*.parquet"):
    try:
        schema = pq.read_schema(f)
        cols = [n for n in schema.names]
        if cols != EXPECTED:
            bad += 1
            LOGGER.warning("[SCHEMA MISMATCH] %s", f)
            LOGGER.warning("found=%s", cols)
            LOGGER.warning("expected=%s", EXPECTED)
    except Exception as e:
        bad += 1
        LOGGER.error("[READ FAIL] %s -> %s", f, e)

LOGGER.info("Done. Bad files: %s", bad)
