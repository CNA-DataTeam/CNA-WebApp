from pathlib import Path
import pyarrow.parquet as pq

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
for f in ROOT.rglob("*.parquet"):
    try:
        schema = pq.read_schema(f)
        cols = [n for n in schema.names]
        if cols != EXPECTED:
            bad += 1
            print(f"[SCHEMA MISMATCH] {f}")
            print("  found:   ", cols)
            print("  expected:", EXPECTED)
    except Exception as e:
        bad += 1
        print(f"[READ FAIL] {f} -> {e}")

print(f"\nDone. Bad files: {bad}")