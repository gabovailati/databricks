# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — File Arrival Sensor
# MAGIC
# MAGIC **Notebook**: `01_raw/00_file_arrival_sensor.py`
# MAGIC **Issue**: #4 — Raw Layer Pipeline
# MAGIC **Purpose**: Poll `datasource_path` for the expected source files and block until all are present or the wait budget is exhausted.
# MAGIC
# MAGIC Run this notebook as the first step in the Raw ingestion workflow. It prints a manifest of found files
# MAGIC and raises an exception on timeout so the calling workflow can alert and retry.

# COMMAND ----------
# MAGIC %md ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "f1_platform", "Unity Catalog name")
dbutils.widgets.text("datasource_path", "/Volumes/f1_platform/raw/landing/datasource", "Path to datasource/ folder")
dbutils.widgets.text("round_number", "1", "Season round number (integer)")
dbutils.widgets.text("max_wait_minutes", "120", "Maximum minutes to wait before raising TimeoutError")
dbutils.widgets.text("retry_interval_minutes", "10", "Minutes to sleep between polling attempts")

CATALOG = dbutils.widgets.get("catalog")
DATASOURCE_PATH = dbutils.widgets.get("datasource_path")
ROUND_NUMBER = int(dbutils.widgets.get("round_number"))
MAX_WAIT_MINUTES = int(dbutils.widgets.get("max_wait_minutes"))
RETRY_INTERVAL_MINUTES = int(dbutils.widgets.get("retry_interval_minutes"))

print(f"Catalog              : {CATALOG}")
print(f"Datasource path      : {DATASOURCE_PATH}")
print(f"Round number         : {ROUND_NUMBER}")
print(f"Max wait (minutes)   : {MAX_WAIT_MINUTES}")
print(f"Retry interval (min) : {RETRY_INTERVAL_MINUTES}")

# COMMAND ----------
# MAGIC %md ## 1. File Registry

# COMMAND ----------

# All 13 source files tracked by the sensor.
# Files marked retired are still detected for auditability but are not ingested downstream.
ALL_SOURCE_FILES = [
    "races_data.csv",
    "race_results.csv",
    "sprint_results.csv",
    "circuit_info.json",
    "drivers_data.csv",
    "driver_standings.csv",
    "qualifying_results.csv",
    "status_data.csv",
    "constructors_data.csv",
    "constructor_results.csv",
    "lap_times.csv",
    "pit_stops.csv",
    # Retired — detected for auditability; not ingested
    "season_data.csv",
    "constructor_standings.csv",
]

RETIRED_FILES = {"season_data.csv", "constructor_standings.csv"}

# COMMAND ----------
# MAGIC %md ## 2. Sensor Loop

# COMMAND ----------

import time

def list_present_files(base_path: str) -> dict:
    """
    Return a dict mapping filename -> FileInfo for every file present under base_path.
    Uses dbutils.fs.ls() which raises an exception if the path does not exist.
    """
    try:
        entries = dbutils.fs.ls(base_path)
    except Exception:
        # Path does not yet exist — treat as empty
        return {}
    return {entry.name: entry for entry in entries if not entry.name.endswith("/")}


def check_all_files_present(base_path: str, expected_files: list) -> tuple:
    """
    Returns (all_present: bool, found: dict, missing: list).
    found maps filename -> FileInfo for files that are present.
    missing lists filenames that are absent.
    """
    present = list_present_files(base_path)
    found = {}
    missing = []
    for fname in expected_files:
        if fname in present:
            found[fname] = present[fname]
        else:
            missing.append(fname)
    return (len(missing) == 0, found, missing)


max_wait_seconds = MAX_WAIT_MINUTES * 60
retry_interval_seconds = RETRY_INTERVAL_MINUTES * 60
elapsed = 0
attempt = 0

while True:
    attempt += 1
    all_present, found_files, missing_files = check_all_files_present(DATASOURCE_PATH, ALL_SOURCE_FILES)

    print(f"\n[Attempt {attempt}] Elapsed: {elapsed // 60}m {elapsed % 60}s")
    print(f"  Found   : {len(found_files)}/{len(ALL_SOURCE_FILES)} files")
    if missing_files:
        print(f"  Missing : {missing_files}")

    if all_present:
        print("  Status  : ALL FILES PRESENT — proceeding.")
        break

    if elapsed >= max_wait_seconds:
        raise TimeoutError(
            f"File arrival sensor timed out after {MAX_WAIT_MINUTES} minutes. "
            f"Missing files: {missing_files}"
        )

    print(f"  Status  : waiting {RETRY_INTERVAL_MINUTES}m before next check ...")
    time.sleep(retry_interval_seconds)
    elapsed += retry_interval_seconds

# COMMAND ----------
# MAGIC %md ## 3. Manifest

# COMMAND ----------

from datetime import datetime, timezone

print(f"\n{'='*80}")
print(f"FILE ARRIVAL MANIFEST — Round {ROUND_NUMBER}")
print(f"Datasource path : {DATASOURCE_PATH}")
print(f"Scan time       : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"{'='*80}")

header = f"  {'File':<40} {'Size (bytes)':>14} {'Modified (UTC)':>22} {'Status':>12}"
print(header)
print(f"  {'-'*40} {'-'*14} {'-'*22} {'-'*12}")

for fname in ALL_SOURCE_FILES:
    fi = found_files[fname]
    mod_ts = datetime.fromtimestamp(fi.modificationTime / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if fname in RETIRED_FILES:
        status = "RETIRED"
        print(f"  {fname:<40} {fi.size:>14,} {mod_ts:>22} {status:>12}")
        print(f"    WARNING: {fname} is a retired source file and will NOT be ingested. "
              f"Detected for auditability only.")
    else:
        status = "ACTIVE"
        print(f"  {fname:<40} {fi.size:>14,} {mod_ts:>22} {status:>12}")

print(f"\n  Active files   : {len(ALL_SOURCE_FILES) - len(RETIRED_FILES)}")
print(f"  Retired files  : {len(RETIRED_FILES)}")
print(f"{'='*80}")
print("Sensor complete — all expected files are present.")
