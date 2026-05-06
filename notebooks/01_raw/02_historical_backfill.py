# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Raw Layer Historical Backfill
# MAGIC
# MAGIC **Notebook**: `01_raw/02_historical_backfill.py`
# MAGIC **Issue**: #4 — Raw Layer Pipeline
# MAGIC **Purpose**: Orchestrate ingestion of all 11 active entities for a single round in one notebook run.
# MAGIC
# MAGIC This notebook loops through every active entity and calls `01_ingest_raw` via `dbutils.notebook.run`.
# MAGIC It is the entry point for historical backfills when a full round of source files lands at once.
# MAGIC
# MAGIC **Partial failure policy**: if any entity fails, execution continues to the next entity.
# MAGIC A summary is printed at the end. If any entity failed, an exception is re-raised so the
# MAGIC calling workflow can alert without silently swallowing errors.

# COMMAND ----------
# MAGIC %md ## 0. Parameters

# COMMAND ----------

import datetime

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("schema", "f1_raw", "Schema / layer name")
dbutils.widgets.text("datasource_path", "/Volumes/workspace/f1_raw/landing/datasource", "Path to datasource/ folder")
dbutils.widgets.text("round_number", "1", "Season round number (integer)")
dbutils.widgets.text("ingestion_date", str(datetime.date.today()), "Ingestion date YYYY-MM-DD")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DATASOURCE_PATH = dbutils.widgets.get("datasource_path")
ROUND_NUMBER = int(dbutils.widgets.get("round_number"))
INGESTION_DATE_STR = dbutils.widgets.get("ingestion_date")

print(f"Catalog        : {CATALOG}")
print(f"Schema         : {SCHEMA}")
print(f"Datasource path: {DATASOURCE_PATH}")
print(f"Round number   : {ROUND_NUMBER}")
print(f"Ingestion date : {INGESTION_DATE_STR}")

# COMMAND ----------
# MAGIC %md ## 1. Entity Execution Plan

# COMMAND ----------

# All 11 active entities in ingestion order.
# Dimension entities (needs_round=False) use round_number=-1 per the ingest notebook contract.
# Fact/result entities (needs_round=True) receive the actual ROUND_NUMBER.
#
# Order: dimensions first, then facts, then high-volume timing data last.
ACTIVE_ENTITIES = [
    # Dimensions — round_number not applicable
    ("races_data",          False),
    ("circuit_info",        False),
    ("drivers_data",        False),
    ("constructors_data",   False),
    # Facts / standings — round_number required
    ("race_results",        True),
    ("sprint_results",      True),
    ("qualifying_results",  True),
    ("driver_standings",    True),
    ("status_data",         True),
    ("constructor_results", True),
    # High-volume timing entities last
    ("lap_times",           True),
    ("pit_stops",           True),
]

print(f"Entities to process: {len(ACTIVE_ENTITIES)}")
for entity, needs_round in ACTIVE_ENTITIES:
    rn_display = str(ROUND_NUMBER) if needs_round else "N/A (dimension)"
    print(f"  {entity:<25} round_number={rn_display}")

# COMMAND ----------
# MAGIC %md ## 2. Backfill Loop

# COMMAND ----------

import time

results = []

for entity, needs_round in ACTIVE_ENTITIES:
    round_arg = str(ROUND_NUMBER) if needs_round else "-1"
    arguments = {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "datasource_path": DATASOURCE_PATH,
        "entity": entity,
        "round_number": round_arg,
        "ingestion_date": INGESTION_DATE_STR,
        "mode": "append",
    }

    print(f"\n[{entity}] Starting ingestion ...")
    start_ts = time.time()
    status = "SUCCESS"
    error_msg = None

    try:
        run_output = dbutils.notebook.run(
            "01_ingest_raw",
            timeout_seconds=600,
            arguments=arguments,
        )
        elapsed = time.time() - start_ts
        print(f"[{entity}] Completed in {elapsed:.1f}s — output: {run_output}")
    except Exception as exc:
        elapsed = time.time() - start_ts
        status = "FAILED"
        error_msg = str(exc)
        print(f"[{entity}] FAILED after {elapsed:.1f}s: {error_msg}")

    results.append({
        "entity": entity,
        "needs_round": needs_round,
        "round_number": ROUND_NUMBER if needs_round else None,
        "status": status,
        "elapsed_s": round(elapsed, 1),
        "error": error_msg,
    })

# COMMAND ----------
# MAGIC %md ## 3. Backfill Summary

# COMMAND ----------

print(f"\n{'='*80}")
print(f"BACKFILL SUMMARY — Round {ROUND_NUMBER} — {INGESTION_DATE_STR}")
print(f"{'='*80}")

header = f"  {'Entity':<25} {'Round':>6} {'Status':>10} {'Elapsed (s)':>12}"
print(header)
print(f"  {'-'*25} {'-'*6} {'-'*10} {'-'*12}")

failed_entities = []
for r in results:
    rn_display = str(r["round_number"]) if r["round_number"] is not None else "N/A"
    print(f"  {r['entity']:<25} {rn_display:>6} {r['status']:>10} {r['elapsed_s']:>12.1f}")
    if r["status"] != "SUCCESS":
        failed_entities.append(r["entity"])

total = len(results)
succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
failed = total - succeeded

print(f"\n  Total   : {total}")
print(f"  Succeeded: {succeeded}")
print(f"  Failed   : {failed}")

if failed_entities:
    print(f"\n  Failed entities: {failed_entities}")
    for r in results:
        if r["status"] != "SUCCESS":
            print(f"    [{r['entity']}] {r['error']}")

print(f"{'='*80}")

if failed_entities:
    raise RuntimeError(
        f"Backfill completed with {failed} failure(s). "
        f"Failed entities: {failed_entities}. "
        f"See cell output above for per-entity error details."
    )

print("All entities ingested successfully.")
