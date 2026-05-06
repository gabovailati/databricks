# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Raw Layer Historical Backfill
# MAGIC
# MAGIC **Notebook**: `01_raw/02_historical_backfill.py`
# MAGIC **Issue**: #4 — Raw Layer Pipeline
# MAGIC **Purpose**: Ingest all 11 active entities for a single round in one notebook run.
# MAGIC
# MAGIC All ingestion logic is inlined — no `dbutils.notebook.run` calls — so this notebook
# MAGIC runs on both all-purpose clusters and Serverless SQL Warehouse compute.
# MAGIC
# MAGIC **Partial failure policy**: if any entity fails, execution continues to the next.
# MAGIC A summary is printed at the end. If any entity failed an exception is re-raised so the
# MAGIC calling workflow marks the run as failed.

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
DATASOURCE_PATH = dbutils.widgets.get("datasource_path").rstrip("/")
ROUND_NUMBER = int(dbutils.widgets.get("round_number"))
INGESTION_DATE_STR = dbutils.widgets.get("ingestion_date")
INGESTION_DATE = datetime.date.fromisoformat(INGESTION_DATE_STR)

print(f"Catalog        : {CATALOG}")
print(f"Schema         : {SCHEMA}")
print(f"Datasource path: {DATASOURCE_PATH}")
print(f"Round number   : {ROUND_NUMBER}")
print(f"Ingestion date : {INGESTION_DATE_STR}")

# COMMAND ----------
# MAGIC %md ## 1. Registry and Helpers

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from functools import reduce


def sanitize_columns(df):
    """Replace spaces and Delta-invalid chars with underscores in all column names."""
    renamed = [re.sub(r'[ ,;{}\(\)\n\t=]+', '_', c).strip('_') for c in df.columns]
    return df.toDF(*renamed)

# Maps entity name -> (source_filename, reader_format, needs_round_number)
ENTITY_REGISTRY = {
    "races_data":          ("races_data.csv",          "csv",  False),
    "race_results":        ("race_results.csv",         "csv",  True),
    "sprint_results":      ("sprint_results.csv",       "csv",  True),
    "circuit_info":        ("circuit_info.json",        "json", False),
    "drivers_data":        ("drivers_data.csv",         "csv",  False),
    "driver_standings":    ("driver_standings.csv",     "csv",  True),
    "qualifying_results":  ("qualifying_results.csv",   "csv",  True),
    "status_data":         ("status_data.csv",          "csv",  True),
    "constructors_data":   ("constructors_data.csv",    "csv",  False),
    "constructor_results": ("constructor_results.csv",  "csv",  True),
    "lap_times":           ("lap_times.csv",            "csv",  True),
    "pit_stops":           ("pit_stops.csv",            "csv",  True),
}

# Dimensions first, then facts, then high-volume timing last
ACTIVE_ENTITIES = [
    ("races_data",          False),
    ("circuit_info",        False),
    ("drivers_data",        False),
    ("constructors_data",   False),
    ("race_results",        True),
    ("sprint_results",      True),
    ("qualifying_results",  True),
    ("driver_standings",    True),
    ("status_data",         True),
    ("constructor_results", True),
    ("lap_times",           True),
    ("pit_stops",           True),
]


def flatten_circuit_info(json_df):
    session_keys = ["Session1", "Session2", "Session3", "Session4", "Session5"]
    session_dfs = []
    for sk in session_keys:
        session_df = json_df.select(
            F.col("name"),
            F.col("country"),
            F.col("event"),
            F.col("format"),
            F.col("event_date"),
            F.col(f"sessions.{sk}.name").alias("session_name"),
            F.col(f"sessions.{sk}.date").alias("session_date"),
            F.col(f"sessions.{sk}.utc").alias("session_utc"),
        ).filter(F.col("session_name").isNotNull())
        session_dfs.append(session_df)
    return reduce(lambda a, b: a.union(b), session_dfs)


def ingest_entity(entity, needs_round):
    source_filename, reader_format, _ = ENTITY_REGISTRY[entity]
    source_path = f"{DATASOURCE_PATH}/{source_filename}"

    if reader_format == "csv":
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("encoding", "UTF-8")
            .csv(source_path)
            .dropna(how="all")
        )
        df = sanitize_columns(df)
    else:
        df = flatten_circuit_info(
            spark.read.option("multiLine", "true").json(source_path)
        )

    df = (
        df
        .withColumn("ingestion_date", F.lit(INGESTION_DATE).cast(DateType()))
        .withColumn("source_file", F.lit(source_filename))
    )
    if needs_round:
        df = df.withColumn("round_number", F.lit(ROUND_NUMBER).cast(IntegerType()))

    target_table = f"{CATALOG}.{SCHEMA}.{entity}"
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .partitionBy("ingestion_date")
        .saveAsTable(target_table)
    )

    rows = spark.table(target_table).filter(
        F.col("ingestion_date") == F.lit(INGESTION_DATE).cast(DateType())
    ).count()
    return rows

# COMMAND ----------
# MAGIC %md ## 2. Execution Plan

# COMMAND ----------

print(f"Entities to process: {len(ACTIVE_ENTITIES)}")
for entity, needs_round in ACTIVE_ENTITIES:
    rn_display = str(ROUND_NUMBER) if needs_round else "N/A (dimension)"
    print(f"  {entity:<25} round_number={rn_display}")

# COMMAND ----------
# MAGIC %md ## 3. Backfill Loop

# COMMAND ----------

import time

results = []

for entity, needs_round in ACTIVE_ENTITIES:
    print(f"\n[{entity}] Starting ...")
    start_ts = time.time()
    status = "SUCCESS"
    error_msg = None
    rows = 0

    try:
        rows = ingest_entity(entity, needs_round)
        elapsed = time.time() - start_ts
        print(f"[{entity}] Done in {elapsed:.1f}s — {rows:,} rows in partition")
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
        "rows": rows,
        "elapsed_s": round(elapsed, 1),
        "error": error_msg,
    })

# COMMAND ----------
# MAGIC %md ## 4. Summary

# COMMAND ----------

print(f"\n{'='*80}")
print(f"BACKFILL SUMMARY — Round {ROUND_NUMBER} — {INGESTION_DATE_STR}")
print(f"{'='*80}")
header = f"  {'Entity':<25} {'Round':>6} {'Rows':>8} {'Status':>10} {'Elapsed (s)':>12}"
print(header)
print(f"  {'-'*25} {'-'*6} {'-'*8} {'-'*10} {'-'*12}")

failed_entities = []
for r in results:
    rn_display = str(r["round_number"]) if r["round_number"] is not None else "N/A"
    print(f"  {r['entity']:<25} {rn_display:>6} {r['rows']:>8,} {r['status']:>10} {r['elapsed_s']:>12.1f}")
    if r["status"] != "SUCCESS":
        failed_entities.append(r["entity"])

total = len(results)
succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
print(f"\n  Total: {total}  |  Succeeded: {succeeded}  |  Failed: {total - succeeded}")

if failed_entities:
    print(f"\n  Failed entities: {failed_entities}")
    for r in results:
        if r["status"] != "SUCCESS":
            print(f"    [{r['entity']}] {r['error']}")

print(f"{'='*80}")

if failed_entities:
    raise RuntimeError(
        f"Backfill completed with {total - succeeded} failure(s). "
        f"Failed entities: {failed_entities}. "
        f"See cell output above for details."
    )

print("All entities ingested successfully.")
