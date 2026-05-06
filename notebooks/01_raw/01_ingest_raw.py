# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Raw Layer Ingestion
# MAGIC
# MAGIC **Notebook**: `01_raw/01_ingest_raw.py`
# MAGIC **Issue**: #4 — Raw Layer Pipeline
# MAGIC **Purpose**: Ingest one entity from `datasource/` into its `f1_platform.raw.<entity>` Delta table.
# MAGIC
# MAGIC This notebook handles all 11 active entities through a single parameterised entry point.
# MAGIC Call once per entity per pipeline run. For multi-entity runs use `02_historical_backfill.py`.
# MAGIC
# MAGIC **Raw layer contract**:
# MAGIC - All CSV columns are read as StringType (inferSchema=False). No type casting occurs here.
# MAGIC - Three pipeline-injected columns are appended to every entity: `ingestion_date`, `source_file`, and (where applicable) `round_number`.
# MAGIC - Delta writes are append-only, partitioned by `ingestion_date`, with mergeSchema=false so provider schema changes surface as errors.

# COMMAND ----------
# MAGIC %md ## 0. Parameters

# COMMAND ----------

import datetime

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("schema", "f1_raw", "Schema / layer name")
dbutils.widgets.text("datasource_path", "/Volumes/workspace/f1_raw/landing/datasource", "Path to datasource/ folder")
dbutils.widgets.text("entity", "", "Entity name (e.g. race_results)")
dbutils.widgets.text("round_number", "-1", "Round number (-1 = not applicable for dimension entities)")
dbutils.widgets.text("ingestion_date", str(datetime.date.today()), "Ingestion date YYYY-MM-DD")
dbutils.widgets.text("mode", "append", "Write mode (always append for Raw; parameter exists for testing)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DATASOURCE_PATH = dbutils.widgets.get("datasource_path").rstrip("/")
ENTITY = dbutils.widgets.get("entity").strip()
ROUND_NUMBER = int(dbutils.widgets.get("round_number"))
INGESTION_DATE_STR = dbutils.widgets.get("ingestion_date")
MODE = dbutils.widgets.get("mode")

print(f"Catalog        : {CATALOG}")
print(f"Schema         : {SCHEMA}")
print(f"Datasource path: {DATASOURCE_PATH}")
print(f"Entity         : {ENTITY}")
print(f"Round number   : {ROUND_NUMBER}")
print(f"Ingestion date : {INGESTION_DATE_STR}")
print(f"Write mode     : {MODE}")

# COMMAND ----------
# MAGIC %md ## 1. Entity Registry

# COMMAND ----------

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

# Retired entities: detected by sensor for auditability but never ingested
RETIRED_ENTITIES = {"season_data", "constructor_standings"}

# COMMAND ----------
# MAGIC %md ## 2. Validation

# COMMAND ----------

if not ENTITY:
    raise ValueError("Widget 'entity' is required and must not be empty.")

if ENTITY in RETIRED_ENTITIES:
    raise ValueError(
        f"Entity '{ENTITY}' is retired and must not be ingested to Raw. "
        f"Retired entities: {sorted(RETIRED_ENTITIES)}"
    )

if ENTITY not in ENTITY_REGISTRY:
    raise ValueError(
        f"Unknown entity '{ENTITY}'. "
        f"Valid active entities: {sorted(ENTITY_REGISTRY.keys())}"
    )

source_filename, reader_format, needs_round_number = ENTITY_REGISTRY[ENTITY]

if needs_round_number and ROUND_NUMBER == -1:
    raise ValueError(
        f"Entity '{ENTITY}' requires a round_number. "
        f"Received -1 (sentinel for dimension entities). "
        f"Pass the correct integer round number for this pipeline run."
    )

# COMMAND ----------
# MAGIC %md ## 3. Helpers

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from functools import reduce


def sanitize_columns(df):
    """Replace spaces and Delta-invalid chars with underscores in all column names."""
    renamed = [re.sub(r'[ ,;{}\(\)\n\t=]+', '_', c).strip('_') for c in df.columns]
    return df.toDF(*renamed)


def flatten_circuit_info(json_df):
    """
    Transform the single-row nested circuit_info JSON document into one row per session.
    Source structure: {name, country, event, format, event_date, sessions: {Session1..Session5}}
    Output columns: name, country, event, format, event_date, session_name, session_date, session_utc
    Only sessions present in the document are emitted as rows — absent sessions produce no rows.
    """
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

# COMMAND ----------
# MAGIC %md ## 4. Read Source File

# COMMAND ----------

source_path = f"{DATASOURCE_PATH}/{source_filename}"
print(f"Reading: {source_path}")

if reader_format == "csv":
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(source_path)
    )
    # Trailing all-null rows are common in pandas-exported CSVs
    raw_df = raw_df.dropna(how="all")
    raw_df = sanitize_columns(raw_df)

elif reader_format == "json":
    raw_json = spark.read.option("multiLine", "true").json(source_path)
    raw_df = flatten_circuit_info(raw_json)

print(f"Source rows read: {raw_df.count():,}")
raw_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 5. Add Injected Columns

# COMMAND ----------

ingestion_date_val = datetime.date.fromisoformat(INGESTION_DATE_STR)

enriched_df = (
    raw_df
    .withColumn("ingestion_date", F.lit(ingestion_date_val).cast(DateType()))
    .withColumn("source_file", F.lit(source_filename))
)

if needs_round_number:
    enriched_df = enriched_df.withColumn("round_number", F.lit(ROUND_NUMBER).cast(IntegerType()))

print(f"Columns after injection: {enriched_df.columns}")

# COMMAND ----------
# MAGIC %md ## 6. Write to Delta

# COMMAND ----------

target_table = f"{CATALOG}.{SCHEMA}.{ENTITY}"
print(f"Writing to: {target_table}")

(
    enriched_df.write
    .format("delta")
    .mode(MODE)
    .option("mergeSchema", "false")
    .partitionBy("ingestion_date")
    .saveAsTable(target_table)
)

# COMMAND ----------
# MAGIC %md ## 7. Post-Write Summary

# COMMAND ----------

written_df = spark.table(target_table).filter(
    F.col("ingestion_date") == F.lit(ingestion_date_val).cast(DateType())
)
rows_this_partition = written_df.count()

partition_count = (
    spark.sql(f"DESCRIBE DETAIL {target_table}")
    .select("numFiles")
    .collect()[0]["numFiles"]
)

print(f"\n{'='*60}")
print(f"  Ingestion complete")
print(f"  Target table       : {target_table}")
print(f"  Entity             : {ENTITY}")
print(f"  Source file        : {source_filename}")
print(f"  Ingestion date     : {INGESTION_DATE_STR}")
if needs_round_number:
    print(f"  Round number       : {ROUND_NUMBER}")
print(f"  Rows this partition: {rows_this_partition:,}")
print(f"  Total table files  : {partition_count:,}")
print(f"{'='*60}")
