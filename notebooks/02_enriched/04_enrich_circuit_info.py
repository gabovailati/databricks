# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched: circuit_info
# MAGIC
# MAGIC **Notebook**: `02_enriched/04_enrich_circuit_info.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Layer**: Enriched
# MAGIC **Purpose**: Cleanse, validate, and merge `circuit_info` from Raw into Enriched. Applies DQ rules CI-01 through CI-09. Casts all datetime columns to TimestampType UTC.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("raw_schema", "f1_raw", "Raw schema name")
dbutils.widgets.text("enriched_schema", "f1_enriched", "Enriched schema name")
dbutils.widgets.text("ingestion_date", "", "Ingestion date (YYYY-MM-DD)")

CATALOG = dbutils.widgets.get("catalog")
RAW_SCHEMA = dbutils.widgets.get("raw_schema")
ENRICHED_SCHEMA = dbutils.widgets.get("enriched_schema")
INGESTION_DATE = dbutils.widgets.get("ingestion_date")

# COMMAND ----------

# MAGIC %run ./00_shared_utils

# COMMAND ----------
# MAGIC %md ## Step 1: Read from Raw

# COMMAND ----------

df = spark.table(f"{CATALOG}.{RAW_SCHEMA}.circuit_info")
rows_read = df.count()
print(f"Rows read from raw: {rows_read:,}")

# COMMAND ----------
# MAGIC %md ## Step 2: Global Sentinel Replacement

# COMMAND ----------

df = replace_sentinels(df)

# COMMAND ----------
# MAGIC %md ## Step 3: Entity-Specific DQ Checks

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

quarantine_rows = []

# CI-01: event not null (NULL_PRIMARY_KEY)
null_event = df.filter(F.col("event").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_event)
df = df.filter(F.col("event").isNotNull())

# CI-02: session_name not null (NULL_PRIMARY_KEY)
null_session = df.filter(F.col("session_name").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_session)
df = df.filter(F.col("session_name").isNotNull())

# CI-04: session_name valid enum (INVALID_SESSION_NAME)
valid_sessions = ["Practice 1", "Practice 2", "Practice 3", "Sprint Qualifying", "Sprint", "Qualifying", "Race"]
invalid_session_name = df.filter(~F.col("session_name").isin(valid_sessions)).withColumn("rejection_reason", F.lit("INVALID_SESSION_NAME"))
quarantine_rows.append(invalid_session_name)
df = df.filter(F.col("session_name").isin(valid_sessions))

# CI-05: session date format — tz-aware local time (INVALID_DATETIME_FORMAT)
session_date_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$"
invalid_session_date = df.filter(
    F.col("session_date").isNull() | (~F.col("session_date").rlike(session_date_pattern))
).withColumn("rejection_reason", F.lit("INVALID_DATETIME_FORMAT"))
quarantine_rows.append(invalid_session_date)
df = df.filter(
    F.col("session_date").isNotNull() & F.col("session_date").rlike(session_date_pattern)
)

# CI-06: session_utc format — naive UTC (INVALID_DATETIME_FORMAT)
utc_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
invalid_utc = df.filter(
    F.col("session_utc").isNull() | (~F.col("session_utc").rlike(utc_pattern))
).withColumn("rejection_reason", F.lit("INVALID_DATETIME_FORMAT"))
quarantine_rows.append(invalid_utc)
df = df.filter(
    F.col("session_utc").isNotNull() & F.col("session_utc").rlike(utc_pattern)
)

# CI-09: Duplicate session per event (DUPLICATE_SESSION)
dup_window = Window.partitionBy("event", "session_name")
df_with_dup = df.withColumn("_dup_count", F.count("*").over(dup_window))
dup_session = df_with_dup.filter(F.col("_dup_count") > 1).drop("_dup_count").withColumn("rejection_reason", F.lit("DUPLICATE_SESSION"))
quarantine_rows.append(dup_session)
df = df_with_dup.filter(F.col("_dup_count") == 1).drop("_dup_count")

# COMMAND ----------
# MAGIC %md ## Step 4: Write Quarantine

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

non_empty = [q for q in quarantine_rows if not q.rdd.isEmpty()]
if non_empty:
    quarantine_df = reduce(DataFrame.unionByName, non_empty, non_empty[0]).dropDuplicates()
    quarantine_df = quarantine_df.withColumn("ingestion_timestamp", F.current_timestamp())
    rows_quarantined = quarantine_df.count()
    write_quarantine(quarantine_df, CATALOG, "circuit_info")
else:
    rows_quarantined = 0

print(f"Rows quarantined: {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 5: Apply Transformations

# COMMAND ----------

df = df \
    .withColumn("session_date_utc", F.to_timestamp(F.col("session_date"))) \
    .withColumn("session_utc_ts", F.to_timestamp(F.concat(F.col("session_utc"), F.lit("+00:00")))) \
    .withColumn("event_date_utc", F.to_timestamp(F.col("event_date"))) \
    .drop("session_date", "session_utc", "event_date") \
    .withColumnRenamed("session_date_utc", "session_date") \
    .withColumnRenamed("session_utc_ts", "session_utc") \
    .withColumnRenamed("event_date_utc", "event_date")

# COMMAND ----------
# MAGIC %md ## Step 6: Deduplication

# COMMAND ----------

pk_cols = ["event", "session_name"]
df = df.dropDuplicates(pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 7: Merge into Enriched

# COMMAND ----------

merge_into_enriched(spark, df, CATALOG, ENRICHED_SCHEMA, "circuit_info", pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 8: Summary

# COMMAND ----------

rows_merged = df.count()
print(f"rows_read      : {rows_read:,}")
print(f"rows_quarantined: {rows_quarantined:,}")
print(f"rows_merged    : {rows_merged:,}")
