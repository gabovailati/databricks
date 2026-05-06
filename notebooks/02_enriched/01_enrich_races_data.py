# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched: races_data
# MAGIC
# MAGIC **Notebook**: `02_enriched/01_enrich_races_data.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Layer**: Enriched
# MAGIC **Purpose**: Cleanse, validate, and merge `races_data` from Raw into Enriched. Applies DQ rules RD-01 through RD-07. Casts `First Session` and `Last Session` to TimestampType UTC.

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

df = spark.table(f"{CATALOG}.{RAW_SCHEMA}.races_data")
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

quarantine_rows = []

# RD-01: Round not null (NULL_PRIMARY_KEY)
null_round = df.filter(F.col("Round").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_round)
df = df.filter(F.col("Round").isNotNull())

# RD-02: Round within season range (ROUND_OUT_OF_RANGE)
round_oor = df.filter((F.col("Round") < 0) | (F.col("Round") > 24)).withColumn("rejection_reason", F.lit("ROUND_OUT_OF_RANGE"))
quarantine_rows.append(round_oor)
df = df.filter((F.col("Round") >= 0) & (F.col("Round") <= 24))

# RD-03: Event Name not null (NULL_EVENT_NAME)
null_event = df.filter(F.col("Event Name").isNull()).withColumn("rejection_reason", F.lit("NULL_EVENT_NAME"))
quarantine_rows.append(null_event)
df = df.filter(F.col("Event Name").isNotNull())

# RD-04: Official Event Name not null (NULL_OFFICIAL_EVENT_NAME)
null_official = df.filter(F.col("Official Event Name").isNull()).withColumn("rejection_reason", F.lit("NULL_OFFICIAL_EVENT_NAME"))
quarantine_rows.append(null_official)
df = df.filter(F.col("Official Event Name").isNotNull())

# RD-05: First Session datetime format (INVALID_DATETIME_FORMAT)
ts_pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$"
invalid_first_session = df.filter(
    F.col("First Session").isNull() | (~F.col("First Session").rlike(ts_pattern))
).withColumn("rejection_reason", F.lit("INVALID_DATETIME_FORMAT"))
quarantine_rows.append(invalid_first_session)
df = df.filter(
    F.col("First Session").isNotNull() & F.col("First Session").rlike(ts_pattern)
)

# RD-07: Non-ASCII encoding check — quarantine if replacement character present
encoding_bad = df.filter(
    F.col("Official Event Name").contains("?") | F.col("Location").isNull()
).withColumn("rejection_reason", F.lit("ENCODING_ERROR"))
quarantine_rows.append(encoding_bad)

# RD-06: Duplicate Round (DUPLICATE_ROUND)
from pyspark.sql.window import Window
dup_window = Window.partitionBy("Round")
df_with_dup = df.withColumn("_dup_count", F.count("*").over(dup_window))
dup_round = df_with_dup.filter(F.col("_dup_count") > 1).drop("_dup_count").withColumn("rejection_reason", F.lit("DUPLICATE_ROUND"))
quarantine_rows.append(dup_round)
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
    write_quarantine(quarantine_df, CATALOG, "races_data")
else:
    rows_quarantined = 0

print(f"Rows quarantined: {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 5: Apply Transformations

# COMMAND ----------

df = df \
    .withColumn("first_session_utc", F.to_timestamp(F.col("First Session"))) \
    .withColumn("last_session_utc", F.to_timestamp(F.col("Last Session"))) \
    .drop("First Session", "Last Session") \
    .withColumnRenamed("first_session_utc", "first_session") \
    .withColumnRenamed("last_session_utc", "last_session")

# COMMAND ----------
# MAGIC %md ## Step 6: Deduplication

# COMMAND ----------

pk_cols = ["Round"]
df = df.dropDuplicates(pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 7: Merge into Enriched

# COMMAND ----------

merge_into_enriched(spark, df, CATALOG, ENRICHED_SCHEMA, "races_data", pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 8: Summary

# COMMAND ----------

rows_merged = df.count()
print(f"rows_read      : {rows_read:,}")
print(f"rows_quarantined: {rows_quarantined:,}")
print(f"rows_merged    : {rows_merged:,}")
