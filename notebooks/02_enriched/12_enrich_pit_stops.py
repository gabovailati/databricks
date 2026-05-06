# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched: pit_stops
# MAGIC
# MAGIC **Notebook**: `02_enriched/12_enrich_pit_stops.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Layer**: Enriched
# MAGIC **Purpose**: Cleanse, validate, and merge `pit_stops` from Raw into Enriched. Applies DQ rules PS-01 through PS-10. `Pit In Time` is 100% null (known collection failure) — retained as null. Parses `Pit Out Time` timedelta.

# COMMAND ----------

dbutils.widgets.text("catalog", "f1_platform", "Unity Catalog name")
dbutils.widgets.text("raw_schema", "raw", "Raw schema name")
dbutils.widgets.text("enriched_schema", "enriched", "Enriched schema name")
dbutils.widgets.text("round_number", "1", "Round number (integer)")
dbutils.widgets.text("ingestion_date", "", "Ingestion date (YYYY-MM-DD)")

CATALOG = dbutils.widgets.get("catalog")
RAW_SCHEMA = dbutils.widgets.get("raw_schema")
ENRICHED_SCHEMA = dbutils.widgets.get("enriched_schema")
ROUND_NUMBER = int(dbutils.widgets.get("round_number"))
INGESTION_DATE = dbutils.widgets.get("ingestion_date")

# COMMAND ----------

# MAGIC %run ./00_shared_utils

# COMMAND ----------
# MAGIC %md ## Step 1: Read from Raw

# COMMAND ----------

df = spark.table(f"{CATALOG}.{RAW_SCHEMA}.pit_stops")
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

# GLOBAL-02: round_number injection check
if ROUND_NUMBER is None or ROUND_NUMBER < 0 or ROUND_NUMBER > 24:
    all_quarantine = df.withColumn("rejection_reason", F.lit("MISSING_ROUND_NUMBER")) \
                       .withColumn("ingestion_timestamp", F.current_timestamp()) \
                       .withColumn("round_number", F.lit(ROUND_NUMBER).cast("int"))
    write_quarantine(all_quarantine, CATALOG, "pit_stops")
    rows_quarantined = df.count()
    print(f"CRITICAL: MISSING_ROUND_NUMBER — all {rows_quarantined} rows quarantined. Halting.")
    dbutils.notebook.exit(f"MISSING_ROUND_NUMBER: {rows_quarantined} rows quarantined")

df = df.withColumn("round_number", F.lit(ROUND_NUMBER).cast("int"))

# PS-01: Driver not null (NULL_PRIMARY_KEY)
null_driver = df.filter(F.col("Driver").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_driver)
df = df.filter(F.col("Driver").isNotNull())

# PS-02: Lap Number not null (NULL_PRIMARY_KEY)
null_lap = df.filter(F.col("Lap Number").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_lap)
df = df.filter(F.col("Lap Number").isNotNull())

# PS-03: Driver abbreviation format (INVALID_ABBREVIATION_FORMAT)
abbr_pattern = r"^[A-Z]{3}$"
invalid_abbr = df.filter(~F.col("Driver").rlike(abbr_pattern)).withColumn("rejection_reason", F.lit("INVALID_ABBREVIATION_FORMAT"))
quarantine_rows.append(invalid_abbr)
df = df.filter(F.col("Driver").rlike(abbr_pattern))

# PS-04: Lap Number range (LAP_NUMBER_OUT_OF_RANGE)
lap_oor = df.filter(
    (F.col("Lap Number").cast("int") < 1) | (F.col("Lap Number").cast("int") > 100)
).withColumn("rejection_reason", F.lit("LAP_NUMBER_OUT_OF_RANGE"))
quarantine_rows.append(lap_oor)
df = df.filter(
    (F.col("Lap Number").cast("int") >= 1) & (F.col("Lap Number").cast("int") <= 100)
)

# PS-05: Pit In Time collection failure flag — if non-null, flag for data steward (do not quarantine individual rows)
pit_in_unexpected = df.filter(F.col("Pit In Time").isNotNull()).withColumn("rejection_reason", F.lit("PIT_IN_TIME_UNEXPECTED_VALUE"))
if not pit_in_unexpected.rdd.isEmpty():
    print(f"WARNING: Unexpected non-null Pit In Time values detected: {pit_in_unexpected.count()} rows. Flagging for data steward review.")

# PS-06: Pit Out Time not null (NULL_PIT_OUT_TIME)
null_pit_out = df.filter(F.col("Pit Out Time").isNull()).withColumn("rejection_reason", F.lit("NULL_PIT_OUT_TIME"))
quarantine_rows.append(null_pit_out)
df = df.filter(F.col("Pit Out Time").isNotNull())

# PS-07: Pit Out Time format (INVALID_LAP_TIME_FORMAT)
td_pattern = r"^0 days \d{2}:\d{2}:\d{2}(\.\d+)?$"
invalid_pit_out = df.filter(
    ~F.col("Pit Out Time").rlike(td_pattern)
).withColumn("rejection_reason", F.lit("INVALID_LAP_TIME_FORMAT"))
quarantine_rows.append(invalid_pit_out)
df = df.filter(F.col("Pit Out Time").rlike(td_pattern))

# PS-10: Duplicate primary key (DUPLICATE_PRIMARY_KEY)
dup_window = Window.partitionBy("round_number", "Driver", "Lap Number")
df_with_dup = df.withColumn("_dup_count", F.count("*").over(dup_window))
dup_pk = df_with_dup.filter(F.col("_dup_count") > 1).drop("_dup_count").withColumn("rejection_reason", F.lit("DUPLICATE_PRIMARY_KEY"))
quarantine_rows.append(dup_pk)
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
    write_quarantine(quarantine_df, CATALOG, "pit_stops")
else:
    rows_quarantined = 0

print(f"Rows quarantined: {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 5: Apply Transformations

# COMMAND ----------

df = df \
    .withColumn("lap_number", F.col("Lap Number").cast("int")) \
    .withColumn("pit_out_time_seconds", parse_timedelta_seconds(F.col("Pit Out Time"))) \
    .drop("Lap Number", "Pit Out Time")

# Pit In Time is 100% null — retained as-is in schema (DQ-03 collection failure marker)

# COMMAND ----------
# MAGIC %md ## Step 6: Deduplication

# COMMAND ----------

pk_cols = ["round_number", "Driver", "lap_number"]
df = df.dropDuplicates(pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 7: Merge into Enriched

# COMMAND ----------

merge_into_enriched(spark, df, CATALOG, ENRICHED_SCHEMA, "pit_stops", pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 8: Summary

# COMMAND ----------

rows_merged = df.count()
print(f"rows_read      : {rows_read:,}")
print(f"rows_quarantined: {rows_quarantined:,}")
print(f"rows_merged    : {rows_merged:,}")
