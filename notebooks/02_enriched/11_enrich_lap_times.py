# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched: lap_times
# MAGIC
# MAGIC **Notebook**: `02_enriched/11_enrich_lap_times.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Layer**: Enriched
# MAGIC **Purpose**: Cleanse, validate, and merge `lap_times` from Raw into Enriched. Applies DQ rules LT-01 through LT-12. Sector 1 null on lap 1 is a structural expectation — not quarantined. Parses all timedelta columns.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("raw_schema", "f1_raw", "Raw schema name")
dbutils.widgets.text("enriched_schema", "f1_enriched", "Enriched schema name")
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

df = spark.table(f"{CATALOG}.{RAW_SCHEMA}.lap_times")
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
    write_quarantine(all_quarantine, CATALOG, "lap_times")
    rows_quarantined = df.count()
    print(f"CRITICAL: MISSING_ROUND_NUMBER — all {rows_quarantined} rows quarantined. Halting.")
    dbutils.notebook.exit(f"MISSING_ROUND_NUMBER: {rows_quarantined} rows quarantined")

df = df.withColumn("round_number", F.lit(ROUND_NUMBER).cast("int"))

# LT-01: Driver not null (NULL_PRIMARY_KEY)
null_driver = df.filter(F.col("Driver").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_driver)
df = df.filter(F.col("Driver").isNotNull())

# LT-02: Lap Number not null (NULL_PRIMARY_KEY)
null_lap = df.filter(F.col("Lap_Number").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_lap)
df = df.filter(F.col("Lap_Number").isNotNull())

# LT-03: Driver abbreviation format (INVALID_ABBREVIATION_FORMAT)
abbr_pattern = r"^[A-Z]{3}$"
invalid_abbr = df.filter(~F.col("Driver").rlike(abbr_pattern)).withColumn("rejection_reason", F.lit("INVALID_ABBREVIATION_FORMAT"))
quarantine_rows.append(invalid_abbr)
df = df.filter(F.col("Driver").rlike(abbr_pattern))

# LT-04: Lap Number range (LAP_NUMBER_OUT_OF_RANGE)
lap_oor = df.filter(
    (F.col("Lap_Number").cast("int") < 1) | (F.col("Lap_Number").cast("int") > 100)
).withColumn("rejection_reason", F.lit("LAP_NUMBER_OUT_OF_RANGE"))
quarantine_rows.append(lap_oor)
df = df.filter(
    (F.col("Lap_Number").cast("int") >= 1) & (F.col("Lap_Number").cast("int") <= 100)
)

# LT-05: Position range (POSITION_OUT_OF_RANGE)
pos_oor = df.filter(
    F.col("Position").isNotNull() &
    ((F.col("Position").cast("int") < 1) | (F.col("Position").cast("int") > 20))
).withColumn("rejection_reason", F.lit("POSITION_OUT_OF_RANGE"))
quarantine_rows.append(pos_oor)
df = df.filter(
    F.col("Position").isNull() |
    ((F.col("Position").cast("int") >= 1) & (F.col("Position").cast("int") <= 20))
)

# LT-06: Lap Time format — should never be null (INVALID_LAP_TIME_FORMAT)
td_pattern = r"^0 days \d{2}:\d{2}:\d{2}(\.\d+)?$"
invalid_lap_time = df.filter(
    F.col("Lap_Time").isNull() | (~F.col("Lap_Time").rlike(td_pattern))
).withColumn("rejection_reason", F.lit("INVALID_LAP_TIME_FORMAT"))
quarantine_rows.append(invalid_lap_time)
df = df.filter(
    F.col("Lap_Time").isNotNull() & F.col("Lap_Time").rlike(td_pattern)
)

# LT-07: Cumulative Time format — should never be null (INVALID_LAP_TIME_FORMAT)
invalid_time = df.filter(
    F.col("Time").isNull() | (~F.col("Time").rlike(td_pattern))
).withColumn("rejection_reason", F.lit("INVALID_LAP_TIME_FORMAT"))
quarantine_rows.append(invalid_time)
df = df.filter(
    F.col("Time").isNotNull() & F.col("Time").rlike(td_pattern)
)

# LT-08: Sector 2 format — should never be null (INVALID_SECTOR_TIME_FORMAT)
invalid_s2 = df.filter(
    F.col("Sector_2").isNull() | (~F.col("Sector_2").rlike(td_pattern))
).withColumn("rejection_reason", F.lit("INVALID_SECTOR_TIME_FORMAT"))
quarantine_rows.append(invalid_s2)
df = df.filter(
    F.col("Sector_2").isNotNull() & F.col("Sector_2").rlike(td_pattern)
)

# LT-09: Sector 3 format — should never be null (INVALID_SECTOR_TIME_FORMAT)
invalid_s3 = df.filter(
    F.col("Sector_3").isNull() | (~F.col("Sector_3").rlike(td_pattern))
).withColumn("rejection_reason", F.lit("INVALID_SECTOR_TIME_FORMAT"))
quarantine_rows.append(invalid_s3)
df = df.filter(
    F.col("Sector_3").isNotNull() & F.col("Sector_3").rlike(td_pattern)
)

# LT-10: Sector 1 format when not null, AND not lap 1 (structural expectation: null on lap 1)
# Sector 1 null on lap 1 is expected — do not quarantine those rows
invalid_s1 = df.filter(
    F.col("Sector_1").isNotNull() &
    (F.col("Lap_Number").cast("int") != 1) &
    (~F.col("Sector_1").rlike(td_pattern))
).withColumn("rejection_reason", F.lit("INVALID_SECTOR_TIME_FORMAT"))
quarantine_rows.append(invalid_s1)

# LT-12: Duplicate primary key (DUPLICATE_PRIMARY_KEY)
dup_window = Window.partitionBy("round_number", "Driver", "Lap_Number")
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
    write_quarantine(quarantine_df, CATALOG, "lap_times")
else:
    rows_quarantined = 0

print(f"Rows quarantined: {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 5: Apply Transformations

# COMMAND ----------

df = df \
    .withColumn("lap_number", F.col("Lap_Number").cast("int")) \
    .withColumn("position", F.col("Position").cast("int")) \
    .withColumn("lap_time_seconds", parse_timedelta_seconds(F.col("Lap_Time"))) \
    .withColumn("time_seconds", parse_timedelta_seconds(F.col("Time"))) \
    .withColumn("sector_2_seconds", parse_timedelta_seconds(F.col("Sector_2"))) \
    .withColumn("sector_3_seconds", parse_timedelta_seconds(F.col("Sector_3"))) \
    .withColumn(
        "sector_1_seconds",
        F.when(F.col("lap_number") == 1, F.lit(None).cast("double"))
         .otherwise(parse_timedelta_seconds(F.col("Sector_1")))
    ) \
    .drop("Lap_Number", "Position", "Lap_Time", "Time", "Sector_1", "Sector_2", "Sector_3")

# COMMAND ----------
# MAGIC %md ## Step 6: Deduplication

# COMMAND ----------

pk_cols = ["round_number", "Driver", "lap_number"]
df = df.dropDuplicates(pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 7: Merge into Enriched

# COMMAND ----------

merge_into_enriched(spark, df, CATALOG, ENRICHED_SCHEMA, "lap_times", pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 8: Summary

# COMMAND ----------

rows_merged = df.count()
print(f"rows_read      : {rows_read:,}")
print(f"rows_quarantined: {rows_quarantined:,}")
print(f"rows_merged    : {rows_merged:,}")
