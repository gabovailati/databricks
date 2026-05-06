# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched: race_results
# MAGIC
# MAGIC **Notebook**: `02_enriched/02_enrich_race_results.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Layer**: Enriched
# MAGIC **Purpose**: Cleanse, validate, and merge `race_results` from Raw into Enriched. Applies DQ rules RR-01 through RR-13. Drops 100%-null artefact columns. Casts numeric types. Parses timedelta. Splits ClassifiedPosition.

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

df = spark.table(f"{CATALOG}.{RAW_SCHEMA}.race_results")
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

# Pre-processing: drop 100%-null artefact columns
df = df.drop("CountryCode", "Q1", "Q2", "Q3")

# GLOBAL-02: round_number injection check
if ROUND_NUMBER is None or ROUND_NUMBER < 0 or ROUND_NUMBER > 24:
    all_quarantine = df.withColumn("rejection_reason", F.lit("MISSING_ROUND_NUMBER")) \
                       .withColumn("ingestion_timestamp", F.current_timestamp()) \
                       .withColumn("round_number", F.lit(ROUND_NUMBER).cast("int"))
    write_quarantine(all_quarantine, CATALOG, "race_results")
    rows_quarantined = df.count()
    print(f"CRITICAL: MISSING_ROUND_NUMBER — all {rows_quarantined} rows quarantined. Halting.")
    dbutils.notebook.exit(f"MISSING_ROUND_NUMBER: {rows_quarantined} rows quarantined")

df = df.withColumn("round_number", F.lit(ROUND_NUMBER).cast("int"))

# RR-01: DriverId not null (NULL_PRIMARY_KEY)
null_driver = df.filter(F.col("DriverId").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_driver)
df = df.filter(F.col("DriverId").isNotNull())

# RR-02: DriverNumber not null (NULL_DRIVER_NUMBER)
null_dnum = df.filter(F.col("DriverNumber").isNull()).withColumn("rejection_reason", F.lit("NULL_DRIVER_NUMBER"))
quarantine_rows.append(null_dnum)
df = df.filter(F.col("DriverNumber").isNotNull())

# RR-03: TeamId not null (NULL_TEAM_ID)
null_team = df.filter(F.col("TeamId").isNull()).withColumn("rejection_reason", F.lit("NULL_TEAM_ID"))
quarantine_rows.append(null_team)
df = df.filter(F.col("TeamId").isNotNull())

# RR-09: ClassifiedPosition format (INVALID_CLASSIFIED_POSITION)
classified_pattern = r"^([0-9]{1,2}|R|D|E|NC|W|F|S)$"
invalid_classified = df.filter(
    F.col("ClassifiedPosition").isNull() | (~F.col("ClassifiedPosition").rlike(classified_pattern))
).withColumn("rejection_reason", F.lit("INVALID_CLASSIFIED_POSITION"))
quarantine_rows.append(invalid_classified)
df = df.filter(
    F.col("ClassifiedPosition").isNotNull() & F.col("ClassifiedPosition").rlike(classified_pattern)
)

# RR-10: Time format when not null (INVALID_LAP_TIME_FORMAT)
td_pattern = r"^0 days \d{2}:\d{2}:\d{2}(\.\d+)?$"
invalid_time = df.filter(
    F.col("Time").isNotNull() & (~F.col("Time").rlike(td_pattern))
).withColumn("rejection_reason", F.lit("INVALID_LAP_TIME_FORMAT"))
quarantine_rows.append(invalid_time)
df = df.filter(
    F.col("Time").isNull() | F.col("Time").rlike(td_pattern)
)

# RR-13: TeamColor format (INVALID_TEAM_COLOR_FORMAT)
hex_pattern = r"^[0-9A-Fa-f]{6}$"
invalid_color = df.filter(
    F.col("TeamColor").isNotNull() & (~F.col("TeamColor").rlike(hex_pattern))
).withColumn("rejection_reason", F.lit("INVALID_TEAM_COLOR_FORMAT"))
quarantine_rows.append(invalid_color)

# RR-05: Position range (POSITION_OUT_OF_RANGE)
pos_oor = df.filter(
    F.col("Position").isNotNull() &
    ((F.col("Position").cast("int") < 1) | (F.col("Position").cast("int") > 20))
).withColumn("rejection_reason", F.lit("POSITION_OUT_OF_RANGE"))
quarantine_rows.append(pos_oor)
df = df.filter(
    F.col("Position").isNull() |
    ((F.col("Position").cast("int") >= 1) & (F.col("Position").cast("int") <= 20))
)

# RR-06: GridPosition range (GRID_POSITION_OUT_OF_RANGE)
grid_oor = df.filter(
    F.col("GridPosition").isNotNull() &
    ((F.col("GridPosition").cast("int") < 0) | (F.col("GridPosition").cast("int") > 20))
).withColumn("rejection_reason", F.lit("GRID_POSITION_OUT_OF_RANGE"))
quarantine_rows.append(grid_oor)
df = df.filter(
    F.col("GridPosition").isNull() |
    ((F.col("GridPosition").cast("int") >= 0) & (F.col("GridPosition").cast("int") <= 20))
)

# RR-07: Points non-negative (NEGATIVE_POINTS)
neg_pts = df.filter(
    F.col("Points").isNotNull() & (F.col("Points").cast("int") < 0)
).withColumn("rejection_reason", F.lit("NEGATIVE_POINTS"))
quarantine_rows.append(neg_pts)
df = df.filter(F.col("Points").isNull() | (F.col("Points").cast("int") >= 0))

# RR-08: Laps non-negative (NEGATIVE_LAPS)
neg_laps = df.filter(
    F.col("Laps").isNotNull() & (F.col("Laps").cast("int") < 0)
).withColumn("rejection_reason", F.lit("NEGATIVE_LAPS"))
quarantine_rows.append(neg_laps)
df = df.filter(F.col("Laps").isNull() | (F.col("Laps").cast("int") >= 0))

# RR-04: Duplicate primary key (DUPLICATE_PRIMARY_KEY)
dup_window = Window.partitionBy("round_number", "DriverId")
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
    write_quarantine(quarantine_df, CATALOG, "race_results")
else:
    rows_quarantined = 0

print(f"Rows quarantined: {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 5: Apply Transformations

# COMMAND ----------

df = df \
    .withColumn("position", F.col("Position").cast("int")) \
    .withColumn("grid_position", F.col("GridPosition").cast("int")) \
    .withColumn("points", F.col("Points").cast("int")) \
    .withColumn("laps", F.col("Laps").cast("int")) \
    .withColumn("time_seconds", parse_timedelta_seconds(F.col("Time"))) \
    .withColumn(
        "finish_position",
        F.when(F.col("ClassifiedPosition").rlike(r"^\d+$"), F.col("ClassifiedPosition").cast("int")).otherwise(None)
    ) \
    .withColumn(
        "is_classified",
        F.when(F.col("ClassifiedPosition") == "R", F.lit(False)).otherwise(F.lit(True))
    ) \
    .drop("Position", "GridPosition", "Points", "Laps", "Time", "ClassifiedPosition")

# COMMAND ----------
# MAGIC %md ## Step 6: Deduplication

# COMMAND ----------

pk_cols = ["round_number", "DriverId"]
df = df.dropDuplicates(pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 7: Merge into Enriched

# COMMAND ----------

merge_into_enriched(spark, df, CATALOG, ENRICHED_SCHEMA, "race_results", pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 8: Summary

# COMMAND ----------

rows_merged = df.count()
print(f"rows_read      : {rows_read:,}")
print(f"rows_quarantined: {rows_quarantined:,}")
print(f"rows_merged    : {rows_merged:,}")
