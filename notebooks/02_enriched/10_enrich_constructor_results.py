# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched: constructor_results
# MAGIC
# MAGIC **Notebook**: `02_enriched/10_enrich_constructor_results.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Layer**: Enriched
# MAGIC **Purpose**: Cleanse, validate, and merge `constructor_results` from Raw into Enriched. Applies DQ rules CR-01 through CR-10. Casts Position and Points to IntegerType. Parses timedelta Time column.

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

df = spark.table(f"{CATALOG}.{RAW_SCHEMA}.constructor_results")
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
    write_quarantine(all_quarantine, CATALOG, "constructor_results")
    rows_quarantined = df.count()
    print(f"CRITICAL: MISSING_ROUND_NUMBER — all {rows_quarantined} rows quarantined. Halting.")
    dbutils.notebook.exit(f"MISSING_ROUND_NUMBER: {rows_quarantined} rows quarantined")

df = df.withColumn("round_number", F.lit(ROUND_NUMBER).cast("int"))

# CR-01: Driver ID not null (NULL_PRIMARY_KEY)
null_id = df.filter(F.col("Driver ID").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_id)
df = df.filter(F.col("Driver ID").isNotNull())

# CR-02: Team not null (NULL_TEAM_REFERENCE)
null_team = df.filter(F.col("Team").isNull()).withColumn("rejection_reason", F.lit("NULL_TEAM_REFERENCE"))
quarantine_rows.append(null_team)
df = df.filter(F.col("Team").isNotNull())

# CR-03: Position not null (NULL_RESULT_POSITION)
null_pos = df.filter(F.col("Position").isNull()).withColumn("rejection_reason", F.lit("NULL_RESULT_POSITION"))
quarantine_rows.append(null_pos)
df = df.filter(F.col("Position").isNotNull())

# CR-04: Position range (POSITION_OUT_OF_RANGE)
pos_oor = df.filter(
    (F.col("Position").cast("int") < 1) | (F.col("Position").cast("int") > 20)
).withColumn("rejection_reason", F.lit("POSITION_OUT_OF_RANGE"))
quarantine_rows.append(pos_oor)
df = df.filter(
    (F.col("Position").cast("int") >= 1) & (F.col("Position").cast("int") <= 20)
)

# CR-05: Points non-negative (NEGATIVE_POINTS)
neg_pts = df.filter(
    F.col("Points").isNotNull() & (F.col("Points").cast("int") < 0)
).withColumn("rejection_reason", F.lit("NEGATIVE_POINTS"))
quarantine_rows.append(neg_pts)
df = df.filter(F.col("Points").isNull() | (F.col("Points").cast("int") >= 0))

# CR-07: Time format when not null (INVALID_LAP_TIME_FORMAT)
td_pattern = r"^0 days \d{2}:\d{2}:\d{2}(\.\d+)?$"
invalid_time = df.filter(
    F.col("Time").isNotNull() & (~F.col("Time").rlike(td_pattern))
).withColumn("rejection_reason", F.lit("INVALID_LAP_TIME_FORMAT"))
quarantine_rows.append(invalid_time)
df = df.filter(F.col("Time").isNull() | F.col("Time").rlike(td_pattern))

# CR-10: Duplicate primary key (DUPLICATE_PRIMARY_KEY)
dup_window = Window.partitionBy("round_number", "Driver ID")
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
    write_quarantine(quarantine_df, CATALOG, "constructor_results")
else:
    rows_quarantined = 0

print(f"Rows quarantined: {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 5: Apply Transformations

# COMMAND ----------

df = df \
    .withColumn("position", F.col("Position").cast("int")) \
    .withColumn("points", F.col("Points").cast("int")) \
    .withColumn("time_seconds", parse_timedelta_seconds(F.col("Time"))) \
    .drop("Full Name", "Position", "Points", "Time")

# COMMAND ----------
# MAGIC %md ## Step 6: Deduplication

# COMMAND ----------

pk_cols = ["round_number", "Driver ID"]
df = df.dropDuplicates(pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 7: Merge into Enriched

# COMMAND ----------

merge_into_enriched(spark, df, CATALOG, ENRICHED_SCHEMA, "constructor_results", pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 8: Summary

# COMMAND ----------

rows_merged = df.count()
print(f"rows_read      : {rows_read:,}")
print(f"rows_quarantined: {rows_quarantined:,}")
print(f"rows_merged    : {rows_merged:,}")
