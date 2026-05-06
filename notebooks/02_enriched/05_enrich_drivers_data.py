# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched: drivers_data
# MAGIC
# MAGIC **Notebook**: `02_enriched/05_enrich_drivers_data.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Layer**: Enriched
# MAGIC **Purpose**: Cleanse, validate, and merge `drivers_data` from Raw into Enriched. Applies DQ rules DR-01 through DR-12. Drops redundant `full_name` column. Canonical driver dimension table.

# COMMAND ----------

dbutils.widgets.text("catalog", "f1_platform", "Unity Catalog name")
dbutils.widgets.text("raw_schema", "raw", "Raw schema name")
dbutils.widgets.text("enriched_schema", "enriched", "Enriched schema name")
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

df = spark.table(f"{CATALOG}.{RAW_SCHEMA}.drivers_data")
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

# DR-01: Driver ID not null (NULL_PRIMARY_KEY)
null_id = df.filter(F.col("Driver ID").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_id)
df = df.filter(F.col("Driver ID").isNotNull())

# DR-02: Abbreviation not null (NULL_ABBREVIATION)
null_abbr = df.filter(F.col("Abbreviation").isNull()).withColumn("rejection_reason", F.lit("NULL_ABBREVIATION"))
quarantine_rows.append(null_abbr)
df = df.filter(F.col("Abbreviation").isNotNull())

# DR-03: Number not null (NULL_DRIVER_NUMBER)
null_number = df.filter(F.col("Number").isNull()).withColumn("rejection_reason", F.lit("NULL_DRIVER_NUMBER"))
quarantine_rows.append(null_number)
df = df.filter(F.col("Number").isNotNull())

# DR-07: First Name not null (NULL_DRIVER_NAME)
null_fname = df.filter(F.col("First Name").isNull()).withColumn("rejection_reason", F.lit("NULL_DRIVER_NAME"))
quarantine_rows.append(null_fname)
df = df.filter(F.col("First Name").isNotNull())

# DR-08: Last Name not null (NULL_DRIVER_NAME)
null_lname = df.filter(F.col("Last Name").isNull()).withColumn("rejection_reason", F.lit("NULL_DRIVER_NAME"))
quarantine_rows.append(null_lname)
df = df.filter(F.col("Last Name").isNotNull())

# DR-09: Team not null (NULL_TEAM_REFERENCE)
null_team = df.filter(F.col("Team").isNull()).withColumn("rejection_reason", F.lit("NULL_TEAM_REFERENCE"))
quarantine_rows.append(null_team)
df = df.filter(F.col("Team").isNotNull())

# DR-04: Driver ID slug format (INVALID_DRIVER_ID_FORMAT)
slug_pattern = r"^[a-z0-9_]+$"
invalid_slug = df.filter(~F.col("Driver ID").rlike(slug_pattern)).withColumn("rejection_reason", F.lit("INVALID_DRIVER_ID_FORMAT"))
quarantine_rows.append(invalid_slug)
df = df.filter(F.col("Driver ID").rlike(slug_pattern))

# DR-05: Abbreviation format (INVALID_ABBREVIATION_FORMAT)
abbr_pattern = r"^[A-Z]{3}$"
invalid_abbr = df.filter(~F.col("Abbreviation").rlike(abbr_pattern)).withColumn("rejection_reason", F.lit("INVALID_ABBREVIATION_FORMAT"))
quarantine_rows.append(invalid_abbr)
df = df.filter(F.col("Abbreviation").rlike(abbr_pattern))

# DR-06: Number range (DRIVER_NUMBER_OUT_OF_RANGE)
num_oor = df.filter(
    (F.col("Number") < 1) | (F.col("Number") > 99)
).withColumn("rejection_reason", F.lit("DRIVER_NUMBER_OUT_OF_RANGE"))
quarantine_rows.append(num_oor)

# DR-11: Duplicate Driver ID (DUPLICATE_PRIMARY_KEY)
dup_id_window = Window.partitionBy("Driver ID")
df_with_dup = df.withColumn("_dup_count", F.count("*").over(dup_id_window))
dup_id = df_with_dup.filter(F.col("_dup_count") > 1).drop("_dup_count").withColumn("rejection_reason", F.lit("DUPLICATE_PRIMARY_KEY"))
quarantine_rows.append(dup_id)
df = df_with_dup.filter(F.col("_dup_count") == 1).drop("_dup_count")

# DR-12: Duplicate Abbreviation (DUPLICATE_ABBREVIATION)
dup_abbr_window = Window.partitionBy("Abbreviation")
df_with_dup_abbr = df.withColumn("_dup_count", F.count("*").over(dup_abbr_window))
dup_abbr = df_with_dup_abbr.filter(F.col("_dup_count") > 1).drop("_dup_count").withColumn("rejection_reason", F.lit("DUPLICATE_ABBREVIATION"))
quarantine_rows.append(dup_abbr)
df = df_with_dup_abbr.filter(F.col("_dup_count") == 1).drop("_dup_count")

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
    write_quarantine(quarantine_df, CATALOG, "drivers_data")
else:
    rows_quarantined = 0

print(f"Rows quarantined: {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 5: Apply Transformations

# COMMAND ----------

df = df.drop("Full Name")

# COMMAND ----------
# MAGIC %md ## Step 6: Deduplication

# COMMAND ----------

pk_cols = ["Driver ID"]
df = df.dropDuplicates(pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 7: Merge into Enriched

# COMMAND ----------

merge_into_enriched(spark, df, CATALOG, ENRICHED_SCHEMA, "drivers_data", pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 8: Summary

# COMMAND ----------

rows_merged = df.count()
print(f"rows_read      : {rows_read:,}")
print(f"rows_quarantined: {rows_quarantined:,}")
print(f"rows_merged    : {rows_merged:,}")
