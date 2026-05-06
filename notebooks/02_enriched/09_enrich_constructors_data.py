# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched: constructors_data
# MAGIC
# MAGIC **Notebook**: `02_enriched/09_enrich_constructors_data.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Layer**: Enriched
# MAGIC **Purpose**: Cleanse, validate, and merge `constructors_data` from Raw into Enriched. Applies DQ rules CD-01 through CD-06. Enriches with `team_id` and `team_color` by joining against `race_results` Raw table.

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

df = spark.table(f"{CATALOG}.{RAW_SCHEMA}.constructors_data")
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

# CD-01: Team Name not null (NULL_PRIMARY_KEY)
null_team = df.filter(F.col("Team Name").isNull()).withColumn("rejection_reason", F.lit("NULL_PRIMARY_KEY"))
quarantine_rows.append(null_team)
df = df.filter(F.col("Team Name").isNotNull())

# CD-02: Team Name non-empty (EMPTY_TEAM_NAME)
empty_team = df.filter(F.length(F.trim(F.col("Team Name"))) == 0).withColumn("rejection_reason", F.lit("EMPTY_TEAM_NAME"))
quarantine_rows.append(empty_team)
df = df.filter(F.length(F.trim(F.col("Team Name"))) > 0)

# CD-03: Duplicate Team Name (DUPLICATE_PRIMARY_KEY)
dup_window = Window.partitionBy("Team Name")
df_with_dup = df.withColumn("_dup_count", F.count("*").over(dup_window))
dup_team = df_with_dup.filter(F.col("_dup_count") > 1).drop("_dup_count").withColumn("rejection_reason", F.lit("DUPLICATE_PRIMARY_KEY"))
quarantine_rows.append(dup_team)
df = df_with_dup.filter(F.col("_dup_count") == 1).drop("_dup_count")

# COMMAND ----------
# MAGIC %md ## Step 4: Write Quarantine (pre-enrichment)

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

non_empty = [q for q in quarantine_rows if not q.rdd.isEmpty()]
if non_empty:
    quarantine_df = reduce(DataFrame.unionByName, non_empty, non_empty[0]).dropDuplicates()
    quarantine_df = quarantine_df.withColumn("ingestion_timestamp", F.current_timestamp())
    rows_quarantined = quarantine_df.count()
    write_quarantine(quarantine_df, CATALOG, "constructors_data")
else:
    rows_quarantined = 0

print(f"Rows quarantined (pre-enrichment): {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 5: Apply Transformations — Enrich with team_id and team_color

# COMMAND ----------

race_results_raw = spark.table(f"{CATALOG}.{RAW_SCHEMA}.race_results") \
    .select("TeamName", "TeamId", "TeamColor") \
    .distinct()

df = df.join(race_results_raw, df["Team Name"] == race_results_raw["TeamName"], how="left") \
    .drop("TeamName") \
    .withColumnRenamed("TeamId", "team_id") \
    .withColumnRenamed("TeamColor", "team_color")

# Post-enrichment DQ checks
quarantine_rows_post = []

# CD-04: team_id not null after enrichment (NULL_TEAM_ID)
null_team_id = df.filter(F.col("team_id").isNull()).withColumn("rejection_reason", F.lit("NULL_TEAM_ID"))
quarantine_rows_post.append(null_team_id)

# CD-05: team_id slug format (INVALID_TEAM_ID_FORMAT)
slug_pattern = r"^[a-z0-9_]+$"
invalid_team_id = df.filter(
    F.col("team_id").isNotNull() & (~F.col("team_id").rlike(slug_pattern))
).withColumn("rejection_reason", F.lit("INVALID_TEAM_ID_FORMAT"))
quarantine_rows_post.append(invalid_team_id)

# CD-06: TeamColor format (INVALID_TEAM_COLOR_FORMAT)
hex_pattern = r"^[0-9A-Fa-f]{6}$"
invalid_color = df.filter(
    F.col("team_color").isNotNull() & (~F.col("team_color").rlike(hex_pattern))
).withColumn("rejection_reason", F.lit("INVALID_TEAM_COLOR_FORMAT"))
quarantine_rows_post.append(invalid_color)

non_empty_post = [q for q in quarantine_rows_post if not q.rdd.isEmpty()]
if non_empty_post:
    quarantine_df_post = reduce(DataFrame.unionByName, non_empty_post, non_empty_post[0]).dropDuplicates()
    quarantine_df_post = quarantine_df_post.withColumn("ingestion_timestamp", F.current_timestamp())
    rows_quarantined_post = quarantine_df_post.count()
    write_quarantine(quarantine_df_post, CATALOG, "constructors_data")
    rows_quarantined += rows_quarantined_post

print(f"Total rows quarantined: {rows_quarantined:,}")

# COMMAND ----------
# MAGIC %md ## Step 6: Deduplication

# COMMAND ----------

pk_cols = ["Team Name"]
df = df.dropDuplicates(pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 7: Merge into Enriched

# COMMAND ----------

merge_into_enriched(spark, df, CATALOG, ENRICHED_SCHEMA, "constructors_data", pk_cols)

# COMMAND ----------
# MAGIC %md ## Step 8: Summary

# COMMAND ----------

rows_merged = df.count()
print(f"rows_read      : {rows_read:,}")
print(f"rows_quarantined: {rows_quarantined:,}")
print(f"rows_merged    : {rows_merged:,}")
