# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Enriched Layer Shared Utilities
# MAGIC
# MAGIC **Notebook**: `02_enriched/00_shared_utils.py`
# MAGIC **Sprint**: Issue #5 — Enriched Layer Pipeline
# MAGIC **Purpose**: Shared utility functions for all Enriched layer entity notebooks. Meant to be invoked via `%run ./00_shared_utils` from each entity notebook.
# MAGIC
# MAGIC Contains:
# MAGIC - `replace_sentinels(df)` — global sentinel string replacement (GLOBAL-01)
# MAGIC - `parse_timedelta_seconds` — UDF for pandas timedelta string → total seconds (DoubleType)
# MAGIC - `write_quarantine(bad_rows_df, catalog, entity)` — append-only quarantine writer
# MAGIC - `merge_into_enriched(spark, source_df, catalog, schema, entity, pk_cols)` — Delta MERGE upsert helper

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from delta.tables import DeltaTable
import re

# COMMAND ----------
# MAGIC %md ## A. Global Sentinel Replacement

# COMMAND ----------

def replace_sentinels(df: DataFrame) -> DataFrame:
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(
            c,
            F.when(F.col(c).isin(["nan", "None", "NaT"]), None).otherwise(F.col(c))
        )
    return df

# COMMAND ----------
# MAGIC %md ## B. Timedelta Parser UDF

# COMMAND ----------

def _parse_timedelta_seconds_fn(s):
    if s is None:
        return None
    pattern = r"^0 days (\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?$"
    m = re.match(pattern, s)
    if m is None:
        return None
    hours = int(m.group(1))
    minutes = int(m.group(2))
    seconds = int(m.group(3))
    microseconds_str = m.group(4)
    if microseconds_str:
        frac = float("0." + microseconds_str)
    else:
        frac = 0.0
    return float(hours * 3600 + minutes * 60 + seconds) + frac

parse_timedelta_seconds = F.udf(_parse_timedelta_seconds_fn, DoubleType())

# COMMAND ----------
# MAGIC %md ## C. Quarantine Writer

# COMMAND ----------

def write_quarantine(bad_rows_df: DataFrame, catalog: str, entity: str, rejection_reason_col: str = "rejection_reason") -> None:
    if bad_rows_df is None or bad_rows_df.rdd.isEmpty():
        return

    required_cols = [rejection_reason_col, "source_file", "ingestion_timestamp", "round_number"]
    for col_name in required_cols:
        if col_name not in bad_rows_df.columns:
            if col_name == "ingestion_timestamp":
                bad_rows_df = bad_rows_df.withColumn("ingestion_timestamp", F.current_timestamp())
            elif col_name == "round_number":
                bad_rows_df = bad_rows_df.withColumn("round_number", F.lit(None).cast("int"))
            elif col_name == "source_file":
                bad_rows_df = bad_rows_df.withColumn("source_file", F.lit(None).cast("string"))
            elif col_name == rejection_reason_col:
                bad_rows_df = bad_rows_df.withColumn(rejection_reason_col, F.lit(None).cast("string"))

    quarantine_table = f"{catalog}.quarantine.{entity}"

    bad_rows_df = bad_rows_df.withColumn(
        "ingestion_date",
        F.to_date(F.col("ingestion_timestamp"))
    )

    bad_rows_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("ingestion_date") \
        .saveAsTable(quarantine_table)

# COMMAND ----------
# MAGIC %md ## D. Merge / Upsert Helper

# COMMAND ----------

def merge_into_enriched(spark: SparkSession, source_df: DataFrame, catalog: str, schema: str, entity: str, pk_cols: list) -> None:
    full_table_name = f"{catalog}.{schema}.{entity}"

    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA AS SELECT * FROM (SELECT * FROM (VALUES (1)) t) WHERE 1=0")

    if not spark.catalog.tableExists(full_table_name):
        source_df.write.format("delta").saveAsTable(full_table_name)
        return

    try:
        dt = DeltaTable.forName(spark, full_table_name)
    except Exception:
        source_df.write.format("delta").saveAsTable(full_table_name)
        return

    match_condition = " AND ".join([f"target.`{c}` = source.`{c}`" for c in pk_cols])

    dt.alias("target") \
        .merge(source_df.alias("source"), match_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
