# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Bootstrap
# MAGIC
# MAGIC **Notebook**: `00_setup/00_bootstrap.py`
# MAGIC **Purpose**: One-time (idempotent) creation of catalog, schemas, and volume. Safe to re-run.
# MAGIC
# MAGIC Run this notebook once on a fresh workspace before executing any pipeline notebook.
# MAGIC All statements use `IF NOT EXISTS` — re-running will not overwrite data.

# COMMAND ----------
# MAGIC %md ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "f1_platform", "Unity Catalog name")
dbutils.widgets.text("landing_volume", "landing", "Volume name for raw file landing")

CATALOG = dbutils.widgets.get("catalog")
LANDING_VOLUME = dbutils.widgets.get("landing_volume")

SCHEMAS = ["raw", "enriched", "ids", "curated", "quarantine"]

print(f"Catalog : {CATALOG}")
print(f"Schemas : {SCHEMAS}")
print(f"Volume  : {CATALOG}.raw.{LANDING_VOLUME}")

# COMMAND ----------
# MAGIC %md ## 1. Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
print(f"✓ Catalog `{CATALOG}` ready")

# COMMAND ----------
# MAGIC %md ## 2. Schemas

# COMMAND ----------

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`")
    print(f"✓ Schema `{CATALOG}`.`{schema}` ready")

# COMMAND ----------
# MAGIC %md ## 3. Landing Volume

# COMMAND ----------

# Volume holds incoming CSV/JSON files delivered by the data provider.
# Pipeline notebooks reference this path as:
#   /Volumes/{catalog}/raw/{landing_volume}/datasource/
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`raw`.`{LANDING_VOLUME}`
    COMMENT 'Landing zone for raw F1 source files (CSV/JSON) delivered by the data provider'
""")
print(f"✓ Volume `{CATALOG}`.raw.`{LANDING_VOLUME}` ready")
print(f"  → Upload datasource/ files to: /Volumes/{CATALOG}/raw/{LANDING_VOLUME}/datasource/")

# COMMAND ----------
# MAGIC %md ## 4. Verify

# COMMAND ----------

print("\n=== Catalog contents ===")
display(spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`"))

# COMMAND ----------

print(f"\n=== Volume path ===")
try:
    files = dbutils.fs.ls(f"/Volumes/{CATALOG}/raw/{LANDING_VOLUME}/")
    for f in files:
        print(f"  {f.path}  ({f.size} bytes)")
except Exception:
    print(f"  /Volumes/{CATALOG}/raw/{LANDING_VOLUME}/ is empty — upload datasource/ files before running pipelines")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Next Steps
# MAGIC
# MAGIC 1. Upload all files from `datasource/` to `/Volumes/f1_platform/raw/landing/datasource/`
# MAGIC    - Use the Databricks UI: Catalog → f1_platform → raw → landing → Upload files
# MAGIC    - Or via Databricks CLI: `databricks fs cp datasource/ dbfs:/Volumes/f1_platform/raw/landing/datasource/ --recursive`
# MAGIC 2. Connect this repo via Databricks Repos (Workspace → Repos → Add Repo → paste GitHub URL)
# MAGIC 3. Run `notebooks/01_raw/00_file_arrival_sensor.py` with `datasource_path = /Volumes/f1_platform/raw/landing/datasource`
# MAGIC 4. Run `notebooks/01_raw/02_historical_backfill.py` with `round_number` for the current round
