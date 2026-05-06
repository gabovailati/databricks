# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Platform — Entity Profiling
# MAGIC
# MAGIC **Notebook**: `00_profiling/profile_entities.py`
# MAGIC **Sprint**: Issue #1 — Data Profiling Sprint
# MAGIC **Purpose**: Profile all 13 source entities: row counts, schema inference, null rates, and distinct value counts.
# MAGIC
# MAGIC Run this notebook against the raw `datasource/` folder before any Bronze ingestion.
# MAGIC Results are printed inline; no Delta tables are written.

# COMMAND ----------
# MAGIC %md ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("schema", "f1_raw", "Schema / database name")
dbutils.widgets.text("datasource_path", "/Volumes/workspace/f1_raw/landing/datasource", "Absolute path to datasource/ folder")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DATASOURCE_PATH = dbutils.widgets.get("datasource_path")

print(f"Catalog        : {CATALOG}")
print(f"Schema         : {SCHEMA}")
print(f"Datasource path: {DATASOURCE_PATH}")

# COMMAND ----------
# MAGIC %md ## 1. Shared Helpers

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def profile_df(df: DataFrame, entity_name: str) -> None:
    """Print row count, inferred schema, null counts, and distinct counts for every column."""
    row_count = df.count()
    col_names = df.columns
    col_count = len(col_names)

    print(f"\n{'='*70}")
    print(f"  ENTITY: {entity_name}")
    print(f"  Rows: {row_count:,}   |   Columns: {col_count}")
    print(f"{'='*70}")

    df.printSchema()

    # Build null-count and distinct-count aggregations in a single pass
    null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"nulls__{c}") for c in col_names]
    distinct_exprs = [F.countDistinct(F.col(c)).alias(f"distinct__{c}") for c in col_names]
    stats = df.agg(*(null_exprs + distinct_exprs)).collect()[0].asDict()

    header = f"{'Column':<35} {'Type':<20} {'Nulls':>8} {'Null%':>8} {'Distinct':>10}"
    print(header)
    print("-" * len(header))

    for field in df.schema.fields:
        c = field.name
        dtype = str(field.dataType)
        nulls = stats[f"nulls__{c}"]
        distinct = stats[f"distinct__{c}"]
        null_pct = (nulls / row_count * 100) if row_count > 0 else 0.0
        flag = "  <<< ENTIRELY NULL" if nulls == row_count and row_count > 0 else ""
        print(f"  {c:<33} {dtype:<20} {nulls:>8,} {null_pct:>7.1f}% {distinct:>10,}{flag}")

    print()


def read_csv(path: str, **options) -> DataFrame:
    """Read a CSV with sensible defaults. dropna(how='all') removes trailing blank rows."""
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .options(**options)
        .csv(path)
    )
    # Trailing blank rows are common in pandas-exported CSVs
    return df.dropna(how="all")

# COMMAND ----------
# MAGIC %md ## 2. Race / Event Entities

# COMMAND ----------
# MAGIC %md ### 2.1 races_data

# COMMAND ----------

races_data_path = f"{DATASOURCE_PATH}/races_data.csv"
races_data = read_csv(races_data_path)
profile_df(races_data, "races_data")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - `Last Session` has 1 null (Round 0 — Pre-Season Testing; expected)
# MAGIC - `First Session` / `Last Session` are tz-aware strings with mixed UTC offsets — not yet UTC-normalised
# MAGIC - `Official Event Name` contains non-ASCII characters
# MAGIC - Round 0 is a testing event, not a race; may need filtering in downstream models

# COMMAND ----------
# MAGIC %md ### 2.2 season_data

# COMMAND ----------

season_data_path = f"{DATASOURCE_PATH}/season_data.csv"
season_data = read_csv(season_data_path)
profile_df(season_data, "season_data")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - This file is a strict column subset of `races_data` (missing only `Official Event Name`)
# MAGIC - All rows are identical to `races_data` — consolidation into `dim_season_calendar` is recommended
# MAGIC - Candidate for retirement: source `dim_season_calendar` from `races_data` only

# COMMAND ----------
# MAGIC %md ### 2.3 race_results

# COMMAND ----------

race_results_path = f"{DATASOURCE_PATH}/race_results.csv"
race_results = read_csv(race_results_path)
profile_df(race_results, "race_results")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - `CountryCode`: 100% null — upstream extraction bug; drop until fixed (DQ-01)
# MAGIC - `Q1`, `Q2`, `Q3`: 100% null — qualifying data was not joined at extraction time (DQ-02)
# MAGIC - `Time`: pandas timedelta string `"0 days HH:MM:SS.ffffff"`. Semantic duality: winner = race duration, others = gap to leader. 3 nulls for retired drivers are expected.
# MAGIC - `ClassifiedPosition`: mixed type — digit strings for finishers, `"R"` for retirees
# MAGIC - `Position`, `GridPosition`, `Points`, `Laps`: stored as float64 — cast to IntegerType in Silver
# MAGIC - No `Round` column — must be injected as a pipeline parameter at ingest time (DQ-05)

# COMMAND ----------
# MAGIC %md ### 2.4 sprint_results

# COMMAND ----------

sprint_results_path = f"{DATASOURCE_PATH}/sprint_results.csv"
sprint_results = read_csv(sprint_results_path)
profile_df(sprint_results, "sprint_results")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - Same schema as `race_results`; same CountryCode / Q1/Q2/Q3 null issues apply
# MAGIC - Ocon's `Time` value is `"0 days 00:00:31"` (no microseconds) — timedelta UDF must handle both `HH:MM:SS.ffffff` and `HH:MM:SS` variants (DQ-15)
# MAGIC - 2 retired drivers in this sprint vs 3 in race_results
# MAGIC - No `Round` column — inject at ingest (DQ-05)

# COMMAND ----------
# MAGIC %md ### 2.5 circuit_info (JSON)

# COMMAND ----------

circuit_info_path = f"{DATASOURCE_PATH}/circuit_info.json"

# Read as a single-row DataFrame with the nested JSON schema
circuit_info_raw = spark.read.option("multiline", "true").json(circuit_info_path)

print(f"\n{'='*70}")
print(f"  ENTITY: circuit_info")
print(f"  Format: Single JSON document (1 row after read)")
print(f"{'='*70}")

circuit_info_raw.printSchema()
circuit_info_raw.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC **Structural notes**:
# MAGIC - `sessions` is a nested struct with generic keys `Session1`–`Session5`
# MAGIC - Each session has `name`, `date` (tz-aware local time, BRT −03:00), and `utc` (naive UTC, no Z suffix)
# MAGIC - `event_date` is a naive datetime string (no timezone)
# MAGIC - Three different datetime formats in a single document — all must be normalised to UTC TimestampType in Silver
# MAGIC - `event` field matches `races_data.Official Event Name`

# COMMAND ----------

# Flatten the nested sessions struct for easier profiling
from pyspark.sql.functions import col

sessions_flat = circuit_info_raw.select(
    col("name").alias("circuit_name"),
    col("country"),
    col("event"),
    col("format"),
    col("event_date"),
    col("sessions.Session1.name").alias("session1_name"),
    col("sessions.Session1.date").alias("session1_date"),
    col("sessions.Session1.utc").alias("session1_utc"),
    col("sessions.Session2.name").alias("session2_name"),
    col("sessions.Session2.date").alias("session2_date"),
    col("sessions.Session2.utc").alias("session2_utc"),
    col("sessions.Session3.name").alias("session3_name"),
    col("sessions.Session3.date").alias("session3_date"),
    col("sessions.Session3.utc").alias("session3_utc"),
    col("sessions.Session4.name").alias("session4_name"),
    col("sessions.Session4.date").alias("session4_date"),
    col("sessions.Session4.utc").alias("session4_utc"),
    col("sessions.Session5.name").alias("session5_name"),
    col("sessions.Session5.date").alias("session5_date"),
    col("sessions.Session5.utc").alias("session5_utc"),
)

print("circuit_info — flattened sessions view:")
sessions_flat.show(truncate=False)
profile_df(sessions_flat, "circuit_info (flattened)")

# COMMAND ----------
# MAGIC %md ## 3. Driver Entities

# COMMAND ----------
# MAGIC %md ### 3.1 drivers_data

# COMMAND ----------

drivers_data_path = f"{DATASOURCE_PATH}/drivers_data.csv"
drivers_data = read_csv(drivers_data_path)
profile_df(drivers_data, "drivers_data")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - `Full Name` is redundant — derivable from `First Name` + `Last Name`
# MAGIC - `Number` (car number) can change between seasons — not a stable surrogate key
# MAGIC - `Driver ID` (slug) is the recommended canonical FK for all downstream joins

# COMMAND ----------
# MAGIC %md ### 3.2 driver_standings

# COMMAND ----------

driver_standings_path = f"{DATASOURCE_PATH}/driver_standings.csv"
driver_standings = read_csv(driver_standings_path)
profile_df(driver_standings, "driver_standings")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - `Points` and `Position` stored as float64 — cast to IntegerType in Silver
# MAGIC - No `Round` temporal context — this is a point-in-time snapshot; inject `Round` at ingest (DQ-05)
# MAGIC - `Driver ID`, `Abbreviation`, `Full Name`, `Team` are redundant with `drivers_data` — Silver fact table should carry only `(Round, Driver ID, Points, Position)`

# COMMAND ----------
# MAGIC %md ### 3.3 qualifying_results

# COMMAND ----------

qualifying_results_path = f"{DATASOURCE_PATH}/qualifying_results.csv"
qualifying_results = read_csv(qualifying_results_path)
profile_df(qualifying_results, "qualifying_results")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - **BOR's `Driver ID` is null** — must be corrected to `bortoleto` before any FK join (DQ-06)
# MAGIC - `Q2 Time` null for 5 Q1-eliminated drivers + BOR: structurally expected, not a data error
# MAGIC - `Q3 Time` null for 10 non-top-10 drivers: structurally expected
# MAGIC - All time columns use pandas timedelta string format — requires shared UDF
# MAGIC - `Position` stored as float64; 1 null (BOR)
# MAGIC - `Abbreviation` is the only safe PK candidate (no nulls, all unique)

# COMMAND ----------

# Verify the BOR null and surface the fix candidate
print("Rows where Driver ID is null:")
qualifying_results.filter(F.col("Driver ID").isNull()).show(truncate=False)

# COMMAND ----------
# MAGIC %md ### 3.4 status_data

# COMMAND ----------

status_data_path = f"{DATASOURCE_PATH}/status_data.csv"
status_data = read_csv(status_data_path)
profile_df(status_data, "status_data")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - Only 2 distinct `Status` values in this sample; production F1 data has many more (`+1 Lap`, `Engine`, `Collision`, etc.)
# MAGIC - No `Driver ID` column — joins to slug-based entities require a lookup via `Abbreviation`
# MAGIC - No `Round` temporal context — inject at ingest (DQ-05)

# COMMAND ----------

# Show status distribution
print("Status distribution:")
status_data.groupBy("Status").count().orderBy(F.col("count").desc()).show()

# COMMAND ----------
# MAGIC %md ## 4. Constructor / Timing Entities

# COMMAND ----------
# MAGIC %md ### 4.1 constructors_data

# COMMAND ----------

constructors_data_path = f"{DATASOURCE_PATH}/constructors_data.csv"
constructors_data = read_csv(constructors_data_path)
profile_df(constructors_data, "constructors_data")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - Single-column table — `Team Name` is the only identifier
# MAGIC - No `TeamId` slug, no `TeamColor`, no `Nationality` — severely underspecified dimension
# MAGIC - String team name is vulnerable to spelling drift across entities; enrichment from `race_results.TeamId` is recommended (DQ-10)

# COMMAND ----------
# MAGIC %md ### 4.2 constructor_standings

# COMMAND ----------

constructor_standings_path = f"{DATASOURCE_PATH}/constructor_standings.csv"
constructor_standings = read_csv(constructor_standings_path)
profile_df(constructor_standings, "constructor_standings")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - **Byte-for-byte identical to `constructor_results.csv`** — one must be differentiated or retired (DQ-04)
# MAGIC - `Position` and `Points` stored as float64
# MAGIC - `Time` pandas timedelta string; 3 nulls for retired drivers — expected
# MAGIC - No `Round` column — inject at ingest (DQ-05)
# MAGIC - Despite the name, this appears to contain driver-level race results, not constructor-aggregated standings

# COMMAND ----------
# MAGIC %md ### 4.3 constructor_results

# COMMAND ----------

constructor_results_path = f"{DATASOURCE_PATH}/constructor_results.csv"
constructor_results = read_csv(constructor_results_path)
profile_df(constructor_results, "constructor_results")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - **Identical to `constructor_standings.csv`** — see DQ-04

# COMMAND ----------

# Confirm the files are identical by comparing row counts and hashes
cs_count = constructor_standings.count()
cr_count = constructor_results.count()

# Compare sorted content: if the two DataFrames are identical, the except() of each against the other should be empty
cs_minus_cr = constructor_standings.exceptAll(constructor_results).count()
cr_minus_cs = constructor_results.exceptAll(constructor_standings).count()

print(f"constructor_standings row count : {cs_count}")
print(f"constructor_results   row count : {cr_count}")
print(f"Rows in standings NOT in results: {cs_minus_cr}")
print(f"Rows in results NOT in standings: {cr_minus_cs}")
print(f"Files are {'IDENTICAL' if cs_minus_cr == 0 and cr_minus_cs == 0 else 'DIFFERENT'}")

# COMMAND ----------
# MAGIC %md ### 4.4 lap_times

# COMMAND ----------

lap_times_path = f"{DATASOURCE_PATH}/lap_times.csv"
lap_times = read_csv(lap_times_path)
profile_df(lap_times, "lap_times")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - All time columns (`Lap Time`, `Time`, `Sector 1`, `Sector 2`, `Sector 3`) use pandas timedelta string format — requires shared UDF
# MAGIC - `Sector 1` null on lap 1 for all 20 drivers — structurally expected (no sector 1 crossing before the start line on lap 1)
# MAGIC - `Driver` uses 3-letter abbreviations — incompatible with slug format in results/standings; mapping table required (DQ-07)
# MAGIC - `Lap Number` and `Position` stored as float64
# MAGIC - No `Round` column — inject at ingest (DQ-05)

# COMMAND ----------

# Show lap 1 sector 1 null pattern — confirm it is exactly 1 null per driver
print("Sector 1 null count by driver on Lap 1:")
lap_times.filter(F.col("Lap Number") == 1).groupBy("Driver").agg(
    F.sum(F.when(F.col("Sector 1").isNull(), 1).otherwise(0)).alias("sector1_nulls")
).orderBy("Driver").show(25)

# COMMAND ----------

# Show distinct driver abbreviations to verify mapping against drivers_data
print("Distinct driver abbreviations in lap_times:")
lap_times.select("Driver").distinct().orderBy("Driver").show(25)

# COMMAND ----------
# MAGIC %md ### 4.5 pit_stops

# COMMAND ----------

pit_stops_path = f"{DATASOURCE_PATH}/pit_stops.csv"
pit_stops = read_csv(pit_stops_path)
profile_df(pit_stops, "pit_stops")

# COMMAND ----------
# MAGIC %md
# MAGIC **Known DQ flags**:
# MAGIC - **`Pit In Time` is 100% null** — data collection failure; pit stop duration cannot be computed (DQ-03)
# MAGIC - `Pit Out Time` uses pandas timedelta string format
# MAGIC - `Lap Number` stored as float64
# MAGIC - `Driver` uses 3-letter abbreviations — same mapping gap as `lap_times` (DQ-07)
# MAGIC - No `Round` column — inject at ingest (DQ-05)

# COMMAND ----------

# Show pit stop counts per driver (expected: ~2 per driver for a standard race)
print("Pit stop counts per driver:")
pit_stops.groupBy("Driver").count().orderBy(F.col("count").desc()).show(25)

# COMMAND ----------
# MAGIC %md ## 5. Cross-Entity Validation

# COMMAND ----------
# MAGIC %md ### 5.1 Driver identifier coverage

# COMMAND ----------

# Driver slugs in drivers_data
drivers_slugs = drivers_data.select(F.col("Driver ID").alias("driver_id")).distinct()

# Driver slugs in race_results
race_slugs = race_results.select(F.col("DriverId").alias("driver_id")).distinct()

# Abbreviations in lap_times — map via drivers_data
lap_abbrevs = lap_times.select(F.col("Driver").alias("abbreviation")).distinct()
driver_abbrev_map = drivers_data.select(
    F.col("Driver ID").alias("driver_id"),
    F.col("Abbreviation").alias("abbreviation")
)

# Drivers in lap_times with no matching drivers_data abbreviation
unmatched_lap = lap_abbrevs.join(driver_abbrev_map, on="abbreviation", how="left_anti")
print(f"lap_times drivers with no match in drivers_data.Abbreviation: {unmatched_lap.count()}")
unmatched_lap.show()

# Drivers in race_results with no match in drivers_data
unmatched_race = race_slugs.join(drivers_slugs, on="driver_id", how="left_anti")
print(f"race_results DriverId values with no match in drivers_data.Driver ID: {unmatched_race.count()}")
unmatched_race.show()

# COMMAND ----------
# MAGIC %md ### 5.2 Duplicate file confirmation (constructor_standings vs constructor_results)

# COMMAND ----------

# Already computed above — print the summary again for the consolidated view
print(f"constructor_standings == constructor_results: {'YES (DUPLICATE)' if cs_minus_cr == 0 and cr_minus_cs == 0 else 'NO (DIFFERENT)'}")

# COMMAND ----------
# MAGIC %md ### 5.3 races_data vs season_data overlap

# COMMAND ----------

# season_data should be a strict column subset of races_data with identical rows
# Compare on shared columns
shared_cols = [c for c in season_data.columns if c in races_data.columns]
print(f"Shared columns between races_data and season_data: {shared_cols}")

races_subset = races_data.select(*[F.col(c) for c in shared_cols])
season_all = season_data.select(*[F.col(c) for c in shared_cols])

rd_minus_sd = races_subset.exceptAll(season_all).count()
sd_minus_rd = season_all.exceptAll(races_subset).count()

print(f"Rows in races_data (shared cols) NOT in season_data: {rd_minus_sd}")
print(f"Rows in season_data NOT in races_data (shared cols): {sd_minus_rd}")
print(f"season_data is a{'PERFECT' if rd_minus_sd == 0 and sd_minus_rd == 0 else 'N IMPERFECT'} subset of races_data on shared columns")

# COMMAND ----------
# MAGIC %md ### 5.4 BOR Driver ID fix validation

# COMMAND ----------

# Confirm BOR abbreviation exists in drivers_data and what slug it maps to
print("BOR entry in drivers_data:")
drivers_data.filter(F.col("Abbreviation") == "BOR").show(truncate=False)

print("BOR entry in qualifying_results (Driver ID should be null):")
qualifying_results.filter(F.col("Abbreviation") == "BOR").show(truncate=False)

# COMMAND ----------
# MAGIC %md ### 5.5 Timedelta string sample inspection

# COMMAND ----------

# Show sample timedelta values to confirm format variations
print("Sample Time values from race_results:")
race_results.select("DriverId", "Time", "Status").show(5, truncate=False)

print("Sample Time values from sprint_results (check Ocon for missing microseconds):")
sprint_results.select("DriverId", "Time", "Status").show(5, truncate=False)

print("Sample Lap Time values from lap_times:")
lap_times.select("Driver", "Lap Number", "Lap Time", "Sector 1", "Sector 2", "Sector 3").show(5, truncate=False)

print("Sample Q1 Time values from qualifying_results:")
qualifying_results.select("Abbreviation", "Q1 Time", "Q2 Time", "Q3 Time").show(5, truncate=False)

# COMMAND ----------
# MAGIC %md ## 6. Summary

# COMMAND ----------

# Print a final summary of entity sizes for quick reference
entities = [
    ("races_data", races_data),
    ("season_data", season_data),
    ("race_results", race_results),
    ("sprint_results", sprint_results),
    ("drivers_data", drivers_data),
    ("driver_standings", driver_standings),
    ("qualifying_results", qualifying_results),
    ("status_data", status_data),
    ("constructors_data", constructors_data),
    ("constructor_standings", constructor_standings),
    ("constructor_results", constructor_results),
    ("lap_times", lap_times),
    ("pit_stops", pit_stops),
]

print(f"\n{'Entity':<30} {'Rows':>8}   {'Columns':>8}")
print("-" * 52)
for name, df in entities:
    print(f"  {name:<28} {df.count():>8,}   {len(df.columns):>8}")

print("\nProfiling complete. Review the output above and the data_profiling_report.md before Gate 1 sign-off.")
