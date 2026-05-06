# Data Mapping — Formula 1 Data Platform
**Issue**: #2 — Data Architecture Foundation
**Date**: 2026-05-06
**Status**: Gate 2 Artifact — Awaiting Approval
**Prepared by**: Data Architect Agent

---

## Table of Contents

1. [Entity Overview](#1-entity-overview)
2. [Primary Key Definitions](#2-primary-key-definitions)
3. [Foreign Key Relationships](#3-foreign-key-relationships)
4. [Driver Identifier Mapping](#4-driver-identifier-mapping)
5. [Unity Catalog Naming Convention](#5-unity-catalog-naming-convention)
6. [Consolidation and Retirement Decisions](#6-consolidation-and-retirement-decisions)

---

## 1. Entity Overview

| Entity | Source File | Row Count | Primary Key | PK Type | Consolidation Action |
|--------|-------------|-----------|-------------|---------|---------------------|
| races_data | `datasource/races_data.csv` | 25 | `Round` | LongType | Source for `dim_season_calendar`; superset of `season_data` |
| season_data | `datasource/season_data.csv` | 25 | `Round` | LongType | **RETIRED** — strict subset of `races_data`; see Section 6 |
| race_results | `datasource/race_results.csv` | 20 | `(Round, DriverId)` | Composite | Active; `Round` injected at ingest |
| sprint_results | `datasource/sprint_results.csv` | 20 | `(Round, DriverId)` | Composite | Active; `Round` injected at ingest |
| circuit_info | `datasource/circuit_info.json` | 1 document / 5 sessions | `(event, session_name)` | Composite (after flatten) | Active; JSON flatten required |
| drivers_data | `datasource/drivers_data.csv` | 20 | `Driver ID` (slug) | StringType | Active; authoritative driver dimension |
| driver_standings | `datasource/driver_standings.csv` | 20 | `(Round, Driver ID)` | Composite | Active; `Round` injected at ingest |
| qualifying_results | `datasource/qualifying_results.csv` | 20 | `(Round, Abbreviation)` | Composite | Active; `Round` injected at ingest; BOR `Driver ID` sentinel fix required |
| status_data | `datasource/status_data.csv` | 20 | `(Round, Abbreviation)` | Composite | Active; `Round` injected at ingest |
| constructors_data | `datasource/constructors_data.csv` | 10 | `Team Name` (fragile) | StringType | Active; `TeamId` slug surrogate to be added |
| constructor_standings | `datasource/constructor_standings.csv` | 20 | — | — | **REDESIGN REQUIRED** — currently identical to `constructor_results`; to carry constructor-aggregated standings; see Section 6 |
| constructor_results | `datasource/constructor_results.csv` | 20 | `(Round, Driver ID)` | Composite | **AUTHORITATIVE** for per-race driver results; `Round` injected at ingest |
| lap_times | `datasource/lap_times.csv` | 1,251 | `(Round, Driver, Lap Number)` | Composite | Active; `Round` injected; `Driver` = 3-letter abbreviation |
| pit_stops | `datasource/pit_stops.csv` | 39 | `(Round, Driver, Lap Number)` | Composite | Active; `Round` injected; `Driver` = 3-letter abbreviation |

---

## 2. Primary Key Definitions

### 2.1 races_data

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | LongType | Range 0–24; Round 0 = Pre-Season Testing. Unique, stable within a season. This is the central FK referenced by all result/standings files. |

Rationale: `Round` is the only uniquely-identifying field that is numeric, stable, and already used as an implicit join key across all result entities. `Event Name` is also unique and human-readable but is a StringType — `Round` is preferred for FK joins.

---

### 2.2 season_data

Retired. See Section 6. `Round` was the candidate PK — identical to `races_data`.

---

### 2.3 race_results

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | IntegerType | Injected at ingest from pipeline parameter `round_number`. Not present in source CSV. |
| `DriverId` | StringType | Slug identifier (e.g. `norris`, `max_verstappen`). Unique per driver per event. |

Rationale: A single race snapshot contains 20 unique `DriverId` values. Multi-round history requires `Round` to disambiguate. Composite `(Round, DriverId)` is the minimal unique key.

---

### 2.4 sprint_results

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | IntegerType | Injected at ingest. |
| `DriverId` | StringType | Slug; same format as `race_results`. |

Rationale: Identical schema to `race_results`; same composite PK logic applies. In the IDS layer these two entities are candidates for unification under a `session_type` discriminator column.

---

### 2.5 circuit_info (after flattening)

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `event` | StringType | Full official event name string (e.g. `"FORMULA 1 MSC CRUISES GRANDE PRÊMIO DE SÃO PAULO 2025"`). Matches `races_data.Official Event Name`. |
| `session_name` | StringType | Session name extracted from nested `sessions.SessionN.name` (e.g. `"Race"`, `"Qualifying"`, `"Sprint"`). |

Rationale: The source JSON is a single document describing one circuit for one event. After flattening the `sessions` nested object (5 sessions per event), each row represents one session at one event. The composite `(event, session_name)` is the minimal unique key. `event` is the FK bridge to `races_data`.

---

### 2.6 drivers_data

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Driver ID` | StringType | Lowercase slug (e.g. `max_verstappen`, `norris`). All 20 values are unique. This is the **canonical driver identifier** across the entire platform. |

Rationale: `Abbreviation` (3-letter code) and `Number` (car number) are also unique in the current snapshot, but car numbers can change between seasons and abbreviations can theoretically conflict across seasons. The slug `Driver ID` is the most stable natural key. It is the FK target referenced by `race_results.DriverId`, `sprint_results.DriverId`, `qualifying_results.Driver ID`, `driver_standings.Driver ID`, `constructor_results.Driver ID`, and `constructor_standings.Driver ID`.

---

### 2.7 driver_standings

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | IntegerType | Injected at ingest. Standings are a point-in-time snapshot — `Round` anchors which round they represent. |
| `Driver ID` | StringType | Slug; FK to `drivers_data.Driver ID`. |

Rationale: The current file is a single-round snapshot with 20 unique `Driver ID` values. Historical standings require `Round` to create a time series. Composite `(Round, Driver ID)` is the minimal unique key.

---

### 2.8 qualifying_results

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | IntegerType | Injected at ingest. |
| `Abbreviation` | StringType | 3-letter uppercase code (e.g. `NOR`, `VER`). 20 unique values, 0 nulls. |

Rationale: `Driver ID` is disqualified as a PK component because BOR's value is the string literal `"nan"` (not SQL NULL). `Abbreviation` has 20 unique, non-null values and is a reliable PK component for this entity. After the Enriched layer sentinel fix corrects BOR's `Driver ID` to `bortoleto`, the preferred long-term PK should be migrated to `(Round, Driver ID)`.

---

### 2.9 status_data

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | IntegerType | Injected at ingest. |
| `Abbreviation` | StringType | 3-letter code; 20 unique values per event. |

Rationale: The file contains no `Driver ID` column — `Abbreviation` is the only available driver identifier. `Round` is required for historical accumulation.

---

### 2.10 constructors_data

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Team Name` | StringType | Only column in the source file. All 10 values are unique. |

Rationale: `Team Name` is the only available key. This is a fragile string key vulnerable to spelling drift (e.g. `"Haas F1 Team"` vs `"Haas"`). In the Enriched layer, a `team_id` slug surrogate (sourced from `race_results.TeamId`) must be added to create a stable FK anchor. Until that enrichment is applied, `Team Name` is the de facto PK.

---

### 2.11 constructor_standings

Primary key is deferred pending redesign. See Section 6. The current file is identical to `constructor_results` and carries driver-level race data, not constructor-aggregated standings. Once redesigned to carry constructor-level aggregated standings, the intended PK will be composite `(Round, TeamId)`.

---

### 2.12 constructor_results

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | IntegerType | Injected at ingest. |
| `Driver ID` | StringType | Slug; FK to `drivers_data.Driver ID`. |

Rationale: Designated as the **authoritative** source for per-race driver results at constructor level. The composite `(Round, Driver ID)` matches the pattern of `race_results` and `driver_standings`. `constructor_standings` is to be redesigned to carry different data.

---

### 2.13 lap_times

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | IntegerType | Injected at ingest. |
| `Driver` | StringType | 3-letter uppercase abbreviation (e.g. `VER`, `NOR`). Note: NOT a slug. See Section 4. |
| `Lap Number` | IntegerType | Lap number (stored as DoubleType in source; cast at ingest). |

Rationale: Each row is uniquely identified by the combination of round, driver, and lap. There are 1,251 rows across 20 drivers × ~71 laps (with some variation for retirees). The 3-letter `Driver` code is the only driver identifier in this file — the slug FK join is deferred to the Enriched layer via `dim_driver_mapping`.

---

### 2.14 pit_stops

| PK Column | PySpark Type | Notes |
|-----------|-------------|-------|
| `Round` | IntegerType | Injected at ingest. |
| `Driver` | StringType | 3-letter uppercase abbreviation; same format as `lap_times.Driver`. |
| `Lap Number` | IntegerType | Lap on which the pit stop occurred (stored as DoubleType in source). |

Rationale: Each pit stop is uniquely identified by round, driver, and the lap on which it occurred. A driver can only pit once per lap. The composite `(Round, Driver, Lap Number)` matches the `lap_times` PK, enabling direct joins.

---

## 3. Foreign Key Relationships

The following table documents all FK relationships between entities. All joins are performed in the Enriched or IDS layer (Raw layer is append-only exact copy with no joins).

| Source Entity | Source Column | Target Entity | Target Column | Join Type | Notes |
|--------------|--------------|--------------|--------------|-----------|-------|
| race_results | `DriverId` | drivers_data | `Driver ID` | INNER | Slug-to-slug; direct join |
| race_results | `TeamName` | constructors_data | `Team Name` | LEFT | String match; fragile until `TeamId` added to `constructors_data` |
| race_results | `TeamId` | constructors_data | `team_id` (to be added) | INNER | Preferred FK once `constructors_data` enriched with slug |
| race_results | `Round` | races_data | `Round` | INNER | Round is injected at ingest; validates event exists |
| sprint_results | `DriverId` | drivers_data | `Driver ID` | INNER | Same as `race_results` |
| sprint_results | `TeamName` | constructors_data | `Team Name` | LEFT | Same as `race_results` |
| sprint_results | `Round` | races_data | `Round` | INNER | Round injected at ingest |
| qualifying_results | `Driver ID` | drivers_data | `Driver ID` | LEFT | BOR's value is sentinel `"nan"` — LEFT JOIN to avoid losing BOR row; fix sentinel in Enriched before promoting to INNER |
| qualifying_results | `Abbreviation` | drivers_data | `Abbreviation` | LEFT | Secondary join path while BOR `Driver ID` is broken |
| qualifying_results | `Round` | races_data | `Round` | INNER | Round injected at ingest |
| driver_standings | `Driver ID` | drivers_data | `Driver ID` | INNER | Slug-to-slug; direct join |
| driver_standings | `Round` | races_data | `Round` | INNER | Round injected at ingest |
| status_data | `Abbreviation` | drivers_data | `Abbreviation` | INNER | Abbreviation-to-abbreviation; no `Driver ID` in `status_data` |
| status_data | `Round` | races_data | `Round` | INNER | Round injected at ingest |
| constructor_results | `Driver ID` | drivers_data | `Driver ID` | INNER | Slug-to-slug |
| constructor_results | `Team` | constructors_data | `Team Name` | LEFT | String match; fragile |
| constructor_results | `Round` | races_data | `Round` | INNER | Round injected at ingest |
| constructor_standings | `Driver ID` | drivers_data | `Driver ID` | — | Relationship deferred pending redesign of `constructor_standings` |
| constructor_standings | `Round` | races_data | `Round` | — | Round injected at ingest; relationship deferred |
| lap_times | `Driver` (3-letter) | dim_driver_mapping | `abbreviation` | INNER | Required intermediate lookup — see Section 4 |
| lap_times | `Round` | races_data | `Round` | INNER | Round injected at ingest |
| pit_stops | `Driver` (3-letter) | dim_driver_mapping | `abbreviation` | INNER | Required intermediate lookup — see Section 4 |
| pit_stops | `(Driver, Lap Number)` | lap_times | `(Driver, Lap Number)` | LEFT | Enriches pit stop rows with lap context; same round scope |
| pit_stops | `Round` | races_data | `Round` | INNER | Round injected at ingest |
| circuit_info | `event` | races_data | `Official Event Name` | LEFT | String match; non-ASCII characters present — UTF-8 collation required |

---

## 4. Driver Identifier Mapping

### The Identifier Split Problem

Two distinct driver identifier formats coexist across the 13 source entities:

| Format | Example | Used In |
|--------|---------|---------|
| **Slug** (`Driver ID`) | `norris`, `max_verstappen` | `drivers_data`, `race_results`, `sprint_results`, `qualifying_results`, `driver_standings`, `constructor_results`, `constructor_standings` |
| **3-letter abbreviation** (`Driver` / `Abbreviation`) | `NOR`, `VER` | `lap_times`, `pit_stops`, `status_data`, `qualifying_results` |

`qualifying_results` contains both formats (columns `Driver ID` and `Abbreviation`), making it the natural bridge.

`drivers_data` is the authoritative dimension table and contains **both** `Driver ID` (slug) and `Abbreviation` (3-letter code) for all 20 drivers. This makes it the source of truth for the mapping.

### dim_driver_mapping Design

A lookup table `dim_driver_mapping` must be derived from `drivers_data` in the Enriched or IDS layer. This table enables `lap_times` and `pit_stops` to join into the broader platform.

**Proposed schema** (Raw layer: `f1_platform.raw.dim_driver_mapping`):

| Column | PySpark Type | Nullable | Source | Description |
|--------|-------------|----------|--------|-------------|
| `driver_id` | StringType | No | `drivers_data.Driver ID` | Canonical slug identifier — FK target for all slug-based entities |
| `abbreviation` | StringType | No | `drivers_data.Abbreviation` | 3-letter uppercase code — FK target for `lap_times.Driver`, `pit_stops.Driver`, `status_data.Abbreviation` |
| `driver_number` | IntegerType | No | `drivers_data.Number` | Car number — tertiary identifier; included for convenience |
| `full_name` | StringType | No | `drivers_data.Full Name` | Human-readable name for display |
| `team_name` | StringType | No | `drivers_data.Team` | Current team assignment |
| `ingestion_date` | TimestampType | No | Pipeline-injected | UTC timestamp of pipeline run that created this row |
| `source_file` | StringType | No | Pipeline-injected | Source filename (`drivers_data.csv`) |

**Usage pattern**:
```
lap_times  → JOIN dim_driver_mapping ON lap_times.Driver = dim_driver_mapping.abbreviation
pit_stops  → JOIN dim_driver_mapping ON pit_stops.Driver = dim_driver_mapping.abbreviation
status_data → JOIN dim_driver_mapping ON status_data.Abbreviation = dim_driver_mapping.abbreviation
```

After the join, `dim_driver_mapping.driver_id` becomes the canonical FK for all downstream IDS and Curated layer joins.

### BOR Sentinel Fix

BOR (`bortoleto`)'s `Driver ID` in `qualifying_results` is the string literal `"nan"` (not SQL NULL). The Enriched layer must:
1. Run a global sentinel-replacement step replacing `"nan"`, `"None"`, `"NaT"` with SQL NULL across all columns.
2. After replacement, BOR's `driver_id` becomes NULL.
3. Apply a targeted fix: `WHEN driver_id IS NULL AND abbreviation = 'BOR' THEN 'bortoleto'`.

---

## 5. Unity Catalog Naming Convention

All Delta tables are registered in Unity Catalog following the three-part `catalog.schema.table` naming convention. Snake_case is used for all identifiers.

### Catalog Structure

| Catalog | Purpose |
|---------|---------|
| `f1_platform` | Production catalog — all governed layers |
| `f1_platform_test` | Test catalog — isolated test runs; mirrors production schema structure |

### Schema (Layer) Structure

| Layer | Schema | Purpose |
|-------|--------|---------|
| Raw | `raw` | Exact copies of source CSVs; append-only; no transformation |
| Enriched | `enriched` | Cleansed, typed, deduped; incremental merge (upsert) |
| IDS | `ids` | Integrated Data Store; entity joins; MDM decisions applied |
| Curated | `curated` | Star schema; Power BI-ready fact and dimension tables |
| Quarantine | `quarantine` | Rejected rows per entity; one table per entity; append-only |

### Table Naming Pattern

**Pattern**: `{catalog}.{schema}.{entity_name}`

All table names use snake_case, match the entity name from the Data Mapping, and do not include layer prefix in the table name (the schema provides layer context).

### Raw Layer — `f1_platform.raw.*`

| Table | Full Unity Catalog Path | Source File |
|-------|------------------------|-------------|
| races_data | `f1_platform.raw.races_data` | `datasource/races_data.csv` |
| race_results | `f1_platform.raw.race_results` | `datasource/race_results.csv` |
| sprint_results | `f1_platform.raw.sprint_results` | `datasource/sprint_results.csv` |
| circuit_info | `f1_platform.raw.circuit_info` | `datasource/circuit_info.json` |
| drivers_data | `f1_platform.raw.drivers_data` | `datasource/drivers_data.csv` |
| driver_standings | `f1_platform.raw.driver_standings` | `datasource/driver_standings.csv` |
| qualifying_results | `f1_platform.raw.qualifying_results` | `datasource/qualifying_results.csv` |
| status_data | `f1_platform.raw.status_data` | `datasource/status_data.csv` |
| constructors_data | `f1_platform.raw.constructors_data` | `datasource/constructors_data.csv` |
| constructor_results | `f1_platform.raw.constructor_results` | `datasource/constructor_results.csv` |
| lap_times | `f1_platform.raw.lap_times` | `datasource/lap_times.csv` |
| pit_stops | `f1_platform.raw.pit_stops` | `datasource/pit_stops.csv` |
| *(season_data)* | *(retired — not ingested to Raw)* | `datasource/season_data.csv` |
| *(constructor_standings)* | *(not ingested until redesigned)* | `datasource/constructor_standings.csv` |

### Enriched Layer — `f1_platform.enriched.*`

| Table | Full Unity Catalog Path | Notes |
|-------|------------------------|-------|
| races_data | `f1_platform.enriched.races_data` | Typed, UTC-normalised datetimes |
| race_results | `f1_platform.enriched.race_results` | Sentinel fix, float→int casts, null `CountryCode` noted |
| sprint_results | `f1_platform.enriched.sprint_results` | Same as `race_results`; Ocon timedelta fix |
| circuit_info | `f1_platform.enriched.circuit_info` | Flattened sessions; UTC-normalised datetimes |
| drivers_data | `f1_platform.enriched.drivers_data` | De-duplicated; canonical driver dimension |
| driver_standings | `f1_platform.enriched.driver_standings` | Float→int casts; `Round` validated |
| qualifying_results | `f1_platform.enriched.qualifying_results` | BOR sentinel fix; `Driver ID` corrected |
| status_data | `f1_platform.enriched.status_data` | Status enum validation |
| constructors_data | `f1_platform.enriched.constructors_data` | Enriched with `TeamId` and `TeamColor` from `race_results` |
| constructor_results | `f1_platform.enriched.constructor_results` | Authoritative per-race constructor data |
| lap_times | `f1_platform.enriched.lap_times` | Float→int casts; timedelta parsed |
| pit_stops | `f1_platform.enriched.pit_stops` | `Pit In Time` null noted as collection failure |
| dim_driver_mapping | `f1_platform.enriched.dim_driver_mapping` | Derived from `drivers_data`; slug↔abbreviation lookup |

### IDS Layer — `f1_platform.ids.*`

| Table | Full Unity Catalog Path | Notes |
|-------|------------------------|-------|
| fact_race_results | `f1_platform.ids.fact_race_results` | `race_results` + `sprint_results` unified with `session_type` |
| fact_lap_times | `f1_platform.ids.fact_lap_times` | `lap_times` with slug FK resolved via `dim_driver_mapping` |
| fact_pit_stops | `f1_platform.ids.fact_pit_stops` | `pit_stops` with slug FK resolved |
| fact_qualifying | `f1_platform.ids.fact_qualifying` | `qualifying_results` with BOR fix applied |
| fact_standings | `f1_platform.ids.fact_standings` | `driver_standings` + future `constructor_standings` unified |
| dim_season_calendar | `f1_platform.ids.dim_season_calendar` | Sourced from `races_data` (superset; `season_data` retired) |
| dim_driver | `f1_platform.ids.dim_driver` | Canonical `drivers_data` dimension |
| dim_constructor | `f1_platform.ids.dim_constructor` | Enriched `constructors_data` |
| dim_circuit | `f1_platform.ids.dim_circuit` | From flattened `circuit_info` |

### Curated Layer — `f1_platform.curated.*`

| Table | Full Unity Catalog Path | Notes |
|-------|------------------------|-------|
| fact_race_results | `f1_platform.curated.fact_race_results` | Power BI-optimised; all FKs resolved to surrogate keys |
| fact_lap_times | `f1_platform.curated.fact_lap_times` | Aggregated lap metrics |
| fact_pit_stops | `f1_platform.curated.fact_pit_stops` | Pit stop analysis table |
| fact_qualifying | `f1_platform.curated.fact_qualifying` | Qualifying performance |
| fact_championship_standings | `f1_platform.curated.fact_championship_standings` | Driver and constructor standings unified |
| dim_driver | `f1_platform.curated.dim_driver` | SCD-ready driver dimension |
| dim_constructor | `f1_platform.curated.dim_constructor` | Constructor dimension |
| dim_circuit | `f1_platform.curated.dim_circuit` | Circuit dimension |
| dim_season_calendar | `f1_platform.curated.dim_season_calendar` | Season/round calendar |

### Quarantine Layer — `f1_platform.quarantine.*`

| Table | Full Unity Catalog Path |
|-------|------------------------|
| race_results | `f1_platform.quarantine.race_results` |
| sprint_results | `f1_platform.quarantine.sprint_results` |
| circuit_info | `f1_platform.quarantine.circuit_info` |
| drivers_data | `f1_platform.quarantine.drivers_data` |
| driver_standings | `f1_platform.quarantine.driver_standings` |
| qualifying_results | `f1_platform.quarantine.qualifying_results` |
| status_data | `f1_platform.quarantine.status_data` |
| constructors_data | `f1_platform.quarantine.constructors_data` |
| constructor_results | `f1_platform.quarantine.constructor_results` |
| lap_times | `f1_platform.quarantine.lap_times` |
| pit_stops | `f1_platform.quarantine.pit_stops` |

All quarantine tables include: `rejection_reason` (StringType), `source_file` (StringType), `ingestion_date` (TimestampType), plus all columns from the rejected row.

### Test Catalog — `f1_platform_test.*`

Mirrors the production schema exactly. Pattern: `f1_platform_test.{layer}.{entity}`.

Examples:
- `f1_platform_test.raw.race_results`
- `f1_platform_test.enriched.qualifying_results`
- `f1_platform_test.curated.fact_race_results`

---

## 6. Consolidation and Retirement Decisions

| Decision | File(s) Affected | Action | Authoritative Source | Rationale |
|----------|-----------------|--------|---------------------|-----------|
| **RETIRE `season_data`** | `datasource/season_data.csv` | Do not ingest to Raw; file is archived. `dim_season_calendar` sourced from `races_data`. | `races_data.csv` | `season_data` is a strict column subset of `races_data` (missing only `Official Event Name`). All rows are identical. Maintaining two tables with overlapping content creates reconciliation risk. |
| **DESIGNATE `constructor_results` as authoritative for per-race driver results** | `datasource/constructor_results.csv`, `datasource/constructor_standings.csv` | `constructor_results` is ingested to Raw as-is. `constructor_standings` is **not ingested** until it is re-extracted with semantically distinct data. | `constructor_results.csv` | Both files are byte-for-byte identical (DQ-04). `constructor_results` is designated the per-race driver data source. `constructor_standings` must be redesigned to carry constructor-aggregated standings (Points, Position per constructor per round) rather than driver-level race rows. Until re-extraction is complete, `constructor_standings` is not loaded to avoid duplicate data. |
| **`constructors_data` enrichment** | `datasource/constructors_data.csv` | Ingest as-is to Raw; in Enriched layer join with `race_results.TeamId` and `race_results.TeamColor` to add `team_id` and `team_color` columns. | `constructors_data.csv` + `race_results.csv` | `constructors_data` is a single-column table (`Team Name` only) with no surrogate key, making FK joins fragile. The `TeamId` slug and `TeamColor` hex from `race_results` are the correct enrichment fields. |
| **`dim_driver_mapping` creation** | Derived from `drivers_data.csv` | Create a pipeline-derived lookup table `f1_platform.enriched.dim_driver_mapping` from `drivers_data`. Not a separate source file. | `drivers_data.csv` | The slug vs. 3-letter abbreviation split across 7+ entities requires an explicit bridge table before any cross-entity join involving `lap_times` or `pit_stops` can execute. |
