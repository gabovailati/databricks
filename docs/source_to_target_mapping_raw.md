# Source-to-Target Mapping — Raw (Bronze) Layer
**Issue**: #2 — Data Architecture Foundation
**Date**: 2026-05-06
**Status**: Gate 2 Artifact — Awaiting Approval
**Prepared by**: Data Architect Agent

---

## Overview

This document is a field-level skeleton mapping every CSV/JSON source column to its corresponding Raw Delta table column. No transformation logic is applied in the Raw layer — it is an exact copy of the source with three pipeline-injected metadata columns added.

### Raw Layer Principles

- **Append-only**: Raw tables are never updated or deleted. Every pipeline run appends rows.
- **No transformation**: All source column values are written exactly as read from the CSV/JSON file.
- **Schema preservation**: Source type inconsistencies (float64 integers, pandas timedelta strings, sentinel strings) are preserved in Raw. Casts and cleansing are the responsibility of the Enriched layer.
- **Injected columns**: Three metadata columns are added by the pipeline to every Raw table. They are not present in the source files.
- **Pass-through columns**: Some entirely-null columns (e.g. `CountryCode`, `Q1/Q2/Q3`) are written to Raw as-is. They are noted as candidates for dropping in Enriched — they are **never dropped in Raw**.

### Injected Columns (all entities)

| Target Column | Target Type | Source | Description |
|--------------|-------------|--------|-------------|
| `ingestion_date` | TimestampType | Pipeline | UTC timestamp of pipeline run that wrote this row. `NOT NULL`. |
| `source_file` | StringType | Pipeline | Source filename (e.g. `"race_results.csv"`). `NOT NULL`. |
| `round_number` | IntegerType | Pipeline job parameter | Round number for the event being ingested. **Only for result/standings files** — see per-entity tables. `NOT NULL` where applicable. |

---

## Table of Contents

1. [races_data](#1-races_data)
2. [season_data](#2-season_data-retired--not-ingested)
3. [race_results](#3-race_results)
4. [sprint_results](#4-sprint_results)
5. [circuit_info](#5-circuit_info)
6. [drivers_data](#6-drivers_data)
7. [driver_standings](#7-driver_standings)
8. [qualifying_results](#8-qualifying_results)
9. [status_data](#9-status_data)
10. [constructors_data](#10-constructors_data)
11. [constructor_standings](#11-constructor_standings-not-ingested--pending-redesign)
12. [constructor_results](#12-constructor_results)
13. [lap_times](#13-lap_times)
14. [pit_stops](#14-pit_stops)

---

## 1. races_data

**Source file**: `datasource/races_data.csv`
**Target table**: `f1_platform.raw.races_data`
**Ingestion mode**: Append
**Round injection**: No (races_data is the round reference — not a result file)

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Round` | LongType | `Round` | LongType | None — exact copy |
| `Event Name` | StringType | `Event Name` | StringType | None — exact copy |
| `Official Event Name` | StringType | `Official Event Name` | StringType | None — exact copy. UTF-8 encoding enforced at read time. |
| `Location` | StringType | `Location` | StringType | None — exact copy |
| `Country` | StringType | `Country` | StringType | None — exact copy |
| `First Session` | StringType | `First Session` | StringType | None — exact copy. Tz-aware string preserved as-is. Cast to TimestampType UTC in Enriched. |
| `Last Session` | StringType | `Last Session` | StringType | None — exact copy. 1 NULL (Round 0) preserved. |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"races_data.csv"` |

---

## 2. season_data — RETIRED / NOT INGESTED

**Source file**: `datasource/season_data.csv`
**Target table**: *None — file retired*

> `season_data` is a strict column subset of `races_data`. It is not ingested to Raw or any downstream layer. `dim_season_calendar` is sourced from `races_data`. See Consolidation and Retirement Decisions in `data_mapping.md`.

---

## 3. race_results

**Source file**: `datasource/race_results.csv`
**Target table**: `f1_platform.raw.race_results`
**Ingestion mode**: Append
**Round injection**: Yes — `round_number` is a required pipeline job parameter

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `DriverNumber` | LongType | `DriverNumber` | LongType | None — exact copy |
| `BroadcastName` | StringType | `BroadcastName` | StringType | None — exact copy |
| `Abbreviation` | StringType | `Abbreviation` | StringType | None — exact copy |
| `DriverId` | StringType | `DriverId` | StringType | None — exact copy |
| `TeamName` | StringType | `TeamName` | StringType | None — exact copy |
| `TeamColor` | StringType | `TeamColor` | StringType | None — exact copy |
| `TeamId` | StringType | `TeamId` | StringType | None — exact copy |
| `FirstName` | StringType | `FirstName` | StringType | None — exact copy |
| `LastName` | StringType | `LastName` | StringType | None — exact copy |
| `FullName` | StringType | `FullName` | StringType | None — exact copy. Pass-through; will be dropped in Enriched (DQ-17). |
| `HeadshotUrl` | StringType | `HeadshotUrl` | StringType | None — exact copy. Colapinto sentinel `"None"` preserved as-is; replaced with SQL NULL in Enriched sentinel step. |
| `CountryCode` | StringType | `CountryCode` | StringType | None — exact copy. **100% NULL in source** (DQ-01). Pass-through; will be dropped in Enriched until source extraction is fixed. |
| `Position` | DoubleType | `Position` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched (DQ-12). |
| `ClassifiedPosition` | StringType | `ClassifiedPosition` | StringType | None — exact copy. Mixed type preserved; split in Enriched (DQ-14). |
| `GridPosition` | DoubleType | `GridPosition` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. |
| `Q1` | StringType | `Q1` | StringType | None — exact copy. **100% NULL in source** (DQ-02). Pass-through; will be dropped in Enriched. Source qualifying times from `qualifying_results`. |
| `Q2` | StringType | `Q2` | StringType | None — exact copy. **100% NULL** (DQ-02). Same as Q1. |
| `Q3` | StringType | `Q3` | StringType | None — exact copy. **100% NULL** (DQ-02). Same as Q1. |
| `Time` | StringType | `Time` | StringType | None — exact copy. Pandas timedelta string preserved; UDF applied in Enriched (DQ-09). 3 NULLs (retired drivers) preserved. |
| `Status` | StringType | `Status` | StringType | None — exact copy |
| `Points` | DoubleType | `Points` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. |
| `Laps` | DoubleType | `Laps` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter value for this event's round number |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"race_results.csv"` |

---

## 4. sprint_results

**Source file**: `datasource/sprint_results.csv`
**Target table**: `f1_platform.raw.sprint_results`
**Ingestion mode**: Append
**Round injection**: Yes — `round_number` is a required pipeline job parameter

> Schema is identical to `race_results`. All transformation notes from Section 3 apply. Only noteworthy differences are documented below.

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `DriverNumber` | LongType | `DriverNumber` | LongType | None — exact copy |
| `BroadcastName` | StringType | `BroadcastName` | StringType | None — exact copy |
| `Abbreviation` | StringType | `Abbreviation` | StringType | None — exact copy |
| `DriverId` | StringType | `DriverId` | StringType | None — exact copy |
| `TeamName` | StringType | `TeamName` | StringType | None — exact copy |
| `TeamColor` | StringType | `TeamColor` | StringType | None — exact copy |
| `TeamId` | StringType | `TeamId` | StringType | None — exact copy |
| `FirstName` | StringType | `FirstName` | StringType | None — exact copy |
| `LastName` | StringType | `LastName` | StringType | None — exact copy |
| `FullName` | StringType | `FullName` | StringType | None — exact copy. Pass-through; will be dropped in Enriched. |
| `HeadshotUrl` | StringType | `HeadshotUrl` | StringType | None — exact copy. Colapinto `"None"` sentinel preserved; replaced in Enriched. |
| `CountryCode` | StringType | `CountryCode` | StringType | None — exact copy. **100% NULL** (DQ-01). Pass-through; will be dropped in Enriched. |
| `Position` | DoubleType | `Position` | DoubleType | None — exact copy. float64; cast in Enriched. |
| `ClassifiedPosition` | StringType | `ClassifiedPosition` | StringType | None — exact copy. Mixed type; split in Enriched. |
| `GridPosition` | DoubleType | `GridPosition` | DoubleType | None — exact copy. float64; cast in Enriched. |
| `Q1` | StringType | `Q1` | StringType | None — exact copy. **100% NULL** (DQ-02). Pass-through; will be dropped in Enriched. |
| `Q2` | StringType | `Q2` | StringType | None — exact copy. **100% NULL** (DQ-02). |
| `Q3` | StringType | `Q3` | StringType | None — exact copy. **100% NULL** (DQ-02). |
| `Time` | StringType | `Time` | StringType | None — exact copy. Pandas timedelta string preserved. **Note**: Ocon's value is `"0 days 00:00:31"` (no microseconds) — UDF in Enriched must handle both formats (DQ-09, DQ-15). |
| `Status` | StringType | `Status` | StringType | None — exact copy |
| `Points` | DoubleType | `Points` | DoubleType | None — exact copy. float64; cast in Enriched. |
| `Laps` | DoubleType | `Laps` | DoubleType | None — exact copy. float64; cast in Enriched. |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"sprint_results.csv"` |

---

## 5. circuit_info

**Source file**: `datasource/circuit_info.json`
**Target table**: `f1_platform.raw.circuit_info`
**Ingestion mode**: Append
**Round injection**: No (circuit info is reference data; `event` field joins to `races_data`)

> The source is a nested JSON document. The Raw ingestion notebook must flatten the `sessions` nested object to produce one row per session. The flattening transforms `sessions.Session1` through `sessions.Session5` into individual rows.

| Source Field (JSON path) | Source Type | Target Column | Target Type | Transformation |
|--------------------------|-------------|--------------|-------------|----------------|
| `name` | StringType | `name` | StringType | None — exact copy. Non-ASCII characters preserved; UTF-8 enforced at read. |
| `country` | StringType | `country` | StringType | None — exact copy |
| `event` | StringType | `event` | StringType | None — exact copy. Non-ASCII characters preserved. FK to `races_data.Official Event Name`. |
| `format` | StringType | `format` | StringType | None — exact copy. Enum value (e.g. `"sprint_qualifying"`). |
| `event_date` | StringType | `event_date` | StringType | None — exact copy. Naive datetime string preserved; cast to DateType in Enriched. |
| `sessions.SessionN.name` | StringType | `session_name` | StringType | **Flatten**: extract from nested sessions key. One row per session. Forms part of composite PK. |
| `sessions.SessionN.date` | StringType | `session_date` | StringType | **Flatten**: extract from nested sessions key. Tz-aware local time preserved as string; cast to TimestampType UTC in Enriched. |
| `sessions.SessionN.utc` | StringType | `session_utc` | StringType | **Flatten**: extract from nested sessions key. Naive UTC string preserved; `+00:00` suffix appended before cast in Enriched. |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"circuit_info.json"` |

**Flatten logic note**: The pipeline must iterate over all `SessionN` keys (Session1 through Session5) and emit one output row per key. If a future JSON document has fewer than 5 sessions, NULL values should not be emitted for missing sessions — only present sessions become rows.

---

## 6. drivers_data

**Source file**: `datasource/drivers_data.csv`
**Target table**: `f1_platform.raw.drivers_data`
**Ingestion mode**: Append
**Round injection**: No (dimension data; not event-scoped)

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Number` | LongType | `Number` | LongType | None — exact copy |
| `Driver ID` | StringType | `Driver ID` | StringType | None — exact copy. Canonical slug PK. |
| `Abbreviation` | StringType | `Abbreviation` | StringType | None — exact copy. 3-letter uppercase code. |
| `First Name` | StringType | `First Name` | StringType | None — exact copy |
| `Last Name` | StringType | `Last Name` | StringType | None — exact copy |
| `Full Name` | StringType | `Full Name` | StringType | None — exact copy. Pass-through; retained in `dim_driver` dimension; dropped from Silver fact tables (DQ-17). |
| `Team` | StringType | `Team` | StringType | None — exact copy |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"drivers_data.csv"` |

---

## 7. driver_standings

**Source file**: `datasource/driver_standings.csv`
**Target table**: `f1_platform.raw.driver_standings`
**Ingestion mode**: Append
**Round injection**: Yes — `round_number` is a required pipeline job parameter

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Driver ID` | StringType | `Driver ID` | StringType | None — exact copy. FK to `drivers_data.Driver ID`. |
| `Abbreviation` | StringType | `Abbreviation` | StringType | None — exact copy |
| `Full Name` | StringType | `Full Name` | StringType | None — exact copy. Pass-through; will be dropped in Enriched fact tables (DQ-17). |
| `Team` | StringType | `Team` | StringType | None — exact copy |
| `Points` | DoubleType | `Points` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. |
| `Position` | DoubleType | `Position` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter; anchors standings snapshot to a round |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"driver_standings.csv"` |

---

## 8. qualifying_results

**Source file**: `datasource/qualifying_results.csv`
**Target table**: `f1_platform.raw.qualifying_results`
**Ingestion mode**: Append
**Round injection**: Yes — `round_number` is a required pipeline job parameter

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Abbreviation` | StringType | `Abbreviation` | StringType | None — exact copy. PK component (used over `Driver ID` because BOR's `Driver ID` is sentinel `"nan"`). |
| `Driver ID` | StringType | `Driver ID` | StringType | None — exact copy. **BOR's value is the string literal `"nan"`** — written to Raw as-is. Sentinel detection and correction applied in Enriched (DQ-06). |
| `Full Name` | StringType | `Full Name` | StringType | None — exact copy. Pass-through; will be dropped in Enriched fact tables. |
| `Team` | StringType | `Team` | StringType | None — exact copy |
| `Q1 Time` | StringType | `Q1 Time` | StringType | None — exact copy. Pandas timedelta string preserved. 1 NULL (BOR) preserved. |
| `Q2 Time` | StringType | `Q2 Time` | StringType | None — exact copy. 6 structural NULLs (Q1-eliminated + BOR) preserved as meaningful NULLs. |
| `Q3 Time` | StringType | `Q3 Time` | StringType | None — exact copy. 11 structural NULLs (non-top-10 drivers) preserved as meaningful NULLs. |
| `Position` | DoubleType | `Position` | DoubleType | None — exact copy. float64 preserved; 1 NULL (BOR) preserved; cast to IntegerType in Enriched. |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"qualifying_results.csv"` |

---

## 9. status_data

**Source file**: `datasource/status_data.csv`
**Target table**: `f1_platform.raw.status_data`
**Ingestion mode**: Append
**Round injection**: Yes — `round_number` is a required pipeline job parameter

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Abbreviation` | StringType | `Abbreviation` | StringType | None — exact copy. PK component. |
| `Full Name` | StringType | `Full Name` | StringType | None — exact copy. Pass-through; will be dropped in Enriched fact tables (DQ-17). |
| `Status` | StringType | `Status` | StringType | None — exact copy. Sample values: `"Finished"`, `"Retired"`. Additional enum values expected from broader data. |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"status_data.csv"` |

---

## 10. constructors_data

**Source file**: `datasource/constructors_data.csv`
**Target table**: `f1_platform.raw.constructors_data`
**Ingestion mode**: Append
**Round injection**: No (dimension data; not event-scoped)

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Team Name` | StringType | `Team Name` | StringType | None — exact copy. Only column in source. PK (fragile string key). |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"constructors_data.csv"` |

**Note**: `team_id` (slug) and `team_color` columns will be added in the Enriched layer by joining with `race_results.TeamId` and `race_results.TeamColor`. They do not exist in the Raw layer.

---

## 11. constructor_standings — NOT INGESTED / PENDING REDESIGN

**Source file**: `datasource/constructor_standings.csv`
**Target table**: *None — not ingested until redesigned*

> This file is byte-for-byte identical to `constructor_results.csv` (DQ-04). It will not be ingested to Raw until it is re-extracted with semantically distinct content (constructor-aggregated standings: Points and Position per constructor per round). Once redesigned, the mapping will be:

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| *(TBD after re-extraction)* | — | — | — | Mapping to be defined after source re-extraction |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"constructor_standings.csv"` |

---

## 12. constructor_results

**Source file**: `datasource/constructor_results.csv`
**Target table**: `f1_platform.raw.constructor_results`
**Ingestion mode**: Append
**Round injection**: Yes — `round_number` is a required pipeline job parameter

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Driver ID` | StringType | `Driver ID` | StringType | None — exact copy. FK to `drivers_data.Driver ID`. PK component. |
| `Team` | StringType | `Team` | StringType | None — exact copy. FK to `constructors_data.Team Name`. |
| `Full Name` | StringType | `Full Name` | StringType | None — exact copy. Pass-through; will be dropped in Enriched fact tables (DQ-17). |
| `Position` | DoubleType | `Position` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. |
| `Points` | DoubleType | `Points` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. |
| `Status` | StringType | `Status` | StringType | None — exact copy. Values: `"Finished"` or `"Retired"`. |
| `Time` | StringType | `Time` | StringType | None — exact copy. Pandas timedelta string preserved; 3 NULLs (retired drivers) preserved; UDF applied in Enriched (DQ-09). |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"constructor_results.csv"` |

---

## 13. lap_times

**Source file**: `datasource/lap_times.csv`
**Target table**: `f1_platform.raw.lap_times`
**Ingestion mode**: Append
**Round injection**: Yes — `round_number` is a required pipeline job parameter

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Driver` | StringType | `Driver` | StringType | None — exact copy. 3-letter abbreviation (not slug). FK to `dim_driver_mapping.abbreviation` in Enriched. PK component. |
| `Lap Number` | DoubleType | `Lap Number` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. PK component. |
| `Lap Time` | StringType | `Lap Time` | StringType | None — exact copy. Pandas timedelta string preserved; UDF applied in Enriched (DQ-09). |
| `Position` | DoubleType | `Position` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. |
| `Time` | StringType | `Time` | StringType | None — exact copy. Cumulative elapsed race time. Pandas timedelta string preserved; UDF applied in Enriched. |
| `Sector 1` | StringType | `Sector 1` | StringType | None — exact copy. ~20 structural NULLs (lap 1 for all drivers) preserved as meaningful NULLs (DQ-18). |
| `Sector 2` | StringType | `Sector 2` | StringType | None — exact copy. 0 NULLs. Pandas timedelta string. |
| `Sector 3` | StringType | `Sector 3` | StringType | None — exact copy. 0 NULLs. Pandas timedelta string. |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter. PK component. |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"lap_times.csv"` |

---

## 14. pit_stops

**Source file**: `datasource/pit_stops.csv`
**Target table**: `f1_platform.raw.pit_stops`
**Ingestion mode**: Append
**Round injection**: Yes — `round_number` is a required pipeline job parameter

| Source Column | Source Type | Target Column | Target Type | Transformation |
|--------------|-------------|--------------|-------------|----------------|
| `Driver` | StringType | `Driver` | StringType | None — exact copy. 3-letter abbreviation. FK to `dim_driver_mapping.abbreviation` in Enriched. PK component. |
| `Lap Number` | DoubleType | `Lap Number` | DoubleType | None — exact copy. float64 preserved; cast to IntegerType in Enriched. PK component. |
| `Pit Out Time` | StringType | `Pit Out Time` | StringType | None — exact copy. Cumulative race elapsed time when driver exited pit lane. Pandas timedelta string. |
| `Pit In Time` | StringType | `Pit In Time` | StringType | None — exact copy. **100% NULL** — data collection failure (DQ-03). Pass-through in Raw. Pit stop duration analytics blocked until source is fixed. Will remain as documented unusable column in Enriched until resolved. |
| *(not in source)* | — | `round_number` | IntegerType | **Injected by pipeline** — job parameter. PK component. |
| *(not in source)* | — | `ingestion_date` | TimestampType | **Injected by pipeline** — UTC ingest timestamp |
| *(not in source)* | — | `source_file` | StringType | **Injected by pipeline** — `"pit_stops.csv"` |

---

## Summary — Round Injection Matrix

The following table summarises which entities require `round_number` injection and which do not.

| Entity | Round Injection Required | Reason |
|--------|--------------------------|--------|
| `races_data` | No | This IS the round reference table |
| `season_data` | No (retired) | Not ingested |
| `race_results` | **Yes** | Result file — no `Round` column in CSV (DQ-05) |
| `sprint_results` | **Yes** | Result file — no `Round` column in CSV (DQ-05) |
| `circuit_info` | No | Reference data; joined via `event` field to `races_data` |
| `drivers_data` | No | Dimension data; not event-scoped |
| `driver_standings` | **Yes** | Standings snapshot — must be anchored to a round (DQ-05) |
| `qualifying_results` | **Yes** | Result file — no `Round` column in CSV (DQ-05) |
| `status_data` | **Yes** | Per-race status — must be anchored to a round (DQ-05) |
| `constructors_data` | No | Dimension data; not event-scoped |
| `constructor_standings` | Not yet (pending redesign) | Not ingested until redesigned |
| `constructor_results` | **Yes** | Result file — no `Round` column in CSV (DQ-05) |
| `lap_times` | **Yes** | Per-lap timing — no `Round` column in CSV (DQ-05) |
| `pit_stops` | **Yes** | Per-stop timing — no `Round` column in CSV (DQ-05) |

**Entities requiring `round_number`**: `race_results`, `sprint_results`, `driver_standings`, `qualifying_results`, `status_data`, `constructor_results`, `lap_times`, `pit_stops` (8 of 14 source files; 8 of 12 active ingested entities).
