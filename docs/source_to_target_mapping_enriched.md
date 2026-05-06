# Source-to-Target Mapping — Enriched Layer
**Issue**: #5 — Enriched Layer Pipeline
**Date**: 2026-05-06
**Status**: Issue #5 Artifact
**Prepared by**: Data Engineer Agent

---

## Overview

This document maps every Raw layer column to its Enriched layer target for all 12 active entities. The Enriched layer applies type casts, sentinel fixes, timedelta parsing, and column drops. Columns sourced by enrichment join are marked INJECTED.

### Enriched Layer Principles

- **Merge (upsert)**: Enriched tables use Delta MERGE. New rows are inserted; existing rows (matched on PK) are updated.
- **Sentinel replacement**: All `StringType` columns have `"nan"`, `"None"`, `"NaT"` replaced with SQL NULL before any DQ check.
- **Quarantine**: Rows failing CRITICAL or HIGH DQ rules are written to `f1_platform.quarantine.<entity>` and excluded from Enriched.
- **Dropped columns**: Columns marked DROPPED are not written to Enriched.
- **Injected columns**: Columns marked INJECTED are added during enrichment and are not present in the Raw source.

### Legend

| Symbol | Meaning |
|--------|---------|
| PASS-THROUGH | Copied exactly from Raw with no transformation |
| CAST | Type cast applied |
| TIMESTAMP_CAST | String cast to TimestampType UTC |
| TIMEDELTA_PARSE | Pandas timedelta string parsed to total seconds (DoubleType) via UDF |
| SPLIT | Source column split into two or more target columns |
| SENTINEL_FIX | Sentinel string replaced with a corrected value (not quarantined) |
| DROPPED | Column is not written to Enriched |
| INJECTED | Column does not exist in source; added by enrichment logic |

---

## Table of Contents

1. [races_data](#1-races_data)
2. [race_results](#2-race_results)
3. [sprint_results](#3-sprint_results)
4. [circuit_info](#4-circuit_info)
5. [drivers_data](#5-drivers_data)
6. [driver_standings](#6-driver_standings)
7. [qualifying_results](#7-qualifying_results)
8. [status_data](#8-status_data)
9. [constructors_data](#9-constructors_data)
10. [constructor_results](#10-constructor_results)
11. [lap_times](#11-lap_times)
12. [pit_stops](#12-pit_stops)

---

## 1. races_data

**Source table**: `f1_platform.raw.races_data`
**Target table**: `f1_platform.enriched.races_data`
**Primary key**: `Round`
**Round injection**: No (races_data is the round reference)

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Round` | LongType | `Round` | LongType | PASS-THROUGH |
| `Event Name` | StringType | `Event Name` | StringType | PASS-THROUGH |
| `Official Event Name` | StringType | `Official Event Name` | StringType | PASS-THROUGH |
| `Location` | StringType | `Location` | StringType | PASS-THROUGH |
| `Country` | StringType | `Country` | StringType | PASS-THROUGH |
| `First Session` | StringType | `first_session` | TimestampType | TIMESTAMP_CAST — tz-aware string cast to UTC TimestampType via `F.to_timestamp()` |
| `Last Session` | StringType | `last_session` | TimestampType | TIMESTAMP_CAST — 1 null (Round 0) preserved as NULL; remaining values cast to UTC |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: RD-01 (NULL_PRIMARY_KEY), RD-02 (ROUND_OUT_OF_RANGE), RD-03 (NULL_EVENT_NAME), RD-04 (NULL_OFFICIAL_EVENT_NAME), RD-05 (INVALID_DATETIME_FORMAT on First Session), RD-06 (DUPLICATE_ROUND), RD-07 (ENCODING_ERROR)

---

## 2. race_results

**Source table**: `f1_platform.raw.race_results`
**Target table**: `f1_platform.enriched.race_results`
**Primary key**: `(round_number, DriverId)`
**Round injection**: Yes — `round_number` widget parameter

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `DriverNumber` | LongType | `DriverNumber` | LongType | PASS-THROUGH |
| `BroadcastName` | StringType | `BroadcastName` | StringType | PASS-THROUGH |
| `Abbreviation` | StringType | `Abbreviation` | StringType | PASS-THROUGH |
| `DriverId` | StringType | `DriverId` | StringType | PASS-THROUGH |
| `TeamName` | StringType | `TeamName` | StringType | PASS-THROUGH |
| `TeamColor` | StringType | `TeamColor` | StringType | PASS-THROUGH |
| `TeamId` | StringType | `TeamId` | StringType | PASS-THROUGH |
| `FirstName` | StringType | `FirstName` | StringType | PASS-THROUGH |
| `LastName` | StringType | `LastName` | StringType | PASS-THROUGH |
| `FullName` | StringType | `FullName` | StringType | PASS-THROUGH |
| `HeadshotUrl` | StringType | `HeadshotUrl` | StringType | Sentinel `"None"` replaced by GLOBAL-01; NULL preserved |
| `CountryCode` | StringType | *(dropped)* | — | DROPPED — 100% null extraction artefact (DQ-01) |
| `Position` | DoubleType | `position` | IntegerType | CAST — float64 → IntegerType |
| `ClassifiedPosition` | StringType | `finish_position` | IntegerType | SPLIT — digit string → IntegerType; NULL when `ClassifiedPosition = 'R'` |
| `ClassifiedPosition` | StringType | `is_classified` | BooleanType | SPLIT — `False` when `ClassifiedPosition = 'R'`; `True` otherwise |
| `GridPosition` | DoubleType | `grid_position` | IntegerType | CAST — float64 → IntegerType |
| `Q1` | StringType | *(dropped)* | — | DROPPED — 100% null extraction artefact (DQ-02) |
| `Q2` | StringType | *(dropped)* | — | DROPPED — 100% null extraction artefact (DQ-02) |
| `Q3` | StringType | *(dropped)* | — | DROPPED — 100% null extraction artefact (DQ-02) |
| `Time` | StringType | `time_seconds` | DoubleType | TIMEDELTA_PARSE — pandas timedelta string → total seconds; NULL preserved for retired drivers |
| `Status` | StringType | `Status` | StringType | PASS-THROUGH |
| `Points` | DoubleType | `points` | IntegerType | CAST — float64 → IntegerType |
| `Laps` | DoubleType | `laps` | IntegerType | CAST — float64 → IntegerType |
| `round_number` | IntegerType | `round_number` | IntegerType | INJECTED — from pipeline widget parameter |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: RR-01 (NULL_PRIMARY_KEY), RR-02 (NULL_DRIVER_NUMBER), RR-03 (NULL_TEAM_ID), RR-04 (DUPLICATE_PRIMARY_KEY), RR-05 (POSITION_OUT_OF_RANGE), RR-06 (GRID_POSITION_OUT_OF_RANGE), RR-07 (NEGATIVE_POINTS), RR-08 (NEGATIVE_LAPS), RR-09 (INVALID_CLASSIFIED_POSITION), RR-10 (INVALID_LAP_TIME_FORMAT), RR-13 (INVALID_TEAM_COLOR_FORMAT)

---

## 3. sprint_results

**Source table**: `f1_platform.raw.sprint_results`
**Target table**: `f1_platform.enriched.sprint_results`
**Primary key**: `(round_number, DriverId)`
**Round injection**: Yes

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `DriverNumber` | LongType | `DriverNumber` | LongType | PASS-THROUGH |
| `BroadcastName` | StringType | `BroadcastName` | StringType | PASS-THROUGH |
| `Abbreviation` | StringType | `Abbreviation` | StringType | PASS-THROUGH |
| `DriverId` | StringType | `DriverId` | StringType | PASS-THROUGH |
| `TeamName` | StringType | `TeamName` | StringType | PASS-THROUGH |
| `TeamColor` | StringType | `TeamColor` | StringType | PASS-THROUGH |
| `TeamId` | StringType | `TeamId` | StringType | PASS-THROUGH |
| `FirstName` | StringType | `FirstName` | StringType | PASS-THROUGH |
| `LastName` | StringType | `LastName` | StringType | PASS-THROUGH |
| `FullName` | StringType | `FullName` | StringType | PASS-THROUGH |
| `HeadshotUrl` | StringType | `HeadshotUrl` | StringType | Sentinel `"None"` replaced by GLOBAL-01; NULL preserved |
| `CountryCode` | StringType | *(dropped)* | — | DROPPED — 100% null extraction artefact (DQ-01) |
| `Position` | DoubleType | `position` | IntegerType | CAST — float64 → IntegerType |
| `ClassifiedPosition` | StringType | `finish_position` | IntegerType | SPLIT — digit string → IntegerType; NULL when `ClassifiedPosition = 'R'` |
| `ClassifiedPosition` | StringType | `is_classified` | BooleanType | SPLIT — `False` when `ClassifiedPosition = 'R'`; `True` otherwise |
| `GridPosition` | DoubleType | `grid_position` | IntegerType | CAST — float64 → IntegerType |
| `Q1` | StringType | *(dropped)* | — | DROPPED — 100% null extraction artefact (DQ-02) |
| `Q2` | StringType | *(dropped)* | — | DROPPED — 100% null extraction artefact (DQ-02) |
| `Q3` | StringType | *(dropped)* | — | DROPPED — 100% null extraction artefact (DQ-02) |
| `Time` | StringType | `time_seconds` | DoubleType | TIMEDELTA_PARSE — handles both `HH:MM:SS.ffffff` and `HH:MM:SS` (Ocon's format) |
| `Status` | StringType | `Status` | StringType | PASS-THROUGH |
| `Points` | DoubleType | `points` | IntegerType | CAST — float64 → IntegerType |
| `Laps` | DoubleType | `laps` | IntegerType | CAST — float64 → IntegerType |
| `round_number` | IntegerType | `round_number` | IntegerType | INJECTED — from pipeline widget parameter |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: SR-01 (NULL_PRIMARY_KEY), SR-02 (NULL_DRIVER_NUMBER), SR-03 (NULL_TEAM_ID), SR-04 (DUPLICATE_PRIMARY_KEY), SR-05 (POSITION_OUT_OF_RANGE), SR-06 (GRID_POSITION_OUT_OF_RANGE), SR-07 (NEGATIVE_POINTS), SR-08 (NEGATIVE_LAPS), SR-09 (INVALID_CLASSIFIED_POSITION), SR-10 (INVALID_LAP_TIME_FORMAT), SR-13 (INVALID_TEAM_COLOR_FORMAT)

---

## 4. circuit_info

**Source table**: `f1_platform.raw.circuit_info`
**Target table**: `f1_platform.enriched.circuit_info`
**Primary key**: `(event, session_name)`
**Round injection**: No

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `name` | StringType | `name` | StringType | PASS-THROUGH |
| `country` | StringType | `country` | StringType | PASS-THROUGH |
| `event` | StringType | `event` | StringType | PASS-THROUGH |
| `format` | StringType | `format` | StringType | PASS-THROUGH |
| `event_date` | StringType | `event_date` | TimestampType | TIMESTAMP_CAST — naive datetime treated as UTC; cast via `F.to_timestamp()` |
| `session_name` | StringType | `session_name` | StringType | PASS-THROUGH |
| `session_date` | StringType | `session_date` | TimestampType | TIMESTAMP_CAST — tz-aware local time; cast to UTC via `F.to_timestamp()` |
| `session_utc` | StringType | `session_utc` | TimestampType | TIMESTAMP_CAST — naive UTC string; `+00:00` appended before cast |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: CI-01 (NULL_PRIMARY_KEY on event), CI-02 (NULL_PRIMARY_KEY on session_name), CI-04 (INVALID_SESSION_NAME), CI-05 (INVALID_DATETIME_FORMAT on session_date), CI-06 (INVALID_DATETIME_FORMAT on session_utc), CI-09 (DUPLICATE_SESSION)

---

## 5. drivers_data

**Source table**: `f1_platform.raw.drivers_data`
**Target table**: `f1_platform.enriched.drivers_data`
**Primary key**: `Driver ID`
**Round injection**: No

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Number` | LongType | `Number` | LongType | PASS-THROUGH |
| `Driver ID` | StringType | `Driver ID` | StringType | PASS-THROUGH |
| `Abbreviation` | StringType | `Abbreviation` | StringType | PASS-THROUGH |
| `First Name` | StringType | `First Name` | StringType | PASS-THROUGH |
| `Last Name` | StringType | `Last Name` | StringType | PASS-THROUGH |
| `Full Name` | StringType | *(dropped)* | — | DROPPED — derivable from `First Name` + `Last Name`; redundant (DQ-17) |
| `Team` | StringType | `Team` | StringType | PASS-THROUGH |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: DR-01 (NULL_PRIMARY_KEY), DR-02 (NULL_ABBREVIATION), DR-03 (NULL_DRIVER_NUMBER), DR-04 (INVALID_DRIVER_ID_FORMAT), DR-05 (INVALID_ABBREVIATION_FORMAT), DR-06 (DRIVER_NUMBER_OUT_OF_RANGE), DR-07 (NULL_DRIVER_NAME on First Name), DR-08 (NULL_DRIVER_NAME on Last Name), DR-09 (NULL_TEAM_REFERENCE), DR-11 (DUPLICATE_PRIMARY_KEY), DR-12 (DUPLICATE_ABBREVIATION)

---

## 6. driver_standings

**Source table**: `f1_platform.raw.driver_standings`
**Target table**: `f1_platform.enriched.driver_standings`
**Primary key**: `(round_number, Driver ID)`
**Round injection**: Yes

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Driver ID` | StringType | `Driver ID` | StringType | PASS-THROUGH |
| `Abbreviation` | StringType | *(dropped)* | — | DROPPED — redundant with `drivers_data.Abbreviation` |
| `Full Name` | StringType | *(dropped)* | — | DROPPED — redundant with `drivers_data.Full Name` (DQ-17) |
| `Team` | StringType | *(dropped)* | — | DROPPED — redundant with `drivers_data.Team` |
| `Points` | DoubleType | `points` | IntegerType | CAST — float64 → IntegerType |
| `Position` | DoubleType | `position` | IntegerType | CAST — float64 → IntegerType |
| `round_number` | IntegerType | `round_number` | IntegerType | INJECTED — from pipeline widget parameter |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: DS-01 (NULL_PRIMARY_KEY), DS-02 (NULL_STANDINGS_POSITION), DS-03 (NULL_STANDINGS_POINTS), DS-04 (POSITION_OUT_OF_RANGE), DS-05 (NEGATIVE_POINTS), DS-07 (DUPLICATE_PRIMARY_KEY)

---

## 7. qualifying_results

**Source table**: `f1_platform.raw.qualifying_results`
**Target table**: `f1_platform.enriched.qualifying_results`
**Primary key**: `(round_number, Abbreviation)`
**Round injection**: Yes

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Abbreviation` | StringType | `Abbreviation` | StringType | PASS-THROUGH |
| `Driver ID` | StringType | `Driver ID` | StringType | SENTINEL_FIX — `"nan"` → NULL via GLOBAL-01; then `NULL` → `'bortoleto'` where `Abbreviation = 'BOR'` (not quarantined — known correction per QR-02) |
| `Full Name` | StringType | *(dropped)* | — | DROPPED — redundant with `drivers_data.Full Name` (DQ-17) |
| `Team` | StringType | *(dropped)* | — | DROPPED — redundant with `drivers_data.Team` |
| `Q1 Time` | StringType | `q1_time_seconds` | DoubleType | TIMEDELTA_PARSE — NULL preserved for BOR (structural expectation) |
| `Q2 Time` | StringType | `q2_time_seconds` | DoubleType | TIMEDELTA_PARSE — NULL preserved for Q1-eliminated drivers (structural expectation) |
| `Q3 Time` | StringType | `q3_time_seconds` | DoubleType | TIMEDELTA_PARSE — NULL preserved for non-top-10 drivers (structural expectation) |
| `Position` | DoubleType | `position` | IntegerType | CAST — float64 → IntegerType; NULL preserved for BOR |
| `round_number` | IntegerType | `round_number` | IntegerType | INJECTED — from pipeline widget parameter |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: QR-01 (NULL_PRIMARY_KEY), QR-05 (POSITION_OUT_OF_RANGE), QR-06 (INVALID_LAP_TIME_FORMAT on Q1), QR-07 (INVALID_LAP_TIME_FORMAT on Q2), QR-08 (INVALID_LAP_TIME_FORMAT on Q3), QR-09 (DUPLICATE_PRIMARY_KEY)

**Special handling**: BOR sentinel fix is applied before DQ checks, not quarantined. See QR-02 note in DQ catalogue.

---

## 8. status_data

**Source table**: `f1_platform.raw.status_data`
**Target table**: `f1_platform.enriched.status_data`
**Primary key**: `(round_number, Abbreviation)`
**Round injection**: Yes

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Abbreviation` | StringType | `Abbreviation` | StringType | PASS-THROUGH |
| `Full Name` | StringType | *(dropped)* | — | DROPPED — redundant with `drivers_data.Full Name` (DQ-17) |
| `Status` | StringType | `Status` | StringType | PASS-THROUGH |
| `round_number` | IntegerType | `round_number` | IntegerType | INJECTED — from pipeline widget parameter |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: ST-01 (NULL_PRIMARY_KEY), ST-02 (NULL_STATUS_VALUE), ST-03 (INVALID_STATUS_VALUE — MEDIUM severity, quarantine with steward review flag), ST-05 (DUPLICATE_PRIMARY_KEY)

---

## 9. constructors_data

**Source table**: `f1_platform.raw.constructors_data`
**Target table**: `f1_platform.enriched.constructors_data`
**Primary key**: `Team Name`
**Round injection**: No

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Team Name` | StringType | `Team Name` | StringType | PASS-THROUGH |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |
| *(not in source)* | — | `team_id` | StringType | INJECTED — joined from `f1_platform.raw.race_results.TeamId` on `TeamName = Team Name` (DQ-10) |
| *(not in source)* | — | `team_color` | StringType | INJECTED — joined from `f1_platform.raw.race_results.TeamColor` on `TeamName = Team Name` (DQ-10) |

**DQ rules applied**: CD-01 (NULL_PRIMARY_KEY), CD-02 (EMPTY_TEAM_NAME), CD-03 (DUPLICATE_PRIMARY_KEY), CD-04 (NULL_TEAM_ID after enrichment), CD-05 (INVALID_TEAM_ID_FORMAT), CD-06 (INVALID_TEAM_COLOR_FORMAT)

**Enrichment join**: `race_results` Raw table is used as the source for `team_id` and `team_color` via `LEFT JOIN` on `Team Name = TeamName`. Rows that do not match are flagged with NULL_TEAM_ID.

---

## 10. constructor_results

**Source table**: `f1_platform.raw.constructor_results`
**Target table**: `f1_platform.enriched.constructor_results`
**Primary key**: `(round_number, Driver ID)`
**Round injection**: Yes

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Driver ID` | StringType | `Driver ID` | StringType | PASS-THROUGH |
| `Team` | StringType | `Team` | StringType | PASS-THROUGH |
| `Full Name` | StringType | *(dropped)* | — | DROPPED — redundant with `drivers_data.Full Name` (DQ-17) |
| `Position` | DoubleType | `position` | IntegerType | CAST — float64 → IntegerType |
| `Points` | DoubleType | `points` | IntegerType | CAST — float64 → IntegerType |
| `Status` | StringType | `Status` | StringType | PASS-THROUGH |
| `Time` | StringType | `time_seconds` | DoubleType | TIMEDELTA_PARSE — NULL preserved for retired drivers (structural expectation) |
| `round_number` | IntegerType | `round_number` | IntegerType | INJECTED — from pipeline widget parameter |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: CR-01 (NULL_PRIMARY_KEY), CR-02 (NULL_TEAM_REFERENCE), CR-03 (NULL_RESULT_POSITION), CR-04 (POSITION_OUT_OF_RANGE), CR-05 (NEGATIVE_POINTS), CR-07 (INVALID_LAP_TIME_FORMAT), CR-10 (DUPLICATE_PRIMARY_KEY)

---

## 11. lap_times

**Source table**: `f1_platform.raw.lap_times`
**Target table**: `f1_platform.enriched.lap_times`
**Primary key**: `(round_number, Driver, lap_number)`
**Round injection**: Yes

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Driver` | StringType | `Driver` | StringType | PASS-THROUGH — 3-letter uppercase abbreviation |
| `Lap Number` | DoubleType | `lap_number` | IntegerType | CAST — float64 → IntegerType |
| `Lap Time` | StringType | `lap_time_seconds` | DoubleType | TIMEDELTA_PARSE — pandas timedelta → total seconds |
| `Position` | DoubleType | `position` | IntegerType | CAST — float64 → IntegerType |
| `Time` | StringType | `time_seconds` | DoubleType | TIMEDELTA_PARSE — cumulative elapsed race time |
| `Sector 1` | StringType | `sector_1_seconds` | DoubleType | TIMEDELTA_PARSE — NULL on lap 1 is preserved as NULL (structural expectation, DQ-18); format validated only when `lap_number != 1` and not null |
| `Sector 2` | StringType | `sector_2_seconds` | DoubleType | TIMEDELTA_PARSE — 0 nulls expected |
| `Sector 3` | StringType | `sector_3_seconds` | DoubleType | TIMEDELTA_PARSE — 0 nulls expected |
| `round_number` | IntegerType | `round_number` | IntegerType | INJECTED — from pipeline widget parameter |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: LT-01 (NULL_PRIMARY_KEY on Driver), LT-02 (NULL_PRIMARY_KEY on Lap Number), LT-03 (INVALID_ABBREVIATION_FORMAT), LT-04 (LAP_NUMBER_OUT_OF_RANGE), LT-05 (POSITION_OUT_OF_RANGE), LT-06 (INVALID_LAP_TIME_FORMAT on Lap Time), LT-07 (INVALID_LAP_TIME_FORMAT on Time), LT-08 (INVALID_SECTOR_TIME_FORMAT on Sector 2), LT-09 (INVALID_SECTOR_TIME_FORMAT on Sector 3), LT-10 (INVALID_SECTOR_TIME_FORMAT on Sector 1 when not null and `lap_number != 1`), LT-12 (DUPLICATE_PRIMARY_KEY)

---

## 12. pit_stops

**Source table**: `f1_platform.raw.pit_stops`
**Target table**: `f1_platform.enriched.pit_stops`
**Primary key**: `(round_number, Driver, lap_number)`
**Round injection**: Yes

| Source Column | Source Type (Raw) | Target Column | Target Type (Enriched) | Transformation |
|--------------|-------------------|--------------|------------------------|----------------|
| `Driver` | StringType | `Driver` | StringType | PASS-THROUGH — 3-letter uppercase abbreviation |
| `Lap Number` | DoubleType | `lap_number` | IntegerType | CAST — float64 → IntegerType |
| `Pit Out Time` | StringType | `pit_out_time_seconds` | DoubleType | TIMEDELTA_PARSE — cumulative elapsed race time at pit exit |
| `Pit In Time` | StringType | `Pit In Time` | StringType | PASS-THROUGH as NULL — 100% null is a collection failure (DQ-03). Column retained in schema to signal gap; individual rows NOT quarantined for this field. |
| `round_number` | IntegerType | `round_number` | IntegerType | INJECTED — from pipeline widget parameter |
| `ingestion_date` | TimestampType | `ingestion_date` | TimestampType | PASS-THROUGH |
| `source_file` | StringType | `source_file` | StringType | PASS-THROUGH |

**DQ rules applied**: PS-01 (NULL_PRIMARY_KEY on Driver), PS-02 (NULL_PRIMARY_KEY on Lap Number), PS-03 (INVALID_ABBREVIATION_FORMAT), PS-04 (LAP_NUMBER_OUT_OF_RANGE), PS-05 (PIT_IN_TIME_UNEXPECTED_VALUE — non-null triggers data steward warning, not quarantine), PS-06 (NULL_PIT_OUT_TIME), PS-07 (INVALID_LAP_TIME_FORMAT on Pit Out Time), PS-10 (DUPLICATE_PRIMARY_KEY)

**Collection failure note**: `Pit In Time` is expected to be 100% null (DQ-03). The column is retained in the Enriched schema so that schema evolution can be tracked when the upstream collection failure is resolved. Individual rows are not quarantined for `Pit In Time` being null — this is a batch-level structural gap, not a row-level defect.

---

*End of Source-to-Target Mapping — Enriched Layer v1.0*
