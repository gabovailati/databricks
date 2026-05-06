# Data Dictionary — Formula 1 Data Platform
**Issue**: #2 — Data Architecture Foundation
**Date**: 2026-05-06
**Status**: Gate 2 Artifact — Awaiting Approval
**Prepared by**: Data Architect Agent

---

## Table of Contents

1. [races_data](#1-races_data)
2. [season_data](#2-season_data-retired)
3. [race_results](#3-race_results)
4. [sprint_results](#4-sprint_results)
5. [circuit_info](#5-circuit_info)
6. [drivers_data](#6-drivers_data)
7. [driver_standings](#7-driver_standings)
8. [qualifying_results](#8-qualifying_results)
9. [status_data](#9-status_data)
10. [constructors_data](#10-constructors_data)
11. [constructor_standings](#11-constructor_standings-pending-redesign)
12. [constructor_results](#12-constructor_results)
13. [lap_times](#13-lap_times)
14. [pit_stops](#14-pit_stops)

---

### Conventions

- **PySpark Type**: Target type in Raw Delta table (matching source unless noted).
- **Source Type (CSV)**: Type as inferred by Spark `read_files` from the CSV/JSON.
- **Nullable**: Whether the Raw Delta column allows NULL. Injected columns are NOT NULL by pipeline contract.
- **Pipeline-injected columns** appear at the bottom of each entity table, marked `[INJECTED]`.
- Raw is an exact copy — no transformations are applied. Source type drift is documented as a note; casts happen in Enriched.

---

## 1. races_data

**Source**: `datasource/races_data.csv` | **Raw table**: `f1_platform.raw.races_data` | **Rows**: 25

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Round` | LongType | No | LongType | Season round number. Range 0–24. Round 0 = Pre-Season Testing. | Natural PK. Referenced as FK by all result/standings files. |
| `Event Name` | StringType | No | StringType | Short event name (e.g. `"Australian Grand Prix"`). All 25 values unique within the season. | Human-readable alias for the round. |
| `Official Event Name` | StringType | No | StringType | Sponsor-prefixed official event name (e.g. `"FORMULA 1 ARAMCO AUSTRALIAN GRAND PRIX 2025"`). Contains non-ASCII characters. | FK target for `circuit_info.event`. Must enforce UTF-8 encoding at read time. |
| `Location` | StringType | No | StringType | City or venue name. Not unique — `"Sakhir"` appears twice (Round 0 Pre-Season Testing + Round 2 Bahrain GP). | Cannot be used as a dimension key. Non-ASCII characters present (e.g. `"São Paulo"`). |
| `Country` | StringType | No | StringType | Host country name. Not unique — `"United States"` ×3, `"Italy"` ×2, `"Bahrain"` ×2. | Informational only in Raw. |
| `First Session` | StringType | No | StringType | Start datetime of first session for this round. Format: `YYYY-MM-DD HH:MM:SS±HH:MM` (tz-aware, mixed UTC offsets). | Cast to `TimestampType` UTC in Enriched. |
| `Last Session` | StringType | Yes | StringType | End datetime of last session for this round. 1 NULL for Round 0 (Pre-Season Testing has no defined last session). | Expected NULL for Round 0. Cast to `TimestampType` UTC in Enriched. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC timestamp when this row was written to Raw by the pipeline. | Set by pipeline; not present in source CSV. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (e.g. `"races_data.csv"`). | Set by pipeline; enables lineage tracing. |

---

## 2. season_data (RETIRED)

**Source**: `datasource/season_data.csv` | **Raw table**: *Not ingested — retired* | **Rows**: 25

> **CONSOLIDATION DECISION**: `season_data` is a strict column subset of `races_data`. It is missing only `Official Event Name` and all rows are byte-for-byte identical. This file is retired. `dim_season_calendar` is sourced exclusively from `races_data`. Do not ingest `season_data` to Raw or any downstream layer.

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Round` | LongType | No | LongType | Season round number. Identical to `races_data.Round`. | Subset of `races_data`. |
| `Event Name` | StringType | No | StringType | Short event name. Identical to `races_data.Event Name`. | Subset of `races_data`. |
| `Location` | StringType | No | StringType | Venue city. Identical to `races_data.Location`. | Subset of `races_data`. |
| `Country` | StringType | No | StringType | Host country. Identical to `races_data.Country`. | Subset of `races_data`. |
| `First Session` | StringType | No | StringType | First session datetime. Identical to `races_data.First Session`. | Subset of `races_data`. |
| `Last Session` | StringType | Yes | StringType | Last session datetime. Identical to `races_data.Last Session`. | Subset of `races_data`. |

---

## 3. race_results

**Source**: `datasource/race_results.csv` | **Raw table**: `f1_platform.raw.race_results` | **Rows**: 20 (single race — 2025 Australian GP)

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `DriverNumber` | LongType | No | LongType | Driver's car number (e.g. `4` for Norris). All 20 values unique within a race. | Not a stable surrogate key — numbers can change between seasons. |
| `BroadcastName` | StringType | No | StringType | TV broadcast display name in format `INITIAL SURNAME` (e.g. `"L NORRIS"`). | Used in broadcast graphics; not a key. |
| `Abbreviation` | StringType | No | StringType | 3-letter uppercase driver code (e.g. `"NOR"`). All 20 unique within a race. | FK bridge to `drivers_data.Abbreviation`. Consistent with abbreviation format in `lap_times` and `pit_stops`. |
| `DriverId` | StringType | No | StringType | Lowercase slug identifier (e.g. `"norris"`, `"max_verstappen"`). All 20 unique within a race. | **Primary FK** to `drivers_data.Driver ID`. Canonical driver key. |
| `TeamName` | StringType | No | StringType | Full team name (e.g. `"McLaren"`, `"Red Bull Racing"`). 10 unique values, 2 drivers each. | FK to `constructors_data.Team Name` (string match — fragile). Alternate FK to `constructors_data.team_id` once enriched. |
| `TeamColor` | StringType | No | StringType | Team primary colour as hex code without `#` prefix (e.g. `"F47600"` for McLaren). | Used to enrich `constructors_data` in Enriched layer. |
| `TeamId` | StringType | No | StringType | Team slug identifier (e.g. `"mclaren"`, `"red_bull"`). | Used to enrich `constructors_data` with a stable surrogate FK in Enriched layer. |
| `FirstName` | StringType | No | StringType | Driver first name. | Redundant with `drivers_data.First Name`. Retained in Raw as exact copy. |
| `LastName` | StringType | No | StringType | Driver last name. | Redundant with `drivers_data.Last Name`. Retained in Raw as exact copy. |
| `FullName` | StringType | No | StringType | Driver full name (concatenation of FirstName + LastName). | Derivable. Retained in Raw; will be dropped in Enriched (`DQ-17`). |
| `HeadshotUrl` | StringType | Yes | StringType | URL to driver headshot image. Colapinto's value is the string `"None"` (not SQL NULL) — must be treated as a sentinel string in Enriched. | Optional metadata. `"None"` sentinel must be replaced with SQL NULL in Enriched global sentinel step. |
| `CountryCode` | StringType | Yes | StringType | Driver nationality code. **100% NULL** — extraction bug in upstream pipeline. | **DQ-01**: Entirely null. Pass through in Raw as-is. Will be documented as unusable until source is fixed. |
| `Position` | DoubleType | No | DoubleType | Finishing position 1–20. Stored as float64 by pandas CSV serialisation. | Cast to IntegerType in Enriched (`DQ-12`). |
| `ClassifiedPosition` | StringType | No | StringType | Official classification: digit string `"1"`–`"17"` for classified finishers, `"R"` for retired drivers. | Mixed type. Split into `finish_position` (IntegerType, nullable) and `classification_status` in Enriched (`DQ-14`). |
| `GridPosition` | DoubleType | No | DoubleType | Starting grid position. Stored as float64. | Cast to IntegerType in Enriched. |
| `Q1` | StringType | Yes | StringType | Q1 qualifying lap time. **100% NULL** — qualifying data not joined into this file. | **DQ-02**: Entirely null. Pass through in Raw; note as pass-through that will be dropped in Enriched. Source qualifying times from `qualifying_results` instead. |
| `Q2` | StringType | Yes | StringType | Q2 qualifying lap time. **100% NULL**. | Same as `Q1` above. |
| `Q3` | StringType | Yes | StringType | Q3 qualifying lap time. **100% NULL**. | Same as `Q1` above. |
| `Time` | StringType | Yes | StringType | Race time. For the winner: total race duration. For all others: gap to race winner. Format: pandas timedelta string `"0 days HH:MM:SS.ffffff"`. 3 NULLs for retired drivers (HAM, LEC, BOR — expected). | **DQ-09**: Requires custom timedelta UDF in Enriched. Semantic duality (duration vs gap) must be documented in Enriched column comment. |
| `Status` | StringType | No | StringType | Race finish status. Values: `"Finished"` (17 rows), `"Retired"` (3 rows). | Enum; production data will include additional values (e.g. `"+1 Lap"`, `"Engine"`). Consistent with `status_data.Status`. |
| `Points` | DoubleType | No | DoubleType | Championship points scored (0–25). Stored as float64. | Cast to IntegerType in Enriched. |
| `Laps` | DoubleType | No | DoubleType | Laps completed. 71.0 for race finishers; lower for retirees. Stored as float64. | Cast to IntegerType in Enriched. |
| `round_number` | IntegerType | No | — | **[INJECTED]** Pipeline job parameter identifying which round this file covers. Not present in source CSV. | **DQ-05**: Critical. Must be injected at ingest. Forms part of composite PK with `DriverId`. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC timestamp when this row was written to Raw. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (e.g. `"race_results.csv"`). | Set by pipeline. |

---

## 4. sprint_results

**Source**: `datasource/sprint_results.csv` | **Raw table**: `f1_platform.raw.sprint_results` | **Rows**: 20 (single sprint race)

> **NOTE**: Schema is byte-for-byte identical to `race_results`. All column definitions below inherit from Section 3. Only differences are documented.

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `DriverNumber` | LongType | No | LongType | Driver car number. | Same as `race_results`. |
| `BroadcastName` | StringType | No | StringType | Broadcast name. | Same as `race_results`. |
| `Abbreviation` | StringType | No | StringType | 3-letter driver code. | Same as `race_results`. |
| `DriverId` | StringType | No | StringType | Slug driver identifier. | Same as `race_results`. FK to `drivers_data.Driver ID`. |
| `TeamName` | StringType | No | StringType | Team full name. | Same as `race_results`. |
| `TeamColor` | StringType | No | StringType | Team hex colour. | Same as `race_results`. |
| `TeamId` | StringType | No | StringType | Team slug. | Same as `race_results`. |
| `FirstName` | StringType | No | StringType | Driver first name. | Same as `race_results`. |
| `LastName` | StringType | No | StringType | Driver last name. | Same as `race_results`. |
| `FullName` | StringType | No | StringType | Driver full name. | Same as `race_results`. |
| `HeadshotUrl` | StringType | Yes | StringType | Headshot image URL. Colapinto sentinel `"None"` applies here too. | Same as `race_results`. |
| `CountryCode` | StringType | Yes | StringType | Nationality code. **100% NULL** — same extraction bug as `race_results`. | **DQ-01** applies. |
| `Position` | DoubleType | No | DoubleType | Sprint race finishing position. | Cast to IntegerType in Enriched. |
| `ClassifiedPosition` | StringType | No | StringType | Official classification. Same mixed-type pattern as `race_results`. | **DQ-14** applies. |
| `GridPosition` | DoubleType | No | DoubleType | Sprint starting grid position. | Cast to IntegerType in Enriched. |
| `Q1` | StringType | Yes | StringType | **100% NULL**. | **DQ-02** applies. |
| `Q2` | StringType | Yes | StringType | **100% NULL**. | **DQ-02** applies. |
| `Q3` | StringType | Yes | StringType | **100% NULL**. | **DQ-02** applies. |
| `Time` | StringType | Yes | StringType | Sprint race time. Same pandas timedelta format as `race_results`. **Critical difference**: Ocon's `Time` = `"0 days 00:00:31"` — no microsecond component. Timedelta UDF must handle both formats. | **DQ-09** + **DQ-15**: UDF must handle `HH:MM:SS.ffffff` and `HH:MM:SS` variants. |
| `Status` | StringType | No | StringType | Sprint finish status (`"Finished"` or `"Retired"`). 2 retirees: Piastri, Colapinto. | Same as `race_results`. |
| `Points` | DoubleType | No | DoubleType | Sprint championship points (0–8 scale). Stored as float64. | Cast to IntegerType in Enriched. |
| `Laps` | DoubleType | No | DoubleType | Laps completed. 24.0 (finishers), 23.0 or 5.0 (retirees). | Cast to IntegerType in Enriched. |
| `round_number` | IntegerType | No | — | **[INJECTED]** Pipeline round number parameter. | Same as `race_results`. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Same as `race_results`. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"sprint_results.csv"`). | Same as `race_results`. |

---

## 5. circuit_info

**Source**: `datasource/circuit_info.json` | **Raw table**: `f1_platform.raw.circuit_info` | **Structure**: Single JSON document flattened to 5 session rows

> **NOTE**: The source is a nested JSON document. Raw ingestion flattens the `sessions` nested object. Each row in the Raw table represents one session for one circuit/event. The flattening schema is defined here.

| Column Name | PySpark Type | Nullable | Source Type (JSON) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `name` | StringType | No | StringType | Circuit/venue name (e.g. `"São Paulo"`). Contains non-ASCII characters. | Corresponds informally to `races_data.Location`. Not a strict FK — string match only. UTF-8 encoding required. |
| `country` | StringType | No | StringType | Host country (e.g. `"Brazil"`). | Informational. |
| `event` | StringType | No | StringType | Official event name (e.g. `"FORMULA 1 MSC CRUISES GRANDE PRÊMIO DE SÃO PAULO 2025"`). Contains non-ASCII. | **FK** to `races_data.Official Event Name`. UTF-8 collation required. Forms part of composite PK. |
| `format` | StringType | No | StringType | Event format enum (e.g. `"sprint_qualifying"`). Other values not yet observed in data. | Undocumented enum — other expected values include `"conventional"`, `"sprint"`. Must expand enum documentation as more events are ingested. |
| `event_date` | StringType | No | StringType | Event date as naive datetime string `"YYYY-MM-DDTHH:MM:SS"` (no timezone). | Cast to `DateType` in Enriched after UTC assumption. **DQ-13**: one of three non-normalised datetime formats in this file. |
| `session_name` | StringType | No | StringType | Session name extracted from flattening `sessions.SessionN.name`. Values: `"Practice 1"`, `"Sprint Qualifying"`, `"Sprint"`, `"Qualifying"`, `"Race"`. | Forms part of composite PK with `event`. Derived from nested JSON key during flatten. |
| `session_date` | StringType | No | StringType | Session local start datetime, tz-aware (e.g. `"2025-11-07T11:30:00-03:00"`). | Derived from `sessions.SessionN.date`. Cast to `TimestampType` UTC in Enriched. One of three non-normalised datetime formats. |
| `session_utc` | StringType | No | StringType | Session start datetime in UTC, naive (no `Z` or `+00:00` suffix) (e.g. `"2025-11-07T14:30:00"`). | Derived from `sessions.SessionN.utc`. Append `+00:00` before casting to `TimestampType` in Enriched. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC timestamp when this row was written to Raw. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"circuit_info.json"`). | Set by pipeline. |

---

## 6. drivers_data

**Source**: `datasource/drivers_data.csv` | **Raw table**: `f1_platform.raw.drivers_data` | **Rows**: 20

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Number` | LongType | No | LongType | Driver car number (e.g. `4` for Norris, `1` for Verstappen). All 20 unique. | Not a stable surrogate key — car numbers can change between seasons. |
| `Driver ID` | StringType | No | StringType | Canonical slug identifier (e.g. `"norris"`, `"max_verstappen"`). All 20 unique. | **Primary Key**. **Canonical driver FK target** for all slug-based entities: `race_results.DriverId`, `sprint_results.DriverId`, `qualifying_results.Driver ID`, `driver_standings.Driver ID`, `constructor_results.Driver ID`. |
| `Abbreviation` | StringType | No | StringType | 3-letter uppercase driver code (e.g. `"NOR"`, `"VER"`). All 20 unique. | Secondary FK target for abbreviation-based entities: `lap_times.Driver`, `pit_stops.Driver`, `status_data.Abbreviation`, `qualifying_results.Abbreviation`. Used as the mapping key in `dim_driver_mapping`. |
| `First Name` | StringType | No | StringType | Driver first name (e.g. `"Lando"`). | Master source for driver first name. |
| `Last Name` | StringType | No | StringType | Driver last name (e.g. `"Norris"`). | Master source for driver last name. |
| `Full Name` | StringType | No | StringType | Concatenated full name (e.g. `"Lando Norris"`). Derivable from `First Name` + `Last Name`. | Redundant but retained in Raw. Will be dropped from Silver fact tables (`DQ-17`). Retained in `dim_driver` dimension. |
| `Team` | StringType | No | StringType | Current team name (e.g. `"McLaren"`). 10 teams, exactly 2 drivers each. | Informational FK to `constructors_data.Team Name`. Subject to string-match fragility. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"drivers_data.csv"`). | Set by pipeline. |

---

## 7. driver_standings

**Source**: `datasource/driver_standings.csv` | **Raw table**: `f1_platform.raw.driver_standings` | **Rows**: 20

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Driver ID` | StringType | No | StringType | Slug driver identifier. All 20 unique. | **FK** to `drivers_data.Driver ID`. Forms part of composite PK with `round_number`. |
| `Abbreviation` | StringType | No | StringType | 3-letter driver code. All 20 unique. | Secondary identifier. Consistent with all driver entities. |
| `Full Name` | StringType | No | StringType | Driver full name. | Redundant with `drivers_data.Full Name`. Retained in Raw; will be dropped from Silver fact tables (`DQ-17`). |
| `Team` | StringType | No | StringType | Team name at time of standings snapshot. 10 teams, 2 drivers each. | String match to `constructors_data.Team Name`. |
| `Points` | DoubleType | No | DoubleType | Championship points accumulated to date. Range 0–25 in sample (single-round snapshot). Stored as float64. | Cast to IntegerType in Enriched (`DQ-12`). |
| `Position` | DoubleType | No | DoubleType | Championship standings position 1–20. Stored as float64. | Cast to IntegerType in Enriched. |
| `round_number` | IntegerType | No | — | **[INJECTED]** Pipeline round number parameter. Anchors this standings snapshot to a specific round. | **DQ-05**: Required for historical standings time series. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"driver_standings.csv"`). | Set by pipeline. |

---

## 8. qualifying_results

**Source**: `datasource/qualifying_results.csv` | **Raw table**: `f1_platform.raw.qualifying_results` | **Rows**: 20

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Abbreviation` | StringType | No | StringType | 3-letter uppercase driver code. All 20 unique, 0 nulls. | **PK component** (used instead of `Driver ID` due to BOR sentinel). FK to `drivers_data.Abbreviation`. |
| `Driver ID` | StringType | Yes | StringType | Slug driver identifier. **BOR's value is the string literal `"nan"`** — Spark reads as non-null, length 3. `IS NULL` = false. | **DQ-06**: Sentinel detected in Enriched layer. Replace `driver_id = 'nan'` with SQL NULL, then apply targeted fix to set BOR's value to `'bortoleto'`. FK to `drivers_data.Driver ID` after fix. |
| `Full Name` | StringType | No | StringType | Driver full name. | Redundant with `drivers_data`. Retained in Raw; dropped in Silver fact tables (`DQ-17`). |
| `Team` | StringType | No | StringType | Team name. 10 teams. | String match to `constructors_data.Team Name`. |
| `Q1 Time` | StringType | Yes | StringType | Q1 best lap time. Pandas timedelta string format `"0 days HH:MM:SS.ffffff"`. 1 NULL (BOR — did not participate or extraction artifact). | **DQ-09**: Timedelta UDF required in Enriched. Authoritative Q1 time source — do not use `Q1` column in `race_results`/`sprint_results` (entirely null). |
| `Q2 Time` | StringType | Yes | StringType | Q2 best lap time. NULL for 5 Q1-eliminated drivers + BOR (6 NULLs total). | Structural NULLs — Q1 elimination means no Q2 time. Preserve as meaningful NULL; do not quarantine. |
| `Q3 Time` | StringType | Yes | StringType | Q3 best lap time. NULL for non-top-10 drivers (11 NULLs total). | Structural NULLs — elimination format. Preserve as meaningful NULL. |
| `Position` | DoubleType | Yes | DoubleType | Qualifying position 1–20. 1 NULL (BOR). Stored as float64. | Cast to IntegerType in Enriched. BOR NULL is consistent with failed qualification data. |
| `round_number` | IntegerType | No | — | **[INJECTED]** Pipeline round number parameter. | **DQ-05**: Required for composite PK. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"qualifying_results.csv"`). | Set by pipeline. |

---

## 9. status_data

**Source**: `datasource/status_data.csv` | **Raw table**: `f1_platform.raw.status_data` | **Rows**: 20

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Abbreviation` | StringType | No | StringType | 3-letter driver code. All 20 unique. | **PK component**. FK to `drivers_data.Abbreviation`. Only driver identifier in this file — no `Driver ID` column. |
| `Full Name` | StringType | No | StringType | Driver full name. | Redundant with `drivers_data`. Retained in Raw; dropped in Silver fact tables (`DQ-17`). |
| `Status` | StringType | No | StringType | Race finish status. Sample values: `"Finished"` (17 rows), `"Retired"` (3 rows: HAM, LEC, BOR). | **DQ-20**: Enum is incomplete in this sample. Production F1 data includes `"+1 Lap"`, `"Engine"`, `"Collision"`, `"Disqualified"`, etc. Consistent with `race_results.Status`. |
| `round_number` | IntegerType | No | — | **[INJECTED]** Pipeline round number parameter. | **DQ-05**: Required for composite PK. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"status_data.csv"`). | Set by pipeline. |

---

## 10. constructors_data

**Source**: `datasource/constructors_data.csv` | **Raw table**: `f1_platform.raw.constructors_data` | **Rows**: 10

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Team Name` | StringType | No | StringType | Full team name (e.g. `"McLaren"`, `"Red Bull Racing"`). All 10 unique. | **PK** — fragile string key. Single-column source table. In Enriched layer, enriched with `TeamId` and `TeamColor` from `race_results` to add stable surrogate (`DQ-10`). |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"constructors_data.csv"`). | Set by pipeline. |

---

## 11. constructor_standings (PENDING REDESIGN)

**Source**: `datasource/constructor_standings.csv` | **Raw table**: *Not ingested until redesigned* | **Rows**: 20

> **CONSOLIDATION DECISION**: This file is byte-for-byte identical to `constructor_results.csv` (DQ-04). It contains driver-level race data, not constructor-aggregated standings. `constructor_results` is designated the authoritative per-race driver results source. `constructor_standings` must be re-extracted to carry constructor-aggregated standings (points and position per constructor per round). Until re-extraction is complete, this file is not ingested to Raw to avoid duplicating `constructor_results` data.
>
> **Intended schema once redesigned** — the columns below represent the current (broken) state of the file, documented for completeness.

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Driver ID` | StringType | No | StringType | Slug identifier. Identical to `constructor_results.Driver ID`. | Currently driver-level data — incorrect for constructor standings. |
| `Team` | StringType | No | StringType | Team name. | FK to `constructors_data.Team Name`. |
| `Full Name` | StringType | No | StringType | Driver full name. | Currently driver-level — incorrect for constructor standings. |
| `Position` | DoubleType | No | DoubleType | Currently driver position — should be constructor championship position. | Cast to IntegerType in Enriched once redesigned. |
| `Points` | DoubleType | No | DoubleType | Currently driver points — should be constructor total. | Cast to IntegerType in Enriched once redesigned. |
| `Status` | StringType | No | StringType | `"Finished"` or `"Retired"`. | Driver-level — incorrect for constructor standings. |
| `Time` | StringType | Yes | StringType | Pandas timedelta string. 3 NULLs (retired drivers). | Driver-level. Identical to `constructor_results.Time`. |

---

## 12. constructor_results

**Source**: `datasource/constructor_results.csv` | **Raw table**: `f1_platform.raw.constructor_results` | **Rows**: 20

> **AUTHORITATIVE** source for per-race constructor-level driver results. Designated as canonical following DQ-04 resolution.

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Driver ID` | StringType | No | StringType | Slug driver identifier. All 20 unique within a race. | **PK component**. FK to `drivers_data.Driver ID`. |
| `Team` | StringType | No | StringType | Team name. 10 teams, 2 drivers each. | FK to `constructors_data.Team Name`. String match. |
| `Full Name` | StringType | No | StringType | Driver full name. | Redundant with `drivers_data`. Retained in Raw; dropped in Silver fact tables (`DQ-17`). |
| `Position` | DoubleType | No | DoubleType | Race finishing position 1–20. Stored as float64. | Cast to IntegerType in Enriched (`DQ-12`). |
| `Points` | DoubleType | No | DoubleType | Championship points scored (0–25). Stored as float64. | Cast to IntegerType in Enriched. |
| `Status` | StringType | No | StringType | Race finish status (`"Finished"` or `"Retired"`). 3 retirees: HAM, LEC, BOR. | Consistent with `race_results.Status` and `status_data.Status`. |
| `Time` | StringType | Yes | StringType | Race time in pandas timedelta format. Winner = total race duration; others = gap to winner. 3 NULLs for retired drivers (expected). | **DQ-09**: Timedelta UDF required. Semantic duality identical to `race_results.Time`. |
| `round_number` | IntegerType | No | — | **[INJECTED]** Pipeline round number parameter. | **DQ-05**: Forms part of composite PK. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"constructor_results.csv"`). | Set by pipeline. |

---

## 13. lap_times

**Source**: `datasource/lap_times.csv` | **Raw table**: `f1_platform.raw.lap_times` | **Rows**: 1,251

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Driver` | StringType | No | StringType | 3-letter uppercase driver abbreviation (e.g. `"VER"`, `"NOR"`). 20 unique values. | **PK component**. FK to `dim_driver_mapping.abbreviation` → then to `drivers_data.Driver ID`. **Not** a slug — see Section 4 of data_mapping.md. |
| `Lap Number` | DoubleType | No | DoubleType | Lap number (1 through ~71). Stored as float64 by pandas. | **PK component**. Cast to IntegerType in Enriched (`DQ-12`). |
| `Lap Time` | StringType | No | StringType | Individual lap duration. Pandas timedelta string `"0 days HH:MM:SS.ffffff"`. High cardinality (~1,251 distinct values). | **DQ-09**: Timedelta UDF required. Lap 1 times are outliers (formation lap). |
| `Position` | DoubleType | No | DoubleType | Driver's track position at end of this lap. 20 distinct values (1–20). Stored as float64. | Cast to IntegerType in Enriched. |
| `Time` | StringType | No | StringType | Cumulative elapsed race time at the end of this lap. Pandas timedelta string. High cardinality. | **DQ-09**: Timedelta UDF required. Distinct from `Lap Time` — this is cumulative, not per-lap duration. |
| `Sector 1` | StringType | Yes | StringType | Sector 1 split time. Pandas timedelta string. NULL for lap 1 for all 20 drivers (~20 NULLs total) — structurally expected (no Sector 1 timing gate crossed on lap 1). | **DQ-18**: Structural NULL — not a data quality failure. Must be preserved as meaningful NULL; do not quarantine. |
| `Sector 2` | StringType | No | StringType | Sector 2 split time. Pandas timedelta string. 0 NULLs in sample. | **DQ-09**: Timedelta UDF required. |
| `Sector 3` | StringType | No | StringType | Sector 3 split time. Pandas timedelta string. 0 NULLs in sample. | **DQ-09**: Timedelta UDF required. |
| `round_number` | IntegerType | No | — | **[INJECTED]** Pipeline round number parameter. | **DQ-05**: Forms part of composite PK `(round_number, Driver, Lap Number)`. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"lap_times.csv"`). | Set by pipeline. |

---

## 14. pit_stops

**Source**: `datasource/pit_stops.csv` | **Raw table**: `f1_platform.raw.pit_stops` | **Rows**: 39

| Column Name | PySpark Type | Nullable | Source Type (CSV) | Description | Notes / Lineage |
|-------------|-------------|----------|--------------------|-------------|-----------------|
| `Driver` | StringType | No | StringType | 3-letter uppercase driver abbreviation. 20 unique values across 39 pit stop rows. | **PK component**. FK to `dim_driver_mapping.abbreviation`. Same format as `lap_times.Driver`. |
| `Lap Number` | DoubleType | No | DoubleType | Lap number on which the pit stop occurred. 35 distinct values (multiple drivers pit on the same lap). Stored as float64. | **PK component**. Cast to IntegerType in Enriched. |
| `Pit Out Time` | StringType | No | StringType | Time at which the driver exited the pit lane, measured as cumulative race elapsed time. Pandas timedelta string. 39 unique values. | **DQ-09**: Timedelta UDF required. Joins to `lap_times.Time` for lap context. |
| `Pit In Time` | StringType | Yes | StringType | Time at which the driver entered the pit lane. **100% NULL** — data collection failure. | **DQ-03**: Critical. Pit stop duration (`Pit Out Time` − `Pit In Time`) cannot be computed without this field. Pass through as NULL in Raw. No pit stop duration analytics possible until source is fixed. |
| `round_number` | IntegerType | No | — | **[INJECTED]** Pipeline round number parameter. | **DQ-05**: Forms part of composite PK `(round_number, Driver, Lap Number)`. |
| `ingestion_date` | TimestampType | No | — | **[INJECTED]** UTC ingest timestamp. | Set by pipeline. |
| `source_file` | StringType | No | — | **[INJECTED]** Source filename (`"pit_stops.csv"`). | Set by pipeline. |
