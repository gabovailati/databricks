# Data Profiling Report — Formula 1 Data Platform
**Sprint**: Issue #1 — Data Profiling Sprint
**Date**: 2026-05-05
**Status**: Gate 1 Artifact — Awaiting Approval
**Prepared by**: Data Engineer Agent (sub-agent synthesis)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Entity Profiles](#2-entity-profiles)
   - 2.1 [races_data](#21-races_data)
   - 2.2 [season_data](#22-season_data)
   - 2.3 [race_results](#23-race_results)
   - 2.4 [sprint_results](#24-sprint_results)
   - 2.5 [circuit_info](#25-circuit_info)
   - 2.6 [drivers_data](#26-drivers_data)
   - 2.7 [driver_standings](#27-driver_standings)
   - 2.8 [qualifying_results](#28-qualifying_results)
   - 2.9 [status_data](#29-status_data)
   - 2.10 [constructors_data](#210-constructors_data)
   - 2.11 [constructor_standings](#211-constructor_standings)
   - 2.12 [constructor_results](#212-constructor_results)
   - 2.13 [lap_times](#213-lap_times)
   - 2.14 [pit_stops](#214-pit_stops)
3. [Primary Key Decisions](#3-primary-key-decisions)
4. [Data Quality Issues Priority Matrix](#4-data-quality-issues-priority-matrix)
5. [Recommended Next Steps](#5-recommended-next-steps)

---

## 1. Executive Summary

Thirteen source entities were profiled across three groups: Race/Event, Driver, and Constructor/Timing. The data represents a **single race weekend snapshot** — result files appear to cover the 2025 Australian GP (Round 1 or 2), while `circuit_info.json` describes São Paulo 2025 (Round 23). No multi-race history is yet present.

### Top Cross-Cutting Findings

| # | Finding | Impact | Scope |
|---|---------|--------|-------|
| 1 | **Driver identifier split**: slug format (`norris`) in result/standings files vs 3-letter code (`NOR`) in `lap_times` and `pit_stops`. A mapping table is mandatory for any cross-entity join. | Critical | 7 of 13 entities |
| 2 | **Three entirely null columns** cannot carry any data: `CountryCode` in `race_results`/`sprint_results` (extraction bug), `Q1/Q2/Q3` in `race_results`/`sprint_results` (qualifying not joined), `Pit In Time` in `pit_stops` (collection failure). | Critical | 3 entities |
| 3 | **No `Round` column on any result file** (`race_results`, `sprint_results`, `qualifying_results`, `constructor_results`, `constructor_standings`). The `Round` key must be injected at ingest time from pipeline parameters or filename metadata to enable multi-race history. | Critical | 5 entities |
| 4 | **All time/duration values are pandas timedelta strings** (`"0 days HH:MM:SS.ffffff"`). PySpark has no native parser for this format; a UDF or regex-based transformation is required. One inconsistency exists: Ocon's `Time` in `sprint_results` lacks the microsecond component. | High | 6 entities |
| 5 | **`constructor_standings` and `constructor_results` are byte-for-byte identical**. One file must be designated the authoritative source and the other retired or populated with distinct data. | Critical | 2 entities |
| 6 | **`races_data` and `season_data` are near-duplicates** — `season_data` is a strict column subset of `races_data`. These should be consolidated into a single `dim_season_calendar` table. | High | 2 entities |
| 7 | **BOR's `Driver ID` is null** in `qualifying_results`. This breaks referential integrity with `drivers_data` and any downstream join on driver slug. Fix value: `bortoleto`. | High | 1 entity |
| 8 | **All logical integers stored as float64** due to pandas CSV type inference (`Position`, `Points`, `GridPosition`, `Laps`, `Lap Number`, etc.). Explicit casts are required in the Bronze → Silver transformation. | Medium | 9 entities |
| 9 | **Datetime values are not normalised**. Three different formats exist: tz-aware strings with mixed UTC offsets (`races_data`), tz-aware local time (`circuit_info` sessions), and naive UTC strings without a `Z` suffix (`circuit_info` utc fields). All must be cast to UTC in Silver. | Medium | 3 entities |
| 10 | **All data is a single-event snapshot**. The schema and key design must be extended to support multi-round, multi-season history before any Silver layer tables are written. | High | All entities |

---

## 2. Entity Profiles

### 2.1 races_data

**Source**: `datasource/races_data.csv` | **Rows**: 25

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Round | LongType | 0 | 25 | Range 0–24; Round 0 = Pre-Season Testing |
| Event Name | StringType | 0 | 25 | All unique short names |
| Official Event Name | StringType | 0 | 25 | Sponsor-prefixed; contains non-ASCII characters |
| Location | StringType | 0 | 24 | "Sakhir" appears twice (Pre-Season + Bahrain GP) |
| Country | StringType | 0 | 19 | "United States" ×3, "Italy" ×2, "Bahrain" ×2 |
| First Session | StringType | 0 | 25 | Tz-aware datetime string `YYYY-MM-DD HH:MM:SS±HH:MM`; mixed UTC offsets; not normalised to UTC |
| Last Session | StringType | 1 | 24 | 1 null: Round 0 (Pre-Season Testing — expected) |

**Candidate PK**: `Round` (unique). `Event Name` also unique and human-readable.

**DQ Issues**:
- `Last Session` null for Round 0 is structurally expected but must be handled in Silver transforms.
- `First Session` / `Last Session` are tz-aware strings with mixed UTC offsets — must be cast to `TimestampType` in UTC.
- `Official Event Name` contains non-ASCII characters (e.g. accented characters in circuit names) — encoding must be enforced at read time (UTF-8).
- Round 0 is a non-race testing event — may need to be filtered for race-only analysis or flagged with an `event_type` flag.
- `Location` is not unique — cannot be used as a dimension key.

**Cross-entity relationships**:
- `Round` → primary FK referenced by all result/standings files (none currently carry it — must be injected).
- `Official Event Name` matches the `event` field in `circuit_info.json`.

---

### 2.2 season_data

**Source**: `datasource/season_data.csv` | **Rows**: 25

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Round | LongType | 0 | 25 | Identical to races_data |
| Event Name | StringType | 0 | 25 | Identical to races_data |
| Location | StringType | 0 | 24 | Identical to races_data |
| Country | StringType | 0 | 19 | Identical to races_data |
| First Session | StringType | 0 | 25 | Identical to races_data |
| Last Session | StringType | 1 | 24 | Identical to races_data |

**Candidate PK**: `Round`.

**DQ Issues**:
- This file is a strict subset of `races_data.csv` — it is missing only `Official Event Name` and all rows are identical.
- **Consolidation decision required**: These two files should be collapsed into a single `dim_season_calendar` Bronze table sourced from `races_data` (the superset). `season_data` should be retired.
- All DQ issues from `races_data` apply equally here.

**Cross-entity relationships**:
- Fully overlapping with `races_data`. See section 2.1.

---

### 2.3 race_results

**Source**: `datasource/race_results.csv` | **Rows**: 20 (single race — 2025 Australian GP)

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| DriverNumber | LongType | 0 | 20 | All unique |
| BroadcastName | StringType | 0 | 20 | Format: `INITIAL SURNAME` |
| Abbreviation | StringType | 0 | 20 | 3-letter uppercase code |
| DriverId | StringType | 0 | 20 | Slug (e.g. `norris`, `max_verstappen`) |
| TeamName | StringType | 0 | 10 | 10 teams, 2 drivers each |
| TeamColor | StringType | 0 | 10 | Hex codes without `#` prefix |
| TeamId | StringType | 0 | 10 | Slug (e.g. `mclaren`, `red_bull`) |
| FirstName | StringType | 0 | 20 | |
| LastName | StringType | 0 | 20 | |
| FullName | StringType | 0 | 20 | |
| HeadshotUrl | StringType | 1 | 19 | 1 null: Colapinto (stored as Python `None`) |
| CountryCode | StringType | 20 | 0 | **ENTIRELY NULL** — extraction bug |
| Position | DoubleType | 0 | 20 | Finishing position 1–20; should be IntegerType |
| ClassifiedPosition | StringType | 0 | 17 | Mixed: digit strings `"1"`–`"17"` + `"R"` for retired |
| GridPosition | DoubleType | 0 | 20 | Starting grid 1–20; should be IntegerType |
| Q1 | StringType | 20 | 0 | **ENTIRELY NULL** — qualifying not joined |
| Q2 | StringType | 20 | 0 | **ENTIRELY NULL** |
| Q3 | StringType | 20 | 0 | **ENTIRELY NULL** |
| Time | StringType | 3 | 17 | Pandas timedelta string; winner = race duration, others = gap to leader; 3 nulls (Hamilton, Leclerc, Bortoleto — retired) |
| Status | StringType | 0 | 2 | `"Finished"` (17) or `"Retired"` (3) |
| Points | DoubleType | 0 | 7 | Championship points; should be IntegerType |
| Laps | DoubleType | 0 | 2 | 71.0 (finishers) or lower (retirees); should be IntegerType |

**Candidate PK**: `DriverId` within a single race. Multi-race PK requires composite `(Round, DriverId)` — no `Round` column currently exists.

**DQ Issues**:
- `CountryCode`: 100% null — extraction bug in upstream pipeline. Drop or leave null until source is fixed.
- `Q1`, `Q2`, `Q3`: 100% null — qualifying data not joined into this file. Remove columns or source from `qualifying_results`.
- `Time`: pandas timedelta string with semantic duality (race duration for winner, gap-to-leader for others); 3 nulls for retired drivers are expected.
- `ClassifiedPosition`: mixed type — numeric strings for classified finishers, `"R"` for retirees. Requires split into `FinishPosition` (IntegerType) and `ClassificationStatus`.
- `HeadshotUrl`: 1 null (Colapinto) — acceptable as optional metadata.
- `Position`, `GridPosition`, `Points`, `Laps`: stored as float64 — must be explicitly cast to IntegerType in Silver.
- No `Round` column — must be injected at ingest.

**Cross-entity relationships**:
- `DriverId` → `drivers_data.Driver ID`
- `TeamId` → `constructors_data` (TeamId slug not present there — join must use `TeamName` ↔ `Team Name`)
- Schema is identical to `sprint_results` — can be unioned with a `session_type` discriminator column (`"Race"` / `"Sprint"`)

---

### 2.4 sprint_results

**Source**: `datasource/sprint_results.csv` | **Rows**: 20 (single sprint race)

Identical schema to `race_results`. Key differences:

| Difference | Detail |
|------------|--------|
| Retired drivers | 2 (Piastri, Colapinto) vs 3 in race_results |
| Lap count | 24.0 (finishers), 23.0 or 5.0 (retirees) |
| `Time` format inconsistency | Ocon's `Time` = `"0 days 00:00:31"` — no microsecond component. This will break uniform timedelta parsing. |
| `CountryCode` | 100% null (same extraction bug) |
| `Q1/Q2/Q3` | 100% null (same issue) |

**Candidate PK**: `DriverId` within a single sprint. Multi-event composite: `(Round, DriverId)`.

**DQ Issues**: All issues from `race_results` apply. Additionally:
- Ocon's `Time` lacks the `.ffffff` microsecond segment — the timedelta UDF must handle both formats.
- No `Round` column — must be injected at ingest.

**Cross-entity relationships**:
- Same as `race_results`. The two entities should be unioned in the Silver layer with a `session_type` column.

---

### 2.5 circuit_info

**Source**: `datasource/circuit_info.json` | **Structure**: Single JSON document (São Paulo 2025)

| Field | Type | Notes |
|-------|------|-------|
| name | StringType | `"São Paulo"` — venue name; non-ASCII |
| country | StringType | `"Brazil"` |
| event | StringType | Matches `races_data.Official Event Name` (Round 23) |
| format | StringType | `"sprint_qualifying"` — event format enum; values undocumented |
| event_date | StringType | `"2025-11-09T00:00:00"` — naive datetime (no timezone) |
| sessions.SessionN.name | StringType | `"Practice 1"`, `"Sprint Qualifying"`, `"Sprint"`, `"Qualifying"`, `"Race"` |
| sessions.SessionN.date | StringType | Tz-aware local time (BRT −03:00) |
| sessions.SessionN.utc | StringType | Naive UTC string — no `Z` or `+00:00` suffix |

**Candidate PK**: Not applicable (single document). If normalised: composite `(event, session_name)`.

**DQ Issues**:
- Three different datetime formats within a single document: tz-aware local, naive UTC, naive `event_date`. All must be normalised to UTC TimestampType.
- Nested `sessions` structure uses generic keys (`Session1`–`Session5`). Flattening requires a lateral explode or manual key enumeration.
- Single document only — unclear if one JSON file per circuit per event is expected, or if this is a singleton. Ingestion logic must account for both patterns.
- `format` enum values are undocumented — `sprint_qualifying` confirmed; others unknown.
- `name` and `event` contain non-ASCII characters.

**Cross-entity relationships**:
- `event` → `races_data.Official Event Name`
- `name` → `races_data.Location`
- Session names map to session types in `race_results` and `sprint_results`

---

### 2.6 drivers_data

**Source**: `datasource/drivers_data.csv` | **Rows**: 20

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Number | LongType | 0 | 20 | Car number 1–87; all unique |
| Driver ID | StringType | 0 | 20 | Slug (e.g. `max_verstappen`); all unique |
| Abbreviation | StringType | 0 | 20 | 3-char uppercase; all unique |
| First Name | StringType | 0 | 20 | |
| Last Name | StringType | 0 | 20 | |
| Full Name | StringType | 0 | 20 | Derivable from First + Last Name |
| Team | StringType | 0 | 10 | 10 teams, exactly 2 drivers each |

**Candidate PK**: `Driver ID` (recommended canonical FK). `Abbreviation` and `Number` are also individually unique.

**DQ Issues**:
- `Number` is a racing car number, not a stable surrogate — it can change between seasons (e.g. a driver taking a retired number).
- `Full Name` is redundant — it is derivable from `First Name` + `Last Name`.
- No surrogate integer key — string slug `Driver ID` is the natural key.

**Cross-entity relationships**:
- `Driver ID` → `driver_standings.Driver ID`, `race_results.DriverId`, `sprint_results.DriverId`, `qualifying_results.Driver ID` (BOR null — fix required)
- `Abbreviation` → `qualifying_results.Abbreviation`, `status_data.Abbreviation`, `lap_times.Driver`, `pit_stops.Driver`
- This is the **authoritative driver dimension** — all other entities should join back to it.

---

### 2.7 driver_standings

**Source**: `datasource/driver_standings.csv` | **Rows**: 20

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Driver ID | StringType | 0 | 20 | Slug; all unique |
| Abbreviation | StringType | 0 | 20 | 3-char; all unique |
| Full Name | StringType | 0 | 20 | |
| Team | StringType | 0 | 10 | |
| Points | DoubleType | 0 | 11 | Values 0–25; should be IntegerType |
| Position | DoubleType | 0 | 20 | Championship position 1–20; should be IntegerType |

**Candidate PK**: `Driver ID`.

**DQ Issues**:
- `Points` and `Position` stored as float64 — must be cast to IntegerType.
- No race/round temporal context — this is a point-in-time snapshot. A `Round` or `as_of_date` column is needed for historical standings.
- `Driver ID`, `Abbreviation`, `Full Name`, and `Team` are all redundant with `drivers_data` — the Silver fact table should carry only `Driver ID` (FK) plus `Points` and `Position`.

**Cross-entity relationships**:
- `Driver ID` → `drivers_data.Driver ID`
- `Abbreviation` consistent with all driver entities

---

### 2.8 qualifying_results

**Source**: `datasource/qualifying_results.csv` | **Rows**: 20

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Abbreviation | StringType | 0 | 20 | 3-char; all unique; no nulls |
| Driver ID | StringType | 1 | 19 + null | **BOR's Driver ID is null** — referential integrity break |
| Full Name | StringType | 0 | 20 | |
| Team | StringType | 0 | 10 | |
| Q1 Time | StringType | 1 | 19 | Pandas timedelta string; 1 null (BOR) |
| Q2 Time | StringType | 6 | 14 | Null for 5 Q1-eliminated drivers + BOR |
| Q3 Time | StringType | 11 | 9 | Null for 10 non-top-10 drivers — structurally expected |
| Position | DoubleType | 1 | 19 | 1 null (BOR); should be IntegerType |

**Candidate PK**: `Abbreviation` (20 unique, 0 nulls). `Driver ID` is disqualified as PK due to BOR null.

**DQ Issues**:
- **BOR's `Driver ID` is null** — must be corrected to `bortoleto`. This is the single most impactful data fix required before Silver layer joins can proceed.
- Q2 and Q3 nulls are structurally expected (elimination format) — must be documented and preserved as meaningful nulls, not treated as missing data.
- Q1 null for BOR — unclear if BOR did not participate or if this is an extraction artifact alongside the null `Driver ID`.
- All time columns (`Q1 Time`, `Q2 Time`, `Q3 Time`) use pandas timedelta string format.
- `Position` stored as float64.
- No `Round` column — must be injected at ingest.

**Cross-entity relationships**:
- `Abbreviation` → all driver entities
- `Driver ID` → `drivers_data.Driver ID` (broken for BOR until fixed)
- `Q1 Time`/`Q2 Time`/`Q3 Time` are the authoritative qualifying times — do not use the null `Q1/Q2/Q3` columns in `race_results`/`sprint_results`

---

### 2.9 status_data

**Source**: `datasource/status_data.csv` | **Rows**: 20

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Abbreviation | StringType | 0 | 20 | 3-char; all unique |
| Full Name | StringType | 0 | 20 | |
| Status | StringType | 0 | 2 | `"Finished"` (17) or `"Retired"` (3: HAM, LEC, BOR) |

**Candidate PK**: `Abbreviation`.

**DQ Issues**:
- Only 2 distinct `Status` values in this sample. Production F1 data includes many more: `"+1 Lap"`, `"Engine"`, `"Collision"`, `"Disqualified"`, etc. The status enum must be expanded and documented before Silver modelling.
- No `Driver ID` column — joins to driver slug entities require a lookup via `Abbreviation` or `Full Name`.
- No temporal/race context — this is a per-race snapshot without `Round`.

**Cross-entity relationships**:
- `Abbreviation` → all driver entities
- `Status` is consistent with `race_results.Status` and `constructor_results.Status`
- BOR `"Retired"` is consistent with null times in `qualifying_results` and `race_results`

---

### 2.10 constructors_data

**Source**: `datasource/constructors_data.csv` | **Rows**: 10

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Team Name | StringType | 0 | 10 | All unique |

**Candidate PK**: `Team Name`.

**DQ Issues**:
- **Single-column table with no surrogate key and no `TeamId` slug**. The string team name is the only identifier — vulnerable to spelling drift across entities (e.g. `"Haas F1 Team"` vs `"Haas"`).
- No `TeamId` slug column (present in `race_results.TeamId`) — a `team_id` slug should be added or the slug from `race_results` should be used as the canonical identifier.
- This dimension table is severely underspecified — it needs at minimum: `TeamId` (slug), `Team Name`, `TeamColor`, `Nationality`, and `Principal`.

**Cross-entity relationships**:
- `Team Name` → `Team` column in all driver and constructor entities (exact string match required)
- `TeamId` slug (from `race_results`) is a richer identifier — should be added to this dimension

---

### 2.11 constructor_standings

**Source**: `datasource/constructor_standings.csv` | **Rows**: 20

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Driver ID | StringType | 0 | 20 | Slug |
| Team | StringType | 0 | 10 | |
| Full Name | StringType | 0 | 20 | |
| Position | DoubleType | 0 | 20 | Float; should be IntegerType |
| Points | DoubleType | 0 | 8 | Float; should be IntegerType |
| Status | StringType | 0 | 2 | `"Finished"` or `"Retired"` |
| Time | StringType | 3 | 18 | Pandas timedelta string; winner = race duration, others = gap; 3 nulls (retired) |

**Candidate PK**: `Driver ID`.

**DQ Issues**:
- **This file is byte-for-byte identical to `constructor_results.csv`**. One file must be designated the authoritative source; the other must be retired or populated with semantically distinct data (e.g. constructor-level aggregated standings vs individual race results).
- `Position` and `Points` stored as float64.
- `Time` null for 3 retired drivers — expected, but must be handled.
- `Time` pandas timedelta string with semantic duality.
- No `Round` column.
- This appears to contain **driver-level** race results data, not constructor-level standings. The entity naming is misleading.

**Cross-entity relationships**:
- `Team` → `constructors_data.Team Name`
- `Driver ID` → `drivers_data.Driver ID`

---

### 2.12 constructor_results

**Source**: `datasource/constructor_results.csv` | **Rows**: 20

**Identical to `constructor_standings.csv`** — same schema, same data, byte-for-byte. All observations from section 2.11 apply.

**Action required**: Determine the intended semantic distinction between these two files. If one is meant to carry per-race driver results and the other constructor-aggregated championship points, the source extraction must be corrected.

---

### 2.13 lap_times

**Source**: `datasource/lap_times.csv` | **Rows**: 1,251

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Driver | StringType | 0 | 20 | 3-letter uppercase abbreviation (e.g. `VER`, `NOR`) |
| Lap Number | DoubleType | 0 | ~71 | Integer lap number stored as float |
| Lap Time | StringType | 0 | ~1,251 | Pandas timedelta string; high cardinality |
| Position | DoubleType | 0 | 20 | Track position per lap; should be IntegerType |
| Time | StringType | 0 | ~1,251 | Cumulative elapsed time at end of lap; timedelta string |
| Sector 1 | StringType | ~20 | ~1,231 | Timedelta string; null on lap 1 for each driver (expected — no sector 1 crossing on lap 1) |
| Sector 2 | StringType | 0 | ~1,251 | |
| Sector 3 | StringType | 0 | ~1,251 | |

**Candidate PK**: Composite `(Driver, Lap Number)`.

**DQ Issues**:
- All time columns (`Lap Time`, `Time`, `Sector 1`, `Sector 2`, `Sector 3`) use pandas timedelta string format — requires custom UDF.
- **Driver identifier uses 3-letter abbreviations** — incompatible with the slug format used in result/standings entities. A mapping table (`Abbreviation` → `Driver ID`) is required.
- `Lap Number` and `Position` stored as float64.
- `Sector 1` null on lap 1 for all 20 drivers — structurally expected domain behaviour, not a data quality failure.
- Lap 1 times are outliers (formation lap + standing start) — should be flagged in analysis.
- No `Round` column.

**Cross-entity relationships**:
- `Driver` (3-letter) → `pit_stops.Driver` (same format — joins directly)
- `Driver` (3-letter) → `drivers_data.Abbreviation` (requires mapping to slug for other joins)
- Composite `(Driver, Lap Number)` → `pit_stops.(Driver, Lap Number)`

---

### 2.14 pit_stops

**Source**: `datasource/pit_stops.csv` | **Rows**: 39

| Column | Inferred Type | Nulls | Distinct | Notes |
|--------|--------------|-------|----------|-------|
| Driver | StringType | 0 | 20 | 3-letter uppercase abbreviation |
| Lap Number | DoubleType | 0 | 35 | Float; should be IntegerType |
| Pit Out Time | StringType | 0 | 39 | Pandas timedelta string; time driver exits pit lane |
| Pit In Time | StringType | 39 | 0 | **ENTIRELY NULL** — 100% null rate; data collection failure |

**Candidate PK**: Composite `(Driver, Lap Number)`.

**DQ Issues**:
- `Pit In Time` is 100% null — it is **impossible to compute pit stop duration** without this field. This is a data collection failure that must be resolved at source before any pit stop analysis is possible.
- `Pit Out Time` uses pandas timedelta string format.
- `Lap Number` stored as float64.
- `Driver` uses 3-letter abbreviations — same mapping gap as `lap_times`.
- No `Round` column.

**Cross-entity relationships**:
- `(Driver, Lap Number)` → `lap_times.(Driver, Lap Number)` (direct join, same abbreviation format)
- `Driver` abbreviation → `drivers_data.Abbreviation` (mapping to slug needed for broader joins)

---

## 3. Primary Key Decisions

This table defines the canonical primary key for each entity. These decisions are the critical path dependency for all Silver layer modelling.

| Entity | Recommended PK | Type | Notes |
|--------|---------------|------|-------|
| races_data | `Round` | LongType | Unique, stable; season-scoped |
| season_data | Retire — consolidate into `races_data` | — | Subset of races_data |
| race_results | `(Round, DriverId)` | Composite | `Round` must be injected at ingest |
| sprint_results | `(Round, DriverId)` | Composite | `Round` must be injected at ingest |
| circuit_info | `(event, session_name)` | Composite | After flattening nested sessions |
| drivers_data | `Driver ID` (slug) | StringType | Canonical driver identifier; used as FK everywhere |
| driver_standings | `(Round, Driver ID)` | Composite | `Round` must be injected; currently a snapshot |
| qualifying_results | `(Round, Abbreviation)` | Composite | `Driver ID` disqualified due to BOR null; fix BOR before switching to `(Round, Driver ID)` |
| status_data | `(Round, Abbreviation)` | Composite | `Round` must be injected |
| constructors_data | `Team Name` | StringType | Fragile — add `TeamId` slug as surrogate |
| constructor_standings | Resolve duplicate with `constructor_results` first | — | Both files identical |
| constructor_results | `(Round, Driver ID)` | Composite | Designate as authoritative; `Round` must be injected |
| lap_times | `(Round, Driver, Lap Number)` | Composite | `Round` injected; `Driver` = 3-letter abbreviation |
| pit_stops | `(Round, Driver, Lap Number)` | Composite | `Round` injected; `Driver` = 3-letter abbreviation |

**Driver identifier mapping table** (required before any cross-entity join involving `lap_times` or `pit_stops`):

| Source column | Target column | Source entities | Mapping via |
|--------------|--------------|-----------------|-------------|
| `Driver` (3-letter) | `Driver ID` (slug) | `lap_times`, `pit_stops` | `drivers_data.Abbreviation` → `drivers_data.Driver ID` |

---

## 4. Data Quality Issues Priority Matrix

### Critical — Block Silver Layer Until Resolved

| ID | Issue | Affected Entities | Resolution |
|----|-------|------------------|------------|
| DQ-01 | `CountryCode` 100% null — extraction bug | `race_results`, `sprint_results` | Fix upstream extraction pipeline; drop column until fixed |
| DQ-02 | `Q1/Q2/Q3` 100% null in results — qualifying not joined | `race_results`, `sprint_results` | Remove columns; source from `qualifying_results` in Silver join |
| DQ-03 | `Pit In Time` 100% null — collection failure | `pit_stops` | Fix data collection; no pit stop duration analytics possible until resolved |
| DQ-04 | `constructor_standings` = `constructor_results` byte-for-byte | Both | Designate one as authoritative; re-extract the other with correct semantic data |
| DQ-05 | No `Round` column on any result/standings file | `race_results`, `sprint_results`, `qualifying_results`, `constructor_results`, `constructor_standings`, `driver_standings`, `status_data` | Inject `Round` as pipeline parameter at ingest time |

### High — Fix Before Silver Layer Joins

| ID | Issue | Affected Entities | Resolution |
|----|-------|------------------|------------|
| DQ-06 | BOR's `Driver ID` null in `qualifying_results` | `qualifying_results` | Set `bortoleto` for BOR; add Bronze-layer data fix step |
| DQ-07 | Driver identifier split: slug vs 3-letter abbreviation | `lap_times`, `pit_stops` vs all others | Build `dim_driver_mapping` from `drivers_data`; apply at Silver join |
| DQ-08 | `races_data` and `season_data` are near-duplicates | Both | Retire `season_data`; source `dim_season_calendar` from `races_data` |
| DQ-09 | Pandas timedelta string format on all time/duration columns | `race_results`, `sprint_results`, `qualifying_results`, `constructor_results`, `constructor_standings`, `lap_times`, `pit_stops` | Write shared timedelta UDF; handle both `HH:MM:SS.ffffff` and `HH:MM:SS` variants |
| DQ-10 | `constructors_data` lacks `TeamId` slug and is severely underspecified | `constructors_data` | Enrich with `TeamId`, `TeamColor`, `Nationality` from `race_results.TeamId/TeamColor` |
| DQ-11 | All data represents a single race weekend — no multi-race history | All | Design ingestion pipeline to accept and accumulate multiple rounds |

### Medium — Fix in Silver Transforms

| ID | Issue | Affected Entities | Resolution |
|----|-------|------------------|------------|
| DQ-12 | All logical integers stored as float64 | 9 entities | Explicit `cast(IntegerType)` in Silver schema enforcement |
| DQ-13 | Datetime strings not normalised to UTC | `races_data`, `season_data`, `circuit_info` | Cast all datetime strings to `TimestampType` with explicit UTC conversion |
| DQ-14 | `ClassifiedPosition` is mixed type (digit strings + `"R"`) | `race_results`, `sprint_results` | Split into `FinishPosition` (IntegerType, nullable) and `IsClassified` (BooleanType) |
| DQ-15 | `Ocon Time` in `sprint_results` lacks microseconds | `sprint_results` | Timedelta UDF must handle both `HH:MM:SS.ffffff` and `HH:MM:SS` |
| DQ-16 | `Official Event Name` and venue names contain non-ASCII characters | `races_data`, `circuit_info` | Enforce UTF-8 encoding at CSV/JSON read; verify collation in Delta tables |
| DQ-17 | `Full Name` redundant in multiple entities | `drivers_data`, `driver_standings`, `qualifying_results`, `status_data` | Drop from Silver fact tables; retain only in dimension `drivers_data` |
| DQ-18 | `Sector 1` null on lap 1 for all drivers | `lap_times` | Document as structurally expected; do not treat as data error |
| DQ-19 | `HeadshotUrl` null for Colapinto | `race_results`, `sprint_results` | Acceptable as optional metadata; handle gracefully in downstream |
| DQ-20 | `status_data` Status enum incomplete (only 2 values in sample) | `status_data` | Document expected enum expansion; validate in Silver |

---

## 5. Recommended Next Steps

### Gate 1 Approval Checklist

The following items must be resolved or formally accepted before proceeding to the Bronze → Silver transformation sprint.

**Blockers (must resolve before Gate 1 sign-off):**

- [ ] **DQ-04**: Determine the intended semantic difference between `constructor_standings` and `constructor_results`. Re-extract one or both files with correct content.
- [ ] **DQ-05**: Confirm the pipeline strategy for injecting `Round` into result files. Recommend: pass `round_number` as a Databricks job parameter and write it as a literal column during Bronze ingest.
- [ ] **DQ-01 / DQ-02 / DQ-03**: Triage whether null-column files (`CountryCode`, `Q1/Q2/Q3`, `Pit In Time`) should be re-extracted now or deferred. Define a sentinel value or documentation note for each.
- [ ] **DQ-08**: Formally retire `season_data` as a source file or document why it should be kept.

**Pre-Silver implementation tasks:**

1. **Build `dim_driver_mapping`**: A Bronze-layer lookup table derived from `drivers_data` mapping `Abbreviation` (3-letter) to `Driver ID` (slug). This unlocks joins from `lap_times` and `pit_stops` to all other entities.
2. **Write shared timedelta UDF**: A PySpark UDF to parse `"0 days HH:MM:SS[.ffffff]"` strings into `DurationType` (seconds as DoubleType) and a parallel `TimestampType` representation for time-of-race values. The UDF must handle both formats (with and without microseconds).
3. **Fix BOR `Driver ID`** (DQ-06): Add a data fix step in the Bronze profiling/cleansing notebook that sets `bortoleto` for BOR in `qualifying_results`.
4. **Enrich `constructors_data`** (DQ-10): Join `constructors_data` with the `TeamId` and `TeamColor` fields from `race_results` to create a richer `dim_constructor` dimension.
5. **Schema enforcement layer**: Define explicit Silver schemas (all integers as IntegerType, all datetimes as TimestampType UTC) and enforce them via Delta table `COMMENT` and column constraints.

**Architecture decisions to document in ADR:**

- ADR-001: Whether `races_data` is the authoritative season calendar source (retire `season_data`).
- ADR-002: How `Round` is injected into result files — job parameter vs filename metadata vs manifest file.
- ADR-003: Whether `race_results` and `sprint_results` are stored as a unified `fact_session_results` table (with `session_type` discriminator) or as separate tables.
- ADR-004: The canonical driver identifier — slug (`Driver ID`) preferred; 3-letter abbreviation retained as a secondary key for `lap_times`/`pit_stops` compatibility.
- ADR-005: Strategy for `circuit_info.json` — one file per event (current pattern) or a consolidated multi-event JSON array.
