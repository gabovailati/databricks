# Data Quality Rules Catalogue — Formula 1 Data Platform

**Sprint**: Issue #3 — Data Quality Rules Catalogue
**Date**: 2026-05-06
**Gate**: Gate 3 Input — Awaiting Human Approval Before Enriched Layer Build
**Prepared by**: Analytics Engineer Agent
**Status**: DRAFT — Pending Gate 3 Approval

---

## Table of Contents

1. [Global Rules](#1-global-rules)
2. [Entity-Specific Rules](#2-entity-specific-rules)
   - 2.1 [races_data (RD)](#21-races_data-rd)
   - 2.2 [race_results (RR)](#22-race_results-rr)
   - 2.3 [sprint_results (SR)](#23-sprint_results-sr)
   - 2.4 [circuit_info (CI)](#24-circuit_info-ci)
   - 2.5 [drivers_data (DR)](#25-drivers_data-dr)
   - 2.6 [driver_standings (DS)](#26-driver_standings-ds)
   - 2.7 [qualifying_results (QR)](#27-qualifying_results-qr)
   - 2.8 [status_data (ST)](#28-status_data-st)
   - 2.9 [constructors_data (CD)](#29-constructors_data-cd)
   - 2.10 [constructor_results (CR)](#210-constructor_results-cr)
   - 2.11 [lap_times (LT)](#211-lap_times-lt)
   - 2.12 [pit_stops (PS)](#212-pit_stops-ps)
3. [Quarantine Rejection Reason Registry](#3-quarantine-rejection-reason-registry)
4. [Structural Expectations — Not DQ Failures](#4-structural-expectations--not-dq-failures)
5. [Rules Ordering and Execution Notes](#5-rules-ordering-and-execution-notes)
6. [Open Questions — Pre-Gate-3 Decisions](#6-open-questions--pre-gate-3-decisions)

---

## Notes on Retired Entities

**`season_data`**: Retired — byte-for-byte column subset of `races_data`. No DQ rules defined. Source all season calendar data from `races_data` (DQ-08 resolution).

**`constructor_standings`**: Retired in favour of `constructor_results` as the authoritative per-race driver result entity (DQ-04 resolution). No DQ rules defined; see Section 6 for open question on semantic redesign.

---

## 1. Global Rules

Global rules apply to **every entity** and must execute **before** any entity-specific rules. Violations at this stage prevent further processing of the affected row.

### GLOBAL-01 — Sentinel String Replacement

**Source DQ issue**: DQ-06b

**Description**: Pandas serialises Python `None`, `float('nan')`, and `pd.NaT` as the verbatim string literals `"nan"`, `"None"`, and `"NaT"` when writing CSV. Spark `read_files` treats these as non-null strings, causing all downstream `IS NULL` checks and FK join filters to silently pass invalid data through as valid.

**Rule**: Before any DQ check or join runs, scan every `StringType` column in every entity and replace the following sentinel literals with SQL `NULL`:

| Sentinel literal | Replacement |
|-----------------|-------------|
| `"nan"` | `NULL` |
| `"None"` | `NULL` |
| `"NaT"` | `NULL` |

**Implementation predicate** (applied per column `c`):
```sql
CASE WHEN c IN ('nan', 'None', 'NaT') THEN NULL ELSE c END
```

**Scope**: All 12 active entities, all `StringType` columns.

**Severity**: CRITICAL

**Quarantine Rejection Reason**: N/A — this is a transformation step, not a quarantine trigger. Sentinel replacement is silent and always succeeds. The corrected value (`NULL`) then flows into downstream DQ checks which may quarantine the row on their own terms (e.g. `NULL_PRIMARY_KEY`).

---

### GLOBAL-02 — Round Number Injection

**Source DQ issue**: DQ-05, DQ-11

**Description**: No result or standings CSV file carries a `Round` column. To support multi-race history, `round_number` must be injected at pipeline ingest time from a Databricks job parameter. Any entity in the result/standings group that reaches the Enriched layer without a non-null `round_number` must be quarantined in its entirety.

**Affected entities**: `race_results`, `sprint_results`, `qualifying_results`, `driver_standings`, `status_data`, `constructor_results`, `lap_times`, `pit_stops`

**Rule**: After `round_number` is injected as a literal column from the pipeline parameter, assert:
```sql
round_number IS NOT NULL AND round_number >= 0 AND round_number <= 24
```

**Severity**: CRITICAL

**Quarantine Rejection Reason**: `MISSING_ROUND_NUMBER`

**Notes**: `round_number = 0` is valid — it represents Pre-Season Testing (Round 0 in `races_data`). The upper bound of 24 reflects the 2025 season calendar; this value must be updated each season via pipeline configuration.

---

## 2. Entity-Specific Rules

### 2.1 races_data (RD)

**Primary Key**: `Round` (LongType, unique, 0 nulls)
**Source**: `datasource/races_data.csv` | Rows: 25

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| RD-01 | Round not null | `Round` | NULLABILITY | `Round IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK column; any null means the row cannot be placed in the season calendar |
| RD-02 | Round within season range | `Round` | RANGE_DOMAIN | `Round < 0 OR Round > 24` | CRITICAL | `ROUND_OUT_OF_RANGE` | 2025 season: 0 (Pre-Season Testing) through 24. Update upper bound each season |
| RD-03 | Event Name not null | `Event Name` | NULLABILITY | `Event Name IS NULL` | HIGH | `NULL_EVENT_NAME` | Human-readable calendar key; required for all downstream event resolution |
| RD-04 | Official Event Name not null | `Official Event Name` | NULLABILITY | `Official Event Name IS NULL` | HIGH | `NULL_OFFICIAL_EVENT_NAME` | Cross-entity FK target: `circuit_info.event` joins on this value |
| RD-05 | First Session datetime format | `First Session` | FORMAT | `First Session IS NULL OR NOT (First Session RLIKE '^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[+-]\\d{2}:\\d{2}$')` | HIGH | `INVALID_DATETIME_FORMAT` | Must match tz-aware format `YYYY-MM-DD HH:MM:SS±HH:MM` to cast to UTC TimestampType |
| RD-06 | Duplicate Round | `Round` | DEDUPLICATION | `COUNT(*) > 1` grouped by `Round` within the batch | CRITICAL | `DUPLICATE_ROUND` | PK uniqueness; 25 distinct Rounds expected for the full 2025 season |
| RD-07 | Non-ASCII encoding check | `Official Event Name`, `Location` | FORMAT | `Official Event Name IS NULL OR Location IS NULL` (encoding validated at read time via `charset=utf-8` reader option) | MEDIUM | `ENCODING_ERROR` | DQ-16: non-ASCII characters (accented venue/event names) must be read with UTF-8 encoding enforced at `read_files`; if characters are garbled, quarantine |

---

### 2.2 race_results (RR)

**Primary Key**: `(round_number, DriverId)` — composite; `round_number` injected via GLOBAL-02
**Source**: `datasource/race_results.csv` | Rows: 20 per race

**Pre-processing required**: Drop columns `CountryCode`, `Q1`, `Q2`, `Q3` before loading — these are 100% null extraction artefacts (DQ-01, DQ-02). Do not evaluate DQ rules on dropped columns.

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| RR-01 | DriverId not null | `DriverId` | NULLABILITY | `DriverId IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | After GLOBAL-01 sentinel replacement; null DriverId means row cannot be keyed |
| RR-02 | DriverNumber not null | `DriverNumber` | NULLABILITY | `DriverNumber IS NULL` | HIGH | `NULL_DRIVER_NUMBER` | Required for joining to `lap_times`/`pit_stops` via number cross-reference |
| RR-03 | TeamId not null | `TeamId` | NULLABILITY | `TeamId IS NULL` | HIGH | `NULL_TEAM_ID` | FK to constructor dimension; null breaks constructor lineage |
| RR-04 | Duplicate primary key | `(round_number, DriverId)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(round_number, DriverId)` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | One row per driver per race expected |
| RR-05 | Position range | `Position` | RANGE_DOMAIN | `CAST(Position AS INT) < 1 OR CAST(Position AS INT) > 20` | HIGH | `POSITION_OUT_OF_RANGE` | DQ-12: cast from float64 first; valid range 1–20 for a 20-car grid |
| RR-06 | GridPosition range | `GridPosition` | RANGE_DOMAIN | `CAST(GridPosition AS INT) < 0 OR CAST(GridPosition AS INT) > 20` | HIGH | `GRID_POSITION_OUT_OF_RANGE` | 0 is valid for pit-lane starts; upper bound is 20 for a full 2025 grid |
| RR-07 | Points non-negative | `Points` | RANGE_DOMAIN | `CAST(Points AS INT) < 0` | HIGH | `NEGATIVE_POINTS` | Championship points cannot be negative |
| RR-08 | Laps non-negative | `Laps` | RANGE_DOMAIN | `CAST(Laps AS INT) < 0` | HIGH | `NEGATIVE_LAPS` | |
| RR-09 | ClassifiedPosition format | `ClassifiedPosition` | TYPE_VALIDITY | `ClassifiedPosition IS NULL OR NOT (ClassifiedPosition RLIKE '^([0-9]{1,2}|R|D|E|NC|W|F|S)$')` | HIGH | `INVALID_CLASSIFIED_POSITION` | DQ-14: valid values are digit strings `"1"`–`"20"`, `"R"` (Retired), `"D"` (Disqualified), `"E"` (Excluded), `"NC"` (Not Classified), `"W"` (Withdrew), `"F"` (Failed to qualify), `"S"` (Sportsmanship). Extend as needed |
| RR-10 | Time format when not null | `Time` | FORMAT | `Time IS NOT NULL AND NOT (Time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | DQ-09: must match pandas timedelta format `"0 days HH:MM:SS"` or `"0 days HH:MM:SS.ffffff"`. Null is valid for retired drivers — see Section 4 |
| RR-11 | Status valid enum | `Status` | RANGE_DOMAIN | `Status IS NULL OR Status NOT IN ('Finished', 'Retired', '+1 Lap', '+2 Laps', 'Engine', 'Collision', 'Gearbox', 'Hydraulics', 'Accident', 'Disqualified', 'Mechanical', 'Power Unit', 'Electrical', 'Tyres', 'Suspension')` | MEDIUM | `INVALID_STATUS_VALUE` | DQ-20: enum must be expanded as more races are loaded; initial known values listed. Extend this list as new values are observed |
| RR-12 | HeadshotUrl sentinel | `HeadshotUrl` | SENTINEL_DETECTION | `HeadshotUrl = 'None'` | MEDIUM | `SENTINEL_HEADSHOT_URL` | DQ-06b / DQ-19: Colapinto's HeadshotUrl is the string `"None"` — GLOBAL-01 must replace it with NULL before this check, making this rule a backstop. If `"None"` reaches this rule, GLOBAL-01 failed |
| RR-13 | TeamColor format | `TeamColor` | FORMAT | `TeamColor IS NOT NULL AND NOT (TeamColor RLIKE '^[0-9A-Fa-f]{6}$')` | MEDIUM | `INVALID_TEAM_COLOR_FORMAT` | Hex colour without `#` prefix; must be exactly 6 hex characters |
| RR-14 | DriverId FK to drivers_data | `DriverId` | REFERENTIAL_INTEGRITY | `DriverId NOT IN (SELECT driver_id FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | Cross-entity join; BOR's ID must be corrected by GLOBAL-01 before this check |
| RR-15 | TeamId FK to constructors_data | `TeamId` | REFERENTIAL_INTEGRITY | `TeamId NOT IN (SELECT team_id FROM enriched.constructors_data)` | HIGH | `UNRESOLVED_TEAM_FK` | Requires `constructors_data` to carry `team_id` slug (DQ-10); run after constructors_data is enriched |

---

### 2.3 sprint_results (SR)

**Primary Key**: `(round_number, DriverId)` — composite; `round_number` injected via GLOBAL-02
**Source**: `datasource/sprint_results.csv` | Rows: 20 per sprint

**Pre-processing required**: Drop columns `CountryCode`, `Q1`, `Q2`, `Q3` — same extraction artefacts as `race_results` (DQ-01, DQ-02).

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| SR-01 | DriverId not null | `DriverId` | NULLABILITY | `DriverId IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | After GLOBAL-01 sentinel replacement |
| SR-02 | DriverNumber not null | `DriverNumber` | NULLABILITY | `DriverNumber IS NULL` | HIGH | `NULL_DRIVER_NUMBER` | |
| SR-03 | TeamId not null | `TeamId` | NULLABILITY | `TeamId IS NULL` | HIGH | `NULL_TEAM_ID` | |
| SR-04 | Duplicate primary key | `(round_number, DriverId)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(round_number, DriverId)` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | One row per driver per sprint event |
| SR-05 | Position range | `Position` | RANGE_DOMAIN | `CAST(Position AS INT) < 1 OR CAST(Position AS INT) > 20` | HIGH | `POSITION_OUT_OF_RANGE` | |
| SR-06 | GridPosition range | `GridPosition` | RANGE_DOMAIN | `CAST(GridPosition AS INT) < 0 OR CAST(GridPosition AS INT) > 20` | HIGH | `GRID_POSITION_OUT_OF_RANGE` | |
| SR-07 | Points non-negative | `Points` | RANGE_DOMAIN | `CAST(Points AS INT) < 0` | HIGH | `NEGATIVE_POINTS` | |
| SR-08 | Laps non-negative | `Laps` | RANGE_DOMAIN | `CAST(Laps AS INT) < 0` | HIGH | `NEGATIVE_LAPS` | Sprint races have fewer laps (24 in sample) — no fixed upper bound rule; non-negative suffices |
| SR-09 | ClassifiedPosition format | `ClassifiedPosition` | TYPE_VALIDITY | `ClassifiedPosition IS NULL OR NOT (ClassifiedPosition RLIKE '^([0-9]{1,2}|R|D|E|NC|W|F|S)$')` | HIGH | `INVALID_CLASSIFIED_POSITION` | DQ-14: same mixed-type issue as `race_results` |
| SR-10 | Time format when not null | `Time` | FORMAT | `Time IS NOT NULL AND NOT (Time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | DQ-09 / DQ-15: timedelta UDF must handle both `HH:MM:SS.ffffff` and `HH:MM:SS` (Ocon's format in the sample) |
| SR-11 | Status valid enum | `Status` | RANGE_DOMAIN | `Status IS NULL OR Status NOT IN ('Finished', 'Retired', '+1 Lap', '+2 Laps', 'Engine', 'Collision', 'Gearbox', 'Hydraulics', 'Accident', 'Disqualified', 'Mechanical', 'Power Unit', 'Electrical', 'Tyres', 'Suspension')` | MEDIUM | `INVALID_STATUS_VALUE` | |
| SR-12 | HeadshotUrl sentinel | `HeadshotUrl` | SENTINEL_DETECTION | `HeadshotUrl = 'None'` | MEDIUM | `SENTINEL_HEADSHOT_URL` | Backstop for GLOBAL-01; same as RR-12 |
| SR-13 | TeamColor format | `TeamColor` | FORMAT | `TeamColor IS NOT NULL AND NOT (TeamColor RLIKE '^[0-9A-Fa-f]{6}$')` | MEDIUM | `INVALID_TEAM_COLOR_FORMAT` | |
| SR-14 | DriverId FK to drivers_data | `DriverId` | REFERENTIAL_INTEGRITY | `DriverId NOT IN (SELECT driver_id FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | |
| SR-15 | TeamId FK to constructors_data | `TeamId` | REFERENTIAL_INTEGRITY | `TeamId NOT IN (SELECT team_id FROM enriched.constructors_data)` | HIGH | `UNRESOLVED_TEAM_FK` | |

---

### 2.4 circuit_info (CI)

**Primary Key**: Composite `(event, session_name)` — after flattening nested `sessions` JSON structure
**Source**: `datasource/circuit_info.json` | Structure: Single document; 5 sessions nested under `sessions` keys

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| CI-01 | event not null | `event` | NULLABILITY | `event IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK component; FK to `races_data.Official Event Name` |
| CI-02 | session_name not null | `session_name` | NULLABILITY | `session_name IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK component; derived from `sessions.SessionN.name` during flattening |
| CI-03 | event FK to races_data | `event` | REFERENTIAL_INTEGRITY | `event NOT IN (SELECT official_event_name FROM enriched.races_data)` | HIGH | `UNRESOLVED_EVENT_FK` | DQ-16: non-ASCII characters must be preserved and matched with UTF-8 collation |
| CI-04 | session_name valid enum | `session_name` | RANGE_DOMAIN | `session_name NOT IN ('Practice 1', 'Practice 2', 'Practice 3', 'Sprint Qualifying', 'Sprint', 'Qualifying', 'Race')` | HIGH | `INVALID_SESSION_NAME` | Enum documented from observed values in sample; extend as more formats are observed |
| CI-05 | session date format | `date` | FORMAT | `date IS NULL OR NOT (date RLIKE '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[+-]\\d{2}:\\d{2}$')` | HIGH | `INVALID_DATETIME_FORMAT` | DQ-13: tz-aware local time format; must be cast to UTC TimestampType in transform |
| CI-06 | session utc format | `utc` | FORMAT | `utc IS NULL OR NOT (utc RLIKE '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$')` | HIGH | `INVALID_DATETIME_FORMAT` | DQ-13: naive UTC string (no `Z` or `+00:00`); append `+00:00` before cast |
| CI-07 | event_date format | `event_date` | FORMAT | `event_date IS NULL OR NOT (event_date RLIKE '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$')` | MEDIUM | `INVALID_DATETIME_FORMAT` | DQ-13: naive datetime; treat as UTC midnight |
| CI-08 | format field valid enum | `format` | RANGE_DOMAIN | `format IS NULL OR format NOT IN ('conventional', 'sprint', 'sprint_qualifying')` | MEDIUM | `INVALID_EVENT_FORMAT` | Observed: `sprint_qualifying`; `conventional` and `sprint` inferred. Extend as more circuit docs are ingested |
| CI-09 | Duplicate session per event | `(event, session_name)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(event, session_name)` | CRITICAL | `DUPLICATE_SESSION` | Each session should appear exactly once per event document |
| CI-10 | Non-ASCII encoding check | `name`, `event` | FORMAT | Validated at read time via `charset=utf-8`; quarantine if characters are garbled (presence of replacement character `�`) | MEDIUM | `ENCODING_ERROR` | DQ-16: `São Paulo` contains non-ASCII characters |

---

### 2.5 drivers_data (DR)

**Primary Key**: `Driver ID` (slug, StringType)
**Source**: `datasource/drivers_data.csv` | Rows: 20

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| DR-01 | Driver ID not null | `Driver ID` | NULLABILITY | `driver_id IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | Canonical driver dimension PK; all other entities join here |
| DR-02 | Abbreviation not null | `Abbreviation` | NULLABILITY | `Abbreviation IS NULL` | CRITICAL | `NULL_ABBREVIATION` | Secondary unique key; required for `lap_times` and `pit_stops` joins |
| DR-03 | Number not null | `Number` | NULLABILITY | `Number IS NULL` | HIGH | `NULL_DRIVER_NUMBER` | Car number; required for grid and lap cross-references |
| DR-04 | Driver ID slug format | `Driver ID` | FORMAT | `NOT (driver_id RLIKE '^[a-z0-9_]+$')` | HIGH | `INVALID_DRIVER_ID_FORMAT` | Slug must be lowercase alphanumeric with underscores only (e.g. `max_verstappen`) |
| DR-05 | Abbreviation format | `Abbreviation` | FORMAT | `NOT (Abbreviation RLIKE '^[A-Z]{3}$')` | HIGH | `INVALID_ABBREVIATION_FORMAT` | Must be exactly 3 uppercase letters |
| DR-06 | Number range | `Number` | RANGE_DOMAIN | `Number < 1 OR Number > 99` | MEDIUM | `DRIVER_NUMBER_OUT_OF_RANGE` | F1 car numbers 1–99; #1 reserved for reigning WDC |
| DR-07 | First Name not null | `First Name` | NULLABILITY | `first_name IS NULL` | HIGH | `NULL_DRIVER_NAME` | Required for identity; `Full Name` is derivable from First + Last |
| DR-08 | Last Name not null | `Last Name` | NULLABILITY | `last_name IS NULL` | HIGH | `NULL_DRIVER_NAME` | |
| DR-09 | Team not null | `Team` | NULLABILITY | `Team IS NULL` | HIGH | `NULL_TEAM_REFERENCE` | Team string must match `constructors_data.Team Name` |
| DR-10 | Team FK to constructors_data | `Team` | REFERENTIAL_INTEGRITY | `Team NOT IN (SELECT team_name FROM enriched.constructors_data)` | HIGH | `UNRESOLVED_TEAM_FK` | Exact string match required; spelling drift across entities is a known risk (DQ-10) |
| DR-11 | Duplicate Driver ID | `Driver ID` | DEDUPLICATION | `COUNT(*) > 1` grouped by `driver_id` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | Dimension table must have one row per driver |
| DR-12 | Duplicate Abbreviation | `Abbreviation` | DEDUPLICATION | `COUNT(*) > 1` grouped by `Abbreviation` | CRITICAL | `DUPLICATE_ABBREVIATION` | Abbreviation is a secondary unique key used as join target |

---

### 2.6 driver_standings (DS)

**Primary Key**: `(round_number, Driver ID)` — composite; `round_number` injected via GLOBAL-02
**Source**: `datasource/driver_standings.csv` | Rows: 20

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| DS-01 | Driver ID not null | `Driver ID` | NULLABILITY | `driver_id IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | After GLOBAL-01; PK component |
| DS-02 | Position not null | `Position` | NULLABILITY | `Position IS NULL` | HIGH | `NULL_STANDINGS_POSITION` | Championship position is core data |
| DS-03 | Points not null | `Points` | NULLABILITY | `Points IS NULL` | HIGH | `NULL_STANDINGS_POINTS` | |
| DS-04 | Position range | `Position` | RANGE_DOMAIN | `CAST(Position AS INT) < 1 OR CAST(Position AS INT) > 20` | HIGH | `POSITION_OUT_OF_RANGE` | DQ-12: standings position 1–20 |
| DS-05 | Points non-negative | `Points` | RANGE_DOMAIN | `CAST(Points AS INT) < 0` | HIGH | `NEGATIVE_POINTS` | Championship points cannot be negative |
| DS-06 | Driver ID FK to drivers_data | `Driver ID` | REFERENTIAL_INTEGRITY | `driver_id NOT IN (SELECT driver_id FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | |
| DS-07 | Duplicate primary key | `(round_number, Driver ID)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(round_number, driver_id)` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | One championship position per driver per round snapshot |

---

### 2.7 qualifying_results (QR)

**Primary Key**: `(round_number, Abbreviation)` — composite; `round_number` injected via GLOBAL-02; `Driver ID` is disqualified as PK because BOR's value is the sentinel string `"nan"`
**Source**: `datasource/qualifying_results.csv` | Rows: 20

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| QR-01 | Abbreviation not null | `Abbreviation` | NULLABILITY | `Abbreviation IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK component; used because `Driver ID` can be sentinel |
| QR-02 | Driver ID sentinel detection | `Driver ID` | SENTINEL_DETECTION | `driver_id = 'nan'` | HIGH | `SENTINEL_DRIVER_ID` | DQ-06: BOR's Driver ID is the string `"nan"`. GLOBAL-01 should convert this to NULL; this rule fires as a backstop if GLOBAL-01 failed. If triggered: replace `driver_id` with `bortoleto` rather than quarantining the row — see Notes |
| QR-03 | Driver ID FK to drivers_data | `Driver ID` | REFERENTIAL_INTEGRITY | `driver_id IS NOT NULL AND driver_id NOT IN (SELECT driver_id FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | Run after GLOBAL-01 sentinel replacement and QR-02 correction |
| QR-04 | Abbreviation FK to drivers_data | `Abbreviation` | REFERENTIAL_INTEGRITY | `Abbreviation NOT IN (SELECT abbreviation FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | Alternative join path to verify identity |
| QR-05 | Position range when not null | `Position` | RANGE_DOMAIN | `Position IS NOT NULL AND (CAST(Position AS INT) < 1 OR CAST(Position AS INT) > 20)` | HIGH | `POSITION_OUT_OF_RANGE` | DQ-12: `Position` is NULL for BOR in sample; null is valid if BOR did not start |
| QR-06 | Q1 Time format when not null | `Q1 Time` | FORMAT | `Q1_time IS NOT NULL AND NOT (Q1_time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | DQ-09: pandas timedelta string; Q1 null for BOR is in Section 4 |
| QR-07 | Q2 Time format when not null | `Q2 Time` | FORMAT | `Q2_time IS NOT NULL AND NOT (Q2_time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | Q2 null for Q1-eliminated drivers is expected — see Section 4 |
| QR-08 | Q3 Time format when not null | `Q3 Time` | FORMAT | `Q3_time IS NOT NULL AND NOT (Q3_time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | Q3 null for non-top-10 drivers is expected — see Section 4 |
| QR-09 | Duplicate primary key | `(round_number, Abbreviation)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(round_number, Abbreviation)` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | One qualifying row per driver per event |

**Note on QR-02**: Rather than quarantining BOR's row, the recommended remediation is a targeted correction: after GLOBAL-01 replaces `"nan"` with NULL, a conditional `COALESCE` or `CASE` expression sets `driver_id = 'bortoleto'` where `Abbreviation = 'BOR'` and `driver_id IS NULL`. Quarantine fires only if BOR's row cannot be corrected (e.g. `Abbreviation` also null).

---

### 2.8 status_data (ST)

**Primary Key**: `(round_number, Abbreviation)` — composite; `round_number` injected via GLOBAL-02
**Source**: `datasource/status_data.csv` | Rows: 20

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| ST-01 | Abbreviation not null | `Abbreviation` | NULLABILITY | `Abbreviation IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK component; `Driver ID` not present in this entity |
| ST-02 | Status not null | `Status` | NULLABILITY | `Status IS NULL` | HIGH | `NULL_STATUS_VALUE` | Core data — cannot be absent |
| ST-03 | Status valid enum | `Status` | RANGE_DOMAIN | `Status NOT IN ('Finished', 'Retired', '+1 Lap', '+2 Laps', '+3 Laps', 'Engine', 'Collision', 'Gearbox', 'Hydraulics', 'Accident', 'Disqualified', 'Mechanical', 'Power Unit', 'Electrical', 'Tyres', 'Suspension', 'Brakes', 'Fuel System', 'Overheating', 'Wheel', 'Debris')` | MEDIUM | `INVALID_STATUS_VALUE` | DQ-20: only 2 values observed in sample (`Finished`, `Retired`). Full known F1 retirement enum listed; extend as new values appear. Violating rows are quarantined with a data-steward review flag rather than hard rejection |
| ST-04 | Abbreviation FK to drivers_data | `Abbreviation` | REFERENTIAL_INTEGRITY | `Abbreviation NOT IN (SELECT abbreviation FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | |
| ST-05 | Duplicate primary key | `(round_number, Abbreviation)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(round_number, Abbreviation)` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | One status row per driver per race |

---

### 2.9 constructors_data (CD)

**Primary Key**: `Team Name` (StringType) — fragile; `team_id` slug to be added from `race_results` (DQ-10)
**Source**: `datasource/constructors_data.csv` | Rows: 10

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| CD-01 | Team Name not null | `Team Name` | NULLABILITY | `team_name IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | Only column in current source; PK cannot be null |
| CD-02 | Team Name non-empty | `Team Name` | TYPE_VALIDITY | `LENGTH(TRIM(team_name)) = 0` | CRITICAL | `EMPTY_TEAM_NAME` | Empty string after trimming is equivalent to null |
| CD-03 | Duplicate Team Name | `Team Name` | DEDUPLICATION | `COUNT(*) > 1` grouped by `team_name` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | Dimension table; one row per constructor |
| CD-04 | team_id not null after enrichment | `team_id` | NULLABILITY | `team_id IS NULL` | HIGH | `NULL_TEAM_ID` | DQ-10: `team_id` slug must be injected from `race_results.TeamId` during Enriched transform. This rule applies after enrichment |
| CD-05 | team_id slug format | `team_id` | FORMAT | `team_id IS NOT NULL AND NOT (team_id RLIKE '^[a-z0-9_]+$')` | HIGH | `INVALID_TEAM_ID_FORMAT` | Slug must be lowercase alphanumeric with underscores |
| CD-06 | TeamColor format after enrichment | `TeamColor` | FORMAT | `TeamColor IS NOT NULL AND NOT (TeamColor RLIKE '^[0-9A-Fa-f]{6}$')` | MEDIUM | `INVALID_TEAM_COLOR_FORMAT` | Injected from `race_results.TeamColor` during enrichment |

---

### 2.10 constructor_results (CR)

**Primary Key**: `(round_number, Driver ID)` — composite; `round_number` injected via GLOBAL-02
**Source**: `datasource/constructor_results.csv` | Rows: 20 per race
**Note**: Designated as the **authoritative** entity for per-race driver-level results within a constructor context. `constructor_standings` is retired (DQ-04). See Section 6 for the outstanding semantic redesign question.

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| CR-01 | Driver ID not null | `Driver ID` | NULLABILITY | `driver_id IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | After GLOBAL-01 sentinel replacement |
| CR-02 | Team not null | `Team` | NULLABILITY | `Team IS NULL` | HIGH | `NULL_TEAM_REFERENCE` | Required for constructor FK |
| CR-03 | Position not null | `Position` | NULLABILITY | `Position IS NULL` | HIGH | `NULL_RESULT_POSITION` | |
| CR-04 | Position range | `Position` | RANGE_DOMAIN | `CAST(Position AS INT) < 1 OR CAST(Position AS INT) > 20` | HIGH | `POSITION_OUT_OF_RANGE` | DQ-12 |
| CR-05 | Points non-negative | `Points` | RANGE_DOMAIN | `CAST(Points AS INT) < 0` | HIGH | `NEGATIVE_POINTS` | |
| CR-06 | Status valid enum | `Status` | RANGE_DOMAIN | `Status IS NULL OR Status NOT IN ('Finished', 'Retired', '+1 Lap', '+2 Laps', 'Engine', 'Collision', 'Gearbox', 'Hydraulics', 'Accident', 'Disqualified', 'Mechanical', 'Power Unit', 'Electrical', 'Tyres', 'Suspension')` | MEDIUM | `INVALID_STATUS_VALUE` | |
| CR-07 | Time format when not null | `Time` | FORMAT | `Time IS NOT NULL AND NOT (Time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | DQ-09: Time null for retired drivers is expected — see Section 4 |
| CR-08 | Driver ID FK to drivers_data | `Driver ID` | REFERENTIAL_INTEGRITY | `driver_id NOT IN (SELECT driver_id FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | |
| CR-09 | Team FK to constructors_data | `Team` | REFERENTIAL_INTEGRITY | `Team NOT IN (SELECT team_name FROM enriched.constructors_data)` | HIGH | `UNRESOLVED_TEAM_FK` | Exact string match; spelling drift is a known risk |
| CR-10 | Duplicate primary key | `(round_number, Driver ID)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(round_number, driver_id)` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | One result row per driver per race |

---

### 2.11 lap_times (LT)

**Primary Key**: `(round_number, Driver, Lap Number)` — composite; `round_number` injected via GLOBAL-02; `Driver` is the 3-letter abbreviation
**Source**: `datasource/lap_times.csv` | Rows: 1,251 per race (20 drivers × ~71 laps, minus non-completions)

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| LT-01 | Driver not null | `Driver` | NULLABILITY | `Driver IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK component; 3-letter abbreviation |
| LT-02 | Lap Number not null | `Lap Number` | NULLABILITY | `Lap Number IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK component |
| LT-03 | Driver abbreviation format | `Driver` | FORMAT | `NOT (Driver RLIKE '^[A-Z]{3}$')` | HIGH | `INVALID_ABBREVIATION_FORMAT` | DQ-07: must be exactly 3 uppercase letters for mapping to `drivers_data.Abbreviation` |
| LT-04 | Lap Number range | `Lap Number` | RANGE_DOMAIN | `CAST(Lap Number AS INT) < 1 OR CAST(Lap Number AS INT) > 100` | HIGH | `LAP_NUMBER_OUT_OF_RANGE` | DQ-12: minimum lap is 1; upper bound 100 is a conservative ceiling above any real F1 race |
| LT-05 | Position range | `Position` | RANGE_DOMAIN | `CAST(Position AS INT) < 1 OR CAST(Position AS INT) > 20` | HIGH | `POSITION_OUT_OF_RANGE` | Track position per lap; must be 1–20 |
| LT-06 | Lap Time format | `Lap Time` | FORMAT | `Lap Time IS NULL OR NOT (Lap Time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | DQ-09: lap time should never be null; null is unexpected and flags a collection failure |
| LT-07 | Time (cumulative) format | `Time` | FORMAT | `Time IS NULL OR NOT (Time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | Cumulative elapsed race time; should never be null |
| LT-08 | Sector 2 format | `Sector 2` | FORMAT | `Sector 2 IS NULL OR NOT (Sector 2 RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_SECTOR_TIME_FORMAT` | Sector 2 should never be null (no structural reason) |
| LT-09 | Sector 3 format | `Sector 3` | FORMAT | `Sector 3 IS NULL OR NOT (Sector 3 RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_SECTOR_TIME_FORMAT` | Same as Sector 2 |
| LT-10 | Sector 1 format when not null | `Sector 1` | FORMAT | `Sector 1 IS NOT NULL AND NOT (Sector 1 RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_SECTOR_TIME_FORMAT` | DQ-18: Sector 1 null on Lap 1 is EXPECTED — see Section 4. Only validate format when not null |
| LT-11 | Driver FK to drivers_data | `Driver` | REFERENTIAL_INTEGRITY | `Driver NOT IN (SELECT abbreviation FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | DQ-07: join via `Abbreviation` column |
| LT-12 | Duplicate primary key | `(round_number, Driver, Lap Number)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(round_number, Driver, Lap Number)` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | One lap record per driver per lap per race |

---

### 2.12 pit_stops (PS)

**Primary Key**: `(round_number, Driver, Lap Number)` — composite; `round_number` injected via GLOBAL-02; `Driver` is the 3-letter abbreviation
**Source**: `datasource/pit_stops.csv` | Rows: 39 per race

**Critical note**: `Pit In Time` is 100% null (DQ-03 — data collection failure). The column must be retained in the schema (to signal the gap) but cannot carry any valid data. No format rule is applied to `Pit In Time`; its null state is flagged via PS-05. Pit stop duration analytics are blocked until this is resolved at source.

| Rule ID | Rule Name | Column(s) | Rule Type | Rule Logic | Severity | Quarantine Rejection Reason | Notes |
|---------|-----------|-----------|-----------|------------|----------|-----------------------------|-------|
| PS-01 | Driver not null | `Driver` | NULLABILITY | `Driver IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK component; 3-letter abbreviation |
| PS-02 | Lap Number not null | `Lap Number` | NULLABILITY | `Lap Number IS NULL` | CRITICAL | `NULL_PRIMARY_KEY` | PK component |
| PS-03 | Driver abbreviation format | `Driver` | FORMAT | `NOT (Driver RLIKE '^[A-Z]{3}$')` | HIGH | `INVALID_ABBREVIATION_FORMAT` | DQ-07: must be 3 uppercase letters |
| PS-04 | Lap Number range | `Lap Number` | RANGE_DOMAIN | `CAST(Lap Number AS INT) < 1 OR CAST(Lap Number AS INT) > 100` | HIGH | `LAP_NUMBER_OUT_OF_RANGE` | Pit stops occur only on laps 1–100 |
| PS-05 | Pit In Time collection failure flag | `Pit In Time` | NULLABILITY | `Pit In Time IS NOT NULL` | CRITICAL | `PIT_IN_TIME_UNEXPECTED_VALUE` | DQ-03: `Pit In Time` is expected to be 100% null (collection failure). If a non-null value appears it indicates a schema change or partial fix — flag for data steward review rather than quarantine |
| PS-06 | Pit Out Time not null | `Pit Out Time` | NULLABILITY | `Pit Out Time IS NULL` | HIGH | `NULL_PIT_OUT_TIME` | `Pit Out Time` has 0 nulls in sample; null here indicates a collection failure on the available field |
| PS-07 | Pit Out Time format | `Pit Out Time` | FORMAT | `Pit Out Time IS NOT NULL AND NOT (Pit Out Time RLIKE '^0 days \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$')` | HIGH | `INVALID_LAP_TIME_FORMAT` | DQ-09: pandas timedelta string format |
| PS-08 | Driver FK to drivers_data | `Driver` | REFERENTIAL_INTEGRITY | `Driver NOT IN (SELECT abbreviation FROM enriched.drivers_data)` | HIGH | `UNRESOLVED_DRIVER_FK` | DQ-07 |
| PS-09 | Driver/Lap FK to lap_times | `(Driver, Lap Number, round_number)` | REFERENTIAL_INTEGRITY | `(Driver, CAST(Lap Number AS INT), round_number) NOT IN (SELECT driver, lap_number, round_number FROM enriched.lap_times)` | HIGH | `UNRESOLVED_LAP_FK` | Every pit stop must correspond to a lap in `lap_times` |
| PS-10 | Duplicate primary key | `(round_number, Driver, Lap Number)` | DEDUPLICATION | `COUNT(*) > 1` grouped by `(round_number, Driver, Lap Number)` | CRITICAL | `DUPLICATE_PRIMARY_KEY` | One pit stop record per driver per lap per race |

---

## 3. Quarantine Rejection Reason Registry

This table is the master registry of all quarantine rejection reason constants used across the Data Quality Rules Catalogue. String constants are in `SCREAMING_SNAKE_CASE`. When a row is written to a quarantine Delta table, the `rejection_reason` column must contain exactly one of these constants.

| Rejection Reason | Description | Severity | Entities Affected |
|-----------------|-------------|----------|--------------------|
| `NULL_PRIMARY_KEY` | A column that is part of the entity primary key is null | CRITICAL | RD, RR, SR, CI, DR, DS, QR, ST, CD, CR, LT, PS |
| `DUPLICATE_PRIMARY_KEY` | Two or more rows share the same primary key value(s) within the same batch | CRITICAL | All entities |
| `MISSING_ROUND_NUMBER` | The `round_number` column was not injected by the pipeline or its value is null | CRITICAL | RR, SR, QR, DS, ST, CR, LT, PS |
| `ROUND_OUT_OF_RANGE` | `round_number` value is outside the valid 0–24 range for the 2025 season | CRITICAL | RD |
| `NULL_EVENT_NAME` | `Event Name` is null in `races_data` | HIGH | RD |
| `NULL_OFFICIAL_EVENT_NAME` | `Official Event Name` is null in `races_data` | HIGH | RD |
| `INVALID_DATETIME_FORMAT` | A datetime string does not match its expected format pattern | HIGH | RD, CI |
| `DUPLICATE_ROUND` | Two rows share the same `Round` value in `races_data` | CRITICAL | RD |
| `ENCODING_ERROR` | Non-ASCII characters could not be decoded correctly using UTF-8 | MEDIUM | RD, CI |
| `NULL_DRIVER_NUMBER` | `DriverNumber` is null in a result entity | HIGH | RR, SR, DR |
| `NULL_TEAM_ID` | `TeamId` is null in a result entity | HIGH | RR, SR, CD |
| `POSITION_OUT_OF_RANGE` | A finishing, grid, or standings position is outside the valid range (1–20) | HIGH | RR, SR, DS, CR, LT |
| `GRID_POSITION_OUT_OF_RANGE` | `GridPosition` is outside the valid range (0–20) | HIGH | RR, SR |
| `NEGATIVE_POINTS` | `Points` value is negative | HIGH | RR, SR, DS, CR |
| `NEGATIVE_LAPS` | `Laps` value is negative | HIGH | RR, SR |
| `INVALID_CLASSIFIED_POSITION` | `ClassifiedPosition` contains a value not in the valid mixed-type enum | HIGH | RR, SR |
| `INVALID_LAP_TIME_FORMAT` | A time or duration string does not match the pandas timedelta pattern | HIGH | RR, SR, QR, CR, LT, PS |
| `INVALID_SECTOR_TIME_FORMAT` | A sector time string does not match the expected pandas timedelta pattern | HIGH | LT |
| `INVALID_STATUS_VALUE` | `Status` contains a value not in the known enum | MEDIUM | RR, SR, ST, CR |
| `SENTINEL_HEADSHOT_URL` | `HeadshotUrl` contains the sentinel string `"None"` after GLOBAL-01 replacement (backstop rule) | MEDIUM | RR, SR |
| `INVALID_TEAM_COLOR_FORMAT` | `TeamColor` is not a 6-character hexadecimal string | MEDIUM | RR, SR, CD |
| `UNRESOLVED_DRIVER_FK` | `DriverId` / `Driver ID` / `Driver` (abbreviation) does not match any record in `enriched.drivers_data` | HIGH | RR, SR, DS, QR, ST, CR, LT, PS |
| `UNRESOLVED_TEAM_FK` | `TeamId` / `Team` does not match any record in `enriched.constructors_data` | HIGH | RR, SR, DR, CR |
| `UNRESOLVED_EVENT_FK` | `event` in `circuit_info` does not match any `official_event_name` in `enriched.races_data` | HIGH | CI |
| `INVALID_SESSION_NAME` | `session_name` in `circuit_info` is not in the known session type enum | HIGH | CI |
| `INVALID_EVENT_FORMAT` | `format` in `circuit_info` is not in the known event format enum | MEDIUM | CI |
| `DUPLICATE_SESSION` | Two rows share the same `(event, session_name)` key in `circuit_info` | CRITICAL | CI |
| `NULL_ABBREVIATION` | `Abbreviation` is null in a driver entity | CRITICAL | DR |
| `INVALID_DRIVER_ID_FORMAT` | `Driver ID` slug does not match `^[a-z0-9_]+$` | HIGH | DR |
| `INVALID_ABBREVIATION_FORMAT` | `Abbreviation` or `Driver` abbreviation does not match `^[A-Z]{3}$` | HIGH | DR, QR, LT, PS |
| `DRIVER_NUMBER_OUT_OF_RANGE` | Driver car number is outside the F1 valid range 1–99 | MEDIUM | DR |
| `NULL_DRIVER_NAME` | `First Name` or `Last Name` is null in `drivers_data` | HIGH | DR |
| `NULL_TEAM_REFERENCE` | `Team` string is null in an entity that carries a team reference | HIGH | DR, CR |
| `DUPLICATE_ABBREVIATION` | Two rows share the same 3-letter abbreviation in `drivers_data` | CRITICAL | DR |
| `NULL_STANDINGS_POSITION` | `Position` is null in `driver_standings` | HIGH | DS |
| `NULL_STANDINGS_POINTS` | `Points` is null in `driver_standings` | HIGH | DS |
| `SENTINEL_DRIVER_ID` | `Driver ID` contains the sentinel string `"nan"` after GLOBAL-01 replacement (backstop rule) | HIGH | QR |
| `NULL_STATUS_VALUE` | `Status` is null in `status_data` | HIGH | ST |
| `NULL_PRIMARY_KEY` | (see first row — applies to all entities) | CRITICAL | All |
| `EMPTY_TEAM_NAME` | `Team Name` in `constructors_data` is an empty or whitespace-only string | CRITICAL | CD |
| `INVALID_TEAM_ID_FORMAT` | `team_id` slug does not match `^[a-z0-9_]+$` | HIGH | CD |
| `NULL_RESULT_POSITION` | `Position` is null in `constructor_results` | HIGH | CR |
| `LAP_NUMBER_OUT_OF_RANGE` | `Lap Number` is outside the valid range 1–100 | HIGH | LT, PS |
| `PIT_IN_TIME_UNEXPECTED_VALUE` | `Pit In Time` is non-null in `pit_stops` when a 100% null rate is expected (collection failure marker) | CRITICAL | PS |
| `NULL_PIT_OUT_TIME` | `Pit Out Time` is null in `pit_stops` | HIGH | PS |
| `UNRESOLVED_LAP_FK` | A `pit_stops` row references a `(Driver, Lap Number, round_number)` not present in `enriched.lap_times` | HIGH | PS |

---

## 4. Structural Expectations — Not DQ Failures

The following null values and anomalies are domain-correct and must NOT trigger quarantine rules. They must be explicitly documented so that the Enriched layer notebook does not misclassify them as bad data.

| Anomaly | Entity | Column(s) | Expected Condition | Handling |
|---------|--------|-----------|-------------------|----------|
| `Sector 1` null on lap 1 | `lap_times` | `Sector 1` | `Sector 1 IS NULL AND CAST(Lap Number AS INT) = 1` for all 20 drivers | Structurally expected — no Sector 1 timing is available on the formation/first lap because the timing beam is not crossed before the lap ends. Do not apply LT-10 (Sector 1 format) when `Lap Number = 1`. |
| Q2 Time null for Q1-eliminated drivers | `qualifying_results` | `Q2 Time` | Drivers eliminated after Q1 (positions 16–20) will have null `Q2 Time` and null `Q3 Time` | Meaningful null representing non-participation; do not quarantine. These are the bottom 5 qualifiers by Q1 time. |
| Q3 Time null for non-top-10 qualifiers | `qualifying_results` | `Q3 Time` | Drivers not in the top 10 after Q2 will have null `Q3 Time` | Same as above — meaningful null. Approximately 10 of 20 drivers will have null `Q3 Time` in every normal qualifying session. |
| `Last Session` null for Round 0 | `races_data` | `Last Session` | `Last Session IS NULL AND Round = 0` | Pre-Season Testing (Round 0) has no defined last session in the F1 FastF1 source; this null is expected and must be preserved. |
| `Time` null for retired drivers | `race_results`, `sprint_results`, `constructor_results` | `Time` | `Time IS NULL AND Status = 'Retired'` | A driver who did not finish the race has no gap-to-leader time. This is structurally correct. The `Status = 'Retired'` column confirms the reason. |
| `HeadshotUrl` null or "None" | `race_results`, `sprint_results` | `HeadshotUrl` | `HeadshotUrl IS NULL` after GLOBAL-01 | Optional metadata from the F1 provider; not all drivers have headshots loaded (e.g. new entrants, reserve drivers). Null is acceptable and must not quarantine the row. Log a warning for data steward awareness only. |
| `Position` null for BOR in `qualifying_results` | `qualifying_results` | `Position` | `Position IS NULL AND Abbreviation = 'BOR'` | BOR did not set a Q1 time in the sample event. His `Position` null may reflect genuine non-participation or an extraction artefact — this is an open question (see Section 6). Until resolved, treat as a non-quarantine null; QR-05 excludes nulls from the range check. |

---

## 5. Rules Ordering and Execution Notes

The following execution order must be applied within every Enriched layer notebook. Steps 1–2 are global and run once per entity load. Steps 3–8 are entity-specific and run per row. DEDUPLICATION (step 8) runs on the full batch after all per-row checks.

```
Step 1: GLOBAL-01 — Sentinel string replacement
         Replace "nan", "None", "NaT" with SQL NULL in all StringType columns
         (No quarantine — silent transformation)

Step 2: GLOBAL-02 — Round number injection check
         Assert: round_number IS NOT NULL AND round_number >= 0 AND round_number <= 24
         Rejection reason: MISSING_ROUND_NUMBER / ROUND_OUT_OF_RANGE
         (If this fails, the entire batch is quarantined — do not proceed to entity checks)

Step 3: Entity-specific NULLABILITY checks
         Evaluate all IS NULL predicates on required columns
         Rejection reasons: NULL_PRIMARY_KEY, NULL_DRIVER_NUMBER, NULL_TEAM_ID, etc.

Step 4: Entity-specific SENTINEL_DETECTION checks
         Evaluate any sentinel backstop rules (QR-02, RR-12, SR-12)
         These run after GLOBAL-01 as a secondary safety net
         Rejection reasons: SENTINEL_DRIVER_ID, SENTINEL_HEADSHOT_URL

Step 5: Entity-specific TYPE_VALIDITY checks
         Evaluate mixed-type and format-adjacent correctness
         (e.g. ClassifiedPosition enum, team name non-empty)
         Rejection reasons: INVALID_CLASSIFIED_POSITION, EMPTY_TEAM_NAME

Step 6: Entity-specific FORMAT checks
         Evaluate regex patterns on time strings, datetime strings, slugs, hex colours
         Rejection reasons: INVALID_LAP_TIME_FORMAT, INVALID_DATETIME_FORMAT,
         INVALID_DRIVER_ID_FORMAT, INVALID_ABBREVIATION_FORMAT, etc.

Step 7: Entity-specific RANGE_DOMAIN checks
         Evaluate numeric range assertions (position 1–20, lap 1–100, points >= 0, etc.)
         Rejection reasons: POSITION_OUT_OF_RANGE, LAP_NUMBER_OUT_OF_RANGE,
         NEGATIVE_POINTS, ROUND_OUT_OF_RANGE, etc.

Step 8: Entity-specific REFERENTIAL_INTEGRITY checks
         Evaluate FK existence assertions against already-loaded Enriched dimension tables
         Pre-condition: enriched.drivers_data and enriched.constructors_data must be
         loaded before any fact or result entity runs referential integrity checks
         Rejection reasons: UNRESOLVED_DRIVER_FK, UNRESOLVED_TEAM_FK,
         UNRESOLVED_EVENT_FK, UNRESOLVED_LAP_FK

Step 9: DEDUPLICATION checks
         Run COUNT(*) GROUP BY primary key on the surviving (non-quarantined) batch
         Duplicate detection runs last to allow corrections (e.g. BOR driver_id fix)
         to complete before key uniqueness is asserted
         Rejection reasons: DUPLICATE_PRIMARY_KEY, DUPLICATE_ROUND,
         DUPLICATE_ABBREVIATION, DUPLICATE_SESSION
```

### Dependency Ordering for Enriched Layer Notebook Execution

The following load order must be enforced by the Databricks Workflow:

```
1. races_data          (no FK dependencies)
2. circuit_info        (FK: races_data.Official Event Name)
3. drivers_data        (no FK dependencies)
4. constructors_data   (no FK dependencies)
5. driver_standings    (FK: drivers_data)
6. qualifying_results  (FK: drivers_data; sentinel correction for BOR)
7. status_data         (FK: drivers_data)
8. race_results        (FK: drivers_data, constructors_data)
9. sprint_results      (FK: drivers_data, constructors_data)
10. constructor_results (FK: drivers_data, constructors_data)
11. lap_times           (FK: drivers_data)
12. pit_stops           (FK: drivers_data, lap_times)
```

### Quarantine Table Schema

Each entity must have a dedicated quarantine Delta table at `enriched_quarantine.<entity_name>` with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `quarantine_id` | StringType | UUID generated at write time |
| `ingestion_timestamp` | TimestampType | When the row was quarantined (UTC) |
| `source_file` | StringType | Full path of the source CSV/JSON file |
| `round_number` | IntegerType | Round number injected by pipeline (may be null if GLOBAL-02 failed) |
| `rejection_reason` | StringType | One of the constants from Section 3 |
| `rejection_rule_id` | StringType | The specific rule ID that triggered (e.g. `RR-05`, `GLOBAL-02`) |
| `failed_column` | StringType | Column name that caused the failure |
| `failed_value` | StringType | String representation of the failing value |
| `raw_row` | StringType | Full source row serialised as JSON for reprocessing |

---

## 6. Open Questions — Pre-Gate-3 Decisions

The following items require explicit human sign-off before the Enriched layer notebook is built. They are listed here as Gate 3 blockers.

### OQ-01 — DQ-04: Confirm constructor_standings Semantic Redesign

**Question**: `constructor_standings.csv` and `constructor_results.csv` are byte-for-byte identical in the current source. The profiling report designates `constructor_results` as authoritative for per-race driver results and retires `constructor_standings`. However, the intended semantic distinction has not been confirmed.

**Options**:
- A. `constructor_results` = per-driver race results; `constructor_standings` = constructor-aggregated championship points table (to be re-extracted from source)
- B. Both files are the same entity; one is permanently retired with no re-extraction needed
- C. A new extraction of `constructor_standings` is required with constructor-level (not driver-level) data

**Decision needed**: Which option applies? This determines whether DQ rules for a new `constructor_standings` entity need to be added to this catalogue before Gate 3.

---

### OQ-02 — DQ-01 / DQ-02 / DQ-03: Confirm Column Drop vs Preserve-Null Strategy

**Question**: Three columns are 100% null due to extraction failures:
- `CountryCode` in `race_results` and `sprint_results` (DQ-01)
- `Q1`, `Q2`, `Q3` in `race_results` and `sprint_results` (DQ-02)
- `Pit In Time` in `pit_stops` (DQ-03)

**This catalogue assumes**: `CountryCode`, `Q1`, `Q2`, `Q3` are **dropped** at the Enriched layer (they carry no data and are sourced elsewhere). `Pit In Time` is **retained as a null column** (to signal the gap and unblock schema evolution when the collection failure is fixed).

**Decision needed**: Confirm this drop/retain strategy. If any of these columns are to be re-extracted and populated before the Enriched layer is built, the DQ rules in this catalogue must be updated to add format and nullability checks for those columns.

---

### OQ-03 — DQ-20: Confirm Expected Status Enum Values for status_data

**Question**: The current sample of `status_data` contains only 2 distinct `Status` values: `"Finished"` and `"Retired"`. The F1 domain has many more (engine failures, collisions, disqualifications, etc.). This catalogue defines an expanded enum in ST-03, RR-11, SR-11, and CR-06, but the list is based on general F1 domain knowledge — not confirmed against the actual FastF1 API output.

**Decision needed**: Provide or confirm the complete enumeration of `Status` values that the FastF1 data provider will emit. Until this is confirmed, ST-03 is flagged as MEDIUM severity with data steward review (not hard quarantine) to avoid rejecting valid new status values from future races.

---

### OQ-04 — BOR Q1 Null: Confirm Whether BOR Participated in Q1

**Question**: In `qualifying_results`, Gabriel Bortoleto's (BOR) `Q1 Time` is null and his `Position` is null, in addition to his `Driver ID` being the sentinel string `"nan"`. It is unclear whether:
- BOR participated in Q1 but his data was not captured (extraction failure alongside the ID bug)
- BOR did not participate in Q1 (e.g. withdrew, mechanical issue before the session)

**Decision needed**: Confirm BOR's participation status for the sampled qualifying session. If he participated but data is missing, `Q1 Time IS NULL` for BOR should be treated as an extraction failure and quarantined (HIGH severity). If he did not participate, the null is structurally valid and belongs in Section 4 of this catalogue.

---

*End of Data Quality Rules Catalogue v1.0 — Gate 3 Approval Required*
