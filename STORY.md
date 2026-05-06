# Building a Data Platform with an AI-Augmented Team

*A story about what happens when Claude Code plays every role on your data team.*

---

## The Problem

Our team needed to build a Formula 1 data platform on Databricks. The scope was real and non-trivial: ingest 13 daily CSV files from a data provider, pass them through four governed data layers (Raw → Enriched → IDS → Curated), expose a star schema to Power BI, and do it on the free tier with no Delta Live Tables and no SQL Warehouse.

In a traditional setup, this kicks off a weeks-long process: workshops to align on requirements, a data architect to design the model, a data engineer to build the pipeline, an analytics engineer to own the semantic layer, a data steward to govern quality, and a data analyst to validate the outputs. Five specialised roles. Multiple handoffs. Plenty of room for misalignment.

We had one person, a laptop, and Claude Code.

---

## The Approach

Instead of hiring or waiting, we used Claude Code as an **AI-augmented team** — not to replace human judgement, but to multiply its reach. The human stays in control at every gate. The agents do the sprint work in between.

The architecture is simple: Claude Code can spawn multiple specialised agents that work in parallel, each playing a defined role (Data Engineer, Data Architect, Analytics Engineer, Data Analyst, Data Steward). They produce structured artefacts. A human reviews and approves before the next phase begins. The agents never skip the gate.

This is not automation. It is leverage.

---

## The Journey

### Step 1 — Shaping the Idea with `/grill-me`

Before writing a single line of code, we stress-tested the idea. The `/grill-me` skill runs a relentless interview — probing assumptions, surfacing contradictions, forcing clarity on every branch of the decision tree.

Questions it pushed on:

- *What does "daily CSV delivery" actually mean — drop to S3, SFTP, email? What happens when the provider is late?*
- *Free tier means no Delta Live Tables. How do you orchestrate without them?*
- *You said "star schema for Power BI" — what is the grain of your fact table? One row per what?*
- *You have 13 entities. Which ones share keys? Are those keys stable across seasons?*

By the end of the session, the idea had hardened into a set of concrete decisions: four Delta layers, append-only Raw, upsert in Enriched, quarantine pattern for bad data, Databricks Workflows for orchestration, six human approval gates.

---

### Step 2 — Writing the PRD with `/to-prd`

With the design decisions clear, `/to-prd` turned the conversation into a formal Product Requirements Document. Not a rough notes file — a structured PRD with:

- A problem statement
- 28 user stories mapped to each agent role
- Explicit implementation decisions for every architectural choice
- A testing strategy that validates observable behaviour, not internal notebook structure
- A clear out-of-scope list (no Delta Live Tables, no ML, no real-time)

The PRD became the single source of truth that every subsequent agent would read before starting work. Documentation quality at each gate directly determines agent performance at the next one.

---

### Step 3 — Breaking Down the Work with `/to-issues`

With the PRD approved, `/to-issues` decomposed it into **12 independently-grabbable GitHub issues** using tracer-bullet vertical slices — each one thin enough to be completable, thick enough to be demoable.

The breakdown respected the project's natural dependency structure: six **HITL (Human-in-the-Loop)** issues at each approval gate, and six **AFK (Away-From-Keyboard)** implementation issues that agents can run autonomously in between.

```
#1  Data Profiling Sprint          HITL  ← start here
#2  Data Architecture Foundation   HITL  blocked by #1
#3  Data Quality Rules Catalogue   HITL  blocked by #1 (parallel with #2)
#4  Raw Layer Pipeline             AFK   blocked by #2
#5  Enriched Layer Pipeline        AFK   blocked by #4 + #3
#6  IDS Data Model                 HITL  blocked by #5
#7  IDS Layer Pipeline             AFK   blocked by #5 + #6
#8  Star Schema Design             HITL  blocked by #7
#9  Curated Layer Pipeline         AFK   blocked by #7 + #8
#10 Databricks Workflows           AFK   blocked by #9
#11 Final Validation & Docs Gate   HITL  blocked by #9 + #10
#12 Power BI Connection            AFK   blocked by #11
```

All 12 issues published to GitHub in under five minutes. Each with acceptance criteria, a blocker reference, and the user stories it addressed.

---

### Step 4 — Running Issue #1: The Data Profiling Sprint

This is where the multi-agent architecture paid off immediately.

Issue #1 required profiling all 13 source entities — row counts, column types, null rates, distinct values, candidate primary keys, data quality issues, and cross-entity join analysis. In a human team, this is two days of work for a data engineer with pandas open in a notebook.

We spun **three parallel Data Engineer Agents**, each assigned a group of entities:

| Agent | Entities |
|-------|---------|
| Agent 1 | `races_data`, `season_data`, `race_results`, `sprint_results`, `circuit_info` |
| Agent 2 | `drivers_data`, `driver_standings`, `qualifying_results`, `status_data` |
| Agent 3 | `constructors_data`, `constructor_standings`, `constructor_results`, `lap_times`, `pit_stops` |

All three ran in parallel. While Agent 1 was profiling race calendars, Agent 2 was discovering a referential integrity break in `qualifying_results`, and Agent 3 was identifying that two files were byte-for-byte identical.

A **fourth synthesis agent** then combined all findings into:

- `docs/data_profiling_report.md` — 537 lines covering all 13 entities, a primary key decisions table, a prioritised data quality matrix, and recommended next steps for Gate 1
- `notebooks/00_profiling/profile_entities.py` — a Databricks-compatible PySpark notebook with per-entity profiling, cross-entity validation cells, and `dbutils.widgets` parameterisation
- The full project folder structure (`notebooks/01_raw/` through `04_curated/`, `workflows/`, `docs/`)

**Elapsed time from "spin the agents" to committed artefacts: under 15 minutes.**

---

### Step 5 — Validating on Real Databricks

Local pandas analysis is fast, but Spark can behave differently. Rather than assuming the findings were correct, we used the **Databricks CLI** to run the profiling directly against the live serverless SQL warehouse.

Thirteen CSV files were uploaded to a Unity Catalog volume. Ten validation queries ran against `read_files()` in the warehouse — row counts, null audits, type inference, duplicate checks, join coverage.

Nine findings confirmed. Two new ones emerged that only Databricks could reveal:

**Finding 1 — BOR's `Driver ID` is not SQL NULL, it is the string `"nan"`**

pandas reads `"nan"` as a float null and surfaces it as `NaN`. Spark reads the same CSV and sees a perfectly valid 3-character string. `IS NULL` returns `false`. Any null-check DQ rule in the Enriched layer would silently pass Bortoleto's row through as valid data — breaking every downstream FK join.

This is a silent failure mode. It would have been invisible until the IDS layer started producing wrong join results.

**Finding 2 — Colapinto's `HeadshotUrl` is the string `"None"`, not SQL NULL**

Same root cause: pandas serialises its own null types as string literals when writing to CSV (`"nan"`, `"None"`, `"NaT"`). Spark has no way to know these are supposed to be nulls.

Both findings were captured as **DQ-06b** in the report: a mandatory global sentinel-replacement step at the top of every Enriched notebook, before any quality check runs.

The profiling report was updated, committed, and pushed. Issue #1 was closed with a full summary comment on GitHub.

---

## What Was Built — In One Session

| Artefact | Description |
|----------|-------------|
| `PRD_formula1_data_platform.md` | Full product requirements document — 28 user stories, 6 approval gates, implementation decisions, testing strategy |
| 12 GitHub issues | Complete project backlog with dependency graph, acceptance criteria, HITL/AFK classification |
| `docs/data_profiling_report.md` | Gate 1 artefact — 13 entities profiled, PK decisions, 20+ DQ issues prioritised |
| `notebooks/00_profiling/profile_entities.py` | Databricks PySpark profiling notebook, parameterised for any catalog/schema |
| Full folder structure | `notebooks/01_raw/` through `04_curated/`, `workflows/`, `docs/` |
| Databricks validation | All 13 entities validated against live serverless warehouse; 2 new findings added to report |

---

## What This Means for the Team

This is not about replacing data engineers. The human made every architectural decision. The human approved every gate. The human read the profiling report and decided whether the findings were acceptable before signing off.

What changed is the **cost of the sprint work between gates**. Profiling 13 entities, writing a structured report, building a parameterised notebook, and validating it against a live warehouse — that used to be two days. It is now 15 minutes of agent runtime and 10 minutes of human review.

The multiplier is not in the thinking. The multiplier is in the doing.

**For a data team that is already skilled, Claude Code compresses the distance between a good decision and a working artefact.** The architects still architect. The engineers still engineer. The standards still hold. The timeline collapses.

---

## What Comes Next

Gate 1 is pending human sign-off on `docs/data_profiling_report.md`.

Once approved, issues #2 (Data Architecture Foundation) and #3 (Data Quality Rules Catalogue) open in parallel — two more agent sprints, two more HITL reviews, before the first line of pipeline code is written.

*This story will be updated at each gate.*

---

*Built with [Claude Code](https://claude.ai/code) · Repository: [gabovailati/databricks](https://github.com/gabovailati/databricks)*
