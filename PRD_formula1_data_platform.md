# PRD: Formula 1 Data Platform on Databricks

## Problem Statement

A custom data provider delivers 13 CSV files daily — one per Formula 1 entity (races, drivers, results, lap_times, pit_stops, constructor, sprint, qualifying, and others) — with no reliable pipeline to ingest, cleanse, integrate, and serve that data for business intelligence consumption. Without a structured pipeline, analysts cannot trust the data, historical records are not preserved, and there is no scalable path to Power BI reporting or future AI/ML use cases.

## Solution

Build a multi-layer data platform on Databricks (free tier) using PySpark notebooks orchestrated by Databricks Workflows. Data flows through four governed layers — Raw, Enriched, IDS, and Curated — with a quarantine pattern for bad data, a full historical backfill on day one, and a daily incremental load thereafter. Each layer transition is gated by a human approval checkpoint. The Curated layer exposes a star schema to Power BI via the cluster JDBC/ODBC endpoint. The entire project is designed to be operated by AI Agents playing specialised roles (Data Engineer, Data Architect, Analytics Engineer, Data Analyst, Data Steward), each producing structured documentation artifacts at their respective layer gate.

## User Stories

1. As a Data Engineer Agent, I want to detect whether today's CSV files have arrived before starting the pipeline, so that the job does not fail silently when the provider is late.
2. As a Data Engineer Agent, I want to ingest each CSV file into a date-partitioned Raw Delta table in append mode, so that every historical snapshot is preserved and reprocessable.
3. As a Data Engineer Agent, I want to run a one-time historical backfill job that processes all existing CSV data before the daily schedule begins, so that the platform has a complete F1 dataset from day one.
4. As a Data Engineer Agent, I want to produce a Data Profiling Report for each entity after the profiling sprint, so that primary keys, nullability, cardinality, and data quality issues are documented before pipeline design is locked in.
5. As a Data Engineer Agent, I want to write rows that fail quality checks to a per-entity quarantine Delta table with rejection reason, source file, and ingestion timestamp, so that bad data is isolated without stopping the pipeline.
6. As a Data Architect Agent, I want to define and document primary keys for all 13 entities in the Data Mapping document, so that upsert and merge logic downstream is unambiguous.
7. As a Data Architect Agent, I want to produce a Data Dictionary covering all fields across all entities, so that every consumer understands the meaning, type, and lineage of each column.
8. As a Data Architect Agent, I want to design and document the Logical and Physical Data Model for the IDS layer, so that entity integration decisions are explicit and reviewable before implementation.
9. As a Data Architect Agent, I want to enforce schema on Delta tables using Unity Catalog, so that schema drift from the provider is caught at ingestion rather than silently corrupting downstream layers.
10. As a Data Engineer Agent, I want to apply incremental merge (upsert) logic in the Enriched layer using defined primary keys, so that corrections from the provider are reflected without duplicating rows.
11. As a Data Engineer Agent, I want to cleanse, type-cast, and deduplicate records in the Enriched layer, so that downstream layers consume consistent, trustworthy data.
12. As a Data Engineer Agent, I want to produce a Source-to-Target Mapping document for each layer transition, so that every field transformation is traceable from source CSV to final table.
13. As an Analytics Engineer Agent, I want to define and document the Data Quality Rules Catalogue before Enriched layer build begins, so that quality checks are agreed upon and not invented ad hoc.
14. As an Analytics Engineer Agent, I want to integrate entities from the Enriched layer into a unified IDS model (e.g. joining race results with drivers, constructors, and circuits), so that the IDS represents a single version of truth for F1 data.
15. As an Analytics Engineer Agent, I want to document an MDM Decision Log for shared entities (drivers, constructors) that appear across multiple source files, so that canonical ID resolution and deduplication decisions are recorded.
16. As an Analytics Engineer Agent, I want to build a star schema in the Curated layer (e.g. fact_race_results, dim_drivers, dim_circuits, dim_constructors), so that Power BI can consume pre-aggregated, business-ready tables efficiently.
17. As an Analytics Engineer Agent, I want to produce a Star Schema Design Document before Curated layer build begins, so that the dimensional model is reviewed and approved before implementation.
18. As an Analytics Engineer Agent, I want to produce a Power BI Semantic Model Document after the Curated layer is built, so that report developers know which tables, measures, and relationships to use.
19. As a Data Analyst Agent, I want to validate business logic in the Curated layer against known F1 facts, so that I can confirm the data is correct before it is connected to Power BI.
20. As a Data Analyst Agent, I want to document validation results as part of the Curated layer gate, so that the human approver has evidence before sign-off.
21. As a Data Steward Agent, I want to review quarantine tables daily and decide whether to fix-and-reprocess or discard rejected rows, so that data quality issues do not silently accumulate.
22. As a Data Steward Agent, I want to produce a Data Lineage Map at the end of the project covering all entities from source CSV to Curated star schema, so that auditors and new team members can trace any data point end-to-end.
23. As a Data Steward Agent, I want to maintain the MDM Decision Log as canonical ID decisions evolve, so that there is a living record of master data governance.
24. As a Data Engineer Agent, I want to produce a Pipeline Runbook after all layers are built, so that the daily job can be monitored, debugged, and re-run by anyone without tribal knowledge.
25. As a human project owner, I want to review and approve documentation artifacts at each layer boundary before the next agent proceeds, so that design mistakes are caught early and every decision is auditable.
26. As a Power BI report developer, I want to connect to the Curated star schema via the cluster JDBC/ODBC endpoint, so that I can build F1 dashboards on top of clean, modelled data.
27. As a Data Engineer Agent, I want the daily Workflow to retry the file arrival sensor every 10 minutes up to a 2-hour timeout, so that late file deliveries from the provider are handled gracefully without manual intervention.
28. As a Data Architect Agent, I want all Delta tables registered in Unity Catalog with appropriate catalog/schema/table naming conventions, so that access control and governance are enforceable from day one.

## Implementation Decisions

### Layer Architecture
- **Raw**: Exact copy of source CSV, no transformation, date-partitioned Delta table, append mode only. One table per entity. Source of truth for reprocessing.
- **Enriched**: Cleansed, typed, deduplicated, primary keys enforced, incremental merge (upsert) applied. Business rules start here. Quarantine table per entity for rejected rows.
- **IDS (Integrated Data Store)**: Entities joined and integrated across sources, standardised to a common data model. Single version of truth. MDM decisions applied here.
- **Curated**: Star schema optimised for Power BI consumption. Fact and dimension tables (e.g. `fact_race_results`, `dim_drivers`, `dim_circuits`, `dim_constructors`, `dim_constructors`). Aggregations and business metrics applied.

### Storage & Compute
- Delta Lake for all layers (ACID, time travel, MERGE support)
- Unity Catalog for all table registration and governance
- PySpark notebooks for all transformation logic
- Databricks Workflows for orchestration and scheduling
- Free tier compatible — no Delta Live Tables, no SQL Warehouse, no autoscaling
- Power BI connects via cluster JDBC/ODBC endpoint

### Ingestion Pattern
- **Historical backfill**: One-time parameterised job processes all existing CSV data before daily schedule begins
- **Daily incremental**: File arrival sensor polls every 10 minutes up to 2-hour timeout, then pipeline runs 30 minutes after expected delivery time
- **Merge strategy**: Upsert on defined primary keys in Enriched and above; append-only in Raw

### Project Structure
```
formula1/
├── datasource/          # raw CSV files from provider
├── notebooks/
│   ├── 00_profiling/    # data profiling sprint notebooks
│   ├── 01_raw/          # CSV → Raw Delta ingestion
│   ├── 02_enriched/     # cleansing, typing, dedup, merge
│   ├── 03_ids/          # entity integration, MDM
│   ├── 04_curated/      # star schema aggregations
├── workflows/           # Databricks Workflow job definitions
└── docs/                # all documentation artifacts
```

### Agent Role Assignments
| Role | Agent | Primary Responsibility |
|---|---|---|
| Data Engineer | Data Engineer Agent | Ingestion, Raw, Enriched build, Workflows, Runbook |
| Data Architect | Data Architect Agent | Data Mapping, Dictionary, Data Model, Unity Catalog |
| Analytics Engineer | Analytics Engineer Agent | IDS, Curated, Star Schema, Power BI doc |
| Data Analyst | Data Analyst Agent | Business logic validation, Curated sign-off |
| Data Steward | Data Steward Agent | Quarantine review, MDM Log, Lineage Map |

### Human Approval Gates
1. After Data Profiling Report → approve before pipeline design
2. After Data Mapping + Data Dictionary → approve before Raw build
3. After Data Quality Rules Catalogue → approve before Enriched build
4. After Logical & Physical Data Model → approve before IDS build
5. After Star Schema Design Doc → approve before Curated build
6. After all layers built → approve Runbook + Lineage Map before Power BI connection

### Documentation Deliverables
| Document | Owner Agent | Gate |
|---|---|---|
| Data Profiling Report | Data Engineer | Gate 1 |
| Data Mapping | Data Architect | Gate 2 |
| Data Dictionary | Data Architect | Gate 2 |
| Source-to-Target Mapping (per layer) | Data Engineer | Each gate |
| Data Quality Rules Catalogue | Analytics Engineer | Gate 3 |
| Logical & Physical Data Model | Data Architect | Gate 4 |
| MDM Decision Log | Data Steward | Gate 4 (ongoing) |
| Star Schema Design Document | Analytics Engineer | Gate 5 |
| Pipeline Runbook | Data Engineer | Gate 6 |
| Power BI Semantic Model Document | Analytics Engineer | Gate 6 |
| Data Lineage Map | Data Steward | Gate 6 |

## Testing Decisions

A good test validates observable, external behaviour — what a layer outputs given a known input — not how a notebook is internally structured. Tests should be runnable independently against a test catalog/schema in Unity Catalog without touching production data.

### Modules to test
- **File arrival sensor**: Given a folder with/without today's file, assert correct proceed/retry/timeout behaviour
- **Raw ingestion**: Given a sample CSV, assert correct Delta table schema, partition column, row count, and append-only behaviour
- **Enriched merge logic**: Given a base dataset and an incremental update CSV, assert correct upsert — updated rows reflected, no duplicates, quarantine table populated for invalid rows
- **Quarantine writer**: Given rows violating defined rules, assert quarantine table contains correct rejection reason, source file, and timestamp
- **IDS integration**: Given known Enriched tables, assert join correctness and canonical ID resolution per MDM decisions
- **Curated star schema**: Given known IDS data, assert fact and dimension table grain, referential integrity, and aggregate correctness against manually verified F1 facts

### Testing approach
- Each notebook accepts parameters for catalog/schema so tests run against an isolated `test_` catalog
- Backfill job and daily incremental job share the same notebook logic with date range parameters — test both parameter modes
- Validation notebook in `00_profiling/` serves as prior art for assertion patterns

## Out of Scope

- Delta Live Tables (not available on free tier)
- SQL Warehouse / Databricks SQL (not available on free tier)
- Real-time or streaming ingestion
- ML feature tables or model training
- Multi-workspace deployment or CI/CD pipeline
- Sandbox/ad-hoc exploration layer (suggested for a future phase)
- API-based ingestion directly from F1 data providers
- Row-level security or column masking in Unity Catalog (future governance phase)

## Further Notes

- Primary keys for all 13 entities must be discovered during the profiling sprint before any pipeline design is finalised. This is the critical path dependency for the entire project.
- F1 data is mostly append-only (new races, results, lap times) but corrections from the provider do occur — the incremental merge pattern in Enriched is specifically designed to handle provider corrections gracefully.
- The MDM Decision Log is particularly important for `drivers` and `constructors` entities, which may appear with inconsistent IDs or names across source files (e.g. driver name spelling variations, constructor rebrands).
- The agentic architecture assumes each agent receives the prior gate's approved documentation as its primary input. Documentation quality at each gate directly determines agent performance at the next gate.
- Power BI connection uses the cluster JDBC/ODBC endpoint. The cluster must be running for Power BI to refresh — consider scheduling the cluster to start before the daily Workflow completes and remain running during business hours.
