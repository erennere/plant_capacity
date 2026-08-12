---
name: Dagsterize Audit
description: Scan the repo for resources (connections, workers, loggers), assets (output files/tables), definitions boundaries, and scheduling logic that map onto Dagster concepts, and propose an incremental migration strategy.
disable-model-invocation: true
---

# Dagsterize Audit

Scan this repository for constructs that map onto Dagster's core concepts, and propose an **incremental** migration strategy — not a full-repo rewrite plan. The goal is to identify where Dagster's abstractions (resources, software-defined assets, a `Definitions` boundary, schedules/sensors) would meaningfully replace ad hoc equivalents already present in this codebase.

## Dagster concept mapping — what to look for

- **Resources** (`ConfigurableResource` / `@resource`): reusable, potentially-stateful, externally-configured objects threaded through multiple functions. Candidates in this repo: DuckDB connections opened/closed ad hoc across many scripts (a known duplication cluster — cross-reference `find-duplication`), `ProcessPoolExecutor`/worker-pool instantiations, logger/print setup (cross-reference the observability inconsistency flagged by `find-logic-inconsistencies`), HTTP/download clients (e.g. Overture Maps, WorldPop raster downloads), and config-derived path roots.
- **Assets** (`@asset`): persistent data objects a script produces that a later stage consumes — output GeoJSON/CSV/GeoTIFF files, DuckDB tables, intermediate parquet/shapefiles. Trace producer→consumer chains: a script's output path declared in `config.yaml` that another script later reads as an input path is a candidate asset dependency edge. Build this as an actual dependency graph, not just a list of files.
- **Definitions boundary** (the `Definitions` object bundling assets/resources/jobs/schedules/sensors for one deployable code location): identify how the current orchestration in `starter.py`/`pipelines.py` (config resolution, `run_voronoi_approach`, the `.sh` wrapper call chains) would map onto one or more Dagster code locations — likely grouped by pipeline stage-family (core voronoi engine, river/risk pipeline, validation against reference datasets, industrial analysis, sensitivity sweeps) rather than a single monolith.
- **Schedules/sensors**: any periodic or triggered execution. SLURM `sbatch` submissions and parameter-sweep scripts in `sensitivity_analysis_scripts/` map to `ScheduleDefinition`/`@schedule`; any file-arrival-driven or manually-re-run-until-fresh-data patterns map to sensors.

## Method

1. Use `Explore` agents in parallel per subsystem directory to inventory: DB/connection open-close call sites, `ProcessPoolExecutor`/multiprocessing usage, logger/print setup, config-driven input/output file paths (to build the producer→consumer asset graph), and `.sh`/SLURM scheduling entry points.
2. Use `config.yaml`'s existing section structure as a first-pass grouping signal for candidate Dagster code locations, since scripts are already organized around config sections.
3. For each candidate, assess migration difficulty: already isolated/parameterized (straightforward to wrap as a resource/asset with minimal change) versus tightly coupled/inlined (needs a prerequisite refactor first). Explicitly cross-reference `simplify-code`/`find-duplication`/`enforce-fail-fast` findings where a clean wrap depends on that refactor happening first.

## Output format

Grouped by Dagster concept:
- **Resources**: candidate, current implementation site(s) (`file:line`), proposed resource class/config shape.
- **Assets**: candidate output, producer script, consumer script(s), proposed asset key + dependency edge.
- **Definitions grouping**: proposed code-location boundaries and the reasoning for each boundary.
- **Schedules/sensors**: candidate, current trigger mechanism (SLURM/cron/manual re-run), proposed Dagster equivalent.

For every finding, state migration difficulty (trivial wrap / needs prerequisite refactor / needs a design decision) and name the prerequisite explicitly when one exists (e.g. "the 3-4 independent DuckDB connection patterns should be consolidated per `find-duplication` before wrapping as a single resource").

## Decision policy

This is a scan-and-propose skill only — do not write any Dagster code, add any dependency, or restructure any file. Present the concept-mapped inventory plus a suggested incremental adoption order (start with the smallest, most self-contained pipeline stage — not the whole repo at once) and let the user decide whether and how to proceed.
