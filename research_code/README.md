# research-code

This folder is the executable heart of the project. In practice, you usually run shell scripts for reproducibility and scheduler compatibility, while Python scripts contain the actual processing logic. Most scripts load settings from `config.yaml` through `starter.py`, so updating the config is the normal way to change paths and parameters.

## How This Folder Is Run

From this folder:

```bash
python -m pip install -e .
```

Typical pipeline commands:

```bash
bash data_merge/combine_locations.sh
bash download_pop.sh
bash create_voronoi.sh
bash add_pop.sh 0
bash industrial_analysis/industrial_analysis.sh
```

## Python Scripts (Logic)

### starter.py
Aim: Centralize configuration loading and override parsing for all modules. Inputs: `config.yaml` and optional runtime overrides from CLI wrappers. Outputs: A flat configuration dict with resolved paths, runtime flags, and callable-name strings. How: It parses overrides, expands path templates, reads every value from the YAML (no hard-coded defaults in Python), and returns one consistent dict used across the pipeline. All parameter defaults live exclusively in `config.yaml`.

### pipelines.py
Aim: Provide shared orchestration helpers used by top-level modules. Functions are grouped by role:
- **Internal helpers**: `_compute_mean_2_nnd_web_mercator` (per-site nearest-neighbour spacing); `_resolve_configured_callable` (maps a config string or callable to an actual function in a given module).
- **Path builders**: `create_output_paths`, `create_pop_output_paths`.
- **Data preparation**: `prepare_data` (loads and enriches WWTP, basin, and country layers). Can be replaced via `cfg['prepare_data_fn']`.
- **Voronoi execution**: `run_voronoi_approach` (runs one approach end-to-end using area/buffer functions resolved from `cfg['calculate_area_fn']` and `cfg['calculate_buffer_fn']`).

No default values are hard-coded in this file; all defaults live in `config.yaml`.

### download_pop.py
Aim: Download and prepare population raster inputs. Inputs: Source URLs and output paths from config. Outputs: Local raster files and organized download artifacts. How: It handles retrieval, extraction, and file placement into configured directories.

### create_voronoi.py
Aim: Build weighted Voronoi service regions for selected approaches. Inputs: Corrected points, clipping boundaries, basin/country data, and optional CLI overrides. Outputs: Voronoi layers and related per-approach artifacts. How: It groups points, computes weighted assignment on a grid, extracts polygons, resolves overlaps, and writes outputs. The data-preparation step is delegated to the function named by `cfg['prepare_data_fn']`.

Key configurable parameters (all in `config.yaml`):
- `dynamic_buffering` / `dynamic_buffer_k` / `min_buffer` — per-site buffer sizing.
- `calculate_area_fn` / `calculate_buffer_fn` — function names in `create_voronoi.py` used for area and buffer computation; accept any compatible callable name.
- `prepare_data_fn` — function name in `pipelines.py` used to load and enrich all spatial inputs; swap to inject a custom data loader without touching orchestration code.
- `site_id_column` / `old_site_id_column` / `basin_column_name` / `country_output_column` / `country_boundary_column` — column names that propagate through all stages.

### add_pop.py
Aim: Attach population values to generated Voronoi layers. Inputs: Voronoi outputs plus country/population rasters. Outputs: Population-enriched layers and summary tables. How: It intersects Voronoi regions with rasters by country and aggregates statistics in parallel.

### combine_watersheds.py
Aim: Build one combined basin dataset from zipped source layers. Inputs: Basin zip directory and output path config. Outputs: Merged basin layer. How: It reads each archive, extracts valid geometry layers, harmonizes schema, and writes one unified file.

### industrial_analysis/download_and_vectorize.py
Aim: Download and vectorize industrial land rasters, then enrich with country and basin attributes. Inputs: Industrial raster archive URL, basin layer, country boundaries cache, and config overrides. Outputs: Merged industrial land polygons in GeoPackage format. How: It downloads the archive, vectorizes all nested raster folders in parallel applying a configurable minimum-cell filter (`industrial_min_cells`), merges geometries, then transfers boundary attributes without clipping geometry.

Caching behavior:
- The merged pre-enrichment polygons are written to `data/industrial_analysis/industrial_areas_mp{N}.gpkg` (where N = `industrial_min_cells`). On subsequent runs the enrichment step is skipped straight to `add_boundary_info` if this file exists and `industrial_vectorize_overwrite=false`.
- When `industrial_persist_rasters=true`, rasters are downloaded to `paths.industrial_raster_persistent_dir` and reused on subsequent runs. When `false`, a temporary directory is used and rasters are discarded after vectorization.

### industrial_analysis/find_unconnected_industrial_areas.py
Aim: Identify industrial polygons not covered by industrial-filtered WWTP Voronoi service regions. Inputs: Merged industrial polygons, corrected WWTP dataset, basin/country data, and selected approach (0 or 1). Outputs: Unconnected industrial area GeoPackage. How: It filters WWTPs by configured industrial categories, runs Voronoi with create_voronoi-compatible parameters, and keeps industrial polygons outside service regions.

## Shell Scripts (Entry Points)

### download_pop.sh
Aim: Reproducible launcher for population download stage. Inputs: Config defaults and optional CLI overrides. Outputs: Stage logs and downloaded population inputs. How: It sets runtime context and runs `python -m research_code.download_pop`.

### create_voronoi.sh
Aim: Reproducible launcher for Voronoi generation. Inputs: Optional overrides for level/version/buffer/weight settings. Outputs: Voronoi stage logs and Voronoi datasets. How: It prepares environment and runs `python -m research_code.create_voronoi`.

### add_pop.sh
Aim: Reproducible launcher for population attachment stage. Inputs: Voronoi file index plus optional overrides. Outputs: Stage logs and population-enriched Voronoi outputs. How: It validates args and runs `python -m research_code.add_pop`.

### combine_watersheds.sh
Aim: Reproducible launcher for basin merge utility. Inputs: Config paths and optional overrides. Outputs: Merge logs and combined basin file. How: It prepares logging and runs `python -m research_code.combine_watersheds`.

### industrial_analysis/industrial_analysis.sh
Aim: Run the industrial analysis branch end-to-end. Inputs: Optional overrides for level/version/buffer/weight settings and dynamic buffering controls. Outputs: Industrial merged layer plus unconnected industrial areas. How: It runs the industrial raster vectorization stage followed by the unconnected-industrial detection stage.

## Shell -> Python Flow Diagram

```text
download_pop.sh        -> python -m research_code.download_pop
create_voronoi.sh      -> python -m research_code.create_voronoi
add_pop.sh             -> python -m research_code.add_pop
combine_watersheds.sh  -> python -m research_code.combine_watersheds
industrial_analysis/industrial_analysis.sh -> python -m research_code.industrial_analysis.download_and_vectorize
										   -> python -m research_code.industrial_analysis.find_unconnected_industrial_areas
```

## Override Conventions

Most shell entry points accept positional dynamic Voronoi overrides after weight args:

```bash
[level] [version] [buffer] [weight_method] [weight_func] [dynamic_buffering] [dynamic_buffer_k]
```

This keeps data merge, Voronoi, population, figure, validation, and sweep flows aligned on one override scheme.

## Related Subfolders

- `annotation_scripts/README.md`
- `data_merge/README.md`
- `figures_scripts/README.md`
- `pop_at_risk_river_calculations/README.md`
- `pop_validation_scripts/README.md`
- `sensitivity_analysis_scripts/README.md`
