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
Aim: Centralize configuration loading and override parsing for all modules. Inputs: `config.yaml` and optional runtime overrides from CLI wrappers. Outputs: A normalized configuration object with resolved paths and runtime flags. How: It parses overrides, expands path templates, and returns one consistent config dictionary used across the pipeline.

### pipelines.py
Aim: Provide shared orchestration helpers used by top-level modules. Inputs: Loaded config and prepared GeoDataFrames from upstream stages. Outputs: Prepared stage inputs, output paths, and helper-driven results for Voronoi/population workflows. How: It consolidates repeated orchestration logic so entry scripts stay focused and consistent.

### download_pop.py
Aim: Download and prepare population raster inputs. Inputs: Source URLs and output paths from config. Outputs: Local raster files and organized download artifacts. How: It handles retrieval, extraction, and file placement into configured directories.

### create_voronoi.py
Aim: Build weighted Voronoi service regions for selected approaches. Inputs: Corrected points, clipping boundaries, watershed/country data, and optional CLI overrides. Outputs: Voronoi layers and related per-approach artifacts. How: It groups points, computes weighted assignment on a grid, extracts polygons, resolves overlaps, and writes outputs.

Recent behavior updates:
- Dynamic buffering controls (dynamic_buffering and dynamic_buffer_k) are parsed and propagated through orchestration.
- min_buffer is propagated from config to weighted_voronoi.
- Output path formatting uses a buffer path token so dynamic runs are separated by k-tokenized path names.

### add_pop.py
Aim: Attach population values to generated Voronoi layers. Inputs: Voronoi outputs plus country/population rasters. Outputs: Population-enriched layers and summary tables. How: It intersects Voronoi regions with rasters by country and aggregates statistics in parallel.

### combine_watersheds.py
Aim: Build one combined watershed dataset from zipped source layers. Inputs: Watershed zip directory and output path config. Outputs: Merged watershed layer. How: It reads each archive, extracts valid geometry layers, harmonizes schema, and writes one unified file.

### industrial_analysis/download_and_vectorize.py
Aim: Download and vectorize industrial land rasters, then enrich with country and basin attributes. Inputs: Industrial raster archive URL, watershed layer, country boundaries cache, and config overrides. Outputs: Merged industrial land polygons in GeoPackage format. How: It downloads the archive, vectorizes all nested raster folders in parallel, merges geometries, then transfers boundary attributes without clipping geometry.

### industrial_analysis/find_unconnected_industrial_areas.py
Aim: Identify industrial polygons not covered by industrial-filtered WWTP Voronoi service regions. Inputs: Merged industrial polygons, corrected WWTP dataset, watershed/country data, and selected approach (0 or 1). Outputs: Unconnected industrial area GeoPackage. How: It filters WWTPs by configured industrial categories, runs Voronoi with create_voronoi-compatible parameters, and keeps industrial polygons outside service regions.

## Shell Scripts (Entry Points)

### download_pop.sh
Aim: Reproducible launcher for population download stage. Inputs: Config defaults and optional CLI overrides. Outputs: Stage logs and downloaded population inputs. How: It sets runtime context and runs `python -m research_code.download_pop`.

### create_voronoi.sh
Aim: Reproducible launcher for Voronoi generation. Inputs: Optional overrides for level/version/buffer/weight settings. Outputs: Voronoi stage logs and Voronoi datasets. How: It prepares environment and runs `python -m research_code.create_voronoi`.

### add_pop.sh
Aim: Reproducible launcher for population attachment stage. Inputs: Voronoi file index plus optional overrides. Outputs: Stage logs and population-enriched Voronoi outputs. How: It validates args and runs `python -m research_code.add_pop`.

### combine_watersheds.sh
Aim: Reproducible launcher for watershed merge utility. Inputs: Config paths and optional overrides. Outputs: Merge logs and combined watershed file. How: It prepares logging and runs `python -m research_code.combine_watersheds`.

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

Most shell entry points now accept positional dynamic Voronoi overrides after weight args:

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
