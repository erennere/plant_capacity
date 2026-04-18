# Plant Capacity Pipeline

This repository contains geospatial workflows for wastewater treatment plant (WWTP) processing, population coverage estimation, and downstream impact analysis.

The code has evolved, and this README now reflects the current scripts and folder layout.

## Repository Layout

- `data/`: Input, intermediate, and output geospatial/tabular data.
- `research_code/`: Python package and executable shell workflows.

## `research_code/` Folder Guide

Top-level files:

- `config.yaml`: Central configuration for paths, parameters, and execution modes.
- `starter.py`: Config loader with path expansion.
- `pipelines.py`: Shared orchestration helpers used by pipeline stages.
- `download_pop.py`, `create_voronoi.py`, `add_pop.py`: Main stage scripts.
- `download_pop.sh`, `create_voronoi.sh`, `add_pop.sh`: Main shell entrypoints.

Subfolders:

- `data_merge/`: WWTP harmonization for heterogeneous sources, including correction, clustering, and segmentation merge integration.
  - Includes `combine_locations.sh`, `correct_locations_w_OSM.py`, `merge_seg_results.py`, `final_data_merge.py`.
- `annotation_scripts/`: Annotation-data processing for Maxar tiles.
  - Uses Overpass/OSM metadata (WWTP-related, landuse, water, industrial tags) around WWTP-centered tiles.
  - Tag overlays are created for downstream multimodal/LLM-assisted annotation workflows.
- `pop_at_risk_river_calculations/`: Unserved-population and impact workflows.
  - Finds unserved population areas, links them to nearest river network context, propagates downstream burden with decay, and creates impact polygons and population-at-risk outputs.
- `figures_scripts/`: Export and plotting scripts for publication/communication outputs.
- `pop_validation_scripts/`: Validation and comparison scripts.
  - Current checks focus on HydroWaste and European WWTP reference comparisons.

## Setup

The codebase is packaged as a Python package (`research-code`).

Run commands from the `research_code/` directory.

```bash
cd research_code
python -m pip install -e .
```

Suggested environment:

- Python 3.9+
- Bash shell
- Optional SLURM for large runs
- Geospatial stack compatible with geopandas/rasterio/shapely

## Configuration

Main config file: `research_code/config.yaml`

Important sections:

- `paths`: Input/output locations and templates
- `params`: Numeric runtime parameters (buffer, workers, thresholds, etc.)
- `booleans`: Feature toggles
- `execution.mode`: Voronoi run mode (`array`, `sequential`, `parallel`)
- `annotations.default_mode`: Raster stage mode (`array`, `sequential`, `parallel`)

Before running full pipelines, verify at minimum:

- `paths.data_dir`
- `arguments.default_version`
- `params.buffer`
- `paths.seg_results_filepath` (segmentation CSV used by merge step)
- `paths.annotations_images_dir` (source image tiles for annotation)
- `paths.annotated_images_output_dir` (annotated image output directory)
- `paths.annotations_results_filepath` (annotation CSV output path)
- `paths.annotations_verf_image_outpath_dir` (verification image output directory)

## Current Main Workflow

Run everything from `research_code/`.

In practice, the order that matches the current codebase is:

1. Run data merge (`data_merge/combine_locations.sh`)
2. Run annotation scripts (after data merge, before population download)
3. Download population and create Voronoi layers, then add population
4. Run pop-at-risk pipeline (`create_rasters`, then impact stages, then danger-pop stage)
5. Run figures and validation scripts as needed

### 1) Data merge first (`data_merge/`)

```bash
bash data_merge/combine_locations.sh
```

This wrapper should be run as-is first. It executes:

1. `research_code.data_merge.correct_locations_w_OSM`
2. `research_code.data_merge.merge_seg_results --variant old`
3. `research_code.data_merge.final_data_merge`
4. `research_code.data_merge.merge_seg_results --variant new`

Why there are two merge variants:

- `--variant old` is kept for legacy index compatibility with segmentation results which were already run before the cleaning of code.
- `--variant new` applies the current segmentation merge path.

If you fully re-run segmentation and do not need backward-compatibility merges, set `booleans.legacy_merge: false` in `research_code/config.yaml`. In that mode the pipeline skips the legacy segmentation merge and `final_data_merge.py` reads from `corrected_south` instead of `seg_corrected_south`.

### 2) Annotation scripts (after data_merge, before population download)

Run grid creation and OSM extraction:

```bash
bash annotation_scripts/grid_generation_and_osm_extract.sh
```

What it runs now:

1. `NEW_01_GENERATEGRIDS.py` (builds annotation grids)
2. `NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py` (extracts OSM features per grid)

Run annotation image generation:

```bash
sbatch annotation_scripts/run_download_bing_annotate_array.sh
```

What it runs:

- `download_bing_annotate.py` in array mode.

Current status notes:

- `NEW_03_WASTEWATERJOIN_GEOJSON.py` is not part of the default shell flow.
- It can be enabled manually in `grid_generation_and_osm_extract.sh` if you want a merged OSM metadata stage.
- In most current reruns, merged OSM metadata is not required because `download_bing_annotate.py` works directly with single bbox GeoJSON inputs.
- `NEW_04_EXPORTGEOTIFF.py` is not needed in the current pipeline.

When to enable `NEW_03_WASTEWATERJOIN_GEOJSON.py`:

- Only if you explicitly need a merged metadata/parquet consolidation stage.
- For normal annotation reruns, keep it disabled.

New scripts added under `annotation_scripts/`:

- `merge_annotations.py` + `merge_annotations.sh`
  - Purpose: parse model annotation text fields and merge them into the main points dataset (`corrected_all_filepath`) by image-derived `idx`.
  - Run timing: after annotation inference CSV is produced, before Voronoi/population stages.

- `annotations_inspection.py` + `annotations_inspection.sh` (optional)
  - Purpose: create a category histogram, write a stratified review sample CSV, and copy sampled images into per-category folders for manual QA.
  - Outputs are written to `paths.annotations_verf_image_outpath_dir`.

### 3) Population + Voronoi + population attachment

First download/process population rasters:

```bash
bash download_pop.sh
```

Then create Voronoi outputs:

```bash
bash create_voronoi.sh
```

Mode is read from `execution.mode` in `config.yaml`.

Then attach population to Voronoi outputs:

Local example (single index):

```bash
bash add_pop.sh 0
```

SLURM array mode:

```bash
sbatch add_pop.sh
```

### 4) Pop-at-risk pipeline (`pop_at_risk_river_calculations/`)

Run these in order.

Step A: create rasters first.

```bash
bash pop_at_risk_river_calculations/create_rasters.sh
```

What it does:

- Builds served/not-served raster products used by downstream steps.
- Uses `annotations.default_mode` (`array`, `sequential`, `parallel`) for execution behavior.

Step B: compute unserved/difference/river/impact products.

```bash
bash pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh
```

What it does internally, in order:

1. `find_unserved_pop`: extracts non-served areas from raster outputs.
2. `find_diff_pop`: calculates population differences and writes diff outputs.
3. `assign_rivers_to_basin`: links rivers to basin IDs.
4. `find_intersection_river`: finds nearest river systems for non-served areas.
5. `impact_polygons_pop`: propagates downstream impact and creates impact polygons.

Step C: run danger-pop post-processing.

```bash
bash pop_at_risk_river_calculations/find_pop_in_danger_pop.sh
```

What it does:

- Aggregates/exports final population-at-risk outputs from impact products.

## Figures and Validation

Figures (`figures_scripts/`):

- `piechart_figure.py`: static summary chart output.
- `piechart_interactive.py`: interactive chart output.
- `convert_voronoi_to_geojson_for_map.py`: prepares map-friendly GeoJSON for visualization.

Validation (`pop_validation_scripts/`):

- `verification_script.py`: core verification checks.
- `hw_comparison.py`, `eu_comparison.py`: source-specific comparisons.
- `comparison.sh`: shell wrapper for running comparisons.

## SLURM Quick Reference

Example submission sequence (from `research_code/`):

```bash
sbatch data_merge/combine_locations.sh
sbatch annotation_scripts/grid_generation_and_osm_extract.sh
sbatch annotation_scripts/run_download_bing_annotate_array.sh
sbatch annotation_scripts/merge_annotations.sh
sbatch annotation_scripts/annotations_inspection.sh  # optional QA sampling
sbatch download_pop.sh
sbatch create_voronoi.sh
sbatch add_pop.sh
sbatch pop_at_risk_river_calculations/create_rasters.sh
sbatch pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh
sbatch pop_at_risk_river_calculations/find_pop_in_danger_pop.sh
```

## Logs

Most shell wrappers write logs under `research_code/logs/`.

If a stage fails, inspect corresponding `.out`, `.err`, and stage-specific `.log` files first.

## Imports

```python
from research_code.starter import load_config
from research_code.pipelines import create_output_paths
```
