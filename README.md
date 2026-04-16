# Plant Capacity SDS Research Code

This repository contains a geospatial processing pipeline for wastewater treatment plant (WWTP) capacity analysis, population coverage estimation, and downstream impact polygon generation.

The workflow combines:
- Population raster download and preprocessing
- WWTP location correction and merging
- Segmentation result integration
- Voronoi service area generation
- Population attachment to service areas
- Non-served population and river-based impact analysis

## Pipeline Order (Main Workflow)

Run the pipeline in this order:

1. `download_pop.sh`
2. `combine_locations.sh` (runs multiple Python scripts internally)
3. `merge_seg_results_v2.sh`
4. `create_voronoi.sh`
5. `add_pop.sh`
6. `create_rasters.sh`
7. `pop_differences_and_impact_polygons.sh` (runs multiple Python scripts internally)

Optional: run the annotation workflow after Step 3 and before Step 4.

This is the same order requested for this project.

## Prerequisites

- Python 3.10+ (recommended)
- Bash shell environment
- Optional: SLURM scheduler (for cluster/HPC execution)
- Core geospatial stack installed (GDAL/GEOS/PROJ compatible with your Python packages)

### Python packages used in this codebase

The scripts import the following major packages:
- geopandas
- shapely
- rasterio
- rasterstats
- pyproj
- pandas
- numpy
- scipy
- scikit-image
- opencv-python
- duckdb
- pycountry
- requests
- tqdm
- joblib
- networkx
- pyyaml

If you do not already have an environment, create one and install these packages before running the pipeline.

## Configuration

Main settings are in `config.yaml`:
- Path templates for all inputs/outputs under `paths`
- Runtime parameters under `params`
- Feature flags under `booleans`
- Threshold values under `figures`
- Voronoi execution mode under `execution.mode` (`array`, `sequential`, `parallel`)

Important defaults from `config.yaml`:
- `params.buffer: 11000`
- `params.max_workers: 16`
- `params.threshold: 500`
- `figures.pop_threshold: 1000`
- `execution.mode: array`

## Detailed Step-by-Step Execution

## 1) Download population rasters

Script: `download_pop.sh`

What it does:
- Validates Python availability
- Runs `download_pop.py`
- Downloads WorldPop country/year rasters
- Organizes files into configured population folders

Typical run:

```bash
bash download_pop.sh
```

SLURM run:

```bash
sbatch download_pop.sh
```

Key outputs (from config paths):
- Population data under `paths.pop_tif_dir` (default `../data/population/unzipped`)

## 2) Combine and correct WWTP locations

Script: `combine_locations.sh`

This stage is more complex due to heterogeneous data sources. The shell script runs these Python scripts in sequence:
1. `correct_locations_w_OSM.py`: Corrects HydroWaste WWTPs where no corresponding WWTP was found by Paul's model.
2. `merge_seg_results.py`: Merges segmentation results with WWTP data. This is mostly legacy behavior (older Bing-based flow) and is superseded by Step 3 (`merge_seg_results_v2.sh`).
3. `combine_locations.py`: Combines and cleans multiple regional sources (US, Germany, EU, Thailand, HydroWaste).
4. `final_data_merge.py`: Applies additional merging and clusters nearby points (under ~500 m).

What this stage does:
- Cleans and harmonizes WWTP coordinates
- Uses OSM-based matching/correction for uncertain points
- Merges segmentation outputs with mapped WWTP index data
- Produces combined/corrected WWTP point datasets

Refactor note:
- `merge_seg_results.py` can likely be removed from this stage.
- Most logic in `combine_locations.py` and `final_data_merge.py` can likely be consolidated.
- If refactored, update `config.yaml` and `starter.py` path usage accordingly.

Run:

```bash
bash combine_locations.sh
```

Key outputs include:
- `paths.corrected_south` (default `../data/corrected_WWTP_enhanced_v{version}.geojson`)
- `paths.corrected_all_filepath` (default `../data/all_merged_v{version}.gpkg`)
- `paths.new_points_filepath` (default `../data/new_locations_v{version}.gpkg`)

## 3) Merge segmentation results (v2)

Script: `merge_seg_results_v2.sh`

What it does:
- Runs `merge_seg_results_v2.py`
- Merges segmentation attributes from `paths.seg_results_filepath`
- Writes updates into `paths.corrected_all_filepath`

Run:

```bash
bash merge_seg_results_v2.sh
```

## Annotation Scripts Workflow (optional; typically before Step 4)

The `annotation_scripts/` folder contains the annotation data preparation and export flow:

1. `NEW_01_GENERATEGRIDS.py`
  - Creates square grid polygons around WWTP points.
2. `NEW_02_EXTRACTOSMDATAFULL_GEOJSON.py`
  - Queries Overpass OSM data per grid cell and writes per-index line/polygon GeoJSON files.
3. `NEW_03_WASTEWATERJOIN_GEOJSON.py`
  - Merges per-index outputs into parquet datasets and generates clustered wastewater polygons.
4. `download_bing_annotate.py`
  - Draws polygon/line labels on imagery tiles for annotation datasets.
5. `NEW_04_EXPORTGEOTIFF.py`
  - Exports georeferenced raster tiles from a grid/layer setup.
  - Not used at the moment.

### Annotation Notes

- By default, images are exported as `.png`, so Step 5 is usually not needed.
- `NEW_03_WASTEWATERJOIN_GEOJSON.py` is also optional for annotation, because `download_bing_annotate.py` can work directly with individual GeoJSON files.
- The merge script is mainly useful when you want to consolidate more than 100k polygon/line GeoJSON files for easier access.

## 4) Create Voronoi layers

Script: `create_voronoi.sh`

What it does:
- Runs `create_voronoi.py`
- Generates service-area (Voronoi) layers for WWTP approaches
- Supports three execution modes:
  - `array`: one approach per SLURM array task
  - `sequential`: one process runs all approaches in sequence
  - `parallel`: one process launches all approaches concurrently

Mode control:
- `config.yaml` -> `execution.mode`

Runs:

```bash
bash create_voronoi.sh
```

or (recommended on SLURM):

```bash
sbatch create_voronoi.sh
```

Key output folder:
- `paths.voronoi_dir` (default `../data/voronoi_layers/v{version}/{buffer}`)

## 5) Add population to Voronoi outputs

Script: `add_pop.sh`

What it does:
- Runs `add_pop.py <index>`
- Picks one Voronoi file by index
- Performs zonal stats intersection against population rasters
- Saves population-enriched GeoPackage as `pop_added_<original_name>.gpkg`

Local run (single index):

```bash
bash add_pop.sh 0
```

SLURM array run:

```bash
sbatch add_pop.sh
```

Key output folder:
- `paths.pop_output_dir` (default `../data/pop_voronoi_layers/v{version}/{buffer}`)

## 6) Create rasters

Script: `create_rasters.sh`

What it does:
- Runs the raster creation stage used before downstream population-difference and impact analysis
- Creates country-level rasters with 0 (population not served) / 1 (population served)
- Creates country-level statistics used for mapping
- Supports execution modes configured in `annotations.default_mode` (`array`, `sequential`, `parallel`)

Run:

```bash
bash create_rasters.sh
```

## 7) Population differences and impact polygons

Script: `pop_differences_and_impact_polygons.sh`

This shell script runs these Python scripts in sequence:
1. `find_unserved_pop.py`
2. `find_diff_pop.py`
3. `assign_rivers_to_basin.py 2`
4. `find_intersection_river.py 32`
5. `impact_polygons_pop.py 64`

What each sub-step does:

- `find_unserved_pop.py`
  - Filters non-served areas using `figures.pop_threshold`
  - Writes `paths.non_served_above_threshold_outpath`

- `find_diff_pop.py`
  - Computes watershed minus served-population differences
  - Writes `diff_*.gpkg` into `paths.pop_dif_output_dir`

- `assign_rivers_to_basin.py 2`
  - Assigns `HYBAS_ID` to river lines
  - Argument `2` is max worker count
  - Writes `paths.rivershed_output_path`

- `find_intersection_river.py 32`
  - Finds river systems/junctions nearest non-served polygons
  - Argument `32` is max worker count
  - Writes `paths.non_served_nxt_river_outpath`

- `impact_polygons_pop.py 64`
  - Propagates environmental load downstream and builds impact polygons
  - Argument `64` is max worker count
  - Writes `paths.impact_pop_polygons_outpath`

Run:

```bash
bash pop_differences_and_impact_polygons.sh
```

## Suggested End-to-End Commands

Local/sequential style:

```bash
bash download_pop.sh
bash combine_locations.sh
bash merge_seg_results_v2.sh
bash create_voronoi.sh

# Process each Voronoi file index you need
bash add_pop.sh 0
bash create_rasters.sh

bash pop_differences_and_impact_polygons.sh
```

SLURM style (where scripts already include SBATCH headers):

```bash
sbatch download_pop.sh
sbatch combine_locations.sh
sbatch merge_seg_results_v2.sh
sbatch create_voronoi.sh
sbatch add_pop.sh
sbatch create_rasters.sh
sbatch pop_differences_and_impact_polygons.sh
```

## Logs and Monitoring

- Most scripts write to `logs/`
- Example files:
  - `logs/pop_*.out`, `logs/pop_*.err`
  - `logs/add_pop_*.out`, `logs/add_pop_*.err`
  - `logs/pop_run.log`, `logs/add_pop_array.log`

Use these logs first for debugging failed jobs.

## Other Useful Scripts in This Repository

- `assign_rivers_to_basin.sh`: standalone basin assignment wrapper
- `find_intersection_river.sh`: standalone river-intersection wrapper
- `find_unserved_pop.sh`: standalone non-served-pop wrapper
- `create_rasters.sh`: helper for raster creation flows
- `combine_watersheds.py`: watershed preparation
- `verification_script.py` and `verification.sh`: validation checks
- `comparison.sh`, `eu_comparison.py`, `hw_comparison.py`: comparison/benchmark scripts

## Practical Notes

- Some shell scripts still use absolute Python script paths under `/mnt/sds-hd/...`.
  - If you moved this repository or run outside that environment, update these paths before execution.
- Many outputs are versioned and buffer-dependent via `config.yaml` path templates.
  - Keep `default_version` and `params.buffer` consistent across all steps.
- Large runs are memory/CPU intensive.
  - Prefer SLURM execution for full global runs.
