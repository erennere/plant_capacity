# Plant Capacity Pipeline

This repository is a practical geospatial workflow for wastewater treatment plant (WWTP) analysis. The goal is not just to map plant points, but to build a usable chain from raw data to service-area estimates, population coverage, and downstream population-at-risk outputs. If you are opening this project for the first time, this README is meant to help you understand what to run, why it matters, and what you should change before a production run.

## What This Project Produces

At a high level, the pipeline produces:

1. A cleaned and merged WWTP point layer.
2. Weighted Voronoi service-area layers.
3. Population-enriched Voronoi outputs.
4. Non-served and river-linked impact outputs.
5. Validation tables and visualization-ready files.

In plain language: it estimates who is likely served, who might be unserved, and where potential downstream burden may accumulate.

## Repository Structure

- data/: inputs, intermediate products, and final outputs.
- research_code/: executable scripts, orchestration wrappers, and package modules.

## First-Time Setup

From research_code/:

```bash
cd research_code
python -m pip install -e .
```

Recommended environment:

- Python 3.9+
- Bash shell (local or SLURM cluster)
- Geospatial dependencies compatible with geopandas, shapely, rasterio, duckdb spatial

## Before Running: Required Files Checklist

The code can run with defaults only if key source files exist where config paths expect them.

Core files commonly required:

- data/bboxes.csv
- data/cities.csv
- data/cleaned_hydrowaste.csv
- data/wastewater_plant.geojson
- data/boundaries/ne_110m_admin_0_countries.shp plus .dbf/.shx/.prj companions
- data/extra_points/UWWTD_TreatmentPlants.gpkg
- data/extra_points/Canada_14_03_2025.csv
- data/extra_points/US_mapped_data_final.csv
- data/extra_points/Germany_Hydra_waste_geospatial_corrected.geojson

Environment-specific external inputs to verify:

- paths.seg_results_filepath in research_code/config.yaml
- paths.annotations_images_dir in research_code/config.yaml
- paths.annotations_results_filepath in research_code/config.yaml

If these are missing or pointed to old mounts, the pipeline may start but fail in mid-run.

## Configuration: What You Usually Need To Change

Main config: research_code/config.yaml

Most users should edit these keys first:

1. arguments.default_version
2. arguments.default_level
3. params.buffer
4. params.weight_method
5. params.weight_func
6. params.dynamic_buffering
7. params.dynamic_buffer_k
8. params.min_buffer
9. params.basin_column_name
10. params.industrial_zenodo_url
11. paths.data_dir
12. paths.seg_results_filepath
13. paths.annotations_images_dir
14. paths.annotations_results_filepath
15. booleans.legacy_merge
16. execution.mode
17. annotations.default_mode

Practical guidance:

- If you are doing a clean rerun and do not need legacy segmentation compatibility, set booleans.legacy_merge to false.
- If you are testing locally, use a smaller buffer and fewer workers first.
- Keep paths.data_dir stable and move version/level/buffer through arguments and params.

## Voronoi Configuration Guide (Important)

The Voronoi stage is the most configurable part of the project.

### Approaches

create_voronoi supports three approaches:

- Approach 0: WWTP buffer Voronoi (no watershed clipping)
- Approach 1: watershed-constrained WWTP Voronoi
- Approach 2: city-based Voronoi

How to run a specific approach:

```bash
python -m research_code.create_voronoi --approach 0
python -m research_code.create_voronoi --approach 1
python -m research_code.create_voronoi --approach 2
```

### Buffer

- Controlled by params.buffer (or CLI positional override).
- Larger buffer generally expands candidate influence area and can smooth boundaries.
- Smaller buffer is faster and more local but may under-cover sparse regions.

### Dynamic Buffering

- Controlled by params.dynamic_buffering and params.dynamic_buffer_k.
- When enabled, per-site buffer lengths are derived from nearest-neighbor spacing and site weights.
- params.min_buffer is propagated to weighted_voronoi and sets the minimum dynamic buffer parameter.
- All shell wrappers and sweep scripts now support dynamic overrides through positional args:
    [dynamic_buffering] [dynamic_buffer_k].

### Weight Normalization Method

Controlled by params.weight_method (or CLI override). This normalization is applied individually within each watershed basin. Allowed values:

- linear
- logarithmic
- square_root
- sigmoid

Interpretation:
- linear keeps original relative magnitude.
- logarithmic compresses large values strongly.
- square_root is a moderate compression.
- sigmoid pushes values into a bounded curve and can dampen extremes.

### Weighting Function

Controlled by params.weight_func (or CLI override). Allowed values:

- mult
- add
- empty string

Interpretation:

- mult uses multiplicative weighted distance behavior.
- add uses additive weighted distance behavior.
- empty string falls back to baseline multiplicative distance flow used by current code path.

### only_round Option

- CLI flag: --only_round
- Applies to approaches 0 and 1.
- Uses only round-area weights rather than all points.

Example with full overrides:

```bash
python -m research_code.create_voronoi 8 2 15000 square_root mult --approach 1 --only_round
```

### Output Path Token Behavior

- Voronoi and derived output paths now use a buffer path token.
- Rigid buffering runs keep numeric buffer tokens.
- Dynamic buffering runs use k-based tokens (for example k0_75) to avoid collisions and to keep outputs grouped by dynamic regime.

## End-to-End Run Order (Recommended)

Run from research_code/:

1. data_merge/combine_locations.sh
2. combine_watersheds.sh 
3. annotation_scripts/grid_generation_and_osm_extract.sh
4. annotation_scripts/run_download_bing_annotate_array.sh 
5. annotation_scripts/merge_annotations.sh (if annotation outputs exist, these values will be used as weights in voronoi tesselation)
6. download_pop.sh
7. create_voronoi.sh
8. add_pop.sh
9. Optional industrial branch: industrial_analysis/industrial_analysis.sh
10. pop_at_risk_river_calculations/create_rasters.sh
11. pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh
12. pop_at_risk_river_calculations/find_pop_in_danger_pop.sh
13. pop_validation_scripts/comparison.sh and figures scripts

Workflow relationship:

```text
data merge -> annotation context -> voronoi -> population overlay
         -> non-served + rivers -> impact -> final at-risk outputs
         -> validation + figures
```

## Sensitivity Analysis

Sensitivity scripts are in research_code/sensitivity_analysis_scripts/README.md.

Use this when you want to test how outcomes change with combinations of:

- level
- buffer
- weight_method
- weight_func
- approach

Typical sweep commands:

```bash
sbatch sensitivity_analysis_scripts/create_voronoi_param_sweep.sh or sbatch sensitivity_analysis_scripts/create_voronoi_param_sweep_parallel.sh
sbatch sensitivity_analysis_scripts/add_pop_param_sweep.sh
sbatch sensitivity_analysis_scripts/industrial_analysis_sweep.sh

```

Sweep updates now included:

- Level grid expanded to 6-9.
- Rigid and dynamic buffering regimes are both generated.
- Dynamic regime sweeps dynamic_buffer_k values.
- Empty weight_func combinations are deduplicated to one canonical method.

## Folder Documentation

Detailed run docs per folder:

- research_code/README.md
- research_code/data_merge/README.md
- research_code/annotation_scripts/README.md
- research_code/pop_at_risk_river_calculations/README.md
- research_code/figures_scripts/README.md
- research_code/pop_validation_scripts/README.md
- research_code/sensitivity_analysis_scripts/README.md

## Logs and Troubleshooting

Most wrappers write .out/.err and .log files. Start debugging there before changing code.

Common quick checks:

- wrong path in config
- missing boundary sidecar files (.dbf/.shx/.prj)
- missing segmentation CSV mount
- running high-parallel jobs without enough memory/CPU
