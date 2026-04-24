# Plant Capacity Pipeline

This repository is a geospatial workflow for wastewater treatment plant (WWTP) analysis. It transforms raw plant points and boundaries into service areas, population overlays, industrial-coverage diagnostics, and downstream risk outputs. The pipeline is designed for reproducible local runs and SLURM-based batch execution.

## What This Project Produces

At a high level, the pipeline produces:

1. **A cleaned and merged WWTP point layer:** Harmonized treatment plant locations from multiple data sources (HydroWaste, UWWTD, country-specific imports) with corrected coordinates using DL and OSM.

2. **Weighted Voronoi service-area layers using segmentation results for weights:** Service regions that delineate areas likely served by each WWTP, computed using spatial weights derived from satellite-detected infrastructure (building types, urban density, industrial zones) normalized across watershed basins. Multiple approaches support different weighting schemes and basin constraints and watershed basins (HydroBASIN).

3. **Population-enriched Voronoi outputs integrated over WorldPOP:** Zonal statistics that aggregate population distributions (WorldPOP) to each Voronoi service region, enabling estimates of population served and exposure to treatment coverage. Results are stratified by country and year.

4. **Non-served and river-linked impact outputs plus validation figures using HydroSHED:** Analysis of unserved populations, downstream propagation of water quality impacts via river networks (HydroSHED), and comparative validation outputs to assess robustness of service-area assumptions across parameter sensitivity sweeps.

5. **Industrial-coverage diagnostics and unconnected industrial areas:** Rasterized industrial land use (Yoo et al., 2025) with overlap analysis against WWTP service regions, identifying industrial areas not currently covered by treatment infrastructure and highlighting potential infrastructure gaps for industrial wastewater management and impact.

6. **Static and interactive figures for communication:** Map-ready visualizations including Voronoi service-area maps, population distributions, area-to-population ratio diagnostics, and interactive web maps. These outputs are designed for scientific reports, and policy briefings on wastewater infrastructure coverage and service adequacy.

7. **Sensitivity analysis sweep results:** Systematic comparisons of how outputs (Voronoi geometry, population estimates, service coverage) change across parameter combinations (level, buffer distance, weighting method, dynamic buffering regimes). Results are organized for batch comparisons and robustness assessment, supporting evidence-based parameter selection for final runs.

## Folder Guide

### data/
This folder stores all datasets used by the pipeline, including raw inputs, intermediates, and generated outputs. It contains boundaries, extra point sources, merged products, figures, and final data exports. Most path templates in configuration resolve somewhere under this folder.

### research_code/
This is the executable codebase for the project. It contains the Python package modules and the shell wrappers you run for reproducible workflows in local or cluster environments. All stage scripts load shared configuration from research_code/config.yaml via starter.py.

### research_code/data_merge/
This folder merges and harmonizes source WWTP-like datasets before geospatial modeling starts. It handles schema alignment and location corrections so downstream Voronoi and population steps receive a consistent base table. Use this as the first stage when rebuilding from source inputs.

### research_code/annotation_scripts/
This folder covers annotation-grid generation, imagery retrieval, and annotation merging workflows. It is mainly used to build or refresh supporting variables that can influence weighting behavior in Voronoi runs. The scripts are designed for scheduled array workflows and batch post-processing.

### research_code/industrial_analysis/
This folder handles industrial-land analysis relative to WWTP coverage. It downloads and vectorizes industrial raster products, applies configurable minimum connected-cell filtering, and computes industrial areas not covered by industrial/mixed WWTP service regions. It also supports persistent raster caching for faster reruns.

### research_code/figures_scripts/
This folder creates communication outputs from processed pipeline data. It includes static and interactive figures plus map-ready exports built from Voronoi and population-enriched layers. Composite diagnostics for area-to-population ratios are also generated here.

### research_code/pop_at_risk_river_calculations/
This folder computes non-served population and river-linked impact products. It runs raster and polygon operations to estimate how exposure may propagate downstream. These scripts generate key risk-analysis outputs consumed by validation and reporting stages.

### research_code/pop_validation_scripts/
This folder validates model outputs through comparisons and QA-oriented summaries. It helps you check whether chosen parameter settings produce stable and plausible population estimates. Use it after generation stages to audit output quality before sharing results.

### research_code/sensitivity_analysis_scripts/
This folder runs parameter sweeps across level, buffer, weighting, and related controls. It is designed for cluster execution and supports both rigid and dynamic buffering regimes. Use it to compare how assumptions change service-area and population outcomes.

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

All parameter defaults live exclusively in `config.yaml`; no defaults are hard-coded in Python. Most users should edit these keys first:

1. arguments.default_version
2. arguments.default_level
3. params.buffer
4. params.weight_method
5. params.weight_func
6. params.dynamic_buffering
7. params.dynamic_buffer_k
8. params.min_buffer
9. params.site_id_column — primary site identifier column name in the WWTP dataset
10. params.old_site_id_column — site ID column in the original (unmodified) dataset
11. params.basin_column_name — basin/watershed identifier column used for spatial joins
12. params.country_output_column — country-code column written to enriched WWTP outputs
13. params.country_boundary_column — country-code column on the country boundary layer
14. params.calculate_area_fn — name of the area-computation function in `create_voronoi.py` (default: `"calculate_area"`)
15. params.calculate_buffer_fn — name of the buffer-computation function in `create_voronoi.py` (default: `"calculate_buffer"`)
16. params.prepare_data_fn — name of the data-loading function in `pipelines.py` (default: `"prepare_data"`); swap to inject a custom loader
17. params.industrial_zenodo_url
18. params.industrial_min_cells
19. params.industrial_persist_rasters
20. paths.data_dir
21. paths.seg_results_filepath
22. paths.annotations_images_dir
23. paths.annotations_results_filepath
24. booleans.legacy_merge
25. execution.mode
26. annotations.default_mode
27. annotations.n_sample_size — number of annotation samples drawn during inspection
28. annotations.random_seed

Practical guidance:

- If you are doing a clean rerun and do not need legacy segmentation compatibility, set booleans.legacy_merge to false.
- Keep paths.data_dir stable and move version, level, and buffer through arguments and params.
- If you are testing locally, use a smaller buffer and fewer workers first to iterate quickly.
- For industrial analysis runs, set `industrial_min_cells` based on your raster resolution (100 cells ≈ 1 ha at 10 m resolution) and toggle `industrial_persist_rasters` to reuse raster downloads across runs.
- Dynamic buffering (when `dynamic_buffering=true`) derives per-site buffer lengths from nearest-neighbor spacing; adjust `dynamic_buffer_k` (typically 0.5–1.0) to control how aggressively buffers scale with local density.
- To swap in a custom data loader without changing orchestration code, implement the function in `pipelines.py` and set `params.prepare_data_fn` to its name. The same pattern applies to `calculate_area_fn` and `calculate_buffer_fn` in `create_voronoi.py`.
- Use the sensitivity analysis scripts to compare outputs across parameter combinations before committing to a final run configuration.

## End-to-End Run Order (Recommended)

Run from research_code/:

1. data_merge/combine_locations.sh
2. combine_watersheds.sh
3. annotation_scripts/grid_generation_and_osm_extract.sh
4. annotation_scripts/run_download_bing_annotate_array.sh
5. annotation_scripts/merge_annotations.sh
6. download_pop.sh
7. create_voronoi.sh
8. add_pop.sh
9. pop_at_risk_river_calculations/create_rasters.sh
10. pop_at_risk_river_calculations/pop_differences_and_impact_polygons.sh
11. pop_at_risk_river_calculations/find_pop_in_danger_pop.sh
12. industrial_analysis/industrial_analysis.sh
13. pop_validation_scripts/comparison.sh and figures scripts

## Sensitivity Analysis

Use research_code/sensitivity_analysis_scripts when you want to test combinations of level, buffer, weighting method, weighting function, and approach. Sweep scripts support rigid and dynamic buffering and can run through SLURM arrays. This is the recommended way to compare parameter robustness before final reporting.

## Folder Documentation

Detailed run docs per folder:

- research_code/README.md
- research_code/data_merge/README.md
- research_code/annotation_scripts/README.md
- research_code/industrial_analysis/README.md
- research_code/pop_at_risk_river_calculations/README.md
- research_code/figures_scripts/README.md
- research_code/pop_validation_scripts/README.md
- research_code/sensitivity_analysis_scripts/README.md

## Logs and Troubleshooting

Most wrappers write .out, .err, and .log files. Start debugging there before changing code. Common quick checks are wrong config paths, missing boundary sidecar files (.dbf/.shx/.prj), missing external mounts, and over-aggressive worker counts for available memory/CPU.